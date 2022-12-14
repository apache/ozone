/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;

/**
 * Handles the EC Under replication processing and forming the respective SCM
 * commands.
 */
public class ECUnderReplicationHandler implements UnhealthyReplicationHandler {

  public static final Logger LOG =
      LoggerFactory.getLogger(ECUnderReplicationHandler.class);
  private final PlacementPolicy containerPlacement;
  private final long currentContainerSize;
  private final NodeManager nodeManager;
  private final ReplicationManager replicationManager;

  private static class CannotFindTargetsException extends IOException {
    CannotFindTargetsException(Throwable cause) {
      super(cause);
    }
  }

  public ECUnderReplicationHandler(final PlacementPolicy containerPlacement,
      final ConfigurationSource conf, NodeManager nodeManager,
      ReplicationManager replicationManager) {
    this.containerPlacement = containerPlacement;
    this.currentContainerSize = (long) conf
        .getStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    this.nodeManager = nodeManager;
    this.replicationManager = replicationManager;
  }

  private boolean validatePlacement(List<DatanodeDetails> replicaNodes,
                                    List<DatanodeDetails> selectedNodes) {
    List<DatanodeDetails> nodes = new ArrayList<>(replicaNodes);
    nodes.addAll(selectedNodes);
    boolean placementStatus = containerPlacement
            .validateContainerPlacement(nodes, nodes.size())
            .isPolicySatisfied();
    if (!placementStatus) {
      LOG.warn("Selected Nodes does not satisfy placement policy: {}. " +
              "Selected nodes: {}. Existing Replica Nodes: {}.",
              containerPlacement.getClass().getName(),
              selectedNodes, replicaNodes);
    }
    return placementStatus;
  }

  /**
   * Identify a new set of datanode(s) to reconstruct the container and form the
   * SCM command to send it to DN. In the case of decommission, it will just
   * generate the replicate commands instead of reconstruction commands.
   *
   * @param replicas - Set of available container replicas.
   * @param pendingOps - Inflight replications and deletion ops.
   * @param result - Health check result.
   * @param remainingMaintenanceRedundancy - represents that how many nodes go
   *                                      into maintenance.
   * @return Returns the key value pair of destination dn where the command gets
   * executed and the command itself. If an empty list is returned, it indicates
   * the container is no longer unhealthy and can be removed from the unhealthy
   * queue. Any exception indicates that the container is still unhealthy and
   * should be retried later.
   */
  @Override
  public Map<DatanodeDetails, SCMCommand<?>> processAndCreateCommands(
      final Set<ContainerReplica> replicas,
      final List<ContainerReplicaOp> pendingOps,
      final ContainerHealthResult result,
      final int remainingMaintenanceRedundancy) throws IOException {
    ContainerInfo container = result.getContainerInfo();
    LOG.debug("Handling under-replicated EC container: {}", container);

    final ECContainerReplicaCount replicaCount =
        new ECContainerReplicaCount(container, replicas, pendingOps,
            remainingMaintenanceRedundancy);
    if (replicaCount.isSufficientlyReplicated()) {
      LOG.info("The container {} state changed and it's not in under"
              + " replication any more.", container.getContainerID());
      return emptyMap();
    }
    if (replicaCount.isSufficientlyReplicated(true)) {
      LOG.info("The container {} with replicas {} will be sufficiently " +
          "replicated after pending replicas are created",
          container.getContainerID(), replicaCount.getReplicas());
      return emptyMap();
    }
    if (replicaCount.isUnrecoverable()) {
      LOG.warn("The container {} is unrecoverable. The available replicas" +
          " are: {}.", container.containerID(), replicaCount.getReplicas());
      return emptyMap();
    }

    // don't place reconstructed replicas on exclude nodes, since they already
    // have replicas
    List<DatanodeDetails> excludedNodes = replicas.stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());
    // DNs that are already waiting to receive replicas cannot be targets
    excludedNodes.addAll(
        pendingOps.stream()
        .filter(containerReplicaOp -> containerReplicaOp.getOpType() ==
            ContainerReplicaOp.PendingOpType.ADD)
        .map(ContainerReplicaOp::getTarget)
        .collect(Collectors.toList()));

    final ContainerID id = container.containerID();
    final Map<DatanodeDetails, SCMCommand<?>> commands = new HashMap<>();
    try {
      final List<DatanodeDetails> deletionInFlight = new ArrayList<>();
      for (ContainerReplicaOp op : pendingOps) {
        if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
          deletionInFlight.add(op.getTarget());
        }
      }

      Map<Integer, Pair<ContainerReplica, NodeStatus>> sources =
          filterSources(replicas, deletionInFlight);
      List<DatanodeDetails> availableSourceNodes =
          sources.values().stream().map(Pair::getLeft)
              .map(ContainerReplica::getDatanodeDetails)
              .filter(datanodeDetails ->
                  datanodeDetails.getPersistedOpState() ==
                      HddsProtos.NodeOperationalState.IN_SERVICE)
              .collect(Collectors.toList());

      try {
        processMissingIndexes(replicaCount, sources, availableSourceNodes,
            excludedNodes, commands);
        processDecommissioningIndexes(replicaCount, replicas,
            availableSourceNodes, excludedNodes, commands);
        processMaintenanceOnlyIndexes(replicaCount, replicas, excludedNodes,
            commands);
        // TODO - we should be able to catch SCMException here and check the
        //        result code but the RackAware topology never sets the code.
      } catch (CannotFindTargetsException e) {
        // If we get here, we tried to find nodes to fix the under replication
        // issues, but were not able to find any at some stage, and the
        // placement policy threw an exception.
        // At this stage. If the cluster is small and there are some
        // over replicated indexes, it could stop us finding a new node as there
        // are no more nodes left to try.
        // If the container is also over replicated, then hand it off to the
        // over-rep handler, and after those over-rep indexes are cleared the
        // under replication can be re-tried in the next iteration of RM.
        // However, we should only hand off to the over rep handler if there are
        // no commands already created. If we have some commands, they may
        // attempt to use sources the over-rep handler would remove. So we
        // should let the commands we have created be processed, and then the
        // container will be re-processed in a further RM pass.
        LOG.debug("Unable to located new target nodes for container {}",
            container, e);
        if (commands.size() > 0) {
          LOG.debug("Some commands have already been created, so returning " +
              "with them only");
          return commands;
        }
        if (replicaCount.isOverReplicated()) {
          LOG.debug("Container {} is both under and over replicated. Cannot " +
              "find enough target nodes, so handing off to the " +
              "OverReplication handler", container);
          return replicationManager.processOverReplicatedContainer(result);
        } else {
          throw (SCMException)e.getCause();
        }
      }
    } catch (IOException | IllegalStateException ex) {
      LOG.warn("Exception while processing for creating the EC reconstruction" +
              " container commands for {}.",
          id, ex);
      throw ex;
    }
    if (commands.size() == 0) {
      LOG.warn("Container {} is under replicated, but no commands were " +
          "created to correct it", id);
    }
    return commands;
  }

  private Map<Integer, Pair<ContainerReplica, NodeStatus>> filterSources(
      Set<ContainerReplica> replicas, List<DatanodeDetails> deletionInFlight) {
    return replicas.stream().filter(r -> r
            .getState() == StorageContainerDatanodeProtocolProtos
            .ContainerReplicaProto.State.CLOSED)
        // Exclude stale and dead nodes. This is particularly important for
        // maintenance nodes, as the replicas will remain present in the
        // container manager, even when they go dead.
        .filter(r -> !deletionInFlight.contains(r.getDatanodeDetails()))
        .map(r -> Pair.of(r, ReplicationManager
            .getNodeStatus(r.getDatanodeDetails(), nodeManager)))
        .filter(pair -> pair.getRight().isHealthy())
        // If there are multiple nodes online for a given index, we just
        // pick any IN_SERVICE one. At the moment, the input streams cannot
        // handle multiple replicas for the same index, so if we passed them
        // all through they would not get used anyway.
        // If neither of the nodes are in service, we just pass one through,
        // as it will be decommission or maintenance.
        .collect(Collectors.toMap(
            pair -> pair.getLeft().getReplicaIndex(),
            pair -> pair,
            (p1, p2) -> {
              if (p1.getRight().getOperationalState() == IN_SERVICE) {
                return p1;
              } else {
                return p2;
              }
            }));
  }

  private List<DatanodeDetails> getTargetDatanodes(
      List<DatanodeDetails> excludedNodes, ContainerInfo container,
      int requiredNodes) throws IOException {
    // We should ensure that the target datanode has enough space
    // for a complete container to be created, but since the container
    // size may be changed smaller than origin, we should be defensive.
    final long dataSizeRequired =
        Math.max(container.getUsedBytes(), currentContainerSize);
    try {
      return containerPlacement
          .chooseDatanodes(excludedNodes, null, requiredNodes, 0,
              dataSizeRequired);
    } catch (SCMException e) {
      // SCMException can come from many places in SCM, so catch it here and
      // throw a more specific exception instead.
      throw new CannotFindTargetsException(e);
    }
  }

  /**
   * Processes replicas that are in maintenance nodes and should need
   * additional copies.
   * @throws IOException
   */
  private void processMissingIndexes(
      ECContainerReplicaCount replicaCount, Map<Integer,
      Pair<ContainerReplica, NodeStatus>> sources,
      List<DatanodeDetails> availableSourceNodes,
      List<DatanodeDetails> excludedNodes,
      Map<DatanodeDetails, SCMCommand<?>> commands) throws IOException {
    ContainerInfo container = replicaCount.getContainer();
    ECReplicationConfig repConfig =
        (ECReplicationConfig)container.getReplicationConfig();
    List<Integer> missingIndexes = replicaCount.unavailableIndexes(true);
    if (missingIndexes.size() == 0) {
      return;
    }

    if (sources.size() >= repConfig.getData()) {
      final List<DatanodeDetails> selectedDatanodes = getTargetDatanodes(
          excludedNodes, container, missingIndexes.size());

      if (validatePlacement(availableSourceNodes, selectedDatanodes)) {
        excludedNodes.addAll(selectedDatanodes);
        // TODO - what are we adding all the selected nodes to available
        //        sources?
        availableSourceNodes.addAll(selectedDatanodes);
        List<ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex>
            sourceDatanodesWithIndex = new ArrayList<>();
        for (Pair<ContainerReplica, NodeStatus> src : sources.values()) {
          sourceDatanodesWithIndex.add(
              new ReconstructECContainersCommand
                  .DatanodeDetailsAndReplicaIndex(
                  src.getLeft().getDatanodeDetails(),
                  src.getLeft().getReplicaIndex()));
        }

        final ReconstructECContainersCommand reconstructionCommand =
            new ReconstructECContainersCommand(container.getContainerID(),
                sourceDatanodesWithIndex, selectedDatanodes,
                int2byte(missingIndexes),
                repConfig);
        // Keeping the first target node as coordinator.
        commands.put(selectedDatanodes.get(0), reconstructionCommand);
      }
    } else {
      LOG.warn("Cannot proceed for EC container reconstruction for {}, due"
              + " to insufficient source replicas found. Number of source "
              + "replicas needed: {}. Number of available source replicas are:"
              + " {}. Available sources are: {}", container.containerID(),
          repConfig.getData(), sources.size(), sources);
    }
  }

  /**
   * Processes replicas that are in maintenance nodes and should need
   * additional copies.
   * @throws IOException
   */
  private void processDecommissioningIndexes(
      ECContainerReplicaCount replicaCount,
      Set<ContainerReplica> replicas,
      List<DatanodeDetails> availableSourceNodes,
      List<DatanodeDetails> excludedNodes,
      Map<DatanodeDetails, SCMCommand<?>> commands) throws IOException {
    ContainerInfo container = replicaCount.getContainer();
    Set<Integer> decomIndexes = replicaCount.decommissioningOnlyIndexes(true);
    if (decomIndexes.size() > 0) {
      final List<DatanodeDetails> selectedDatanodes =
          getTargetDatanodes(excludedNodes, container, decomIndexes.size());
      if (validatePlacement(availableSourceNodes, selectedDatanodes)) {
        excludedNodes.addAll(selectedDatanodes);
        Iterator<DatanodeDetails> iterator = selectedDatanodes.iterator();
        // In this case we need to do one to one copy.
        for (ContainerReplica replica : replicas) {
          if (decomIndexes.contains(replica.getReplicaIndex())) {
            if (!iterator.hasNext()) {
              LOG.warn("Couldn't find enough targets. Available source"
                      + " nodes: {}, the target nodes: {}, excluded nodes: {}"
                      + "  and the decommission indexes: {}",
                  replicas, selectedDatanodes, excludedNodes, decomIndexes);
              break;
            }
            DatanodeDetails decommissioningSrcNode
                = replica.getDatanodeDetails();
            final ReplicateContainerCommand replicateCommand =
                new ReplicateContainerCommand(container.getContainerID(),
                    ImmutableList.of(decommissioningSrcNode));
            // For EC containers, we need to track the replica index which is
            // to be replicated, so add it to the command.
            replicateCommand.setReplicaIndex(replica.getReplicaIndex());
            DatanodeDetails target = iterator.next();
            commands.put(target, replicateCommand);
          }
        }
      }
    }

  }

  /**
   * Processes replicas that are in maintenance nodes and should need
   * additional copies.
   * @param replicaCount
   * @param replicas set of container replicas
   * @param excludedNodes nodes that should not be targets for new copies
   * @param commands
   * @throws IOException
   */
  private void processMaintenanceOnlyIndexes(
      ECContainerReplicaCount replicaCount, Set<ContainerReplica> replicas,
      List<DatanodeDetails> excludedNodes,
      Map<DatanodeDetails, SCMCommand<?>> commands) throws IOException {
    Set<Integer> maintIndexes = replicaCount.maintenanceOnlyIndexes(true);
    if (maintIndexes.isEmpty()) {
      return;
    }

    ContainerInfo container = replicaCount.getContainer();
    // this many maintenance replicas need another copy
    int additionalMaintenanceCopiesNeeded =
        replicaCount.additionalMaintenanceCopiesNeeded(true);
    List<DatanodeDetails> targets = getTargetDatanodes(excludedNodes, container,
        additionalMaintenanceCopiesNeeded);
    excludedNodes.addAll(targets);

    Iterator<DatanodeDetails> iterator = targets.iterator();
    // copy replica from source maintenance DN to a target DN
    for (ContainerReplica replica : replicas) {
      if (maintIndexes.contains(replica.getReplicaIndex()) &&
          additionalMaintenanceCopiesNeeded > 0) {
        if (!iterator.hasNext()) {
          LOG.warn("Couldn't find enough targets. Available source"
                  + " nodes: {}, target nodes: {}, excluded nodes: {} and"
                  + " maintenance indexes: {}",
              replicas, targets, excludedNodes, maintIndexes);
          break;
        }
        DatanodeDetails maintenanceSourceNode = replica.getDatanodeDetails();
        final ReplicateContainerCommand replicateCommand =
            new ReplicateContainerCommand(
                container.containerID().getProtobuf().getId(),
                ImmutableList.of(maintenanceSourceNode));
        // For EC containers we need to track the replica index which is
        // to be replicated, so add it to the command.
        replicateCommand.setReplicaIndex(replica.getReplicaIndex());
        DatanodeDetails target = iterator.next();
        commands.put(target, replicateCommand);
        additionalMaintenanceCopiesNeeded -= 1;
      }
    }
  }

  private static byte[] int2byte(List<Integer> src) {
    byte[] dst = new byte[src.size()];

    for (int i = 0; i < src.size(); i++) {
      dst[i] = src.get(i).byteValue();
    }
    return dst;
  }
}
