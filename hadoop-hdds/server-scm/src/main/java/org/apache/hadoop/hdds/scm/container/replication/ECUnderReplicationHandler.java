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
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
  private final ReplicationManager replicationManager;

  private static class CannotFindTargetsException extends IOException {
    CannotFindTargetsException(Throwable cause) {
      super(cause);
    }
  }

  public ECUnderReplicationHandler(final PlacementPolicy containerPlacement,
      final ConfigurationSource conf, ReplicationManager replicationManager) {
    this.containerPlacement = containerPlacement;
    this.currentContainerSize = (long) conf
        .getStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
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
   * @return The number of commands sent.
   */
  @Override
  public int processAndSendCommands(
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
      return 0;
    }
    if (replicaCount.isSufficientlyReplicated(true)) {
      LOG.info("The container {} with replicas {} will be sufficiently " +
          "replicated after pending replicas are created",
          container.getContainerID(), replicaCount.getReplicas());
      return 0;
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
    int commandsSent = 0;
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
        commandsSent += processMissingIndexes(replicaCount, sources,
            availableSourceNodes, excludedNodes);
        commandsSent += processDecommissioningIndexes(replicaCount, sources,
            availableSourceNodes, excludedNodes);
        commandsSent +=  processMaintenanceOnlyIndexes(replicaCount, sources,
            excludedNodes);
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
        if (commandsSent > 0) {
          LOG.debug("Some commands have already been created, so returning " +
              "with them only");
          return commandsSent;
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
              " container commands for container {}.",
          id, ex);
      throw ex;
    }
    if (commandsSent == 0) {
      LOG.warn("Container {} is under replicated, but no commands were " +
          "created to correct it", id);
    }
    return commandsSent;
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
        .map(r -> {
          try {
            return Pair.of(r,
                replicationManager.getNodeStatus(r.getDatanodeDetails()));
          } catch (NodeNotFoundException e) {
            throw new IllegalStateException("Unable to find NodeStatus for "
                + r.getDatanodeDetails(), e);
          }
        })
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
   * @return number of commands sent
   * @throws IOException
   */
  private int processMissingIndexes(
      ECContainerReplicaCount replicaCount, Map<Integer,
      Pair<ContainerReplica, NodeStatus>> sources,
      List<DatanodeDetails> availableSourceNodes,
      List<DatanodeDetails> excludedNodes) throws IOException {
    ContainerInfo container = replicaCount.getContainer();
    ECReplicationConfig repConfig =
        (ECReplicationConfig)container.getReplicationConfig();
    List<Integer> missingIndexes = replicaCount.unavailableIndexes(true);
    if (missingIndexes.size() == 0) {
      return 0;
    }

    int commandsSent = 0;
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
        replicationManager.sendDatanodeCommand(reconstructionCommand,
            container, selectedDatanodes.get(0));
        commandsSent++;
      }
    } else {
      LOG.warn("Cannot proceed for EC container reconstruction for {}, due"
              + " to insufficient source replicas found. Number of source "
              + "replicas needed: {}. Number of available source replicas are:"
              + " {}. Available sources are: {}", container.containerID(),
          repConfig.getData(), sources.size(), sources);
    }
    return commandsSent;
  }

  /**
   * Processes replicas that are in maintenance nodes and should need
   * additional copies.
   * @return number of commands sent
   * @throws IOException
   */
  private int processDecommissioningIndexes(
      ECContainerReplicaCount replicaCount,
      Map<Integer, Pair<ContainerReplica, NodeStatus>> sources,
      List<DatanodeDetails> availableSourceNodes,
      List<DatanodeDetails> excludedNodes) throws IOException {
    ContainerInfo container = replicaCount.getContainer();
    Set<Integer> decomIndexes = replicaCount.decommissioningOnlyIndexes(true);
    int commandsSent = 0;
    if (decomIndexes.size() > 0) {
      final List<DatanodeDetails> selectedDatanodes =
          getTargetDatanodes(excludedNodes, container, decomIndexes.size());
      if (validatePlacement(availableSourceNodes, selectedDatanodes)) {
        excludedNodes.addAll(selectedDatanodes);
        Iterator<DatanodeDetails> iterator = selectedDatanodes.iterator();
        // In this case we need to do one to one copy.
        for (Integer decomIndex : decomIndexes) {
          Pair<ContainerReplica, NodeStatus> source = sources.get(decomIndex);
          if (source == null) {
            LOG.warn("Cannot find source replica for decommissioning index " +
                    "{} in container {}", decomIndex, container.containerID());
            continue;
          }
          ContainerReplica sourceReplica = source.getLeft();
          if (!iterator.hasNext()) {
            LOG.warn("Couldn't find enough targets. Available source"
                + " nodes: {}, the target nodes: {}, excluded nodes: {}"
                + "  and the decommission indexes: {}",
                sources.values().stream()
                    .map(Pair::getLeft).collect(Collectors.toSet()),
                selectedDatanodes, excludedNodes, decomIndexes);
            break;
          }
          createReplicateCommand(container, iterator, sourceReplica);
          commandsSent++;
        }
      }
    }
    return commandsSent;
  }

  /**
   * Processes replicas that are in maintenance nodes and should need
   * additional copies.
   * @param replicaCount
   * @param sources Map of Replica Index to a pair of ContainerReplica and
   *                NodeStatus. This is the list of available replicas.
   * @param excludedNodes nodes that should not be targets for new copies
   * @@return number of commands sent
   * @throws IOException
   */
  private int processMaintenanceOnlyIndexes(
      ECContainerReplicaCount replicaCount,
      Map<Integer, Pair<ContainerReplica, NodeStatus>> sources,
      List<DatanodeDetails> excludedNodes) throws IOException {
    Set<Integer> maintIndexes = replicaCount.maintenanceOnlyIndexes(true);
    if (maintIndexes.isEmpty()) {
      return 0;
    }

    ContainerInfo container = replicaCount.getContainer();
    // this many maintenance replicas need another copy
    int additionalMaintenanceCopiesNeeded =
        replicaCount.additionalMaintenanceCopiesNeeded(true);
    if (additionalMaintenanceCopiesNeeded == 0) {
      return 0;
    }
    List<DatanodeDetails> targets = getTargetDatanodes(excludedNodes, container,
        additionalMaintenanceCopiesNeeded);
    excludedNodes.addAll(targets);

    Iterator<DatanodeDetails> iterator = targets.iterator();
    int commandsSent = 0;
    // copy replica from source maintenance DN to a target DN

    for (Integer maintIndex : maintIndexes) {
      if (additionalMaintenanceCopiesNeeded <= 0) {
        break;
      }
      Pair<ContainerReplica, NodeStatus> source = sources.get(maintIndex);
      if (source == null) {
        LOG.warn("Cannot find source replica for maintenance index " +
            "{} in container {}", maintIndex, container.containerID());
        continue;
      }
      ContainerReplica sourceReplica = source.getLeft();
      if (!iterator.hasNext()) {
        LOG.warn("Couldn't find enough targets. Available source"
                + " nodes: {}, target nodes: {}, excluded nodes: {} and"
                + " maintenance indexes: {}",
            sources.values().stream()
                .map(Pair::getLeft).collect(Collectors.toSet()),
            targets, excludedNodes, maintIndexes);
        break;
      }
      createReplicateCommand(container, iterator, sourceReplica);
      commandsSent++;
      additionalMaintenanceCopiesNeeded -= 1;
    }
    return commandsSent;
  }

  private void createReplicateCommand(
      ContainerInfo container, Iterator<DatanodeDetails> iterator,
      ContainerReplica replica)
      throws AllSourcesOverloadedException, NotLeaderException {
    final boolean push = replicationManager.getConfig().isPush();
    DatanodeDetails source = replica.getDatanodeDetails();
    DatanodeDetails target = iterator.next();
    final long containerID = container.getContainerID();

    if (push) {
      replicationManager.sendThrottledReplicationCommand(
          container, Collections.singletonList(source), target,
          replica.getReplicaIndex());
    } else {
      ReplicateContainerCommand replicateCommand =
          ReplicateContainerCommand.fromSources(containerID,
          ImmutableList.of(source));
      // For EC containers, we need to track the replica index which is
      // to be replicated, so add it to the command.
      replicateCommand.setReplicaIndex(replica.getReplicaIndex());
      replicationManager.sendDatanodeCommand(replicateCommand, container,
          target);
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
