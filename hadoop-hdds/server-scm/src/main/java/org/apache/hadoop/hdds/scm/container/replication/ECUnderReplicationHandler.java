/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.replication;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.InsufficientDatanodesException;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the EC Under replication processing and forming the respective SCM
 * commands.
 */
public class ECUnderReplicationHandler implements UnhealthyReplicationHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(ECUnderReplicationHandler.class);
  private final PlacementPolicy containerPlacement;
  private final long currentContainerSize;
  private final ReplicationManager replicationManager;
  private final ReplicationManagerMetrics metrics;

  ECUnderReplicationHandler(final PlacementPolicy containerPlacement,
      final ConfigurationSource conf, ReplicationManager replicationManager) {
    this.containerPlacement = containerPlacement;
    this.currentContainerSize = (long) conf
        .getStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    this.replicationManager = replicationManager;
    this.metrics = replicationManager.getMetrics();
  }

  private ContainerPlacementStatus validatePlacement(
      ContainerInfo container,
      List<DatanodeDetails> replicaNodes,
      List<DatanodeDetails> selectedNodes) {
    LOG.trace("Validating placement policy for container {}. Available " +
            "replica nodes: {}. Selected target nodes: {}.",
        container.containerID(), replicaNodes, selectedNodes);
    List<DatanodeDetails> nodes = new ArrayList<>(replicaNodes);
    nodes.addAll(selectedNodes);
    return containerPlacement.validateContainerPlacement(nodes, nodes.size());
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
    LOG.debug("Handling under-replicated container: {}", container);

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
    LOG.debug("Under-replicated container {} currently has replicas: {}.",
        container.containerID(), replicas);

    ReplicationManagerUtil.ExcludedAndUsedNodes excludedAndUsedNodes =
        ReplicationManagerUtil.getExcludedAndUsedNodes(container,
            new ArrayList<>(replicas), Collections.emptySet(), pendingOps,
            replicationManager);
    List<DatanodeDetails> excludedNodes
        = excludedAndUsedNodes.getExcludedNodes();
    List<DatanodeDetails> usedNodes
        = excludedAndUsedNodes.getUsedNodes();

    final ContainerID id = container.containerID();
    int commandsSent = 0;
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
      IOException firstException = null;
      try {
        commandsSent += processMissingIndexes(replicaCount, sources,
            availableSourceNodes, excludedNodes, usedNodes);
      } catch (InsufficientDatanodesException
          | CommandTargetOverloadedException  e) {
        firstException = e;
      }

      excludedNodes.addAll(replicationManager.getExcludedNodes());
      try {
        commandsSent += processDecommissioningIndexes(replicaCount, sources,
            availableSourceNodes, excludedNodes, usedNodes);
      } catch (InsufficientDatanodesException
          | CommandTargetOverloadedException e) {
        if (firstException == null) {
          firstException = e;
        }
      }
      try {
        commandsSent += processMaintenanceOnlyIndexes(replicaCount, sources,
            excludedNodes, usedNodes);
      } catch (InsufficientDatanodesException
          | CommandTargetOverloadedException e) {
        if (firstException == null) {
          firstException = e;
        }
      }
      if (firstException != null) {
        // We had partial success through some of the steps, so just throw the
        // first exception we got. This will cause the container to be
        // re-queued and try again later.
        throw firstException;
      }
    } catch (SCMException e) {
      SCMException.ResultCodes code = e.getResult();
      if (code != SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE) {
        throw e;
      }
      // If we get here, we got an exception indicating the placement policy
      // was not able to find ANY nodes to satisfy the replication at one of
      // the processing stages (missing index, decom or maint). It is
      // possible that some commands were sent to partially fix the
      // replication, but a further run will be needed to fix the rest.
      // On a small cluster, it is possible that over replication could stop
      // nodes getting selected, so to check if that is the case, we run
      // the over rep handler, which may free some nodes for the next run.
      if (replicaCount.isOverReplicated()) {
        LOG.debug("Container {} is both under and over replicated. Cannot " +
            "find enough target nodes, so handing off to the " +
            "OverReplication handler", container);
        replicationManager.processOverReplicatedContainer(result);
      }

      /* If we get here, the scenario is:
      1. Under replicated.
      2. Not over replicated.
      3. Placement Policy not able to find enough targets.
      Check if there are some UNHEALTHY replicas. In a small cluster, these
      UNHEALTHY replicas could block DNs that could otherwise be targets
      for new EC replicas. Deleting an UNHEALTHY replica can make its host DN
      available as a target.
      */
      checkAndRemoveUnhealthyReplica(replicaCount, deletionInFlight);
      // As we want to re-queue and try again later, we just re-throw
      throw e;
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
            .getState() == State.CLOSED)
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

  /**
   * Processes replicas that are on in-service nodes and should need
   * additional copies.
   * @return number of commands sent
   */
  private int processMissingIndexes(
      ECContainerReplicaCount replicaCount, Map<Integer,
      Pair<ContainerReplica, NodeStatus>> sources,
      List<DatanodeDetails> availableSourceNodes,
      List<DatanodeDetails> excludedNodes,
      List<DatanodeDetails> usedNodes) throws IOException {
    ContainerInfo container = replicaCount.getContainer();
    ECReplicationConfig repConfig =
        (ECReplicationConfig)container.getReplicationConfig();
    List<Integer> missingIndexes = replicaCount.unavailableIndexes(true);
    LOG.debug("Processing missing indexes {} for container {}.", missingIndexes,
        container.containerID());
    final int expectedTargetCount = missingIndexes.size();
    boolean recoveryIsCritical = expectedTargetCount == repConfig.getParity();
    if (expectedTargetCount == 0) {
      return 0;
    }

    int commandsSent = 0;
    if (sources.size() >= repConfig.getData()) {
      Set<DatanodeDetails> excludedDueToLoad =
          replicationManager.getExcludedNodes();
      final boolean hasOverloaded = !excludedDueToLoad.isEmpty();
      final List<DatanodeDetails> excludedOrOverloadedNodes = hasOverloaded
          ? new ArrayList<>(ImmutableSet.<DatanodeDetails>builder()
              .addAll(excludedNodes)
              .addAll(excludedDueToLoad)
              .build())
          : excludedNodes;

      // placement with overloaded nodes excluded
      final List<DatanodeDetails> selectedDatanodes = getTargetDatanodes(
          container, expectedTargetCount, usedNodes, excludedOrOverloadedNodes
      );
      final int targetCount = selectedDatanodes.size();

      if (hasOverloaded &&
          // selection allows partial recovery
          0 < targetCount && targetCount < expectedTargetCount &&
          // recovery is not yet critical
          !recoveryIsCritical) {

        // check if placement exists when overloaded nodes are not excluded
        final List<DatanodeDetails> targetsMaybeOverloaded = getTargetDatanodes(
            container, expectedTargetCount, usedNodes, excludedNodes);

        if (targetsMaybeOverloaded.size() == expectedTargetCount) {
          final int overloadedCount = expectedTargetCount - targetCount;
          LOG.info("Deferring reconstruction of container {}, which requires {}"
                  + " target nodes to be fully reconstructed, but {} selected"
                  + " nodes are currently overloaded.",
              container.getContainerID(), expectedTargetCount, overloadedCount);
          metrics.incrECPartialReconstructionSkippedTotal();
          throw new InsufficientDatanodesException(expectedTargetCount,
              targetCount);
        }
      }

      // If we got less targets than missing indexes, we need to prune the
      // missing index list so it only tries to recover the number of indexes
      // we have targets for.
      if (targetCount < expectedTargetCount) {
        missingIndexes.subList(targetCount, expectedTargetCount).clear();
      }

      ContainerPlacementStatus placementStatusWithSelectedTargets =
          validatePlacement(container, availableSourceNodes, selectedDatanodes);
      if (!placementStatusWithSelectedTargets.isPolicySatisfied()) {
        LOG.debug("Target nodes + existing nodes for EC container {}" +
                " will not satisfy placement policy {}. Reason: {}. Selected" +
                " nodes: {}. Available source nodes: {}. Resuming " +
                "reconstruction regardless.",
            container.containerID(), containerPlacement.getClass().getName(),
            placementStatusWithSelectedTargets.misReplicatedReason(),
            selectedDatanodes, availableSourceNodes);
      }
      if (0 < targetCount) {
        usedNodes.addAll(selectedDatanodes);
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
                integers2ByteString(missingIndexes),
                repConfig);
        // This can throw a CommandTargetOverloadedException, but there is no
        // point in retrying here. The sources we picked already have the
        // overloaded nodes excluded, so we should not get an overloaded
        // exception, but it could happen due to other threads adding work to
        // the DNs. If it happens here, we just let the exception bubble up.
        replicationManager.sendThrottledReconstructionCommand(
            container, reconstructionCommand);
        for (int i = 0; i < missingIndexes.size(); i++) {
          adjustPendingOps(
              replicaCount, selectedDatanodes.get(i), missingIndexes.get(i));
        }
        commandsSent++;
      }
      if (targetCount != expectedTargetCount) {
        LOG.debug("Insufficient nodes were returned from the placement policy" +
            " to fully reconstruct container {}. Requested {} received {}",
            container.getContainerID(), expectedTargetCount, targetCount);
        if (hasOverloaded && recoveryIsCritical) {
          metrics.incrECPartialReconstructionCriticalTotal();
        } else {
          metrics.incrEcPartialReconstructionNoneOverloadedTotal();
        }
        throw new InsufficientDatanodesException(expectedTargetCount,
            targetCount);
      }
    } else {
      LOG.warn("Cannot proceed for EC container reconstruction for {}, due"
              + " to insufficient source replicas found. Number of source "
              + "replicas needed: {}. Number of available source replicas are:"
              + " {}. Available sources are: {}", container.containerID(),
          repConfig.getData(), sources.size(), sources);
    }
    LOG.trace("Sent {} commands for container {}.", commandsSent,
        container.containerID());
    return commandsSent;
  }

  private List<DatanodeDetails> getTargetDatanodes(
      ContainerInfo container, int requiredNodes,
      List<DatanodeDetails> usedNodes,
      List<DatanodeDetails> excludedNodes
  ) throws SCMException {
    return ReplicationManagerUtil.getTargetDatanodes(
        containerPlacement, requiredNodes,
        usedNodes, excludedNodes,
        currentContainerSize, container);
  }

  /**
   * Processes replicas that are in decommissioning nodes and should need
   * additional copies.
   * @return number of commands sent
   */
  private int processDecommissioningIndexes(
      ECContainerReplicaCount replicaCount,
      Map<Integer, Pair<ContainerReplica, NodeStatus>> sources,
      List<DatanodeDetails> availableSourceNodes,
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> usedNodes)
      throws IOException {
    ContainerInfo container = replicaCount.getContainer();
    Set<Integer> decomIndexes = replicaCount.decommissioningOnlyIndexes(true);
    int commandsSent = 0;
    if (!decomIndexes.isEmpty()) {
      LOG.debug("Processing decommissioning indexes {} for container {}.",
          decomIndexes, container.containerID());
      final List<DatanodeDetails> selectedDatanodes = getTargetDatanodes(
          container, decomIndexes.size(), usedNodes, excludedNodes);

      ContainerPlacementStatus placementStatusWithSelectedTargets =
          validatePlacement(container, availableSourceNodes, selectedDatanodes);
      if (!placementStatusWithSelectedTargets.isPolicySatisfied()) {
        LOG.debug("Target nodes + existing nodes for EC container {}" +
                " will not satisfy placement policy {}. Reason: {}. Selected" +
                " nodes: {}. Available source nodes: {}. Resuming recovery " +
                "regardless.",
            container.containerID(), containerPlacement.getClass().getName(),
            placementStatusWithSelectedTargets.misReplicatedReason(),
            selectedDatanodes, availableSourceNodes);
      }

      usedNodes.addAll(selectedDatanodes);
      Iterator<DatanodeDetails> iterator = selectedDatanodes.iterator();
      // In this case we need to do one to one copy.
      CommandTargetOverloadedException overloadedException = null;
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
              + " nodes: {}, the target nodes: {}, excluded nodes: {},"
              + " usedNodes: {}, and the decommission indexes: {}",
              sources.values().stream()
                  .map(Pair::getLeft).collect(Collectors.toSet()),
              selectedDatanodes, excludedNodes, usedNodes, decomIndexes);
          break;
        }
        try {
          createReplicateCommand(
              container, iterator, sourceReplica, replicaCount);
          commandsSent++;
        } catch (CommandTargetOverloadedException e) {
          LOG.debug("Unable to send Replicate command for container {}" +
              " index {} because the source node {} is overloaded.",
              container.getContainerID(), sourceReplica.getReplicaIndex(),
              sourceReplica.getDatanodeDetails());
          overloadedException = e;
        }
      }
      if (overloadedException != null) {
        throw overloadedException;
      }

      if (selectedDatanodes.size() != decomIndexes.size()) {
        LOG.debug("Insufficient nodes were returned from the placement policy" +
            " to fully replicate the decommission indexes for container {}." +
            " Requested {} received {}", container.getContainerID(),
            decomIndexes.size(), selectedDatanodes.size());
        metrics.incrEcPartialReplicationForOutOfServiceReplicasTotal();
        throw new InsufficientDatanodesException(decomIndexes.size(),
            selectedDatanodes.size());
      }
    }
    LOG.trace("Sent {} commands for container {}.", commandsSent,
        container.containerID());
    return commandsSent;
  }

  /**
   * Processes replicas that are in maintenance nodes and should need
   * additional copies.
   * @param sources Map of Replica Index to a pair of ContainerReplica and
   *                NodeStatus. This is the list of available replicas.
   * @param excludedNodes nodes that should not be targets for new copies
   * @return number of commands sent
   */
  private int processMaintenanceOnlyIndexes(
      ECContainerReplicaCount replicaCount,
      Map<Integer, Pair<ContainerReplica, NodeStatus>> sources,
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> usedNodes)
      throws IOException {
    Set<Integer> maintIndexes = replicaCount.maintenanceOnlyIndexes(true);
    if (maintIndexes.isEmpty()) {
      return 0;
    }

    ContainerInfo container = replicaCount.getContainer();
    LOG.debug("Processing maintenance indexes {} for container {}.",
        maintIndexes, container.containerID());
    // this many maintenance replicas need another copy
    int additionalMaintenanceCopiesNeeded =
        replicaCount.additionalMaintenanceCopiesNeeded(true);
    if (additionalMaintenanceCopiesNeeded == 0) {
      return 0;
    }
    LOG.debug("Number of maintenance replicas of container {} that need " +
            "additional copies: {}.", container.containerID(),
        additionalMaintenanceCopiesNeeded);
    List<DatanodeDetails> targets = getTargetDatanodes(
        container, maintIndexes.size(), usedNodes, excludedNodes
    );
    usedNodes.addAll(targets);

    Iterator<DatanodeDetails> iterator = targets.iterator();
    int commandsSent = 0;
    // copy replica from source maintenance DN to a target DN

    CommandTargetOverloadedException overloadedException = null;
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
                + " nodes: {}, target nodes: {}, excluded nodes: {},"
                + " usedNodes: {} and"
                + " maintenance indexes: {}",
            sources.values().stream()
                .map(Pair::getLeft).collect(Collectors.toSet()),
            targets, excludedNodes, usedNodes, maintIndexes);
        break;
      }
      try {
        createReplicateCommand(
            container, iterator, sourceReplica, replicaCount);
        commandsSent++;
        additionalMaintenanceCopiesNeeded -= 1;
      } catch (CommandTargetOverloadedException e) {
        LOG.debug("Unable to send Replicate command for container {}" +
            " index {} because the source node {} is overloaded.",
            container.getContainerID(), sourceReplica.getReplicaIndex(),
            sourceReplica.getDatanodeDetails());
        overloadedException = e;
      }
    }
    if (overloadedException != null) {
      throw overloadedException;
    }
    if (targets.size() != maintIndexes.size()) {
      LOG.debug("Insufficient nodes were returned from the placement policy" +
              " to fully replicate the maintenance indexes for container {}." +
              " Requested {} received {}", container.getContainerID(),
          maintIndexes.size(), targets.size());
      metrics.incrEcPartialReplicationForOutOfServiceReplicasTotal();
      throw new InsufficientDatanodesException(maintIndexes.size(),
          targets.size());
    }
    LOG.trace("Sent {} commands for container {}.", commandsSent,
        container.containerID());
    return commandsSent;
  }

  private void createReplicateCommand(
      ContainerInfo container, Iterator<DatanodeDetails> iterator,
      ContainerReplica replica, ECContainerReplicaCount replicaCount)
      throws CommandTargetOverloadedException, NotLeaderException {
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
    adjustPendingOps(replicaCount, target, replica.getReplicaIndex());
  }

  private void adjustPendingOps(ECContainerReplicaCount replicaCount,
                                DatanodeDetails target, int replicaIndex) {
    replicaCount.addPendingOp(new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.ADD, target, replicaIndex, null,
        Long.MAX_VALUE, 0));
  }

  static ByteString integers2ByteString(List<Integer> src) {
    byte[] dst = new byte[src.size()];

    for (int i = 0; i < src.size(); i++) {
      dst[i] = src.get(i).byteValue();
    }

    return dst.length > 0 ? UnsafeByteOperations.unsafeWrap(dst)
        : ByteString.EMPTY;
  }

  /**
   * Deletes one UNHEALTHY replica so that its host datanode becomes available
   * to host a healthy replica. This can be helpful if reconstruction or
   * replication is blocked because DNs that follow the placement policy are
   * not available as targets.
   * @param replicaCount ECContainerReplicaCount object of this container
   * @param deletionInFlight pending deletes of this container's replicas
   */
  private void checkAndRemoveUnhealthyReplica(
      ECContainerReplicaCount replicaCount,
      List<DatanodeDetails> deletionInFlight) {
    LOG.debug("Finding an UNHEALTHY replica of container {} to delete so its " +
        "host datanode can be available for replication/reconstruction.",
        replicaCount.getContainer());
    if (!deletionInFlight.isEmpty()) {
      LOG.debug("There are {} pending deletes. Completing them could " +
          "free up nodes to fix under replication. Not deleting UNHEALTHY" +
          " replicas in this iteration.", deletionInFlight.size());
      return;
    }

    ContainerInfo container = replicaCount.getContainer();
    // ensure that the container is recoverable
    if (replicaCount.isUnrecoverable()) {
      LOG.warn("Cannot recover container {}.", container);
      return;
    }

    // don't consider replicas that aren't on IN_SERVICE and HEALTHY DNs
    Set<Integer> closedReplicas = new HashSet<>();
    Set<ContainerReplica> unhealthyReplicas = new HashSet<>();
    for (ContainerReplica replica : replicaCount.getReplicas()) {
      try {
        NodeStatus nodeStatus =
            replicationManager.getNodeStatus(replica.getDatanodeDetails());
        if (!nodeStatus.isHealthy() || !nodeStatus.isInService()) {
          continue;
        }
      } catch (NodeNotFoundException e) {
        LOG.debug("Skipping replica {} when trying to unblock under " +
            "replication handling.", replica, e);
        continue;
      }

      if (replica.getState().equals(State.CLOSED)) {
        // collect CLOSED replicas for later
        closedReplicas.add(replica.getReplicaIndex());
      } else if (replica.getState().equals(State.UNHEALTHY)) {
        unhealthyReplicas.add(replica);
      }
    }

    if (unhealthyReplicas.isEmpty()) {
      LOG.debug("Container {} does not have any UNHEALTHY replicas.",
          container.containerID());
      return;
    }

    /*
    If an index has both an UNHEALTHY and CLOSED replica, prefer deleting the
    UNHEALTHY replica of this index and return. Otherwise, delete any UNHEALTHY
    replica.
    */
    for (ContainerReplica unhealthyReplica : unhealthyReplicas) {
      if (closedReplicas.contains(unhealthyReplica.getReplicaIndex())) {
        try {
          replicationManager.sendThrottledDeleteCommand(
              replicaCount.getContainer(), unhealthyReplica.getReplicaIndex(),
              unhealthyReplica.getDatanodeDetails(), true);
          return;
        } catch (NotLeaderException | CommandTargetOverloadedException e) {
          LOG.debug("Skipping sending a delete command for replica {} to " +
                  "Datanode {}.", unhealthyReplica,
              unhealthyReplica.getDatanodeDetails());
        }
      }
    }

    /*
     We didn't delete in the earlier loop - just delete any UNHEALTHY
     replica now.
    */
    for (ContainerReplica unhealthyReplica : unhealthyReplicas) {
      try {
        replicationManager.sendThrottledDeleteCommand(
            replicaCount.getContainer(), unhealthyReplica.getReplicaIndex(),
            unhealthyReplica.getDatanodeDetails(), true);
        return;
      } catch (NotLeaderException | CommandTargetOverloadedException e) {
        LOG.debug("Skipping sending a delete command for replica {} to " +
                "Datanode {}.", unhealthyReplica,
            unhealthyReplica.getDatanodeDetails());
      }
    }
  }

}
