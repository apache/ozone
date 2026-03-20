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
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Proto2Utils;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
  private final ReplicationManagerMetrics metrics;
  private final java.time.Clock clock;

  ECUnderReplicationHandler(final PlacementPolicy containerPlacement,
      final ConfigurationSource conf, ReplicationManager replicationManager) {
    this.containerPlacement = containerPlacement;
    this.currentContainerSize = (long) conf
        .getStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    this.replicationManager = replicationManager;
    this.metrics = replicationManager.getMetrics();
    this.clock = replicationManager.getClock();
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
    return containerPlacement.validateContainerPlacement(nodes,
        container.getReplicationConfig().getRequiredNodes());
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

    // Identify decommissioning indexes that should be reconstructed
    // instead of replicated based on source node load.
    Set<Integer> decommissionIndexes =
        replicaCount.decommissioningOnlyIndexes(true);
    List<Integer> indexesToReconstruct = new ArrayList<>(
        replicaCount.unavailableIndexes(true));
    List<Integer> indexesToReplicate = new ArrayList<>();

    int reconstructionThreshold = replicationManager.getConfig()
        .getDecommissionEcReconstructionThreshold();
    boolean reconstructionEnabled = replicationManager.getConfig()
        .isDecommissionEcReconstructionEnabled();

    for (Integer index : decommissionIndexes) {
      Pair<ContainerReplica, NodeStatus> source = sources.get(index);
      if (source != null) {
        try {
          int queuedCount = replicationManager
              .getQueuedReplicationCount(source.getLeft().getDatanodeDetails());
          if (reconstructionEnabled && queuedCount >= reconstructionThreshold) {
            LOG.debug("Source node {} for container {} index {} is overloaded " +
                "(queued: {} >= threshold: {}). Switching to reconstruction.",
                source.getLeft().getDatanodeDetails(), id, index,
                queuedCount, reconstructionThreshold);
            indexesToReconstruct.add(index);
          } else {
            indexesToReplicate.add(index);
          }
        } catch (NodeNotFoundException e) {
          LOG.warn("Source node {} not found.",
              source.getLeft().getDatanodeDetails());
          indexesToReplicate.add(index);
        }
      }
    }

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
        commandsSent += processIndexes(replicaCount, sources,
            availableSourceNodes, excludedNodes, usedNodes,
            indexesToReconstruct);
      } catch (InsufficientDatanodesException
          | CommandTargetOverloadedException  e) {
        firstException = e;
      }

      excludedNodes.addAll(replicationManager.getExcludedNodes());
      try {
        commandsSent += processReplication(replicaCount, sources,
            availableSourceNodes, excludedNodes, usedNodes,
            indexesToReplicate);
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
      if (replicaCount.isOverReplicated()) {
        replicationManager.processOverReplicatedContainer(result);
      }
      checkAndRemoveUnhealthyReplica(replicaCount, deletionInFlight);
      throw e;
    }
    return commandsSent;
  }

  private Map<Integer, Pair<ContainerReplica, NodeStatus>> filterSources(
      Set<ContainerReplica> replicas, List<DatanodeDetails> deletionInFlight) {
    return replicas.stream().filter(r -> r
            .getState() == State.CLOSED)
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
   * Processes replicas that should be reconstructed.
   * @return number of commands sent
   */
  private int processIndexes(
      ECContainerReplicaCount replicaCount, Map<Integer,
      Pair<ContainerReplica, NodeStatus>> sources,
      List<DatanodeDetails> availableSourceNodes,
      List<DatanodeDetails> excludedNodes,
      List<DatanodeDetails> usedNodes,
      List<Integer> missingIndexes) throws IOException {
    ContainerInfo container = replicaCount.getContainer();
    ECReplicationConfig repConfig =
        (ECReplicationConfig)container.getReplicationConfig();
    LOG.debug("Processing indexes {} for reconstruction of container {}.",
        missingIndexes, container.containerID());
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

      final List<DatanodeDetails> selectedDatanodes = getTargetDatanodes(
          container, expectedTargetCount, usedNodes, excludedOrOverloadedNodes
      );
      final int targetCount = selectedDatanodes.size();

      if (hasOverloaded &&
          0 < targetCount && targetCount < expectedTargetCount &&
          !recoveryIsCritical) {
        final List<DatanodeDetails> targetsMaybeOverloaded = getTargetDatanodes(
            container, expectedTargetCount, usedNodes, excludedNodes);

        if (targetsMaybeOverloaded.size() == expectedTargetCount) {
          metrics.incrECPartialReconstructionSkippedTotal();
          throw new InsufficientDatanodesException(expectedTargetCount,
              targetCount);
        }
      }

      if (targetCount < expectedTargetCount) {
        missingIndexes.subList(targetCount, expectedTargetCount).clear();
      }

      ContainerPlacementStatus placementStatusWithSelectedTargets =
          validatePlacement(container, availableSourceNodes, selectedDatanodes);
      if (!placementStatusWithSelectedTargets.isPolicySatisfied()) {
        LOG.debug("Target nodes + existing nodes for EC container {} " +
                "will not satisfy placement policy {}. Reason: {}. " +
                "Selected nodes: {}. Available source nodes: {}. " +
                "Resuming recovery regardless.",
            container.containerID(), containerPlacement.getClass().getName(),
            placementStatusWithSelectedTargets.misReplicatedReason(),
            selectedDatanodes, availableSourceNodes);
      }

      if (0 < targetCount) {
        usedNodes.addAll(selectedDatanodes);
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

        // If any of the missing indexes are decommissioning, we should
        // track one of them as the decommission source so we can throttle.
        DatanodeDetails decommissionSource = null;
        if (!replicationManager.isDecommissionThrottled()) {
          Set<Integer> decomIndexes = replicaCount.decommissioningOnlyIndexes(true);
          for (Integer index : missingIndexes) {
            if (decomIndexes.contains(index)) {
              Pair<ContainerReplica, NodeStatus> src = sources.get(index);
              if (src != null) {
                decommissionSource = src.getLeft().getDatanodeDetails();
                break;
              }
            }
          }
        }

        final ReconstructECContainersCommand reconstructionCommand =
            new ReconstructECContainersCommand(container.getContainerID(),
                sourceDatanodesWithIndex, selectedDatanodes,
                integers2ByteString(missingIndexes),
                repConfig);
        replicationManager.sendThrottledReconstructionCommand(
            container, reconstructionCommand, decommissionSource);
        for (int i = 0; i < missingIndexes.size(); i++) {
          adjustPendingOps(
              replicaCount, selectedDatanodes.get(i), missingIndexes.get(i),
              decommissionSource);
        }
        commandsSent++;
      }
      if (targetCount != expectedTargetCount) {
        if (hasOverloaded && recoveryIsCritical) {
          metrics.incrECPartialReconstructionCriticalTotal();
        } else {
          metrics.incrEcPartialReconstructionNoneOverloadedTotal();
        }
        throw new InsufficientDatanodesException(expectedTargetCount,
            targetCount);
      }
    }
    return commandsSent;
  }

  /**
   * Processes replicas that should be replicated.
   * @return number of commands sent
   */
  private int processReplication(
      ECContainerReplicaCount replicaCount,
      Map<Integer, Pair<ContainerReplica, NodeStatus>> sources,
      List<DatanodeDetails> availableSourceNodes,
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> usedNodes,
      List<Integer> indexesToReplicate)
      throws IOException {
    ContainerInfo container = replicaCount.getContainer();
    int commandsSent = 0;
    if (!indexesToReplicate.isEmpty()) {
      LOG.debug("Processing replication indexes {} for container {}.",
          indexesToReplicate, container.containerID());
      final List<DatanodeDetails> selectedDatanodes = getTargetDatanodes(
          container, indexesToReplicate.size(), usedNodes, excludedNodes);

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
      Set<Integer> decommissionIndexes =
          replicaCount.decommissioningOnlyIndexes(true);
      for (Integer index : indexesToReplicate) {
        Pair<ContainerReplica, NodeStatus> source = sources.get(index);
        if (source == null) {
          LOG.warn("Cannot find source replica for index " +
                  "{} in container {}", index, container.containerID());
          continue;
        }
        ContainerReplica sourceReplica = source.getLeft();
        if (!iterator.hasNext()) {
          break;
        }

        DatanodeDetails decommissionSource = null;
        if (decommissionIndexes.contains(index)) {
          if (replicationManager.isDecommissionThrottled()) {
            LOG.debug("Global decommissioning concurrency limit reached. " +
                "Skipping replication of index {} for container {}.",
                index, container.getContainerID());
            continue;
          }
          decommissionSource = sourceReplica.getDatanodeDetails();
        }

        try {
          createReplicateCommand(
              container, iterator, sourceReplica, replicaCount,
              decommissionSource);
          commandsSent++;
        } catch (CommandTargetOverloadedException e) {
          overloadedException = e;
        }
      }
      if (overloadedException != null) {
        throw overloadedException;
      }

      if (selectedDatanodes.size() != indexesToReplicate.size()) {
        metrics.incrEcPartialReplicationForOutOfServiceReplicasTotal();
        throw new InsufficientDatanodesException(indexesToReplicate.size(),
            selectedDatanodes.size());
      }
    }
    return commandsSent;
  }

  /**
   * Processes replicas that are in maintenance nodes and should need
   * additional copies.
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
    int additionalMaintenanceCopiesNeeded =
        replicaCount.additionalMaintenanceCopiesNeeded(true);
    if (additionalMaintenanceCopiesNeeded == 0) {
      return 0;
    }
    List<DatanodeDetails> targets = getTargetDatanodes(
        container, maintIndexes.size(), usedNodes, excludedNodes
    );
    usedNodes.addAll(targets);

    Iterator<DatanodeDetails> iterator = targets.iterator();
    int commandsSent = 0;

    CommandTargetOverloadedException overloadedException = null;
    for (Integer maintIndex : maintIndexes) {
      if (additionalMaintenanceCopiesNeeded <= 0) {
        break;
      }
      Pair<ContainerReplica, NodeStatus> source = sources.get(maintIndex);
      if (source == null) {
        continue;
      }
      ContainerReplica sourceReplica = source.getLeft();
      if (!iterator.hasNext()) {
        break;
      }
      try {
        createReplicateCommand(
            container, iterator, sourceReplica, replicaCount, null);
        commandsSent++;
        additionalMaintenanceCopiesNeeded -= 1;
      } catch (CommandTargetOverloadedException e) {
        overloadedException = e;
      }
    }
    if (overloadedException != null) {
      throw overloadedException;
    }
    if (targets.size() != maintIndexes.size()) {
      metrics.incrEcPartialReplicationForOutOfServiceReplicasTotal();
      throw new InsufficientDatanodesException(maintIndexes.size(),
          targets.size());
    }

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

  private void createReplicateCommand(
      ContainerInfo container, Iterator<DatanodeDetails> iterator,
      ContainerReplica replica, ECContainerReplicaCount replicaCount,
      DatanodeDetails decommissionSource)
      throws CommandTargetOverloadedException, NotLeaderException {
    final boolean push = replicationManager.getConfig().isPush();
    DatanodeDetails source = replica.getDatanodeDetails();
    DatanodeDetails target = iterator.next();
    final long containerID = container.getContainerID();

    if (push) {
      replicationManager.sendThrottledReplicationCommand(
          container, Collections.singletonList(source), target,
          replica.getReplicaIndex(), decommissionSource);
    } else {
      ReplicateContainerCommand replicateCommand =
          ReplicateContainerCommand.fromSources(containerID,
          ImmutableList.of(source));
      replicateCommand.setReplicaIndex(replica.getReplicaIndex());
      replicationManager.sendDatanodeCommand(replicateCommand, container,
          target, clock.millis() + replicationManager.getConfig().getEventTimeout(),
          decommissionSource);
    }
    adjustPendingOps(replicaCount, target, replica.getReplicaIndex(),
        decommissionSource);
  }

  private void adjustPendingOps(ECContainerReplicaCount replicaCount,
                                DatanodeDetails target, int replicaIndex,
                                DatanodeDetails decommissionSource) {
    replicaCount.addPendingOp(new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.ADD, target, replicaIndex, null,
        Long.MAX_VALUE, 0, decommissionSource));
  }

  static ByteString integers2ByteString(List<Integer> src) {
    byte[] dst = new byte[src.size()];

    for (int i = 0; i < src.size(); i++) {
      dst[i] = src.get(i).byteValue();
    }
    return Proto2Utils.unsafeByteString(dst);
  }

  private void checkAndRemoveUnhealthyReplica(
      ECContainerReplicaCount replicaCount,
      List<DatanodeDetails> deletionInFlight) {
    if (!deletionInFlight.isEmpty()) {
      return;
    }
    if (replicaCount.isUnrecoverable()) {
      return;
    }
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
        continue;
      }
      if (replica.getState().equals(State.CLOSED)) {
        closedReplicas.add(replica.getReplicaIndex());
      } else if (replica.getState().equals(State.UNHEALTHY)) {
        unhealthyReplicas.add(replica);
      }
    }
    if (unhealthyReplicas.isEmpty()) {
      return;
    }
    for (ContainerReplica unhealthyReplica : unhealthyReplicas) {
      if (closedReplicas.contains(unhealthyReplica.getReplicaIndex())) {
        try {
          replicationManager.sendThrottledDeleteCommand(
              replicaCount.getContainer(), unhealthyReplica.getReplicaIndex(),
              unhealthyReplica.getDatanodeDetails(), true);
          return;
        } catch (NotLeaderException | CommandTargetOverloadedException e) {
        }
      }
    }
    for (ContainerReplica unhealthyReplica : unhealthyReplicas) {
      try {
        replicationManager.sendThrottledDeleteCommand(
            replicaCount.getContainer(), unhealthyReplica.getReplicaIndex(),
            unhealthyReplica.getDatanodeDetails(), true);
        return;
      } catch (NotLeaderException | CommandTargetOverloadedException e) {
      }
    }
  }
}
