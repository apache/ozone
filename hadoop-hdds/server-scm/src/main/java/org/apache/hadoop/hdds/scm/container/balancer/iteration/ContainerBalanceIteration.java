/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.balancer.iteration;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaNotFoundException;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerMoveSelection;
import org.apache.hadoop.hdds.scm.container.balancer.MoveManager;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 *  A single step in container balance operation.
 *  The step is responsible for moving containers for source to target nodes
 */
public class ContainerBalanceIteration {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerBalanceIteration.class);
  private final int maxDatanodeCountToUseInIteration;
  private final long maxSizeToMovePerIteration;
  private final double upperLimit;
  private final double lowerLimit;
  private final long maxSizeEnteringTarget;
  private final List<DatanodeUsageInfo> overUtilizedNodes = new ArrayList<>();
  private final List<DatanodeUsageInfo> underUtilizedNodes = new ArrayList<>();
  private final List<DatanodeUsageInfo> withinThresholdUtilizedNodes = new ArrayList<>();
  private final FindTargetStrategy findTargetStrategy;
  private final FindSourceStrategy findSourceStrategy;
  private final ContainerSelectionCriteria selectionCriteria;

  /*
    Since a container can be selected only once during an iteration, these maps
    use it as a primary key to track source to target pairings.
  */
  private final Map<ContainerID, DatanodeDetails> containerToSourceMap = new HashMap<>();
  private final Map<ContainerID, DatanodeDetails> containerToTargetMap = new HashMap<>();

  private final Set<DatanodeDetails> selectedTargets = new HashSet<>();
  private final Set<DatanodeDetails> selectedSources = new HashSet<>();
  private final ArrayList<MoveState> moveStateList = new ArrayList<>();
  private final StorageContainerManager scm;
  private final IterationMetrics metrics = new IterationMetrics();

  public ContainerBalanceIteration(
      @Nonnull ContainerBalancerConfiguration config,
      @Nonnull StorageContainerManager scm,
      @Nonnull List<DatanodeUsageInfo> datanodeUsageInfos
  ) {
    this.scm = scm;
    maxSizeToMovePerIteration = config.getMaxSizeToMovePerIteration();
    int totalNodesInCluster = datanodeUsageInfos.size();
    maxDatanodeCountToUseInIteration = (int) (config.getMaxDatanodesRatioToInvolvePerIteration() * totalNodesInCluster);

    resetMoveManager(config, scm);

    double clusterAvgUtilisation = calculateAvgUtilization(datanodeUsageInfos);
    LOGGER.debug("Average utilization of the cluster is {}", clusterAvgUtilisation);

    double threshold = config.getThresholdAsRatio();
    // over utilized nodes have utilization greater than upper limit
    upperLimit = clusterAvgUtilisation + threshold;
    // under utilized nodes have utilization less than lower limit
    lowerLimit = clusterAvgUtilisation - threshold;

    findSourceStrategy = new FindSourceGreedy(scm.getScmNodeManager());
    findTargetStrategy = FindTargetStrategyFactory.create(scm, config.getNetworkTopologyEnable());

    LOGGER.debug("Lower limit for utilization is {} and Upper limit for utilization is {}", lowerLimit, upperLimit);

    selectionCriteria = new ContainerSelectionCriteria(config, scm);
    maxSizeEnteringTarget = config.getMaxSizeEnteringTarget();
  }

  private static void resetMoveManager(@Nonnull ContainerBalancerConfiguration config,
                                       @Nonnull StorageContainerManager scm
  ) {
    MoveManager moveManager = scm.getMoveManager();
    moveManager.resetState();
    moveManager.setMoveTimeout(config.getMoveTimeout().toMillis());
    moveManager.setReplicationTimeout(config.getMoveReplicationTimeout().toMillis());
  }

  public boolean findUnBalancedNodes(
      @Nonnull Supplier<Boolean> isTaskRunning,
      @Nonnull List<DatanodeUsageInfo> datanodeUsageInfos
  ) {
    long totalOverUtilizedBytes = 0L, totalUnderUtilizedBytes = 0L;
    // find over and under utilized nodes
    for (DatanodeUsageInfo datanodeUsageInfo : datanodeUsageInfos) {
      if (!isTaskRunning.get()) {
        return false;
      }
      double utilization = datanodeUsageInfo.calculateUtilization();
      SCMNodeStat scmNodeStat = datanodeUsageInfo.getScmNodeStat();
      long capacity = scmNodeStat.getCapacity().get();

      LOGGER.debug("Utilization for node {} with capacity {}B, used {}B, and remaining {}B is {}",
          datanodeUsageInfo.getDatanodeDetails().getUuidString(),
          capacity,
          scmNodeStat.getScmUsed().get(),
          scmNodeStat.getRemaining().get(),
          utilization);
      if (Double.compare(utilization, upperLimit) > 0) {
        overUtilizedNodes.add(datanodeUsageInfo);
        metrics.addToUnbalancedDatanodeCount(1);

        // amount of bytes greater than upper limit in this node
        long overUtilizedBytes = ratioToBytes(capacity, utilization) - ratioToBytes(capacity, upperLimit);
        totalOverUtilizedBytes += overUtilizedBytes;
      } else if (Double.compare(utilization, lowerLimit) < 0) {
        underUtilizedNodes.add(datanodeUsageInfo);
        metrics.addToUnbalancedDatanodeCount(1);

        // amount of bytes lesser than lower limit in this node
        long underUtilizedBytes = ratioToBytes(capacity, lowerLimit) - ratioToBytes(capacity, utilization);
        totalUnderUtilizedBytes += underUtilizedBytes;
      } else {
        withinThresholdUtilizedNodes.add(datanodeUsageInfo);
      }
    }
    metrics.addToUnbalancedDataSizeInBytes(Math.max(totalOverUtilizedBytes, totalUnderUtilizedBytes));

    if (overUtilizedNodes.isEmpty() && underUtilizedNodes.isEmpty()) {
      LOGGER.info("Did not find any unbalanced Datanodes.");
      return false;
    }

    LOGGER.info("Container Balancer has identified {} Over-Utilized and {} Under-Utilized Datanodes " +
            "that need to be balanced.",
        overUtilizedNodes.size(), underUtilizedNodes.size());

    if (LOGGER.isDebugEnabled()) {
      overUtilizedNodes.forEach(entry -> {
        LOGGER.debug("Datanode {} {} is Over-Utilized.",
            entry.getDatanodeDetails().getHostName(),
            entry.getDatanodeDetails().getUuid());
      });

      underUtilizedNodes.forEach(entry -> {
        LOGGER.debug("Datanode {} {} is Under-Utilized.",
            entry.getDatanodeDetails().getHostName(),
            entry.getDatanodeDetails().getUuid());
      });
    }

    return true;
  }

  public @Nonnull IterationResult doIteration(
      @Nonnull Supplier<Boolean> isTaskRunning,
      @Nonnull ContainerBalancerConfiguration config
  ) {
    // potential and selected targets are updated in the following loop
    // TODO(jacksonyao): take withinThresholdUtilizedNodes as candidate for both source and target
    findTargetStrategy.reInitialize(getPotentialTargets());
    findSourceStrategy.reInitialize(getPotentialSources());

    boolean isMoveGeneratedInThisIteration = false;
    boolean canAdaptWhenNearingLimits = config.adaptBalanceWhenCloseToLimits();
    boolean canAdaptOnReachingLimits = config.adaptBalanceWhenReachTheLimits();
    IterationResult currentState = IterationResult.ITERATION_COMPLETED;

    // match each source node with a target
    while (true) {
      if (!isTaskRunning.get()) {
        currentState = IterationResult.ITERATION_INTERRUPTED;
        break;
      }

      // break out if we've reached max size to move limit
      if (reachedMaxSizeToMovePerIteration()) {
        break;
      }

      // If balancer is approaching the iteration limits for max datanodes to involve,
      // take some action in adaptWhenNearingIterationLimits()
      if (canAdaptWhenNearingLimits && adaptWhenNearingIterationLimits()) {
        canAdaptWhenNearingLimits = false;
      }
      // check if balancer has hit the iteration limits and take some action
      if (canAdaptOnReachingLimits && adaptOnReachingIterationLimits()) {
        canAdaptOnReachingLimits = false;
        canAdaptWhenNearingLimits = false;
      }

      DatanodeDetails source = findSourceStrategy.getNextCandidateSourceDataNode();
      if (source == null) {
        // no more source DNs are present
        break;
      }

      ContainerMoveSelection moveSelection = matchSourceWithTarget(source);
      if (moveSelection != null && processMoveSelection(source, moveSelection)) {
        isMoveGeneratedInThisIteration = true;
      } else {
        // can not find any target for this source
        findSourceStrategy.removeCandidateSourceDataNode(source);
      }
    }

    if (isMoveGeneratedInThisIteration) {
      checkIterationMoveResults(config.getMoveTimeout().toMillis());
      metrics.setInvolvedDatanodeCount(selectedSources.size() + selectedTargets.size());
    } else {
      // If no move was generated during this iteration then we don't need to check the move results
      currentState = IterationResult.CAN_NOT_BALANCE_ANY_MORE;
    }
    return currentState;
  }

  /**
   * Match a source datanode with a target datanode and identify the container to move.
   *
   * @return ContainerMoveSelection containing the selected target and container
   */
  private @Nullable ContainerMoveSelection matchSourceWithTarget(@Nonnull DatanodeDetails source) {
    Set<ContainerID> candidateContainers = selectionCriteria.getCandidateContainers(
        source, findSourceStrategy, metrics.getMovedBytesCount(), lowerLimit);

    if (candidateContainers.isEmpty()) {
      LOGGER.debug("ContainerBalancer could not find any candidate containers for datanode {}", source.getUuidString());
      return null;
    }

    LOGGER.debug("ContainerBalancer is finding suitable target for source datanode {}", source.getUuidString());

    ContainerMoveSelection moveSelection = findTargetStrategy.findTargetForContainerMove(
            source, candidateContainers, maxSizeEnteringTarget, upperLimit);

    if (moveSelection != null) {
      LOGGER.info("ContainerBalancer matched source datanode {} with target datanode {} for container move.",
          source.getUuidString(),
          moveSelection.getTargetNode().getUuidString());
    } else {
      LOGGER.debug("ContainerBalancer could not find a suitable target for source node {}.", source.getUuidString());
    }

    return moveSelection;
  }

  private boolean reachedMaxSizeToMovePerIteration() {
    // since candidate containers in ContainerBalancerSelectionCriteria are
    // filtered out according to this limit, balancer should not have crossed it
    long bytesMovedInLatestIteration = metrics.getMovedBytesCount();
    if (bytesMovedInLatestIteration >= maxSizeToMovePerIteration) {
      LOGGER.warn("Reached max size to move limit. {} bytes have already been scheduled for balancing and " +
              "the limit is {} bytes.",
          bytesMovedInLatestIteration,
          maxSizeToMovePerIteration);
      return true;
    }
    return false;
  }

  /**
   * Restricts potential target datanodes to nodes that have already been selected if balancer is one datanode away from
   * "datanodes.involved.max.percentage.per.iteration" limit.
   *
   * @return true if potential targets were restricted, else false
   */
  private boolean adaptWhenNearingIterationLimits() {
    // check if we're nearing max datanodes to involve
    if (metrics.getInvolvedDatanodeCount() + 1 == maxDatanodeCountToUseInIteration) {
      // We're one datanode away from reaching the limit. Restrict potential targets to targets
      // that have already been selected.
      findTargetStrategy.resetPotentialTargets(selectedTargets);
      LOGGER.debug("Approaching max datanodes to involve limit. {} datanodes have already been selected for " +
              "balancing and the limit is {}. Only already selected targets can be selected as targets now.",
          metrics.getInvolvedDatanodeCount(), maxDatanodeCountToUseInIteration);
      return true;
    }
    // return false if we didn't adapt
    return false;
  }

  /**
   * Restricts potential source and target datanodes to nodes that have already been selected if balancer has reached
   * "datanodes.involved.max.percentage.per.iteration" limit.
   * @return true if potential sources and targets were restricted, else false
   */
  private boolean adaptOnReachingIterationLimits() {
    // check if we've reached max datanodes to involve limit
    if (metrics.getInvolvedDatanodeCount() == maxDatanodeCountToUseInIteration) {
      // restrict both to already selected sources and targets
      findTargetStrategy.resetPotentialTargets(selectedTargets);
      findSourceStrategy.resetPotentialSources(selectedSources);
      LOGGER.debug("Reached max datanodes to involve limit. {} datanodes have already been selected for balancing " +
              "and the limit is {}. Only already selected sources and targets can be involved in balancing now.",
          metrics.getInvolvedDatanodeCount(), maxDatanodeCountToUseInIteration);
      return true;
    }
    // return false if we didn't adapt
    return false;
  }

  private boolean processMoveSelection(
      @Nonnull DatanodeDetails source,
      @Nonnull ContainerMoveSelection moveSelection
  ) {
    ContainerID containerID = moveSelection.getContainerID();
    if (containerToSourceMap.containsKey(containerID) || containerToTargetMap.containsKey(containerID)) {
      LOGGER.warn("Container {} has already been selected for move from source {} to target {} earlier. " +
              "Not moving this container again.",
          containerID,
          containerToSourceMap.get(containerID),
          containerToTargetMap.get(containerID));
      return false;
    }

    ContainerInfo containerInfo;
    try {
      containerInfo = scm.getContainerManager().getContainer(containerID);
    } catch (ContainerNotFoundException e) {
      LOGGER.warn("Could not get container {} from Container Manager before starting a container move", containerID, e);
      return false;
    }
    LOGGER.info("ContainerBalancer is trying to move container {} with size {}B from source datanode {} " +
            "to target datanode {}",
        containerID.toString(),
        containerInfo.getUsedBytes(),
        source.getUuidString(),
        moveSelection.getTargetNode().getUuidString());

    if (moveContainer(source, moveSelection)) {
      // consider move successful for now, and update selection criteria
      updateTargetsAndSelectionCriteria(moveSelection, source);
    }
    return true;
  }

  /**
   * Asks {@link ReplicationManager} or {@link MoveManager} to move the specified container from source to target.
   *
   * @param sourceDnDetails        the source datanode
   * @param moveSelection the selected container to move and target datanode
   * @return false if an exception occurred or the move completed with a result other than
   *         MoveManager.MoveResult.COMPLETED.
   *         Returns true if the move completed with MoveResult.COMPLETED or move is not yet done
   */
  private boolean moveContainer(
      @Nonnull DatanodeDetails sourceDnDetails,
      @Nonnull ContainerMoveSelection moveSelection
  ) {
    ContainerID containerID = moveSelection.getContainerID();
    CompletableFuture<MoveManager.MoveResult> future;
    try {
      ContainerInfo containerInfo = scm.getContainerManager().getContainer(containerID);
      /*
      If LegacyReplicationManager is enabled, ReplicationManager will redirect to it. Otherwise, use MoveManager.
       */
      ReplicationManager replicationManager = scm.getReplicationManager();
      DatanodeDetails targetDnDetails = moveSelection.getTargetNode();
      if (replicationManager.getConfig().isLegacyEnabled()) {
        future = replicationManager.move(containerID, sourceDnDetails, targetDnDetails);
      } else {
        future = scm.getMoveManager().move(containerID, sourceDnDetails, targetDnDetails);
      }
      metrics.addToScheduledContainerMovesCount(1);

      future = future.whenComplete((result, ex) -> {
        if (result != null) {
          metrics.addToContainerMoveMetrics(result, 1);
        }
        String sourceUUID = sourceDnDetails.getUuidString();
        String targetUUUID = targetDnDetails.getUuidString();
        if (ex != null) {
          LOGGER.info("Container move for container {} from source {} to target {} failed with exceptions.",
              containerID.toString(), sourceUUID, targetUUUID, ex);
          metrics.addToFailedContainerMovesCount(1);
        } else {
          if (result == MoveManager.MoveResult.COMPLETED) {
            metrics.addToMovedBytesCount(containerInfo.getUsedBytes());
            LOGGER.debug("Container move completed for container {} from source {} to target {}",
                containerID, sourceUUID, targetUUUID);
          } else {
            LOGGER.warn("Container move for container {} from source {} to target {} failed: {}",
                containerInfo, sourceUUID, targetUUUID, result);
          }
        }
      });
    } catch (ContainerNotFoundException e) {
      LOGGER.warn("Could not find Container {} for container move", containerID, e);
      metrics.addToFailedContainerMovesCount(1);
      return false;
    } catch (NodeNotFoundException | TimeoutException | ContainerReplicaNotFoundException e) {
      LOGGER.warn("Container move failed for container {}", containerID, e);
      metrics.addToFailedContainerMovesCount(1);
      return false;
    }

    /*
    If the future hasn't failed yet, put it in moveSelectionToFutureMap for processing later
     */
    if (future.isDone()) {
      if (future.isCompletedExceptionally()) {
        return false;
      } else {
        MoveManager.MoveResult result = future.join();
        moveStateList.add(new MoveState(moveSelection, future));
        return result == MoveManager.MoveResult.COMPLETED;
      }
    } else {
      moveStateList.add(new MoveState(moveSelection, future));
      return true;
    }
  }

  /**
   * Update targets, sources, and selection criteria after a move.
   *
   * @param moveSelection latest selected target datanode and container
   * @param source        the source datanode
   */
  private void updateTargetsAndSelectionCriteria(
      @Nonnull ContainerMoveSelection moveSelection,
      @Nonnull DatanodeDetails source
  ) {
    ContainerID containerID = moveSelection.getContainerID();
    DatanodeDetails target = moveSelection.getTargetNode();

    // count target if it has not been involved in move earlier
    metrics.addToInvolvedDatanodeCount(selectedSources.contains(source) ? 0 : 1);
    metrics.addToInvolvedDatanodeCount(selectedTargets.contains(target) ? 0 : 1);

    incSizeSelectedForMoving(source, moveSelection);
    containerToSourceMap.put(containerID, source);
    containerToTargetMap.put(containerID, target);
    selectedSources.add(source);
    selectedTargets.add(target);
    selectionCriteria.updateSelectedContainer(containerToSourceMap.keySet());
  }

  /**
   * Updates conditions for balancing, such as total size moved by balancer, total size that has entered a datanode,
   * and total size that has left a datanode in this iteration.
   *
   * @param source        source datanode
   * @param moveSelection selected target datanode and container
   */
  private void incSizeSelectedForMoving(
      @Nonnull DatanodeDetails source,
      @Nonnull ContainerMoveSelection moveSelection
  ) {
    try {
      ContainerInfo container = scm.getContainerManager().getContainer(moveSelection.getContainerID());
      long usedBytes = container.getUsedBytes();

      // update sizeLeavingNode map with the recent moveSelection
      findSourceStrategy.increaseSizeLeaving(source, usedBytes);

      // update sizeEnteringNode map with the recent moveSelection
      findTargetStrategy.increaseSizeEntering(moveSelection.getTargetNode(), usedBytes, maxSizeEnteringTarget);
    } catch (ContainerNotFoundException e) {
      LOGGER.warn("Could not find Container {} while matching source and target nodes in ContainerBalancer",
          moveSelection.getContainerID(), e);
    }
  }

  /**
   * Checks the results of all move operations when exiting an iteration.
   */
  @SuppressWarnings("rawtypes")
  private void checkIterationMoveResults(long timeoutInMillis) {
    CompletableFuture[] futureArray = new CompletableFuture[moveStateList.size()];
    for (int i = 0; i < moveStateList.size(); ++i) {
      futureArray[i] = moveStateList.get(i).result;
    }
    CompletableFuture<Void> allFuturesResult = CompletableFuture.allOf(futureArray);
    try {
      allFuturesResult.get(timeoutInMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOGGER.warn("Container balancer is interrupted");
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      long timeoutCounts = cancelMovesThatExceedTimeoutDuration();
      LOGGER.warn("{} Container moves are canceled.", timeoutCounts);
      metrics.addToTimeoutContainerMovesCount(timeoutCounts);
    } catch (ExecutionException e) {
      LOGGER.error("Got exception while checkIterationMoveResults", e);
    }
  }

  /**
   * Cancels container moves that are not yet done. Note that if a move command has already been sent out to a Datanode,
   * we don't yet have the capability to cancel it.
   * However, those commands in the DN should time out if they haven't been processed yet.
   *
   * @return number of moves that did not complete (timed out) and were cancelled.
   */
  private long cancelMovesThatExceedTimeoutDuration() {
    int numCancelled = 0;
    // iterate through all moves and cancel ones that aren't done yet
    for (MoveState state : moveStateList) {
      CompletableFuture<MoveManager.MoveResult> future = state.result;
      if (!future.isDone()) {
        ContainerMoveSelection moveSelection = state.moveSelection;
        ContainerID containerID = moveSelection.getContainerID();
        LOGGER.warn("Container move timed out for container {} from source {} to target {}.",
            containerID,
            containerToSourceMap.get(containerID).getUuidString(),
            moveSelection.getTargetNode().getUuidString()
        );

        future.cancel(true);
        numCancelled += 1;
      }
    }

    return numCancelled;
  }

  /**
   * Get potential targets for container move. Potential targets are under utilized and within threshold utilized nodes.
   *
   * @return A list of potential target DatanodeUsageInfo.
   */
  private @Nonnull List<DatanodeUsageInfo> getPotentialTargets() {
    // TODO(jacksonyao): take withinThresholdUtilizedNodes as candidate for both source and target
    return getUnderUtilizedNodes();
  }

  /**
   * Get potential sourecs for container move. Potential sourecs are over utilized and within threshold utilized nodes.
   *
   * @return A list of potential source DatanodeUsageInfo.
   */
  private @Nonnull List<DatanodeUsageInfo> getPotentialSources() {
    // TODO(jacksonyao): take withinThresholdUtilizedNodes as candidate for both source and target
    return getOverUtilizedNodes();
  }

  /**
   * Calculates the average utilization for the specified nodes.
   * Utilization is (capacity - remaining) divided by capacity.
   *
   * @param nodes List of DatanodeUsageInfo to find the average utilization for
   * @return Average utilization value
   */
  @VisibleForTesting
  public static double calculateAvgUtilization(@Nonnull List<DatanodeUsageInfo> nodes) {
    if (nodes.isEmpty()) {
      LOGGER.warn("No nodes to calculate average utilization for ContainerBalancer.");
      return 0;
    }
    SCMNodeStat aggregatedStats = new SCMNodeStat(0, 0, 0);
    for (DatanodeUsageInfo node : nodes) {
      aggregatedStats.add(node.getScmNodeStat());
    }
    long clusterCapacity = aggregatedStats.getCapacity().get();
    long clusterRemaining = aggregatedStats.getRemaining().get();

    return (clusterCapacity - clusterRemaining) / (double) clusterCapacity;
  }

  /**
   * Calculates the number of used bytes given capacity and utilization ratio.
   *
   * @param nodeCapacity     capacity of the node.
   * @param utilizationRatio used space by capacity ratio of the node.
   * @return number of bytes
   */
  private long ratioToBytes(long nodeCapacity, double utilizationRatio) {
    return (long) (nodeCapacity * utilizationRatio);
  }

  public @Nonnull List<DatanodeUsageInfo> getOverUtilizedNodes() {
    return overUtilizedNodes;
  }

  public @Nonnull List<DatanodeUsageInfo> getUnderUtilizedNodes() {
    return underUtilizedNodes;
  }

  public @Nonnull Set<DatanodeDetails> getSelectedTargets() {
    return selectedTargets;
  }

  public @Nonnull Map<ContainerID, DatanodeDetails> getContainerToTargetMap() {
    return containerToTargetMap;
  }

  public @Nonnull Map<ContainerID, DatanodeDetails> getContainerToSourceMap() {
    return containerToSourceMap;
  }

  public IterationMetrics getMetrics() {
    return metrics;
  }

  /**
   * Class represents the current move state of iteration.
   */
  private static final class MoveState {
    private final ContainerMoveSelection moveSelection;
    private final CompletableFuture<MoveManager.MoveResult> result;

    private MoveState(
        @Nonnull ContainerMoveSelection moveSelection,
        @Nonnull CompletableFuture<MoveManager.MoveResult> future
    ) {
      this.moveSelection = moveSelection;
      this.result = future;
    }
  }
}
