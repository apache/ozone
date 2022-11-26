/*
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

package org.apache.hadoop.hdds.scm.container.balancer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PlacementPolicyValidateProxy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.container.replication.LegacyReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL_DEFAULT;

/**
 * Container balancer task performs move of containers between over- and
 * under-utilized datanodes.
 */
public class ContainerBalancerTask implements Runnable {

  public static final Logger LOG =
      LoggerFactory.getLogger(ContainerBalancerTask.class);

  private NodeManager nodeManager;
  private ContainerManager containerManager;
  private ReplicationManager replicationManager;
  private OzoneConfiguration ozoneConfiguration;
  private ContainerBalancer containerBalancer;
  private final SCMContext scmContext;
  private double threshold;
  private int totalNodesInCluster;
  private double maxDatanodesRatioToInvolvePerIteration;
  private long maxSizeToMovePerIteration;
  private int countDatanodesInvolvedPerIteration;
  private long sizeScheduledForMoveInLatestIteration;
  // count actual size moved in bytes
  private long sizeActuallyMovedInLatestIteration;
  private int iterations;
  private List<DatanodeUsageInfo> unBalancedNodes;
  private List<DatanodeUsageInfo> overUtilizedNodes;
  private List<DatanodeUsageInfo> underUtilizedNodes;
  private List<DatanodeUsageInfo> withinThresholdUtilizedNodes;
  private Set<String> excludeNodes;
  private Set<String> includeNodes;
  private ContainerBalancerConfiguration config;
  private ContainerBalancerMetrics metrics;
  private long clusterCapacity;
  private long clusterRemaining;
  private double clusterAvgUtilisation;
  private PlacementPolicyValidateProxy placementPolicyValidateProxy;
  private NetworkTopology networkTopology;
  private double upperLimit;
  private double lowerLimit;
  private ContainerBalancerSelectionCriteria selectionCriteria;
  private volatile Status taskStatus = Status.RUNNING;

  /*
  Since a container can be selected only once during an iteration, these maps
   use it as a primary key to track source to target pairings.
  */
  private final Map<ContainerID, DatanodeDetails> containerToSourceMap;
  private final Map<ContainerID, DatanodeDetails> containerToTargetMap;

  private Set<DatanodeDetails> selectedTargets;
  private Set<DatanodeDetails> selectedSources;
  private FindTargetStrategy findTargetStrategy;
  private FindSourceStrategy findSourceStrategy;
  private Map<ContainerMoveSelection,
      CompletableFuture<LegacyReplicationManager.MoveResult>>
      moveSelectionToFutureMap;
  private IterationResult iterationResult;
  private int nextIterationIndex;

  /**
   * Constructs ContainerBalancerTask with the specified arguments.
   *
   * @param scm the storage container manager
   * @param nextIterationIndex next iteration index for continue
   * @param containerBalancer the container balancer
   * @param metrics the metrics
   * @param config the config
   */
  public ContainerBalancerTask(StorageContainerManager scm,
                               int nextIterationIndex,
                               ContainerBalancer containerBalancer,
                               ContainerBalancerMetrics metrics,
                               ContainerBalancerConfiguration config) {
    this.nodeManager = scm.getScmNodeManager();
    this.containerManager = scm.getContainerManager();
    this.replicationManager = scm.getReplicationManager();
    this.ozoneConfiguration = scm.getConfiguration();
    this.containerBalancer = containerBalancer;
    this.config = config;
    this.metrics = metrics;
    this.scmContext = scm.getScmContext();
    this.overUtilizedNodes = new ArrayList<>();
    this.underUtilizedNodes = new ArrayList<>();
    this.withinThresholdUtilizedNodes = new ArrayList<>();
    this.unBalancedNodes = new ArrayList<>();
    this.placementPolicyValidateProxy = scm.getPlacementPolicyValidateProxy();
    this.networkTopology = scm.getClusterMap();
    this.nextIterationIndex = nextIterationIndex;
    this.containerToSourceMap = new HashMap<>();
    this.containerToTargetMap = new HashMap<>();
    this.selectedSources = new HashSet<>();
    this.selectedTargets = new HashSet<>();
    findSourceStrategy = new FindSourceGreedy(nodeManager);
  }

  /**
   * Run the container balancer task.
   */
  public void run() {
    try {
      balance();
    } catch (Exception e) {
      LOG.error("Container Balancer is stopped abnormally, ", e);
    } finally {
      synchronized (this) {
        taskStatus = Status.STOPPED;
      }
    }
  }

  /**
   * Changes the status from RUNNING to STOPPING.
   */
  public void stop() {
    synchronized (this) {
      if (taskStatus == Status.RUNNING) {
        taskStatus = Status.STOPPING;
      }
    }
  }

  private void balance() {
    this.iterations = config.getIterations();
    if (this.iterations == -1) {
      //run balancer infinitely
      this.iterations = Integer.MAX_VALUE;
    }

    // nextIterationIndex is the iteration that balancer should start from on
    // leader change or restart
    int i = nextIterationIndex;
    for (; i < iterations && isBalancerRunning(); i++) {
      // reset some variables and metrics for this iteration
      resetState();
      if (config.getTriggerDuEnable()) {
        // before starting a new iteration, we trigger all the datanode
        // to run `du`. this is an aggressive action, with which we can
        // get more precise usage info of all datanodes before moving.
        // this is helpful for container balancer to make more appropriate
        // decisions. this will increase the disk io load of data nodes, so
        // please enable it with caution.
        nodeManager.refreshAllHealthyDnUsageInfo();
        try {
          long nodeReportInterval =
              ozoneConfiguration.getTimeDuration(HDDS_NODE_REPORT_INTERVAL,
                  HDDS_NODE_REPORT_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
          // one for sending command , one for running du, and one for
          // reporting back make it like this for now, a more suitable
          // value. can be set in the future if needed
          Thread.sleep(3 * nodeReportInterval);
        } catch (InterruptedException e) {
          LOG.info("Container Balancer was interrupted while waiting for" +
              "datanodes refreshing volume usage info");
          Thread.currentThread().interrupt();
          return;
        }
      }

      if (!isBalancerRunning()) {
        return;
      }

      // initialize this iteration. stop balancing on initialization failure
      if (!initializeIteration()) {
        // just return if the reason for initialization failure is that
        // balancer has been stopped in another thread
        if (!isBalancerRunning()) {
          return;
        }
        // otherwise, try to stop balancer
        tryStopWithSaveConfiguration("Could not initialize " +
            "ContainerBalancer's iteration number " + i);
        return;
      }

      IterationResult iR = doIteration();
      metrics.incrementNumIterations(1);
      LOG.info("Result of this iteration of Container Balancer: {}", iR);

      // if no new move option is generated, it means the cluster cannot be
      // balanced anymore; so just stop balancer
      if (iR == IterationResult.CAN_NOT_BALANCE_ANY_MORE) {
        tryStopWithSaveConfiguration(iR.toString());
        return;
      }

      // persist next iteration index
      if (iR == IterationResult.ITERATION_COMPLETED) {
        try {
          saveConfiguration(config, true, i + 1);
        } catch (IOException | TimeoutException e) {
          LOG.warn("Could not persist next iteration index value for " +
              "ContainerBalancer after completing an iteration", e);
        }
      }

      // return if balancing has been stopped
      if (!isBalancerRunning()) {
        return;
      }

      // wait for configured time before starting next iteration, unless
      // this was the final iteration
      if (i != iterations - 1) {
        try {
          Thread.sleep(config.getBalancingInterval().toMillis());
        } catch (InterruptedException e) {
          LOG.info("Container Balancer was interrupted while waiting for" +
              " next iteration.");
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
    
    tryStopWithSaveConfiguration("Completed all iterations.");
  }

  /**
   * Logs the reason for stop and save configuration and stop the task.
   * 
   * @param stopReason a string specifying the reason for stop
   */
  private void tryStopWithSaveConfiguration(String stopReason) {
    synchronized (this) {
      try {
        LOG.info("Save Configuration for stopping. Reason: {}", stopReason);
        saveConfiguration(config, false, 0);
        stop();
      } catch (IOException | TimeoutException e) {
        LOG.warn("Save configuration failed. Reason for " +
            "stopping: {}", stopReason, e);
      }
    }
  }

  private void saveConfiguration(ContainerBalancerConfiguration configuration,
                                 boolean shouldRun, int index)
      throws IOException, TimeoutException {
    if (!isValidSCMState()) {
      LOG.warn("Save configuration is not allowed as not in valid State.");
      return;
    }
    synchronized (this) {
      if (isBalancerRunning()) {
        containerBalancer.saveConfiguration(configuration, shouldRun, index);
      }
    }
  }

  /**
   * Initializes an iteration during balancing. Recognizes over, under, and
   * within threshold utilized nodes. Decides whether balancing needs to
   * continue or should be stopped.
   *
   * @return true if successfully initialized, otherwise false.
   */
  private boolean initializeIteration() {
    if (!isValidSCMState()) {
      return false;
    }
    // sorted list in order from most to least used
    List<DatanodeUsageInfo> datanodeUsageInfos =
        nodeManager.getMostOrLeastUsedDatanodes(true);
    if (datanodeUsageInfos.isEmpty()) {
      LOG.warn("Received an empty list of datanodes from Node Manager when " +
          "trying to identify which nodes to balance");
      return false;
    }

    this.threshold = config.getThresholdAsRatio();
    this.maxDatanodesRatioToInvolvePerIteration =
        config.getMaxDatanodesRatioToInvolvePerIteration();
    this.maxSizeToMovePerIteration = config.getMaxSizeToMovePerIteration();
    if (config.getNetworkTopologyEnable()) {
      findTargetStrategy = new FindTargetGreedyByNetworkTopology(
          containerManager, placementPolicyValidateProxy,
          nodeManager, networkTopology);
    } else {
      findTargetStrategy = new FindTargetGreedyByUsageInfo(containerManager,
          placementPolicyValidateProxy, nodeManager);
    }
    this.excludeNodes = config.getExcludeNodes();
    this.includeNodes = config.getIncludeNodes();
    // include/exclude nodes from balancing according to configs
    datanodeUsageInfos.removeIf(datanodeUsageInfo -> shouldExcludeDatanode(
        datanodeUsageInfo.getDatanodeDetails()));

    this.totalNodesInCluster = datanodeUsageInfos.size();

    clusterAvgUtilisation = calculateAvgUtilization(datanodeUsageInfos);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Average utilization of the cluster is {}",
          clusterAvgUtilisation);
    }

    // over utilized nodes have utilization(that is, used / capacity) greater
    // than upper limit
    this.upperLimit = clusterAvgUtilisation + threshold;
    // under utilized nodes have utilization(that is, used / capacity) less
    // than lower limit
    this.lowerLimit = clusterAvgUtilisation - threshold;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Lower limit for utilization is {} and Upper limit for " +
          "utilization is {}", lowerLimit, upperLimit);
    }

    long totalOverUtilizedBytes = 0L, totalUnderUtilizedBytes = 0L;
    // find over and under utilized nodes
    for (DatanodeUsageInfo datanodeUsageInfo : datanodeUsageInfos) {
      if (!isBalancerRunning()) {
        return false;
      }
      double utilization = datanodeUsageInfo.calculateUtilization();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Utilization for node {} with capacity {}B, used {}B, and " +
                "remaining {}B is {}",
            datanodeUsageInfo.getDatanodeDetails().getUuidString(),
            datanodeUsageInfo.getScmNodeStat().getCapacity().get(),
            datanodeUsageInfo.getScmNodeStat().getScmUsed().get(),
            datanodeUsageInfo.getScmNodeStat().getRemaining().get(),
            utilization);
      }
      if (Double.compare(utilization, upperLimit) > 0) {
        overUtilizedNodes.add(datanodeUsageInfo);
        metrics.incrementNumDatanodesUnbalanced(1);

        // amount of bytes greater than upper limit in this node
        long overUtilizedBytes = ratioToBytes(
            datanodeUsageInfo.getScmNodeStat().getCapacity().get(),
            utilization) - ratioToBytes(
            datanodeUsageInfo.getScmNodeStat().getCapacity().get(),
            upperLimit);
        totalOverUtilizedBytes += overUtilizedBytes;
      } else if (Double.compare(utilization, lowerLimit) < 0) {
        underUtilizedNodes.add(datanodeUsageInfo);
        metrics.incrementNumDatanodesUnbalanced(1);

        // amount of bytes lesser than lower limit in this node
        long underUtilizedBytes = ratioToBytes(
            datanodeUsageInfo.getScmNodeStat().getCapacity().get(),
            lowerLimit) - ratioToBytes(
            datanodeUsageInfo.getScmNodeStat().getCapacity().get(),
            utilization);
        totalUnderUtilizedBytes += underUtilizedBytes;
      } else {
        withinThresholdUtilizedNodes.add(datanodeUsageInfo);
      }
    }
    metrics.incrementDataSizeUnbalancedGB(
        Math.max(totalOverUtilizedBytes, totalUnderUtilizedBytes) /
            OzoneConsts.GB);
    Collections.reverse(underUtilizedNodes);

    unBalancedNodes = new ArrayList<>(
        overUtilizedNodes.size() + underUtilizedNodes.size());
    unBalancedNodes.addAll(overUtilizedNodes);
    unBalancedNodes.addAll(underUtilizedNodes);

    if (unBalancedNodes.isEmpty()) {
      LOG.info("Did not find any unbalanced Datanodes.");
      return false;
    }

    LOG.info("Container Balancer has identified {} Over-Utilized and {} " +
            "Under-Utilized Datanodes that need to be balanced.",
        overUtilizedNodes.size(), underUtilizedNodes.size());

    if (LOG.isDebugEnabled()) {
      overUtilizedNodes.forEach(entry -> {
        LOG.debug("Datanode {} {} is Over-Utilized.",
            entry.getDatanodeDetails().getHostName(),
            entry.getDatanodeDetails().getUuid());
      });

      underUtilizedNodes.forEach(entry -> {
        LOG.debug("Datanode {} {} is Under-Utilized.",
            entry.getDatanodeDetails().getHostName(),
            entry.getDatanodeDetails().getUuid());
      });
    }

    selectionCriteria = new ContainerBalancerSelectionCriteria(config,
        nodeManager, replicationManager, containerManager, findSourceStrategy);
    return true;
  }

  private boolean isValidSCMState() {
    if (scmContext.isInSafeMode()) {
      LOG.error("Container Balancer cannot operate while SCM is in Safe Mode.");
      return false;
    }
    if (!scmContext.isLeaderReady()) {
      LOG.warn("Current SCM is not the leader.");
      return false;
    }
    return true;
  }

  private IterationResult doIteration() {
    // note that potential and selected targets are updated in the following
    // loop
    //TODO(jacksonyao): take withinThresholdUtilizedNodes as candidate for both
    // source and target
    List<DatanodeUsageInfo> potentialTargets = getPotentialTargets();
    findTargetStrategy.reInitialize(potentialTargets, config, upperLimit);
    findSourceStrategy.reInitialize(getPotentialSources(), config, lowerLimit);

    moveSelectionToFutureMap = new HashMap<>(unBalancedNodes.size());
    boolean isMoveGeneratedInThisIteration = false;
    iterationResult = IterationResult.ITERATION_COMPLETED;
    boolean canAdaptWhenNearingLimits = true;
    boolean canAdaptOnReachingLimits = true;

    // match each source node with a target
    while (true) {
      if (!isBalancerRunning()) {
        iterationResult = IterationResult.ITERATION_INTERRUPTED;
        break;
      }

      // break out if we've reached max size to move limit
      if (reachedMaxSizeToMovePerIteration()) {
        break;
      }

      /* if balancer is approaching the iteration limits for max datanodes to
       involve, take some action in adaptWhenNearingIterationLimits()
      */
      if (canAdaptWhenNearingLimits) {
        if (adaptWhenNearingIterationLimits()) {
          canAdaptWhenNearingLimits = false;
        }
      }
      if (canAdaptOnReachingLimits) {
        // check if balancer has hit the iteration limits and take some action
        if (adaptOnReachingIterationLimits()) {
          canAdaptOnReachingLimits = false;
          canAdaptWhenNearingLimits = false;
        }
      }

      DatanodeDetails source =
          findSourceStrategy.getNextCandidateSourceDataNode();
      if (source == null) {
        // no more source DNs are present
        break;
      }

      ContainerMoveSelection moveSelection = matchSourceWithTarget(source);
      if (moveSelection != null) {
        if (processMoveSelection(source, moveSelection)) {
          isMoveGeneratedInThisIteration = true;
        }
      } else {
        // can not find any target for this source
        findSourceStrategy.removeCandidateSourceDataNode(source);
      }
    }

    checkIterationResults(isMoveGeneratedInThisIteration);
    return iterationResult;
  }

  private boolean processMoveSelection(DatanodeDetails source,
                                       ContainerMoveSelection moveSelection) {
    ContainerID containerID = moveSelection.getContainerID();
    if (containerToSourceMap.containsKey(containerID) ||
        containerToTargetMap.containsKey(containerID)) {
      LOG.warn("Container {} has already been selected for move from source " +
              "{} to target {} earlier. Not moving this container again.",
          containerID,
          containerToSourceMap.get(containerID),
          containerToTargetMap.get(containerID));
      return false;
    }

    ContainerInfo containerInfo;
    try {
      containerInfo =
          containerManager.getContainer(containerID);
    } catch (ContainerNotFoundException e) {
      LOG.warn("Could not get container {} from Container Manager before " +
          "starting a container move", containerID, e);
      return false;
    }
    LOG.info("ContainerBalancer is trying to move container {} with size " +
            "{}B from source datanode {} to target datanode {}",
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
   * Check the iteration results. Result can be:
   * <p>ITERATION_INTERRUPTED if balancing was stopped</p>
   * <p>CAN_NOT_BALANCE_ANY_MORE if no move was generated during this iteration
   * </p>
   * <p>ITERATION_COMPLETED</p>
   * @param isMoveGeneratedInThisIteration whether a move was generated during
   *                                       the iteration
   */
  private void checkIterationResults(boolean isMoveGeneratedInThisIteration) {
    if (!isMoveGeneratedInThisIteration) {
      /*
       If no move was generated during this iteration then we don't need to
       check the move results
       */
      iterationResult = IterationResult.CAN_NOT_BALANCE_ANY_MORE;
    } else {
      checkIterationMoveResults();
    }
  }

  /**
   * Checks the results of all move operations when exiting an iteration.
   */
  private void checkIterationMoveResults() {
    this.countDatanodesInvolvedPerIteration = 0;
    CompletableFuture<Void> allFuturesResult = CompletableFuture.allOf(
        moveSelectionToFutureMap.values()
            .toArray(new CompletableFuture[moveSelectionToFutureMap.size()]));
    try {
      allFuturesResult.get(config.getMoveTimeout().toMillis(),
          TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Container balancer is interrupted");
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      long timeoutCounts = cancelAndCountPendingMoves();
      LOG.warn("{} Container moves are canceled.", timeoutCounts);
      metrics.incrementNumContainerMovesTimeoutInLatestIteration(timeoutCounts);
    } catch (ExecutionException e) {
      LOG.error("Got exception while checkIterationMoveResults", e);
    }

    countDatanodesInvolvedPerIteration =
        selectedSources.size() + selectedTargets.size();
    metrics.incrementNumDatanodesInvolvedInLatestIteration(
        countDatanodesInvolvedPerIteration);
    metrics.incrementNumContainerMovesCompleted(
        metrics.getNumContainerMovesCompletedInLatestIteration());
    metrics.incrementNumContainerMovesTimeout(
        metrics.getNumContainerMovesTimeoutInLatestIteration());
    metrics.incrementDataSizeMovedGBInLatestIteration(
        sizeActuallyMovedInLatestIteration / OzoneConsts.GB);
    metrics.incrementDataSizeMovedGB(
        metrics.getDataSizeMovedGBInLatestIteration());
    metrics.incrementNumContainerMovesFailed(
        metrics.getNumContainerMovesFailedInLatestIteration());
    LOG.info("Iteration Summary. Number of Datanodes involved: {}. Size " +
            "moved: {} ({} Bytes). Number of Container moves completed: {}.",
        countDatanodesInvolvedPerIteration,
        StringUtils.byteDesc(sizeActuallyMovedInLatestIteration),
        sizeActuallyMovedInLatestIteration,
        metrics.getNumContainerMovesCompletedInLatestIteration());
  }

  private long cancelAndCountPendingMoves() {
    return moveSelectionToFutureMap.entrySet().stream()
        .filter(entry -> !entry.getValue().isDone())
        .peek(entry -> {
          LOG.warn("Container move timeout for container {} from source {}" +
                  " to target {}.",
              entry.getKey().getContainerID(),
              containerToSourceMap.get(entry.getKey().getContainerID())
                  .getUuidString(),
              entry.getKey().getTargetNode().getUuidString());
          entry.getValue().cancel(true);
        }).count();
  }

  /**
   * Match a source datanode with a target datanode and identify the container
   * to move.
   *
   * @return ContainerMoveSelection containing the selected target and container
   */
  private ContainerMoveSelection matchSourceWithTarget(DatanodeDetails source) {
    NavigableSet<ContainerID> candidateContainers =
        selectionCriteria.getCandidateContainers(source,
            sizeScheduledForMoveInLatestIteration);

    if (candidateContainers.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ContainerBalancer could not find any candidate containers " +
            "for datanode {}", source.getUuidString());
      }
      return null;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("ContainerBalancer is finding suitable target for source " +
          "datanode {}", source.getUuidString());
    }
    ContainerMoveSelection moveSelection =
        findTargetStrategy.findTargetForContainerMove(
            source, candidateContainers);

    if (moveSelection == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ContainerBalancer could not find a suitable target for " +
            "source node {}.", source.getUuidString());
      }
      return null;
    }
    LOG.info("ContainerBalancer matched source datanode {} with target " +
            "datanode {} for container move.", source.getUuidString(),
        moveSelection.getTargetNode().getUuidString());

    return moveSelection;
  }

  private boolean reachedMaxSizeToMovePerIteration() {
    // since candidate containers in ContainerBalancerSelectionCriteria are
    // filtered out according to this limit, balancer should not have crossed it
    if (sizeScheduledForMoveInLatestIteration >= maxSizeToMovePerIteration) {
      LOG.warn("Reached max size to move limit. {} bytes have already been" +
              " scheduled for balancing and the limit is {} bytes.",
          sizeScheduledForMoveInLatestIteration, maxSizeToMovePerIteration);
      return true;
    }
    return false;
  }

  /**
   * Restricts potential target datanodes to nodes that have
   * already been selected if balancer is one datanode away from
   * "datanodes.involved.max.percentage.per.iteration" limit.
   * @return true if potential targets were restricted, else false
   */
  private boolean adaptWhenNearingIterationLimits() {
    // check if we're nearing max datanodes to involve
    int maxDatanodesToInvolve =
        (int) (maxDatanodesRatioToInvolvePerIteration * totalNodesInCluster);
    if (countDatanodesInvolvedPerIteration + 1 == maxDatanodesToInvolve) {
      /* We're one datanode away from reaching the limit. Restrict potential
      targets to targets that have already been selected.
       */
      findTargetStrategy.resetPotentialTargets(selectedTargets);
      LOG.debug("Approaching max datanodes to involve limit. {} datanodes " +
              "have already been selected for balancing and the limit is " +
              "{}. Only already selected targets can be selected as targets" +
              " now.",
          countDatanodesInvolvedPerIteration, maxDatanodesToInvolve);
      return true;
    }

    // return false if we didn't adapt
    return false;
  }

  /**
   * Restricts potential source and target datanodes to nodes that have
   * already been selected if balancer has reached
   * "datanodes.involved.max.percentage.per.iteration" limit.
   * @return true if potential sources and targets were restricted, else false
   */
  private boolean adaptOnReachingIterationLimits() {
    // check if we've reached max datanodes to involve limit
    int maxDatanodesToInvolve =
        (int) (maxDatanodesRatioToInvolvePerIteration * totalNodesInCluster);
    if (countDatanodesInvolvedPerIteration == maxDatanodesToInvolve) {
      // restrict both to already selected sources and targets
      findTargetStrategy.resetPotentialTargets(selectedTargets);
      findSourceStrategy.resetPotentialSources(selectedSources);
      LOG.debug("Reached max datanodes to involve limit. {} datanodes " +
              "have already been selected for balancing and the limit " +
              "is {}. Only already selected sources and targets can be " +
              "involved in balancing now.",
          countDatanodesInvolvedPerIteration, maxDatanodesToInvolve);
      return true;
    }

    // return false if we didn't adapt
    return false;
  }

  /**
   * Asks {@link ReplicationManager} to move the specified container from
   * source to target.
   *
   * @param source the source datanode
   * @param moveSelection the selected container to move and target datanode
   * @return false if an exception occurred or the move completed with a
   * result other than ReplicationManager.MoveResult.COMPLETED. Returns true
   * if the move completed with MoveResult.COMPLETED or move is not yet done
   */
  private boolean moveContainer(DatanodeDetails source,
                                ContainerMoveSelection moveSelection) {
    ContainerID containerID = moveSelection.getContainerID();
    CompletableFuture<LegacyReplicationManager.MoveResult> future;
    try {
      ContainerInfo containerInfo = containerManager.getContainer(containerID);
      future = replicationManager
          .move(containerID, source, moveSelection.getTargetNode())
          .whenComplete((result, ex) -> {

            metrics.incrementCurrentIterationContainerMoveMetric(result, 1);
            if (ex != null) {
              LOG.info("Container move for container {} from source {} to " +
                      "target {} failed with exceptions {}",
                  containerID.toString(),
                  source.getUuidString(),
                  moveSelection.getTargetNode().getUuidString(), ex);
              metrics.incrementNumContainerMovesFailedInLatestIteration(1);
            } else {
              if (result == LegacyReplicationManager.MoveResult.COMPLETED) {
                sizeActuallyMovedInLatestIteration +=
                    containerInfo.getUsedBytes();
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Container move completed for container {} from " +
                          "source {} to target {}", containerID,
                      source.getUuidString(),
                      moveSelection.getTargetNode().getUuidString());
                }
              } else {
                LOG.warn(
                    "Container move for container {} from source {} to target" +
                        " {} failed: {}",
                    moveSelection.getContainerID(), source.getUuidString(),
                    moveSelection.getTargetNode().getUuidString(), result);
              }
            }
          });
    } catch (ContainerNotFoundException e) {
      LOG.warn("Could not find Container {} for container move",
          containerID, e);
      metrics.incrementNumContainerMovesFailedInLatestIteration(1);
      return false;
    } catch (NodeNotFoundException | TimeoutException e) {
      LOG.warn("Container move failed for container {}", containerID, e);
      metrics.incrementNumContainerMovesFailedInLatestIteration(1);
      return false;
    }

    if (future.isDone()) {
      if (future.isCompletedExceptionally()) {
        return false;
      } else {
        LegacyReplicationManager.MoveResult result = future.join();
        moveSelectionToFutureMap.put(moveSelection, future);
        return result == LegacyReplicationManager.MoveResult.COMPLETED;
      }
    } else {
      moveSelectionToFutureMap.put(moveSelection, future);
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
      ContainerMoveSelection moveSelection, DatanodeDetails source) {
    ContainerID containerID = moveSelection.getContainerID();
    DatanodeDetails target = moveSelection.getTargetNode();

    // count source if it has not been involved in move earlier
    if (!selectedSources.contains(source)) {
      countDatanodesInvolvedPerIteration += 1;
    }
    // count target if it has not been involved in move earlier
    if (!selectedTargets.contains(target)) {
      countDatanodesInvolvedPerIteration += 1;
    }

    incSizeSelectedForMoving(source, moveSelection);
    containerToSourceMap.put(containerID, source);
    containerToTargetMap.put(containerID, target);
    selectedTargets.add(target);
    selectedSources.add(source);
    selectionCriteria.setSelectedContainers(
        new HashSet<>(containerToSourceMap.keySet()));
  }

  /**
   * Calculates the number of used bytes given capacity and utilization ratio.
   *
   * @param nodeCapacity     capacity of the node.
   * @param utilizationRatio used space by capacity ratio of the node.
   * @return number of bytes
   */
  private long ratioToBytes(Long nodeCapacity, double utilizationRatio) {
    return (long) (nodeCapacity * utilizationRatio);
  }

  /**
   * Calculates the average utilization for the specified nodes.
   * Utilization is (capacity - remaining) divided by capacity.
   *
   * @param nodes List of DatanodeUsageInfo to find the average utilization for
   * @return Average utilization value
   */
  @VisibleForTesting
  double calculateAvgUtilization(List<DatanodeUsageInfo> nodes) {
    if (nodes.size() == 0) {
      LOG.warn("No nodes to calculate average utilization for in " +
          "ContainerBalancer.");
      return 0;
    }
    SCMNodeStat aggregatedStats = new SCMNodeStat(
        0, 0, 0);
    for (DatanodeUsageInfo node : nodes) {
      aggregatedStats.add(node.getScmNodeStat());
    }
    clusterCapacity = aggregatedStats.getCapacity().get();
    clusterRemaining = aggregatedStats.getRemaining().get();

    return (clusterCapacity - clusterRemaining) / (double) clusterCapacity;
  }

  /**
   * Get potential targets for container move. Potential targets are under
   * utilized and within threshold utilized nodes.
   *
   * @return A list of potential target DatanodeUsageInfo.
   */
  private List<DatanodeUsageInfo> getPotentialTargets() {
    //TODO(jacksonyao): take withinThresholdUtilizedNodes as candidate for both
    // source and target
    return underUtilizedNodes;
  }

  /**
   * Get potential sourecs for container move. Potential sourecs are over
   * utilized and within threshold utilized nodes.
   *
   * @return A list of potential source DatanodeUsageInfo.
   */
  private List<DatanodeUsageInfo> getPotentialSources() {
    //TODO(jacksonyao): take withinThresholdUtilizedNodes as candidate for both
    // source and target
    return overUtilizedNodes;
  }

  /**
   * Consults the configurations {@link ContainerBalancerTask#includeNodes} and
   * {@link ContainerBalancerTask#excludeNodes} to check if the specified
   * Datanode should be excluded from balancing.
   * @param datanode DatanodeDetails to check
   * @return true if Datanode should be excluded, else false
   */
  private boolean shouldExcludeDatanode(DatanodeDetails datanode) {
    if (excludeNodes.contains(datanode.getHostName()) ||
        excludeNodes.contains(datanode.getIpAddress())) {
      return true;
    } else if (!includeNodes.isEmpty()) {
      return !includeNodes.contains(datanode.getHostName()) &&
          !includeNodes.contains(datanode.getIpAddress());
    }
    return false;
  }

  /**
   * Updates conditions for balancing, such as total size moved by balancer,
   * total size that has entered a datanode, and total size that has left a
   * datanode in this iteration.
   *
   * @param source        source datanode
   * @param moveSelection selected target datanode and container
   */
  private void incSizeSelectedForMoving(DatanodeDetails source,
                                        ContainerMoveSelection moveSelection) {
    DatanodeDetails target = moveSelection.getTargetNode();
    ContainerInfo container;
    try {
      container =
          containerManager.getContainer(moveSelection.getContainerID());
    } catch (ContainerNotFoundException e) {
      LOG.warn("Could not find Container {} while matching source and " +
              "target nodes in ContainerBalancer",
          moveSelection.getContainerID(), e);
      return;
    }
    long size = container.getUsedBytes();
    sizeScheduledForMoveInLatestIteration += size;

    // update sizeLeavingNode map with the recent moveSelection
    findSourceStrategy.increaseSizeLeaving(source, size);

    // update sizeEnteringNode map with the recent moveSelection
    findTargetStrategy.increaseSizeEntering(target, size);
  }

  /**
   * Resets some variables and metrics for this iteration.
   */
  private void resetState() {
    this.clusterCapacity = 0L;
    this.clusterRemaining = 0L;
    this.overUtilizedNodes.clear();
    this.underUtilizedNodes.clear();
    this.unBalancedNodes.clear();
    this.containerToSourceMap.clear();
    this.containerToTargetMap.clear();
    this.selectedSources.clear();
    this.selectedTargets.clear();
    this.countDatanodesInvolvedPerIteration = 0;
    this.sizeScheduledForMoveInLatestIteration = 0;
    this.sizeActuallyMovedInLatestIteration = 0;
    metrics.resetDataSizeMovedGBInLatestIteration();
    metrics.resetNumContainerMovesCompletedInLatestIteration();
    metrics.resetNumContainerMovesTimeoutInLatestIteration();
    metrics.resetNumDatanodesInvolvedInLatestIteration();
    metrics.resetDataSizeUnbalancedGB();
    metrics.resetNumDatanodesUnbalanced();
    metrics.resetNumContainerMovesFailedInLatestIteration();
  }

  /**
   * Checks if ContainerBalancerTask is currently running.
   *
   * @return true if the status is RUNNING, otherwise false
   */
  private boolean isBalancerRunning() {
    return taskStatus == Status.RUNNING;
  }

  /**
   * Gets the list of unBalanced nodes, that is, the over and under utilized
   * nodes in the cluster.
   *
   * @return List of DatanodeUsageInfo containing unBalanced nodes.
   */
  @VisibleForTesting
  List<DatanodeUsageInfo> getUnBalancedNodes() {
    return unBalancedNodes;
  }

  /**
   * Gets a map with selected containers and their source datanodes.
   * @return map with mappings from {@link ContainerID} to
   * {@link DatanodeDetails}
   */
  @VisibleForTesting
  Map<ContainerID, DatanodeDetails> getContainerToSourceMap() {
    return containerToSourceMap;
  }

  /**
   * Gets a map with selected containers and target datanodes.
   * @return map with mappings from {@link ContainerID} to
   * {@link DatanodeDetails}.
   */
  @VisibleForTesting
  Map<ContainerID, DatanodeDetails> getContainerToTargetMap() {
    return containerToTargetMap;
  }

  @VisibleForTesting
  Set<DatanodeDetails> getSelectedTargets() {
    return selectedTargets;
  }

  @VisibleForTesting
  int getCountDatanodesInvolvedPerIteration() {
    return countDatanodesInvolvedPerIteration;
  }

  @VisibleForTesting
  public long getSizeScheduledForMoveInLatestIteration() {
    return sizeScheduledForMoveInLatestIteration;
  }

  public ContainerBalancerMetrics getMetrics() {
    return metrics;
  }

  @VisibleForTesting
  IterationResult getIterationResult() {
    return iterationResult;
  }

  @VisibleForTesting
  void setConfig(ContainerBalancerConfiguration config) {
    this.config = config;
  }

  public Status getBalancerStatus() {
    return taskStatus;
  }

  @Override
  public String toString() {
    String status = String.format("%nContainer Balancer Task status:%n" +
        "%-30s %s%n" +
        "%-30s %b%n", "Key", "Value", "Running", isBalancerRunning());
    return status + config.toString();
  }

  /**
   * The result of {@link ContainerBalancerTask#doIteration()}.
   */
  enum IterationResult {
    ITERATION_COMPLETED,
    ITERATION_INTERRUPTED,
    CAN_NOT_BALANCE_ANY_MORE
  }

  /**
   * The status of {@link ContainerBalancerTask}.
   */
  enum Status {
    RUNNING,
    STOPPING,
    STOPPED
  }
}
