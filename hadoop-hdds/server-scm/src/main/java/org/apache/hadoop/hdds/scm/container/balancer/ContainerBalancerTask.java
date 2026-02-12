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

package org.apache.hadoop.hdds.scm.container.balancer;

import static java.time.OffsetDateTime.now;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL_DEFAULT;
import static org.apache.hadoop.util.StringUtils.byteDesc;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.scm.PlacementPolicyValidateProxy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaNotFoundException;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container balancer task performs move of containers between over- and
 * under-utilized datanodes.
 */
public class ContainerBalancerTask implements Runnable {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerBalancerTask.class);
  public static final long ABSENCE_OF_DURATION = -1L;

  private NodeManager nodeManager;
  private ContainerManager containerManager;
  private ReplicationManager replicationManager;
  private MoveManager moveManager;
  private OzoneConfiguration ozoneConfiguration;
  private ContainerBalancer containerBalancer;
  private final SCMContext scmContext;
  private int totalNodesInCluster;
  private double maxDatanodesRatioToInvolvePerIteration;
  private long maxSizeToMovePerIteration;
  private int countDatanodesInvolvedPerIteration;
  private long sizeScheduledForMoveInLatestIteration;
  // count actual size moved in bytes
  private long sizeActuallyMovedInLatestIteration;
  private final List<DatanodeUsageInfo> overUtilizedNodes;
  private final List<DatanodeUsageInfo> underUtilizedNodes;
  private List<DatanodeUsageInfo> withinThresholdUtilizedNodes;
  private Set<String> excludeNodes;
  private Set<String> includeNodes;
  private ContainerBalancerConfiguration config;
  private ContainerBalancerMetrics metrics;
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
  private Map<ContainerMoveSelection, CompletableFuture<MoveManager.MoveResult>>
      moveSelectionToFutureMap;
  private IterationResult iterationResult;
  private int nextIterationIndex;
  private boolean delayStart;
  private Queue<ContainerBalancerTaskIterationStatusInfo> iterationsStatistic;
  private OffsetDateTime currentIterationStarted;
  private AtomicBoolean isCurrentIterationInProgress = new AtomicBoolean(false);

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
                               ContainerBalancerConfiguration config,
                               boolean delayStart) {
    this.nodeManager = scm.getScmNodeManager();
    this.containerManager = scm.getContainerManager();
    this.replicationManager = scm.getReplicationManager();
    this.moveManager = scm.getMoveManager();
    this.moveManager.setMoveTimeout(config.getMoveTimeout().toMillis());
    this.moveManager.setReplicationTimeout(
        config.getMoveReplicationTimeout().toMillis());
    this.delayStart = delayStart;
    this.ozoneConfiguration = scm.getConfiguration();
    this.containerBalancer = containerBalancer;
    this.config = config;
    this.metrics = metrics;
    this.scmContext = scm.getScmContext();
    this.overUtilizedNodes = new ArrayList<>();
    this.underUtilizedNodes = new ArrayList<>();
    this.withinThresholdUtilizedNodes = new ArrayList<>();
    PlacementPolicyValidateProxy placementPolicyValidateProxy = scm.getPlacementPolicyValidateProxy();
    NetworkTopology networkTopology = scm.getClusterMap();
    this.nextIterationIndex = nextIterationIndex;
    this.containerToSourceMap = new HashMap<>();
    this.containerToTargetMap = new HashMap<>();
    this.selectedSources = new HashSet<>();
    this.selectedTargets = new HashSet<>();
    findSourceStrategy = new FindSourceGreedy(nodeManager);
    if (config.getNetworkTopologyEnable()) {
      findTargetStrategy = new FindTargetGreedyByNetworkTopology(
          containerManager, placementPolicyValidateProxy,
          nodeManager, networkTopology);
    } else {
      findTargetStrategy = new FindTargetGreedyByUsageInfo(containerManager,
          placementPolicyValidateProxy, nodeManager);
    }
    this.iterationsStatistic = new ConcurrentLinkedQueue<>();
  }

  /**
   * Run the container balancer task.
   */
  @Override
  public void run() {
    try {
      if (delayStart) {
        long delayDuration = ozoneConfiguration.getTimeDuration(
            HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
            HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT_DEFAULT,
            TimeUnit.SECONDS);
        LOG.info("ContainerBalancer will sleep for {} seconds before starting" +
            " balancing.", delayDuration);
        Thread.sleep(Duration.ofSeconds(delayDuration).toMillis());
      }
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
    int iterations = config.getIterations();
    if (iterations == -1) {
      //run balancer infinitely
      iterations = Integer.MAX_VALUE;
    }

    // nextIterationIndex is the iteration that balancer should start from on
    // leader change or restart
    int i = nextIterationIndex;
    for (; i < iterations && isBalancerRunning(); i++) {
      currentIterationStarted = now();

      isCurrentIterationInProgress.compareAndSet(false, true);

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
          long sleepTime = 3 * nodeReportInterval;
          LOG.info("ContainerBalancer will sleep for {} ms while waiting " +
              "for updated usage information from Datanodes.", sleepTime);
          Thread.sleep(sleepTime);
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

      IterationResult currentIterationResult = doIteration();
      ContainerBalancerTaskIterationStatusInfo iterationStatistic =
          getIterationStatistic(i + 1, currentIterationResult, getCurrentIterationDuration());
      iterationsStatistic.offer(iterationStatistic);

      isCurrentIterationInProgress.compareAndSet(true, false);

      findTargetStrategy.clearSizeEnteringNodes();
      findSourceStrategy.clearSizeLeavingNodes();

      metrics.incrementNumIterations(1);

      LOG.info("Result of this iteration of Container Balancer: {}", currentIterationResult);

      // if no new move option is generated, it means the cluster cannot be
      // balanced anymore; so just stop balancer
      if (currentIterationResult == IterationResult.CAN_NOT_BALANCE_ANY_MORE) {
        tryStopWithSaveConfiguration(currentIterationResult.toString());
        return;
      }

      // persist next iteration index
      if (currentIterationResult == IterationResult.ITERATION_COMPLETED) {
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

  private ContainerBalancerTaskIterationStatusInfo getIterationStatistic(Integer iterationNumber,
                                                                         IterationResult currentIterationResult,
                                                                         long iterationDuration) {
    String currentIterationResultName = currentIterationResult == null ? null : currentIterationResult.name();
    Map<DatanodeID, Long> sizeEnteringDataToNodes =
        convertToNodeIdToTrafficMap(findTargetStrategy.getSizeEnteringNodes());
    Map<DatanodeID, Long> sizeLeavingDataFromNodes =
        convertToNodeIdToTrafficMap(findSourceStrategy.getSizeLeavingNodes());
    IterationInfo iterationInfo = new IterationInfo(
        iterationNumber,
        currentIterationResultName,
        iterationDuration
    );
    ContainerMoveInfo containerMoveInfo = new ContainerMoveInfo(metrics);

    DataMoveInfo dataMoveInfo =
        getDataMoveInfo(currentIterationResultName, sizeEnteringDataToNodes, sizeLeavingDataFromNodes);
    return new ContainerBalancerTaskIterationStatusInfo(iterationInfo, containerMoveInfo, dataMoveInfo);
  }

  private DataMoveInfo getDataMoveInfo(String currentIterationResultName, Map<DatanodeID, Long> sizeEnteringDataToNodes,
                                       Map<DatanodeID, Long> sizeLeavingDataFromNodes) {
    if (currentIterationResultName == null) {
      // For unfinished iteration
      return new DataMoveInfo(
          getSizeScheduledForMoveInLatestIteration(),
          sizeActuallyMovedInLatestIteration,
          sizeEnteringDataToNodes,
          sizeLeavingDataFromNodes
      );
    } else {
      // For finished iteration
      return new DataMoveInfo(
          getSizeScheduledForMoveInLatestIteration(),
          metrics.getDataSizeMovedInLatestIteration(),
          sizeEnteringDataToNodes,
          sizeLeavingDataFromNodes
      );
    }
  }

  private Map<DatanodeID, Long> convertToNodeIdToTrafficMap(Map<DatanodeDetails, Long> nodeTrafficMap) {
    return nodeTrafficMap
        .entrySet()
        .stream()
        .filter(Objects::nonNull)
        .filter(datanodeDetailsLongEntry -> datanodeDetailsLongEntry.getValue() > 0)
        .collect(
            Collectors.toMap(
                entry -> entry.getKey().getID(),
                Map.Entry::getValue
            )
        );
  }

  /**
   * Get current iteration statistics.
   * @return current iteration statistic
   */
  public List<ContainerBalancerTaskIterationStatusInfo> getCurrentIterationsStatistic() {
    List<ContainerBalancerTaskIterationStatusInfo> resultList = new ArrayList<>(iterationsStatistic);
    ContainerBalancerTaskIterationStatusInfo currentIterationStatistic = createCurrentIterationStatistic();
    if (currentIterationStatistic != null) {
      resultList.add(currentIterationStatistic);
    }
    return resultList;
  }

  private ContainerBalancerTaskIterationStatusInfo createCurrentIterationStatistic() {
    List<ContainerBalancerTaskIterationStatusInfo> resultList = new ArrayList<>(iterationsStatistic);

    int lastIterationNumber = resultList.stream()
        .mapToInt(ContainerBalancerTaskIterationStatusInfo::getIterationNumber)
        .max()
        .orElse(0);
    long iterationDuration = getCurrentIterationDuration();

    if (isCurrentIterationInProgress.get()) {
      return getIterationStatistic(lastIterationNumber + 1, null, iterationDuration);
    } else {
      return null;
    }
  }

  private long getCurrentIterationDuration() {
    if (currentIterationStarted == null) {
      return ABSENCE_OF_DURATION;
    } else {
      return now().toEpochSecond() - currentIterationStarted.toEpochSecond();
    }
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

    this.maxDatanodesRatioToInvolvePerIteration =
        config.getMaxDatanodesRatioToInvolvePerIteration();
    this.maxSizeToMovePerIteration = config.getMaxSizeToMovePerIteration();

    this.excludeNodes = config.getExcludeNodes();
    this.includeNodes = config.getIncludeNodes();
    // include/exclude nodes from balancing according to configs
    datanodeUsageInfos.removeIf(datanodeUsageInfo -> shouldExcludeDatanode(
        datanodeUsageInfo.getDatanodeDetails()));

    this.totalNodesInCluster = datanodeUsageInfos.size();

    double clusterAvgUtilisation = calculateAvgUtilization(datanodeUsageInfos);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Average utilization of the cluster is {}", clusterAvgUtilisation);
    }

    double threshold = config.getThresholdAsRatio();
    // over utilized nodes have utilization(that is, used / capacity) greater than upper limit
    this.upperLimit = clusterAvgUtilisation + threshold;
    // under utilized nodes have utilization(that is, used / capacity) less than lower limit
    this.lowerLimit = clusterAvgUtilisation - threshold;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Lower limit for utilization is {} and Upper limit for utilization is {}", lowerLimit, upperLimit);
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
            datanodeUsageInfo.getDatanodeDetails(),
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

    if (overUtilizedNodes.isEmpty() && underUtilizedNodes.isEmpty()) {
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
            entry.getDatanodeDetails().getID());
      });

      underUtilizedNodes.forEach(entry -> {
        LOG.debug("Datanode {} {} is Under-Utilized.",
            entry.getDatanodeDetails().getHostName(),
            entry.getDatanodeDetails().getID());
      });
    }

    selectionCriteria = new ContainerBalancerSelectionCriteria(config,
        nodeManager, replicationManager, containerManager, findSourceStrategy,
        containerToSourceMap);
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

    moveSelectionToFutureMap = new ConcurrentHashMap<>();
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
      // add source back to queue as a different container can be selected in next run.
      findSourceStrategy.addBackSourceDataNode(source);
      // exclude the container which caused failure of move to avoid error in next run.
      selectionCriteria.addToExcludeDueToFailContainers(moveSelection.getContainerID());
      return false;
    }

    ContainerInfo containerInfo;
    try {
      containerInfo =
          containerManager.getContainer(containerID);
    } catch (ContainerNotFoundException e) {
      LOG.warn("Could not get container {} from Container Manager before " +
          "starting a container move", containerID, e);
      // add source back to queue as a different container can be selected in next run.
      findSourceStrategy.addBackSourceDataNode(source);
      // exclude the container which caused failure of move to avoid error in next run.
      selectionCriteria.addToExcludeDueToFailContainers(moveSelection.getContainerID());
      return false;
    }
    LOG.info("ContainerBalancer is trying to move container {} with size " +
            "{}B from source datanode {} to target datanode {}",
        containerID.toString(),
        containerInfo.getUsedBytes(),
        source,
        moveSelection.getTargetNode());

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
    Collection<CompletableFuture<MoveManager.MoveResult>> futures =
        moveSelectionToFutureMap.values();
    if (!futures.isEmpty()) {
      CompletableFuture<Void> allFuturesResult = CompletableFuture.allOf(
          futures.toArray(new CompletableFuture[0]));
      try {
        allFuturesResult.get(config.getMoveTimeout().toMillis(),
            TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Container balancer is interrupted");
        Thread.currentThread().interrupt();
      } catch (TimeoutException e) {
        long timeoutCounts = cancelMovesThatExceedTimeoutDuration();
        LOG.warn("{} Container moves are canceled.", timeoutCounts);
        metrics.incrementNumContainerMovesTimeoutInLatestIteration(
            timeoutCounts);
      } catch (ExecutionException e) {
        LOG.error("Got exception while checkIterationMoveResults", e);
      }
    }

    countDatanodesInvolvedPerIteration = selectedSources.size() + selectedTargets.size();

    metrics.incrementNumDatanodesInvolvedInLatestIteration(countDatanodesInvolvedPerIteration);

    metrics.incrementNumContainerMovesScheduled(metrics.getNumContainerMovesScheduledInLatestIteration());

    metrics.incrementNumContainerMovesCompleted(metrics.getNumContainerMovesCompletedInLatestIteration());

    metrics.incrementNumContainerMovesTimeout(metrics.getNumContainerMovesTimeoutInLatestIteration());

    metrics.incrementDataSizeMovedGBInLatestIteration(sizeActuallyMovedInLatestIteration / OzoneConsts.GB);

    metrics.incrementDataSizeMovedInLatestIteration(sizeActuallyMovedInLatestIteration);

    metrics.incrementDataSizeMovedGB(metrics.getDataSizeMovedGBInLatestIteration());

    metrics.incrementNumContainerMovesFailed(metrics.getNumContainerMovesFailedInLatestIteration());

    LOG.info("Iteration Summary. Number of Datanodes involved: {}. Size " +
            "moved: {} ({} Bytes). Number of Container moves completed: {}.",
        countDatanodesInvolvedPerIteration,
        byteDesc(sizeActuallyMovedInLatestIteration),
        sizeActuallyMovedInLatestIteration,
        metrics.getNumContainerMovesCompletedInLatestIteration());
  }

  /**
   * Cancels container moves that are not yet done. Note that if a move
   * command has already been sent out to a Datanode, we don't yet have the
   * capability to cancel it. However, those commands in the DN should time out
   * if they haven't been processed yet.
   *
   * @return number of moves that did not complete (timed out) and were
   * cancelled.
   */
  private long cancelMovesThatExceedTimeoutDuration() {
    Set<Map.Entry<ContainerMoveSelection,
        CompletableFuture<MoveManager.MoveResult>>>
        entries = moveSelectionToFutureMap.entrySet();
    Iterator<Map.Entry<ContainerMoveSelection,
        CompletableFuture<MoveManager.MoveResult>>>
        iterator = entries.iterator();

    int numCancelled = 0;
    // iterate through all moves and cancel ones that aren't done yet
    while (iterator.hasNext()) {
      Map.Entry<ContainerMoveSelection,
          CompletableFuture<MoveManager.MoveResult>>
          entry = iterator.next();
      if (!entry.getValue().isDone()) {
        LOG.warn("Container move timed out for container {} from source {}" +
                " to target {}.", entry.getKey().getContainerID(),
            containerToSourceMap.get(entry.getKey().getContainerID()),
            entry.getKey().getTargetNode());

        entry.getValue().cancel(true);
        numCancelled += 1;
      }
    }

    return numCancelled;
  }

  /**
   * Match a source datanode with a target datanode and identify the container
   * to move.
   *
   * @return ContainerMoveSelection containing the selected target and container
   */
  private ContainerMoveSelection matchSourceWithTarget(DatanodeDetails source) {
    Set<ContainerID> sourceContainerIDSet =
        selectionCriteria.getContainerIDSet(source);

    if (sourceContainerIDSet.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ContainerBalancer could not find any candidate containers " +
            "for datanode {}", source);
      }
      return null;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("ContainerBalancer is finding suitable target for source " +
          "datanode {}", source);
    }

    ContainerMoveSelection moveSelection = null;
    Set<ContainerID> toRemoveContainerIds = new HashSet<>();
    for (ContainerID containerId: sourceContainerIDSet) {
      if (selectionCriteria.shouldBeExcluded(containerId, source,
          sizeScheduledForMoveInLatestIteration)) {
        toRemoveContainerIds.add(containerId);
        continue;
      }
      moveSelection = findTargetStrategy.findTargetForContainerMove(source,
          containerId);
      if (moveSelection != null) {
        break;
      }
    }
    // Update cached containerIDSet in setMap
    sourceContainerIDSet.removeAll(toRemoveContainerIds);

    if (moveSelection == null) {
      LOG.info("ContainerBalancer could not find a suitable target for " +
          "source node {}.", source);
      return null;
    }
    LOG.info("ContainerBalancer matched source datanode {} with target " +
            "datanode {} for container move.", source, moveSelection.getTargetNode());

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
   * Asks {@link ReplicationManager} or {@link MoveManager} to move the
   * specified container from source to target.
   *
   * @param source the source datanode
   * @param moveSelection the selected container to move and target datanode
   * @return false if an exception occurred or the move completed with a
   * result other than MoveManager.MoveResult.COMPLETED. Returns true
   * if the move completed with MoveResult.COMPLETED or move is not yet done
   */
  private boolean moveContainer(DatanodeDetails source,
                                ContainerMoveSelection moveSelection) {
    ContainerID containerID = moveSelection.getContainerID();
    CompletableFuture<MoveManager.MoveResult> future;
    try {
      ContainerInfo containerInfo = containerManager.getContainer(containerID);
      future = moveManager.move(containerID, source, moveSelection.getTargetNode());

      metrics.incrementNumContainerMovesScheduledInLatestIteration(1);

      future = future.whenComplete((result, ex) -> {
        metrics.incrementCurrentIterationContainerMoveMetric(result, 1);
        moveSelectionToFutureMap.remove(moveSelection);
        if (ex != null) {
          LOG.info("Container move for container {} from source {} to " +
                  "target {} failed with exceptions.",
              containerID, source,
              moveSelection.getTargetNode(), ex);
          metrics.incrementNumContainerMovesFailedInLatestIteration(1);
        } else {
          if (result == MoveManager.MoveResult.COMPLETED) {
            sizeActuallyMovedInLatestIteration +=
                containerInfo.getUsedBytes();
            LOG.debug("Container move completed for container {} from " +
                    "source {} to target {}", containerID, source,
                moveSelection.getTargetNode());
          } else {
            LOG.warn(
                "Container move for container {} from source {} to target" +
                    " {} failed: {}",
                moveSelection.getContainerID(), source,
                moveSelection.getTargetNode(), result);
          }
        }
      });
    } catch (ContainerNotFoundException e) {
      LOG.warn("Could not find Container {} for container move",
          containerID, e);
      // add source back to queue as a different container can be selected in next run.
      findSourceStrategy.addBackSourceDataNode(source);
      // exclude the container which caused failure of move to avoid error in next run.
      selectionCriteria.addToExcludeDueToFailContainers(moveSelection.getContainerID());
      metrics.incrementNumContainerMovesFailedInLatestIteration(1);
      return false;
    } catch (NodeNotFoundException e) {
      LOG.warn("Container move failed for container {}", containerID, e);
      metrics.incrementNumContainerMovesFailedInLatestIteration(1);
      return false;
    } catch (ContainerReplicaNotFoundException e) {
      LOG.warn("Container move failed for container {}", containerID, e);
      metrics.incrementNumContainerMovesFailedInLatestIteration(1);
      // add source back to queue for replica not found only
      // the container is not excluded as it is a replica related failure
      findSourceStrategy.addBackSourceDataNode(source);
      return false;
    }

    /*
    If the future hasn't failed yet, put it in moveSelectionToFutureMap for
    processing later
     */
    if (future.isDone()) {
      if (future.isCompletedExceptionally()) {
        return false;
      } else {
        MoveManager.MoveResult result = future.join();
        if (result == MoveManager.MoveResult.REPLICATION_FAIL_NOT_EXIST_IN_SOURCE ||
            result == MoveManager.MoveResult.REPLICATION_FAIL_EXIST_IN_TARGET ||
            result == MoveManager.MoveResult.REPLICATION_FAIL_CONTAINER_NOT_CLOSED ||
            result == MoveManager.MoveResult.REPLICATION_FAIL_INFLIGHT_DELETION ||
            result == MoveManager.MoveResult.REPLICATION_FAIL_INFLIGHT_REPLICATION) {
          // add source back to queue as a different container can be selected in next run.
          // the container which caused failure of move is not excluded
          // as it is an intermittent failure or a replica related failure
          findSourceStrategy.addBackSourceDataNode(source);
        }
        return result == MoveManager.MoveResult.COMPLETED;
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
  public static double calculateAvgUtilization(List<DatanodeUsageInfo> nodes) {
    if (nodes.isEmpty()) {
      LOG.warn("No nodes to calculate average utilization for in " +
          "ContainerBalancer.");
      return 0;
    }
    SCMNodeStat aggregatedStats = new SCMNodeStat(
        0, 0, 0, 0, 0, 0);
    for (DatanodeUsageInfo node : nodes) {
      aggregatedStats.add(node.getScmNodeStat());
    }
    long clusterCapacity = aggregatedStats.getCapacity().get();
    long clusterRemaining = aggregatedStats.getRemaining().get();

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
    moveManager.resetState();
    this.overUtilizedNodes.clear();
    this.underUtilizedNodes.clear();
    this.containerToSourceMap.clear();
    this.containerToTargetMap.clear();
    this.selectedSources.clear();
    this.selectedTargets.clear();
    this.countDatanodesInvolvedPerIteration = 0;
    this.sizeScheduledForMoveInLatestIteration = 0;
    this.sizeActuallyMovedInLatestIteration = 0;
    metrics.resetDataSizeMovedGBInLatestIteration();
    metrics.resetDataSizeMovedInLatestIteration();
    metrics.resetNumContainerMovesScheduledInLatestIteration();
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

  @VisibleForTesting
  public List<DatanodeUsageInfo> getOverUtilizedNodes() {
    return overUtilizedNodes;
  }

  @VisibleForTesting
  public List<DatanodeUsageInfo> getUnderUtilizedNodes() {
    return underUtilizedNodes;
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
  Set<DatanodeDetails> getSelectedSources() {
    return selectedSources;
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

  ContainerBalancerConfiguration getConfig() {
    return config;
  }

  @VisibleForTesting
  void setConfig(ContainerBalancerConfiguration config) {
    this.config = config;
  }

  @VisibleForTesting
  void setTaskStatus(Status taskStatus) {
    this.taskStatus = taskStatus;
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
