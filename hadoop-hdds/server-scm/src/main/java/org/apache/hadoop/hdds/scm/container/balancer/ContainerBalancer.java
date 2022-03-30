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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.fs.DUFactory;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL_DEFAULT;

/**
 * Container balancer is a service in SCM to move containers between over- and
 * under-utilized datanodes.
 */
public class ContainerBalancer implements SCMService {

  public static final Logger LOG =
      LoggerFactory.getLogger(ContainerBalancer.class);

  private NodeManager nodeManager;
  private ContainerManager containerManager;
  private ReplicationManager replicationManager;
  private OzoneConfiguration ozoneConfiguration;
  private final SCMContext scmContext;
  private double threshold;
  private int totalNodesInCluster;
  private double maxDatanodesRatioToInvolvePerIteration;
  private long maxSizeToMovePerIteration;
  private int countDatanodesInvolvedPerIteration;
  private long sizeMovedPerIteration;
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
  private long clusterUsed;
  private long clusterRemaining;
  private double clusterAvgUtilisation;
  private PlacementPolicy placementPolicy;
  private NetworkTopology networkTopology;
  private double upperLimit;
  private double lowerLimit;
  private volatile boolean balancerRunning;
  private volatile Thread currentBalancingThread;
  private Lock lock;
  private ContainerBalancerSelectionCriteria selectionCriteria;
  private Map<DatanodeDetails, ContainerMoveSelection> sourceToTargetMap;
  private Set<ContainerID> selectedContainers;
  private FindTargetStrategy findTargetStrategy;
  private FindSourceStrategy findSourceStrategy;
  private Map<ContainerMoveSelection,
      CompletableFuture<ReplicationManager.MoveResult>>
      moveSelectionToFutureMap;

  /**
   * Constructs ContainerBalancer with the specified arguments. Initializes
   * new ContainerBalancerConfiguration and ContainerBalancerMetrics.
   * Container Balancer does not start on construction.
   *
   * @param scm the storage container manager
   */
  public ContainerBalancer(StorageContainerManager scm) {
    this.nodeManager = scm.getScmNodeManager();
    this.containerManager = scm.getContainerManager();
    this.replicationManager = scm.getReplicationManager();
    this.ozoneConfiguration = scm.getConfiguration();
    this.config = ozoneConfiguration.getObject(
        ContainerBalancerConfiguration.class);
    this.metrics = ContainerBalancerMetrics.create();
    this.scmContext = scm.getScmContext();
    this.selectedContainers = new HashSet<>();
    this.overUtilizedNodes = new ArrayList<>();
    this.underUtilizedNodes = new ArrayList<>();
    this.withinThresholdUtilizedNodes = new ArrayList<>();
    this.unBalancedNodes = new ArrayList<>();
    this.placementPolicy = scm.getContainerPlacementPolicy();
    this.networkTopology = scm.getClusterMap();

    this.lock = new ReentrantLock();
    findSourceStrategy = new FindSourceGreedy(nodeManager);
  }

  /**
   * Balances the cluster.
   */
  private void balance() {
    this.iterations = config.getIterations();
    if (this.iterations == -1) {
      //run balancer infinitely
      this.iterations = Integer.MAX_VALUE;
    }

    for (int i = 0; i < iterations && balancerRunning; i++) {
      if (config.getTriggerDuEnable()) {
        // before starting a new iteration, we trigger all the datanode
        // to run `du`. this is an aggressive action, with which we can
        // get more precise usage info of all datanodes before moving.
        // this is helpful for container balancer to make more appropriate
        // decisions. this will increase the disk io load of data nodes, so
        // please enable it with caution.
        nodeManager.refreshAllHealthyDnUsageInfo();
        synchronized (this) {
          try {
            long nodeReportInterval =
                ozoneConfiguration.getTimeDuration(HDDS_NODE_REPORT_INTERVAL,
                HDDS_NODE_REPORT_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
            // one for sending command , one for running du, and one for
            // reporting back make it like this for now, a more suitable
            // value. can be set in the future if needed
            wait(3 * nodeReportInterval);
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
      }

      // stop balancing if iteration is not initialized
      if (!initializeIteration()) {
        stopBalancer();
        return;
      }

      //if no new move option is generated, it means the cluster can
      //not be balanced any more , so just stop
      IterationResult iR = doIteration();
      metrics.incrementNumIterations(1);
      LOG.info("Result of this iteration of Container Balancer: {}", iR);
      if (iR == IterationResult.CAN_NOT_BALANCE_ANY_MORE) {
        stopBalancer();
        return;
      }

      // return if balancing has been stopped
      if (!isBalancerRunning()) {
        return;
      }

      // wait for configured time before starting next iteration, unless
      // this was the final iteration
      if (i != iterations - 1) {
        synchronized (this) {
          try {
            wait(config.getBalancingInterval().toMillis());
          } catch (InterruptedException e) {
            LOG.info("Container Balancer was interrupted while waiting for" +
                " next iteration.");
            Thread.currentThread().interrupt();
            return;
          }
        }
      }
    }
    stopBalancer();
  }

  /**
   * Initializes an iteration during balancing. Recognizes over, under, and
   * within threshold utilized nodes. Decides whether balancing needs to
   * continue or should be stopped.
   *
   * @return true if successfully initialized, otherwise false.
   */
  private boolean initializeIteration() {
    if (scmContext.isInSafeMode()) {
      LOG.error("Container Balancer cannot operate while SCM is in Safe Mode.");
      return false;
    }
    if (!scmContext.isLeader()) {
      LOG.warn("Current SCM is not the leader.");
      return false;
    }
    // sorted list in order from most to least used
    List<DatanodeUsageInfo> datanodeUsageInfos =
        nodeManager.getMostOrLeastUsedDatanodes(true);
    if (datanodeUsageInfos.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Container Balancer could not retrieve nodes from Node " +
            "Manager.");
      }
      return false;
    }
    this.threshold = config.getThresholdAsRatio();
    this.maxDatanodesRatioToInvolvePerIteration =
        config.getMaxDatanodesRatioToInvolvePerIteration();
    this.maxSizeToMovePerIteration = config.getMaxSizeToMovePerIteration();
    if (config.getNetworkTopologyEnable()) {
      findTargetStrategy = new FindTargetGreedyByNetworkTopology(
          containerManager, placementPolicy, nodeManager, networkTopology);
    } else {
      findTargetStrategy = new FindTargetGreedyByUsageInfo(containerManager,
          placementPolicy, nodeManager);
    }
    this.excludeNodes = config.getExcludeNodes();
    this.includeNodes = config.getIncludeNodes();
    // include/exclude nodes from balancing according to configs
    datanodeUsageInfos.removeIf(datanodeUsageInfo -> shouldExcludeDatanode(
        datanodeUsageInfo.getDatanodeDetails()));

    this.totalNodesInCluster = datanodeUsageInfos.size();

    // reset some variables and metrics for this iteration
    resetState();

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
        Long overUtilizedBytes = ratioToBytes(
            datanodeUsageInfo.getScmNodeStat().getCapacity().get(),
            utilization) - ratioToBytes(
            datanodeUsageInfo.getScmNodeStat().getCapacity().get(),
            upperLimit);
        totalOverUtilizedBytes += overUtilizedBytes;
      } else if (Double.compare(utilization, lowerLimit) < 0) {
        underUtilizedNodes.add(datanodeUsageInfo);
        metrics.incrementNumDatanodesUnbalanced(1);

        // amount of bytes lesser than lower limit in this node
        Long underUtilizedBytes = ratioToBytes(
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
    sourceToTargetMap = new HashMap<>(overUtilizedNodes.size() +
        withinThresholdUtilizedNodes.size());
    return true;
  }

  private IterationResult doIteration() {
    // note that potential and selected targets are updated in the following
    // loop
    //TODO(jacksonyao): take withinThresholdUtilizedNodes as candidate for both
    // source and target
    findSourceStrategy.reInitialize(getPotentialSources(), config, lowerLimit);
    List<DatanodeUsageInfo> potentialTargets = getPotentialTargets();
    findTargetStrategy.reInitialize(potentialTargets, config, upperLimit);

    Set<DatanodeDetails> selectedTargets =
        new HashSet<>(potentialTargets.size());
    moveSelectionToFutureMap = new HashMap<>(unBalancedNodes.size());
    boolean isMoveGenerated = false;
    try {
      // match each overUtilized node with a target
      while (true) {
        if (!isBalancerRunning()) {
          return IterationResult.ITERATION_INTERRUPTED;
        }

        IterationResult result = checkConditionsForBalancing();
        if (result != null) {
          return result;
        }

        DatanodeDetails source =
            findSourceStrategy.getNextCandidateSourceDataNode();
        if (source == null) {
          break;
        }

        ContainerMoveSelection moveSelection = matchSourceWithTarget(source);
        if (moveSelection != null) {
          isMoveGenerated = true;
          LOG.info("ContainerBalancer is trying to move container {} from " +
                  "source datanode {} to target datanode {}",
              moveSelection.getContainerID().toString(),
              source.getUuidString(),
              moveSelection.getTargetNode().getUuidString());

          if (moveContainer(source, moveSelection)) {
            // consider move successful for now, and update selection criteria
            updateTargetsAndSelectionCriteria(
                selectedTargets, moveSelection, source);
          }
        } else {
          // can not find any target for this source
          findSourceStrategy.removeCandidateSourceDataNode(source);
        }
      }

      if (!isMoveGenerated) {
        //no move option is generated, so the cluster can not be
        //balanced any more, just stop iteration and exit
        return IterationResult.CAN_NOT_BALANCE_ANY_MORE;
      }
      return IterationResult.ITERATION_COMPLETED;
    } finally {
      checkIterationMoveResults(selectedTargets);
    }
  }

  /**
   * Checks the results of all move operations when exiting an iteration.
   * @param selectedTargets Set of target datanodes that were selected in
   *                        current iteration
   */
  private void checkIterationMoveResults(Set<DatanodeDetails> selectedTargets) {
    this.countDatanodesInvolvedPerIteration = 0;
    this.sizeMovedPerIteration = 0;
    for (Map.Entry<ContainerMoveSelection,
            CompletableFuture<ReplicationManager.MoveResult>>
        futureEntry : moveSelectionToFutureMap.entrySet()) {
      ContainerMoveSelection moveSelection = futureEntry.getKey();
      CompletableFuture<ReplicationManager.MoveResult> future =
          futureEntry.getValue();
      try {
        ReplicationManager.MoveResult result = future.get(
            config.getMoveTimeout().toMillis(), TimeUnit.MILLISECONDS);
        if (result == ReplicationManager.MoveResult.COMPLETED) {
          try {
            ContainerInfo container =
                containerManager.getContainer(moveSelection.getContainerID());
            this.sizeMovedPerIteration += container.getUsedBytes();
            metrics.incrementNumMovedContainersInLatestIteration(1);
            LOG.info("Move completed for container {} to target {}",
                container.containerID(),
                moveSelection.getTargetNode().getUuidString());
          } catch (ContainerNotFoundException e) {
            LOG.warn("Could not find Container {} while " +
                    "checking move results in ContainerBalancer",
                moveSelection.getContainerID(), e);
          }
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for container move result for " +
                "container {}.",
            moveSelection.getContainerID(), e);
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        LOG.warn("Container move for container {} completed exceptionally.",
            moveSelection.getContainerID(), e);
      } catch (TimeoutException e) {
        LOG.warn("Container move for container {} timed out.",
            moveSelection.getContainerID(), e);
      }
    }
    countDatanodesInvolvedPerIteration =
        sourceToTargetMap.size() + selectedTargets.size();
    metrics.incrementNumDatanodesInvolvedInLatestIteration(
        countDatanodesInvolvedPerIteration);
    metrics.incrementDataSizeMovedGBInLatestIteration(
        sizeMovedPerIteration / OzoneConsts.GB);
    LOG.info("Number of datanodes involved in this iteration: {}. Size moved " +
            "in this iteration: {}B.",
        countDatanodesInvolvedPerIteration, sizeMovedPerIteration);
  }

  /**
   * Match a source datanode with a target datanode and identify the container
   * to move.
   *
   * @return ContainerMoveSelection containing the selected target and container
   */
  private ContainerMoveSelection matchSourceWithTarget(DatanodeDetails source) {
    NavigableSet<ContainerID> candidateContainers =
        selectionCriteria.getCandidateContainers(source);

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

  /**
   * Checks if limits maxDatanodesPercentageToInvolvePerIteration and
   * maxSizeToMovePerIteration have not been hit.
   *
   * @return {@link IterationResult#MAX_DATANODES_TO_INVOLVE_REACHED} if reached
   * max datanodes to involve limit,
   * {@link IterationResult#MAX_SIZE_TO_MOVE_REACHED} if reached max size to
   * move limit, or null if balancing can continue
   */
  private IterationResult checkConditionsForBalancing() {
    int maxDatanodesToInvolve =
        (int) (maxDatanodesRatioToInvolvePerIteration * totalNodesInCluster);
    if (countDatanodesInvolvedPerIteration + 2 > maxDatanodesToInvolve) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Hit max datanodes to involve limit. {} datanodes have" +
                " already been involved and the limit is {}.",
            countDatanodesInvolvedPerIteration, maxDatanodesToInvolve);
      }
      return IterationResult.MAX_DATANODES_TO_INVOLVE_REACHED;
    }
    if (sizeMovedPerIteration + (long) ozoneConfiguration.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT,
        StorageUnit.BYTES) > maxSizeToMovePerIteration) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Hit max size to move limit. {} bytes have already been " +
                "moved and the limit is {} bytes.", sizeMovedPerIteration,
            maxSizeToMovePerIteration);
      }
      return IterationResult.MAX_SIZE_TO_MOVE_REACHED;
    }
    return null;
  }

  /**
   * Asks {@link ReplicationManager} to move the specified container from
   * source to target.
   *
   * @param source the source datanode
   * @param moveSelection the selected container to move and target datanode
   * @return false if an exception occurred, the move completed
   * exceptionally, or the move completed with a result other than
   * ReplicationManager.MoveResult.COMPLETED. Returns true if the move
   * completed with MoveResult.COMPLETED or move is not yet done
   */
  private boolean moveContainer(DatanodeDetails source,
                                ContainerMoveSelection moveSelection) {
    ContainerID container = moveSelection.getContainerID();
    CompletableFuture<ReplicationManager.MoveResult> future;
    try {
      future = replicationManager
          .move(container, source, moveSelection.getTargetNode());
    } catch (ContainerNotFoundException e) {
      LOG.warn("Could not find Container {} for container move", container, e);
      return false;
    } catch (NodeNotFoundException e) {
      LOG.warn("Container move failed for container {}", container, e);
      return false;
    }
    if (future.isDone()) {
      if (future.isCompletedExceptionally()) {
        LOG.info("Container move for container {} from source {} to target {}" +
                "completed exceptionally",
            container.toString(),
            source.getUuidString(),
            moveSelection.getTargetNode().getUuidString());
        return false;
      } else {
        ReplicationManager.MoveResult result = future.join();
        moveSelectionToFutureMap.put(moveSelection, future);
        return result == ReplicationManager.MoveResult.COMPLETED;
      }
    } else {
      moveSelectionToFutureMap.put(moveSelection, future);
      return true;
    }
  }

  /**
   * Update targets and selection criteria after a move.
   *
   * @param selectedTargets  selected target datanodes
   * @param moveSelection    the target datanode and container that has been
   *                         just selected
   * @param source           the source datanode
   * @return List of updated potential targets
   */
  private void updateTargetsAndSelectionCriteria(
      Set<DatanodeDetails> selectedTargets,
      ContainerMoveSelection moveSelection, DatanodeDetails source) {
    // count source if it has not been involved in move earlier
    if (!sourceToTargetMap.containsKey(source) &&
        !selectedTargets.contains(source)) {
      countDatanodesInvolvedPerIteration += 1;
    }
    // count target if it has not been involved in move earlier
    if (!selectedTargets.contains(moveSelection.getTargetNode())) {
      countDatanodesInvolvedPerIteration += 1;
    }
    incSizeSelectedForMoving(source, moveSelection);
    sourceToTargetMap.put(source, moveSelection);
    selectedTargets.add(moveSelection.getTargetNode());
    selectedContainers.add(moveSelection.getContainerID());
    selectionCriteria.setSelectedContainers(selectedContainers);
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
    clusterUsed = aggregatedStats.getScmUsed().get();
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
   * Consults the configurations {@link ContainerBalancer#includeNodes} and
   * {@link ContainerBalancer#excludeNodes} to check if the specified
   * Datanode should be excluded from balancing.
   * @param datanode DatanodeDetails to check
   * @return true if Datanode should be excluded, else false
   */
  boolean shouldExcludeDatanode(DatanodeDetails datanode) {
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
    sizeMovedPerIteration += size;

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
    this.clusterUsed = 0L;
    this.clusterRemaining = 0L;
    this.selectedContainers.clear();
    this.overUtilizedNodes.clear();
    this.underUtilizedNodes.clear();
    this.unBalancedNodes.clear();
    this.countDatanodesInvolvedPerIteration = 0;
    this.sizeMovedPerIteration = 0;
    metrics.resetDataSizeMovedGBInLatestIteration();
    metrics.resetNumMovedContainersInLatestIteration();
    metrics.resetNumDatanodesInvolvedInLatestIteration();
    metrics.resetDataSizeUnbalancedGB();
    metrics.resetNumDatanodesUnbalanced();
  }

  /**
   * Receives a notification for raft or safe mode related status changes.
   * Stops ContainerBalancer if it's running and the current SCM becomes a
   * follower or enters safe mode.
   */
  @Override
  public void notifyStatusChanged() {
    if (!scmContext.isLeader() || scmContext.isInSafeMode()) {
      if (isBalancerRunning()) {
        stopBalancingThread();
      }
    }
  }

  /**
   * Checks if ContainerBalancer should run.
   * @return false
   */
  @Override
  public boolean shouldRun() {
    return false;
  }

  /**
   * @return Name of this service.
   */
  @Override
  public String getServiceName() {
    return ContainerBalancer.class.getSimpleName();
  }

  /**
   * Starts ContainerBalancer as an SCMService.
   */
  @Override
  public void start() {
    if (shouldRun()) {
      startBalancingThread();
    }
  }

  /**
   * Starts Container Balancer after checking its state and validating
   * configuration.
   *
   * @throws IllegalContainerBalancerStateException if ContainerBalancer is
   * not in a start-appropriate state
   * @throws InvalidContainerBalancerConfigurationException if
   * {@link ContainerBalancerConfiguration} config file is incorrectly
   * configured
   */
  public void startBalancer() throws IllegalContainerBalancerStateException,
      InvalidContainerBalancerConfigurationException {
    lock.lock();
    try {
      canStart();
      validateConfiguration(this.config);
      startBalancingThread();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Starts a new balancing thread asynchronously.
   */
  private void startBalancingThread() {
    lock.lock();
    try {
      balancerRunning = true;
      currentBalancingThread = new Thread(this::balance);
      currentBalancingThread.setName("ContainerBalancer");
      currentBalancingThread.setDaemon(true);
      currentBalancingThread.start();
    } finally {
      lock.unlock();
    }
    LOG.info("Starting Container Balancer... {}", this);
  }

  /**
   * Checks if ContainerBalancer can start.
   * @throws IllegalContainerBalancerStateException if ContainerBalancer is
   * already running, SCM is in safe mode, or SCM is not leader ready
   */
  private void canStart() throws IllegalContainerBalancerStateException {
    if (!scmContext.isLeaderReady()) {
      LOG.warn("SCM is not leader ready");
      throw new IllegalContainerBalancerStateException("SCM is not leader " +
          "ready");
    }
    if (scmContext.isInSafeMode()) {
      LOG.warn("SCM is in safe mode");
      throw new IllegalContainerBalancerStateException("SCM is in safe mode");
    }
    lock.lock();
    try {
      if (isBalancerRunning() || currentBalancingThread != null) {
        LOG.warn("Cannot start ContainerBalancer because it's already running");
        throw new IllegalContainerBalancerStateException(
            "Cannot start ContainerBalancer because it's already running");
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Stops the SCM service.
   */
  @Override
  public void stop() {
    stopBalancer();
  }

  /**
   * Stops ContainerBalancer gracefully.
   */
  public void stopBalancer() {
    lock.lock();
    try {
      if (!isBalancerRunning()) {
        LOG.info("Container Balancer is not running.");
        return;
      }
      stopBalancingThread();
    } finally {
      lock.unlock();
    }
  }

  private void stopBalancingThread() {
    lock.lock();
    try {
      balancerRunning = false;
    } finally {
      lock.unlock();
    }
    // wait for currentBalancingThread to die
    if (Thread.currentThread().getId() != currentBalancingThread.getId()) {
      currentBalancingThread.interrupt();
      try {
        currentBalancingThread.join();
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
      }
    }
    lock.lock();
    try {
      currentBalancingThread = null;
    } finally {
      lock.unlock();
    }
    LOG.info("Container Balancer stopped successfully.");
  }

  private void validateConfiguration(ContainerBalancerConfiguration conf)
      throws InvalidContainerBalancerConfigurationException {
    // maxSizeEnteringTarget and maxSizeLeavingSource should by default be
    // greater than container size
    long size = (long) ozoneConfiguration.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);

    if (conf.getMaxSizeEnteringTarget() <= size) {
      LOG.warn("hdds.container.balancer.size.entering.target.max {} should " +
          "be greater than ozone.scm.container.size {}",
          conf.getMaxSizeEnteringTarget(), size);
      throw new InvalidContainerBalancerConfigurationException(
          "hdds.container.balancer.size.entering.target.max should be greater" +
              " than ozone.scm.container.size");
    }
    if (conf.getMaxSizeLeavingSource() <= size) {
      LOG.warn("hdds.container.balancer.size.leaving.source.max {} should " +
              "be greater than ozone.scm.container.size {}",
          conf.getMaxSizeLeavingSource(), size);
      throw new InvalidContainerBalancerConfigurationException(
          "hdds.container.balancer.size.leaving.source.max should be greater" +
              " than ozone.scm.container.size");
    }

    // balancing interval should be greater than DUFactory refresh period
    DUFactory.Conf duConf = ozoneConfiguration.getObject(DUFactory.Conf.class);
    long refreshPeriod = duConf.getRefreshPeriod().toMillis();
    if (conf.getBalancingInterval().toMillis() <= refreshPeriod) {
      LOG.warn("hdds.container.balancer.balancing.iteration.interval {} " +
              "should be greater than hdds.datanode.du.refresh.period {}",
          conf.getBalancingInterval(), refreshPeriod);
    }
  }

  public void setNodeManager(NodeManager nodeManager) {
    this.nodeManager = nodeManager;
  }

  public void setContainerManager(
      ContainerManager containerManager) {
    this.containerManager = containerManager;
  }

  public void setReplicationManager(
      ReplicationManager replicationManager) {
    this.replicationManager = replicationManager;
  }

  public void setOzoneConfiguration(
      OzoneConfiguration ozoneConfiguration) {
    this.ozoneConfiguration = ozoneConfiguration;
  }

  /**
   * Sets the configuration that ContainerBalancer will use. This should be
   * set before starting balancer.
   * @param config ContainerBalancerConfiguration
   */
  public void setConfig(ContainerBalancerConfiguration config) {
    this.config = config;
  }

  /**
   * Gets the list of unBalanced nodes, that is, the over and under utilized
   * nodes in the cluster.
   *
   * @return List of DatanodeUsageInfo containing unBalanced nodes.
   */
  List<DatanodeUsageInfo> getUnBalancedNodes() {
    return unBalancedNodes;
  }

  /**
   * Sets the unBalanced nodes, that is, the over and under utilized nodes in
   * the cluster.
   *
   * @param unBalancedNodes List of DatanodeUsageInfo
   */
  public void setUnBalancedNodes(
      List<DatanodeUsageInfo> unBalancedNodes) {
    this.unBalancedNodes = unBalancedNodes;
  }

  /**
   * Sets the {@link FindTargetStrategy}.
   *
   * @param findTargetStrategy the strategy using which balancer selects a
   *                           target datanode and container for a source
   */
  public void setFindTargetStrategy(
      FindTargetStrategy findTargetStrategy) {
    this.findTargetStrategy = findTargetStrategy;
  }

  /**
   * Gets source datanodes mapped to their selected
   * {@link ContainerMoveSelection}, consisting of target datanode and
   * container to move.
   *
   * @return Map of {@link DatanodeDetails} to {@link ContainerMoveSelection}
   */
  public Map<DatanodeDetails, ContainerMoveSelection> getSourceToTargetMap() {
    return sourceToTargetMap;
  }

  /**
   * Checks if ContainerBalancer is currently running.
   *
   * @return true if ContainerBalancer is running, false if not running.
   */
  public boolean isBalancerRunning() {
    return balancerRunning;
  }

  int getCountDatanodesInvolvedPerIteration() {
    return countDatanodesInvolvedPerIteration;
  }

  public long getSizeMovedPerIteration() {
    return sizeMovedPerIteration;
  }

  public ContainerBalancerMetrics getMetrics() {
    return metrics;
  }

  public static int ratioToPercent(double ratio) {
    return (int) (ratio * 100);
  }

  @Override
  public String toString() {
    String status = String.format("%nContainer Balancer status:%n" +
        "%-30s %s%n" +
        "%-30s %b%n", "Key", "Value", "Running", balancerRunning);
    return status + config.toString();
  }

  /**
   * The result of {@link ContainerBalancer#doIteration()}.
   */
  enum IterationResult {
    ITERATION_COMPLETED,
    MAX_DATANODES_TO_INVOLVE_REACHED,
    MAX_SIZE_TO_MOVE_REACHED,
    ITERATION_INTERRUPTED,
    CAN_NOT_BALANCE_ANY_MORE
  }
}
