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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.placement.metrics.LongMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
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
import java.util.stream.Collectors;

/**
 * Container balancer is a service in SCM to move containers between over- and
 * under-utilized datanodes.
 */
public class ContainerBalancer {

  private static final Logger LOG =
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
  private int idleIteration;
  private List<DatanodeUsageInfo> unBalancedNodes;
  private List<DatanodeUsageInfo> overUtilizedNodes;
  private List<DatanodeUsageInfo> underUtilizedNodes;
  private List<DatanodeUsageInfo> withinThresholdUtilizedNodes;
  private ContainerBalancerConfiguration config;
  private ContainerBalancerMetrics metrics;
  private long clusterCapacity;
  private long clusterUsed;
  private long clusterRemaining;
  private double clusterAvgUtilisation;
  private double upperLimit;
  private volatile boolean balancerRunning;
  private Thread currentBalancingThread;
  private Lock lock;
  private ContainerBalancerSelectionCriteria selectionCriteria;
  private Map<DatanodeDetails, ContainerMoveSelection> sourceToTargetMap;
  private Map<DatanodeDetails, Long> sizeLeavingNode;
  private Map<DatanodeDetails, Long> sizeEnteringNode;
  private Set<ContainerID> selectedContainers;
  private FindTargetStrategy findTargetStrategy;
  private Map<ContainerMoveSelection,
      CompletableFuture<ReplicationManager.MoveResult>>
      moveSelectionToFutureMap;

  /**
   * Constructs ContainerBalancer with the specified arguments. Initializes
   * new ContainerBalancerConfiguration and ContainerBalancerMetrics.
   * Container Balancer does not start on construction.
   *
   * @param nodeManager        NodeManager
   * @param containerManager   ContainerManager
   * @param replicationManager ReplicationManager
   * @param ozoneConfiguration OzoneConfiguration
   */
  public ContainerBalancer(
      NodeManager nodeManager,
      ContainerManager containerManager,
      ReplicationManager replicationManager,
      OzoneConfiguration ozoneConfiguration,
      final SCMContext scmContext,
      PlacementPolicy placementPolicy) {
    this.nodeManager = nodeManager;
    this.containerManager = containerManager;
    this.replicationManager = replicationManager;
    this.ozoneConfiguration = ozoneConfiguration;
    this.config = new ContainerBalancerConfiguration(ozoneConfiguration);
    this.metrics = new ContainerBalancerMetrics();
    this.scmContext = scmContext;

    this.selectedContainers = new HashSet<>();
    this.overUtilizedNodes = new ArrayList<>();
    this.underUtilizedNodes = new ArrayList<>();
    this.withinThresholdUtilizedNodes = new ArrayList<>();
    this.unBalancedNodes = new ArrayList<>();

    this.lock = new ReentrantLock();
    findTargetStrategy =
        new FindTargetGreedy(containerManager, placementPolicy);
  }

  /**
   * Starts ContainerBalancer. Current implementation is incomplete.
   *
   * @param balancerConfiguration Configuration values.
   */
  public boolean start(ContainerBalancerConfiguration balancerConfiguration) {
    lock.lock();
    try {
      if (balancerRunning) {
        LOG.error("Container Balancer is already running.");
        return false;
      }

      balancerRunning = true;
      this.config = balancerConfiguration;
      this.ozoneConfiguration = config.getOzoneConfiguration();
      LOG.info("Starting Container Balancer...{}", this);

      //we should start a new balancer thread async
      //and response to cli as soon as possible

      //TODO: this is a temporary implementation
      //modify this later
      currentBalancingThread = new Thread(this::balance);
      currentBalancingThread.setName("ContainerBalancer");
      currentBalancingThread.setDaemon(true);
      currentBalancingThread.start();
      ////////////////////////
    } finally {
      lock.unlock();
    }
    return true;
  }

  /**
   * Balances the cluster.
   */
  private void balance() {
    this.idleIteration = config.getIdleIteration();
    if(this.idleIteration == -1) {
      //run balancer infinitely
      this.idleIteration = Integer.MAX_VALUE;
    }
    this.threshold = config.getThreshold();
    this.maxDatanodesRatioToInvolvePerIteration =
        config.getMaxDatanodesRatioToInvolvePerIteration();
    this.maxSizeToMovePerIteration = config.getMaxSizeToMovePerIteration();
    for (int i = 0; i < idleIteration && balancerRunning; i++) {
      // stop balancing if iteration is not initialized
      if (!initializeIteration()) {
        stop();
        return;
      }

      if (doIteration() == IterationResult.CAN_NOT_BALANCE_ANY_MORE) {
        stop();
        return;
      }

      // return if balancing has been stopped
      if (!isBalancerRunning()) {
        return;
      }

      // wait for configured time before starting next iteration, unless
      // this was the final iteration
      if (i != idleIteration - 1) {
        synchronized (this) {
          try {
            wait(config.getBalancingInterval().toMillis());
          } catch (InterruptedException e) {
            LOG.info("Container Balancer was interrupted while waiting for" +
                " next iteration.");
            stop();
            return;
          }
        }
      }
    }
    stop();
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

    this.totalNodesInCluster = datanodeUsageInfos.size();
    this.clusterCapacity = 0L;
    this.clusterUsed = 0L;
    this.clusterRemaining = 0L;
    this.selectedContainers.clear();
    this.overUtilizedNodes.clear();
    this.underUtilizedNodes.clear();
    this.withinThresholdUtilizedNodes.clear();
    this.unBalancedNodes.clear();
    this.countDatanodesInvolvedPerIteration = 0;
    this.sizeMovedPerIteration = 0;

    clusterAvgUtilisation = calculateAvgUtilization(datanodeUsageInfos);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Average utilization of the cluster is {}",
          clusterAvgUtilisation);
    }

    // under utilized nodes have utilization(that is, used / capacity) less
    // than lower limit
    double lowerLimit = clusterAvgUtilisation - threshold;

    // over utilized nodes have utilization(that is, used / capacity) greater
    // than upper limit
    this.upperLimit = clusterAvgUtilisation + threshold;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Lower limit for utilization is {} and Upper limit for " +
          "utilization is {}", lowerLimit, upperLimit);
    }

    long countDatanodesToBalance = 0L;
    double overLoadedBytes = 0D, underLoadedBytes = 0D;

    // find over and under utilized nodes
    for (DatanodeUsageInfo datanodeUsageInfo : datanodeUsageInfos) {
      double utilization = datanodeUsageInfo.calculateUtilization();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Utilization for node {} is {}",
            datanodeUsageInfo.getDatanodeDetails().getUuidString(),
            utilization);
      }
      if (utilization > upperLimit) {
        overUtilizedNodes.add(datanodeUsageInfo);
        countDatanodesToBalance += 1;

        // amount of bytes greater than upper limit in this node
        overLoadedBytes += ratioToBytes(
            datanodeUsageInfo.getScmNodeStat().getCapacity().get(),
            utilization) - ratioToBytes(
            datanodeUsageInfo.getScmNodeStat().getCapacity().get(),
            upperLimit);
      } else if (utilization < lowerLimit) {
        underUtilizedNodes.add(datanodeUsageInfo);
        countDatanodesToBalance += 1;

        // amount of bytes lesser than lower limit in this node
        underLoadedBytes += ratioToBytes(
            datanodeUsageInfo.getScmNodeStat().getCapacity().get(),
            lowerLimit) - ratioToBytes(
            datanodeUsageInfo.getScmNodeStat().getCapacity().get(),
            utilization);
      } else {
        withinThresholdUtilizedNodes.add(datanodeUsageInfo);
      }
    }
    metrics.setDatanodesNumToBalance(new LongMetric(countDatanodesToBalance));
    // TODO update dataSizeToBalanceGB metric with overLoadedBytes and
    //  underLoadedBytes
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

    selectionCriteria = new ContainerBalancerSelectionCriteria(config,
        nodeManager, replicationManager, containerManager);
    sourceToTargetMap = new HashMap<>(overUtilizedNodes.size() +
        withinThresholdUtilizedNodes.size());

    // initialize maps to track how much size is leaving and entering datanodes
    sizeLeavingNode = new HashMap<>(overUtilizedNodes.size() +
        withinThresholdUtilizedNodes.size());
    overUtilizedNodes.forEach(datanodeUsageInfo -> sizeLeavingNode
        .put(datanodeUsageInfo.getDatanodeDetails(), 0L));
    withinThresholdUtilizedNodes.forEach(datanodeUsageInfo -> sizeLeavingNode
        .put(datanodeUsageInfo.getDatanodeDetails(), 0L));

    sizeEnteringNode = new HashMap<>(underUtilizedNodes.size() +
        withinThresholdUtilizedNodes.size());
    underUtilizedNodes.forEach(datanodeUsageInfo -> sizeEnteringNode
        .put(datanodeUsageInfo.getDatanodeDetails(), 0L));
    withinThresholdUtilizedNodes.forEach(datanodeUsageInfo -> sizeEnteringNode
        .put(datanodeUsageInfo.getDatanodeDetails(), 0L));

    return true;
  }

  private IterationResult doIteration() {
    List<DatanodeDetails> potentialTargets = getPotentialTargets();
    Set<DatanodeDetails> selectedTargets =
        new HashSet<>(potentialTargets.size());
    moveSelectionToFutureMap = new HashMap<>(unBalancedNodes.size());
    boolean isMoveGenerated = false;

    // match each overUtilized node with a target
    for (DatanodeUsageInfo datanodeUsageInfo : overUtilizedNodes) {
      DatanodeDetails source = datanodeUsageInfo.getDatanodeDetails();
      IterationResult result = checkConditionsForBalancing();
      if (result != null) {
        LOG.info("Exiting current iteration: {}", result);
        return result;
      }

      ContainerMoveSelection moveSelection =
          matchSourceWithTarget(source, potentialTargets);
      if (moveSelection != null) {
        isMoveGenerated = true;
        LOG.info("ContainerBalancer is trying to move container {} from " +
                "source datanode {} to target datanode {}",
            moveSelection.getContainerID().toString(), source.getUuidString(),
            moveSelection.getTargetNode().getUuidString());

        if (moveContainer(source, moveSelection)) {
          // consider move successful for now, and update selection criteria
          potentialTargets = updateTargetsAndSelectionCriteria(potentialTargets,
              selectedTargets, moveSelection, source);
        }
      }
    }

    // if not all underUtilized nodes have been selected, try to match
    // withinThresholdUtilized nodes with underUtilized nodes
    if (selectedTargets.size() < underUtilizedNodes.size()) {
      potentialTargets.removeAll(selectedTargets);
      Collections.reverse(withinThresholdUtilizedNodes);

      for (DatanodeUsageInfo datanodeUsageInfo : withinThresholdUtilizedNodes) {
        DatanodeDetails source = datanodeUsageInfo.getDatanodeDetails();
        IterationResult result = checkConditionsForBalancing();
        if (result != null) {
          LOG.info("Exiting current iteration: {}", result);
          return result;
        }

        ContainerMoveSelection moveSelection =
            matchSourceWithTarget(source, potentialTargets);
        if (moveSelection != null) {
          isMoveGenerated = true;
          LOG.info("ContainerBalancer is trying to move container {} from " +
                  "source datanode {} to target datanode {}",
              moveSelection.getContainerID().toString(),
              source.getUuidString(),
              moveSelection.getTargetNode().getUuidString());

          if (moveContainer(source, moveSelection)) {
            // consider move successful for now, and update selection criteria
            potentialTargets =
                updateTargetsAndSelectionCriteria(potentialTargets,
                    selectedTargets, moveSelection, source);
          }
        }
      }
    }

    if (!isMoveGenerated) {
      //no move option is generated, so the cluster can not be
      //balanced any more, just stop iteration and exit
      return IterationResult.CAN_NOT_BALANCE_ANY_MORE;
    }

    // check move results
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
            this.countDatanodesInvolvedPerIteration += 2;
          } catch (ContainerNotFoundException e) {
            LOG.warn("Could not find Container {} while " +
                    "checking move results in ContainerBalancer",
                moveSelection.getContainerID(), e);
          }
          metrics.incrementMovedContainersNum(1);
          // TODO incrementing size balanced this way incorrectly counts the
          //  size moved twice
          metrics.incrementDataSizeBalancedGB(sizeMovedPerIteration);
        }
      } catch (InterruptedException e) {
        LOG.warn("Container move for container {} was interrupted.",
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
    LOG.info("Number of datanodes involved in this iteration: {}. Size moved " +
            "in this iteration: {}B.",
        countDatanodesInvolvedPerIteration, sizeMovedPerIteration);
    return IterationResult.ITERATION_COMPLETED;
  }

  /**
   * Match a source datanode with a target datanode and identify the container
   * to move.
   *
   * @param potentialTargets Collection of potential targets to move
   *                         container to
   * @return ContainerMoveSelection containing the selected target and container
   */
  private ContainerMoveSelection matchSourceWithTarget(
      DatanodeDetails source, Collection<DatanodeDetails> potentialTargets) {
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
            source, potentialTargets, candidateContainers,
            this::canSizeEnterTarget);

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
   * Checks if limits maxDatanodesRatioToInvolvePerIteration and
   * maxSizeToMovePerIteration have not been hit.
   *
   * @return {@link IterationResult#MAX_DATANODES_TO_INVOLVE_REACHED} if reached
   * max datanodes to involve limit,
   * {@link IterationResult#MAX_SIZE_TO_MOVE_REACHED} if reached max size to
   * move limit, or null if balancing can continue
   */
  private IterationResult checkConditionsForBalancing() {
    if (countDatanodesInvolvedPerIteration + 2 >
        maxDatanodesRatioToInvolvePerIteration * totalNodesInCluster) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Hit max datanodes to involve limit. {} datanodes have" +
                " already been involved and the limit is {}.",
            countDatanodesInvolvedPerIteration,
            maxDatanodesRatioToInvolvePerIteration * totalNodesInCluster);
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
   * Update targets and selection criteria at the end of an iteration.
   *
   * @param potentialTargets potential target datanodes
   * @param selectedTargets  selected target datanodes
   * @param moveSelection    the target datanode and container that has been
   *                         just selected
   * @param source           the source datanode
   * @return List of updated potential targets
   */
  private List<DatanodeDetails> updateTargetsAndSelectionCriteria(
      Collection<DatanodeDetails> potentialTargets,
      Set<DatanodeDetails> selectedTargets,
      ContainerMoveSelection moveSelection, DatanodeDetails source) {
    // TODO: counting datanodes involved this way is incorrect when the same
    //  datanode is involved in different moves
    countDatanodesInvolvedPerIteration += 2;
    incSizeSelectedForMoving(source, moveSelection);
    sourceToTargetMap.put(source, moveSelection);
    selectedTargets.add(moveSelection.getTargetNode());
    selectedContainers.add(moveSelection.getContainerID());
    selectionCriteria.setSelectedContainers(selectedContainers);

    return potentialTargets.stream()
        .filter(node -> sizeEnteringNode.get(node) <
            config.getMaxSizeEnteringTarget()).collect(Collectors.toList());
  }

  /**
   * Calculates the number of used bytes given capacity and utilization ratio.
   *
   * @param nodeCapacity     capacity of the node.
   * @param utilizationRatio used space by capacity ratio of the node.
   * @return number of bytes
   */
  private double ratioToBytes(Long nodeCapacity, double utilizationRatio) {
    return nodeCapacity * utilizationRatio;
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
   * Checks if specified size can enter specified target datanode
   * according to configuration.
   *
   * @param target target datanode in which size is entering
   * @param size   size in bytes
   * @return true if size can enter target, else false
   */
  boolean canSizeEnterTarget(DatanodeDetails target, long size) {
    if (sizeEnteringNode.containsKey(target)) {
      long sizeEnteringAfterMove = sizeEnteringNode.get(target) + size;
      SCMNodeMetric scmNM = nodeManager.getNodeStat(target);
      Preconditions.checkNotNull(scmNM);
      SCMNodeStat scmNodeStat = scmNM.get();
      double capacity = scmNodeStat.getCapacity().get();
      Preconditions.checkArgument(capacity > 0);
      double usedAfterMove =
          capacity - scmNodeStat.getRemaining().get() + size;

      return sizeEnteringAfterMove <= config.getMaxSizeEnteringTarget() &&
          usedAfterMove/capacity <= upperLimit;
    }
    return false;
  }

  /**
   * Get potential targets for container move. Potential targets are under
   * utilized and within threshold utilized nodes.
   *
   * @return A list of potential target DatanodeDetails.
   */
  private List<DatanodeDetails> getPotentialTargets() {
    List<DatanodeDetails> potentialTargets = new ArrayList<>(
        underUtilizedNodes.size() + withinThresholdUtilizedNodes.size());

    underUtilizedNodes
        .forEach(node -> potentialTargets.add(node.getDatanodeDetails()));
    withinThresholdUtilizedNodes
        .forEach(node -> potentialTargets.add(node.getDatanodeDetails()));
    return potentialTargets;
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
    sizeLeavingNode.put(source, sizeLeavingNode.get(source) + size);

    // update sizeEnteringNode map with the recent moveSelection
    sizeEnteringNode.put(target, sizeEnteringNode.get(target) + size);
  }

  /**
   * Stops ContainerBalancer.
   */
  public void stop() {
    lock.lock();
    try {
      // we should stop the balancer thread gracefully
      if (!balancerRunning) {
        LOG.info("Container Balancer is not running.");
        return;
      }
      balancerRunning = false;
      currentBalancingThread.interrupt();
      currentBalancingThread.join(1000);

      // allow garbage collector to collect balancing thread
      currentBalancingThread = null;
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for balancing thread to join.");
      Thread.currentThread().interrupt();
    } finally {
      lock.unlock();
    }
    LOG.info("Container Balancer stopped successfully.");
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

  long getSizeMovedPerIteration() {
    return sizeMovedPerIteration;
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
    CAN_NOT_BALANCE_ANY_MORE
  }
}
