/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private ContainerManagerV2 containerManager;
  private ReplicationManager replicationManager;
  private OzoneConfiguration ozoneConfiguration;
  private final SCMContext scmContext;
  private double threshold;
  private int maxDatanodesToBalance;
  private long maxSizeToMove;
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
  private final AtomicBoolean balancerRunning = new AtomicBoolean(false);
  private Thread currentBalancingThread;
  private Lock lock;
  private ContainerBalancerSelectionCriteria selectionCriteria;
  private Map<DatanodeDetails, ContainerMoveSelection> sourceToTarget;
  private Map<DatanodeDetails, Long> sizeLeavingNode;
  private Map<DatanodeDetails, Long> sizeEnteringNode;
  private Set<ContainerID> selectedContainers;
  private FindTargetStrategy findTargetStrategy;
  private PlacementPolicy placementPolicy;

  /**
   * Constructs ContainerBalancer with the specified arguments. Initializes
   * new ContainerBalancerConfiguration and ContainerBalancerMetrics.
   * Container Balancer does not start on construction.
   *
   * @param nodeManager NodeManager
   * @param containerManager ContainerManager
   * @param replicationManager ReplicationManager
   * @param ozoneConfiguration OzoneConfiguration
   */
  public ContainerBalancer(
      NodeManager nodeManager,
      ContainerManagerV2 containerManager,
      ReplicationManager replicationManager,
      OzoneConfiguration ozoneConfiguration,
      final SCMContext scmContext,
      PlacementPolicy placementPolicy) {
    this.nodeManager = nodeManager;
    this.containerManager = containerManager;
    this.replicationManager = replicationManager;
    this.ozoneConfiguration = ozoneConfiguration;
    this.config = new ContainerBalancerConfiguration();
    this.metrics = new ContainerBalancerMetrics();
    this.scmContext = scmContext;
    this.placementPolicy = placementPolicy;

    this.lock = new ReentrantLock();
    findTargetStrategy =
        new FindTargetGreedy(containerManager, placementPolicy);
  }

  /**
   * Starts ContainerBalancer. Current implementation is incomplete.
   *
   * @param balancerConfiguration Configuration values.
   */
  public boolean start(
      ContainerBalancerConfiguration balancerConfiguration) {
    lock.lock();
    try {
      if (!balancerRunning.compareAndSet(false, true)) {
        LOG.error("Container Balancer is already running.");
        return false;
      }

      ozoneConfiguration = new OzoneConfiguration();
      this.config = balancerConfiguration;
      this.idleIteration = config.getIdleIteration();
      this.threshold = config.getThreshold();
      this.maxDatanodesToBalance = config.getMaxDatanodesToBalance();
      this.maxSizeToMove = config.getMaxSizeToMove();
      LOG.info("Starting Container Balancer...{}", this);
      LOG.info("Max size entering node config is {}",
          config.getMaxSizeEnteringTarget());

      //we should start a new balancer thread async
      //and response to cli as soon as possible


      //TODO: this is a temporary implementation
      //modify this later
      currentBalancingThread = new Thread(this::balance);
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
    this.clusterCapacity = 0L;
    this.clusterUsed = 0L;
    this.clusterRemaining = 0L;

    this.selectedContainers = new HashSet<>();
    this.overUtilizedNodes = new ArrayList<>();
    this.underUtilizedNodes = new ArrayList<>();
    this.withinThresholdUtilizedNodes = new ArrayList<>();
    this.unBalancedNodes = new ArrayList<>();

    for (int i = 0; i < idleIteration; i++) {
      if (!initializeIteration()) {
        //balancer should be stopped immediately
        break;
      }
      doIteration();
    }
    balancerRunning.compareAndSet(true, false);
  }

  /**
   * Initializes an iteration during balancing. Recognizes over, under, and
   * within threshold utilized nodes. Decides whether balancing needs to
   * continue or should be stopped.
   *
   * TODO: update method for changes made to configs max datanodes balanced-
   * TODO: -per iteration and max size moved per iteration.
   *
   * @return true if successfully initialized, otherwise false.
   */
  private boolean initializeIteration() {
    if (scmContext.isInSafeMode()) {
      LOG.error("Container Balancer cannot operate while SCM is in Safe Mode.");
      return false;
    }
    // sorted list in order from most to least used
    List<DatanodeUsageInfo> datanodeUsageInfos =
        nodeManager.getMostOrLeastUsedDatanodes(true);
    if (datanodeUsageInfos.isEmpty()) {
      LOG.info("Container Balancer could not retrieve nodes from Node " +
          "Manager.");
      return false;
    }

    overUtilizedNodes = new ArrayList<>();
    underUtilizedNodes = new ArrayList<>();
    withinThresholdUtilizedNodes = new ArrayList<>();

    clusterAvgUtilisation = calculateAvgUtilization(datanodeUsageInfos);
    LOG.info("Average utilization of the cluster is {}", clusterAvgUtilisation);

    // under utilized nodes have utilization(that is, used / capacity) less
    // than lower limit
    double lowerLimit = clusterAvgUtilisation - threshold;

    // over utilized nodes have utilization(that is, used / capacity) greater
    // than upper limit
    double upperLimit = clusterAvgUtilisation + threshold;

    LOG.info("Lower limit for utilization is {} and Upper limit for " +
        "utilization is {}", lowerLimit, upperLimit);

    long countDatanodesToBalance = 0L;
    double overLoadedBytes = 0D, underLoadedBytes = 0D;

    // find over and under utilized nodes
    for (DatanodeUsageInfo datanodeUsageInfo : datanodeUsageInfos) {
      double utilization = calculateUtilization(datanodeUsageInfo);
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
    Collections.reverse(underUtilizedNodes);

//    long countDatanodesBalanced = 0;
//    // count number of nodes that were balanced in previous iteration
//    for (DatanodeUsageInfo node : unBalancedNodes) {
//      if (!containsNode(overUtilizedNodes, node) &&
//          !containsNode(underUtilizedNodes, node)) {
//        countDatanodesBalanced += 1;
//      }
//    }
//    // calculate total number of nodes that have been balanced so far
//    countDatanodesBalanced =
//        metrics.incrementDatanodesNumBalanced(countDatanodesBalanced);

    unBalancedNodes = new ArrayList<>(
        overUtilizedNodes.size() + underUtilizedNodes.size());

//    if (countDatanodesBalanced + countDatanodesToBalance >
//        maxDatanodesToBalance) {
//      LOG.info("Approaching Max Datanodes To Balance limit in Container " +
//          "Balancer. Stopping Balancer.");
//      stop();
//      return false;
//    }
//
    unBalancedNodes.addAll(overUtilizedNodes);
    unBalancedNodes.addAll(underUtilizedNodes);

    if (unBalancedNodes.isEmpty()) {
      LOG.info("Did not find any unbalanced Datanodes.");
      return false;
    }

    LOG.info("Container Balancer has identified Datanodes that need to be" +
            " balanced.");

    selectionCriteria = new ContainerBalancerSelectionCriteria(config,
        nodeManager, replicationManager, containerManager);
    selectionCriteria.setSelectedContainers(selectedContainers);
    sourceToTarget = new HashMap<>(overUtilizedNodes.size() +
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

  public void doIteration() {
    List<DatanodeDetails> potentialTargets = findPotentialTargets();
    Set<DatanodeDetails> selectedTargets =
        new HashSet<>(potentialTargets.size());

    // match each overUtilized node with a target
    for (DatanodeUsageInfo datanodeUsageInfo : overUtilizedNodes) {
      DatanodeDetails source = datanodeUsageInfo.getDatanodeDetails();
      ContainerMoveSelection moveSelection =
          matchSourceWithTarget(source, potentialTargets, selectedTargets);

      if (moveSelection != null) {
        LOG.info("ContainerBalancer is trying to move container {} from " +
                "source datanode {} to target datanode {}",
            moveSelection.getContainerID().toString(), source.getUuidString(),
            moveSelection.getTargetNode().getUuidString());

          // move container
      }
    }

    // if not all underUtilized nodes have been selected, try to match
    // withinThresholdUtilized nodes with underUtilized nodes
    if (selectedTargets.size() < underUtilizedNodes.size()) {
      potentialTargets.removeAll(selectedTargets);
      Collections.reverse(withinThresholdUtilizedNodes);

      for (DatanodeUsageInfo datanodeUsageInfo : withinThresholdUtilizedNodes) {
        DatanodeDetails source = datanodeUsageInfo.getDatanodeDetails();
        if (!selectedTargets.contains(source)) {
          ContainerMoveSelection moveSelection = matchSourceWithTarget(source,
              potentialTargets, selectedTargets);

          if (moveSelection != null) {
            LOG.info("ContainerBalancer is trying to move container {} from " +
                    "source datanode {} to target datanode {}",
                moveSelection.getContainerID().toString(),
                source.getUuidString(),
                moveSelection.getTargetNode().getUuidString());

            // move container
          }
        }
      }
    }
  }

  /**
   * Match a source datanode with a target datanode and identify the container
   * to move.
   * @param potentialTargets List of potential targets to move container to
   * @param selectedTargets Set of already selected targets
   * @return ContainerMoveSelection containing the selected target and container
   */
  public ContainerMoveSelection matchSourceWithTarget(DatanodeDetails source,
                                    List<DatanodeDetails> potentialTargets,
                                     Set<DatanodeDetails> selectedTargets) {

    NavigableSet<ContainerID> candidateContainers =
        selectionCriteria.getCandidateContainers(source);

    if (candidateContainers.isEmpty()) {
      LOG.info("ContainerBalancer could not find any candidate containers for" +
          " datanode {}", source.getUuidString());
      return null;
    }
    LOG.info("ContainerBalancer is finding suitable target for source " +
        "datanode {}", source.getUuidString());
    ContainerMoveSelection moveSelection =
        findTargetStrategy.findTargetForContainerMove(
            source, potentialTargets, candidateContainers,
            this::canSizeEnterTarget);

    if (moveSelection == null) {
      LOG.info("ContainerBalancer could not find a suitable target for " +
          "source node {}.", source.getUuidString());
      return null;
    }

    LOG.info("ContainerBalancer matched source datanode {} with target " +
            "datanode {} for container move.", source.getUuidString(),
        moveSelection.getTargetNode().getUuidString());

    incSizeSelectedForMoving(source, moveSelection);
    updatePotentialTargets(potentialTargets);
    sourceToTarget.put(source, moveSelection);
    selectedTargets.add(moveSelection.getTargetNode());
    selectedContainers.add(moveSelection.getContainerID());
    selectionCriteria.setSelectedContainers(selectedContainers);
    return moveSelection;
  }

  /**
   * Performs binary search to determine if the specified listToSearch
   * contains the specified node.
   *
   * @param listToSearch List of DatanodeUsageInfo to be searched.
   * @param node DatanodeUsageInfo to be searched for.
   * @return true if the specified node is present in listToSearch, otherwise
   * false.
   */
  private boolean containsNode(
      List<DatanodeUsageInfo> listToSearch, DatanodeUsageInfo node) {
    int index = 0;
    Comparator<DatanodeUsageInfo> comparator =
        DatanodeUsageInfo.getMostUsedByRemainingRatio();
    int size = listToSearch.size();
    if (size == 0) {
      return false;
    }

    if (comparator.compare(listToSearch.get(0),
        listToSearch.get(size - 1)) < 0) {
      index =
          Collections.binarySearch(listToSearch, node, comparator.reversed());
    } else {
      index = Collections.binarySearch(listToSearch, node, comparator);
    }
    return index >= 0 && listToSearch.get(index).equals(node);
  }

  /**
   * Calculates the number of used bytes given capacity and utilization ratio.
   *
   * @param nodeCapacity capacity of the node.
   * @param utilizationRatio used space by capacity ratio of the node.
   * @return number of bytes
   */
  private double ratioToBytes(Long nodeCapacity, double utilizationRatio) {
    return nodeCapacity * utilizationRatio;
  }

  /**
   * Calculates the average utilization for the specified nodes.
   * Utilization is used space divided by capacity.
   *
   * @param nodes List of DatanodeUsageInfo to find the average utilization for
   * @return Average utilization value
   */
  private double calculateAvgUtilization(List<DatanodeUsageInfo> nodes) {
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

    return clusterUsed / (double) clusterCapacity;
  }

  /**
   * Calculates the utilization, that is used space divided by capacity, for
   * the given datanodeUsageInfo.
   *
   * @param datanodeUsageInfo DatanodeUsageInfo to calculate utilization for
   * @return Utilization value
   */
  public static double calculateUtilization(
      DatanodeUsageInfo datanodeUsageInfo) {
    SCMNodeStat stat = datanodeUsageInfo.getScmNodeStat();

    return stat.getScmUsed().get().doubleValue() /
        stat.getCapacity().get().doubleValue();
  }

  public boolean canSizeLeaveSource(DatanodeDetails source, long size) {
    if (sizeLeavingNode.containsKey(source)) {
      return sizeLeavingNode.get(source) + size <=
          config.getMaxSizeLeavingSource();
    }
    return false;
  }

  public boolean canSizeEnterTarget(DatanodeDetails target,
                                    long size) {
    if (sizeEnteringNode.containsKey(target)) {
      return sizeEnteringNode.get(target) + size <=
          config.getMaxSizeEnteringTarget();
    }
    return false;
  }

  private List<DatanodeDetails> findPotentialTargets() {
    List<DatanodeDetails> potentialTargets = new ArrayList<>(
        underUtilizedNodes.size() + withinThresholdUtilizedNodes.size());

    underUtilizedNodes
        .forEach(node -> potentialTargets.add(node.getDatanodeDetails()));
    withinThresholdUtilizedNodes
        .forEach(node -> potentialTargets.add(node.getDatanodeDetails()));
    return potentialTargets;
  }

  public void incSizeSelectedForMoving(DatanodeDetails source,
                                       ContainerMoveSelection moveSelection) {
    DatanodeDetails target =
        moveSelection.getTargetNode();
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

    // update sizeLeavingNode map with the recent moveSelection
    if (sizeLeavingNode.containsKey(source)) {
      sizeLeavingNode
          .put(source, sizeLeavingNode.get(source) + container.getUsedBytes());
    } else {
      sizeLeavingNode.put(source, container.getUsedBytes());
    }

    // update sizeEnteringNode map with the recent moveSelection
    if (sizeEnteringNode.containsKey(target)) {
      sizeEnteringNode
          .put(target, sizeEnteringNode.get(target) + container.getUsedBytes());
    } else {
      sizeEnteringNode.put(target, container.getUsedBytes());
    }
  }

  public List<DatanodeDetails> updatePotentialTargets(
      List<DatanodeDetails> potentialTargets) {
    return potentialTargets.stream()
        .filter(node -> sizeEnteringNode.get(node) <=
            config.getMaxSizeEnteringTarget()).collect(Collectors.toList());
  }

  /**
   * Stops ContainerBalancer.
   */
  public void stop() {
    lock.lock();
    try {
      //we should stop the balancer thread gracefully
      if(!balancerRunning.get()) {
        LOG.info("Container Balancer is not running.");
        return;
      }

      //TODO: this is a temporary implementation
      //modify this later
      if (currentBalancingThread.isAlive()) {
        currentBalancingThread.stop();
      }
      ///////////////////////////

      balancerRunning.compareAndSet(true, false);
    } finally {
      lock.unlock();
    }
    LOG.info("Container Balancer stopped successfully.");
  }

  public void setNodeManager(NodeManager nodeManager) {
    this.nodeManager = nodeManager;
  }

  public void setContainerManager(
      ContainerManagerV2 containerManager) {
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
   * Gets the average utilization of the cluster as calculated by
   * ContainerBalancer.
   *
   * @return average utilization value
   */
  public double getClusterAvgUtilisation() {
    return clusterAvgUtilisation;
  }

  /**
   * Gets the list of unBalanced nodes, that is, the over and under utilized
   * nodes in the cluster.
   *
   * @return List of DatanodeUsageInfo containing unBalanced nodes.
   */
  public List<DatanodeUsageInfo> getUnBalancedNodes() {
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

  public void setFindTargetStrategy(
      FindTargetStrategy findTargetStrategy) {
    this.findTargetStrategy = findTargetStrategy;
  }

  /**
   * Checks if ContainerBalancer is currently running.
   *
   * @return true if ContainerBalancer is running, false if not running.
   */
  public boolean isBalancerRunning() {
    return balancerRunning.get();
  }

  @Override
  public String toString() {
    String status = String.format("%nContainer Balancer status:%n" +
        "%-30s %s%n" +
        "%-30s %b%n", "Key", "Value", "Running", balancerRunning);
    return status + config.toString();
  }
}
