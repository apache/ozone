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
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
      final SCMContext scmContext) {
    this.nodeManager = nodeManager;
    this.containerManager = containerManager;
    this.replicationManager = replicationManager;
    this.ozoneConfiguration = ozoneConfiguration;
    this.config = new ContainerBalancerConfiguration();
    this.metrics = new ContainerBalancerMetrics();
    this.scmContext = scmContext;

    this.clusterCapacity = 0L;
    this.clusterUsed = 0L;
    this.clusterRemaining = 0L;

    this.overUtilizedNodes = new ArrayList<>();
    this.underUtilizedNodes = new ArrayList<>();
    this.unBalancedNodes = new ArrayList<>();
    this.withinThresholdUtilizedNodes = new ArrayList<>();
  }

  /**
   * Starts ContainerBalancer. Current implementation is incomplete.
   *
   * @param balancerConfiguration Configuration values.
   */
  public boolean start(ContainerBalancerConfiguration balancerConfiguration) {
    if (!balancerRunning.compareAndSet(false, true)) {
      LOG.error("Container Balancer is already running.");
      return false;
    }

    ozoneConfiguration = new OzoneConfiguration();
    this.config = balancerConfiguration;
    this.threshold = config.getThreshold();
    this.maxDatanodesToBalance = config.getMaxDatanodesToBalance();
    this.maxSizeToMove = config.getMaxSizeToMove();
    this.unBalancedNodes = new ArrayList<>();

    LOG.info("Starting Container Balancer...{}", this);
    balance();
    return true;
  }

  /**
   * Balances the cluster.
   */
  private void balance() {
    initializeIteration();

    // unBalancedNodes is not cleared since the next iteration uses this
    // iteration's unBalancedNodes to find out how many nodes were balanced
    overUtilizedNodes.clear();
    underUtilizedNodes.clear();
    withinThresholdUtilizedNodes.clear();
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
    // sorted list in order from most to least used
    List<DatanodeUsageInfo> datanodeUsageInfos =
        nodeManager.getMostOrLeastUsedDatanodes(true);
    if (datanodeUsageInfos.isEmpty()) {
      LOG.info("Container Balancer could not retrieve nodes from Node " +
          "Manager.");
      stop();
      return false;
    }

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

    long countDatanodesBalanced = 0;
    // count number of nodes that were balanced in previous iteration
    for (DatanodeUsageInfo node : unBalancedNodes) {
      if (!containsNode(overUtilizedNodes, node) &&
          !containsNode(underUtilizedNodes, node)) {
        countDatanodesBalanced += 1;
      }
    }
    // calculate total number of nodes that have been balanced so far
    countDatanodesBalanced =
        metrics.incrementDatanodesNumBalanced(countDatanodesBalanced);

    unBalancedNodes = new ArrayList<>(
        overUtilizedNodes.size() + underUtilizedNodes.size());

    if (countDatanodesBalanced + countDatanodesToBalance >
        maxDatanodesToBalance) {
      LOG.info("Approaching Max Datanodes To Balance limit in Container " +
          "Balancer. Stopping Balancer.");
      stop();
      return false;
    } else {
      unBalancedNodes.addAll(overUtilizedNodes);
      unBalancedNodes.addAll(underUtilizedNodes);

      if (unBalancedNodes.isEmpty()) {
        LOG.info("Did not find any unbalanced Datanodes.");
        stop();
        return false;
      } else {
        LOG.info("Container Balancer has identified Datanodes that need to be" +
            " balanced.");
      }
    }
    return true;
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

  /**
   * Stops ContainerBalancer.
   */
  public void stop() {
    balancerRunning.set(false);
    LOG.info("Container Balancer stopped.");
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
