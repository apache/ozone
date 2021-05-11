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
import org.apache.hadoop.hdds.scm.container.placement.metrics.LongMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ContainerBalancer {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerBalancer.class);

  private NodeManager nodeManager;
  private ContainerManagerV2 containerManager;
  private ReplicationManager replicationManager;
  private OzoneConfiguration ozoneConfiguration;
  private double threshold;
  private int maxDatanodesToBalance;
  private long maxSizeToMove;
  private boolean balancerRunning;
  private List<DatanodeUsageInfo> sourceNodes;
  private List<DatanodeUsageInfo> overUtilizedNodes;
  private List<DatanodeUsageInfo> underUtilizedNodes;
  private List<DatanodeUsageInfo> aboveAverageUtilizedNodes;
  private List<DatanodeUsageInfo> belowAverageUtilizedNodes;
  private ContainerBalancerConfiguration config;
  private ContainerBalancerMetrics metrics;
  private long clusterCapacity;
  private long clusterUsed;
  private long clusterRemaining;
  private double clusterAvgUtilisation;

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
      OzoneConfiguration ozoneConfiguration) {
    this.nodeManager = nodeManager;
    this.containerManager = containerManager;
    this.replicationManager = replicationManager;
    this.ozoneConfiguration = ozoneConfiguration;
    this.balancerRunning = false;
    this.config = new ContainerBalancerConfiguration();
    this.metrics = new ContainerBalancerMetrics();
  }

  /**
   * Starts ContainerBalancer. Current implementation is incomplete.
   *
   * @param balancerConfiguration Configuration values.
   */
  public void start(ContainerBalancerConfiguration balancerConfiguration) {
    if (balancerRunning) {
      LOG.info("Container Balancer is already running.");
      throw new RuntimeException();
    }
    this.balancerRunning = true;
    ozoneConfiguration = new OzoneConfiguration();

    this.config = balancerConfiguration;
    this.threshold = config.getThreshold();
    this.maxDatanodesToBalance = config.getMaxDatanodesToBalance();
    this.maxSizeToMove = config.getMaxSizeToMove();

    this.clusterCapacity = 0L;
    this.clusterUsed = 0L;
    this.clusterRemaining = 0L;

    this.overUtilizedNodes = new ArrayList<>();
    this.underUtilizedNodes = new ArrayList<>();
    this.aboveAverageUtilizedNodes = new ArrayList<>();
    this.belowAverageUtilizedNodes = new ArrayList<>();
    this.sourceNodes = new ArrayList<>();

    LOG.info("Starting Container Balancer...");
    LOG.info(toString());

    balance();
  }

  /**
   * Balances the cluster.
   */
  private void balance() {
    overUtilizedNodes.clear();
    underUtilizedNodes.clear();
    aboveAverageUtilizedNodes.clear();
    belowAverageUtilizedNodes.clear();
    initializeIteration();
  }

  /**
   * Initializes an iteration during balancing. Recognizes over, under,
   * below-average,and under-average utilizes nodes. Decides whether
   * balancing needs to continue or should be stopped.
   *
   * @return true if successfully initialized, otherwise false.
   */
  private boolean initializeIteration() {
    List<DatanodeUsageInfo> nodes;
    try {
      // sorted list in order from most to least used
      nodes = nodeManager.getMostOrLeastUsedDatanodes(true);
    } catch (NullPointerException e) {
      LOG.error("Container Balancer could not retrieve nodes from Node " +
          "Manager.", e);
      stop();
      return false;
    }

    try {
      clusterAvgUtilisation = calculateAvgUtilization(nodes);
    } catch(ArithmeticException e) {
      LOG.warn("Container Balancer failed to initialize an iteration", e);
      return false;
    }
    LOG.info("Average utilization of the cluster is {}", clusterAvgUtilisation);

    // under utilized nodes have utilization(that is, used / capacity) less
    // than lower limit
    double lowerLimit = clusterAvgUtilisation - threshold;

    // over utilized nodes have utilization(that is, used / capacity) greater
    // than upper limit
    double upperLimit = clusterAvgUtilisation + threshold;

    LOG.info("Lower limit for utilization is {}", lowerLimit);
    LOG.info("Upper limit for utilization is {}", upperLimit);

    long numDatanodesToBalance = 0L;
    double overLoadedBytes = 0D, underLoadedBytes = 0D;

    // find over and under utilized nodes
    for (DatanodeUsageInfo node : nodes) {
      double utilization = calculateUtilization(node);
      if (utilization > clusterAvgUtilisation) {
        if (utilization > upperLimit) {
          overUtilizedNodes.add(node);
          numDatanodesToBalance += 1;

          // amount of bytes greater than upper limit in this node
          overLoadedBytes +=
              ratioToBytes(node.getScmNodeStat().getCapacity().get(),
                  utilization) -
                  ratioToBytes(node.getScmNodeStat().getCapacity().get(),
                      upperLimit);
        } else {
          aboveAverageUtilizedNodes.add(node);
        }
      } else if (utilization < clusterAvgUtilisation) {
        if (utilization < lowerLimit) {
          underUtilizedNodes.add(node);
          numDatanodesToBalance += 1;

          // amount of bytes lesser than lower limit in this node
          underLoadedBytes +=
              ratioToBytes(node.getScmNodeStat().getCapacity().get(),
                  lowerLimit) -
                  ratioToBytes(node.getScmNodeStat().getCapacity().get(),
                      utilization);
        } else {
          belowAverageUtilizedNodes.add(node);
        }
      }
    }

    Collections.reverse(underUtilizedNodes);
    Collections.reverse(belowAverageUtilizedNodes);

    long numDatanodesBalanced = 0;
    // count number of nodes that were balanced in previous iteration
    for (DatanodeUsageInfo node : sourceNodes) {
      if (!containsNode(overUtilizedNodes, node) &&
          !containsNode(underUtilizedNodes, node)) {
        numDatanodesBalanced += 1;
      }
    }

    // calculate total number of nodes that have been balanced
    numDatanodesBalanced =
        numDatanodesBalanced + metrics.getNumDatanodesBalanced().get();
    metrics.setNumDatanodesBalanced(new LongMetric(numDatanodesBalanced));
    sourceNodes = new ArrayList<>(
        overUtilizedNodes.size() + underUtilizedNodes.size());

    if (numDatanodesBalanced + numDatanodesToBalance > maxDatanodesToBalance) {
      LOG.info("Approaching Max Datanodes To Balance limit in Container " +
          "Balancer. Stopping Balancer.");
      stop();
      return false;
    } else {
      sourceNodes.addAll(overUtilizedNodes);
      sourceNodes.addAll(underUtilizedNodes);

      if (sourceNodes.isEmpty()) {
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

    if (comparator.compare(listToSearch.get(0),
        listToSearch.get(listToSearch.size() - 1)) < 0) {
      index =
          Collections.binarySearch(listToSearch, node, comparator.reversed());
    } else {
      index = Collections.binarySearch(listToSearch, node, comparator);
    }
    return index >= 0;
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
   * Calculates the average datanode utilization for the specified nodes.
   * Utilization is used space divided by capacity.
   *
   * @param nodes List of DatanodeUsageInfo to find the average utilization for
   * @return Average utilization value
   * @throws ArithmeticException
   */
  private double calculateAvgUtilization(List<DatanodeUsageInfo> nodes)
      throws ArithmeticException {
    SCMNodeStat aggregatedStats = new SCMNodeStat(
        0, 0, 0);
    for (DatanodeUsageInfo node : nodes) {
      aggregatedStats.add(node.getScmNodeStat());
    }
    clusterCapacity = aggregatedStats.getCapacity().get();
    clusterUsed = aggregatedStats.getScmUsed().get();
    clusterRemaining = aggregatedStats.getRemaining().get();

    try {
      return clusterUsed / (double) clusterCapacity;
    } catch (ArithmeticException e) {
      if (clusterCapacity == 0) {
        LOG.warn("Cluster capacity found to be 0 while calculating average " +
            "utilisation of the cluster for Container Balancer, resulting in " +
            "division by 0.", e);
      } else {
        LOG.warn("Exception occurred while calculating average utilisation of" +
            " the cluster for Container Balancer.", e);
      }
      throw e;
    }
  }

  /**
   * Calculates the utilization, that is used space divided by capacity, for
   * the given datanodeUsageInfo.
   *
   * @param datanodeUsageInfo DatanodeUsageInfo to calculate utilization for
   * @return Utilization value
   */
  private double calculateUtilization(DatanodeUsageInfo datanodeUsageInfo) {
    SCMNodeStat stat = datanodeUsageInfo.getScmNodeStat();

    return stat.getScmUsed().get().doubleValue() /
        stat.getCapacity().get().doubleValue();
  }

  /**
   * Stops ContainerBalancer.
   */
  public void stop() {
    LOG.info("Stopping Container Balancer...");
    balancerRunning = false;
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
   * Gets the list of source nodes, that is, the over and under utilized nodes
   * in the cluster.
   *
   * @return List of DatanodeUsageInfo containing source nodes.
   */
  public List<DatanodeUsageInfo> getSourceNodes() {
    return sourceNodes;
  }

  /**
   * Sets the source nodes, that is, the over and under utilized nodes in the
   * cluster.
   *
   * @param sourceNodes List of DatanodeUsageInfo
   */
  public void setSourceNodes(
      List<DatanodeUsageInfo> sourceNodes) {
    this.sourceNodes = sourceNodes;
  }

  /**
   * Checks if ContainerBalancer is currently running.
   *
   * @return true if ContainerBalancer is running, false if not running.
   */
  public boolean isBalancerRunning() {
    return balancerRunning;
  }

  @Override
  public String toString() {
    String status = String.format("Container Balancer status:%n" +
        "%-30s %s%n" +
        "%-30s %b%n", "Key", "Value", "Running", balancerRunning);
    return status + config.toString();
  }
}
