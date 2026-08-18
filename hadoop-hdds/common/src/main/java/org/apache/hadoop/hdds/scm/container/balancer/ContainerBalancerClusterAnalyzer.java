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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeUsageInfoProto;

/**
 * Classifies datanode usage protos using the same rules as
 * {@code ContainerBalancerTask.initializeIteration()}.
 */
public final class ContainerBalancerClusterAnalyzer {

  private static final int TOP_NODE_LIMIT = 5;

  private ContainerBalancerClusterAnalyzer() {
  }

  /**
   * Core shared formula: (totalCapacity - totalRemaining) / totalCapacity.
   */
  public static double calculateAvgUtilization(long totalCapacity, long totalRemaining) {
    if (totalCapacity == 0) {
      return 0;
    }
    return (totalCapacity - totalRemaining) / (double) totalCapacity;
  }

  /**
   * Cluster average from datanode usage protos.
   */
  public static double calculateAvgUtilization(List<DatanodeUsageInfoProto> nodes) {
    if (nodes.isEmpty()) {
      return 0;
    }
    long totalCapacity = 0;
    long totalRemaining = 0;
    for (DatanodeUsageInfoProto node : nodes) {
      totalCapacity += node.getCapacity();
      totalRemaining += node.getRemaining();
    }
    return calculateAvgUtilization(totalCapacity, totalRemaining);
  }

  /**
   * Builds a cluster snapshot after applying include/exclude filters.
   *
   * @param nodes all nodes from getDatanodeUsageInfo (typically healthy IN_SERVICE)
   * @param thresholdRatio threshold as ratio, e.g. 0.10 for 10%
   * @param includeNodes empty = all included; non-empty = allow-list
   * @param excludeNodes nodes to skip
   */
  public static ContainerBalancerClusterSnapshot analyze(
      List<DatanodeUsageInfoProto> nodes,
      double thresholdRatio,
      Set<String> includeNodes,
      Set<String> excludeNodes) {
    List<DatanodeUsageInfoProto> eligible = filterEligibleNodes(nodes, includeNodes, excludeNodes);

    if (eligible.isEmpty()) {
      return emptySnapshot(thresholdRatio);
    }

    long clusterCapacityBytes = 0;
    for (DatanodeUsageInfoProto node : eligible) {
      clusterCapacityBytes += node.getCapacity();
    }

    double clusterAvgUtilization = calculateAvgUtilization(eligible);
    double upperLimit = clusterAvgUtilization + thresholdRatio;
    double lowerLimit = clusterAvgUtilization - thresholdRatio;

    List<NodeClassification> sources = new ArrayList<>();
    List<NodeClassification> targets = new ArrayList<>();
    double maxUtilization = Double.NEGATIVE_INFINITY;
    double minUtilization = Double.POSITIVE_INFINITY;
    long totalOverUtilizedBytes = 0;
    long totalUnderUtilizedBytes = 0;

    for (DatanodeUsageInfoProto node : eligible) {
      long capacity = node.getCapacity();
      double utilization = calculateNodeUtilization(node);

      maxUtilization = Math.max(maxUtilization, utilization);
      minUtilization = Math.min(minUtilization, utilization);

      String hostname = getDisplayHostname(node);
      if (Double.compare(utilization, upperLimit) > 0) {
        long overBytes = ratioToBytes(capacity, utilization)
            - ratioToBytes(capacity, upperLimit);
        totalOverUtilizedBytes += overBytes;
        sources.add(new NodeClassification(hostname, utilization));
      } else if (Double.compare(utilization, lowerLimit) < 0) {
        long underBytes = ratioToBytes(capacity, lowerLimit)
            - ratioToBytes(capacity, utilization);
        totalUnderUtilizedBytes += underBytes;
        targets.add(new NodeClassification(hostname, utilization));
      }
    }

    sources.sort(Comparator.comparingDouble(NodeClassification::getUtilization).reversed());
    targets.sort(Comparator.comparingDouble(NodeClassification::getUtilization));

    double imbalance = maxUtilization - minUtilization;

    return new ContainerBalancerClusterSnapshot(
        eligible.size(),
        clusterAvgUtilization,
        clusterCapacityBytes,
        maxUtilization,
        minUtilization,
        upperLimit,
        lowerLimit,
        sources.size(),
        targets.size(),
        totalOverUtilizedBytes,
        totalUnderUtilizedBytes,
        imbalance,
        topHostnames(sources),
        topHostnames(targets));
  }

  /**
   * Function that excludes is datanode should be excluded or not.
   * The node is in exclude set or the node is not in include set
   * when include set is not empty.
   */
  public static boolean shouldExcludeDatanode(DatanodeDetails datanode,
                                       Set<String> excludeNodes, Set<String> includeNodes) {
    if (excludeNodes.contains(datanode.getHostName()) ||
        excludeNodes.contains(datanode.getIpAddress())) {
      return true;
    } else if (!includeNodes.isEmpty()) {
      return !includeNodes.contains(datanode.getHostName()) &&
          !includeNodes.contains(datanode.getIpAddress());
    }
    return false;
  }

  static double calculateNodeUtilization(DatanodeUsageInfoProto node) {
    long capacity = node.getCapacity();
    if (capacity == 0) {
      return 0;
    }
    return (capacity - node.getRemaining()) / (double) capacity;
  }

  private static List<DatanodeUsageInfoProto> filterEligibleNodes(
      List<DatanodeUsageInfoProto> nodes,
      Set<String> includeNodes,
      Set<String> excludeNodes) {
    List<DatanodeUsageInfoProto> eligible = new ArrayList<>();
    for (DatanodeUsageInfoProto node : nodes) {
      if (!node.hasNode()) {
        continue;
      }
      DatanodeDetails datanode = DatanodeDetails.getFromProtoBuf(node.getNode());
      if (!shouldExcludeDatanode(datanode, excludeNodes, includeNodes)) {
        eligible.add(node);
      }
    }
    return eligible;
  }

  /**
   * Calculates the number of used bytes given capacity and utilization ratio.
   *
   * @param nodeCapacity     capacity of the node.
   * @param utilizationRatio used space by capacity ratio of the node.
   * @return number of bytes
   */
  public static long ratioToBytes(long nodeCapacity, double utilizationRatio) {
    return (long) (nodeCapacity * utilizationRatio);
  }

  private static String getDisplayHostname(DatanodeUsageInfoProto node) {
    DatanodeDetails datanode = DatanodeDetails.getFromProtoBuf(node.getNode());
    String hostname = datanode.getHostName();
    if (hostname != null && !hostname.isEmpty()) {
      return hostname;
    }
    return datanode.getIpAddress();
  }

  private static List<String> topHostnames(List<NodeClassification> nodes) {
    int limit = Math.min(TOP_NODE_LIMIT, nodes.size());
    List<String> hostnames = new ArrayList<>(limit);
    for (int i = 0; i < limit; i++) {
      hostnames.add(nodes.get(i).getHostname());
    }
    return hostnames;
  }

  private static ContainerBalancerClusterSnapshot emptySnapshot(double thresholdRatio) {
    return new ContainerBalancerClusterSnapshot(
        0,
        0,
        0,
        0,
        0,
        thresholdRatio,
        -thresholdRatio,
        0,
        0,
        0,
        0,
        0,
        Collections.emptyList(),
        Collections.emptyList());
  }

  private static final class NodeClassification {
    private final String hostname;
    private final double utilization;

    private NodeClassification(String hostname, double utilization) {
      this.hostname = hostname;
      this.utilization = utilization;
    }

    private String getHostname() {
      return hostname;
    }

    private double getUtilization() {
      return utilization;
    }
  }
}
