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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeUsageInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeUsageInfoProto;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerClusterSnapshot.NodeUtilization;

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
   * Returns true if utilization is above the upper limit (source candidate).
   */
  public static boolean isOverUtilized(double utilization, double upperLimit) {
    return Double.compare(utilization, upperLimit) > 0;
  }

  /**
   * Returns true if utilization is below the lower limit (target candidate).
   */
  public static boolean isUnderUtilized(double utilization, double lowerLimit) {
    return Double.compare(utilization, lowerLimit) < 0;
  }

  /**
   * Bytes above the upper limit on an over-utilized node.
   */
  public static long overUtilizedBytes(long capacity, double utilization, double upperLimit) {
    return ratioToBytes(capacity, utilization) - ratioToBytes(capacity, upperLimit);
  }

  /**
   * Bytes below the lower limit on an under-utilized node.
   */
  public static long underUtilizedBytes(long capacity, double utilization, double lowerLimit) {
    return ratioToBytes(capacity, lowerLimit) - ratioToBytes(capacity, utilization);
  }

  /**
   * Upper utilization band: cluster average plus threshold.
   * Nodes above this are candidates to be source nodes.
   */
  public static double computeUpperLimit(double clusterAvgUtilization, double thresholdRatio) {
    return clusterAvgUtilization + thresholdRatio;
  }

  /**
   * Lower utilization band: cluster average minus threshold.
   * Nodes below this are candidates to be target nodes.
   */
  public static double computeLowerLimit(double clusterAvgUtilization, double thresholdRatio) {
    return clusterAvgUtilization - thresholdRatio;
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
    return analyze(nodes, thresholdRatio, includeNodes, excludeNodes, TOP_NODE_LIMIT);
  }

  /**
   * Builds a cluster snapshot after applying include/exclude filters.
   *
   * @param nodes all nodes from getDatanodeUsageInfo (typically healthy IN_SERVICE)
   * @param thresholdRatio threshold as ratio, e.g. 0.10 for 10%
   * @param includeNodes empty = all included; non-empty = allow-list
   * @param excludeNodes nodes to skip
   * @param topNodeLimit max source/target nodes to include in the snapshot lists
   */
  public static ContainerBalancerClusterSnapshot analyze(
      List<DatanodeUsageInfoProto> nodes,
      double thresholdRatio,
      Set<String> includeNodes,
      Set<String> excludeNodes,
      int topNodeLimit) {
    if (topNodeLimit < 1) {
      throw new IllegalArgumentException("topNodeLimit must be at least 1.");
    }
    List<DatanodeUsageInfoProto> eligible = filterEligibleNodes(nodes, includeNodes, excludeNodes);

    Map<StorageType, List<DatanodeUsageInfoProto>> byType = new EnumMap<>(StorageType.class);
    boolean incomplete = false;
    for (DatanodeUsageInfoProto node : eligible) {
      long reportedCapacity = 0;
      for (StorageTypeUsageInfoProto usage : node.getStorageTypeUsageList()) {
        if (!usage.hasStorageType() || !usage.hasRemaining() || usage.getCapacity() <= 0) {
          continue;
        }
        reportedCapacity += usage.getCapacity();
        StorageType type = StorageTypeUtils.getFromProtobuf(usage.getStorageType());
        byType.computeIfAbsent(type, ignored -> new ArrayList<>()).add(node.toBuilder().clearStorageTypeUsage()
            .setCapacity(usage.getCapacity()).setUsed(usage.getUsed()).setRemaining(usage.getRemaining()).build());
      }
      incomplete |= reportedCapacity != node.getCapacity();
    }
    ContainerBalancerClusterSnapshot overall = analyzeAggregate(eligible, thresholdRatio, topNodeLimit, null);
    if (byType.isEmpty()) {
      return overall;
    }
    Map<StorageType, ContainerBalancerClusterSnapshot> snapshots = new EnumMap<>(StorageType.class);
    byType.forEach((type, usages) -> snapshots.put(type, analyzeAggregate(usages, thresholdRatio, topNodeLimit, type)));
    return combineStorageTypes(overall, snapshots, topNodeLimit, incomplete);
  }

  private static ContainerBalancerClusterSnapshot analyzeAggregate(List<DatanodeUsageInfoProto> eligible,
      double thresholdRatio, int topNodeLimit, StorageType storageType) {
    if (eligible.isEmpty()) {
      return emptySnapshot(thresholdRatio);
    }

    double clusterAvgUtilization = calculateAvgUtilization(eligible);
    double upperLimit = computeUpperLimit(clusterAvgUtilization, thresholdRatio);
    double lowerLimit = computeLowerLimit(clusterAvgUtilization, thresholdRatio);

    List<NodeUtilization> sources = new ArrayList<>();
    List<NodeUtilization> targets = new ArrayList<>();
    double maxUtilization = Double.NEGATIVE_INFINITY;
    double minUtilization = Double.POSITIVE_INFINITY;
    long clusterCapacityBytes = 0;
    long totalOverUtilizedBytes = 0;
    long totalUnderUtilizedBytes = 0;
    Set<String> sourceDatanodes = new HashSet<>();
    Set<String> targetDatanodes = new HashSet<>();

    for (DatanodeUsageInfoProto node : eligible) {
      long capacity = node.getCapacity();
      clusterCapacityBytes += capacity;
      double utilization = calculateNodeUtilization(node);

      maxUtilization = Math.max(maxUtilization, utilization);
      minUtilization = Math.min(minUtilization, utilization);

      if (capacity <= 0) {
        continue;
      }

      String hostname = getDisplayHostname(node);
      String id = DatanodeDetails.getFromProtoBuf(node.getNode()).getID().toString();
      if (isOverUtilized(utilization, upperLimit)) {
        totalOverUtilizedBytes += overUtilizedBytes(capacity, utilization, upperLimit);
        sources.add(new NodeUtilization(hostname, utilization, id, storageType));
        sourceDatanodes.add(id);
      } else if (isUnderUtilized(utilization, lowerLimit)) {
        totalUnderUtilizedBytes += underUtilizedBytes(capacity, utilization, lowerLimit);
        targets.add(new NodeUtilization(hostname, utilization, id, storageType));
        targetDatanodes.add(id);
      }
    }

    sources.sort(Comparator.comparingDouble(NodeUtilization::getUtilization).reversed());
    targets.sort(Comparator.comparingDouble(NodeUtilization::getUtilization));

    double imbalance = maxUtilization - minUtilization;

    return ContainerBalancerClusterSnapshot.newBuilder()
        .setTotalEligibleDatanodes(eligible.size())
        .setClusterAvgUtilization(clusterAvgUtilization)
        .setClusterCapacityBytes(clusterCapacityBytes)
        .setMaxUtilization(maxUtilization)
        .setMinUtilization(minUtilization)
        .setUpperLimit(upperLimit)
        .setLowerLimit(lowerLimit)
        .setSourceCount(sources.size())
        .setTargetCount(targets.size())
        .setTotalOverUtilizedBytes(totalOverUtilizedBytes)
        .setTotalUnderUtilizedBytes(totalUnderUtilizedBytes)
        .setBytesToMove(storageType != null && (sources.isEmpty() || targets.isEmpty()) ? 0 : totalOverUtilizedBytes)
        .setSourceDatanodes(sourceDatanodes)
        .setTargetDatanodes(targetDatanodes)
        .setImbalance(imbalance)
        .setTopSourceNodes(topNodes(sources, topNodeLimit))
        .setBottomTargetNodes(topNodes(targets, topNodeLimit))
        .build();
  }

  private static ContainerBalancerClusterSnapshot combineStorageTypes(ContainerBalancerClusterSnapshot overall,
      Map<StorageType, ContainerBalancerClusterSnapshot> snapshots, int topNodeLimit, boolean incomplete) {
    Set<String> sources = new HashSet<>();
    Set<String> targets = new HashSet<>();
    Map<String, NodeUtilization> topSources = new HashMap<>();
    Map<String, NodeUtilization> bottomTargets = new HashMap<>();
    long overBytes = 0, underBytes = 0, bytesToMove = 0;
    double max = 0, min = 0, imbalance = -1;
    for (ContainerBalancerClusterSnapshot snapshot : snapshots.values()) {
      sources.addAll(snapshot.getSourceDatanodes());
      targets.addAll(snapshot.getTargetDatanodes());
      for (NodeUtilization node : snapshot.getTopSourceNodes()) {
        topSources.merge(node.getDatanodeId(), node,
            (a, b) -> a.getUtilization() >= b.getUtilization() ? a : b);
      }
      for (NodeUtilization node : snapshot.getBottomTargetNodes()) {
        bottomTargets.merge(node.getDatanodeId(), node,
            (a, b) -> a.getUtilization() <= b.getUtilization() ? a : b);
      }
      overBytes += snapshot.getTotalOverUtilizedBytes();
      underBytes += snapshot.getTotalUnderUtilizedBytes();
      bytesToMove += snapshot.getBytesToMove();
      if (snapshot.getImbalance() > imbalance) {
        max = snapshot.getMaxUtilization();
        min = snapshot.getMinUtilization();
        imbalance = snapshot.getImbalance();
      }
    }
    List<NodeUtilization> sourceNodes = new ArrayList<>(topSources.values());
    sourceNodes.sort(Comparator.comparingDouble(NodeUtilization::getUtilization).reversed()
        .thenComparing(NodeUtilization::getDatanodeId));
    List<NodeUtilization> targetNodes = new ArrayList<>(bottomTargets.values());
    targetNodes.sort(Comparator.comparingDouble(NodeUtilization::getUtilization)
        .thenComparing(NodeUtilization::getDatanodeId));
    return ContainerBalancerClusterSnapshot.newBuilder()
        .setTotalEligibleDatanodes(overall.getTotalEligibleDatanodes())
        .setClusterAvgUtilization(overall.getClusterAvgUtilization())
        .setClusterCapacityBytes(overall.getClusterCapacityBytes())
        .setUpperLimit(overall.getUpperLimit()).setLowerLimit(overall.getLowerLimit())
        .setMaxUtilization(max).setMinUtilization(min).setImbalance(imbalance)
        .setSourceCount(sources.size()).setTargetCount(targets.size())
        .setSourceDatanodes(sources).setTargetDatanodes(targets)
        .setTotalOverUtilizedBytes(overBytes).setTotalUnderUtilizedBytes(underBytes).setBytesToMove(bytesToMove)
        .setTopSourceNodes(topNodes(sourceNodes, topNodeLimit))
        .setBottomTargetNodes(topNodes(targetNodes, topNodeLimit))
        .setStorageTypeSnapshots(snapshots).setIncompleteStorageTypeUsage(incomplete).build();
  }

  /**
   * Function that returns whether a datanode should be excluded from cluster analysis.
   * A datanode is excluded if it appears in excludeNodes set, or if
   * includeNodes is non-empty and the datanode is not listed there
   * by hostname or IP address.
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

  private static List<NodeUtilization> topNodes(List<NodeUtilization> nodes, int topNodeLimit) {
    int limit = Math.min(topNodeLimit, nodes.size());
    List<NodeUtilization> result = new ArrayList<>(limit);
    for (int i = 0; i < limit; i++) {
      result.add(nodes.get(i));
    }
    return result;
  }

  private static ContainerBalancerClusterSnapshot emptySnapshot(double thresholdRatio) {
    return ContainerBalancerClusterSnapshot.newBuilder()
        .setTotalEligibleDatanodes(0)
        .setClusterAvgUtilization(0)
        .setClusterCapacityBytes(0)
        .setMaxUtilization(0)
        .setMinUtilization(0)
        .setUpperLimit(computeUpperLimit(0, thresholdRatio))
        .setLowerLimit(computeLowerLimit(0, thresholdRatio))
        .setSourceCount(0)
        .setTargetCount(0)
        .setTotalOverUtilizedBytes(0)
        .setTotalUnderUtilizedBytes(0)
        .setImbalance(0)
        .setTopSourceNodes(Collections.emptyList())
        .setBottomTargetNodes(Collections.emptyList())
        .build();
  }
}
