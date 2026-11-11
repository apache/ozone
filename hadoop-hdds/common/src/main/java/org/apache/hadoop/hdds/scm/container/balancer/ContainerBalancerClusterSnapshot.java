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

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.fs.StorageType;

/**
 * Point-in-time cluster imbalance view built from datanode usage protos.
 * Utilization and limits describe whole datanodes. When storage-type reports are available,
 * movement totals and source/target datanode sets combine the per-type results; a datanode is counted once.
 * Combined source/target counts and lists include only types with a positive movement estimate.
 * Per-type snapshots retain all nodes outside their utilization limits.
 */
public final class ContainerBalancerClusterSnapshot {

  private final int totalEligibleDatanodes;
  private final double clusterAvgUtilization;
  private final long clusterCapacityBytes;
  private final double maxUtilization;
  private final double minUtilization;
  private final double upperLimit;
  private final double lowerLimit;
  private final int sourceCount;
  private final int targetCount;
  private final long totalOverUtilizedBytes;
  private final long totalUnderUtilizedBytes;

  // Legacy snapshots use totalOverUtilizedBytes. Typed snapshots exclude storage types without a source/target pair.
  private final long totalBytesToMove;
  private final double imbalance;
  private final double maxImbalance;
  private final StorageType mostImbalancedStorageType;
  private final List<NodeUtilization> topSourceNodesAcrossStorageTypes;
  private final List<NodeUtilization> bottomTargetNodesAcrossStorageTypes;
  private final Map<StorageType, StorageTypeSnapshot> storageTypeSnapshots;
  private final Set<String> sourceDatanodes;
  private final Set<String> targetDatanodes;
  private final boolean incompleteStorageTypeUsage;

  private ContainerBalancerClusterSnapshot(Builder b) {
    this.totalEligibleDatanodes = b.totalEligibleDatanodes;
    this.clusterAvgUtilization = b.clusterAvgUtilization;
    this.clusterCapacityBytes = b.clusterCapacityBytes;
    this.maxUtilization = b.maxUtilization;
    this.minUtilization = b.minUtilization;
    this.upperLimit = b.upperLimit;
    this.lowerLimit = b.lowerLimit;
    this.sourceCount = b.sourceCount;
    this.targetCount = b.targetCount;
    this.totalOverUtilizedBytes = b.totalOverUtilizedBytes;
    this.totalUnderUtilizedBytes = b.totalUnderUtilizedBytes;
    this.totalBytesToMove = b.totalBytesToMove == null ? b.totalOverUtilizedBytes : b.totalBytesToMove;
    this.imbalance = b.imbalance;
    this.maxImbalance = b.maxImbalance == null ? b.imbalance : b.maxImbalance;
    this.mostImbalancedStorageType = b.mostImbalancedStorageType;
    this.topSourceNodesAcrossStorageTypes = Collections.unmodifiableList(
        Objects.requireNonNull(b.topSourceNodesAcrossStorageTypes));
    this.bottomTargetNodesAcrossStorageTypes = Collections.unmodifiableList(
        Objects.requireNonNull(b.bottomTargetNodesAcrossStorageTypes));
    Map<StorageType, StorageTypeSnapshot> byType = new EnumMap<>(StorageType.class);
    byType.putAll(b.storageTypeSnapshots);
    this.storageTypeSnapshots = Collections.unmodifiableMap(byType);
    this.sourceDatanodes = Collections.unmodifiableSet(new HashSet<>(b.sourceDatanodes));
    this.targetDatanodes = Collections.unmodifiableSet(new HashSet<>(b.targetDatanodes));
    this.incompleteStorageTypeUsage = b.incompleteStorageTypeUsage;
  }

  public int getTotalEligibleDatanodes() {
    return totalEligibleDatanodes;
  }

  public double getClusterAvgUtilization() {
    return clusterAvgUtilization;
  }

  public long getClusterCapacityBytes() {
    return clusterCapacityBytes;
  }

  public double getMaxUtilization() {
    return maxUtilization;
  }

  public double getMinUtilization() {
    return minUtilization;
  }

  public double getUpperLimit() {
    return upperLimit;
  }

  public double getLowerLimit() {
    return lowerLimit;
  }

  public int getSourceCount() {
    return sourceCount;
  }

  public int getTargetCount() {
    return targetCount;
  }

  public long getTotalOverUtilizedBytes() {
    return totalOverUtilizedBytes;
  }

  public long getTotalUnderUtilizedBytes() {
    return totalUnderUtilizedBytes;
  }

  /** Returns the combined movement total; retained for existing callers. */
  public long getBytesToMove() {
    return getTotalBytesToMove();
  }

  public long getTotalBytesToMove() {
    return totalBytesToMove;
  }

  /** Returns the largest per-type imbalance, or whole-datanode imbalance for legacy reports. */
  public double getMaxImbalance() {
    return maxImbalance;
  }

  /** Returns the storage type with the largest imbalance, or null for legacy reports. */
  public StorageType getMostImbalancedStorageType() {
    return mostImbalancedStorageType;
  }

  /** Returns the difference between the highest and lowest whole-datanode utilization. */
  public double getImbalance() {
    return imbalance;
  }

  /** Returns source nodes across storage types, or whole-datanode sources for legacy reports. */
  public List<NodeUtilization> getTopSourceNodes() {
    return getTopSourceNodesAcrossStorageTypes();
  }

  public List<NodeUtilization> getTopSourceNodesAcrossStorageTypes() {
    return topSourceNodesAcrossStorageTypes;
  }

  /** Returns target nodes across storage types, or whole-datanode targets for legacy reports. */
  public List<NodeUtilization> getBottomTargetNodes() {
    return getBottomTargetNodesAcrossStorageTypes();
  }

  public List<NodeUtilization> getBottomTargetNodesAcrossStorageTypes() {
    return bottomTargetNodesAcrossStorageTypes;
  }

  public Map<StorageType, StorageTypeSnapshot> getStorageTypeSnapshots() {
    return storageTypeSnapshots;
  }

  public Set<String> getSourceDatanodes() {
    return sourceDatanodes;
  }

  public Set<String> getTargetDatanodes() {
    return targetDatanodes;
  }

  public boolean hasIncompleteStorageTypeUsage() {
    return incompleteStorageTypeUsage;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for building ContainerBalancerClusterSnapshot.
   */
  public static final class Builder {
    private int totalEligibleDatanodes;
    private double clusterAvgUtilization;
    private long clusterCapacityBytes;
    private double maxUtilization;
    private double minUtilization;
    private double upperLimit;
    private double lowerLimit;
    private int sourceCount;
    private int targetCount;
    private long totalOverUtilizedBytes;
    private long totalUnderUtilizedBytes;
    private double imbalance;
    private Double maxImbalance;
    private StorageType mostImbalancedStorageType;
    private List<NodeUtilization> topSourceNodesAcrossStorageTypes = Collections.emptyList();
    private List<NodeUtilization> bottomTargetNodesAcrossStorageTypes = Collections.emptyList();
    private Map<StorageType, StorageTypeSnapshot> storageTypeSnapshots = Collections.emptyMap();
    private Set<String> sourceDatanodes = Collections.emptySet();
    private Set<String> targetDatanodes = Collections.emptySet();
    private Long totalBytesToMove;
    private boolean incompleteStorageTypeUsage;

    private Builder() {
    }

    public Builder setTotalEligibleDatanodes(int totalEligibleDatanodes) {
      this.totalEligibleDatanodes = totalEligibleDatanodes;
      return this;
    }

    public Builder setClusterAvgUtilization(double clusterAvgUtilization) {
      this.clusterAvgUtilization = clusterAvgUtilization;
      return this;
    }

    public Builder setClusterCapacityBytes(long clusterCapacityBytes) {
      this.clusterCapacityBytes = clusterCapacityBytes;
      return this;
    }

    public Builder setMaxUtilization(double maxUtilization) {
      this.maxUtilization = maxUtilization;
      return this;
    }

    public Builder setMinUtilization(double minUtilization) {
      this.minUtilization = minUtilization;
      return this;
    }

    public Builder setUpperLimit(double upperLimit) {
      this.upperLimit = upperLimit;
      return this;
    }

    public Builder setLowerLimit(double lowerLimit) {
      this.lowerLimit = lowerLimit;
      return this;
    }

    public Builder setSourceCount(int sourceCount) {
      this.sourceCount = sourceCount;
      return this;
    }

    public Builder setTargetCount(int targetCount) {
      this.targetCount = targetCount;
      return this;
    }

    public Builder setTotalOverUtilizedBytes(long totalOverUtilizedBytes) {
      this.totalOverUtilizedBytes = totalOverUtilizedBytes;
      return this;
    }

    public Builder setTotalUnderUtilizedBytes(long totalUnderUtilizedBytes) {
      this.totalUnderUtilizedBytes = totalUnderUtilizedBytes;
      return this;
    }

    public Builder setMaxImbalance(double maxImbalance) {
      this.maxImbalance = maxImbalance;
      return this;
    }

    public Builder setMostImbalancedStorageType(StorageType storageType) {
      this.mostImbalancedStorageType = storageType;
      return this;
    }

    public Builder setImbalance(double imbalance) {
      this.imbalance = imbalance;
      return this;
    }

    public Builder setTopSourceNodes(List<NodeUtilization> nodes) {
      return setTopSourceNodesAcrossStorageTypes(nodes);
    }

    public Builder setTopSourceNodesAcrossStorageTypes(List<NodeUtilization> topSourceNodesAcrossStorageTypes) {
      this.topSourceNodesAcrossStorageTypes = topSourceNodesAcrossStorageTypes;
      return this;
    }

    public Builder setBottomTargetNodes(List<NodeUtilization> nodes) {
      return setBottomTargetNodesAcrossStorageTypes(nodes);
    }

    public Builder setBottomTargetNodesAcrossStorageTypes(List<NodeUtilization> bottomTargetNodesAcrossStorageTypes) {
      this.bottomTargetNodesAcrossStorageTypes = bottomTargetNodesAcrossStorageTypes;
      return this;
    }

    public ContainerBalancerClusterSnapshot build() {
      return new ContainerBalancerClusterSnapshot(this);
    }

    public Builder setStorageTypeSnapshots(Map<StorageType, StorageTypeSnapshot> snapshots) {
      this.storageTypeSnapshots = snapshots;
      return this;
    }

    public Builder setSourceDatanodes(Set<String> nodes) {
      this.sourceDatanodes = nodes;
      return this;
    }

    public Builder setTargetDatanodes(Set<String> nodes) {
      this.targetDatanodes = nodes;
      return this;
    }

    public Builder setBytesToMove(long bytes) {
      return setTotalBytesToMove(bytes);
    }

    public Builder setTotalBytesToMove(long bytes) {
      this.totalBytesToMove = bytes;
      return this;
    }

    public Builder setIncompleteStorageTypeUsage(boolean incomplete) {
      this.incompleteStorageTypeUsage = incomplete;
      return this;
    }
  }

  /**
   * Hostname and utilization ratio of a single datanode, retained on the snapshot
   * for per-node display in CLI output (e.g., assessment).
   */
  public static final class NodeUtilization {
    private final String hostname;
    private final double utilization;
    private final String datanodeId;
    private final StorageType storageType;

    public NodeUtilization(String hostname, double utilization) {
      this(hostname, utilization, hostname, null);
    }

    public NodeUtilization(String hostname, double utilization, String datanodeId, StorageType storageType) {
      this.hostname = hostname;
      this.utilization = utilization;
      this.datanodeId = datanodeId;
      this.storageType = storageType;
    }

    public String getHostname() {
      return hostname;
    }

    public double getUtilization() {
      return utilization;
    }

    public String getDatanodeId() {
      return datanodeId;
    }

    public StorageType getStorageType() {
      return storageType;
    }
  }
}
