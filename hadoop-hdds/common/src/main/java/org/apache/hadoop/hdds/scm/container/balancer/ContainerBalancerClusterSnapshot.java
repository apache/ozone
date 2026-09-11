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
import java.util.List;
import java.util.Objects;

/**
 * Point-in-time cluster imbalance view built from datanode usage protos.
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
  private final long bytesToMove;
  private final double imbalance;
  private final List<NodeUtilization> topSourceNodes;
  private final List<NodeUtilization> bottomTargetNodes;

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
    this.bytesToMove = b.totalOverUtilizedBytes;
    this.imbalance = b.imbalance;
    this.topSourceNodes = Collections.unmodifiableList(
        Objects.requireNonNull(b.topSourceNodes));
    this.bottomTargetNodes = Collections.unmodifiableList(
        Objects.requireNonNull(b.bottomTargetNodes));
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

  public long getBytesToMove() {
    return bytesToMove;
  }

  public double getImbalance() {
    return imbalance;
  }

  public List<NodeUtilization> getTopSourceNodes() {
    return topSourceNodes;
  }

  public List<NodeUtilization> getBottomTargetNodes() {
    return bottomTargetNodes;
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
    private List<NodeUtilization> topSourceNodes = Collections.emptyList();
    private List<NodeUtilization> bottomTargetNodes = Collections.emptyList();

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

    public Builder setImbalance(double imbalance) {
      this.imbalance = imbalance;
      return this;
    }

    public Builder setTopSourceNodes(List<NodeUtilization> topSourceNodes) {
      this.topSourceNodes = topSourceNodes;
      return this;
    }

    public Builder setBottomTargetNodes(List<NodeUtilization> bottomTargetNodes) {
      this.bottomTargetNodes = bottomTargetNodes;
      return this;
    }

    public ContainerBalancerClusterSnapshot build() {
      return new ContainerBalancerClusterSnapshot(this);
    }
  }

  /**
   * Hostname and utilization ratio of a single datanode, retained on the snapshot
   * for per-node display in CLI output (e.g., assessment).
   */
  public static final class NodeUtilization {
    private final String hostname;
    private final double utilization;

    public NodeUtilization(String hostname, double utilization) {
      this.hostname = hostname;
      this.utilization = utilization;
    }

    public String getHostname() {
      return hostname;
    }

    public double getUtilization() {
      return utilization;
    }
  }
}
