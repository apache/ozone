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
  private final List<String> topSourceNodeHostnames;
  private final List<String> bottomTargetNodeHostnames;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public ContainerBalancerClusterSnapshot(
      int totalEligibleDatanodes,
      double clusterAvgUtilization,
      long clusterCapacityBytes,
      double maxUtilization,
      double minUtilization,
      double upperLimit,
      double lowerLimit,
      int sourceCount,
      int targetCount,
      long totalOverUtilizedBytes,
      long totalUnderUtilizedBytes,
      long bytesToMove,
      double imbalance,
      List<String> topSourceNodeHostnames,
      List<String> bottomTargetNodeHostnames) {
    this.totalEligibleDatanodes = totalEligibleDatanodes;
    this.clusterAvgUtilization = clusterAvgUtilization;
    this.clusterCapacityBytes = clusterCapacityBytes;
    this.maxUtilization = maxUtilization;
    this.minUtilization = minUtilization;
    this.upperLimit = upperLimit;
    this.lowerLimit = lowerLimit;
    this.sourceCount = sourceCount;
    this.targetCount = targetCount;
    this.totalOverUtilizedBytes = totalOverUtilizedBytes;
    this.totalUnderUtilizedBytes = totalUnderUtilizedBytes;
    this.bytesToMove = bytesToMove;
    this.imbalance = imbalance;
    this.topSourceNodeHostnames = Collections.unmodifiableList(
        Objects.requireNonNull(topSourceNodeHostnames));
    this.bottomTargetNodeHostnames = Collections.unmodifiableList(
        Objects.requireNonNull(bottomTargetNodeHostnames));
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

  public List<String> getTopSourceNodeHostnames() {
    return topSourceNodeHostnames;
  }

  public List<String> getBottomTargetNodeHostnames() {
    return bottomTargetNodeHostnames;
  }
}
