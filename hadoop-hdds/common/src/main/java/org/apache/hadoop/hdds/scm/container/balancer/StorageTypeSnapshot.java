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

import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerClusterSnapshot.NodeUtilization;

/** Utilization and movement assessment for one storage type, independent of the cluster summary. */
public final class StorageTypeSnapshot {

  private final StorageType storageType;
  private final int eligibleDatanodes;
  private final double averageUtilization;
  private final long capacityBytes;
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
  private final Set<String> sourceDatanodes;
  private final Set<String> targetDatanodes;

  StorageTypeSnapshot(StorageType storageType, ContainerBalancerClusterSnapshot analysis) {
    this.storageType = storageType;
    this.eligibleDatanodes = analysis.getTotalEligibleDatanodes();
    this.averageUtilization = analysis.getClusterAvgUtilization();
    this.capacityBytes = analysis.getClusterCapacityBytes();
    this.maxUtilization = analysis.getMaxUtilization();
    this.minUtilization = analysis.getMinUtilization();
    this.upperLimit = analysis.getUpperLimit();
    this.lowerLimit = analysis.getLowerLimit();
    this.sourceCount = analysis.getSourceCount();
    this.targetCount = analysis.getTargetCount();
    this.totalOverUtilizedBytes = analysis.getTotalOverUtilizedBytes();
    this.totalUnderUtilizedBytes = analysis.getTotalUnderUtilizedBytes();
    this.bytesToMove = analysis.getTotalBytesToMove();
    this.imbalance = analysis.getImbalance();
    this.topSourceNodes = analysis.getTopSourceNodesAcrossStorageTypes();
    this.bottomTargetNodes = analysis.getBottomTargetNodesAcrossStorageTypes();
    this.sourceDatanodes = analysis.getSourceDatanodes();
    this.targetDatanodes = analysis.getTargetDatanodes();
  }

  public StorageType getStorageType() {
    return storageType;
  }

  public int getEligibleDatanodes() {
    return eligibleDatanodes;
  }

  public double getAverageUtilization() {
    return averageUtilization;
  }

  public long getCapacityBytes() {
    return capacityBytes;
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

  public Set<String> getSourceDatanodes() {
    return sourceDatanodes;
  }

  public Set<String> getTargetDatanodes() {
    return targetDatanodes;
  }
}
