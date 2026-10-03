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

package org.apache.hadoop.hdds.scm.node;

import jakarta.annotation.Nullable;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Set;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeUsageInfoProto;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;

/**
 * Bundles datanode details with usage statistics.
 */
public class DatanodeUsageInfo {

  private DatanodeDetails datanodeDetails;
  private SCMNodeStat scmNodeStat;
  private int containerCount;
  private int pipelineCount;
  private long reserved;
  private boolean fsUsagePresent;
  private long fsCapacity;
  private long fsAvailable;

  /**
   * Constructs a DatanodeUsageInfo with DatanodeDetails and SCMNodeStat.
   *
   * @param datanodeDetails DatanodeDetails
   * @param scmNodeStat SCMNodeStat
   */
  public DatanodeUsageInfo(
      DatanodeDetails datanodeDetails,
      SCMNodeStat scmNodeStat) {
    this.datanodeDetails = datanodeDetails;
    this.scmNodeStat = scmNodeStat;
    this.containerCount = -1;
    this.pipelineCount = -1;
  }

  /**
   * Compares two DatanodeUsageInfo on the basis of their utilization values,
   * calculated using
   * {@link DatanodeUsageInfo#calculateUtilization(long plusSize)}.
   *
   * @param first DatanodeUsageInfo
   * @param second DatanodeUsageInfo
   * @return a value greater than 0 first is more utilized, lesser than 0 if
   * first is less utilized, and 0 if both have equal utilization values or
   * first.equals(second) is true.
   */
  private static int compareByUtilization(DatanodeUsageInfo first,
                             DatanodeUsageInfo second) {
    if (first.equals(second)) {
      return 0;
    }
    return Double.compare(first.calculateUtilization(),
        second.calculateUtilization());
  }

  /**
   * Calculates utilization of a datanode after adding a specified size.
   * Utilization of a datanode is defined as its used space divided
   * by its capacity. Here, we prefer calculating
   * used space as (capacity - remaining), instead of using
   * {@link SCMNodeStat#getScmUsed()} (see HDDS-5728).
   *
   * @param plusSize the increased size
   * @return (capacity - remaining) / capacity of this datanode
   */
  public double calculateUtilization(long plusSize) {
    long capacity = scmNodeStat.getCapacity().get();
    if (capacity == 0) {
      return 0;
    }
    long numerator = capacity - scmNodeStat.getRemaining().get() + plusSize;
    return numerator / (double) capacity;
  }

  /**
   * Calculates current utilization of a datanode .
   * Utilization of a datanode is defined as its used space divided
   * by its capacity. Here, we prefer calculating
   * used space as (capacity - remaining), instead of using
   * {@link SCMNodeStat#getScmUsed()} (see HDDS-5728).
   *
   * @return (capacity - remaining) / capacity of this datanode
   */
  public double calculateUtilization() {
    return calculateUtilization(0);
  }

  /**
   * Calculates utilization of a single storage type on this datanode, so the
   * balancer can compare nodes on the tier it is actually moving data within.
   *
   * @param plusSize    the increased size
   * @param storageType the storage type to measure, or null to measure the
   *                    datanode as a whole
   * @return (capacity - remaining) / capacity for that storage type, or 0 when
   *         the datanode has no volume of that type
   */
  public double calculateUtilization(long plusSize,
      @Nullable StorageType storageType) {
    if (storageType == null) {
      return calculateUtilization(plusSize);
    }
    long capacity = scmNodeStat.getCapacity(storageType).get();
    if (capacity == 0) {
      return 0;
    }
    long numerator =
        capacity - scmNodeStat.getRemaining(storageType).get() + plusSize;
    return numerator / (double) capacity;
  }

  /**
   * Calculates current utilization of a single storage type on this datanode.
   *
   * @param storageType the storage type to measure, or null to measure the
   *                    datanode as a whole
   */
  public double calculateUtilization(@Nullable StorageType storageType) {
    return calculateUtilization(0, storageType);
  }

  /**
   * Storage types this datanode actually has capacity for. Used by the balancer
   * to decide which tiers are worth considering on this node.
   */
  public Set<StorageType> getStorageTypes() {
    Set<StorageType> types = EnumSet.noneOf(StorageType.class);
    for (StorageType type : StorageType.values()) {
      if (scmNodeStat.getCapacity(type).get() > 0) {
        types.add(type);
      }
    }
    return types;
  }

  /**
   * Sets DatanodeDetails of this DatanodeUsageInfo.
   *
   * @param datanodeDetails the DatanodeDetails to use
   */
  public void setDatanodeDetails(
      DatanodeDetails datanodeDetails) {
    this.datanodeDetails = datanodeDetails;
  }

  /**
   * Sets SCMNodeStat of this DatanodeUsageInfo.
   *
   * @param scmNodeStat the SCMNodeStat to use.
   */
  public void setScmNodeStat(
      SCMNodeStat scmNodeStat) {
    this.scmNodeStat = scmNodeStat;
  }

  /**
   * Gets DatanodeDetails of this DatanodeUsageInfo.
   *
   * @return DatanodeDetails
   */
  public DatanodeDetails getDatanodeDetails() {
    return datanodeDetails;
  }

  public DatanodeID getDatanodeID() {
    return datanodeDetails.getID();
  }

  /**
   * Gets SCMNodeStat of this DatanodeUsageInfo.
   *
   * @return SCMNodeStat
   */
  public SCMNodeStat getScmNodeStat() {
    return scmNodeStat;
  }

  public int getContainerCount() {
    return containerCount;
  }

  public void setContainerCount(int containerCount) {
    this.containerCount = containerCount;
  }

  public int getPipelineCount() {
    return pipelineCount;
  }

  public long getReserved() { 
    return reserved; 
  }

  public void setPipelineCount(int pipelineCount) {
    this.pipelineCount = pipelineCount;
  }

  public void setReserved(long reserved) { 
    this.reserved = reserved; 
  }

  public void setFilesystemUsage(long capacity, long available) {
    this.fsUsagePresent = true;
    this.fsCapacity = capacity;
    this.fsAvailable = available;
  }

  /**
   * Gets Comparator that compares two DatanodeUsageInfo on the basis of
   * their utilization values. Utilization is (capacity - remaining) divided
   * by capacity.
   *
   * @return Comparator to compare two DatanodeUsageInfo. The comparison
   * function returns a value greater than 0 if first DatanodeUsageInfo has
   * greater utilization, a value lesser than 0 if first DatanodeUsageInfo
   * has lesser utilization, and 0 if both have equal utilization values or
   * first.equals(second) is true
   */
  public static Comparator<DatanodeUsageInfo> getMostUtilized() {
    return DatanodeUsageInfo::compareByUtilization;
  }

  /**
   * Checks if the specified Object o is equal to this DatanodeUsageInfo.
   *
   * @param o Object to check
   * @return true if both refer to the same object or if both have the same
   * DatanodeDetails, false otherwise
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatanodeUsageInfo that = (DatanodeUsageInfo) o;
    return datanodeDetails.equals(that.datanodeDetails);
  }

  @Override
  public int hashCode() {
    return datanodeDetails.hashCode();
  }

  /**
   * Converts an object of type DatanodeUsageInfo to Protobuf type
   * HddsProtos.DatanodeUsageInfoProto.
   *
   * @return Protobuf HddsProtos.DatanodeUsageInfo
   */
  public DatanodeUsageInfoProto toProto(int clientVersion) {
    return toProtoBuilder(clientVersion).build();
  }

  private DatanodeUsageInfoProto.Builder toProtoBuilder(int clientVersion) {
    DatanodeUsageInfoProto.Builder builder =
        DatanodeUsageInfoProto.newBuilder();

    if (datanodeDetails != null) {
      builder.setNode(datanodeDetails.toProto(clientVersion));
    }
    if (scmNodeStat != null) {
      builder.setCapacity(scmNodeStat.getCapacity().get());
      builder.setUsed(scmNodeStat.getScmUsed().get());
      builder.setRemaining(scmNodeStat.getRemaining().get());
      builder.setCommitted(scmNodeStat.getCommitted().get());
      builder.setFreeSpaceToSpare(scmNodeStat.getFreeSpaceToSpare().get());
    }

    builder.setContainerCount(containerCount);
    builder.setPipelineCount(pipelineCount);
    builder.setReserved(reserved);
    if (fsUsagePresent) {
      builder.setFsCapacity(fsCapacity);
      builder.setFsAvailable(fsAvailable);
    }
    return builder;
  }
}
