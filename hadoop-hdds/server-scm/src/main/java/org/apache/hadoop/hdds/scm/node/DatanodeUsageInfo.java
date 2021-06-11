/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;

import java.util.Comparator;

/**
 * Bundles datanode details with usage statistics.
 */
public class DatanodeUsageInfo {

  private DatanodeDetails datanodeDetails;
  private SCMNodeStat scmNodeStat;

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
  }

  /**
   * Compares two DatanodeUsageInfo on the basis of remaining space to capacity
   * ratio.
   *
   * @param first DatanodeUsageInfo
   * @param second DatanodeUsageInfo
   * @return a value greater than 0 if second has higher remaining to
   * capacity ratio, a value lesser than 0 if first has higher remaining to
   * capacity ratio, and 0 if both have equal ratios or first.equals(second)
   * is true
   */
  private static int compareByRemainingRatio(DatanodeUsageInfo first,
                             DatanodeUsageInfo second) {
    if (first.equals(second)) {
      return 0;
    }
    return first.getScmNodeStat()
        .compareByRemainingRatio(second.getScmNodeStat());
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

  /**
   * Gets SCMNodeStat of this DatanodeUsageInfo.
   *
   * @return SCMNodeStat
   */
  public SCMNodeStat getScmNodeStat() {
    return scmNodeStat;
  }

  /**
   * Gets Comparator that compares two DatanodeUsageInfo on the basis of
   * remaining space to capacity ratio.
   *
   * @return Comparator to compare two DatanodeUsageInfo. The comparison
   * function returns a value greater than 0 if second DatanodeUsageInfo has
   * greater remaining space to capacity ratio, a value lesser than 0 if
   * first DatanodeUsageInfo has greater remaining space to capacity ratio,
   * and 0 if both have equal ratios or first.equals(second) is true
   */
  public static Comparator<DatanodeUsageInfo> getMostUsedByRemainingRatio() {
    return DatanodeUsageInfo::compareByRemainingRatio;
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
  public HddsProtos.DatanodeUsageInfoProto toProto() {
    return toProtoBuilder().build();
  }

  private HddsProtos.DatanodeUsageInfoProto.Builder toProtoBuilder() {
    HddsProtos.DatanodeUsageInfoProto.Builder builder =
        HddsProtos.DatanodeUsageInfoProto.newBuilder();

    if (datanodeDetails != null) {
      builder.setNode(
          datanodeDetails.toProto(datanodeDetails.getCurrentVersion()));
    }
    if (scmNodeStat != null) {
      builder.setCapacity(scmNodeStat.getCapacity().get());
      builder.setUsed(scmNodeStat.getScmUsed().get());
      builder.setRemaining(scmNodeStat.getRemaining().get());
    }
    return builder;
  }
}
