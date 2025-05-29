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

package org.apache.hadoop.hdds.protocol;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Objects;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * This class is used to record disk failure conditions.
 * The failureTime may be 0, and capacityLost may be 0, because if the DN restarts,
 * we will not know the original capacity of the failed disk.
 */
public final class VolumeInfo implements Comparable<VolumeInfo> {

  private DatanodeID datanodeID;
  private String hostName;
  private String volumeName;
  private boolean failed;
  private long failureTime;
  private long capacity;

  private VolumeInfo(VolumeInfo.Builder b) {
    this.datanodeID = b.datanodeID;
    this.volumeName = b.volumeName;
    this.failureTime = b.failureTime;
    this.hostName = b.hostName;
    this.failed = b.failed;
    this.capacity = b.capacity;
  }

  public static VolumeInfo fromProtobuf(HddsProtos.VolumeInfoProto info) {
    VolumeInfo.Builder builder = new VolumeInfo.Builder();
    builder.setDatanodeID(DatanodeID.fromProto(info.getDataNodeId()))
        .setHostName(info.getHostName())
        .setFailed(info.getFailed())
        .setVolumeName(info.getVolumeName())
        .setFailureTime(info.getFailureTime())
        .setCapacity(info.getCapacity());
    return builder.build();
  }

  @JsonIgnore
  public HddsProtos.VolumeInfoProto getProtobuf() {
    HddsProtos.VolumeInfoProto.Builder builder =
        HddsProtos.VolumeInfoProto.newBuilder();
    final HddsProtos.DatanodeIDProto id = getDatanodeID().toProto();
    builder.setDataNodeId(id)
        .setHostName(getHostName())
        .setFailed(isFailed())
        .setVolumeName(getVolumeName())
        .setFailureTime(getFailureTime())
        .setCapacity(getCapacity());
    return builder.build();
  }

  /**
   * Builder class for creating an instance of a complex object.
   * <p>
   * This Builder provides a fluent interface for gradually setting
   * the object's properties. Finally, the build() method is used
   * to create the object.
   * </p>
   */
  public static class Builder {
    private DatanodeID datanodeID;
    private String hostName;
    private boolean failed;
    private String volumeName;
    private long failureTime;
    private long capacity;

    public VolumeInfo.Builder setDatanodeID(DatanodeID pDatanodeID) {
      this.datanodeID = pDatanodeID;
      return this;
    }

    public VolumeInfo.Builder setHostName(String pHostName) {
      this.hostName = pHostName;
      return this;
    }

    public VolumeInfo.Builder setFailed(boolean pFailed) {
      this.failed = pFailed;
      return this;
    }

    public VolumeInfo.Builder setVolumeName(String pVolumeName) {
      this.volumeName = pVolumeName;
      return this;
    }

    public VolumeInfo.Builder setFailureTime(long pFailureTime) {
      this.failureTime = pFailureTime;
      return this;
    }

    public VolumeInfo.Builder setCapacity(long pCapacity) {
      this.capacity = pCapacity;
      return this;
    }

    public VolumeInfo build() {
      return new VolumeInfo(this);
    }
  }

  public DatanodeID getDatanodeID() {
    return datanodeID;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public long getFailureTime() {
    return failureTime;
  }

  public long getCapacity() {
    return capacity;
  }

  public String getHostName() {
    return hostName;
  }

  public boolean isFailed() {
    return failed;
  }

  @Override
  public int compareTo(VolumeInfo that) {
    Objects.requireNonNull(that);
    return new CompareToBuilder()
        .append(this.datanodeID, that.datanodeID)
        .append(this.hostName, that.hostName)
        .append(this.failed, that.failed)
        .append(this.volumeName, that.volumeName)
        .append(this.failureTime, that.failureTime)
        .append(this.capacity, that.capacity)
        .build();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(61, 71)
        .append(this.datanodeID)
        .append(this.hostName)
        .append(this.failed)
        .append(this.volumeName)
        .append(this.failureTime)
        .append(this.capacity)
        .toHashCode();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final VolumeInfo that = (VolumeInfo) o;
    return new EqualsBuilder()
        .append(this.datanodeID, that.datanodeID)
        .append(this.hostName, that.hostName)
        .append(this.failed, that.failed)
        .append(this.volumeName, that.volumeName)
        .append(this.failureTime, that.failureTime)
        .append(this.capacity, that.capacity)
        .isEquals();
  }
}
