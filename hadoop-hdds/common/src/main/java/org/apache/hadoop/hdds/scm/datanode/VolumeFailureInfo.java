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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.datanode;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;

/**
 * This class is used to record disk failure conditions.
 * The failureDate may be -1, and capacityLost may be 0, because if the DN restarts,
 * we will not know the original capacity of the failed disk.
 */
public final class VolumeFailureInfo implements Comparable<VolumeFailureInfo> {

  private String node;
  private String volumeName;
  private long failureDate;
  private long capacityLost;

  private static final Codec<VolumeFailureInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(HddsProtos.VolumeFailureInfoProto.getDefaultInstance()),
      VolumeFailureInfo::fromProtobuf,
      VolumeFailureInfo::getProtobuf,
      VolumeFailureInfo.class);

  public static Codec<VolumeFailureInfo> getCodec() {
    return CODEC;
  }

  private VolumeFailureInfo(VolumeFailureInfo.Builder b) {
    this.node = b.node;
    this.volumeName = b.volumeName;
    this.failureDate = b.failureDate;
    this.capacityLost = b.capacityLost;
  }

  public static VolumeFailureInfo fromProtobuf(HddsProtos.VolumeFailureInfoProto info) {
    VolumeFailureInfo.Builder builder = new VolumeFailureInfo.Builder();
    builder.setNode(info.getNode())
        .setVolumeName(info.getVolumeName())
        .setFailureDate(info.getFailureDate())
        .setCapacityLost(info.getCapacityLost());
    return builder.build();
  }

  @JsonIgnore
  public HddsProtos.VolumeFailureInfoProto getProtobuf() {
    HddsProtos.VolumeFailureInfoProto.Builder builder =
            HddsProtos.VolumeFailureInfoProto.newBuilder();
    builder.setNode(getNode())
        .setVolumeName(getVolumeName())
        .setFailureDate(getFailureDate())
        .setCapacityLost(getCapacityLost());
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
    private String node;
    private String volumeName;
    private long failureDate;
    private long capacityLost;

    public VolumeFailureInfo.Builder setNode(String pNode) {
      this.node = pNode;
      return this;
    }

    public VolumeFailureInfo.Builder setVolumeName(String pVolumeName) {
      this.volumeName = pVolumeName;
      return this;
    }

    public VolumeFailureInfo.Builder setFailureDate(long pFailureDate) {
      this.failureDate = pFailureDate;
      return this;
    }

    public VolumeFailureInfo.Builder setCapacityLost(long pCapacityLost) {
      this.capacityLost = pCapacityLost;
      return this;
    }

    public VolumeFailureInfo build() {
      return new VolumeFailureInfo(this);
    }
  }

  public String getNode() {
    return node;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public long getFailureDate() {
    return failureDate;
  }

  public long getCapacityLost() {
    return capacityLost;
  }

  @Override
  public int compareTo(VolumeFailureInfo that) {
    Preconditions.checkNotNull(that);
    return new CompareToBuilder()
        .append(this.node, that.node)
        .append(this.volumeName, that.volumeName)
        .append(this.failureDate, that.failureDate)
        .append(this.capacityLost, that.capacityLost)
        .build();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(61, 71)
        .append(this.node)
        .append(this.volumeName)
        .append(this.failureDate)
        .append(this.capacityLost)
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
    final VolumeFailureInfo that = (VolumeFailureInfo) o;
    return new EqualsBuilder()
        .append(this.node, that.node)
        .append(this.volumeName, that.volumeName)
        .append(this.failureDate, that.failureDate)
        .append(this.capacityLost, that.capacityLost)
        .isEquals();
  }
}
