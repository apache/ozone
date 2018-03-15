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

package org.apache.hadoop.scm.container.common.helpers;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos;
import org.apache.hadoop.ozone.scm.container.ContainerStates.ContainerID;
import org.apache.hadoop.util.Time;

import java.util.Comparator;

/**
 * Class wraps ozone container info.
 */
public class ContainerInfo
    implements Comparator<ContainerInfo>, Comparable<ContainerInfo> {
  private HdslProtos.LifeCycleState state;
  private Pipeline pipeline;
  // Bytes allocated by SCM for clients.
  private long allocatedBytes;
  // Actual container usage, updated through heartbeat.
  private long usedBytes;
  private long numberOfKeys;
  private long lastUsed;
  // The wall-clock ms since the epoch at which the current state enters.
  private long stateEnterTime;
  private String owner;
  private String containerName;
  private long containerID;
  ContainerInfo(
      long containerID,
      final String containerName,
      HdslProtos.LifeCycleState state,
      Pipeline pipeline,
      long allocatedBytes,
      long usedBytes,
      long numberOfKeys,
      long stateEnterTime,
      String owner) {
    this.containerID = containerID;
    this.containerName = containerName;
    this.pipeline = pipeline;
    this.allocatedBytes = allocatedBytes;
    this.usedBytes = usedBytes;
    this.numberOfKeys = numberOfKeys;
    this.lastUsed = Time.monotonicNow();
    this.state = state;
    this.stateEnterTime = stateEnterTime;
    this.owner = owner;
  }

  /**
   * Needed for serialization findbugs.
   */
  public ContainerInfo() {
  }

  public static ContainerInfo fromProtobuf(HdslProtos.SCMContainerInfo info) {
    ContainerInfo.Builder builder = new ContainerInfo.Builder();
    builder.setPipeline(Pipeline.getFromProtoBuf(info.getPipeline()));
    builder.setAllocatedBytes(info.getAllocatedBytes());
    builder.setUsedBytes(info.getUsedBytes());
    builder.setNumberOfKeys(info.getNumberOfKeys());
    builder.setState(info.getState());
    builder.setStateEnterTime(info.getStateEnterTime());
    builder.setOwner(info.getOwner());
    builder.setContainerName(info.getContainerName());
    builder.setContainerID(info.getContainerID());
    return builder.build();
  }

  public long getContainerID() {
    return containerID;
  }

  public String getContainerName() {
    return containerName;
  }

  public HdslProtos.LifeCycleState getState() {
    return state;
  }

  public void setState(HdslProtos.LifeCycleState state) {
    this.state = state;
  }

  public long getStateEnterTime() {
    return stateEnterTime;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public long getAllocatedBytes() {
    return allocatedBytes;
  }

  /**
   * Set Allocated bytes.
   *
   * @param size - newly allocated bytes -- negative size is case of deletes
   * can be used.
   */
  public void updateAllocatedBytes(long size) {
    this.allocatedBytes += size;
  }

  public long getUsedBytes() {
    return usedBytes;
  }

  public long getNumberOfKeys() {
    return numberOfKeys;
  }

  public ContainerID containerID() {
    return new ContainerID(getContainerID());
  }

  /**
   * Gets the last used time from SCM's perspective.
   *
   * @return time in milliseconds.
   */
  public long getLastUsed() {
    return lastUsed;
  }

  public void updateLastUsedTime() {
    lastUsed = Time.monotonicNow();
  }

  public void allocate(long size) {
    // should we also have total container size in ContainerInfo
    // and check before allocating?
    allocatedBytes += size;
  }

  public HdslProtos.SCMContainerInfo getProtobuf() {
    HdslProtos.SCMContainerInfo.Builder builder =
        HdslProtos.SCMContainerInfo.newBuilder();
    builder.setPipeline(getPipeline().getProtobufMessage());
    builder.setAllocatedBytes(getAllocatedBytes());
    builder.setUsedBytes(getUsedBytes());
    builder.setNumberOfKeys(getNumberOfKeys());
    builder.setState(state);
    builder.setStateEnterTime(stateEnterTime);
    builder.setContainerID(getContainerID());

    if (getOwner() != null) {
      builder.setOwner(getOwner());
    }
    builder.setContainerName(getContainerName());
    return builder.build();
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  @Override
  public String toString() {
    return "ContainerInfo{"
        + "state=" + state
        + ", pipeline=" + pipeline
        + ", stateEnterTime=" + stateEnterTime
        + ", owner=" + owner
        + ", containerName='" + containerName
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ContainerInfo that = (ContainerInfo) o;

    return new EqualsBuilder()
        .append(pipeline.getContainerName(), that.pipeline.getContainerName())

        // TODO : Fix this later. If we add these factors some tests fail.
        // So Commenting this to continue and will enforce this with
        // Changes in pipeline where we remove Container Name to
        // SCMContainerinfo from Pipeline.
        // .append(pipeline.getFactor(), that.pipeline.getFactor())
        // .append(pipeline.getType(), that.pipeline.getType())
        .append(owner, that.owner)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(11, 811)
        .append(pipeline.getContainerName())
        .append(pipeline.getFactor())
        .append(pipeline.getType())
        .append(owner)
        .toHashCode();
  }

  /**
   * Compares its two arguments for order.  Returns a negative integer, zero, or
   * a positive integer as the first argument is less than, equal to, or greater
   * than the second.<p>
   *
   * @param o1 the first object to be compared.
   * @param o2 the second object to be compared.
   * @return a negative integer, zero, or a positive integer as the first
   * argument is less than, equal to, or greater than the second.
   * @throws NullPointerException if an argument is null and this comparator
   *                              does not permit null arguments
   * @throws ClassCastException   if the arguments' types prevent them from
   *                              being compared by this comparator.
   */
  @Override
  public int compare(ContainerInfo o1, ContainerInfo o2) {
    return Long.compare(o1.getLastUsed(), o2.getLastUsed());
  }

  /**
   * Compares this object with the specified object for order.  Returns a
   * negative integer, zero, or a positive integer as this object is less than,
   * equal to, or greater than the specified object.
   *
   * @param o the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object is
   * less than, equal to, or greater than the specified object.
   * @throws NullPointerException if the specified object is null
   * @throws ClassCastException   if the specified object's type prevents it
   *                              from being compared to this object.
   */
  @Override
  public int compareTo(ContainerInfo o) {
    return this.compare(this, o);
  }

  /**
   * Builder class for ContainerInfo.
   */
  public static class Builder {
    private HdslProtos.LifeCycleState state;
    private Pipeline pipeline;
    private long allocated;
    private long used;
    private long keys;
    private long stateEnterTime;
    private String owner;
    private String containerName;
    private long containerID;

    public Builder setContainerID(long id) {
      Preconditions.checkState(id >= 0);
      this.containerID = id;
      return this;
    }

    public Builder setState(HdslProtos.LifeCycleState lifeCycleState) {
      this.state = lifeCycleState;
      return this;
    }

    public Builder setPipeline(Pipeline containerPipeline) {
      this.pipeline = containerPipeline;
      return this;
    }

    public Builder setAllocatedBytes(long bytesAllocated) {
      this.allocated = bytesAllocated;
      return this;
    }

    public Builder setUsedBytes(long bytesUsed) {
      this.used = bytesUsed;
      return this;
    }

    public Builder setNumberOfKeys(long keyCount) {
      this.keys = keyCount;
      return this;
    }

    public Builder setStateEnterTime(long time) {
      this.stateEnterTime = time;
      return this;
    }

    public Builder setOwner(String containerOwner) {
      this.owner = containerOwner;
      return this;
    }

    public Builder setContainerName(String container) {
      this.containerName = container;
      return this;
    }

    public ContainerInfo build() {
      return new
          ContainerInfo(containerID, containerName, state, pipeline,
          allocated, used, keys, stateEnterTime, owner);
    }
  }
}
