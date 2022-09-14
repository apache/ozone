/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container;

import java.util.Optional;
import java.util.UUID;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * In-memory state of a container replica.
 */
public final class ContainerReplica implements Comparable<ContainerReplica> {

  private final ContainerID containerID;
  private final ContainerReplicaProto.State state;
  private final DatanodeDetails datanodeDetails;
  private final UUID placeOfBirth;
  private final int replicaIndex;

  private Long sequenceId;
  private final long keyCount;
  private final long bytesUsed;


  private ContainerReplica(
      final ContainerID containerID,
      final ContainerReplicaProto.State state,
      final int replicaIndex,
      final DatanodeDetails datanode,
      final UUID originNodeId,
      long keyNum,
      long dataSize) {
    this.containerID = containerID;
    this.state = state;
    this.datanodeDetails = datanode;
    this.placeOfBirth = originNodeId;
    this.keyCount = keyNum;
    this.bytesUsed = dataSize;
    this.replicaIndex = replicaIndex;
  }

  private void setSequenceId(Long seqId) {
    sequenceId = seqId;
  }

  /**
   * Returns the DatanodeDetails to which this replica belongs.
   *
   * @return DatanodeDetails
   */
  public DatanodeDetails getDatanodeDetails() {
    return datanodeDetails;
  }

  /**
   * Returns the UUID of Datanode where this replica originated.
   *
   * @return UUID
   */
  public UUID getOriginDatanodeId() {
    return placeOfBirth;
  }

  /**
   * Returns the state of this replica.
   *
   * @return replica state
   */
  public ContainerReplicaProto.State getState() {
    return state;
  }

  /**
   * Returns the Sequence Id of this replica.
   *
   * @return Sequence Id
   */
  public Long getSequenceId() {
    return sequenceId;
  }

  /**
   * Returns the key count of of this replica.
   *
   * @return Key count
   */
  public long getKeyCount() {
    return keyCount;
  }

  /**
   * Returns the data size of this replica.
   *
   * @return Data size
   */
  public long getBytesUsed() {
    return bytesUsed;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(61, 71)
        .append(containerID)
        .append(datanodeDetails)
        .toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ContainerReplica that = (ContainerReplica) o;

    return new EqualsBuilder()
        .append(containerID, that.containerID)
        .append(datanodeDetails, that.datanodeDetails)
        .isEquals();
  }

  @Override
  public int compareTo(ContainerReplica that) {
    Preconditions.checkNotNull(that);
    return new CompareToBuilder()
        .append(this.containerID, that.containerID)
        .append(this.datanodeDetails, that.datanodeDetails)
        .build();
  }

  /**
   * Returns a new Builder to construct ContainerReplica.
   *
   * @return ContainerReplicaBuilder
   */
  public static ContainerReplicaBuilder newBuilder() {
    return new ContainerReplicaBuilder();
  }

  @Override
  public String toString() {
    return "ContainerReplica{" +
        "containerID=" + containerID +
        ", state=" + state +
        ", datanodeDetails=" + datanodeDetails +
        ", placeOfBirth=" + placeOfBirth +
        ", sequenceId=" + sequenceId +
        ", keyCount=" + keyCount +
        ", bytesUsed=" + bytesUsed + ((replicaIndex > 0) ?
        ",replicaIndex=" + replicaIndex :
        "") +
        '}';
  }

  /**
   * Used for building ContainerReplica instance.
   */
  public static class ContainerReplicaBuilder {

    private ContainerID containerID;
    private ContainerReplicaProto.State state;
    private DatanodeDetails datanode;
    private UUID placeOfBirth;
    private Long sequenceId;
    private long bytesUsed;
    private long keyCount;
    private int replicaIndex;

    /**
     * Set Container Id.
     *
     * @param cID ContainerID
     * @return ContainerReplicaBuilder
     */
    public ContainerReplicaBuilder setContainerID(
        final ContainerID cID) {
      this.containerID = cID;
      return this;
    }

    public ContainerReplicaBuilder setContainerState(
        final ContainerReplicaProto.State  containerState) {
      state = containerState;
      return this;
    }

    /**
     * Set DatanodeDetails.
     *
     * @param datanodeDetails DatanodeDetails
     * @return ContainerReplicaBuilder
     */
    public ContainerReplicaBuilder setDatanodeDetails(
        DatanodeDetails datanodeDetails) {
      datanode = datanodeDetails;
      return this;
    }

    public ContainerReplicaBuilder setReplicaIndex(
        int index) {
      this.replicaIndex = index;
      return this;
    }

    /**
     * Set replica origin node id.
     *
     * @param originNodeId origin node UUID
     * @return ContainerReplicaBuilder
     */
    public ContainerReplicaBuilder setOriginNodeId(UUID originNodeId) {
      placeOfBirth = originNodeId;
      return this;
    }

    /**
     * Set sequence Id of the replica.
     *
     * @param seqId container sequence Id
     * @return ContainerReplicaBuilder
     */
    public ContainerReplicaBuilder setSequenceId(long seqId) {
      sequenceId = seqId;
      return this;
    }

    public ContainerReplicaBuilder setKeyCount(long count) {
      keyCount = count;
      return this;
    }

    public ContainerReplicaBuilder setBytesUsed(long used) {
      bytesUsed = used;
      return this;
    }

    /**
     * Constructs new ContainerReplicaBuilder.
     *
     * @return ContainerReplicaBuilder
     */
    public ContainerReplica build() {
      Preconditions.checkNotNull(containerID,
          "Container Id can't be null");
      Preconditions.checkNotNull(state,
          "Container state can't be null");
      Preconditions.checkNotNull(datanode,
          "DatanodeDetails can't be null");
      ContainerReplica replica = new ContainerReplica(
          containerID, state, replicaIndex, datanode,
          Optional.ofNullable(placeOfBirth).orElse(datanode.getUuid()),
          keyCount, bytesUsed);
      Optional.ofNullable(sequenceId).ifPresent(replica::setSequenceId);
      return replica;
    }
  }

  public int getReplicaIndex() {
    return replicaIndex;
  }
}
