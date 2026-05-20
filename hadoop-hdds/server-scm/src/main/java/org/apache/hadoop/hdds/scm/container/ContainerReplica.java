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

package org.apache.hadoop.hdds.scm.container;

import java.util.Objects;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;

/**
 * In-memory state of a container replica.
 */
public final class ContainerReplica implements Comparable<ContainerReplica> {

  private final ContainerID containerID;
  private final ContainerReplicaProto.State state;
  private final DatanodeDetails datanodeDetails;
  /**
   * The origin creation of this replica.
   * null: origin is the same as {@link #datanodeDetails}.
   */
  private final DatanodeID originDatanodeId;
  /** The position at the pipeline. */
  private final int replicaIndex;

  private final Long sequenceId;
  private final long keyCount;
  private final long bytesUsed;
  private final boolean isEmpty;
  private final ContainerChecksums checksums;

  private ContainerReplica(ContainerReplicaBuilder b) {
    this.containerID = Objects.requireNonNull(b.containerID, "containerID == null");
    this.state = Objects.requireNonNull(b.state, "state == null");
    this.datanodeDetails = Objects.requireNonNull(b.datanode, "datanode == null");
    this.originDatanodeId = b.placeOfBirth;
    this.keyCount = b.keyCount;
    this.bytesUsed = b.bytesUsed;
    this.replicaIndex = b.replicaIndex;
    this.isEmpty = b.isEmpty;
    this.sequenceId = b.sequenceId;
    this.checksums = Objects.requireNonNull(b.checksums, "checksums == null");
  }

  public ContainerID getContainerID() {
    return containerID;
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
  public DatanodeID getOriginDatanodeId() {
    return originDatanodeId != null ? originDatanodeId : datanodeDetails.getID();
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

  public boolean isEmpty() {
    return isEmpty;
  }

  public ContainerChecksums getChecksums() {
    return checksums;
  }

  public long getDataChecksum() {
    return checksums.getDataChecksum();
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
    Objects.requireNonNull(that);
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

  public ContainerReplicaBuilder toBuilder() {
    return newBuilder()
        .setBytesUsed(bytesUsed)
        .setContainerID(containerID)
        .setContainerState(state)
        .setDatanodeDetails(datanodeDetails)
        .setKeyCount(keyCount)
        .setOriginNodeId(originDatanodeId)
        .setReplicaIndex(replicaIndex)
        .setSequenceId(sequenceId)
        .setEmpty(isEmpty)
        .setChecksums(checksums);
  }

  @Override
  public String toString() {
    return "ContainerReplica{" + containerID
        + " (" + state
        + ") currentDN=" + datanodeDetails
        + (originDatanodeId != null ? ", originDN=" + originDatanodeId : " (origin)")
        + ", bcsid=" + sequenceId
        + (replicaIndex > 0 ? ", replicaIndex=" + replicaIndex : "")
        + ", keyCount=" + keyCount
        + ", bytesUsed=" + bytesUsed
        + ", " + (isEmpty ? "empty" : "non-empty")
        + ", checksums=" + checksums
        + '}';
  }

  /**
   * Used for building ContainerReplica instance.
   */
  public static class ContainerReplicaBuilder {

    private ContainerID containerID;
    private ContainerReplicaProto.State state;
    private DatanodeDetails datanode;
    private DatanodeID placeOfBirth;
    private Long sequenceId;
    private long bytesUsed;
    private long keyCount;
    private int replicaIndex;
    private boolean isEmpty;
    private ContainerChecksums checksums;

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
    public ContainerReplicaBuilder setOriginNodeId(DatanodeID originNodeId) {
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

    public ContainerReplicaBuilder setEmpty(boolean empty) {
      isEmpty = empty;
      return this;
    }

    public ContainerReplicaBuilder setChecksums(ContainerChecksums checksums) {
      this.checksums = checksums;
      return this;
    }

    /**
     * Constructs new ContainerReplicaBuilder.
     *
     * @return ContainerReplicaBuilder
     */
    public ContainerReplica build() {
      if (this.checksums == null) {
        this.checksums = ContainerChecksums.unknown();
      }
      return new ContainerReplica(this);
    }
  }

  public int getReplicaIndex() {
    return replicaIndex;
  }
}
