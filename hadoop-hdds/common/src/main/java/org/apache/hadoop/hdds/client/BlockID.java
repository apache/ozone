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

package org.apache.hadoop.hdds.client;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Objects;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * BlockID of Ozone (containerID + localID + blockCommitSequenceId + replicaIndex).
 */
public class BlockID {

  private final ContainerBlockID containerBlockID;
  private long blockCommitSequenceId;
  // null value when not set with private constructor.(This is to avoid confusion of replica index 0 & null value).
  // This value would be only set when deserializing from ContainerProtos.DatanodeBlockID or copying from another
  // BlockID object.
  private final Integer replicaIndex;

  // Represents storage type of Block on a particular Datanode.
  // Note this variable in the OM side will be null,
  // because the OmKeyLocationInfo#getProtobuf Use BlockID#getProtobuf to get Protobuf object,
  // BlockID#getProtobuf does not set storageType value in Protobuf.

  // Currently for java class BlockID, OM and Datanode share the same java class object,
  // but the protobuf objects HddsProtos.BlockID and ContainerProtos.DatanodeBlockID are
  // used for OM and Datanode respectively.
  private StorageType storageType;

  public BlockID(long containerID, long localID) {
    this(containerID, localID, 0, null, null);
  }

  private BlockID(long containerID, long localID, long bcsID, Integer repIndex,
      StorageType storageType) {
    containerBlockID = new ContainerBlockID(containerID, localID);
    blockCommitSequenceId = bcsID;
    this.replicaIndex = repIndex;
    this.storageType = storageType;
  }

  public BlockID(BlockID blockID) {
    this(blockID.getContainerID(), blockID.getLocalID(), blockID.getBlockCommitSequenceId(),
        blockID.getReplicaIndex(), blockID.getStorageType());
  }

  public BlockID(ContainerBlockID containerBlockID) {
    this(containerBlockID, 0, null, null);
  }

  private BlockID(ContainerBlockID containerBlockID, long bcsId, Integer repIndex, StorageType storageType) {
    this.containerBlockID = containerBlockID;
    blockCommitSequenceId = bcsId;
    this.replicaIndex = repIndex;
    this.storageType = storageType;
  }

  public long getContainerID() {
    return containerBlockID.getContainerID();
  }

  public long getLocalID() {
    return containerBlockID.getLocalID();
  }

  public long getBlockCommitSequenceId() {
    return blockCommitSequenceId;
  }

  public void setBlockCommitSequenceId(long blockCommitSequenceId) {
    this.blockCommitSequenceId = blockCommitSequenceId;
  }

  // Can return a null value in case it is not set.
  public Integer getReplicaIndex() {
    return replicaIndex;
  }

  public ContainerBlockID getContainerBlockID() {
    return containerBlockID;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(64);
    appendTo(sb);
    return sb.toString();
  }

  public void appendTo(StringBuilder sb) {
    containerBlockID.appendTo(sb);
    sb.append(" bcsId: ").append(blockCommitSequenceId)
        .append(" replicaIndex: ").append(replicaIndex)
        .append(" storageType: ").append(storageType);
  }

  @JsonIgnore
  public ContainerProtos.DatanodeBlockID getDatanodeBlockIDProtobuf() {
    ContainerProtos.DatanodeBlockID.Builder blockID = getDatanodeBlockIDProtobufBuilder();
    if (replicaIndex != null) {
      blockID.setReplicaIndex(replicaIndex);
    }
    if (storageType != null) {
      blockID.setStorageTypeID(StorageTypeUtils.getID(storageType));
    }
    return blockID.build();
  }

  @JsonIgnore
  public ContainerProtos.DatanodeBlockID.Builder getDatanodeBlockIDProtobufBuilder() {
    return ContainerProtos.DatanodeBlockID.newBuilder().
        setContainerID(containerBlockID.getContainerID())
        .setLocalID(containerBlockID.getLocalID())
        .setBlockCommitSequenceId(blockCommitSequenceId);
  }

  @JsonIgnore
  public static BlockID getFromProtobuf(ContainerProtos.DatanodeBlockID blockID) {
    StorageType storageType = null;
    if (blockID.hasStorageTypeID() && blockID.getStorageTypeID() > 0) {
      storageType = StorageTypeUtils.getStorageTypeFromID(blockID.getStorageTypeID());
    }
    return new BlockID(blockID.getContainerID(),
        blockID.getLocalID(),
        blockID.getBlockCommitSequenceId(),
        blockID.hasReplicaIndex() ? blockID.getReplicaIndex() : null,
        storageType);
  }

  @JsonIgnore
  public HddsProtos.BlockID getProtobuf() {
    return HddsProtos.BlockID.newBuilder()
        .setContainerBlockID(containerBlockID.getProtobuf())
        .setBlockCommitSequenceId(blockCommitSequenceId).build();
  }

  @JsonIgnore
  public static BlockID getFromProtobuf(HddsProtos.BlockID blockID) {
    return new BlockID(
        ContainerBlockID.getFromProtobuf(blockID.getContainerBlockID()),
        blockID.getBlockCommitSequenceId(), null, null);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BlockID blockID = (BlockID) o;
    return this.getContainerBlockID().equals(blockID.getContainerBlockID())
        && this.getBlockCommitSequenceId() == blockID.getBlockCommitSequenceId()
        && Objects.equals(this.getReplicaIndex(), blockID.getReplicaIndex())
        && Objects.equals(this.getStorageType(), blockID.getStorageType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(containerBlockID.getContainerID(), containerBlockID.getLocalID(),
        blockCommitSequenceId, replicaIndex, storageType);
  }
}
