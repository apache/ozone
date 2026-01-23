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

  public BlockID(long containerID, long localID) {
    this(containerID, localID, 0, null);
  }

  private BlockID(long containerID, long localID, long bcsID, Integer repIndex) {
    containerBlockID = new ContainerBlockID(containerID, localID);
    blockCommitSequenceId = bcsID;
    this.replicaIndex = repIndex;
  }

  public BlockID(BlockID blockID) {
    this(blockID.getContainerID(), blockID.getLocalID(), blockID.getBlockCommitSequenceId(),
        blockID.getReplicaIndex());
  }

  public BlockID(ContainerBlockID containerBlockID) {
    this(containerBlockID, 0, null);
  }

  private BlockID(ContainerBlockID containerBlockID, long bcsId, Integer repIndex) {
    this.containerBlockID = containerBlockID;
    blockCommitSequenceId = bcsId;
    this.replicaIndex = repIndex;
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

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(64);
    appendTo(sb);
    return sb.toString();
  }

  public void appendTo(StringBuilder sb) {
    containerBlockID.appendTo(sb);
    sb.append(" bcsId: ").append(blockCommitSequenceId);
    sb.append(" replicaIndex: ").append(replicaIndex);
  }

  @JsonIgnore
  public ContainerProtos.DatanodeBlockID getDatanodeBlockIDProtobuf() {
    ContainerProtos.DatanodeBlockID.Builder blockID = getDatanodeBlockIDProtobufBuilder();
    if (replicaIndex != null) {
      blockID.setReplicaIndex(replicaIndex);
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
    return new BlockID(blockID.getContainerID(),
        blockID.getLocalID(),
        blockID.getBlockCommitSequenceId(),
        blockID.hasReplicaIndex() ? blockID.getReplicaIndex() : null);
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
        blockID.getBlockCommitSequenceId(), null);
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
        && Objects.equals(this.getReplicaIndex(), blockID.getReplicaIndex());
  }

  @Override
  public int hashCode() {
    return Objects.hash(containerBlockID.getContainerID(), containerBlockID.getLocalID(),
        blockCommitSequenceId, replicaIndex);
  }
}
