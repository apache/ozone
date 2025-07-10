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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * DeletedBlock of Ozone (BlockID + usedBytes).
 */
public class DeletedBlock {

  private BlockID blockID;
  private long replicatedSize;
  private long unreplicatedSize;

  public DeletedBlock(BlockID blockID, long replicatedSize, long unreplicatedSize) {
    this.blockID = blockID;
    this.replicatedSize = replicatedSize;
    this.unreplicatedSize = unreplicatedSize;
  }

  public BlockID getBlockID() {
    return this.blockID;
  }

  public long getReplicatedSize() {
    return this.replicatedSize;
  }

  public long getUnreplicatedSize() {
    return this.unreplicatedSize;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(64);
    appendTo(sb);
    return sb.toString();
  }

  public void appendTo(StringBuilder sb) {
    sb.append(" localID: ").append(blockID.getContainerBlockID().getLocalID());
    sb.append(" containerID: ").append(blockID.getContainerBlockID().getContainerID());
    sb.append(" replicatedSize: ").append(replicatedSize);
    sb.append(" unreplicatedSize: ").append(unreplicatedSize);
  }

  @JsonIgnore
  public HddsProtos.DeletedBlock getProtobuf() {
    return HddsProtos.DeletedBlock.newBuilder()
        .setBlockId(blockID.getProtobuf())
        .setReplicatedSize(replicatedSize)
        .setUnreplicatedSize(unreplicatedSize)
        .build();
  }

  @JsonIgnore
  public static DeletedBlock getFromProtobuf(HddsProtos.DeletedBlock deletedBlockID) {
    return new DeletedBlock(
        BlockID.getFromProtobuf(deletedBlockID.getBlockId()),
        deletedBlockID.getReplicatedSize(),
        deletedBlockID.getUnreplicatedSize());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeletedBlock delBlockID = (DeletedBlock) o;
    return this.getBlockID().equals(delBlockID.getBlockID())
        && this.getReplicatedSize() == delBlockID.getReplicatedSize()
        && this.getUnreplicatedSize() == delBlockID.getUnreplicatedSize();
  }

  @Override
  public int hashCode() {
    return Objects.hash(blockID.getContainerBlockID().getContainerID(), blockID.getContainerBlockID().getLocalID(),
        replicatedSize, unreplicatedSize);
  }
}
