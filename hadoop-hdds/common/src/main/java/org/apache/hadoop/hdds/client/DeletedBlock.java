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
 * BlockID of Ozone (containerID + localID + blockCommitSequenceId + replicaIndex).
 */
public class DeletedBlock {

  private BlockID blockID;
  private long usedBytes;

  public DeletedBlock(BlockID blockID, long usedBytes) {
    this.blockID = blockID;
    this.usedBytes = usedBytes;
  }

  public BlockID getBlockID() {
    return this.blockID;
  }

  public long getUsedBytes() {
    return this.usedBytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(64);
    appendTo(sb);
    return sb.toString();
  }

  public void appendTo(StringBuilder sb) {
    sb.append(" blockIS: ").append(blockID.getContainerBlockID().getLocalID());
    sb.append(" usedSpace: ").append(usedBytes);
  }

  @JsonIgnore
  public HddsProtos.DeletedBlock getProtobuf() {
    return HddsProtos.DeletedBlock.newBuilder()
        .setBlockId(blockID.getProtobuf())
        .setUsedBytes(usedBytes).build();
  }

  @JsonIgnore
  public static DeletedBlock getFromProtobuf(HddsProtos.DeletedBlock deletedBlockID) {
    return new DeletedBlock(
        BlockID.getFromProtobuf(deletedBlockID.getBlockId()), deletedBlockID.getUsedBytes());
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
        && this.getUsedBytes() == delBlockID.getUsedBytes();
  }

  @Override
  public int hashCode() {
    return Objects.hash(blockID.getContainerBlockID().getContainerID(), blockID.getContainerBlockID().getLocalID(),
        usedBytes);
  }
}
