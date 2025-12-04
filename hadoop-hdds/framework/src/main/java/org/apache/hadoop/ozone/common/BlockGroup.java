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

package org.apache.hadoop.ozone.common;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.KeyBlocks;

/**
 * A group of blocks relations relevant, e.g belong to a certain object key.
 */
public final class BlockGroup {

  private String groupID;
  private List<DeletedBlock> deletedBlocks;
  public static final long SIZE_NOT_AVAILABLE = -1;

  private BlockGroup(String groupID, List<DeletedBlock> deletedBlocks) {
    this.groupID = groupID;
    this.deletedBlocks = deletedBlocks;
  }

  public List<DeletedBlock> getDeletedBlocks() {
    return deletedBlocks;
  }

  public String getGroupID() {
    return groupID;
  }

  public KeyBlocks getProto() {
    KeyBlocks.Builder kbb = KeyBlocks.newBuilder();
    for (DeletedBlock deletedBlock : deletedBlocks) {
      kbb.addBlocks(deletedBlock.getBlockID().getProtobuf());
      kbb.addSize(deletedBlock.getSize());
      kbb.addReplicatedSize(deletedBlock.getReplicatedSize());
    }
    return kbb.setKey(groupID).build();
  }

  /**
   * Parses a KeyBlocks proto to a group of blocks.
   * @param proto KeyBlocks proto.
   * @return a group of blocks.
   */
  public static BlockGroup getFromProto(KeyBlocks proto) {
    List<DeletedBlock> deletedBlocksList = new ArrayList<>();
    for (int i = 0; i < proto.getBlocksCount(); i++) {
      long repSize = SIZE_NOT_AVAILABLE;
      long size = SIZE_NOT_AVAILABLE;
      if (proto.getSizeCount() > i) {
        size = proto.getSize(i);
      }
      if (proto.getReplicatedSizeCount() > i) {
        repSize = proto.getReplicatedSize(i);
      }
      BlockID block = new BlockID(proto.getBlocks(i).getContainerBlockID().getContainerID(),
          proto.getBlocks(i).getContainerBlockID().getLocalID());
      deletedBlocksList.add(new DeletedBlock(block, size, repSize));
    }
    return BlockGroup.newBuilder().setKeyName(proto.getKey())
        .addAllDeletedBlocks(deletedBlocksList)
        .build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return "BlockGroup[" +
        "groupID='" + groupID + '\'' +
        ", deletedBlocks=" + deletedBlocks +
        ']';
  }

  /**
   * BlockGroup instance builder.
   */
  public static class Builder {

    private String groupID;
    private List<DeletedBlock> deletedBlocks;

    public Builder setKeyName(String blockGroupID) {
      this.groupID = blockGroupID;
      return this;
    }

    public Builder addAllDeletedBlocks(List<DeletedBlock> deletedBlockList) {
      this.deletedBlocks = deletedBlockList;
      return this;
    }

    public BlockGroup build() {
      return new BlockGroup(groupID, deletedBlocks);
    }
  }
}
