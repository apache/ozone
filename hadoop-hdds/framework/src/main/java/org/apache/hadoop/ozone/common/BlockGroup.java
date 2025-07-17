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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.KeyBlocks;

/**
 * A group of blocks relations relevant, e.g belong to a certain object key.
 */
public final class BlockGroup {

  private String groupID;
  private List<DeletedBlock> deletedBlocks;

  private BlockGroup(String groupID, List<BlockID> blockIDs, List<DeletedBlock> deletedBlocks) {
    this.groupID = groupID;
    this.deletedBlocks = deletedBlocks == null ? new ArrayList<>() : deletedBlocks;
  }

  public List<DeletedBlock> getAllDeletedBlocks() {
    return deletedBlocks;
  }

  public String getGroupID() {
    return groupID;
  }

  public KeyBlocks getProto(boolean isDistributionEnabled) {
    return isDistributionEnabled ? getProtoForDeletedBlock() : getProtoForBlockID();
  }

  public KeyBlocks getProtoForDeletedBlock() {
    KeyBlocks.Builder kbb = KeyBlocks.newBuilder();
    for (DeletedBlock block : deletedBlocks) {
      ScmBlockLocationProtocolProtos.DeletedBlock deletedBlock = ScmBlockLocationProtocolProtos.DeletedBlock
          .newBuilder()
          .setBlockId(block.getBlockID().getProtobuf())
          .setSize(block.getSize())
          .setReplicatedSize(block.getReplicatedSize())
          .build();
      kbb.addDeletedBlocks(deletedBlock);
    }
    return kbb.setKey(groupID).build();
  }

  public KeyBlocks getProtoForBlockID() {
    KeyBlocks.Builder kbb = KeyBlocks.newBuilder();
    for (DeletedBlock block : deletedBlocks) {
      kbb.addBlocks(block.getBlockID().getProtobuf());
    }
    return kbb.setKey(groupID).build();
  }

  /**
   * Parses a KeyBlocks proto to a group of blocks.
   * @param proto KeyBlocks proto.
   * @return a group of blocks.
   */
  public static BlockGroup getFromProto(KeyBlocks proto) {
    return proto.getDeletedBlocksList().isEmpty() ? getFromBlockIDProto(proto) : getFromDeletedBlockProto(proto);
  }

  public static BlockGroup getFromBlockIDProto(KeyBlocks proto) {
    List<DeletedBlock> deletedBlocks = new ArrayList<>();
    for (HddsProtos.BlockID block : proto.getBlocksList()) {
      deletedBlocks.add(new DeletedBlock(new BlockID(block.getContainerBlockID().getContainerID(),
          block.getContainerBlockID().getLocalID()), 0, 0));
    }
    return BlockGroup.newBuilder().setKeyName(proto.getKey())
        .addAllDeletedBlocks(deletedBlocks).build();
  }

  public static BlockGroup getFromDeletedBlockProto(KeyBlocks proto) {
    List<DeletedBlock> blocks = new ArrayList<>();
    for (ScmBlockLocationProtocolProtos.DeletedBlock block : proto.getDeletedBlocksList()) {
      HddsProtos.ContainerBlockID containerBlockId = block.getBlockId().getContainerBlockID();

      blocks.add(new DeletedBlock(new BlockID(containerBlockId.getContainerID(),
          containerBlockId.getLocalID()),
          block.getSize(),
          block.getReplicatedSize()));
    }
    return BlockGroup.newBuilder().setKeyName(proto.getKey())
        .addAllDeletedBlocks(blocks).build();
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
    private List<BlockID> blocksIDs;
    private List<DeletedBlock> blocks;

    public Builder setKeyName(String blockGroupID) {
      this.groupID = blockGroupID;
      return this;
    }

    public Builder addAllBlockIDs(List<BlockID> blockIDs) {
      this.blocksIDs = blockIDs;
      return this;
    }

    public Builder addAllDeletedBlocks(List<DeletedBlock> keyBlocks) {
      this.blocks = keyBlocks;
      return this;
    }

    public BlockGroup build() {
      return new BlockGroup(groupID, blocksIDs, blocks);
    }
  }

}
