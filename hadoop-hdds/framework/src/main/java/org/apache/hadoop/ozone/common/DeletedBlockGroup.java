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
import org.apache.hadoop.hdds.client.DeletedBlock;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.DeletedKeyBlocks;

/**
 * A group of blocks relations relevant, e.g belong to a certain object key.
 */
public final class DeletedBlockGroup {

  private String groupID;
  private List<DeletedBlock> blocks;

  private DeletedBlockGroup(String groupID, List<DeletedBlock> blocks) {
    this.groupID = groupID;
    this.blocks = blocks;
  }

  public List<DeletedBlock> getAllBlocks() {
    return blocks;
  }

  public String getGroupID() {
    return groupID;
  }

  public DeletedKeyBlocks getProto() {
    DeletedKeyBlocks.Builder kbb = DeletedKeyBlocks.newBuilder();
    for (DeletedBlock block : blocks) {
      kbb.addBlocks(block.getProtobuf());
    }
    return kbb.setKey(groupID).build();
  }

  public static DeletedBlockGroup getFromProto(DeletedKeyBlocks proto) {
    List<DeletedBlock> blocks = new ArrayList<>();
    for (HddsProtos.DeletedBlock block : proto.getBlocksList()) {
      HddsProtos.ContainerBlockID containerBlockId = block.getBlockId().getContainerBlockID();
      blocks.add(new DeletedBlock(new BlockID(containerBlockId.getContainerID(), containerBlockId.getLocalID()),
          block.getUsedBytes()));
    }
    return DeletedBlockGroup.newBuilder().setKeyName(proto.getKey())
        .addAllBlocks(blocks).build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return "DeletedBlockGroup[" +
        "groupID='" + groupID + '\'' +
        ", blocks=" + blocks +
        ']';
  }

  /**
   * BlockGroup instance builder.
   */
  public static class Builder {

    private String groupID;
    private List<DeletedBlock> blocks;

    public Builder setKeyName(String blockGroupID) {
      this.groupID = blockGroupID;
      return this;
    }

    public Builder addAllBlocks(List<DeletedBlock> keyBlocks) {
      this.blocks = keyBlocks;
      return this;
    }

    public DeletedBlockGroup build() {
      return new DeletedBlockGroup(groupID, blocks);
    }
  }

}
