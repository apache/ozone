/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.client;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * State represents persisted data of one specific datanode.
 */
public class MockDatanodeStorage {

  private final Map<DatanodeBlockID, BlockData> blocks = new HashedMap();
  private final Map<Long, List<DatanodeBlockID>>
      containerBlocks = new HashedMap();
  private final Map<BlockID, String> fullBlockData = new HashMap<>();

  private final Map<String, ChunkInfo> chunks = new HashMap<>();

  private final Map<String, ByteString> data = new HashMap<>();

  private IOException exception = null;

  public void setStorageFailed(IOException reason) {
    this.exception = reason;
  }

  public void putBlock(DatanodeBlockID blockID, BlockData blockData) {
    blocks.put(blockID, blockData);
    List<DatanodeBlockID> dnBlocks = containerBlocks
        .getOrDefault(blockID.getContainerID(), new ArrayList<>());
    dnBlocks.add(blockID);
    containerBlocks.put(blockID.getContainerID(), dnBlocks);
  }

  public BlockData getBlock(DatanodeBlockID blockID) {
    return blocks.get(blockID);
  }

  public List<BlockData> listBlock(long containerID) {
    List<DatanodeBlockID> datanodeBlockIDS = containerBlocks.get(containerID);
    List<BlockData> listBlocksData = new ArrayList<>();
    for (DatanodeBlockID dBlock : datanodeBlockIDS) {
      listBlocksData.add(blocks.get(dBlock));
    }
    return listBlocksData;
  }

  public void writeChunk(
      DatanodeBlockID blockID,
      ChunkInfo chunkInfo, ByteString bytes) throws IOException {
    if (exception != null) {
      throw exception;
    }
    data.put(createKey(blockID, chunkInfo),
        ByteString.copyFrom(bytes.toByteArray()));
    chunks.put(createKey(blockID, chunkInfo), chunkInfo);
    fullBlockData
        .put(new BlockID(blockID.getContainerID(), blockID.getLocalID()),
            fullBlockData.getOrDefault(blockID, "")
                .concat(bytes.toStringUtf8()));
  }

  public ChunkInfo readChunkInfo(
      DatanodeBlockID blockID,
      ChunkInfo chunkInfo) {
    return chunks.get(createKey(blockID, chunkInfo));
  }

  public ByteString readChunkData(
      DatanodeBlockID blockID,
      ChunkInfo chunkInfo) {
    return data.get(createKey(blockID, chunkInfo));

  }

  private String createKey(DatanodeBlockID blockId, ChunkInfo chunkInfo) {
    return blockId.getContainerID() + "_" + blockId.getLocalID() + "_"
        + chunkInfo.getChunkName() + "_" + chunkInfo.getOffset();
  }

  public Map<String, ByteString> getAllBlockData() {
    return this.data;
  }

  public String getFullBlockData(BlockID blockID) {
    return this.fullBlockData.get(blockID);
  }

}
