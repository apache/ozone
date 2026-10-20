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

package org.apache.hadoop.ozone.client;

import static org.apache.hadoop.ozone.OzoneConsts.INCREMENTAL_CHUNK_LIST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State represents persisted data of one specific datanode.
 */
public class MockDatanodeStorage {
  private static final Logger LOG =
      LoggerFactory.getLogger(MockDatanodeStorage.class);
  public static final String FULL_CHUNK = "full";
  public static final ContainerProtos.KeyValue FULL_CHUNK_KV =
      ContainerProtos.KeyValue.newBuilder().setKey(FULL_CHUNK).build();

  private final Map<BlockID, BlockData> blocks = new HashedMap();
  private final Map<Long, List<DatanodeBlockID>>
      containerBlocks = new HashedMap();
  private final Map<BlockID, ByteString> fullBlockData = new HashMap<>();

  private final Map<String, ByteString> data = new HashMap<>();

  private IOException exception = null;
  private final AtomicInteger getBlockFailures = new AtomicInteger();
  private final List<String> failedGetBlockThreads = new CopyOnWriteArrayList<>();
  private final CountDownLatch getBlockFailed = new CountDownLatch(1);
  private volatile CountDownLatch getBlockGate;

  public void setStorageFailed(IOException reason) {
    this.exception = reason;
  }

  /** Makes the next {@code count} GetBlock calls fail with CONTAINER_NOT_FOUND, as after a container move. */
  public void failNextGetBlocks(int count) {
    getBlockFailures.set(count);
  }

  /** Returns true, and records the calling thread, if this GetBlock call should fail. */
  boolean consumeGetBlockFailure() {
    if (getBlockFailures.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
      failedGetBlockThreads.add(Thread.currentThread().getName());
      getBlockFailed.countDown();
      return true;
    }
    return false;
  }

  /** Makes GetBlock calls on this datanode wait, for at most 10 seconds, until a GetBlock call on other has failed. */
  public void holdGetBlocksUntilFailureOn(MockDatanodeStorage other) {
    getBlockGate = other.getBlockFailed;
  }

  void awaitGetBlockGate() {
    CountDownLatch gate = getBlockGate;
    if (gate != null) {
      try {
        gate.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Names of the threads whose GetBlock call failed. */
  public List<String> getFailedGetBlockThreads() {
    return failedGetBlockThreads;
  }

  private boolean isIncrementalChunkList(BlockData blockData) {
    for (ContainerProtos.KeyValue kv : blockData.getMetadataList()) {
      if (kv.getKey().equals(INCREMENTAL_CHUNK_LIST)) {
        return true;
      }
    }
    return false;
  }

  private BlockID toBlockID(DatanodeBlockID datanodeBlockID) {
    return new BlockID(datanodeBlockID.getContainerID(),
        datanodeBlockID.getLocalID());
  }

  public void putBlock(DatanodeBlockID blockID, BlockData blockData) {
    if (isIncrementalChunkList(blockData)) {
      LOG.debug("incremental chunk list");
      putBlockIncremental(blockID, blockData);
    } else {
      LOG.debug("full chunk list");
      putBlockFull(blockID, blockData);
    }
  }

  private boolean isFullChunk(ChunkInfo chunkInfo) {
    return (chunkInfo.getMetadataList().contains(FULL_CHUNK_KV));
  }

  public void putBlockIncremental(
      DatanodeBlockID blockID, BlockData blockData) {
    BlockID id = toBlockID(blockID);
    if (blocks.containsKey(id)) {
      // block already exists. let's append the chunk list to it.
      BlockData existing = blocks.get(id);
      if (existing.getChunksCount() == 0) {
        // empty chunk list. override it.
        putBlockFull(blockID, blockData);
      } else {
        BlockData.Builder blockDataBuilder = pruneLastPartialChunks(existing);
        blockDataBuilder.addAllChunks(blockData.getChunksList());
        blocks.put(id, blockDataBuilder.build());
      }
      // TODO: verify the chunk list beginning/offset/len is sane
    } else {
      // the block does not exist yet, simply add it
      putBlockFull(blockID, blockData);
    }
  }

  private BlockData.Builder pruneLastPartialChunks(BlockData existing) {
    BlockData.Builder blockDataBuilder = BlockData.newBuilder(existing);
    int lastChunkIndex = existing.getChunksCount() - 1;
    // if the last chunk in the existing block is full, append after it.
    ChunkInfo chunkInfo = existing.getChunks(lastChunkIndex);
    if (!isFullChunk(chunkInfo)) {
      // otherwise, remove it and append
      blockDataBuilder.removeChunks(lastChunkIndex);
    }
    return blockDataBuilder;
  }

  public void putBlockFull(DatanodeBlockID blockID, BlockData blockData) {
    BlockID id = toBlockID(blockID);
    blocks.put(id, blockData);
    List<DatanodeBlockID> dnBlocks = containerBlocks
        .getOrDefault(blockID.getContainerID(), new ArrayList<>());
    dnBlocks.add(blockID);
    containerBlocks.put(blockID.getContainerID(), dnBlocks);
  }

  public BlockData getBlock(DatanodeBlockID blockID) {
    BlockID id = toBlockID(blockID);
    //assert blocks.containsKey(blockID);
    if (!blocks.containsKey(id)) {
      StringBuilder sb = new StringBuilder();
      for (BlockID bid : blocks.keySet()) {
        sb.append(bid).append('\n');
      }
      throw new AssertionError("blockID " + id +
          " not found in blocks. Available block ID: \n" + sb);
    }
    return blocks.get(id);
  }

  public List<BlockData> listBlock(long containerID) {
    List<DatanodeBlockID> datanodeBlockIDS = containerBlocks.get(containerID);
    List<BlockData> listBlocksData = new ArrayList<>();
    for (DatanodeBlockID dBlock : datanodeBlockIDS) {
      listBlocksData.add(blocks.get(toBlockID(dBlock)));
    }
    return listBlocksData;
  }

  public void writeChunk(
      DatanodeBlockID blockID,
      ChunkInfo chunkInfo, ByteString bytes) throws IOException {
    if (exception != null) {
      throw exception;
    }
    String blockKey = createKey(blockID);
    ByteString storedBytes = ByteString.copyFrom(bytes.asReadOnlyByteBuffer());
    ByteString block;
    if (data.containsKey(blockKey)) {
      block = data.get(blockKey);
      assert block.size() == chunkInfo.getOffset();
      data.put(blockKey, block.concat(storedBytes));
    } else {
      assert chunkInfo.getOffset() == 0;
      data.put(blockKey, storedBytes);
    }

    BlockID id = toBlockID(blockID);
    fullBlockData.put(id, fullBlockData.getOrDefault(id, ByteString.EMPTY).concat(storedBytes));
  }

  public ByteString readChunkData(
      DatanodeBlockID blockID,
      ChunkInfo chunkInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "readChunkData: blockID={}, offset={}, len={}",
          createKey(blockID), chunkInfo.getOffset(), chunkInfo.getLen());
    }
    ByteString str = data.get(createKey(blockID)).substring(
        (int)chunkInfo.getOffset(),
        (int)chunkInfo.getOffset() + (int)chunkInfo.getLen());
    return str;
  }

  private String createKey(DatanodeBlockID blockId) {
    return blockId.getContainerID() + "_" + blockId.getLocalID();
  }

  public Map<String, ByteString> getAllBlockData() {
    return this.data;
  }

  public ByteString getFullBlockData(BlockID blockID) {
    return this.fullBlockData.get(blockID);
  }

}
