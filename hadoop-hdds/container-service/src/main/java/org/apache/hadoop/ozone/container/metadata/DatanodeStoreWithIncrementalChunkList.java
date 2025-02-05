/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.metadata;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_BLOCK;
import static org.apache.hadoop.ozone.OzoneConsts.INCREMENTAL_CHUNK_LIST;
import static org.apache.hadoop.ozone.container.keyvalue.impl.BlockManagerImpl.FULL_CHUNK;

/**
 * Constructs a datanode store in accordance with schema version 2, which uses
 * three column families/tables:
 * 1. A block data table.
 * 2. A metadata table.
 * 3. A Delete Transaction Table.
 */
public class DatanodeStoreWithIncrementalChunkList extends AbstractDatanodeStore {
  private final Cache<String, Optional<BlockData>> cachedBlockData;
  private final Cache<String, Optional<BlockData>> cachedLastChunkData;
 /**
  * Constructs the metadata store and starts the DB services.
  *
  * @param config - Ozone Configuration.
  * @throws IOException - on Failure.
  */
  public DatanodeStoreWithIncrementalChunkList(ConfigurationSource config,
      AbstractDatanodeDBDefinition dbDef, boolean openReadOnly) throws IOException {
    super(config, dbDef, openReadOnly);

    this.cachedBlockData = CacheBuilder.newBuilder()
        .recordStats()
        .expireAfterAccess(60000, MILLISECONDS)
        .maximumSize(1024)
        .build();
    this.cachedLastChunkData = CacheBuilder.newBuilder()
        .recordStats()
        .expireAfterAccess(60000, MILLISECONDS)
        .maximumSize(1024)
        .build();
  }


  @Override
  public BlockData getCompleteBlockData(BlockData blockData,
      BlockID blockID, String blockKey) throws IOException {
    Optional<BlockData> lastChunk = Optional.empty();
    if (blockData == null || isPartialChunkList(blockData)) {
      // check last chunk table
      lastChunk = getLastChunkData(blockKey);
    }

    if (blockData == null) {
      if (!lastChunk.isPresent()) {
        throw new StorageContainerException(
            NO_SUCH_BLOCK_ERR_MSG + " BlockID : " + blockID, NO_SUCH_BLOCK);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("blockData=(null), lastChunk={}", lastChunk.get().getChunks());
        }
        return lastChunk.get();
      }
    } else {
      if (lastChunk.isPresent()) {
        reconcilePartialChunks(lastChunk.get(), blockData);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("blockData={}, lastChunk=(null)", blockData.getChunks());
        }
      }
    }

    return blockData;
  }

  private void reconcilePartialChunks(
      BlockData lastChunk, BlockData blockData) {
    LOG.debug("blockData={}, lastChunk={}",
        blockData.getChunks(), lastChunk.getChunks());
    Preconditions.checkState(lastChunk.getChunks().size() == 1);
    if (!blockData.getChunks().isEmpty()) {
      ContainerProtos.ChunkInfo lastChunkInBlockData =
              blockData.getChunks().get(blockData.getChunks().size() - 1);
      if (lastChunkInBlockData != null) {
        Preconditions.checkState(
            lastChunkInBlockData.getOffset() + lastChunkInBlockData.getLen()
                == lastChunk.getChunks().get(0).getOffset(),
            "chunk offset does not match");
      }
    }

    // append last partial chunk to the block data
    List<ContainerProtos.ChunkInfo> chunkInfos =
        new ArrayList<>(blockData.getChunks());
    chunkInfos.add(lastChunk.getChunks().get(0));
    blockData.setChunks(chunkInfos);

    blockData.setBlockCommitSequenceId(
        lastChunk.getBlockCommitSequenceId());
  }

  private static boolean isPartialChunkList(BlockData data) {
    return data.getMetadata().containsKey(INCREMENTAL_CHUNK_LIST);
  }

  private static boolean isFullChunk(ContainerProtos.ChunkInfo chunkInfo) {
    for (ContainerProtos.KeyValue kv: chunkInfo.getMetadataList()) {
      if (kv.getKey().equals(FULL_CHUNK)) {
        return true;
      }
    }
    return false;
  }

  // if eob or if the last chunk is full,
  private static boolean shouldAppendLastChunk(boolean endOfBlock,
      BlockData data) {
    if (endOfBlock || data.getChunks().isEmpty()) {
      return true;
    }
    return isFullChunk(data.getChunks().get(data.getChunks().size() - 1));
  }

  public void putBlockByID(BatchOperation batch, boolean incremental,
      long localID, BlockData data, KeyValueContainerData containerData,
      boolean endOfBlock) throws IOException {
    if (!incremental || !isPartialChunkList(data)) {
      // Case (1) old client: override chunk list.
      putBlockData(batch, containerData.getBlockKey(localID), Optional.of(data));
    } else if (shouldAppendLastChunk(endOfBlock, data)) {
      moveLastChunkToBlockData(batch, localID, data, containerData);
    } else {
      // incremental chunk list,
      // not end of block, has partial chunks
      putBlockWithPartialChunks(batch, localID, data, containerData);
    }
  }

  private void moveLastChunkToBlockData(BatchOperation batch, long localID,
      BlockData data, KeyValueContainerData containerData) throws IOException {
    // if data has no chunks, fetch the last chunk info from lastChunkInfoTable
    if (data.getChunks().isEmpty()) {
      Optional<BlockData> lastChunk = getLastChunkData(containerData.getBlockKey(localID));
      lastChunk.ifPresent(blockData -> reconcilePartialChunks(blockData, data));
    }
    // if eob or if the last chunk is full,
    // the 'data' is full so append it to the block table's chunk info
    // and then remove from lastChunkInfo
    Optional<BlockData> blockData = getBlockData(containerData.getBlockKey(localID));
    if (!blockData.isPresent()) {
      // Case 2.1 if the block did not have full chunks before
      // the block's chunk is what received from client this time, plus the chunks in lastChunkInfoTable
      blockData = Optional.of(data);
    } else {
      // case 2.2 the block already has some full chunks
      List<ContainerProtos.ChunkInfo> chunkInfoList = blockData.get().getChunks();
      blockData.get().setChunks(new ArrayList<>(chunkInfoList));
      for (ContainerProtos.ChunkInfo chunk : data.getChunks()) {
        blockData.get().addChunk(chunk);
      }
      blockData.get().setBlockCommitSequenceId(data.getBlockCommitSequenceId());
    }
    // delete the entry from last chunk info table
    deleteLastChunkData(batch, containerData.getBlockKey(localID));
    // update block data table
    putBlockData(batch, containerData.getBlockKey(localID), blockData);
  }

  private void putBlockWithPartialChunks(BatchOperation batch, long localID,
      BlockData data, KeyValueContainerData containerData) throws IOException {
    String blockKey = containerData.getBlockKey(localID);
    Optional<BlockData> blockData = getBlockData(blockKey);
    if (data.getChunks().size() == 1) {
      // Case (3.1) replace/update the last chunk info table
      putLastChunkData(batch, blockKey, data);
      // If the block does not exist in the block data table because it is the first chunk
      // and the chunk is not full, then add an empty block data to populate the block table.
      // This is required because some of the test code and utilities expect the block to be
      // present in the block data table, they don't check the last chunk info table.
      if (!blockData.isPresent()) {
        // populate blockDataTable with empty chunk list
        blockData = Optional.of(new BlockData(data.getBlockID()));
        blockData.get().addMetadata(INCREMENTAL_CHUNK_LIST, "");
        blockData.get().setBlockCommitSequenceId(data.getBlockCommitSequenceId());
        putBlockData(batch, blockKey, blockData);
      }
    } else {
      int lastChunkIndex = data.getChunks().size() - 1;
      // received more than one chunk this time
      List<ContainerProtos.ChunkInfo> lastChunkInfo =
          Collections.singletonList(
              data.getChunks().get(lastChunkIndex));
      if (!blockData.isPresent()) {
        // Case 3.2: if the block does not exist in the block data table
        List<ContainerProtos.ChunkInfo> chunkInfos =
            new ArrayList<>(data.getChunks());
        chunkInfos.remove(lastChunkIndex);
        blockData = Optional.of(new BlockData(data.getBlockID()));
        blockData.get().addMetadata(INCREMENTAL_CHUNK_LIST, "");
        blockData.get().setBlockCommitSequenceId(data.getBlockCommitSequenceId());
        blockData.get().setChunks(chunkInfos);
        LOG.debug("block {} does not have full chunks yet. Adding the " +
            "chunks to it {}", localID, blockData);
      } else {
        // Case 3.3: if the block exists in the block data table,
        // append chunks till except the last one (supposedly partial)
        List<ContainerProtos.ChunkInfo> chunkInfos =
            new ArrayList<>(blockData.get().getChunks());

        LOG.debug("blockData.getChunks()={}", chunkInfos);
        LOG.debug("data.getChunks()={}", data.getChunks());

        for (int i = 0; i < lastChunkIndex; i++) {
          chunkInfos.add(data.getChunks().get(i));
        }
        blockData.get().setChunks(chunkInfos);
        blockData.get().setBlockCommitSequenceId(data.getBlockCommitSequenceId());
      }
      putBlockData(batch, containerData.getBlockKey(localID), blockData);
      // update the last partial chunk
      data.setChunks(lastChunkInfo);
      putLastChunkData(batch, containerData.getBlockKey(localID), data);
    }
  }

  private void putBlockData(BatchOperation batch, String key, Optional<BlockData> blockData)
      throws IOException {
    getBlockDataTable().putWithBatch(batch, key, blockData.get());
    cachedBlockData.put(key, blockData);
  }

  private Optional<BlockData> getBlockData(String key)
      throws IOException {
    try {
      return cachedBlockData.get(key, () -> Optional.ofNullable(getBlockDataTable().get(key)));
    } catch (ExecutionException e) {
      throw (IOException) e.getCause();
    }
  }

  private void putLastChunkData(BatchOperation batch, String key, BlockData blockData)
      throws IOException {
    getLastChunkInfoTable().putWithBatch(batch, key, blockData);
    cachedLastChunkData.put(key, Optional.of(blockData));
  }

  private void deleteLastChunkData(BatchOperation batch, String key)
      throws IOException {
    getLastChunkInfoTable().deleteWithBatch(batch, key);
    cachedLastChunkData.invalidate(key);
  }

  private Optional<BlockData> getLastChunkData(String key)
      throws IOException {
    try {
      return cachedLastChunkData.get(key, () -> Optional.ofNullable(getLastChunkInfoTable().get(key)));
    } catch (ExecutionException e) {
      throw (IOException) e.getCause();
    }
  }
}
