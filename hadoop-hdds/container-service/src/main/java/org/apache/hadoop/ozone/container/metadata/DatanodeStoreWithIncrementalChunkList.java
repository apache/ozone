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

package org.apache.hadoop.ozone.container.metadata;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_BLOCK;
import static org.apache.hadoop.ozone.OzoneConsts.INCREMENTAL_CHUNK_LIST;
import static org.apache.hadoop.ozone.container.keyvalue.impl.BlockManagerImpl.FULL_CHUNK;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

/**
 * Constructs a datanode store in accordance with schema version 2, which uses
 * three column families/tables:
 * 1. A block data table.
 * 2. A metadata table.
 * 3. A Delete Transaction Table.
 */
public class DatanodeStoreWithIncrementalChunkList extends AbstractDatanodeStore {
 /**
  * Constructs the metadata store and starts the DB services.
  *
  * @param config - Ozone Configuration.
  * @throws IOException - on Failure.
  */
  public DatanodeStoreWithIncrementalChunkList(ConfigurationSource config,
      AbstractDatanodeDBDefinition dbDef, boolean openReadOnly) throws IOException {
    super(config, dbDef, openReadOnly);
  }

  @Override
  public BlockData getCompleteBlockData(BlockData blockData,
      BlockID blockID, String blockKey) throws IOException {
    BlockData lastChunk = null;
    if (blockData == null || isPartialChunkList(blockData)) {
      // check last chunk table
      lastChunk = getLastChunkInfoTable().get(blockKey);
    }

    if (blockData == null) {
      if (lastChunk == null) {
        throw new StorageContainerException(
            NO_SUCH_BLOCK_ERR_MSG + " BlockID : " + blockID, NO_SUCH_BLOCK);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("blockData=(null), lastChunk={}", lastChunk.getChunks());
        }
        return lastChunk;
      }
    } else {
      if (lastChunk != null) {
        reconcilePartialChunks(lastChunk, blockData);
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

  @Override
  public void putBlockByID(BatchOperation batch, boolean incremental,
      long localID, BlockData data, KeyValueContainerData containerData,
      boolean endOfBlock) throws IOException {
    if (!incremental || !isPartialChunkList(data)) {
      // Case (1) old client: override chunk list.
      getBlockDataTable().putWithBatch(
          batch, containerData.getBlockKey(localID), data);
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
      BlockData lastChunk = getLastChunkInfoTable().get(containerData.getBlockKey(localID));
      if (lastChunk != null) {
        reconcilePartialChunks(lastChunk, data);
      }
    }
    // if eob or if the last chunk is full,
    // the 'data' is full so append it to the block table's chunk info
    // and then remove from lastChunkInfo
    BlockData blockData = getBlockDataTable().get(
        containerData.getBlockKey(localID));
    if (blockData == null) {
      // Case 2.1 if the block did not have full chunks before
      // the block's chunk is what received from client this time, plus the chunks in lastChunkInfoTable
      blockData = data;
    } else {
      // case 2.2 the block already has some full chunks
      List<ContainerProtos.ChunkInfo> chunkInfoList = blockData.getChunks();
      blockData.setChunks(new ArrayList<>(chunkInfoList));
      for (ContainerProtos.ChunkInfo chunk : data.getChunks()) {
        blockData.addChunk(chunk);
      }
      blockData.setBlockCommitSequenceId(data.getBlockCommitSequenceId());
    }
    // delete the entry from last chunk info table
    getLastChunkInfoTable().deleteWithBatch(
        batch, containerData.getBlockKey(localID));
    // update block data table
    getBlockDataTable().putWithBatch(batch,
        containerData.getBlockKey(localID), blockData);
  }

  private void putBlockWithPartialChunks(BatchOperation batch, long localID,
      BlockData data, KeyValueContainerData containerData) throws IOException {
    String blockKey = containerData.getBlockKey(localID);
    BlockData blockData = getBlockDataTable().get(blockKey);
    if (data.getChunks().size() == 1) {
      // Case (3.1) replace/update the last chunk info table
      getLastChunkInfoTable().putWithBatch(
          batch, blockKey, data);
      // If the block does not exist in the block data table because it is the first chunk
      // and the chunk is not full, then add an empty block data to populate the block table.
      // This is required because some of the test code and utilities expect the block to be
      // present in the block data table, they don't check the last chunk info table.
      if (blockData == null) {
        // populate blockDataTable with empty chunk list
        blockData = new BlockData(data.getBlockID());
        blockData.addMetadata(INCREMENTAL_CHUNK_LIST, "");
        blockData.setBlockCommitSequenceId(data.getBlockCommitSequenceId());
        getBlockDataTable().putWithBatch(batch, blockKey, blockData);
      }
    } else {
      int lastChunkIndex = data.getChunks().size() - 1;
      // received more than one chunk this time
      List<ContainerProtos.ChunkInfo> lastChunkInfo =
          Collections.singletonList(
              data.getChunks().get(lastChunkIndex));
      if (blockData == null) {
        // Case 3.2: if the block does not exist in the block data table
        List<ContainerProtos.ChunkInfo> chunkInfos =
            new ArrayList<>(data.getChunks());
        chunkInfos.remove(lastChunkIndex);
        data.setChunks(chunkInfos);
        blockData = data;
        LOG.debug("block {} does not have full chunks yet. Adding the " +
            "chunks to it {}", localID, blockData);
      } else {
        // Case 3.3: if the block exists in the block data table,
        // append chunks till except the last one (supposedly partial)
        List<ContainerProtos.ChunkInfo> chunkInfos =
            new ArrayList<>(blockData.getChunks());

        LOG.debug("blockData.getChunks()={}", chunkInfos);
        LOG.debug("data.getChunks()={}", data.getChunks());

        for (int i = 0; i < lastChunkIndex; i++) {
          chunkInfos.add(data.getChunks().get(i));
        }
        blockData.setChunks(chunkInfos);
        blockData.setBlockCommitSequenceId(data.getBlockCommitSequenceId());
      }
      getBlockDataTable().putWithBatch(batch,
          containerData.getBlockKey(localID), blockData);
      // update the last partial chunk
      data.setChunks(lastChunkInfo);
      getLastChunkInfoTable().putWithBatch(
          batch, containerData.getBlockKey(localID), data);
    }
  }
}
