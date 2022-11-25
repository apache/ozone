/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;

import com.google.common.base.Preconditions;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.BCSID_MISMATCH;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_BLOCK;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNKNOWN_BCSID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for performing block related operations on the KeyValue
 * Container.
 */
public class BlockManagerImpl implements BlockManager {

  static final Logger LOG = LoggerFactory.getLogger(BlockManagerImpl.class);

  private ConfigurationSource config;

  private static final String DB_NULL_ERR_MSG = "DB cannot be null here";
  private static final String NO_SUCH_BLOCK_ERR_MSG =
      "Unable to find the block.";

  // Default Read Buffer capacity when Checksum is not present
  private final long defaultReadBufferCapacity;

  /**
   * Constructs a Block Manager.
   *
   * @param conf - Ozone configuration
   */
  public BlockManagerImpl(ConfigurationSource conf) {
    Preconditions.checkNotNull(conf, "Config cannot be null");
    this.config = conf;
    this.defaultReadBufferCapacity = (long) config.getStorageSize(
        ScmConfigKeys.OZONE_CHUNK_READ_BUFFER_DEFAULT_SIZE_KEY,
        ScmConfigKeys.OZONE_CHUNK_READ_BUFFER_DEFAULT_SIZE_DEFAULT,
        StorageUnit.BYTES);
  }

  /**
   * Puts or overwrites a block.
   *
   * @param container - Container for which block need to be added.
   * @param data     - BlockData.
   * @return length of the block.
   * @throws IOException
   */
  @Override
  public long putBlock(Container container, BlockData data) throws IOException {
    return putBlock(container, data, true);
  }
  /**
   * Puts or overwrites a block.
   *
   * @param container - Container for which block need to be added.
   * @param data - BlockData.
   * @param endOfBlock - The last putBlock call for this block (when
   *                   all the chunks are written and stream is closed)
   * @return length of the block.
   * @throws IOException
   */
  @Override
  public long putBlock(Container container, BlockData data,
      boolean endOfBlock) throws IOException {
    return persistPutBlock(
        (KeyValueContainer) container,
        data,
        config,
        endOfBlock);
  }

  public static long persistPutBlock(KeyValueContainer container,
      BlockData data, ConfigurationSource config, boolean endOfBlock)
      throws IOException {
    Preconditions.checkNotNull(data, "BlockData cannot be null for put " +
        "operation.");
    Preconditions.checkState(data.getContainerID() >= 0, "Container Id " +
        "cannot be negative");

    KeyValueContainerData containerData = container.getContainerData();

    // We are not locking the key manager since LevelDb serializes all actions
    // against a single DB. We rely on DB level locking to avoid conflicts.
    try (DBHandle db = BlockUtils.getDB(containerData, config)) {
      // This is a post condition that acts as a hint to the user.
      // Should never fail.
      Preconditions.checkNotNull(db, DB_NULL_ERR_MSG);

      long bcsId = data.getBlockCommitSequenceId();
      long containerBCSId = containerData.getBlockCommitSequenceId();

      // default blockCommitSequenceId for any block is 0. It the putBlock
      // request is not coming via Ratis(for test scenarios), it will be 0.
      // In such cases, we should overwrite the block as well
      if ((bcsId != 0) && (bcsId <= containerBCSId)) {
        // Since the blockCommitSequenceId stored in the db is greater than
        // equal to blockCommitSequenceId to be updated, it means the putBlock
        // transaction is reapplied in the ContainerStateMachine on restart.
        // It also implies that the given block must already exist in the db.
        // just log and return
        LOG.debug("blockCommitSequenceId {} in the Container Db is greater"
                + " than the supplied value {}. Ignoring it",
            containerBCSId, bcsId);
        return data.getSize();
      }

      // Check if the block is present in the pendingPutBlockCache for the
      // container to determine whether the blockCount is already incremented
      // for this block in the DB or not.
      long localID = data.getLocalID();
      boolean isBlockInCache = container.isBlockInPendingPutBlockCache(localID);
      boolean incrBlockCount = false;

      // update the blockData as well as BlockCommitSequenceId here
      try (BatchOperation batch = db.getStore().getBatchHandler()
          .initBatchOperation()) {

        // If the block does not exist in the pendingPutBlockCache of the
        // container, then check the DB to ascertain if it exists or not.
        // If block exists in cache, blockCount should not be incremented.
        if (!isBlockInCache) {
          if (db.getStore().getBlockDataTable().get(
              containerData.blockKey(localID)) == null) {
            // Block does not exist in DB => blockCount needs to be
            // incremented when the block is added into DB.
            incrBlockCount = true;
          }
        }

        db.getStore().getBlockDataTable().putWithBatch(
            batch, containerData.blockKey(localID), data);
        if (bcsId != 0) {
          db.getStore().getMetadataTable().putWithBatch(
              batch, containerData.bcsIdKey(), bcsId);
        }

        // Set Bytes used, this bytes used will be updated for every write and
        // only get committed for every put block. In this way, when datanode
        // is up, for computation of disk space by container only committed
        // block length is used, And also on restart the blocks committed to DB
        // is only used to compute the bytes used. This is done to keep the
        // current behavior and avoid DB write during write chunk operation.
        db.getStore().getMetadataTable().putWithBatch(
            batch, containerData.bytesUsedKey(),
            containerData.getBytesUsed());

        // Set Block Count for a container.
        if (incrBlockCount) {
          db.getStore().getMetadataTable().putWithBatch(
              batch, containerData.blockCountKey(),
              containerData.getBlockCount() + 1);
        }

        db.getStore().getBatchHandler().commitBatchOperation(batch);
      }

      if (bcsId != 0) {
        container.updateBlockCommitSequenceId(bcsId);
      }

      // Increment block count and add block to pendingPutBlockCache
      // in-memory after the DB update.
      if (incrBlockCount) {
        containerData.incrBlockCount();
      }

      // If the Block is not in PendingPutBlockCache (and it is not endOfBlock),
      // add it there so that subsequent putBlock calls for this block do not
      // have to read the DB to check for block existence
      if (!isBlockInCache && !endOfBlock) {
        container.addToPendingPutBlockCache(localID);
      } else if (isBlockInCache && endOfBlock) {
        // Remove the block from the PendingPutBlockCache as there would not
        // be any more writes to this block
        container.removeFromPendingPutBlockCache(localID);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Block " + data.getBlockID() + " successfully committed with bcsId "
                + bcsId + " chunk size " + data.getChunks().size());
      }
      return data.getSize();
    }
  }

  /**
   * Gets an existing block.
   *
   * @param container - Container from which block need to be fetched.
   * @param blockID - BlockID of the block.
   * @return Key Data.
   * @throws IOException
   */
  @Override
  public BlockData getBlock(Container container, BlockID blockID)
      throws IOException {
    long bcsId = blockID.getBlockCommitSequenceId();
    Preconditions.checkNotNull(blockID,
        "BlockID cannot be null in GetBlock request");
    Preconditions.checkNotNull(container,
        "Container cannot be null");

    KeyValueContainerData containerData = (KeyValueContainerData) container
        .getContainerData();
    long containerBCSId = containerData.getBlockCommitSequenceId();
    if (containerBCSId < bcsId) {
      throw new StorageContainerException(
          "Unable to find the block with bcsID " + bcsId + " .Container "
              + containerData.getContainerID() + " bcsId is "
              + containerBCSId + ".", UNKNOWN_BCSID);
    }

    try (DBHandle db = BlockUtils.getDB(containerData, config)) {
      // This is a post condition that acts as a hint to the user.
      // Should never fail.
      Preconditions.checkNotNull(db, DB_NULL_ERR_MSG);
      BlockData blockData = getBlockByID(db, blockID, containerData);
      long id = blockData.getBlockID().getBlockCommitSequenceId();
      if (id < bcsId) {
        throw new StorageContainerException(
            "bcsId " + bcsId + " mismatches with existing block Id "
                + id + " for block " + blockID + ".", BCSID_MISMATCH);
      }
      return blockData;
    }
  }

  /**
   * Returns the length of the committed block.
   *
   * @param container - Container from which block need to be fetched.
   * @param blockID - BlockID of the block.
   * @return length of the block.
   * @throws IOException in case, the block key does not exist in db.
   */
  @Override
  public long getCommittedBlockLength(Container container, BlockID blockID)
      throws IOException {
    KeyValueContainerData containerData = (KeyValueContainerData) container
        .getContainerData();
    try (DBHandle db = BlockUtils.getDB(containerData, config)) {
      // This is a post condition that acts as a hint to the user.
      // Should never fail.
      Preconditions.checkNotNull(db, DB_NULL_ERR_MSG);
      BlockData blockData = getBlockByID(db, blockID, containerData);
      return blockData.getSize();
    }
  }

  @Override
  public long getDefaultReadBufferCapacity() {
    return defaultReadBufferCapacity;
  }

  /**
   * Deletes an existing block.
   */
  @Override
  public void deleteBlock(Container container, BlockID blockID) throws
      IOException {
    // Block/ Chunk Deletion is handled by BlockDeletingService.
    // SCM sends Block Deletion commands directly to Datanodes and not
    // through a Pipeline.
    throw new UnsupportedOperationException();
  }

  /**
   * List blocks in a container.
   *
   * @param container - Container from which blocks need to be listed.
   * @param startLocalID  - Key to start from, 0 to begin.
   * @param count    - Number of blocks to return.
   * @return List of Blocks that match the criteria.
   */
  @Override
  public List<BlockData> listBlock(Container container, long startLocalID, int
      count) throws IOException {
    Preconditions.checkNotNull(container, "container cannot be null");
    Preconditions.checkState(startLocalID >= 0 || startLocalID == -1,
        "startLocal ID cannot be negative");
    Preconditions.checkArgument(count > 0,
        "Count must be a positive number.");
    container.readLock();
    try {
      List<BlockData> result = null;
      KeyValueContainerData cData =
          (KeyValueContainerData) container.getContainerData();
      try (DBHandle db = BlockUtils.getDB(cData, config)) {
        result = new ArrayList<>();
        String startKey = (startLocalID == -1) ? cData.startKeyEmpty()
            : cData.blockKey(startLocalID);
        List<? extends Table.KeyValue<String, BlockData>> range =
            db.getStore().getBlockDataTable()
                .getSequentialRangeKVs(startKey, count,
                    cData.containerPrefix(), cData.getUnprefixedKeyFilter());
        for (Table.KeyValue<String, BlockData> entry : range) {
          result.add(entry.getValue());
        }
        return result;
      }
    } finally {
      container.readUnlock();
    }
  }

  /**
   * Shutdown KeyValueContainerManager.
   */
  @Override
  public void shutdown() {
    BlockUtils.shutdownCache(config);
  }

  private BlockData getBlockByID(DBHandle db, BlockID blockID,
      KeyValueContainerData containerData) throws IOException {
    String blockKey = containerData.blockKey(blockID.getLocalID());

    BlockData blockData = db.getStore().getBlockDataTable().get(blockKey);
    if (blockData == null) {
      throw new StorageContainerException(NO_SUCH_BLOCK_ERR_MSG,
          NO_SUCH_BLOCK);
    }

    return blockData;
  }
}