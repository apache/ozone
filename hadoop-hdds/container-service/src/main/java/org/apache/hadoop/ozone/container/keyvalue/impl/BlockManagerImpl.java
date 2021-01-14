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
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
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

  /**
   * Constructs a Block Manager.
   *
   * @param conf - Ozone configuration
   */
  public BlockManagerImpl(ConfigurationSource conf) {
    Preconditions.checkNotNull(conf, "Config cannot be null");
    this.config = conf;
  }

  /**
   * Puts or overwrites a block.
   *
   * @param container - Container for which block need to be added.
   * @param data     - BlockData.
   * @return length of the block.
   * @throws IOException
   */
  public long putBlock(Container container, BlockData data) throws IOException {
    return putBlock(container, data, true);
  }
  /**
   * Puts or overwrites a block.
   *
   * @param container - Container for which block need to be added.
   * @param data     - BlockData.
   * @param incrKeyCount - for FilePerBlockStrategy, increase key count only
   *                     when the whole block file is written.
   * @return length of the block.
   * @throws IOException
   */
  public long putBlock(Container container, BlockData data,
      boolean incrKeyCount) throws IOException {
    return persistPutBlock(
        (KeyValueContainer) container,
        data,
        config,
        incrKeyCount);
  }

  public static long persistPutBlock(KeyValueContainer container,
      BlockData data, ConfigurationSource config, boolean incrKeyCount)
      throws IOException {
    Preconditions.checkNotNull(data, "BlockData cannot be null for put " +
        "operation.");
    Preconditions.checkState(data.getContainerID() >= 0, "Container Id " +
        "cannot be negative");
    // We are not locking the key manager since LevelDb serializes all actions
    // against a single DB. We rely on DB level locking to avoid conflicts.
    try(ReferenceCountedDB db = BlockUtils.
        getDB(container.getContainerData(), config)) {
      // This is a post condition that acts as a hint to the user.
      // Should never fail.
      Preconditions.checkNotNull(db, DB_NULL_ERR_MSG);

      long bcsId = data.getBlockCommitSequenceId();
      long containerBCSId = container.
          getContainerData().getBlockCommitSequenceId();

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
      // update the blockData as well as BlockCommitSequenceId here
      try (BatchOperation batch = db.getStore().getBatchHandler()
          .initBatchOperation()) {
        db.getStore().getBlockDataTable().putWithBatch(
            batch, Long.toString(data.getLocalID()), data);
        db.getStore().getMetadataTable().putWithBatch(
            batch, OzoneConsts.BLOCK_COMMIT_SEQUENCE_ID, bcsId);

        // Set Bytes used, this bytes used will be updated for every write and
        // only get committed for every put block. In this way, when datanode
        // is up, for computation of disk space by container only committed
        // block length is used, And also on restart the blocks committed to DB
        // is only used to compute the bytes used. This is done to keep the
        // current behavior and avoid DB write during write chunk operation.
        db.getStore().getMetadataTable().putWithBatch(
            batch, OzoneConsts.CONTAINER_BYTES_USED,
            container.getContainerData().getBytesUsed());

        // Set Block Count for a container.
        if (incrKeyCount) {
          db.getStore().getMetadataTable().putWithBatch(
              batch, OzoneConsts.BLOCK_COUNT,
              container.getContainerData().getKeyCount() + 1);
        }

        db.getStore().getBatchHandler().commitBatchOperation(batch);
      }

      container.updateBlockCommitSequenceId(bcsId);
      // Increment block count finally here for in-memory.
      if (incrKeyCount) {
        container.getContainerData().incrKeyCount();
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
    try(ReferenceCountedDB db = BlockUtils.getDB(containerData, config)) {
      // This is a post condition that acts as a hint to the user.
      // Should never fail.
      Preconditions.checkNotNull(db, DB_NULL_ERR_MSG);

      long containerBCSId = containerData.getBlockCommitSequenceId();
      if (containerBCSId < bcsId) {
        throw new StorageContainerException(
            "Unable to find the block with bcsID " + bcsId + " .Container "
                + container.getContainerData().getContainerID() + " bcsId is "
                + containerBCSId + ".", UNKNOWN_BCSID);
      }
      BlockData blockData = getBlockByID(db, blockID);
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
    try(ReferenceCountedDB db = BlockUtils.getDB(containerData, config)) {
      // This is a post condition that acts as a hint to the user.
      // Should never fail.
      Preconditions.checkNotNull(db, DB_NULL_ERR_MSG);
      BlockData blockData = getBlockByID(db, blockID);
      return blockData.getSize();
    }
  }

  /**
   * Deletes an existing block.
   *
   * @param container - Container from which block need to be deleted.
   * @param blockID - ID of the block.
   * @throws StorageContainerException
   */
  public void deleteBlock(Container container, BlockID blockID) throws
      IOException {
    Preconditions.checkNotNull(blockID, "block ID cannot be null.");
    Preconditions.checkState(blockID.getContainerID() >= 0,
        "Container ID cannot be negative.");
    Preconditions.checkState(blockID.getLocalID() >= 0,
        "Local ID cannot be negative.");

    KeyValueContainerData cData = (KeyValueContainerData) container
        .getContainerData();
    try(ReferenceCountedDB db = BlockUtils.getDB(cData, config)) {
      // This is a post condition that acts as a hint to the user.
      // Should never fail.
      Preconditions.checkNotNull(db, DB_NULL_ERR_MSG);
      // Note : There is a race condition here, since get and delete
      // are not atomic. Leaving it here since the impact is refusing
      // to delete a Block which might have just gotten inserted after
      // the get check.

      // Throw an exception if block data not found in the block data table.
      getBlockByID(db, blockID);

      // Update DB to delete block and set block count and bytes used.
      try (BatchOperation batch = db.getStore().getBatchHandler()
          .initBatchOperation()) {
        String localID = Long.toString(blockID.getLocalID());
        db.getStore().getBlockDataTable().deleteWithBatch(batch, localID);
        // Update DB to delete block and set block count.
        // No need to set bytes used here, as bytes used is taken care during
        // delete chunk.
        long blockCount = container.getContainerData().getKeyCount() - 1;
        db.getStore().getMetadataTable()
            .putWithBatch(batch, OzoneConsts.BLOCK_COUNT, blockCount);
        db.getStore().getBatchHandler().commitBatchOperation(batch);
      }

      // Decrement block count here
      container.getContainerData().decrKeyCount();
    }
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
    Preconditions.checkState(startLocalID >= 0, "startLocal ID cannot be " +
        "negative");
    Preconditions.checkArgument(count > 0,
        "Count must be a positive number.");
    container.readLock();
    try {
      List<BlockData> result = null;
      KeyValueContainerData cData =
          (KeyValueContainerData) container.getContainerData();
      try (ReferenceCountedDB db = BlockUtils.getDB(cData, config)) {
        result = new ArrayList<>();
        List<? extends Table.KeyValue<String, BlockData>> range =
            db.getStore().getBlockDataTable()
                .getSequentialRangeKVs(Long.toString(startLocalID), count,
                    MetadataKeyFilters.getUnprefixedKeyFilter());
        for (Table.KeyValue<String, BlockData> entry : range) {
          BlockData data = new BlockData(entry.getValue().getBlockID());
          result.add(data);
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
  public void shutdown() {
    BlockUtils.shutdownCache(ContainerCache.getInstance(config));
  }

  private BlockData getBlockByID(ReferenceCountedDB db, BlockID blockID)
      throws IOException {
    String blockKey = Long.toString(blockID.getLocalID());

    BlockData blockData = db.getStore().getBlockDataTable().get(blockKey);
    if (blockData == null) {
      throw new StorageContainerException(NO_SUCH_BLOCK_ERR_MSG,
          NO_SUCH_BLOCK);
    }

    return blockData;
  }
}