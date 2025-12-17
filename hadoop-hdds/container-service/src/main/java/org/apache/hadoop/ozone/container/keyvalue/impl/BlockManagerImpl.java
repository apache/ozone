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

package org.apache.hadoop.ozone.container.keyvalue.impl;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.BCSID_MISMATCH;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNSUPPORTED_REQUEST;
import static org.apache.hadoop.ozone.OzoneConsts.INCREMENTAL_CHUNK_LIST;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for performing block related operations on the KeyValue
 * Container.
 */
public class BlockManagerImpl implements BlockManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(BlockManagerImpl.class);

  private ConfigurationSource config;

  public static final String FULL_CHUNK = "full";

  // Default Read Buffer capacity when Checksum is not present
  private final int defaultReadBufferCapacity;
  private final int readMappedBufferThreshold;
  private final int readMappedBufferMaxCount;
  private final boolean readNettyChunkedNioFile;

  /**
   * Constructs a Block Manager.
   *
   * @param conf - Ozone configuration
   */
  public BlockManagerImpl(ConfigurationSource conf) {
    Objects.requireNonNull(conf, "conf == null");
    this.config = conf;
    this.defaultReadBufferCapacity = config.getBufferSize(
        ScmConfigKeys.OZONE_CHUNK_READ_BUFFER_DEFAULT_SIZE_KEY,
        ScmConfigKeys.OZONE_CHUNK_READ_BUFFER_DEFAULT_SIZE_DEFAULT);
    this.readMappedBufferThreshold = config.getBufferSize(
        ScmConfigKeys.OZONE_CHUNK_READ_MAPPED_BUFFER_THRESHOLD_KEY,
        ScmConfigKeys.OZONE_CHUNK_READ_MAPPED_BUFFER_THRESHOLD_DEFAULT);
    this.readMappedBufferMaxCount = config.getInt(
        ScmConfigKeys.OZONE_CHUNK_READ_MAPPED_BUFFER_MAX_COUNT_KEY,
        ScmConfigKeys.OZONE_CHUNK_READ_MAPPED_BUFFER_MAX_COUNT_DEFAULT);
    this.readNettyChunkedNioFile = config.getBoolean(
        ScmConfigKeys.OZONE_CHUNK_READ_NETTY_CHUNKED_NIO_FILE_KEY,
        ScmConfigKeys.OZONE_CHUNK_READ_NETTY_CHUNKED_NIO_FILE_DEFAULT);
  }

  @Override
  public long putBlock(Container container, BlockData data) throws IOException {
    return putBlock(container, data, true);
  }

  @Override
  public long putBlock(Container container, BlockData data,
      boolean endOfBlock) throws IOException {
    return persistPutBlock(
        (KeyValueContainer) container,
        data, endOfBlock);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long putBlockForClosedContainer(Container container, BlockData data, boolean overwriteBcsId)
          throws IOException {
    Objects.requireNonNull(data, "data == null");
    Preconditions.checkState(data.getContainerID() >= 0, "Container Id cannot be negative");

    KeyValueContainerData containerData = (KeyValueContainerData) container.getContainerData();

    // We are not locking the key manager since RocksDB serializes all actions
    // against a single DB. We rely on DB level locking to avoid conflicts.
    try (DBHandle db = BlockUtils.getDB(containerData, config)) {
      Objects.requireNonNull(db, "db == null");

      long blockBcsID = data.getBlockCommitSequenceId();
      long containerBcsID = containerData.getBlockCommitSequenceId();

      // Check if the block is already present in the DB of the container to determine whether
      // the blockCount is already incremented for this block in the DB or not.
      long localID = data.getLocalID();
      boolean incrBlockCount = false;

      // update the blockData as well as BlockCommitSequenceId here
      try (BatchOperation batch = db.getStore().getBatchHandler()
          .initBatchOperation()) {
        // If block already exists in the DB, blockCount should not be incremented.
        if (db.getStore().getBlockDataTable().get(containerData.getBlockKey(localID)) == null) {
          incrBlockCount = true;
        }

        db.getStore().getBlockDataTable().putWithBatch(batch, containerData.getBlockKey(localID), data);
        if (overwriteBcsId && blockBcsID > containerBcsID) {
          db.getStore().getMetadataTable().putWithBatch(batch, containerData.getBcsIdKey(), blockBcsID);
        }

        // Set Bytes used, this bytes used will be updated for every write and
        // only get committed for every put block. In this way, when datanode
        // is up, for computation of disk space by container only committed
        // block length is used, And also on restart the blocks committed to DB
        // is only used to compute the bytes used. This is done to keep the
        // current behavior and avoid DB write during write chunk operation.
        db.getStore().getMetadataTable().putWithBatch(batch, containerData.getBytesUsedKey(),
            containerData.getBytesUsed());

        // Set Block Count for a container.
        if (incrBlockCount) {
          db.getStore().getMetadataTable().putWithBatch(batch, containerData.getBlockCountKey(),
              containerData.getBlockCount() + 1);
        }

        db.getStore().getBatchHandler().commitBatchOperation(batch);
      }

      if (overwriteBcsId && blockBcsID > containerBcsID) {
        container.updateBlockCommitSequenceId(blockBcsID);
      }

      // Increment block count in-memory after the DB update.
      if (incrBlockCount) {
        containerData.getStatistics().incrementBlockCount();
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Block {} successfully persisted for closed container {} with bcsId {} chunk size {}",
            data.getBlockID(), containerData.getContainerID(), blockBcsID, data.getChunks().size());
      }
      return data.getSize();
    }
  }

  public long persistPutBlock(KeyValueContainer container,
      BlockData data, boolean endOfBlock)
      throws IOException {
    Objects.requireNonNull(data, "data == null");
    Preconditions.checkState(data.getContainerID() >= 0, "Container Id " +
        "cannot be negative");

    KeyValueContainerData containerData = container.getContainerData();

    // We are not locking the key manager since LevelDb serializes all actions
    // against a single DB. We rely on DB level locking to avoid conflicts.
    try (DBHandle db = BlockUtils.getDB(containerData, config)) {
      // This is a post condition that acts as a hint to the user.
      // Should never fail.
      Objects.requireNonNull(db, "db == null");

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
              containerData.getBlockKey(localID)) == null) {
            // Block does not exist in DB => blockCount needs to be
            // incremented when the block is added into DB.
            incrBlockCount = true;
          }
        }

        boolean incrementalEnabled = true;
        if (!VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.HBASE_SUPPORT)) {
          if (isPartialChunkList(data)) {
            throw new StorageContainerException("DataNode has not finalized " +
                "upgrading to a version that supports incremental chunk list.", UNSUPPORTED_REQUEST);
          }
          incrementalEnabled = false;
        }
        db.getStore().putBlockByID(batch, incrementalEnabled, localID, data,
            containerData, endOfBlock);
        if (bcsId != 0) {
          db.getStore().getMetadataTable().putWithBatch(
              batch, containerData.getBcsIdKey(), bcsId);
        }

        // Set Bytes used, this bytes used will be updated for every write and
        // only get committed for every put block. In this way, when datanode
        // is up, for computation of disk space by container only committed
        // block length is used, And also on restart the blocks committed to DB
        // is only used to compute the bytes used. This is done to keep the
        // current behavior and avoid DB write during write chunk operation.
        final ContainerData.BlockByteAndCounts b = containerData.getStatistics().getBlockByteAndCounts();
        db.getStore().getMetadataTable().putWithBatch(batch, containerData.getBytesUsedKey(), b.getBytes());

        // Set Block Count for a container.
        if (incrBlockCount) {
          db.getStore().getMetadataTable().putWithBatch(batch, containerData.getBlockCountKey(), b.getCount() + 1);
        }

        db.getStore().getBatchHandler().commitBatchOperation(batch);
      }

      if (bcsId != 0) {
        container.updateBlockCommitSequenceId(bcsId);
      }

      // Increment block count and add block to pendingPutBlockCache
      // in-memory after the DB update.
      if (incrBlockCount) {
        containerData.getStatistics().incrementBlockCount();
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

  @Override
  public void finalizeBlock(Container container, BlockID blockId)
      throws IOException {
    Objects.requireNonNull(blockId, "blockId == null");
    Preconditions.checkState(blockId.getContainerID() >= 0,
        "Container Id cannot be negative");

    KeyValueContainer kvContainer = (KeyValueContainer)container;

    long localID = blockId.getLocalID();

    kvContainer.removeFromPendingPutBlockCache(localID);

    try (DBHandle db = BlockUtils.getDB(kvContainer.getContainerData(),
        config)) {
      // Should never fail.
      Objects.requireNonNull(db, "db == null");

      // persist finalizeBlock
      try (BatchOperation batch = db.getStore().getBatchHandler()
          .initBatchOperation()) {
        db.getStore().getFinalizeBlocksTable().putWithBatch(batch,
            kvContainer.getContainerData().getBlockKey(localID), localID);
        db.getStore().getBatchHandler().commitBatchOperation(batch);

        mergeLastChunkForBlockFinalization(blockId, db, kvContainer, batch, localID);
      }
    }
  }

  private void mergeLastChunkForBlockFinalization(BlockID blockId, DBHandle db,
                         KeyValueContainer kvContainer, BatchOperation batch,
                         long localID) throws IOException {
    // if the chunk list of the block to be finalized was written incremental,
    // merge the last chunk into block data.
    BlockData blockData = getBlockByID(db, blockId, kvContainer.getContainerData());
    if (blockData.getMetadata().containsKey(INCREMENTAL_CHUNK_LIST)) {
      BlockData emptyBlockData = new BlockData(blockId);
      emptyBlockData.addMetadata(INCREMENTAL_CHUNK_LIST, "");
      db.getStore().putBlockByID(batch, true, localID,
          emptyBlockData, kvContainer.getContainerData(), true);
    }
  }

  @Override
  public BlockData getBlock(Container container, BlockID blockID) throws IOException {
    BlockUtils.verifyBCSId(container, blockID);
    KeyValueContainerData containerData = (KeyValueContainerData) container
        .getContainerData();
    long bcsId = blockID.getBlockCommitSequenceId();
    try (DBHandle db = BlockUtils.getDB(containerData, config)) {
      // This is a post condition that acts as a hint to the user.
      // Should never fail.
      Objects.requireNonNull(db, "db == null");
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

  @Override
  public long getCommittedBlockLength(Container container, BlockID blockID)
      throws IOException {
    KeyValueContainerData containerData = (KeyValueContainerData) container
        .getContainerData();
    try (DBHandle db = BlockUtils.getDB(containerData, config)) {
      // This is a post condition that acts as a hint to the user.
      // Should never fail.
      Objects.requireNonNull(db, "db == null");
      BlockData blockData = getBlockByID(db, blockID, containerData);
      return blockData.getSize();
    }
  }

  @Override
  public int getDefaultReadBufferCapacity() {
    return defaultReadBufferCapacity;
  }

  @Override
  public int getReadMappedBufferThreshold() {
    return readMappedBufferThreshold;
  }

  @Override
  public int getReadMappedBufferMaxCount() {
    return readMappedBufferMaxCount;
  }

  @Override
  public boolean isReadNettyChunkedNioFile() {
    return readNettyChunkedNioFile;
  }

  /**
   * Deletes an existing block.
   * As Deletion is handled by BlockDeletingService,
   * UnsupportedOperationException is thrown always
   *
   * @param container - Container from which block need to be deleted.
   * @param blockID - ID of the block.
   */
  @Override
  public void deleteBlock(Container container, BlockID blockID) throws
      IOException {
    // Block/ Chunk Deletion is handled by BlockDeletingService.
    // SCM sends Block Deletion commands directly to Datanodes and not
    // through a Pipeline.
    throw new UnsupportedOperationException();
  }

  @Override
  public List<BlockData> listBlock(Container container, long startLocalID, int
      count) throws IOException {
    Objects.requireNonNull(container, "container == null");
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
            : cData.getBlockKey(startLocalID);
        final List<Table.KeyValue<String, BlockData>> range = db.getStore().getBlockDataTable().getRangeKVs(
            startKey, count, cData.containerPrefix(), cData.getUnprefixedKeyFilter(), true);
        for (Table.KeyValue<String, BlockData> entry : range) {
          result.add(db.getStore().getCompleteBlockData(entry.getValue(), null, entry.getKey()));
        }
        return result;
      }
    } finally {
      container.readUnlock();
    }
  }

  @Override
  public boolean blockExists(Container container, BlockID blockID) throws IOException {
    KeyValueContainerData containerData = (KeyValueContainerData) container
        .getContainerData();
    try (DBHandle db = BlockUtils.getDB(containerData, config)) {
      // This is a post condition that acts as a hint to the user.
      // Should never fail.
      Objects.requireNonNull(db, "db == null");
      String blockKey = containerData.getBlockKey(blockID.getLocalID());
      return db.getStore().getBlockDataTable().isExist(blockKey);
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
    String blockKey = containerData.getBlockKey(blockID.getLocalID());
    return db.getStore().getBlockByID(blockID, blockKey);
  }

  private static boolean isPartialChunkList(BlockData data) {
    return data.getMetadata().containsKey(INCREMENTAL_CHUNK_LIST);
  }
}
