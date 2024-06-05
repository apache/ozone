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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.MissingBlock;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
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
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V3;

import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition;
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
  private final int defaultReadBufferCapacity;
  private final int readMappedBufferThreshold;

  /**
   * Constructs a Block Manager.
   *
   * @param conf - Ozone configuration
   */
  public BlockManagerImpl(ConfigurationSource conf) {
    Preconditions.checkNotNull(conf, "Config cannot be null");
    this.config = conf;
    this.defaultReadBufferCapacity = config.getBufferSize(
        ScmConfigKeys.OZONE_CHUNK_READ_BUFFER_DEFAULT_SIZE_KEY,
        ScmConfigKeys.OZONE_CHUNK_READ_BUFFER_DEFAULT_SIZE_DEFAULT);
    this.readMappedBufferThreshold = config.getBufferSize(
        ScmConfigKeys.OZONE_CHUNK_READ_MAPPED_BUFFER_THRESHOLD_KEY,
        ScmConfigKeys.OZONE_CHUNK_READ_MAPPED_BUFFER_THRESHOLD_DEFAULT);
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
              containerData.getBlockKey(localID)) == null) {
            // Block does not exist in DB => blockCount needs to be
            // incremented when the block is added into DB.
            incrBlockCount = true;
          }
        }

        db.getStore().getBlockDataTable().putWithBatch(
            batch, containerData.getBlockKey(localID), data);
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
        db.getStore().getMetadataTable().putWithBatch(
            batch, containerData.getBytesUsedKey(),
            containerData.getBytesUsed());

        // Set Block Count for a container.
        if (incrBlockCount) {
          db.getStore().getMetadataTable().putWithBatch(
              batch, containerData.getBlockCountKey(),
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
          "Unable to find the block with bcsID " + bcsId + ". Container "
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
  public int getDefaultReadBufferCapacity() {
    return defaultReadBufferCapacity;
  }

  public int getReadMappedBufferThreshold() {
    return readMappedBufferThreshold;
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
            : cData.getBlockKey(startLocalID);
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

  public Set<MissingBlock> headBlocks(Container container, Set<Long> localIds) throws IOException {
    Preconditions.checkNotNull(container, "container cannot be null");
    Preconditions.checkNotNull(localIds, "local ID cannot be null");
    if (localIds.isEmpty()) {
      return new HashSet<>();
    }

    container.readLock();
    try {
      KeyValueContainerData containerData = (KeyValueContainerData) container.getContainerData();
      if (!containerData.getLayoutVersion().equals(ContainerLayoutVersion.FILE_PER_BLOCK)) {
        throw new UnsupportedEncodingException("Not support Container Layout " + containerData.getLayoutVersion());
      }
      boolean isV3Schema = containerData.hasSchema(SCHEMA_V3);
      File chunksPath = new File(containerData.getChunksPath());

      Set<Long> existingOnDB = getExistingBlocksFromDB(containerData, localIds, isV3Schema);
      Set<Long> existingOnDisk = getExistingBlocksFromDisk(chunksPath, localIds);

      return findMissingBlocks(localIds, existingOnDB, existingOnDisk);
    } finally {
      container.readUnlock();
    }
  }

  private Set<Long> getExistingBlocksFromDB(KeyValueContainerData containerData, Set<Long> localIds,
      boolean isV3Schema) throws IOException {
    Set<Long> existingOnDB = new HashSet<>();
    Long minLocalID = Collections.min(localIds);

    try (DBHandle db = BlockUtils.getDB(containerData, config)) {
      String startKey = containerData.getBlockKey(minLocalID);
      List<? extends Table.KeyValue<String, BlockData>> range = db.getStore().getBlockDataTable()
          .getSequentialRangeKVs(startKey, Integer.MAX_VALUE, containerData.containerPrefix(),
              containerData.getUnprefixedKeyFilter());

      for (Table.KeyValue<String, BlockData> entry : range) {
        long blockId = parseBlockId(entry.getKey(), isV3Schema);
        if (localIds.contains(blockId)) {
          existingOnDB.add(blockId);
          if (existingOnDB.size() == localIds.size()) {
            break;
          }
        }
      }
    }
    return existingOnDB;
  }

  private long parseBlockId(String key, boolean isV3Schema) {
    return isV3Schema ? Long.parseLong(DatanodeSchemaThreeDBDefinition.getKeyWithoutPrefix(key)) : Long.parseLong(key);
  }

  private Set<Long> getExistingBlocksFromDisk(File chunksPath, Set<Long> localIds) throws IOException {
    Set<Long> existingOnDisk = new HashSet<>();

    Preconditions.checkArgument(chunksPath.isDirectory());
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(chunksPath.toPath())) {
      for (Path entry : stream) {
        Path fileNamePath = entry.getFileName();
        if (fileNamePath == null) {
          continue;
        }
        String fileName = fileNamePath.toString();
        if (fileName.endsWith(".block")) {
          try {
            long blockId = Long.parseLong(fileName.substring(0, fileName.lastIndexOf(".block")));
            if (localIds.contains(blockId)) {
              existingOnDisk.add(blockId);
              if (existingOnDisk.size() == localIds.size()) {
                break;
              }
            }
          } catch (NumberFormatException e) {
            LOG.error("Invalid long value in file name: " + fileName);
          }
        }
      }
    }
    return existingOnDisk;
  }

  private Set<MissingBlock> findMissingBlocks(Set<Long> localIds, Set<Long> existingOnDB, Set<Long> existingOnDisk) {
    Set<MissingBlock> missingBlocks = new HashSet<>();
    for (Long localId : localIds) {
      boolean onDB = existingOnDB.contains(localId);
      boolean onDisk = existingOnDisk.contains(localId);
      if (!onDB || !onDisk) {
        MissingBlock missingBlock = MissingBlock.newBuilder()
            .setLocalID(localId)
            .setOnDB(onDB)
            .setOnDisk(onDisk)
            .build();
        missingBlocks.add(missingBlock);
      }
    }
    return missingBlocks;
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

    BlockData blockData = db.getStore().getBlockDataTable().get(blockKey);
    if (blockData == null) {
      throw new StorageContainerException(NO_SUCH_BLOCK_ERR_MSG +
              " BlockID : " + blockID, NO_SUCH_BLOCK);
    }

    return blockData;
  }
}
