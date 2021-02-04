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
package org.apache.hadoop.ozone.container.keyvalue.helpers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaOneImpl;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaTwoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which defines utility methods for KeyValueContainer.
 */

public final class KeyValueContainerUtil {

  /* Never constructed. */
  private KeyValueContainerUtil() {

  }

  private static final Logger LOG = LoggerFactory.getLogger(
      KeyValueContainerUtil.class);

  /**
   *
   * @param containerMetaDataPath
   * @throws IOException
   */

  /**
   * creates metadata path, chunks path and metadata DB for the specified
   * container.
   *
   * @param containerMetaDataPath Path to the container's metadata directory.
   * @param chunksPath Path were chunks for this container should be stored.
   * @param dbFile Path to the container's .db file.
   * @param schemaVersion The schema version of the container. Since this
   * method is used when creating new containers, the
   * {@link OzoneConsts#SCHEMA_LATEST} variable can be
   * used to construct the container. If this method has
   * not been updated after a schema version addition
   * and does not recognize the latest SchemaVersion, an
   * {@link IllegalArgumentException} is thrown.
   * @param conf The configuration to use for this container.
   * @throws IOException
   */
  public static void createContainerMetaData(long containerID,
      File containerMetaDataPath, File chunksPath, File dbFile,
      String schemaVersion, ConfigurationSource conf) throws IOException {
    Preconditions.checkNotNull(containerMetaDataPath);
    Preconditions.checkNotNull(conf);

    if (!containerMetaDataPath.mkdirs()) {
      LOG.error("Unable to create directory for metadata storage. Path: {}",
          containerMetaDataPath);
      throw new IOException("Unable to create directory for metadata storage." +
          " Path: " + containerMetaDataPath);
    }

    if (!chunksPath.mkdirs()) {
      LOG.error("Unable to create chunks directory Container {}",
          chunksPath);
      //clean up container metadata path and metadata db
      FileUtils.deleteDirectory(containerMetaDataPath);
      FileUtils.deleteDirectory(containerMetaDataPath.getParentFile());
      throw new IOException("Unable to create directory for data storage." +
          " Path: " + chunksPath);
    }

    DatanodeStore store;
    if (schemaVersion.equals(OzoneConsts.SCHEMA_V1)) {
      store = new DatanodeStoreSchemaOneImpl(conf,
              containerID, dbFile.getAbsolutePath(), false);
    } else if (schemaVersion.equals(OzoneConsts.SCHEMA_V2)) {
      store = new DatanodeStoreSchemaTwoImpl(conf,
              containerID, dbFile.getAbsolutePath(), false);
    } else {
      throw new IllegalArgumentException(
              "Unrecognized schema version for container: " + schemaVersion);
    }

    ReferenceCountedDB db =
        new ReferenceCountedDB(store, dbFile.getAbsolutePath());
    //add db handler into cache
    BlockUtils.addDB(db, dbFile.getAbsolutePath(), conf);
  }

  /**
   * remove Container if it is empty.
   * <p>
   * There are three things we need to delete.
   * <p>
   * 1. Container file and metadata file. 2. The Level DB file 3. The path that
   * we created on the data location.
   *
   * @param containerData - Data of the container to remove.
   * @param conf - configuration of the cluster.
   * @throws IOException
   */
  public static void removeContainer(KeyValueContainerData containerData,
                                     ConfigurationSource conf)
      throws IOException {
    Preconditions.checkNotNull(containerData);
    File containerMetaDataPath = new File(containerData
        .getMetadataPath());
    File chunksPath = new File(containerData.getChunksPath());

    // Close the DB connection and remove the DB handler from cache
    BlockUtils.removeDB(containerData, conf);

    // Delete the Container MetaData path.
    FileUtils.deleteDirectory(containerMetaDataPath);

    //Delete the Container Chunks Path.
    FileUtils.deleteDirectory(chunksPath);

    //Delete Container directory
    FileUtils.deleteDirectory(containerMetaDataPath.getParentFile());
  }

  /**
   * Parse KeyValueContainerData and verify checksum. Set block related
   * metadata like block commit sequence id, block count, bytes used and
   * pending delete block count and delete transaction id.
   * @param kvContainerData
   * @param config
   * @throws IOException
   */
  public static void parseKVContainerData(KeyValueContainerData kvContainerData,
      ConfigurationSource config) throws IOException {

    long containerID = kvContainerData.getContainerID();
    File metadataPath = new File(kvContainerData.getMetadataPath());

    // Verify Checksum
    ContainerUtils.verifyChecksum(kvContainerData);

    File dbFile = KeyValueContainerLocationUtil.getContainerDBFile(
        metadataPath, containerID);
    if (!dbFile.exists()) {
      LOG.error("Container DB file is missing for ContainerID {}. " +
          "Skipping loading of this container.", containerID);
      // Don't further process this container, as it is missing db file.
      return;
    }
    kvContainerData.setDbFile(dbFile);

    if (kvContainerData.getSchemaVersion() == null) {
      // If this container has not specified a schema version, it is in the old
      // format with one default column family.
      kvContainerData.setSchemaVersion(OzoneConsts.SCHEMA_V1);
    }

    boolean isBlockMetadataSet = false;
    ReferenceCountedDB cachedDB = null;
    DatanodeStore store = null;
    try {
      try {
        store = BlockUtils.getUncachedDatanodeStore(
            kvContainerData, config, true);
      } catch (IOException e) {
        // If an exception is thrown, then it may indicate the RocksDB is
        // already open in the container cache. As this code is only executed at
        // DN startup, this should only happen in the tests.
        cachedDB = BlockUtils.getDB(kvContainerData, config);
        store = cachedDB.getStore();
        LOG.warn("Attempt to get an uncached RocksDB handle failed and an " +
            "instance was retrieved from the cache. This should only happen " +
            "in tests");
      }
      Table<String, Long> metadataTable = store.getMetadataTable();

      // Set pending deleted block count.
      Long pendingDeleteBlockCount =
          metadataTable.get(OzoneConsts.PENDING_DELETE_BLOCK_COUNT);
      if (pendingDeleteBlockCount != null) {
        kvContainerData.incrPendingDeletionBlocks(
                pendingDeleteBlockCount);
      } else {
        // Set pending deleted block count.
        MetadataKeyFilters.KeyPrefixFilter filter =
                MetadataKeyFilters.getDeletingKeyFilter();
        int numPendingDeletionBlocks =
            store.getBlockDataTable()
            .getSequentialRangeKVs(null, Integer.MAX_VALUE, filter)
            .size();
        kvContainerData.incrPendingDeletionBlocks(numPendingDeletionBlocks);
      }

      // Set delete transaction id.
      Long delTxnId =
          metadataTable.get(OzoneConsts.DELETE_TRANSACTION_KEY);
      if (delTxnId != null) {
        kvContainerData
            .updateDeleteTransactionId(delTxnId);
      }

      // Set BlockCommitSequenceId.
      Long bcsId = metadataTable.get(
          OzoneConsts.BLOCK_COMMIT_SEQUENCE_ID);
      if (bcsId != null) {
        kvContainerData
            .updateBlockCommitSequenceId(bcsId);
      }

      // Set bytes used.
      // commitSpace for Open Containers relies on usedBytes
      Long bytesUsed =
          metadataTable.get(OzoneConsts.CONTAINER_BYTES_USED);
      if (bytesUsed != null) {
        isBlockMetadataSet = true;
        kvContainerData.setBytesUsed(bytesUsed);
      }

      // Set block count.
      Long blockCount = metadataTable.get(OzoneConsts.BLOCK_COUNT);
      if (blockCount != null) {
        isBlockMetadataSet = true;
        kvContainerData.setKeyCount(blockCount);
      }
      if (!isBlockMetadataSet) {
        initializeUsedBytesAndBlockCount(store, kvContainerData);
      }
    } finally {
      if (cachedDB != null) {
        // If we get a cached instance, calling close simply decrements the
        // reference count.
        cachedDB.close();
      } else if (store != null) {
        // We only stop the store if cacheDB is null, as otherwise we would
        // close the rocksDB handle in the cache and the next reader would fail
        try {
          store.stop();
        } catch (IOException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException("Unexpected exception closing the " +
              "RocksDB when loading containers", e);
        }
      }
    }
  }

  /**
   * Initialize bytes used and block count.
   * @param kvData
   * @throws IOException
   */
  private static void initializeUsedBytesAndBlockCount(DatanodeStore store,
      KeyValueContainerData kvData) throws IOException {
    final String errorMessage = "Failed to parse block data for" +
        " Container " + kvData.getContainerID();
    long blockCount = 0;
    long usedBytes = 0;

    try (BlockIterator<BlockData> blockIter =
             store.getBlockIterator(
                 MetadataKeyFilters.getUnprefixedKeyFilter())) {

      while (blockIter.hasNext()) {
        blockCount++;
        try {
          usedBytes += getBlockLength(blockIter.nextBlock());
        } catch (IOException ex) {
          LOG.error(errorMessage);
        }
      }
    }

    // Count all deleting blocks.
    try (BlockIterator<BlockData> blockIter =
             store.getBlockIterator(
                 MetadataKeyFilters.getDeletingKeyFilter())) {

      while (blockIter.hasNext()) {
        blockCount++;
        try {
          usedBytes += getBlockLength(blockIter.nextBlock());
        } catch (IOException ex) {
          LOG.error(errorMessage);
        }
      }
    }
    kvData.setBytesUsed(usedBytes);
    kvData.setKeyCount(blockCount);
  }

  private static long getBlockLength(BlockData block) throws IOException {
    long blockLen = 0;
    List<ContainerProtos.ChunkInfo> chunkInfoList = block.getChunks();

    for (ContainerProtos.ChunkInfo chunk : chunkInfoList) {
      ChunkInfo info = ChunkInfo.getFromProtoBuf(chunk);
      blockLen += info.getLen();
    }

    return blockLen;
  }

  /**
   * Returns the path where data or chunks live for a given container.
   *
   * @param kvContainerData - KeyValueContainerData
   * @return - Path to the chunks directory
   */
  public static Path getDataDirectory(KeyValueContainerData kvContainerData) {

    String chunksPath = kvContainerData.getChunksPath();
    Preconditions.checkNotNull(chunksPath);

    return Paths.get(chunksPath);
  }

  /**
   * Container metadata directory -- here is where the level DB and
   * .container file lives.
   *
   * @param kvContainerData - KeyValueContainerData
   * @return Path to the metadata directory
   */
  public static Path getMetadataDirectory(
      KeyValueContainerData kvContainerData) {

    String metadataPath = kvContainerData.getMetadataPath();
    Preconditions.checkNotNull(metadataPath);

    return Paths.get(metadataPath);

  }
}
