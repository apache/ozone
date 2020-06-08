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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueBlockIterator;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.hdds.utils.MetadataStore;
import org.apache.hadoop.hdds.utils.MetadataStoreBuilder;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.DB_BLOCK_COMMIT_SEQUENCE_ID_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.DB_BLOCK_COUNT_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.DB_CONTAINER_BYTES_USED_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.DB_CONTAINER_DELETE_TRANSACTION_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.DB_PENDING_DELETE_BLOCK_COUNT_KEY;

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
   * creates metadata path, chunks path and  metadata DB for the specified
   * container.
   *
   * @param containerMetaDataPath
   * @throws IOException
   */
  public static void createContainerMetaData(File containerMetaDataPath, File
      chunksPath, File dbFile, ConfigurationSource conf) throws IOException {
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

    MetadataStore store = MetadataStoreBuilder.newBuilder().setConf(conf)
        .setCreateIfMissing(true).setDbFile(dbFile).build();
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


    boolean isBlockMetadataSet = false;

    try(ReferenceCountedDB containerDB = BlockUtils.getDB(kvContainerData,
        config)) {

      // Set pending deleted block count.
      byte[] pendingDeleteBlockCount =
          containerDB.getStore().get(DB_PENDING_DELETE_BLOCK_COUNT_KEY);
      if (pendingDeleteBlockCount != null) {
        kvContainerData.incrPendingDeletionBlocks(
            Ints.fromByteArray(pendingDeleteBlockCount));
      } else {
        // Set pending deleted block count.
        MetadataKeyFilters.KeyPrefixFilter filter =
            new MetadataKeyFilters.KeyPrefixFilter()
                .addFilter(OzoneConsts.DELETING_KEY_PREFIX);
        int numPendingDeletionBlocks =
            containerDB.getStore().getSequentialRangeKVs(null,
                Integer.MAX_VALUE, filter)
                .size();
        kvContainerData.incrPendingDeletionBlocks(numPendingDeletionBlocks);
      }

      // Set delete transaction id.
      byte[] delTxnId =
          containerDB.getStore().get(DB_CONTAINER_DELETE_TRANSACTION_KEY);
      if (delTxnId != null) {
        kvContainerData
            .updateDeleteTransactionId(Longs.fromByteArray(delTxnId));
      }

      // Set BlockCommitSequenceId.
      byte[] bcsId = containerDB.getStore().get(
          DB_BLOCK_COMMIT_SEQUENCE_ID_KEY);
      if (bcsId != null) {
        kvContainerData
            .updateBlockCommitSequenceId(Longs.fromByteArray(bcsId));
      }

      // Set bytes used.
      // commitSpace for Open Containers relies on usedBytes
      byte[] bytesUsed =
          containerDB.getStore().get(DB_CONTAINER_BYTES_USED_KEY);
      if (bytesUsed != null) {
        isBlockMetadataSet = true;
        kvContainerData.setBytesUsed(Longs.fromByteArray(bytesUsed));
      }

      // Set block count.
      byte[] blockCount = containerDB.getStore().get(DB_BLOCK_COUNT_KEY);
      if (blockCount != null) {
        isBlockMetadataSet = true;
        kvContainerData.setKeyCount(Longs.fromByteArray(blockCount));
      }
    }

    if (!isBlockMetadataSet) {
      initializeUsedBytesAndBlockCount(kvContainerData);
    }
  }


  /**
   * Initialize bytes used and block count.
   * @param kvContainerData
   * @throws IOException
   */
  private static void initializeUsedBytesAndBlockCount(
      KeyValueContainerData kvContainerData) throws IOException {

    long blockCount = 0;
    try (KeyValueBlockIterator blockIter = new KeyValueBlockIterator(
        kvContainerData.getContainerID(),
        new File(kvContainerData.getContainerPath()))) {
      long usedBytes = 0;


      boolean success = true;
      while (success) {
        try {
          if (blockIter.hasNext()) {
            BlockData block = blockIter.nextBlock();
            long blockLen = 0;

            List< ContainerProtos.ChunkInfo > chunkInfoList = block.getChunks();
            for (ContainerProtos.ChunkInfo chunk : chunkInfoList) {
              ChunkInfo info = ChunkInfo.getFromProtoBuf(chunk);
              blockLen += info.getLen();
            }

            usedBytes += blockLen;
            blockCount++;
          } else {
            success = false;
          }
        } catch (IOException ex) {
          LOG.error("Failed to parse block data for Container {}",
              kvContainerData.getContainerID());
        }
      }
      kvContainerData.setBytesUsed(usedBytes);
      kvContainerData.setKeyCount(blockCount);
    }
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
