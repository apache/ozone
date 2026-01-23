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

package org.apache.hadoop.ozone.container.keyvalue.helpers;

import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V1;
import static org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerMetadataInspector.getAggregatePendingDelete;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerChecksumInfo;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.ContainerInspectorUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.PendingDelete;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaOneImpl;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaTwoImpl;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which defines utility methods for KeyValueContainer.
 */

public final class KeyValueContainerUtil {

  private static final Logger LOG = LoggerFactory.getLogger(KeyValueContainerUtil.class);

  /* Never constructed. */
  private KeyValueContainerUtil() {
  }

  /**
   * creates metadata path, chunks path and metadata DB for the specified
   * container.
   *
   * @param containerMetaDataPath Path to the container's metadata directory.
   * @param chunksPath Path were chunks for this container should be stored.
   * @param dbFile Path to the container's .db file.
   * @param schemaVersion The schema version of the container. If this method
   * has not been updated after a schema version addition
   * and does not recognize the latest SchemaVersion, an
   * {@link IllegalArgumentException} is thrown.
   * @param conf The configuration to use for this container.
   * @throws IOException
   */
  public static void createContainerMetaData(
      File containerMetaDataPath, File chunksPath, File dbFile,
      String schemaVersion, ConfigurationSource conf) throws IOException {
    Objects.requireNonNull(containerMetaDataPath,  "containerMetaDataPath == null");
    Objects.requireNonNull(conf, "conf == null");

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
    if (isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V1)) {
      store = new DatanodeStoreSchemaOneImpl(conf, dbFile.getAbsolutePath(),
          false);
    } else if (isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V2)) {
      store = new DatanodeStoreSchemaTwoImpl(conf, dbFile.getAbsolutePath(),
          false);
    } else if (isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V3)) {
      // We don't create per-container store for schema v3 containers,
      // they should use per-volume db store.
      return;
    } else {
      throw new IllegalArgumentException(
              "Unrecognized schema version for container: " + schemaVersion);
    }

    //add db handler into cache
    BlockUtils.addDB(store, dbFile.getAbsolutePath(), conf, schemaVersion);
  }

  /**
   * remove Container 1. remove db, 2. move to tmp directory.
   *
   * @param containerData - Data of the container to remove.
   * @throws IOException
   */
  public static void removeContainer(
      KeyValueContainerData containerData, ConfigurationSource conf)
      throws IOException {
    Objects.requireNonNull(containerData, "containerData == null");
    KeyValueContainerUtil.removeContainerDB(containerData, conf);
    KeyValueContainerUtil.moveToDeletedContainerDir(containerData,
        containerData.getVolume());
  }

  /**
   * remove Container db, the Level DB file.
   *
   * @param containerData - Data of the container to remove.
   * @param conf - configuration of the cluster.
   * @throws IOException
   */
  public static void removeContainerDB(
      KeyValueContainerData containerData, ConfigurationSource conf)
      throws IOException {
    if (containerData.hasSchema(OzoneConsts.SCHEMA_V3)) {
      // DB failure is catastrophic, the disk needs to be replaced.
      // In case of an exception, LOG the message and rethrow the exception.
      try {
        BlockUtils.removeContainerFromDB(containerData, conf);
      } catch (IOException ex) {
        LOG.error("DB failure, unable to remove container. " +
            "Disk need to be replaced.", ex);
        throw ex;
      }
    } else {
      // Close the DB connection and remove the DB handler from cache
      BlockUtils.removeDB(containerData, conf);
    }
  }

  /**
   * Returns if there are no blocks in the container.
   * @param store DBStore
   * @param containerData Container to check
   * @param bCheckChunksFilePath Whether to check chunksfilepath has any blocks
   * @return true if the directory containing blocks is empty
   * @throws IOException
   */
  public static boolean noBlocksInContainer(DatanodeStore store,
                                            KeyValueContainerData
                                            containerData,
                                            boolean bCheckChunksFilePath)
      throws IOException {
    Objects.requireNonNull(store, "store == null");
    Objects.requireNonNull(containerData, "containerData == null");
    if (containerData.isOpen()) {
      return false;
    }
    try (BlockIterator<BlockData> blockIterator =
             store.getBlockIterator(containerData.getContainerID())) {
      if (blockIterator.hasNext()) {
        return false;
      }
    }
    if (bCheckChunksFilePath) {
      File chunksPath = new File(containerData.getChunksPath());
      Preconditions.checkArgument(chunksPath.isDirectory());
      try (DirectoryStream<Path> dir
               = Files.newDirectoryStream(chunksPath.toPath())) {
        return !dir.iterator().hasNext();
      }
    }
    return true;
  }

  /**
   * Parse KeyValueContainerData and verify checksum. Set block related
   * metadata like block commit sequence id, block count, bytes used and
   * pending delete block count and delete transaction id.
   * This method will verify checksum by default.
   * @param kvContainerData
   * @param config
   * @throws IOException
   */
  public static void parseKVContainerData(KeyValueContainerData kvContainerData,
      ConfigurationSource config) throws IOException {
    parseKVContainerData(kvContainerData, config, false);
  }

  /**
   * @param kvContainerData
   * @param config
   * @param skipVerifyChecksum checksum verification should be skipped if the state
   * has changed to RECOVERING during container import, false otherwise
   * @throws IOException
   */
  public static void parseKVContainerData(KeyValueContainerData kvContainerData,
      ConfigurationSource config, boolean skipVerifyChecksum) throws IOException {

    long containerID = kvContainerData.getContainerID();

    // Verify Checksum
    // skip verify checksum if the state has changed to RECOVERING during container import
    if (!skipVerifyChecksum) {
      ContainerUtils.verifyContainerFileChecksum(kvContainerData, config);
    }

    if (kvContainerData.getSchemaVersion() == null) {
      // If this container has not specified a schema version, it is in the old
      // format with one default column family.
      kvContainerData.setSchemaVersion(OzoneConsts.SCHEMA_V1);
    }

    File dbFile = KeyValueContainerLocationUtil.getContainerDBFile(
        kvContainerData);
    if (!dbFile.exists()) {
      LOG.error("Container DB file is missing at {} for ContainerID {}. " +
          "Skipping loading of this container.", dbFile, containerID);
      // Don't further process this container, as it is missing db file.
      throw new IOException("Container DB file is missing for containerID "
          + containerID);
    }
    kvContainerData.setDbFile(dbFile);

    DatanodeConfiguration dnConf =
        config.getObject(DatanodeConfiguration.class);
    boolean bCheckChunksFilePath = dnConf.getCheckEmptyContainerDir();

    if (kvContainerData.hasSchema(OzoneConsts.SCHEMA_V3)) {
      try (DBHandle db = BlockUtils.getDB(kvContainerData, config)) {
        populateContainerMetadata(kvContainerData,
            db.getStore(), bCheckChunksFilePath);
      }
      return;
    }

    DBHandle cachedDB = null;
    DatanodeStore store = null;
    try {
      try {
        // Open RocksDB in write mode, as it is required for container checksum updates and inspector repair operations.
        // The method KeyValueContainerMetadataInspector.buildErrorAndRepair will determine if write access to the DB
        // is permitted based on the mode.
        store = BlockUtils.getUncachedDatanodeStore(kvContainerData, config, false);
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
      populateContainerMetadata(kvContainerData, store, bCheckChunksFilePath);
    } finally {
      if (cachedDB != null) {
        // If we get a cached instance, calling close simply decrements the
        // reference count.
        cachedDB.close();
      } else if (store != null) {
        // We only stop the store if cacheDB is null, as otherwise we would
        // close the rocksDB handle in the cache and the next reader would fail
        store.stop();
      }
    }
  }

  private static void loadAndSetContainerDataChecksum(KeyValueContainerData kvContainerData,
                                                      Table<String, Long> metadataTable) {
    if (kvContainerData.isOpen()) {
      return;
    }

    try {
      Long containerDataChecksum = metadataTable.get(kvContainerData.getContainerDataChecksumKey());
      if (containerDataChecksum != null && kvContainerData.needsDataChecksum()) {
        kvContainerData.setDataChecksum(containerDataChecksum);
        return;
      }

      ContainerChecksumInfo containerChecksumInfo = ContainerChecksumTreeManager.readChecksumInfo(kvContainerData);
      if (ContainerChecksumTreeManager.hasDataChecksum(containerChecksumInfo) && kvContainerData.needsDataChecksum()) {
        containerDataChecksum = containerChecksumInfo.getContainerMerkleTree().getDataChecksum();
        kvContainerData.setDataChecksum(containerDataChecksum);
        metadataTable.put(kvContainerData.getContainerDataChecksumKey(), containerDataChecksum);
      }
    } catch (IOException ex) {
      LOG.warn("Failed to read checksum info for container {}", kvContainerData.getContainerID(), ex);
    }
  }

  private static void populateContainerMetadata(
      KeyValueContainerData kvContainerData, DatanodeStore store,
      boolean bCheckChunksFilePath)
      throws IOException {
    Table<String, Long> metadataTable = store.getMetadataTable();

    // Set pending deleted block count and bytes
    PendingDelete pendingDeletions = populatePendingDeletionMetadata(kvContainerData, metadataTable, store);

    // Set delete transaction id
    Long delTxnId = metadataTable.get(kvContainerData.getLatestDeleteTxnKey());
    if (delTxnId != null) {
      kvContainerData.updateDeleteTransactionId(delTxnId);
    }

    // Set BlockCommitSequenceId
    Long bcsId = metadataTable.get(kvContainerData.getBcsIdKey());
    if (bcsId != null) {
      kvContainerData.updateBlockCommitSequenceId(bcsId);
    }

    // Set block statistics
    populateBlockStatistics(kvContainerData, metadataTable, store);
    kvContainerData.getStatistics().setBlockPendingDeletion(pendingDeletions.getCount(), pendingDeletions.getBytes());

    // If the container is missing a chunks directory, possibly due to the
    // bug fixed by HDDS-6235, create it here.
    File chunksDir = new File(kvContainerData.getChunksPath());
    if (!chunksDir.exists()) {
      Files.createDirectories(chunksDir.toPath());
    }

    if (noBlocksInContainer(store, kvContainerData, bCheckChunksFilePath)) {
      kvContainerData.markAsEmpty();
    }

    loadAndSetContainerDataChecksum(kvContainerData, metadataTable);

    // Run advanced container inspection/repair operations if specified on
    // startup. If this method is called but not as a part of startup,
    // The inspectors will be unloaded and this will be a no-op.
    ContainerInspectorUtil.process(kvContainerData, store);

    // Load finalizeBlockLocalIds for container in memory.
    populateContainerFinalizeBlock(kvContainerData, store);
  }

  private static PendingDelete populatePendingDeletionMetadata(
      KeyValueContainerData kvContainerData, Table<String, Long> metadataTable,
      DatanodeStore store) throws IOException {

    Long pendingDeletionBlockBytes = metadataTable.get(kvContainerData.getPendingDeleteBlockBytesKey());
    Long pendingDeleteBlockCount = metadataTable.get(kvContainerData.getPendingDeleteBlockCountKey());

    if (!VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.STORAGE_SPACE_DISTRIBUTION)) {
      return handlePreDataDistributionFeature(pendingDeleteBlockCount, metadataTable, store, kvContainerData);
    } else if (pendingDeleteBlockCount != null) {
      return handlePostDataDistributionFeature(pendingDeleteBlockCount, pendingDeletionBlockBytes,
          metadataTable, store, kvContainerData);
    } else {
      LOG.warn("Missing pendingDeleteBlockCount/size from {}: recalculate them from delete txn tables",
          metadataTable.getName());
      return getAggregatePendingDelete(store, kvContainerData, kvContainerData.getSchemaVersion());
    }
  }

  private static PendingDelete handlePreDataDistributionFeature(
      Long pendingDeleteBlockCount, Table<String, Long> metadataTable,
      DatanodeStore store, KeyValueContainerData kvContainerData) throws IOException {

    if (pendingDeleteBlockCount != null) {
      return new PendingDelete(pendingDeleteBlockCount, 0L);
    } else {
      LOG.warn("Missing pendingDeleteBlockCount/size from {}: recalculate them from delete txn tables",
          metadataTable.getName());
      return getAggregatePendingDelete(store, kvContainerData, kvContainerData.getSchemaVersion());
    }
  }

  private static PendingDelete handlePostDataDistributionFeature(
      Long pendingDeleteBlockCount, Long pendingDeletionBlockBytes,
      Table<String, Long> metadataTable, DatanodeStore store,
      KeyValueContainerData kvContainerData) throws IOException {

    if (pendingDeletionBlockBytes != null) {
      return new PendingDelete(pendingDeleteBlockCount, pendingDeletionBlockBytes);
    } else {
      LOG.warn("Missing pendingDeleteBlockSize from {}: recalculate them from delete txn tables",
          metadataTable.getName());
      PendingDelete pendingDeletions = getAggregatePendingDelete(
          store, kvContainerData, kvContainerData.getSchemaVersion());
      return new PendingDelete(pendingDeleteBlockCount, pendingDeletions.getBytes());
    }
  }

  private static void populateBlockStatistics(
      KeyValueContainerData kvContainerData, Table<String, Long> metadataTable,
      DatanodeStore store) throws IOException {

    final Long metadataTableBytesUsed = metadataTable.get(kvContainerData.getBytesUsedKey());
    final Long metadataTableBlockCount = metadataTable.get(kvContainerData.getBlockCountKey());

    final long blockBytes;
    final long blockCount;

    if (metadataTableBytesUsed != null && metadataTableBlockCount != null) {
      blockBytes = metadataTableBytesUsed;
      blockCount = metadataTableBlockCount;
    } else {
      LOG.warn("Missing bytesUsed={} or blockCount={} from {}: recalculate them from block table",
          metadataTableBytesUsed, metadataTableBlockCount, metadataTable.getName());
      final ContainerData.BlockByteAndCounts blockData = getUsedBytesAndBlockCount(store, kvContainerData);
      blockBytes = blockData.getBytes();
      blockCount = blockData.getCount();
    }

    kvContainerData.getStatistics().updateBlocks(blockBytes, blockCount);
  }

  /**
   * Loads finalizeBlockLocalIds for container in memory.
   * @param kvContainerData - KeyValueContainerData
   * @param store - DatanodeStore
   * @throws IOException
   */
  private static void populateContainerFinalizeBlock(
      KeyValueContainerData kvContainerData, DatanodeStore store)
      throws IOException {
    if (store.getFinalizeBlocksTable() != null) {
      try (BlockIterator<Long> iter =
               store.getFinalizeBlockIterator(kvContainerData.getContainerID(),
                   kvContainerData.getUnprefixedKeyFilter())) {
        while (iter.hasNext()) {
          kvContainerData.addToFinalizedBlockSet(iter.nextBlock());
        }
      }
    }
  }

  private static ContainerData.BlockByteAndCounts getUsedBytesAndBlockCount(DatanodeStore store,
      KeyValueContainerData kvData) throws IOException {
    long blockCount = 0;
    long usedBytes = 0;

    try (BlockIterator<BlockData> blockIter =
             store.getBlockIterator(kvData.getContainerID(),
                 kvData.getUnprefixedKeyFilter())) {

      while (blockIter.hasNext()) {
        blockCount++;
        usedBytes += getBlockLengthTryCatch(blockIter.nextBlock());
      }
    }

    // Count all deleting blocks.
    try (BlockIterator<BlockData> blockIter =
             store.getBlockIterator(kvData.getContainerID(),
                 kvData.getDeletingBlockKeyFilter())) {

      while (blockIter.hasNext()) {
        blockCount++;
        usedBytes += getBlockLengthTryCatch(blockIter.nextBlock());
      }
    }
    return new ContainerData.BlockByteAndCounts(usedBytes, blockCount, 0, 0);
  }

  public static long getBlockLengthTryCatch(BlockData block) {
    try {
      return block.getChunks().stream()
          .mapToLong(ContainerProtos.ChunkInfo::getLen)
          .sum();
    } catch (Exception e) {
      LOG.error("Failed to getBlockLength for block {}", block.getBlockID(), e);
      return 0;
    }
  }

  public static long getBlockLength(BlockData block) {
    return block.getChunks().stream()
        .mapToLong(ContainerProtos.ChunkInfo::getLen)
        .sum();
  }

  public static boolean isSameSchemaVersion(String schema, String other) {
    String effective1 = schema != null ? schema : SCHEMA_V1;
    String effective2 = other != null ? other : SCHEMA_V1;
    return effective1.equals(effective2);
  }

  /**
   * Moves container directory to a new location
   * under "volume/hdds/cluster-id/tmp/deleted-containers"
   * and updates metadata and chunks path.
   * Containers will be moved under it before getting deleted
   * to avoid, in case of failure, having artifact leftovers
   * on the default container path on the disk.
   *
   * Delete operation for Schema &lt; V3
   * 1. Container is marked DELETED
   * 2. Container is removed from memory container set
   * 3. Container DB handler from cache is removed and closed
   * 4. Container directory renamed to tmp directory.
   * 5. Container is deleted from tmp directory.
   *
   * Delete operation for Schema V3
   * 1. Container is marked DELETED
   * 2. Container is removed from memory container set
   * 3. Container from DB is removed
   * 4. Container directory renamed to tmp directory.
   * 5. Container is deleted from tmp directory.
   *
   * @param keyValueContainerData
   */
  public static void moveToDeletedContainerDir(
      KeyValueContainerData keyValueContainerData,
      HddsVolume hddsVolume) throws IOException {
    String containerPath = keyValueContainerData.getContainerPath();
    File container = new File(containerPath);
    Path destinationDirPath = getTmpDirectoryPath(keyValueContainerData,
        hddsVolume);
    File destinationDirFile = destinationDirPath.toFile();

    // If a container by the same name was moved to the delete directory but
    // the final delete failed, clear it out before adding another container
    // with the same name.
    if (destinationDirFile.exists()) {
      FileUtils.deleteDirectory(destinationDirFile);
    }

    Files.move(container.toPath(), destinationDirPath);
    LOG.debug("Container {} has been successfully moved under {}",
        container.getName(), hddsVolume.getDeletedContainerDir());
  }

  public static Path getTmpDirectoryPath(
      KeyValueContainerData keyValueContainerData,
      HddsVolume hddsVolume) {
    String containerPath = keyValueContainerData.getContainerPath();
    File container = new File(containerPath);
    String containerDirName = container.getName();
    Path destinationDirPath = hddsVolume.getDeletedContainerDir().toPath()
        .resolve(Paths.get(containerDirName));
    return destinationDirPath;
  }
}
