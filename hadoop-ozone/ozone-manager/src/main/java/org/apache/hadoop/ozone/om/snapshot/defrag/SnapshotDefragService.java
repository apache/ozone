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

package org.apache.hadoop.ozone.om.snapshot.defrag;

import static java.nio.file.Files.createDirectories;
import static org.apache.commons.io.file.PathUtils.deleteDirectory;
import static org.apache.hadoop.hdds.StringUtils.getLexicographicallyHigherString;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DEFRAG_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DEFRAG_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT;
import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.BOOTSTRAP_LOCK;
import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.SNAPSHOT_DB_CONTENT_LOCK;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.SST_FILE_EXTENSION;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult.EmptyTaskResult;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.CodecBufferCodec;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RDBSstFileWriter;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.SstFileSetReader;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.snapshot.MultiSnapshotLocks;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager.WritableOmSnapshotLocalDataProvider;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.snapshot.diff.delta.CompositeDeltaDiffComputer;
import org.apache.hadoop.ozone.om.snapshot.util.TableMergeIterator;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.util.ClosableIterator;
import org.apache.hadoop.util.Time;
import org.apache.logging.log4j.util.Strings;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background service for defragmenting snapshots in the active snapshot chain.
 * When snapshots are taken, they capture the entire OM RocksDB state but may contain
 * fragmented data. This service defragments snapshots by creating new compacted
 * RocksDB instances with only the necessary data for tracked column families.
 * <p>
 * The service processes snapshots in the active chain sequentially, starting with
 * the first non-defragmented snapshot. For the first snapshot in the chain, it
 * performs a full defragmentation by copying all keys. For subsequent snapshots,
 * it uses incremental defragmentation based on diffs from the previous defragmented
 * snapshot.
 */
public class SnapshotDefragService extends BackgroundService
    implements BootstrapStateHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(SnapshotDefragService.class);

  // Use only a single thread for snapshot defragmentation to avoid conflicts
  private static final int DEFRAG_CORE_POOL_SIZE = 1;

  private final OzoneManager ozoneManager;
  private final AtomicLong runCount = new AtomicLong(0);

  // Number of snapshots to be processed in a single iteration
  private final long snapshotLimitPerTask;

  private final AtomicLong snapshotsDefraggedCount;
  private final AtomicBoolean running;

  private final MultiSnapshotLocks snapshotContentLocks;
  private final OzoneConfiguration conf;

  private final BootstrapStateHandler.Lock lock;
  private final String tmpDefragDir;
  private final OmSnapshotManager omSnapshotManager;
  private final OmSnapshotLocalDataManager snapshotLocalDataManager;
  private final List<UUID> lockIds;
  private final CompositeDeltaDiffComputer deltaDiffComputer;
  private final Path differTmpDir;

  public SnapshotDefragService(long interval, TimeUnit unit, long serviceTimeout,
      OzoneManager ozoneManager, OzoneConfiguration configuration) throws IOException {
    super("SnapshotDefragService", interval, unit, DEFRAG_CORE_POOL_SIZE,
        serviceTimeout, ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.omSnapshotManager = ozoneManager.getOmSnapshotManager();
    this.snapshotLocalDataManager = omSnapshotManager.getSnapshotLocalDataManager();
    this.snapshotLimitPerTask = configuration
        .getLong(SNAPSHOT_DEFRAG_LIMIT_PER_TASK,
            SNAPSHOT_DEFRAG_LIMIT_PER_TASK_DEFAULT);
    this.conf = configuration;
    snapshotsDefraggedCount = new AtomicLong(0);
    running = new AtomicBoolean(false);
    IOzoneManagerLock omLock = ozoneManager.getMetadataManager().getLock();
    this.snapshotContentLocks = new MultiSnapshotLocks(omLock, SNAPSHOT_DB_CONTENT_LOCK, true, 1);
    this.lock = new BootstrapStateHandler.Lock((readLock) ->
        omLock.acquireLock(BOOTSTRAP_LOCK, getServiceName(), readLock));
    Path tmpDefragDirPath = ozoneManager.getMetadataManager().getSnapshotParentDir().toAbsolutePath()
        .resolve("tmp_defrag");
    // Delete and recreate tmp dir if it exists
    if (tmpDefragDirPath.toFile().exists()) {
      deleteDirectory(tmpDefragDirPath);
    }
    this.tmpDefragDir = tmpDefragDirPath.toString();
    this.differTmpDir = tmpDefragDirPath.resolve("differSstFiles");
    createDirectories(differTmpDir);
    this.deltaDiffComputer = new CompositeDeltaDiffComputer(omSnapshotManager,
        ozoneManager.getMetadataManager(), differTmpDir, (status) -> {
      LOG.debug("Snapshot defragmentation diff status: {}", status);
    }, false, false);
    this.lockIds = new ArrayList<>(1);
  }

  @Override
  public void start() {
    running.set(true);
    super.start();
  }

  @VisibleForTesting
  public void pause() {
    running.set(false);
  }

  @VisibleForTesting
  public void resume() {
    running.set(true);
  }

  boolean isRunning() {
    return running.get();
  }

  /**
   * Determines whether the specified snapshot requires defragmentation and returns
   * a pair indicating the need for defragmentation and the corresponding version of the snapshot.
   *
   * @param snapshotInfo Information about the snapshot to be checked for defragmentation.
   * @return A pair containing a boolean value and an integer:
   *         - The boolean value indicates whether the snapshot requires defragmentation
   *         (true if needed, false otherwise).
   *         - The integer represents the version of the snapshot being evaluated.
   * @throws IOException If an I/O error occurs while accessing the local snapshot data or metadata.
   */
  @VisibleForTesting
  Pair<Boolean, Integer> needsDefragmentation(SnapshotInfo snapshotInfo) throws IOException {
    // Update snapshot local metadata to point to the correct previous snapshotId if it was different and check if
    // snapshot needs defrag.
    try (WritableOmSnapshotLocalDataProvider writableOmSnapshotLocalDataProvider =
             snapshotLocalDataManager.getWritableOmSnapshotLocalData(snapshotInfo)) {
      // Read snapshot local metadata from YAML
      // Check if snapshot needs compaction (defragmentation)
      writableOmSnapshotLocalDataProvider.commit();
      boolean needsDefrag = writableOmSnapshotLocalDataProvider.needsDefrag();
      OmSnapshotLocalData localData = writableOmSnapshotLocalDataProvider.getSnapshotLocalData();
      if (!needsDefrag) {
        Optional<OmSnapshotLocalData> previousLocalData =
            writableOmSnapshotLocalDataProvider.getPreviousSnapshotLocalData();
        LOG.debug("Skipping defragmentation since snapshot has already been defragmented: id : {}(version: {}=>{}) " +
                "previousId: {}(version: {})", snapshotInfo.getSnapshotId(), localData.getVersion(),
            localData.getVersionSstFileInfos().get(localData.getVersion()).getPreviousSnapshotVersion(),
            snapshotInfo.getPathPreviousSnapshotId(),
            previousLocalData.map(OmSnapshotLocalData::getVersion).orElse(null));
      } else {
        LOG.debug("Snapshot {} needsDefragmentation field value: true", snapshotInfo.getSnapshotId());
      }
      return Pair.of(needsDefrag, localData.getVersion());
    }
  }

  private Pair<String, String> getTableBounds(Table<String, ?> table) throws RocksDatabaseException, CodecException {
    String tableLowestValue = null, tableHighestValue = null;
    try (TableIterator<String, String> keyIterator = table.keyIterator()) {
      if (keyIterator.hasNext()) {
        // Setting the lowest value to the first key in the table.
        tableLowestValue = keyIterator.next();
      }
      keyIterator.seekToLast();
      if (keyIterator.hasNext()) {
        // Setting the highest value to the last key in the table.
        tableHighestValue = keyIterator.next();
      }
    }
    return Pair.of(tableLowestValue, tableHighestValue);
  }

  /**
   * Performs a full defragmentation process for specified tables in the metadata manager.
   * This method processes all the entries in the tables for the provided prefix information,
   * deletes specified key ranges, and compacts the tables to remove tombstones.
   *
   * @param checkpointDBStore the metadata manager responsible for managing tables during the checkpoint process
   * @param prefixInfo the prefix information used to identify bucket prefix and determine key ranges in the tables
   * @param incrementalTables the set of tables for which incremental defragmentation is performed.
   * @throws IOException if an I/O error occurs during table operations or compaction
   */
  @VisibleForTesting
  void performFullDefragmentation(DBStore checkpointDBStore, TablePrefixInfo prefixInfo,
      Set<String> incrementalTables) throws IOException {
    for (String table : incrementalTables) {
      Table<String, CodecBuffer> checkpointTable = checkpointDBStore.getTable(table, StringCodec.get(),
          CodecBufferCodec.get(true));
      String tableBucketPrefix = prefixInfo.getTablePrefix(table);
      String prefixUpperBound = getLexicographicallyHigherString(tableBucketPrefix);

      Pair<String, String> tableBounds = getTableBounds(checkpointTable);
      String tableLowestValue = tableBounds.getLeft();
      String tableHighestValue = tableBounds.getRight();

      // If lowest value is not null and if the bucket prefix corresponding to the table is greater than lower then
      // delete the range between lowest value and bucket prefix.
      if (tableLowestValue != null && tableLowestValue.compareTo(tableBucketPrefix) < 0) {
        checkpointTable.deleteRange(tableLowestValue, tableBucketPrefix);
      }
      // If highest value is not null and if the next higher lexicographical string of bucket prefix corresponding to
      // the table is less than equal to the highest value then delete the range between bucket prefix
      // and also the highest value.
      if (tableHighestValue != null && tableHighestValue.compareTo(prefixUpperBound) >= 0) {
        checkpointTable.deleteRange(prefixUpperBound, tableHighestValue);
        checkpointTable.delete(tableHighestValue);
      }
      // Compact the table completely with kForce to get rid of tombstones.
      try (ManagedCompactRangeOptions compactRangeOptions = new ManagedCompactRangeOptions()) {
        compactRangeOptions.setBottommostLevelCompaction(ManagedCompactRangeOptions.BottommostLevelCompaction.kForce);
        // Need to allow the range tombstones to change levels and get propogated to bottommost level.
        compactRangeOptions.setChangeLevel(true);
        checkpointDBStore.compactTable(table, compactRangeOptions);
      }
    }
  }

  /**
   * Spills table difference into an SST file based on the provided delta file paths,
   * current snapshot table, previous snapshot table, and an optional table key prefix.
   *
   * The method reads the delta files and compares the records against the snapshot tables.
   * Any differences, including tombstones (deleted entries), are written to a new SST file.
   *
   * @param deltaFilePaths the list of paths to the delta files to process
   * @param snapshotTable the current snapshot table for comparison
   * @param previousSnapshotTable the previous snapshot table for comparison
   * @param tableKeyPrefix the prefix for filtering certain keys, or null if all keys are to be included
   * @return a pair of the path of the created SST file containing the differences and a boolean
   *         indicating whether any delta entries were written (true if there are differences, false otherwise)
   * @throws IOException if an I/O error occurs during processing
   */
  private Pair<Path, Boolean> spillTableDiffIntoSstFile(List<Path> deltaFilePaths,
      Table<String, CodecBuffer> snapshotTable, Table<String, CodecBuffer> previousSnapshotTable,
      String tableKeyPrefix) throws IOException {
    String sstFileReaderUpperBound = null;
    if (Strings.isNotEmpty(tableKeyPrefix)) {
      sstFileReaderUpperBound = getLexicographicallyHigherString(tableKeyPrefix);
    }
    SstFileSetReader sstFileSetReader = new SstFileSetReader(deltaFilePaths);
    Path fileToBeIngested = differTmpDir.resolve(snapshotTable.getName() + "-" + UUID.randomUUID()
        + SST_FILE_EXTENSION);
    int deltaEntriesCount = 0;
    try (ClosableIterator<String> keysToCheck =
             sstFileSetReader.getKeyStreamWithTombstone(tableKeyPrefix, sstFileReaderUpperBound);
         TableMergeIterator<String, CodecBuffer> tableMergeIterator = new TableMergeIterator<>(keysToCheck,
             tableKeyPrefix, snapshotTable, previousSnapshotTable);
         RDBSstFileWriter rdbSstFileWriter = new RDBSstFileWriter(fileToBeIngested.toFile())) {
      while (tableMergeIterator.hasNext()) {
        Table.KeyValue<String, List<CodecBuffer>> kvs = tableMergeIterator.next();
        // Check if the values are equal or if they are not equal then the value should be written to the
        // delta sstFile.
        CodecBuffer snapValue = kvs.getValue().get(0);
        CodecBuffer prevValue = kvs.getValue().get(1);
        boolean valuesEqual = false;
        if (snapValue == null && prevValue == null) {
          valuesEqual = true;
        } else if (snapValue != null && prevValue != null) {
          valuesEqual = snapValue.readableBytes() == prevValue.readableBytes() && snapValue.startsWith(prevValue);
        }
        if (!valuesEqual) {
          try (CodecBuffer key = StringCodec.get().toDirectCodecBuffer(kvs.getKey())) {
            // If the value is null then add a tombstone to the delta sstFile.
            if (snapValue == null) {
              rdbSstFileWriter.delete(key);
            } else {
              rdbSstFileWriter.put(key, snapValue);
            }
          }
          deltaEntriesCount++;
        }
      }
    }
    // If there are no delta entries then delete the delta file. No need to ingest the file as a diff.
    return Pair.of(fileToBeIngested, deltaEntriesCount != 0);
  }

  /**
   * Performs an incremental defragmentation process, which involves determining
   * and processing delta files between snapshots for metadata updates. The method
   * computes the changes, manages file ingestion to the checkpoint metadata manager,
   * and ensures that all delta files are deleted after processing.
   *
   * @param previousSnapshotInfo information about the previous snapshot.
   * @param snapshotInfo information about the current snapshot for which
   *                     incremental defragmentation is performed.
   * @param snapshotVersion the version of the snapshot to be processed.
   * @param checkpointStore the dbStore instance where data
   *                        updates are ingested after being processed.
   * @param bucketPrefixInfo table prefix information associated with buckets,
   *                         used to determine bounds for processing keys.
   * @param incrementalTables the set of tables for which incremental defragmentation is performed.
   * @throws IOException if an I/O error occurs during processing.
   */
  @VisibleForTesting
  void performIncrementalDefragmentation(SnapshotInfo previousSnapshotInfo, SnapshotInfo snapshotInfo,
      int snapshotVersion, DBStore checkpointStore, TablePrefixInfo bucketPrefixInfo, Set<String> incrementalTables)
      throws IOException {
    // Map of delta files grouped on the basis of the tableName.
    Collection<Pair<Path, SstFileInfo>> allTableDeltaFiles = this.deltaDiffComputer.getDeltaFiles(
        previousSnapshotInfo, snapshotInfo, incrementalTables);

    Map<String, List<Path>> tableGroupedDeltaFiles = allTableDeltaFiles.stream()
        .collect(Collectors.groupingBy(pair -> pair.getValue().getColumnFamily(),
            Collectors.mapping(Pair::getKey, Collectors.toList())));

    String volumeName = snapshotInfo.getVolumeName();
    String bucketName = snapshotInfo.getBucketName();

    Set<Path> filesToBeDeleted = new HashSet<>();
    // All files computed as delta must be deleted irrespective of whether ingestion succeeded or not.
    allTableDeltaFiles.forEach(pair -> filesToBeDeleted.add(pair.getKey()));
    try (UncheckedAutoCloseableSupplier<OmSnapshot> snapshot =
             omSnapshotManager.getActiveSnapshot(volumeName, bucketName, snapshotInfo.getName());
         UncheckedAutoCloseableSupplier<OmSnapshot> previousSnapshot =
             omSnapshotManager.getActiveSnapshot(volumeName, bucketName, previousSnapshotInfo.getName())) {
      for (Map.Entry<String, List<Path>> entry : tableGroupedDeltaFiles.entrySet()) {
        String table = entry.getKey();
        List<Path> deltaFiles = entry.getValue();
        Path fileToBeIngested;
        if (deltaFiles.size() == 1 && snapshotVersion > 0) {
          // If there is only one delta file for the table and the snapshot version is also not 0 then the same delta
          // file can reingested into the checkpointStore.
          fileToBeIngested = deltaFiles.get(0);
        } else {
          Table<String, CodecBuffer> snapshotTable = snapshot.get().getMetadataManager().getStore()
              .getTable(table, StringCodec.get(), CodecBufferCodec.get(true));
          Table<String, CodecBuffer> previousSnapshotTable = previousSnapshot.get().getMetadataManager().getStore()
              .getTable(table, StringCodec.get(), CodecBufferCodec.get(true));
          String tableBucketPrefix = bucketPrefixInfo.getTablePrefix(table);
          Pair<Path, Boolean> spillResult = spillTableDiffIntoSstFile(deltaFiles, snapshotTable,
              previousSnapshotTable, tableBucketPrefix);
          fileToBeIngested = spillResult.getValue() ? spillResult.getLeft() : null;
          filesToBeDeleted.add(spillResult.getLeft());
        }
        if (fileToBeIngested != null) {
          if (!fileToBeIngested.toFile().exists()) {
            throw new IOException("Delta file does not exist: " + fileToBeIngested);
          }
          Table checkpointTable = checkpointStore.getTable(table);
          checkpointTable.loadFromFile(fileToBeIngested.toFile());
        }
      }
    } finally {
      for (Path path : filesToBeDeleted) {
        if (path.toFile().exists()) {
          if (!path.toFile().delete()) {
            LOG.warn("Failed to delete file: {}", path);
          }
        }
      }
    }
  }

  /**
   * Ingests non-incremental tables from a snapshot into a checkpoint database store.
   * This involves exporting table data from the snapshot to intermediate SST files
   * and ingesting them into the corresponding tables in the checkpoint database store.
   * Tables that are part of incremental defragmentation are excluded from this process.
   *
   * @param checkpointDBStore the database store where non-incremental tables are ingested.
   * @param snapshotInfo the metadata information of the snapshot being processed.
   * @param bucketPrefixInfo prefix information used for determining table prefixes.
   * @param incrementalTables the set of tables identified for incremental defragmentation.
   * @throws IOException if an I/O error occurs during table ingestion or file operations.
   */
  @VisibleForTesting
  void ingestNonIncrementalTables(DBStore checkpointDBStore, SnapshotInfo snapshotInfo,
      TablePrefixInfo bucketPrefixInfo, Set<String> incrementalTables) throws IOException {
    String volumeName = snapshotInfo.getVolumeName();
    String bucketName = snapshotInfo.getBucketName();
    String snapshotName = snapshotInfo.getName();
    Set<Path> filesToBeDeleted = new HashSet<>();
    try (UncheckedAutoCloseableSupplier<OmSnapshot> snapshot = omSnapshotManager.getActiveSnapshot(volumeName,
        bucketName, snapshotName)) {
      DBStore snapshotDBStore = snapshot.get().getMetadataManager().getStore();
      for (Table snapshotTable : snapshotDBStore.listTables()) {
        String snapshotTableName = snapshotTable.getName();
        if (!incrementalTables.contains(snapshotTable.getName())) {
          Path tmpSstFile = differTmpDir.resolve(snapshotTable.getName() + "-" + UUID.randomUUID()
              + SST_FILE_EXTENSION);
          filesToBeDeleted.add(tmpSstFile);
          String prefix = bucketPrefixInfo.getTablePrefix(snapshotTableName);
          try (CodecBuffer prefixBytes = Strings.isBlank(prefix) ? null :
              StringCodec.get().toDirectCodecBuffer(prefix)) {
            Table<CodecBuffer, CodecBuffer> snapshotTableBytes = snapshotDBStore.getTable(snapshotTableName,
                CodecBufferCodec.get(true), CodecBufferCodec.get(true));
            snapshotTableBytes.dumpToFileWithPrefix(tmpSstFile.toFile(), prefixBytes);
          }
          Table<CodecBuffer, CodecBuffer> checkpointTable = checkpointDBStore.getTable(snapshotTableName,
              CodecBufferCodec.get(true), CodecBufferCodec.get(true));
          checkpointTable.loadFromFile(tmpSstFile.toFile());
        }
      }
    } finally {
      for (Path path : filesToBeDeleted) {
        if (path.toFile().exists()) {
          if (!path.toFile().delete()) {
            LOG.warn("Failed to delete file for ingesting non incremental table: {}", path);
          }
        }
      }
    }
  }

  /**
   * Atomically switches the current snapshot database to a new version derived
   * from the provided checkpoint directory. This involves moving the checkpoint
   * path to a versioned directory, updating the snapshot metadata, and committing
   * the changes to persist the snapshot version update.
   *
   * @param snapshotId The UUID identifying the snapshot to update.
   * @param checkpointPath The path to the checkpoint directory that serves as the basis
   *                       for the updated snapshot version.
   * @return The previous version number of the snapshot prior to the update.
   * @throws IOException If an I/O error occurs during file operations, checkpoint processing,
   *                     or snapshot metadata updates.
   */
  @VisibleForTesting
  int atomicSwitchSnapshotDB(UUID snapshotId, Path checkpointPath) throws IOException {
    try (WritableOmSnapshotLocalDataProvider snapshotLocalDataProvider =
             snapshotLocalDataManager.getWritableOmSnapshotLocalData(snapshotId)) {
      OmSnapshotLocalData localData = snapshotLocalDataProvider.getSnapshotLocalData();
      Path nextVersionPath = OmSnapshotManager.getSnapshotPath(ozoneManager.getMetadataManager(), snapshotId,
          localData.getVersion() + 1);
      // Remove the directory if it exists.
      if (nextVersionPath.toFile().exists()) {
        deleteDirectory(nextVersionPath);
      }
      // Move the checkpoint directory to the next version directory.
      LOG.info("Moving checkpoint directory {} to {}", checkpointPath, nextVersionPath);
      Files.move(checkpointPath, nextVersionPath);
      RocksDBCheckpoint dbCheckpoint = new RocksDBCheckpoint(nextVersionPath);
      // Add a new version to the local data file.
      try (OmMetadataManagerImpl newVersionCheckpointMetadataManager =
               OmMetadataManagerImpl.createCheckpointMetadataManager(conf, dbCheckpoint, true)) {
        RDBStore newVersionCheckpointStore = (RDBStore) newVersionCheckpointMetadataManager.getStore();
        snapshotLocalDataProvider.addSnapshotVersion(newVersionCheckpointStore);
        snapshotLocalDataProvider.commit();
      }
      return localData.getVersion() - 1;
    }
  }

  private final class SnapshotDefragTask implements BackgroundTask {

    @Override
    public BackgroundTaskResult call() throws Exception {
      // Check OM leader and readiness
      if (shouldRun()) {
        triggerSnapshotDefragOnce();
      }

      return EmptyTaskResult.newResult();
    }
  }

  /**
   * Creates a new checkpoint by modifying the metadata manager from a snapshot.
   * This involves generating a temporary checkpoint and truncating specified
   * column families from the checkpoint before returning the updated metadata manager.
   *
   * @param snapshotInfo Information about the snapshot for which the checkpoint
   *                     is being created.
   * @param incrementalColumnFamilies A set of table names representing incremental
   *                                   column families to be retained in the checkpoint.
   * @return A new instance of OmMetadataManagerImpl initialized with the modified
   *         checkpoint.
   * @throws IOException If an I/O error occurs during snapshot processing,
   *                     checkpoint creation, or table operations.
   */
  @VisibleForTesting
  OmMetadataManagerImpl createCheckpoint(SnapshotInfo snapshotInfo,
      Set<String> incrementalColumnFamilies) throws IOException {
    try (UncheckedAutoCloseableSupplier<OmSnapshot> snapshot = omSnapshotManager.getActiveSnapshot(
        snapshotInfo.getVolumeName(), snapshotInfo.getBucketName(), snapshotInfo.getName())) {
      DBCheckpoint checkpoint = snapshot.get().getMetadataManager().getStore().getCheckpoint(tmpDefragDir, true);
      try (OmMetadataManagerImpl metadataManagerBeforeTruncate =
               OmMetadataManagerImpl.createCheckpointMetadataManager(conf, checkpoint, false)) {
        DBStore dbStore = metadataManagerBeforeTruncate.getStore();
        for (String table : metadataManagerBeforeTruncate.listTableNames()) {
          if (!incrementalColumnFamilies.contains(table)) {
            dbStore.dropTable(table);
          }
        }
      } catch (Exception e) {
        throw new IOException("Failed to close checkpoint of snapshot: " + snapshotInfo.getSnapshotId(), e);
      }
      // This will recreate the column families in the checkpoint.
      return OmMetadataManagerImpl.createCheckpointMetadataManager(conf, checkpoint, false);
    }
  }

  private void acquireContentLock(UUID snapshotID) throws IOException {
    lockIds.clear();
    lockIds.add(snapshotID);
    OMLockDetails lockDetails = snapshotContentLocks.acquireLock(lockIds);
    if (!lockDetails.isLockAcquired()) {
      throw new IOException("Failed to acquire lock on snapshot: " + snapshotID);
    }
    LOG.debug("Acquired MultiSnapshotLocks on snapshot: {}", snapshotID);
  }

  @VisibleForTesting
  boolean checkAndDefragSnapshot(SnapshotChainManager chainManager, UUID snapshotId) throws IOException {
    SnapshotInfo snapshotInfo = SnapshotUtils.getSnapshotInfo(ozoneManager, chainManager, snapshotId);

    if (snapshotInfo.getSnapshotStatus() != SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE) {
      LOG.debug("Skipping defragmentation for non-active snapshot: {} (ID: {})",
          snapshotInfo.getName(), snapshotInfo.getSnapshotId());
      return false;
    }
    Pair<Boolean, Integer> needsDefragVersionPair = needsDefragmentation(snapshotInfo);
    if (!needsDefragVersionPair.getLeft()) {
      return false;
    }
    LOG.info("Defragmenting snapshot: {} (ID: {})", snapshotInfo.getTableKey(), snapshotInfo.getSnapshotId());
    long start = Time.monotonicNow();
    // Create a checkpoint of the previous snapshot or the current snapshot if it is the first snapshot in the chain.
    SnapshotInfo checkpointSnapshotInfo = snapshotInfo.getPathPreviousSnapshotId() == null ? snapshotInfo :
        SnapshotUtils.getSnapshotInfo(ozoneManager, chainManager, snapshotInfo.getPathPreviousSnapshotId());

    OmMetadataManagerImpl checkpointMetadataManager = createCheckpoint(checkpointSnapshotInfo,
        COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT);
    Path checkpointLocation = checkpointMetadataManager.getStore().getDbLocation().toPath();
    try {
      DBStore checkpointDBStore = checkpointMetadataManager.getStore();
      TablePrefixInfo prefixInfo = ozoneManager.getMetadataManager().getTableBucketPrefix(snapshotInfo.getVolumeName(),
          snapshotInfo.getBucketName());
      // If first snapshot in the chain perform full defragmentation.
      if (snapshotInfo.getPathPreviousSnapshotId() == null) {
        LOG.info("Performing full defragmentation for snapshot: {} (ID: {})", snapshotInfo.getTableKey(),
            snapshotInfo.getSnapshotId());
        performFullDefragmentation(checkpointDBStore, prefixInfo, COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT);
      } else {
        LOG.info("Performing incremental defragmentation for snapshot: {} (ID: {})", snapshotInfo.getTableKey(),
            snapshotInfo.getSnapshotId());
        performIncrementalDefragmentation(checkpointSnapshotInfo, snapshotInfo, needsDefragVersionPair.getValue(),
            checkpointDBStore, prefixInfo, COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT);
      }
      int previousVersion;
      // Acquire Content lock on the snapshot to ensure the contents of the table doesn't get changed.
      acquireContentLock(snapshotId);
      try {
        // Ingestion of incremental tables KeyTable/FileTable/DirectoryTable done now we need to just reingest the
        // remaining tables from the original snapshot.
        ingestNonIncrementalTables(checkpointDBStore, snapshotInfo, prefixInfo, COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT);
        checkpointMetadataManager.close();
        checkpointMetadataManager = null;
        // Switch the snapshot DB location to the new version.
        previousVersion  = atomicSwitchSnapshotDB(snapshotId, checkpointLocation);
        omSnapshotManager.deleteSnapshotCheckpointDirectories(snapshotId, previousVersion);
      } finally {
        snapshotContentLocks.releaseLock();
      }
      LOG.info("Completed defragmentation for snapshot: {} (ID: {}) in {} ms", snapshotInfo.getTableKey(),
          snapshotInfo.getSnapshotId(), Time.monotonicNow() - start);
    } finally {
      if (checkpointMetadataManager != null) {
        checkpointMetadataManager.close();
      }
    }
    return true;
  }

  public synchronized boolean triggerSnapshotDefragOnce() throws IOException {

    final long count = runCount.incrementAndGet();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initiating Snapshot Defragmentation Task: run # {}", count);
    }

    Optional<OmSnapshotManager> snapshotManager = Optional.ofNullable(ozoneManager)
        .map(OzoneManager::getOmSnapshotManager);
    if (!snapshotManager.isPresent()) {
      LOG.debug("OmSnapshotManager not available, skipping defragmentation task");
      return false;
    }

    // Get the SnapshotChainManager to iterate through the global snapshot chain
    final SnapshotChainManager snapshotChainManager =
        ((OmMetadataManagerImpl) ozoneManager.getMetadataManager()).getSnapshotChainManager();

    // Use iterator(false) to iterate forward through the snapshot chain
    Iterator<UUID> snapshotIterator = snapshotChainManager.iterator(false);

    long snapshotLimit = snapshotLimitPerTask;
    while (snapshotLimit > 0 && running.get() && snapshotIterator.hasNext()) {
      // Get SnapshotInfo for the current snapshot in the chain.
      UUID snapshotId = snapshotIterator.next();
      try (UncheckedAutoCloseable lock = getBootstrapStateLock().acquireReadLock()) {
        if (checkAndDefragSnapshot(snapshotChainManager, snapshotId)) {
          snapshotLimit--;
          snapshotsDefraggedCount.getAndIncrement();
        }
      } catch (IOException e) {
        LOG.error("Exception while defragmenting snapshot: {}", snapshotId, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.error("Snapshot defragmentation task interrupted", e);
        return false;
      }
    }
    return true;
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    // TODO: Can be parallelized for different buckets
    queue.add(new SnapshotDefragTask());
    return queue;
  }

  /**
   * Returns true if the service run conditions are satisfied, false otherwise.
   */
  private boolean shouldRun() {
    if (ozoneManager == null) {
      // OzoneManager can be null for testing
      return true;
    }
    if (ozoneManager.getOmRatisServer() == null) {
      LOG.warn("OzoneManagerRatisServer is not initialized yet");
      return false;
    }
    // The service only runs if current OM node is ready
    return running.get() && ozoneManager.isRunning() &&
        ozoneManager.getVersionManager().isAllowed(OMLayoutFeature.SNAPSHOT_DEFRAG);
  }

  public AtomicLong getSnapshotsDefraggedCount() {
    return snapshotsDefraggedCount;
  }

  @Override
  public BootstrapStateHandler.Lock getBootstrapStateLock() {
    return lock;
  }

  @Override
  public void shutdown() {
    running.set(false);
    super.shutdown();
    try {
      deltaDiffComputer.close();
    } catch (IOException e) {
      LOG.error("Error while closing delta diff computer.", e);
    }
    Path tmpDirPath =  Paths.get(tmpDefragDir);
    if (tmpDirPath.toFile().exists()) {
      try {
        deleteDirectory(tmpDirPath);
      } catch (IOException e) {
        LOG.error("Failed to delete temporary directory: {}", tmpDirPath, e);
      }
    }
  }
}
