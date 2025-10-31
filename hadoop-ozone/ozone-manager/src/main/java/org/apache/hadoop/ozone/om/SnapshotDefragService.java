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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DEFRAGGED_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DEFRAG_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DEFRAG_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.lock.FlatResource.SNAPSHOT_GC_LOCK;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.getColumnFamilyToKeyPrefixMap;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult.EmptyTaskResult;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RDBSstFileWriter;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRawSSTFileReader;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.snapshot.MultiSnapshotLocks;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager.WritableOmSnapshotLocalDataProvider;
import org.apache.hadoop.ozone.om.snapshot.SnapshotDiffManager;
import org.apache.ozone.rocksdb.util.SstFileSetReader;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.rocksdb.RocksDBException;
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

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotDefragService.class);

  // Use only a single thread for snapshot defragmentation to avoid conflicts
  private static final int DEFRAG_CORE_POOL_SIZE = 1;
  private static final String CHECKPOINT_STATE_DEFRAGGED_DIR = OM_SNAPSHOT_CHECKPOINT_DEFRAGGED_DIR;

  private final OzoneManager ozoneManager;
  // Number of snapshots to be processed in a single iteration
  private final long snapshotLimitPerTask;
  private final AtomicLong snapshotsDefraggedCount;
  private final AtomicLong runCount = new AtomicLong(0);
  private final AtomicBoolean running;
  private final MultiSnapshotLocks snapshotIdLocks;
  private final BootstrapStateHandler.Lock lock = new BootstrapStateHandler.Lock();

  public SnapshotDefragService(long interval, TimeUnit unit, long serviceTimeout,
      OzoneManager ozoneManager, OzoneConfiguration configuration) {
    super(SnapshotDefragService.class.getSimpleName(), interval, unit, DEFRAG_CORE_POOL_SIZE,
        serviceTimeout, ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.snapshotLimitPerTask = configuration
        .getLong(SNAPSHOT_DEFRAG_LIMIT_PER_TASK,
            SNAPSHOT_DEFRAG_LIMIT_PER_TASK_DEFAULT);
    snapshotsDefraggedCount = new AtomicLong(0);
    running = new AtomicBoolean(false);
    IOzoneManagerLock omLock = ozoneManager.getMetadataManager().getLock();
    this.snapshotIdLocks = new MultiSnapshotLocks(omLock, SNAPSHOT_GC_LOCK, true);
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

  /**
   * Checks if rocks-tools native library is available.
   */
  private boolean isRocksToolsNativeLibAvailable() {
    try {
      return ManagedRawSSTFileReader.tryLoadLibrary();
    } catch (Exception e) {
      LOG.warn("Failed to check native code availability", e);
      return false;
    }
  }

  /**
   * Checks if a snapshot needs defragmentation by examining its YAML metadata.
   */
  @VisibleForTesting
  public boolean needsDefragmentation(SnapshotInfo snapshotInfo) {
    try (OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataProvider readableOmSnapshotLocalDataProvider =
             ozoneManager.getOmSnapshotManager().getSnapshotLocalDataManager().getOmSnapshotLocalData(snapshotInfo)) {
      // Read snapshot local metadata from YAML
      OmSnapshotLocalData snapshotLocalData = readableOmSnapshotLocalDataProvider.getSnapshotLocalData();

      // Check if snapshot needs compaction (defragmentation)
      boolean needsDefrag = snapshotLocalData.getNeedsDefrag();
      LOG.debug("Snapshot {} needsDefragmentation field value: {}",
          snapshotInfo.getName(), needsDefrag);

      return needsDefrag;
    } catch (IOException e) {
      LOG.warn("Failed to read YAML metadata for snapshot {}, assuming defrag needed",
          snapshotInfo.getName(), e);
      return true;
    }
  }

  /**
   * Returns a lexicographically higher string by appending a byte with maximum value.
   * For example, "/" -> "/\xFF"
   */
  private String getLexicographicallyHigherString(String str) {
    return str + "\uFFFF";
  }

  /**
   * Deletes unwanted key ranges from a column family.
   * Deletes ranges: ["", prefix) and [lexHigher(prefix), lexHigher("/")]
   */
  private void deleteUnwantedRanges(RocksDatabase db, RocksDatabase.ColumnFamily cf,
      String prefix, String cfName) throws RocksDatabaseException {
    if (cf == null) {
      LOG.warn("Column family {} not found, skipping range deletion", cfName);
      return;
    }

    try (ManagedWriteBatch writeBatch = new ManagedWriteBatch()) {
      // Delete range ["", prefix)
      byte[] emptyKey = "".getBytes(StandardCharsets.UTF_8);
      byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
      cf.batchDeleteRange(writeBatch, emptyKey, prefixBytes);

      // Delete range [lexicographicalHigherString(prefix), lexicographicalHigherString("/")]
      String highPrefixStr = getLexicographicallyHigherString(prefix);
      byte[] highPrefix = highPrefixStr.getBytes(StandardCharsets.UTF_8);
      byte[] highSlash = getLexicographicallyHigherString("/").getBytes(StandardCharsets.UTF_8);
      cf.batchDeleteRange(writeBatch, highPrefix, highSlash);

      db.batchWrite(writeBatch);
      LOG.info("Deleted unwanted ranges from {}", cfName);
    }
  }

  /**
   * Processes the checkpoint DB: deletes unwanted ranges and compacts.
   */
  private void processCheckpointDb(
      String checkpointDirName, RocksDatabase originalDb, SnapshotInfo snapshotInfo) throws IOException {
    // Step 2: Create checkpoint MetadataManager after taking a checkpoint
    LOG.info("Step 2: Opening checkpoint DB for defragmentation. checkpointDirName = {}", checkpointDirName);

    final String volumeName = snapshotInfo.getVolumeName();
    final String bucketName = snapshotInfo.getBucketName();
    OzoneConfiguration conf = ozoneManager.getConfiguration();
    final int maxOpenSstFilesInSnapshotDb = conf.getInt(
        OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES, OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES_DEFAULT);

    final String snapshotDirName = checkpointDirName.substring(OM_DB_NAME.length());
    try (OMMetadataManager defragDbMetadataManager = new OmMetadataManagerImpl(
        conf, snapshotDirName, maxOpenSstFilesInSnapshotDb, true)) {
      LOG.info("Opened checkpoint DB for defragmentation");
      try (RDBStore rdbStore = (RDBStore) defragDbMetadataManager.getStore()) {
        try (RocksDatabase checkpointDb = rdbStore.getDb()) {

          // Step 3-5: DeleteRange from tables
          final String obsPrefix = defragDbMetadataManager.getOzoneKey(volumeName, bucketName, OM_KEY_PREFIX);
          // TODO: Double check OBS prefix calculation
          LOG.info("Step 3: Deleting unwanted ranges from KeyTable. obsPrefix = {}", obsPrefix);
          deleteUnwantedRanges(checkpointDb, checkpointDb.getColumnFamily(KEY_TABLE),
              obsPrefix, KEY_TABLE);

          final String fsoPrefix = defragDbMetadataManager.getOzoneKeyFSO(volumeName, bucketName, OM_KEY_PREFIX);
          // TODO: Double check FSO prefix calculation
          LOG.info("Step 4: Deleting unwanted ranges from DirectoryTable. fsoPrefix = {}", fsoPrefix);
          deleteUnwantedRanges(checkpointDb, checkpointDb.getColumnFamily(DIRECTORY_TABLE),
              fsoPrefix, DIRECTORY_TABLE);

          LOG.info("Step 5: Deleting unwanted ranges from FileTable. fsoPrefix = {}", fsoPrefix);
          deleteUnwantedRanges(checkpointDb, checkpointDb.getColumnFamily(FILE_TABLE),
              fsoPrefix, FILE_TABLE);

          // Do we need to drop other tables here as well?

          // Step 6: Force compact all tables in the checkpoint
          LOG.info("Step 6: Force compacting all tables in checkpoint DB");
          try (ManagedCompactRangeOptions compactOptions = new ManagedCompactRangeOptions()) {
            compactOptions.setChangeLevel(true);
            compactOptions.setBottommostLevelCompaction(
                ManagedCompactRangeOptions.BottommostLevelCompaction.kForce);
            checkpointDb.compactDB(compactOptions);
          }
          LOG.info("Completed force compaction of all tables");

          // Verify defrag DB integrity
          verifyDbIntegrity(originalDb, checkpointDb, snapshotInfo);

          // Update snapshot metadata to mark defragmentation as complete
          updateSnapshotMetadataAfterDefrag(rdbStore, snapshotInfo);
        }
      }
    } catch (IOException e) {
      throw new IOException("Failed to process checkpoint DB", e);
    } catch (Exception e) {
      // Handle Exception from AutoCloseable.close()
      throw new IOException("Failed to process checkpoint DB", e);
    }
  }

  /**
   * Performs full defragmentation for the first snapshot in the chain.
   * Steps:
   * 1. Take checkpoint of current DB
   * 2. Create checkpoint MetadataManager after taking a checkpoint
   * 3. DeleteRange KeyTable from ["", obsPrefix) +
   *    [lexicographicalHigherString(keyTablePrefix), lexicographicalHigherString("/")]
   * 4. DeleteRange DirectoryTable from ["", fsoPrefix) +
   *    [lexicographicalHigherString(fsoPrefix), lexicographicalHigherString("/")]
   * 5. DeleteRange FileTable from ["", fsoPrefix) +
   *    [lexicographicalHigherString(fsoPrefix), lexicographicalHigherString("/")]
   * 6. Force compact all tables in the checkpoint.
   */
  private void performFullDefragmentation(SnapshotInfo snapshotInfo,
      OmSnapshot omSnapshot) throws IOException {

    String snapshotPath = OmSnapshotManager.getSnapshotPath(ozoneManager.getConfiguration(), snapshotInfo);

    // For defraggedDbPath, we need to go up to the parent directory and use checkpointStateDefragged
    String parentDir = OMStorage.getOmDbDir(ozoneManager.getConfiguration()).toString();
    String checkpointDirName = OM_DB_NAME + snapshotInfo.getCheckpointDirName();
    // TODO: Append version number to defraggedDbPath after defrag. e.g. om.db-fef74426-d01b-4c67-b20b-7750376c17dd-v1
    String defraggedDbPath = Paths.get(parentDir, CHECKPOINT_STATE_DEFRAGGED_DIR, checkpointDirName).toString();

    LOG.info("Starting full defragmentation for snapshot: {} at path: {}",
        snapshotInfo.getName(), snapshotPath);
    LOG.info("Target defragmented DB path: {}", defraggedDbPath);

    // Get snapshot checkpoint DB
    RDBStore originalStore = (RDBStore) omSnapshot.getMetadataManager().getStore();
    RocksDatabase originalDb = originalStore.getDb();
    if (originalDb == null) {
      throw new IOException("Failed to get RocksDatabase from original snapshot store");
    }
    assert !originalDb.isClosed();

    try {
      LOG.info("Starting defragmentation process for snapshot: {}", snapshotInfo.getName());
      LOG.info("Defragmented DB path: {}", defraggedDbPath);

      // Step 1: Take checkpoint of current DB
      LOG.info("Step 1: Creating checkpoint from original DB to defragmented path");
      try (RocksDatabase.RocksCheckpoint checkpoint = originalDb.createCheckpoint()) {
        checkpoint.createCheckpoint(Paths.get(defraggedDbPath));
        LOG.info("Created checkpoint at: {}", defraggedDbPath);
      }

      // Steps 2-6: Process checkpoint DB (delete ranges and compact)
      processCheckpointDb(checkpointDirName, originalDb, snapshotInfo);

      LOG.info("Successfully completed full defragmentation for snapshot: {}",
          snapshotInfo.getName());

    } catch (RocksDatabaseException e) {
      LOG.error("RocksDB error during defragmentation of snapshot: {}", snapshotInfo.getName(), e);
      throw new IOException("Failed to defragment snapshot: " + snapshotInfo.getName(), e);
    } catch (Exception e) {
      LOG.error("Unexpected error during defragmentation of snapshot: {}", snapshotInfo.getName(), e);
      throw new IOException("Failed to defragment snapshot: " + snapshotInfo.getName(), e);
    }
  }

  /**
   * Performs incremental defragmentation using diff from previous defragmented snapshot.
   */
  private void performIncrementalDefragmentation(SnapshotInfo currentSnapshot,
      SnapshotInfo previousDefraggedSnapshot, OmSnapshot currentOmSnapshot)
      throws IOException {

    // Fix path construction similar to performFullDefragmentation
    String previousParentDir = OMStorage.getOmDbDir(ozoneManager.getConfiguration()).toString();
    String previousCheckpointDirName = OM_DB_NAME + previousDefraggedSnapshot.getCheckpointDirName();
    // TODO: Append version number
    String previousDefraggedDbPath = Paths.get(previousParentDir, CHECKPOINT_STATE_DEFRAGGED_DIR,
        previousCheckpointDirName).toString();

    String currentParentDir = OMStorage.getOmDbDir(ozoneManager.getConfiguration()).toString();
    String currentCheckpointDirName = OM_DB_NAME + currentSnapshot.getCheckpointDirName();
    // TODO: Append version number as well. e.g. om.db-fef74426-d01b-4c67-b20b-7750376c17dd-v1
    String currentDefraggedDbPath = Paths.get(currentParentDir, CHECKPOINT_STATE_DEFRAGGED_DIR,
        currentCheckpointDirName).toString();

    LOG.info("Starting incremental defragmentation for snapshot: {} using previous defragged snapshot: {}",
        currentSnapshot.getName(), previousDefraggedSnapshot.getName());
    LOG.info("Previous defragmented DB: {}", previousDefraggedDbPath);
    LOG.info("Current target DB: {}", currentDefraggedDbPath);

    // Note: Don't create target directory - RocksDB createCheckpoint() will create it
    // and will fail with "Directory exists" if we create it first

    try {
      // Check if previous defragmented DB exists
      if (!Files.exists(Paths.get(previousDefraggedDbPath))) {
        // TODO: Should err and quit instead of falling back to full defrag
        LOG.warn("Previous defragmented DB not found at {}, falling back to full defragmentation",
            previousDefraggedDbPath);
        performFullDefragmentation(currentSnapshot, currentOmSnapshot);
        return;
      }

      // Create a checkpoint from the previous defragmented DB directly at target location
      LOG.info("Creating checkpoint from previous defragmented DB directly to target location");

      final OmSnapshotManager snapshotManager = ozoneManager.getOmSnapshotManager();
      try (UncheckedAutoCloseableSupplier<OmSnapshot> prevSnapshotSupplier =
               snapshotManager.getSnapshot(previousDefraggedSnapshot.getSnapshotId())) {
        LOG.info("Opened previous (defragged) snapshot: {}", previousDefraggedSnapshot.getName());
        OmSnapshot prevSnapshot = prevSnapshotSupplier.get();
        // Sanity check: Ensure previous snapshot is marked as defragmented
        if (needsDefragmentation(previousDefraggedSnapshot)) {
          LOG.error("Previous snapshot {} is not marked as defragmented. Something is wrong.",
              previousDefraggedSnapshot.getName());
          return;
        }
        RDBStore prevStore = (RDBStore) prevSnapshot.getMetadataManager().getStore();
        RocksDatabase prevDb = prevStore.getDb();
        if (prevDb == null) {
          throw new IOException("Failed to get RocksDatabase from previous defragmented snapshot store");
        }
        assert !prevDb.isClosed();

        try (RocksDatabase.RocksCheckpoint checkpoint = prevDb.createCheckpoint()) {
          checkpoint.createCheckpoint(Paths.get(currentDefraggedDbPath));
          LOG.info("Created checkpoint at: {}", currentDefraggedDbPath);
        }
      }

      OzoneConfiguration conf = ozoneManager.getConfiguration();
      final int maxOpenSstFilesInSnapshotDb = conf.getInt(
          OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES, OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES_DEFAULT);

      final String currSnapshotDirName = currentCheckpointDirName.substring(OM_DB_NAME.length());
      try (OMMetadataManager currDefragDbMetadataManager = new OmMetadataManagerImpl(
          conf, currSnapshotDirName, maxOpenSstFilesInSnapshotDb, true)) {
        LOG.info("Opened OMMetadataManager for checkpoint DB for incremental update");
        try (RDBStore currentDefraggedStore = (RDBStore) currDefragDbMetadataManager.getStore()) {
          try (RocksDatabase currentDefraggedDb = currentDefraggedStore.getDb()) {
            LOG.info("Opened checkpoint as working defragmented DB for incremental update");

            // Apply incremental changes from current snapshot
            RDBStore currentSnapshotStore = (RDBStore) currentOmSnapshot.getMetadataManager().getStore();
            RocksDatabase currentSnapshotDb = currentSnapshotStore.getDb();

            long incrementalKeysCopied = applyIncrementalChanges(currentSnapshotDb, currentDefraggedStore,
                currentSnapshot, previousDefraggedSnapshot);

            LOG.info("Applied {} incremental changes for snapshot: {}",
                incrementalKeysCopied, currentSnapshot.getName());

            // Verify defrag DB integrity. TODO: Abort and stop defrag service if verification fails?
            verifyDbIntegrity(currentSnapshotDb, currentDefraggedDb, currentSnapshot);

            // Create a new version in YAML metadata, which also indicates that the defragmentation is complete
            updateSnapshotMetadataAfterDefrag(currentDefraggedStore, currentSnapshot);

            LOG.info("Successfully completed incremental defragmentation for snapshot: {} with {} incremental changes",
                currentSnapshot.getName(), incrementalKeysCopied);
          }
        }
      }

    } catch (RocksDatabaseException e) {
      LOG.error("RocksDB error during incremental defragmentation of snapshot: {}",
          currentSnapshot.getName(), e);
//      LOG.warn("Falling back to full defragmentation due to error");
//      performFullDefragmentation(currentSnapshot, currentOmSnapshot);
    } catch (Exception e) {
      LOG.error("Unexpected error during incremental defragmentation of snapshot: {}",
          currentSnapshot.getName(), e);
      LOG.warn("Falling back to full defragmentation due to error");
      performFullDefragmentation(currentSnapshot, currentOmSnapshot);
    }
  }

  /**
   * Applies incremental changes by using SnapshotDiffManager#getDeltaFiles() to get SST files,
   * then iterating through keys using SstFileSetReader with byte-level comparison.
   * Uses RDBSstFileWriter to directly write changes to SST files and then ingests them.
   */
  @SuppressWarnings("checkstyle:MethodLength")
  private long applyIncrementalChanges(RocksDatabase currentSnapshotDb, DBStore targetStore,
      SnapshotInfo currentSnapshot, SnapshotInfo previousSnapshot) throws IOException {

    LOG.info("Applying incremental changes for snapshot: {} since: {} using delta files approach",
        currentSnapshot.getName(), previousSnapshot.getName());

    long totalChanges = 0;

    // Create temporary directory for SST files
    String parentDir = OMStorage.getOmDbDir(ozoneManager.getConfiguration()).toString();
    String omSnapshotDir = Paths.get(parentDir, OM_SNAPSHOT_DIR).toString();
    String tempSstDir = Paths.get(omSnapshotDir, OzoneConsts.TEMP_DIFF_SST_FILES_DIR).toString();
    String diffDir = Paths.get(omSnapshotDir, CHECKPOINT_STATE_DEFRAGGED_DIR,
        "deltaFilesDiff-" + UUID.randomUUID()).toString();

    try {
      Files.createDirectories(Paths.get(tempSstDir));
      LOG.info("Created temporary SST directory: {}", tempSstDir);
      Files.createDirectories(Paths.get(diffDir));
      LOG.info("Created diff directory: {}", diffDir);

      // Get OmSnapshotManager
      OmSnapshotManager snapshotManager = ozoneManager.getOmSnapshotManager();

      // Get OmSnapshot instances for previous and current snapshots
      try (UncheckedAutoCloseableSupplier<OmSnapshot> previousSnapshotSupplier =
               snapshotManager.getSnapshot(previousSnapshot.getSnapshotId());
           UncheckedAutoCloseableSupplier<OmSnapshot> currentSnapshotSupplier =
               snapshotManager.getSnapshot(currentSnapshot.getSnapshotId())) {

        OmSnapshot previousOmSnapshot = previousSnapshotSupplier.get();
        OmSnapshot currentOmSnapshot = currentSnapshotSupplier.get();

        // Get the SnapshotDiffManager
        SnapshotDiffManager diffManager = snapshotManager.getSnapshotDiffManager();

        // Get column family to key prefix map for filtering SST files
        Map<String, String> tablePrefixes = getColumnFamilyToKeyPrefixMap(
            ozoneManager.getMetadataManager(),
            currentSnapshot.getVolumeName(),
            currentSnapshot.getBucketName());

        // Get table references for target database
        RDBStore targetRdbStore = (RDBStore) targetStore;
        RocksDatabase targetDb = targetRdbStore.getDb();
        if (targetDb == null) {
          throw new IOException("Failed to get RocksDatabase from target store");
        }

        RDBStore previousDefraggedStore = (RDBStore) targetStore; // The previous defragged DB is our target base
        RocksDatabase previousDefraggedDb = previousDefraggedStore.getDb();
        if (previousDefraggedDb == null) {
          throw new IOException("Failed to get RocksDatabase from previous defragmented store");
        }

        // Process each column family
        for (String cfName : COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT) {
          RocksDatabase.ColumnFamily currentCf = currentSnapshotDb.getColumnFamily(cfName);
          RocksDatabase.ColumnFamily previousCf = previousDefraggedDb.getColumnFamily(cfName);
          RocksDatabase.ColumnFamily targetCf = targetDb.getColumnFamily(cfName);

          if (currentCf == null || previousCf == null || targetCf == null) {
            LOG.warn("Column family {} not found in one of the databases, skipping incremental changes", cfName);
            continue;
          }

          Table<byte[], byte[]> targetTable = targetRdbStore.getTable(cfName);
          if (targetTable == null) {
            LOG.warn("Table {} not found in target store, skipping", cfName);
            continue;
          }

          // Get delta files for this column family
          List<String> tablesToLookUp = Collections.singletonList(cfName);
          Set<String> deltaFiles;
          try {
            deltaFiles = diffManager.getDeltaFiles(
                previousOmSnapshot, currentOmSnapshot,
                tablesToLookUp,
                previousSnapshot, currentSnapshot,
                false,  // useFullDiff = false
                tablePrefixes,
                diffDir,
                "defrag-" + currentSnapshot.getSnapshotId()  // jobKey
            );
            LOG.info("Got {} delta SST files for column family: {}", deltaFiles.size(), cfName);
          } catch (Exception e) {
            LOG.error("Failed to get delta files for column family {}: {}", cfName, e.getMessage(), e);
            continue;
          }

          if (deltaFiles.isEmpty()) {
            LOG.info("No delta files for column family {}, skipping", cfName);
            continue;
          }

          AtomicLong cfChanges = new AtomicLong(0);
          String sstFileName = cfName + "_" + currentSnapshot.getSnapshotId() + ".sst";
          File sstFile = new File(tempSstDir, sstFileName);

          LOG.debug("Creating SST file for column family {} changes: {}", cfName, sstFile.getAbsolutePath());

          // Use SstFileSetReader to read keys from delta files
          SstFileSetReader sstFileReader = new SstFileSetReader(deltaFiles);

          try (RDBSstFileWriter sstWriter = new RDBSstFileWriter(sstFile)) {

            // Get key stream with tombstones from delta files
            try (Stream<String> keysToCheck = sstFileReader.getKeyStreamWithTombstone(null, null)) {

              keysToCheck.forEach(keyStr -> {
                try {
                  byte[] key = keyStr.getBytes(StandardCharsets.UTF_8);

                  // Get values from previous defragmented snapshot and current snapshot
                  byte[] previousValue = previousDefraggedDb.get(previousCf, key);
                  byte[] currentValue = currentSnapshotDb.get(currentCf, key);

                  // Byte-level comparison: only write if values are different
                  if (!Arrays.equals(previousValue, currentValue)) {
                    if (currentValue != null) {
                      // Key exists in current snapshot - write the new value
                      sstWriter.put(key, currentValue);
                    } else {
                      // Key doesn't exist in current snapshot (deleted) - write tombstone
                      sstWriter.delete(key);
                    }

                    // Increment change counter
                    cfChanges.getAndIncrement();
                  }
                } catch (Exception e) {
                  LOG.error("Error processing key {} for column family {}: {}",
                      keyStr, cfName, e.getMessage(), e);
                  throw new RuntimeException(e);
                }
              });

            } catch (RocksDBException e) {
              LOG.error("RocksDB error reading keys from delta files for column family {}: {}",
                  cfName, e.getMessage(), e);
            }

            LOG.debug("Finished writing {} changes for column family: {} to SST file",
                cfChanges.get(), cfName);

          } catch (Exception e) {
            LOG.error("Error processing column family {} for snapshot {}: {}",
                cfName, currentSnapshot.getName(), e.getMessage(), e);
          }

          // Ingest SST file into target database if there were changes
          if (sstFile.exists() && sstFile.length() > 0) {
            try {
              targetTable.loadFromFile(sstFile);
              LOG.info("Successfully ingested SST file for column family {}: {} changes",
                  cfName, cfChanges);
            } catch (Exception e) {
              LOG.error("Failed to ingest SST file for column family {}: {}", cfName, e.getMessage(), e);
            }
          } else {
            LOG.debug("No changes to ingest for column family {}", cfName);
          }

          // Clean up SST file after ingestion
          try {
            if (sstFile.exists()) {
              Files.delete(sstFile.toPath());
              LOG.debug("Cleaned up SST file: {}", sstFile.getAbsolutePath());
            }
          } catch (IOException e) {
            LOG.warn("Failed to clean up SST file: {}", sstFile.getAbsolutePath(), e);
          }

          totalChanges += cfChanges.get();
          LOG.debug("Applied {} incremental changes for column family: {}", cfChanges, cfName);
        }
      }

      // Clean up temporary directories
      try {
        Files.deleteIfExists(Paths.get(tempSstDir));
        LOG.debug("Cleaned up temporary SST directory: {}", tempSstDir);
      } catch (IOException e) {
        LOG.warn("Failed to clean up temporary SST directory: {}", tempSstDir, e);
      }

      try {
        org.apache.commons.io.FileUtils.deleteDirectory(new File(diffDir));
        LOG.debug("Cleaned up diff directory: {}", diffDir);
      } catch (IOException e) {
        LOG.warn("Failed to clean up diff directory: {}", diffDir, e);
      }

    } catch (IOException e) {
      throw new IOException("applyIncrementalChanges failed", e);
    }

    LOG.info("Applied {} total incremental changes using delta files approach", totalChanges);
    return totalChanges;
  }

  /**
   * Updates the snapshot metadata in the YAML file after defragmentation.
   * Creates a new version in the metadata with the defragmented SST file list,
   * marks the snapshot as no longer needing defragmentation, and sets the last defrag time.
   */
  private void updateSnapshotMetadataAfterDefrag(RDBStore defraggedStore, SnapshotInfo snapshotInfo)
      throws IOException {
    LOG.info("Updating snapshot metadata for defragmented snapshot: {}", snapshotInfo.getName());

    final OmSnapshotLocalDataManager localDataManager =
        ozoneManager.getOmSnapshotManager().getSnapshotLocalDataManager();
    try (WritableOmSnapshotLocalDataProvider writableProvider =
             localDataManager.getWritableOmSnapshotLocalData(snapshotInfo)) {
      // Add a new version with the defragmented SST file list
      writableProvider.addSnapshotVersion(defraggedStore);
      // Get the snapshot local data to update flags
      OmSnapshotLocalData snapshotLocalData = writableProvider.getSnapshotLocalData();
      snapshotLocalData.setNeedsDefrag(false);
      snapshotLocalData.setSstFiltered(true);
      snapshotLocalData.setLastDefragTime(System.currentTimeMillis());
      writableProvider.commit();

      LOG.info("Successfully updated snapshot metadata for snapshot: {} with new version: {}",
          snapshotInfo.getName(), snapshotLocalData.getVersion());
    } catch (IOException e) {
      LOG.error("Failed to update snapshot metadata for snapshot: {}", snapshotInfo.getName(), e);
      throw e;
    }
  }

  /**
   * Verifies DB integrity by comparing key counts and spot-checking keys/values
   * between the original and defragmented databases.
   */
  private void verifyDbIntegrity(RocksDatabase originalDb, RocksDatabase defraggedDb,
      SnapshotInfo snapshotInfo) {

    LOG.info("Starting DB integrity verification for snapshot: {}", snapshotInfo.getName());

    boolean verificationPassed = true;
    long totalOriginalKeys = 0;
    long totalDefraggedKeys = 0;

    for (String cfName : COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT) {
      LOG.debug("Verifying column family: {} for snapshot: {}",
          cfName, snapshotInfo.getName());

      RocksDatabase.ColumnFamily originalCf = originalDb.getColumnFamily(cfName);
      RocksDatabase.ColumnFamily defraggedCf = defraggedDb.getColumnFamily(cfName);

      if (originalCf == null) {
        LOG.warn("Column family {} not found in original DB, skipping verification", cfName);
        continue;
      }

      if (defraggedCf == null) {
        LOG.error("Column family {} not found in defragmented DB, verification failed", cfName);
        verificationPassed = false;
        continue;
      }

      try {
        // Count keys in original DB
        long originalKeyCount = 0;
        try (ManagedRocksIterator originalIterator = originalDb.newIterator(originalCf)) {
          originalIterator.get().seekToFirst();
          while (originalIterator.get().isValid()) {
            originalKeyCount++;
            originalIterator.get().next();
          }
        }

        // Count keys in defragmented DB
        long defraggedKeyCount = 0;
        try (ManagedRocksIterator defraggedIterator = defraggedDb.newIterator(defraggedCf)) {
          defraggedIterator.get().seekToFirst();
          while (defraggedIterator.get().isValid()) {
            defraggedKeyCount++;
            defraggedIterator.get().next();
          }
        }

        totalOriginalKeys += originalKeyCount;
        totalDefraggedKeys += defraggedKeyCount;

        // Verify key counts match
        if (originalKeyCount != defraggedKeyCount) {
          LOG.error("Key count mismatch for column family {}: original={}, defragmented={}",
              cfName, originalKeyCount, defraggedKeyCount);
          verificationPassed = false;
        } else {
          LOG.info("Key count verification passed for column family {}: {} keys",
              cfName, originalKeyCount);
        }

        // Full verification - check every single key-value pair
        long fullCheckCount = 0;
        long fullCheckErrors = 0;

        try (ManagedRocksIterator originalIterator = originalDb.newIterator(originalCf)) {
          originalIterator.get().seekToFirst();

          while (originalIterator.get().isValid()) {
            byte[] originalKey = originalIterator.get().key();
            byte[] originalValue = originalIterator.get().value();

            // Get the same key from defragmented DB
            byte[] defraggedValue = defraggedDb.get(defraggedCf, originalKey);

            if (defraggedValue == null) {
              LOG.error("Key missing in defragmented DB for column family {}: key #{}",
                  cfName, fullCheckCount);
              verificationPassed = false;
              fullCheckErrors++;
            } else if (!java.util.Arrays.equals(originalValue, defraggedValue)) {
              LOG.error("Value mismatch for column family {}: key #{}",
                  cfName, fullCheckCount);
              verificationPassed = false;
              fullCheckErrors++;
            }

            fullCheckCount++;

            // Log progress every 10,000 keys to avoid log spam
            if (fullCheckCount % 10000 == 0) {
              LOG.debug("Full verification progress for column family {}: checked {} keys, {} errors so far",
                  cfName, fullCheckCount, fullCheckErrors);
            }

            if (fullCheckErrors > 10) {
              LOG.warn("Too many errors found during full verification for column family {}, stopping further checks",
                  cfName);
              break; // Stop if too many errors to avoid flooding logs
            }

            originalIterator.get().next();
          }
        }

        if (fullCheckErrors == 0) {
          LOG.info("Full verification PASSED for column family {}: all {} keys verified successfully",
              cfName, fullCheckCount);
        } else {
          LOG.error("Full verification FAILED for column family {}: {} errors found out of {} keys checked",
              cfName, fullCheckErrors, fullCheckCount);
        }

      } catch (IOException e) {
        LOG.error("Error during verification of column family {} for snapshot {}: {}",
            cfName, snapshotInfo.getName(), e.getMessage(), e);
        verificationPassed = false;
      }
    }

    // Log final verification results
    if (verificationPassed) {
      LOG.info("DB integrity verification PASSED for snapshot: {} " +
              "(total original keys: {}, total defragmented keys: {})",
          snapshotInfo.getName(), totalOriginalKeys, totalDefraggedKeys);
    } else {
      LOG.error("DB integrity verification FAILED for snapshot: {} " +
              "(total original keys: {}, total defragmented keys: {})",
          snapshotInfo.getName(), totalOriginalKeys, totalDefraggedKeys);
      // Consider throwing an exception here if verification failure should halt the process
      // throw new IOException("DB integrity verification failed for snapshot: " + snapshotInfo.getName());
    }
  }

  private final class SnapshotDefragTask implements BackgroundTask {

    @Override
    public BackgroundTaskResult call() throws Exception {
      // Check OM leader and readiness
      if (shouldRun()) {
        final long count = runCount.incrementAndGet();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Initiating Snapshot Defragmentation Task: run # {}", count);
        }
        triggerSnapshotDefragOnce();
      }

      return EmptyTaskResult.newResult();
    }
  }

  public synchronized boolean triggerSnapshotDefragOnce() throws IOException {
    // Check if rocks-tools native lib is available
    if (!isRocksToolsNativeLibAvailable()) {
      LOG.warn("Rocks-tools native library is not available. " +
          "Stopping SnapshotDefragService.");
      return false;
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

    final Table<String, SnapshotInfo> snapshotInfoTable =
        ozoneManager.getMetadataManager().getSnapshotInfoTable();

    // Use iterator(false) to iterate forward through the snapshot chain
    Iterator<UUID> snapshotIterator = snapshotChainManager.iterator(false);

    long snapshotLimit = snapshotLimitPerTask;

    while (snapshotLimit > 0 && running.get() && snapshotIterator.hasNext()) {
      // Get SnapshotInfo for the current snapshot in the chain
      UUID snapshotId = snapshotIterator.next();
      String snapshotTableKey = snapshotChainManager.getTableKey(snapshotId);
      SnapshotInfo snapshotToDefrag = snapshotInfoTable.get(snapshotTableKey);
      if (snapshotToDefrag == null) {
        LOG.warn("Snapshot with ID '{}' not found in snapshot info table", snapshotId);
        continue;
      }

      // Skip deleted snapshots
      if (snapshotToDefrag.getSnapshotStatus() == SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED) {
        LOG.debug("Skipping deleted snapshot: {} (ID: {})",
            snapshotToDefrag.getName(), snapshotToDefrag.getSnapshotId());
        continue;
      }

      // Check if this snapshot needs defragmentation
      if (!needsDefragmentation(snapshotToDefrag)) {
        LOG.debug("Skipping already defragged snapshot: {} (ID: {})",
            snapshotToDefrag.getName(), snapshotToDefrag.getSnapshotId());
        continue;
      }

      LOG.info("Will defrag snapshot: {} (ID: {})",
          snapshotToDefrag.getName(), snapshotToDefrag.getSnapshotId());

      // Acquire MultiSnapshotLocks
      if (!snapshotIdLocks.acquireLock(Collections.singletonList(snapshotToDefrag.getSnapshotId()))
          .isLockAcquired()) {
        LOG.error("Abort. Failed to acquire lock on snapshot: {} (ID: {})",
            snapshotToDefrag.getName(), snapshotToDefrag.getSnapshotId());
        break;
      }

      try {
        LOG.info("Processing snapshot defragmentation for: {} (ID: {})",
            snapshotToDefrag.getName(), snapshotToDefrag.getSnapshotId());

        // Get snapshot through SnapshotCache for proper locking
        try (UncheckedAutoCloseableSupplier<OmSnapshot> snapshotSupplier =
                 snapshotManager.get().getSnapshot(snapshotToDefrag.getSnapshotId())) {

          OmSnapshot omSnapshot = snapshotSupplier.get();

          UUID pathPreviousSnapshotId = snapshotToDefrag.getPathPreviousSnapshotId();
          boolean isFirstSnapshotInPath = pathPreviousSnapshotId == null;
          if (isFirstSnapshotInPath) {
            LOG.info("Performing full defragmentation for first snapshot (in path): {}",
                snapshotToDefrag.getName());
            performFullDefragmentation(snapshotToDefrag, omSnapshot);
          } else {
            final String psIdtableKey = snapshotChainManager.getTableKey(pathPreviousSnapshotId);
            SnapshotInfo previousDefraggedSnapshot = snapshotInfoTable.get(psIdtableKey);

            LOG.info("Performing incremental defragmentation for snapshot: {} " +
                    "based on previous defragmented snapshot: {}",
                snapshotToDefrag.getName(), previousDefraggedSnapshot.getName());

            // If previous path snapshot is not null, it must have been defragmented already
            // Sanity check to ensure previous snapshot exists and is defragmented
            if (needsDefragmentation(previousDefraggedSnapshot)) {
              LOG.error("Fatal error before defragging snapshot: {}. " +
                      "Previous snapshot in path {} was not defragged while it is expected to be.",
                  snapshotToDefrag.getName(), previousDefraggedSnapshot.getName());
              break;
            }

            performIncrementalDefragmentation(
                snapshotToDefrag, previousDefraggedSnapshot, omSnapshot);
          }

          // Close and evict the original snapshot DB from SnapshotCache
          // TODO: Check if there are any SnapshotCache clean up to be done?

          LOG.info("Defragmentation completed for snapshot: {}", snapshotToDefrag.getName());

          snapshotLimit--;
          snapshotsDefraggedCount.getAndIncrement();

        } catch (OMException ome) {
          if (ome.getResult() == OMException.ResultCodes.FILE_NOT_FOUND) {
            LOG.info("Snapshot {} was deleted during defragmentation",
                snapshotToDefrag.getName());
          } else {
            LOG.error("OMException during snapshot defragmentation for: {}",
                snapshotToDefrag.getName(), ome);
            return false;
          }
        }

      } catch (Exception e) {
        LOG.error("Exception during snapshot defragmentation for: {}",
            snapshotToDefrag.getName(), e);
        return false;
      } finally {
        // Release lock MultiSnapshotLocks
        snapshotIdLocks.releaseLock();
        LOG.debug("Released MultiSnapshotLocks on snapshot: {} (ID: {})",
            snapshotToDefrag.getName(), snapshotToDefrag.getSnapshotId());

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
    return running.get() && ozoneManager.isRunning();
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
  }
}

