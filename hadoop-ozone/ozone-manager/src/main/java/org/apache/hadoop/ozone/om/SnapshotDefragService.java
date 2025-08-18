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

import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DEFRAGED_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DEFRAG_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DEFRAG_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.FlatResource.SNAPSHOT_GC_LOCK;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult.EmptyTaskResult;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRawSSTFileReader;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
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

  private static final String CHECKPOINT_STATE_DEFRAGED_DIR = OM_SNAPSHOT_CHECKPOINT_DEFRAGED_DIR;

  private final OzoneManager ozoneManager;
  private final AtomicLong runCount = new AtomicLong(0);

  // Number of snapshots to be processed in a single iteration
  private final long snapshotLimitPerTask;

  private final AtomicLong snapshotsDefraggedCount;
  private final AtomicBoolean running;

  private final BootstrapStateHandler.Lock lock = new BootstrapStateHandler.Lock();

  public SnapshotDefragService(long interval, TimeUnit unit, long serviceTimeout,
      OzoneManager ozoneManager, OzoneConfiguration configuration) {
    super("SnapshotDefragService", interval, unit, DEFRAG_CORE_POOL_SIZE,
        serviceTimeout, ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.snapshotLimitPerTask = configuration
        .getLong(SNAPSHOT_DEFRAG_LIMIT_PER_TASK,
            SNAPSHOT_DEFRAG_LIMIT_PER_TASK_DEFAULT);
    snapshotsDefraggedCount = new AtomicLong(0);
    running = new AtomicBoolean(false);
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
  private boolean needsDefragmentation(SnapshotInfo snapshotInfo) {
    String snapshotPath = OmSnapshotManager.getSnapshotPath(
        ozoneManager.getConfiguration(), snapshotInfo);

    try {
      // Read YAML metadata using the correct API
      File yamlFile = new File(snapshotPath + ".yaml");
      OmSnapshotLocalDataYaml yamlData = OmSnapshotLocalDataYaml.getFromYamlFile(yamlFile);

      // Check if snapshot needs compaction (defragmentation)
      boolean needsDefrag = yamlData.getNeedsDefragmentation();
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
   * Finds the first active snapshot in the chain that needs defragmentation.
   */
  private SnapshotInfo findFirstSnapshotNeedingDefrag(
      Table<String, SnapshotInfo> snapshotInfoTable) throws IOException {

    LOG.debug("Searching for first snapshot needing defragmentation in active chain");

    try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>> iterator =
             snapshotInfoTable.iterator()) {
      iterator.seekToFirst();

      while (iterator.hasNext()) {
        Table.KeyValue<String, SnapshotInfo> keyValue = iterator.next();
        SnapshotInfo snapshotInfo = keyValue.getValue();

        // Skip deleted snapshots
        if (snapshotInfo.getSnapshotStatus() == SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED) {
          LOG.debug("Skipping deleted snapshot: {}", snapshotInfo.getName());
          continue;
        }

        // Check if this snapshot needs defragmentation
        if (needsDefragmentation(snapshotInfo)) {
          LOG.info("Found snapshot needing defragmentation: {} (ID: {})",
              snapshotInfo.getName(), snapshotInfo.getSnapshotId());
          return snapshotInfo;
        }

        LOG.debug("Snapshot {} already defragmented, continuing search",
            snapshotInfo.getName());
      }
    }

    LOG.debug("No snapshots found needing defragmentation");
    return null;
  }

  /**
   * Finds the previous defragmented snapshot in the chain.
   */
  private SnapshotInfo findPreviousDefraggedSnapshot(SnapshotInfo currentSnapshot,
      Table<String, SnapshotInfo> snapshotInfoTable) throws IOException {

    LOG.debug("Searching for previous defragmented snapshot before: {}",
        currentSnapshot.getName());

    // Walk backwards through the snapshot chain using pathPreviousSnapshotId
    String previousSnapshotId = currentSnapshot.getPathPreviousSnapshotId() != null ?
        currentSnapshot.getPathPreviousSnapshotId().toString() : null;

    while (previousSnapshotId != null) {
      try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>> iterator =
               snapshotInfoTable.iterator()) {
        iterator.seekToFirst();

        while (iterator.hasNext()) {
          Table.KeyValue<String, SnapshotInfo> keyValue = iterator.next();
          SnapshotInfo snapshotInfo = keyValue.getValue();

          if (snapshotInfo.getSnapshotId().toString().equals(previousSnapshotId)) {
            if (!needsDefragmentation(snapshotInfo)) {
              LOG.info("Found previous defragmented snapshot: {} (ID: {})",
                  snapshotInfo.getName(), snapshotInfo.getSnapshotId());
              return snapshotInfo;
            }

            // Continue searching with this snapshot's previous
            previousSnapshotId = snapshotInfo.getPathPreviousSnapshotId() != null ?
                snapshotInfo.getPathPreviousSnapshotId().toString() : null;
            break;
          }
        }
      }
    }

    LOG.debug("No previous defragmented snapshot found");
    return null;
  }

  /**
   * Performs full defragmentation for the first snapshot in the chain.
   * This is a simplified implementation that demonstrates the concept.
   */
  private void performFullDefragmentation(SnapshotInfo snapshotInfo,
      OmSnapshot omSnapshot) throws IOException {

    String snapshotPath = OmSnapshotManager.getSnapshotPath(
        ozoneManager.getConfiguration(), snapshotInfo);

    // For defraggedDbPath, we need to go up to the parent directory and use checkpointStateDefragged
    String parentDir = Paths.get(snapshotPath).getParent().getParent().toString();
    String checkpointDirName = Paths.get(snapshotPath).getFileName().toString();
    String defraggedDbPath = Paths.get(parentDir, CHECKPOINT_STATE_DEFRAGED_DIR, checkpointDirName).toString();

    LOG.info("Starting full defragmentation for snapshot: {} at path: {}",
        snapshotInfo.getName(), snapshotPath);
    LOG.info("Target defragmented DB path: {}", defraggedDbPath);

    // Create defragmented directory
    Files.createDirectories(Paths.get(defraggedDbPath));

    // TODO: Get snapshot checkpoint DB via SnapshotCache
    RDBStore originalStore = (RDBStore) omSnapshot.getMetadataManager().getStore();
    RocksDatabase originalDb = originalStore.getDb();

    LOG.info("Starting defragmentation process for snapshot: {}", snapshotInfo.getName());
    LOG.info("Original DB path: {}", snapshotPath);
    LOG.info("Defragmented DB path: {}", defraggedDbPath);

    // Implement actual RocksDB defragmentation
    try {
      // 1. Create a new RocksDB instance at defraggedDbPath
      DBStoreBuilder defraggedDbBuilder = DBStoreBuilder.newBuilder(ozoneManager.getConfiguration())
          .setName(checkpointDirName)
          .setPath(Paths.get(defraggedDbPath).getParent())
          .setCreateCheckpointDirs(false);

      // Add all the tracked column families to the new DB
      for (String cfName : COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT) {
        defraggedDbBuilder.addTable(cfName);
        LOG.debug("Added column family {} to defragmented DB", cfName);
      }

      // Build the new defragmented database
      DBStore defraggedStore = defraggedDbBuilder.build();
      RocksDatabase defraggedDb = ((RDBStore) defraggedStore).getDb();

      LOG.info("Created new defragmented DB instance for snapshot: {}", snapshotInfo.getName());

      // 2. & 3. Iterate through tracked column families and copy all key-value pairs
      long totalKeysCopied = 0;
      for (String cfName : COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT) {
        LOG.info("Starting defragmentation of column family: {} for snapshot: {}",
            cfName, snapshotInfo.getName());

        RocksDatabase.ColumnFamily originalCf = originalDb.getColumnFamily(cfName);
        RocksDatabase.ColumnFamily defraggedCf = defraggedDb.getColumnFamily(cfName);

        if (originalCf == null) {
          LOG.warn("Column family {} not found in original DB, skipping", cfName);
          continue;
        }

        if (defraggedCf == null) {
          LOG.warn("Column family {} not found in defragmented DB, skipping", cfName);
          continue;
        }

        long cfKeysCopied = 0;
        try (ManagedWriteBatch writeBatch = new ManagedWriteBatch();
             ManagedRocksIterator iterator = originalDb.newIterator(originalCf)) {

          iterator.get().seekToFirst();

          while (iterator.get().isValid()) {
            byte[] key = iterator.get().key();
            byte[] value = iterator.get().value();

            // Add to batch for efficient writing
            defraggedCf.batchPut(writeBatch, key, value);
            cfKeysCopied++;

            // Commit batch every 1000 keys to avoid memory issues
            if (cfKeysCopied % 1000 == 0) {
              defraggedDb.batchWrite(writeBatch);
              writeBatch.clear();
              LOG.debug("Copied {} keys for column family {} so far", cfKeysCopied, cfName);
            }

            iterator.get().next();
          }

          // Commit any remaining keys in the batch
          if (writeBatch.count() > 0) {
            defraggedDb.batchWrite(writeBatch);
          }

          totalKeysCopied += cfKeysCopied;
          LOG.info("Completed copying {} keys for column family: {} in snapshot: {}",
              cfKeysCopied, cfName, snapshotInfo.getName());
        }
      }

      LOG.info("Copied total of {} keys across all column families for snapshot: {}",
          totalKeysCopied, snapshotInfo.getName());

      // 4. Perform compaction on the new DB to ensure it's fully defragmented
      LOG.info("Starting compaction of defragmented DB for snapshot: {}", snapshotInfo.getName());
      try (ManagedCompactRangeOptions compactOptions = new ManagedCompactRangeOptions()) {
        compactOptions.setChangeLevel(true);
        compactOptions.setTargetLevel(1);
        defraggedDb.compactDB(compactOptions);
      }
      LOG.info("Completed compaction of defragmented DB for snapshot: {}", snapshotInfo.getName());

      // 5. Verify data integrity between original and defragmented DBs
      verifyDbIntegrity(originalDb, defraggedDb, snapshotInfo);

      // Close the defragmented DB
      defraggedStore.close();

      // TODO: Rename om.db to the om.db-UUID (at least for full defrag)

      LOG.info("Successfully completed full defragmentation for snapshot: {} with {} keys copied",
          snapshotInfo.getName(), totalKeysCopied);

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

    String currentSnapshotPath = OmSnapshotManager.getSnapshotPath(
        ozoneManager.getConfiguration(), currentSnapshot);
    String previousSnapshotPath = OmSnapshotManager.getSnapshotPath(
        ozoneManager.getConfiguration(), previousDefraggedSnapshot);

    // Fix path construction similar to performFullDefragmentation
    String previousParentDir = Paths.get(previousSnapshotPath).getParent().getParent().toString();
    String previousCheckpointDirName = Paths.get(previousSnapshotPath).getFileName().toString();
    String previousDefraggedDbPath = Paths.get(previousParentDir, CHECKPOINT_STATE_DEFRAGED_DIR,
        previousCheckpointDirName).toString();

    String currentParentDir = Paths.get(currentSnapshotPath).getParent().getParent().toString();
    String currentCheckpointDirName = Paths.get(currentSnapshotPath).getFileName().toString();
    String currentDefraggedDbPath = Paths.get(currentParentDir, CHECKPOINT_STATE_DEFRAGED_DIR,
        currentCheckpointDirName).toString();

    LOG.info("Starting incremental defragmentation for snapshot: {} using previous: {}",
        currentSnapshot.getName(), previousDefraggedSnapshot.getName());
    LOG.info("Previous defragmented DB: {}", previousDefraggedDbPath);
    LOG.info("Current target DB: {}", currentDefraggedDbPath);

    // Note: Don't create target directory - RocksDB createCheckpoint() will create it
    // and will fail with "Directory exists" if we create it first

    try {
      // Check if previous defragmented DB exists
      if (!Files.exists(Paths.get(previousDefraggedDbPath))) {
        LOG.warn("Previous defragmented DB not found at {}, falling back to full defragmentation",
            previousDefraggedDbPath);
        performFullDefragmentation(currentSnapshot, currentOmSnapshot);
        return;
      }

      // 1. Create a checkpoint from the previous defragmented DB directly at target location
      LOG.info("Creating checkpoint from previous defragmented DB directly to target location");

      // Open the previous defragmented DB to create checkpoint.
      // TODO: via SnapshotCache or something equivalent for lock protection
      DBStoreBuilder previousDbBuilder = DBStoreBuilder.newBuilder(ozoneManager.getConfiguration())
          .setName(previousCheckpointDirName)
          .setPath(Paths.get(previousDefraggedDbPath).getParent())
          .setOpenReadOnly(true)
          .setCreateCheckpointDirs(false)
          .disableDefaultCFAutoCompaction(true);

      // Add tracked column families
      for (String cfName : COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT) {
        previousDbBuilder.addTable(cfName);
      }

      try (DBStore previousDefraggedStore = previousDbBuilder.build()) {
        RocksDatabase previousDefraggedDb = ((RDBStore) previousDefraggedStore).getDb();

        // Create checkpoint directly at the target location
        try (RocksDatabase.RocksCheckpoint checkpoint = previousDefraggedDb.createCheckpoint()) {
          checkpoint.createCheckpoint(Paths.get(currentDefraggedDbPath));
          LOG.info("Created checkpoint directly at target: {}", currentDefraggedDbPath);
        }
      }

      // 2. Open the checkpoint as our working defragmented DB and apply incremental changes
      DBStoreBuilder currentDbBuilder = DBStoreBuilder.newBuilder(ozoneManager.getConfiguration())
          .setName(currentCheckpointDirName)
          .setPath(Paths.get(currentDefraggedDbPath).getParent())
          .setCreateCheckpointDirs(false)
          // Disable compaction for defragmented snapshot DB
          .disableDefaultCFAutoCompaction(true);

      // Add tracked column families
      for (String cfName : COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT) {
        currentDbBuilder.addTable(cfName);
      }

      // Build DB from the checkpoint
      DBStore currentDefraggedStore = currentDbBuilder.build();
      RocksDatabase currentDefraggedDb = ((RDBStore) currentDefraggedStore).getDb();

      LOG.info("Opened checkpoint as working defragmented DB for incremental update (compaction disabled)");

      // 3. Apply incremental changes from current snapshot
      RDBStore currentSnapshotStore = (RDBStore) currentOmSnapshot.getMetadataManager().getStore();
      RocksDatabase currentSnapshotDb = currentSnapshotStore.getDb();

      long incrementalKeysCopied = applyIncrementalChanges(currentSnapshotDb, currentDefraggedStore,
          currentSnapshot, previousDefraggedSnapshot);

      LOG.info("Applied {} incremental changes for snapshot: {}",
          incrementalKeysCopied, currentSnapshot.getName());

      // 4. Flush each table to exactly one SST file
      LOG.info("Flushing defragged DB for snapshot: {}", currentSnapshot.getName());
      currentDefraggedStore.flushDB();
//      for (String cfName : COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT) {
//        try {
//          Table<byte[], byte[]> table = currentDefraggedStore.getTable(cfName);
//          if (table != null) {
//            ((RDBStore) currentDefraggedStore).flushDB(cfName);
//            LOG.debug("Flushed table {} to SST file", cfName);
//          }
//        } catch (Exception e) {
//          LOG.warn("Failed to flush table {}: {}", cfName, e.getMessage(), e);
//        }
//      }

      // 4. Perform compaction on the updated DB (commented out)
//      LOG.info("Starting compaction of incrementally defragmented DB for snapshot: {}",
//          currentSnapshot.getName());
//      try (ManagedCompactRangeOptions compactOptions = new ManagedCompactRangeOptions()) {
//        compactOptions.setChangeLevel(true);
//        compactOptions.setTargetLevel(1);
//        currentDefraggedDb.compactDB(compactOptions);
//      }
//      LOG.info("Completed compaction of incrementally defragmented DB");

      // 5. Verify data integrity
      verifyDbIntegrity(currentSnapshotDb, currentDefraggedDb, currentSnapshot);

      // Close the defragmented DB. TODO: Close in finally block instead
      currentDefraggedStore.close();

      LOG.info("Successfully completed incremental defragmentation for snapshot: {} with {} incremental changes",
          currentSnapshot.getName(), incrementalKeysCopied);

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
   * Applies incremental changes by using snapshotDiff to compute the diff list,
   * then iterating that diff list against the current snapshot checkpoint DB.
   * Uses direct batch writing to the defragmented DB instead of SST files to avoid sorting requirements.
   */
  @SuppressWarnings("checkstyle:MethodLength")
  private long applyIncrementalChanges(RocksDatabase currentSnapshotDb, DBStore targetStore,
      SnapshotInfo currentSnapshot, SnapshotInfo previousSnapshot) throws RocksDatabaseException {

    LOG.info("Applying incremental changes for snapshot: {} since: {} using direct batch writing",
        currentSnapshot.getName(), previousSnapshot.getName());

    long totalChanges = 0;

    try {
      // Use snapshotDiff to compute the diff list between previous and current snapshot
      LOG.info("Computing snapshot diff between {} and {}",
          previousSnapshot.getName(), currentSnapshot.getName());

      SnapshotDiffResponse diffResponse;
      try {
        // Call snapshotDiff to get the diff list
        diffResponse = ozoneManager.snapshotDiff(
            currentSnapshot.getVolumeName(),
            currentSnapshot.getBucketName(),
            previousSnapshot.getName(),
            currentSnapshot.getName(),
            null, // token - start from beginning
            0,    // pageSize - get all diffs at once
            false, // forceFullDiff
            false  // disableNativeDiff
        );

        // Wait for snapshotDiff computation to complete if it's still in progress
        while (diffResponse.getJobStatus() == SnapshotDiffResponse.JobStatus.IN_PROGRESS ||
            diffResponse.getJobStatus() == SnapshotDiffResponse.JobStatus.QUEUED) {
          LOG.info("Snapshot diff computation in progress, waiting {} ms...",
              diffResponse.getWaitTimeInMs());
          // TODO: This can be improved by triggering snapdiff first, before any locks are grabbed,
          //  so that we don't have to wait here
          Thread.sleep(diffResponse.getWaitTimeInMs());

          // Poll for updated status
          diffResponse = ozoneManager.snapshotDiff(
              currentSnapshot.getVolumeName(),
              currentSnapshot.getBucketName(),
              previousSnapshot.getName(),
              currentSnapshot.getName(),
              null, // token
              0,    // pageSize
              false, // forceFullDiff
              false  // disableNativeDiff
          );
        }

        if (diffResponse.getJobStatus() != SnapshotDiffResponse.JobStatus.DONE) {
          throw new RocksDatabaseException("Snapshot diff computation failed with status: " +
              diffResponse.getJobStatus() + ", reason: " + diffResponse.getReason());
        }

        LOG.info("Snapshot diff computation completed successfully");

      } catch (Exception e) {
        throw new RocksDatabaseException("Failed to compute snapshot diff", e);
      }

      SnapshotDiffReportOzone diffReport = diffResponse.getSnapshotDiffReport();
      if (diffReport == null || diffReport.getDiffList() == null) {
        LOG.info("No differences found between snapshots, no changes to apply");
        return 0;
      }

      // TODO: Handle pagination when diffList is bigger than server page size
      LOG.info("Found {} differences to process", diffReport.getDiffList().size());

      // Get table references for target database
      RDBStore targetRdbStore = (RDBStore) targetStore;
      RocksDatabase targetDb = targetRdbStore.getDb();

      int nextToken = 0;
      while (diffReport.getDiffList() != null && !diffReport.getDiffList().isEmpty()) {
        final List<DiffReportEntry> diffList = diffReport.getDiffList();

        // Group diff entries by column family and process each CF separately
        // TODO: Use bucket layout to determine which column families to process
        for (String cfName : COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT) {
          RocksDatabase.ColumnFamily currentCf = currentSnapshotDb.getColumnFamily(cfName);
          RocksDatabase.ColumnFamily targetCf = targetDb.getColumnFamily(cfName);

          if (currentCf == null || targetCf == null) {
            LOG.warn("Column family {} not found, skipping incremental changes", cfName);
            continue;
          }

          long cfChanges = 0;

          LOG.debug("Processing {} diff entries for column family {}", diffList.size(), cfName);

          // Use batch writing for efficient operations (no sorting required)
          try (ManagedWriteBatch writeBatch = new ManagedWriteBatch()) {

            // Iterate through the diff list and process each entry
            for (DiffReportEntry diffEntry : diffList) {
              String sourcePath = new String(diffEntry.getSourcePath(), StandardCharsets.UTF_8);

              // Extract the key from the path using volume and bucket from snapshot context
              byte[] key = extractKeyFromPath(sourcePath, cfName,
                  currentSnapshot.getVolumeName(), currentSnapshot.getBucketName());
              if (key == null) {
                continue; // Skip if this entry doesn't belong to current column family
              }

              DiffType diffType = diffEntry.getType();

              switch (diffType) {
              case CREATE:
              case MODIFY:
                // Key was created or modified - get current value and add to batch
                byte[] currentValue = currentSnapshotDb.get(currentCf, key);
                if (currentValue != null) {
                  targetCf.batchPut(writeBatch, key, currentValue);
                  cfChanges++;
                }
                break;

              case DELETE:
                // Key was deleted - add deletion to batch
                targetCf.batchDelete(writeBatch, key);
                cfChanges++;
                break;

              case RENAME:
                // Handle rename - delete old key and create new key
                if (diffEntry.getTargetPath() != null) {
                  String targetPath = new String(diffEntry.getTargetPath(), StandardCharsets.UTF_8);
                  byte[] newKey = extractKeyFromPath(targetPath, cfName,
                      currentSnapshot.getVolumeName(), currentSnapshot.getBucketName());

                  if (newKey != null) {
                    // Delete old key
                    targetCf.batchDelete(writeBatch, key);

                    // Add new key with current value
                    byte[] newValue = currentSnapshotDb.get(currentCf, newKey);
                    if (newValue != null) {
                      targetCf.batchPut(writeBatch, newKey, newValue);
                    }
                    cfChanges += 2; // Count both delete and put
                  }
                }
                break;

              default:
                LOG.warn("Unknown diff type: {}, skipping entry", diffType);
                break;
              }

              // Commit batch every 1000 operations to avoid memory issues
              if (cfChanges % 1000 == 0 && writeBatch.count() > 0) {
                targetDb.batchWrite(writeBatch);
                writeBatch.clear();
                LOG.debug("Committed batch for column family {} after {} changes", cfName, cfChanges);
              }
            }

            // Commit any remaining operations in the batch
            if (writeBatch.count() > 0) {
              targetDb.batchWrite(writeBatch);
              LOG.debug("Final batch commit for column family {} with {} total changes", cfName, cfChanges);
            }

          } catch (Exception e) {
            LOG.error("Error processing column family {} for snapshot {}: {}",
                cfName, currentSnapshot.getName(), e.getMessage(), e);
            throw new RocksDatabaseException("Failed to apply incremental changes for column family: " + cfName, e);
          }

          totalChanges += cfChanges;
          LOG.info("Applied {} incremental changes for column family: {}", cfChanges, cfName);
        }

        // Get next page of differences if available
        nextToken += diffList.size();
        LOG.debug("Retrieving next page of snapshot diff with token: {}", nextToken);
        diffResponse = ozoneManager.snapshotDiff(
            currentSnapshot.getVolumeName(),
            currentSnapshot.getBucketName(),
            previousSnapshot.getName(),
            currentSnapshot.getName(),
            String.valueOf(nextToken), // token
            0,    // pageSize
            false, // forceFullDiff
            false  // disableNativeDiff
        );

        if (diffResponse.getJobStatus() != SnapshotDiffResponse.JobStatus.DONE) {
          throw new RocksDatabaseException("Expecting DONE but got unexpected snapshot diff status: " +
              diffResponse.getJobStatus() + ", reason: " + diffResponse.getReason());
        }

        LOG.info("Retrieved next page of snapshot diff, size: {}",
            diffResponse.getSnapshotDiffReport().getDiffList().size());
        diffReport = diffResponse.getSnapshotDiffReport();
      }

    } catch (Exception e) {
      throw new RocksDatabaseException("Failed to apply incremental changes", e);
    }

    LOG.info("Applied {} total incremental changes using direct batch writing", totalChanges);
    return totalChanges;
  }

  /**
   * Extracts the database key from a diff report path for a specific column family.
   * This method converts paths from snapshot diff reports into database keys.
   */
  private byte[] extractKeyFromPath(String path, String columnFamily, String volume, String bucket) {
    try {
      if (KEY_TABLE.equals(columnFamily)) {
        // For keyTable, use OmMetadataManagerImpl#getOzoneKey
        // Path in diff report contains just the key part (after volume/bucket)
        String dbKey = ozoneManager.getMetadataManager().getOzoneKey(volume, bucket, path);
        return dbKey.getBytes(StandardCharsets.UTF_8);
      } else if (FILE_TABLE.equals(columnFamily)) {  // TODO: FSO code path not tested
        // For fileTable, use OmMetadataManagerImpl#getOzoneKeyFSO
        // Path in diff report contains just the key part (after volume/bucket)
        String dbKey = ozoneManager.getMetadataManager().getOzoneKeyFSO(volume, bucket, path);
        return dbKey.getBytes(StandardCharsets.UTF_8);
      }

      // If we can't extract a valid key for this column family, return null
      // This will cause the entry to be skipped for this column family
      return null;

    } catch (Exception e) {
      LOG.warn("Failed to extract key from path: {} for column family: {}, volume: {}, bucket: {}, error: {}",
          path, columnFamily, volume, bucket, e.getMessage(), e);
      return null;
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

      } catch (Exception e) {
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

  /**
   * Updates snapshot metadata to point to the new defragmented DB location.
   */
  private void updateSnapshotMetadata(SnapshotInfo snapshotInfo) throws IOException {
    String snapshotPath = OmSnapshotManager.getSnapshotPath(
        ozoneManager.getConfiguration(), snapshotInfo);

    LOG.info("Updating snapshot metadata for: {} at path: {}",
        snapshotInfo.getName(), snapshotPath);

    try {
      // Read current YAML data using the correct API
      File yamlFile = new File(snapshotPath + ".yaml");
      OmSnapshotLocalDataYaml yamlData = OmSnapshotLocalDataYaml.getFromYamlFile(yamlFile);

      // Mark as defragmented by setting needsCompaction to false
      yamlData.setNeedsDefragmentation(false);

      // Write updated YAML data
      yamlData.writeToYaml(yamlFile);

      LOG.info("Successfully updated metadata for snapshot: {}, " +
              "marked as defragmented (needsCompaction=false)",
          snapshotInfo.getName());

    } catch (IOException e) {
      LOG.error("Failed to update metadata for snapshot: {}", snapshotInfo.getName(), e);
      throw e;
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

    Table<String, SnapshotInfo> snapshotInfoTable =
        ozoneManager.getMetadataManager().getSnapshotInfoTable();

    long snapshotLimit = snapshotLimitPerTask;

    while (snapshotLimit > 0 && running.get()) {
      // Find the first snapshot needing defragmentation
      SnapshotInfo snapshotToDefrag = findFirstSnapshotNeedingDefrag(snapshotInfoTable);

      if (snapshotToDefrag == null) {
        LOG.info("No snapshots found needing defragmentation");
        break;
      }

      // Acquire SNAPSHOT_GC_LOCK
      OMLockDetails gcLockDetails = ozoneManager.getMetadataManager().getLock()
          .acquireWriteLock(SNAPSHOT_GC_LOCK, "defrag-" + snapshotToDefrag.getSnapshotId());

      if (!gcLockDetails.isLockAcquired()) {
        LOG.warn("Failed to acquire SNAPSHOT_GC_LOCK for snapshot: {}",
            snapshotToDefrag.getName());
        break;
      }

      try {
        LOG.info("Processing snapshot defragmentation for: {} (ID: {})",
            snapshotToDefrag.getName(), snapshotToDefrag.getSnapshotId());

        // Get snapshot through SnapshotCache for proper locking
        try (UncheckedAutoCloseableSupplier<OmSnapshot> snapshotSupplier =
                 snapshotManager.get().getActiveSnapshot(
                     snapshotToDefrag.getVolumeName(),
                     snapshotToDefrag.getBucketName(),
                     snapshotToDefrag.getName())) {

          OmSnapshot omSnapshot = snapshotSupplier.get();

          // Check if this is the first snapshot in the chain
          SnapshotInfo previousDefraggedSnapshot = findPreviousDefraggedSnapshot(
              snapshotToDefrag, snapshotInfoTable);

          if (previousDefraggedSnapshot == null) {
            LOG.info("Performing full defragmentation for first snapshot: {}",
                snapshotToDefrag.getName());
            performFullDefragmentation(snapshotToDefrag, omSnapshot);
          } else {
            LOG.info("Performing incremental defragmentation for snapshot: {} " +
                    "based on previous defragmented snapshot: {}",
                snapshotToDefrag.getName(), previousDefraggedSnapshot.getName());
            performIncrementalDefragmentation(snapshotToDefrag,
                previousDefraggedSnapshot, omSnapshot);
          }

          // Update snapshot metadata
          updateSnapshotMetadata(snapshotToDefrag);

          // Close and evict the original snapshot DB from SnapshotCache
          // TODO: Implement proper eviction from SnapshotCache
          LOG.info("Defragmentation completed for snapshot: {}",
              snapshotToDefrag.getName());

          snapshotLimit--;
          snapshotsDefraggedCount.getAndIncrement();

        } catch (OMException ome) {
          if (ome.getResult() == OMException.ResultCodes.FILE_NOT_FOUND) {
            LOG.info("Snapshot {} was deleted during defragmentation",
                snapshotToDefrag.getName());
          } else {
            LOG.error("OMException during snapshot defragmentation for: {}",
                snapshotToDefrag.getName(), ome);
          }
        }

      } catch (Exception e) {
        LOG.error("Exception during snapshot defragmentation for: {}",
            snapshotToDefrag.getName(), e);
        return false;
      } finally {
        // Release SNAPSHOT_GC_LOCK
        ozoneManager.getMetadataManager().getLock()
            .releaseWriteLock(SNAPSHOT_GC_LOCK, "defrag-" + snapshotToDefrag.getSnapshotId());
        LOG.debug("Released SNAPSHOT_GC_LOCK for snapshot: {}", snapshotToDefrag.getName());
      }
    }

    return true;
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
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

