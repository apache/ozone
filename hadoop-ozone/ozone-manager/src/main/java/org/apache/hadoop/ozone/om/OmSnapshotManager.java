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

package org.apache.hadoop.ozone.om;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.cache.RemovalListener;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.service.SnapshotDiffCleanupService;
import org.apache.hadoop.ozone.om.helpers.SnapshotDiffJob;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.om.snapshot.SnapshotCache;
import org.apache.hadoop.ozone.om.snapshot.SnapshotDiffManager;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.ratis.util.function.CheckedFunction;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.Nonnull;

import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.DEFAULT_COLUMN_FAMILY_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIFF_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_RUN_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_RUN_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_DB_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_REPORT_MAX_PAGE_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_REPORT_MAX_PAGE_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_KEY_NAME;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotDiffManager.getSnapshotRootPath;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.checkSnapshotActive;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.dropColumnFamilyHandle;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.getOzonePathKeyForFso;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;

/**
 * This class is used to manage/create OM snapshots.
 */
public final class OmSnapshotManager implements AutoCloseable {
  public static final String OM_HARDLINK_FILE = "hardLinkFile";
  public static final Logger LOG =
      LoggerFactory.getLogger(OmSnapshotManager.class);

  // Threshold for the table iterator loop in nanoseconds.
  private static final long DB_TABLE_ITER_LOOP_THRESHOLD_NS = 100000;

  private final OzoneManager ozoneManager;
  private final SnapshotDiffManager snapshotDiffManager;
  // Per-OM instance of snapshot cache map
  private final SnapshotCache snapshotCache;
  private final ManagedRocksDB snapshotDiffDb;

  public static final String DELIMITER = "-";

  /**
   * Contains all the snap diff job which are either queued, in_progress or
   * done. This table is used to make sure that there is only single job for
   * requests with the same snapshot pair at any point of time.
   * |------------------------------------------------|
   * |  KEY                         |  VALUE          |
   * |------------------------------------------------|
   * |  fromSnapshotId-toSnapshotId | SnapshotDiffJob |
   * |------------------------------------------------|
   */
  public static final String SNAP_DIFF_JOB_TABLE_NAME =
      "snap-diff-job-table";

  /**
   * Global table to keep the diff report. Each key is prefixed by the jobId
   * to improve look up and clean up. JobId comes from snap-diff-job-table.
   * |--------------------------------|
   * |  KEY         |  VALUE          |
   * |--------------------------------|
   * |  jobId-index | DiffReportEntry |
   * |--------------------------------|
   */
  public static final String SNAP_DIFF_REPORT_TABLE_NAME =
      "snap-diff-report-table";

  /**
   * Contains all the snap diff job which can be purged either due to max
   * allowed time is over, FAILED or REJECTED.
   * |-------------------------------------------|
   * |  KEY     |  VALUE                         |
   * |-------------------------------------------|
   * |  jobId   | numOfTotalEntriesInReportTable |
   * |-------------------------------------------|
   */
  private static final String SNAP_DIFF_PURGED_JOB_TABLE_NAME =
      "snap-diff-purged-job-table";

  private final long diffCleanupServiceInterval;
  private final int maxOpenSstFilesInSnapshotDb;
  private final ManagedColumnFamilyOptions columnFamilyOptions;
  private final ManagedDBOptions options;
  private final List<ColumnFamilyDescriptor> columnFamilyDescriptors;
  private final List<ColumnFamilyHandle> columnFamilyHandles;
  private final SnapshotDiffCleanupService snapshotDiffCleanupService;

  private final int maxPageSize;

  // Soft limit of the snapshot cache size.
  private final int softCacheSize;

  public OmSnapshotManager(OzoneManager ozoneManager) {

    boolean isFilesystemSnapshotEnabled =
        ozoneManager.isFilesystemSnapshotEnabled();
    LOG.info("Ozone filesystem snapshot feature is {}.",
        isFilesystemSnapshotEnabled ? "enabled" : "disabled");

    // Confirm that snapshot feature can be safely disabled.
    // Throw unchecked exception if that is not the case.
    if (!isFilesystemSnapshotEnabled &&
        !canDisableFsSnapshot(ozoneManager.getMetadataManager())) {
      throw new RuntimeException("Ozone Manager is refusing to start up" +
          "because filesystem snapshot feature is disabled in config while" +
          "there are still snapshots remaining in the system (including the " +
          "ones that are marked as deleted but not yet cleaned up by the " +
          "background worker thread). " +
          "Please set config ozone.filesystem.snapshot.enabled to true and " +
          "try to start this Ozone Manager again.");
    }

    this.options = new ManagedDBOptions();
    this.options.setCreateIfMissing(true);
    this.columnFamilyOptions = new ManagedColumnFamilyOptions();
    this.columnFamilyDescriptors = new ArrayList<>();
    this.columnFamilyHandles = new ArrayList<>();
    CodecRegistry codecRegistry = createCodecRegistryForSnapDiff();
    this.maxPageSize = ozoneManager.getConfiguration().getInt(
        OZONE_OM_SNAPSHOT_DIFF_REPORT_MAX_PAGE_SIZE,
        OZONE_OM_SNAPSHOT_DIFF_REPORT_MAX_PAGE_SIZE_DEFAULT
    );
    this.maxOpenSstFilesInSnapshotDb = ozoneManager.getConfiguration().getInt(
        OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES,
        OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES_DEFAULT
    );
    ColumnFamilyHandle snapDiffJobCf;
    ColumnFamilyHandle snapDiffReportCf;
    ColumnFamilyHandle snapDiffPurgedJobCf;
    String dbPath = getDbPath(ozoneManager.getConfiguration());

    try {
      // Add default CF
      columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
          StringUtils.string2Bytes(DEFAULT_COLUMN_FAMILY_NAME),
          new ManagedColumnFamilyOptions(columnFamilyOptions)));

      columnFamilyDescriptors.addAll(getExitingColumnFamilyDescriptors(dbPath));

      this.snapshotDiffDb = createRocksDbForSnapshotDiff(options,
          dbPath, columnFamilyDescriptors, columnFamilyHandles);

      snapDiffJobCf = getOrCreateColumnFamily(SNAP_DIFF_JOB_TABLE_NAME,
          columnFamilyDescriptors, columnFamilyHandles);
      snapDiffReportCf = getOrCreateColumnFamily(SNAP_DIFF_REPORT_TABLE_NAME,
          columnFamilyDescriptors, columnFamilyHandles);
      snapDiffPurgedJobCf = getOrCreateColumnFamily(
          SNAP_DIFF_PURGED_JOB_TABLE_NAME, columnFamilyDescriptors,
          columnFamilyHandles);

      dropUnknownColumnFamilies(columnFamilyHandles);
    } catch (RuntimeException exception) {
      close();
      throw exception;
    }

    this.ozoneManager = ozoneManager;
    RocksDBCheckpointDiffer differ = ozoneManager
        .getMetadataManager()
        .getStore()
        .getRocksDBCheckpointDiffer();

    // Soft-limit of lru cache size
    this.softCacheSize = ozoneManager.getConfiguration().getInt(
        OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE,
        OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE_DEFAULT);

    CacheLoader<String, OmSnapshot> loader = createCacheLoader();

    // TODO: [SNAPSHOT] Remove this if not going to make SnapshotCache impl
    //  pluggable.
    RemovalListener<String, OmSnapshot> removalListener = notification -> {
      try {
        final String snapshotTableKey = notification.getKey();
        final OmSnapshot omSnapshot = notification.getValue();
        if (omSnapshot != null) {
          // close snapshot's rocksdb on eviction
          LOG.debug("Closing OmSnapshot '{}' due to {}",
              snapshotTableKey, notification.getCause());
          omSnapshot.close();
        } else {
          // Cache value would never be null in RemovalListener.

          // When a soft reference is GC'ed by the JVM, this RemovalListener
          // would be called. But the cache value should still exist at that
          // point. Thus in-theory this condition won't be triggered by JVM GC
          throw new IllegalStateException("Unexpected: OmSnapshot is null");
        }
      } catch (IOException e) {
        LOG.error("Failed to close OmSnapshot: {}", notification.getKey(), e);
      }
    };

    // Init snapshot cache
    this.snapshotCache = new SnapshotCache(this, loader, softCacheSize);

    this.snapshotDiffManager = new SnapshotDiffManager(snapshotDiffDb, differ,
        ozoneManager, snapshotCache, snapDiffJobCf, snapDiffReportCf,
        columnFamilyOptions, codecRegistry);

    diffCleanupServiceInterval = ozoneManager.getConfiguration()
        .getTimeDuration(OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_RUN_INTERVAL,
            OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_RUN_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);

    long diffCleanupServiceTimeout = ozoneManager.getConfiguration()
        .getTimeDuration(OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_TIMEOUT,
            OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_TIMEOUT_DEFAULT,
            TimeUnit.MILLISECONDS);

    if (ozoneManager.isFilesystemSnapshotEnabled()) {
      this.snapshotDiffCleanupService = new SnapshotDiffCleanupService(
          diffCleanupServiceInterval,
          diffCleanupServiceTimeout,
          ozoneManager,
          snapshotDiffDb,
          snapDiffJobCf,
          snapDiffPurgedJobCf,
          snapDiffReportCf,
          codecRegistry
      );
      this.snapshotDiffCleanupService.start();
    } else {
      this.snapshotDiffCleanupService = null;
    }
  }

  /**
   * Help reject OM startup if snapshot feature is disabled
   * but there are snapshots remaining in this OM. Note: snapshots that are
   * already deleted but not cleaned up yet still counts.
   * @param ommm OMMetadataManager
   * @return true if SnapshotInfoTable is empty, false otherwise.
   */
  @VisibleForTesting
  public boolean canDisableFsSnapshot(OMMetadataManager ommm) {

    boolean isSnapshotInfoTableEmpty;
    try {
      isSnapshotInfoTableEmpty = ommm.getSnapshotInfoTable().isEmpty();
    } catch (IOException e) {
      throw new IllegalStateException(
          "Unable to check SnapshotInfoTable emptiness", e);
    }

    return isSnapshotInfoTableEmpty;
  }

  private CacheLoader<String, OmSnapshot> createCacheLoader() {
    return new CacheLoader<String, OmSnapshot>() {

      @Nonnull
      @Override
      public OmSnapshot load(@Nonnull String snapshotTableKey)
          throws IOException {
        // Check if the snapshot exists
        final SnapshotInfo snapshotInfo = getSnapshotInfo(snapshotTableKey);

        // Block snapshot from loading when it is no longer active e.g. DELETED,
        // unless this is called from SnapshotDeletingService.
        checkSnapshotActive(snapshotInfo, true);

        CacheValue<SnapshotInfo> cacheValue = ozoneManager.getMetadataManager()
            .getSnapshotInfoTable()
            .getCacheValue(new CacheKey<>(snapshotTableKey));

        boolean isSnapshotInCache = Objects.nonNull(cacheValue) &&
            Objects.nonNull(cacheValue.getCacheValue());

        // read in the snapshot
        OzoneConfiguration conf = ozoneManager.getConfiguration();

        // Create the snapshot metadata manager by finding the corresponding
        // RocksDB instance, creating an OmMetadataManagerImpl instance based
        // on that.
        OMMetadataManager snapshotMetadataManager;
        try {
          snapshotMetadataManager = new OmMetadataManagerImpl(conf,
              snapshotInfo.getCheckpointDirName(), isSnapshotInCache,
              maxOpenSstFilesInSnapshotDb);
        } catch (IOException e) {
          LOG.error("Failed to retrieve snapshot: {}", snapshotTableKey, e);
          throw e;
        }

        try {
          // create the other manager instances based on snapshot
          // metadataManager
          PrefixManagerImpl pm = new PrefixManagerImpl(snapshotMetadataManager,
              false);
          KeyManagerImpl km = new KeyManagerImpl(ozoneManager,
              ozoneManager.getScmClient(), snapshotMetadataManager, conf,
              ozoneManager.getBlockTokenSecretManager(),
              ozoneManager.getKmsProvider(), ozoneManager.getPerfMetrics());

          return new OmSnapshot(km, pm, ozoneManager,
              snapshotInfo.getVolumeName(),
              snapshotInfo.getBucketName(),
              snapshotInfo.getName());
        } catch (Exception e) {
          // Close RocksDB if there is any failure.
          if (!snapshotMetadataManager.getStore().isClosed()) {
            snapshotMetadataManager.getStore().close();
          }
          throw new IOException(e);
        }
      }
    };
  }

  private static CodecRegistry createCodecRegistryForSnapDiff() {
    final CodecRegistry.Builder registry = CodecRegistry.newBuilder();
    // DiffReportEntry codec for Diff Report.
    registry.addCodec(SnapshotDiffReportOzone.DiffReportEntry.class,
        SnapshotDiffReportOzone.getDiffReportEntryCodec());
    registry.addCodec(SnapshotDiffJob.class, SnapshotDiffJob.getCodec());
    return registry.build();
  }

  /**
   * Get snapshot instance LRU cache.
   * @return LoadingCache
   */
  public SnapshotCache getSnapshotCache() {
    return snapshotCache;
  }

  /**
   * Creates snapshot checkpoint that corresponds to snapshotInfo.
   * @param omMetadataManager the metadata manager
   * @param snapshotInfo The metadata of snapshot to be created
   * @return instance of DBCheckpoint
   */
  public static DBCheckpoint createOmSnapshotCheckpoint(
      OMMetadataManager omMetadataManager, SnapshotInfo snapshotInfo)
      throws IOException {
    RDBStore store = (RDBStore) omMetadataManager.getStore();

    final DBCheckpoint dbCheckpoint;

    // Acquire active DB deletedDirectoryTable write lock to block
    // DirDeletingTask
    omMetadataManager.getTableLock(OmMetadataManagerImpl.DELETED_DIR_TABLE)
        .writeLock().lock();
    // Acquire active DB deletedTable write lock to block KeyDeletingTask
    omMetadataManager.getTableLock(OmMetadataManagerImpl.DELETED_TABLE)
        .writeLock().lock();

    boolean snapshotDirExist = false;

    try {
      // Create DB checkpoint for snapshot
      String checkpointPrefix = store.getDbLocation().getName();
      Path snapshotDirPath = Paths.get(store.getSnapshotsParentDir(),
          checkpointPrefix + snapshotInfo.getCheckpointDir());
      if (Files.exists(snapshotDirPath)) {
        snapshotDirExist = true;
        dbCheckpoint = new RocksDBCheckpoint(snapshotDirPath);
      } else {
        dbCheckpoint = store.getSnapshot(snapshotInfo.getCheckpointDirName());
      }

      // Clean up active DB's deletedTable right after checkpoint is taken,
      // with table write lock held
      deleteKeysFromDelKeyTableInSnapshotScope(omMetadataManager,
          snapshotInfo.getVolumeName(), snapshotInfo.getBucketName());
      // Clean up deletedDirectoryTable as well
      deleteKeysFromDelDirTableInSnapshotScope(omMetadataManager,
          snapshotInfo.getVolumeName(), snapshotInfo.getBucketName());
    } finally {
      // Release deletedTable write lock
      omMetadataManager.getTableLock(OmMetadataManagerImpl.DELETED_TABLE)
          .writeLock().unlock();
      // Release deletedDirectoryTable write lock
      omMetadataManager.getTableLock(OmMetadataManagerImpl.DELETED_DIR_TABLE)
          .writeLock().unlock();
    }

    if (dbCheckpoint != null && snapshotDirExist) {
      LOG.info("Checkpoint : {} for snapshot {} already exists.",
          dbCheckpoint.getCheckpointLocation(), snapshotInfo.getName());
      return dbCheckpoint;
    } else if (dbCheckpoint != null) {
      LOG.info("Created checkpoint : {} for snapshot {}",
          dbCheckpoint.getCheckpointLocation(), snapshotInfo.getName());
    }

    return dbCheckpoint;
  }

  /**
   * Helper method to delete DB keys in the snapshot scope (bucket)
   * from active DB's deletedDirectoryTable.
   * @param omMetadataManager OMMetadataManager instance
   * @param volumeName volume name
   * @param bucketName bucket name
   */
  private static void deleteKeysFromDelDirTableInSnapshotScope(
      OMMetadataManager omMetadataManager, String volumeName,
      String bucketName) throws IOException {

    // Range delete start key (inclusive)
    final String keyPrefix = getOzonePathKeyForFso(omMetadataManager,
        volumeName, bucketName);

    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
         iter = omMetadataManager.getDeletedDirTable().iterator(keyPrefix)) {
      performOperationOnKeys(iter,
          entry -> {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Removing key {} from DeletedDirTable", entry.getKey());
            }
            omMetadataManager.getDeletedDirTable().delete(entry.getKey());
            return null;
          });
    }
  }

  @VisibleForTesting
  public SnapshotDiffManager getSnapshotDiffManager() {
    return snapshotDiffManager;
  }

  @VisibleForTesting
  public SnapshotDiffCleanupService getSnapshotDiffCleanupService() {
    return snapshotDiffCleanupService;
  }

  /**
   * Helper method to perform operation on keys with a given iterator.
   * @param keyIter TableIterator
   * @param operationFunction operation to be performed for each key.
   */
  private static void performOperationOnKeys(
      TableIterator<String, ? extends Table.KeyValue<String, ?>> keyIter,
      CheckedFunction<Table.KeyValue<String, ?>,
      Void, IOException> operationFunction) throws IOException {
    // Continue only when there are entries of snapshot (bucket) scope
    // in deletedTable in the first place
    // Loop until prefix matches.
    // Start performance tracking timer
    long startTime = System.nanoTime();
    while (keyIter.hasNext()) {
      Table.KeyValue<String, ?> entry = keyIter.next();
      operationFunction.apply(entry);
    }
    // Time took for the iterator to finish (in ns)
    long timeElapsed = System.nanoTime() - startTime;
    if (timeElapsed >= DB_TABLE_ITER_LOOP_THRESHOLD_NS) {
      // Print time elapsed
      LOG.warn("Took {} ns to find endKey. Caller is {}", timeElapsed,
          new Throwable().fillInStackTrace().getStackTrace()[1]
              .getMethodName());
    }
  }

  /**
   * Helper method to do deleteRange on a table, including endKey.
   * TODO: Do remove this method, it is not used anywhere. Need to check if
   *       deleteRange causes RocksDB corruption.
   * TODO: Move this into {@link Table} ?
   * @param table Table
   * @param beginKey begin key
   * @param endKey end key
   */
  private static void deleteRangeInclusive(
      Table<String, ?> table, String beginKey, String endKey)
      throws IOException {

    if (endKey != null) {
      table.deleteRange(beginKey, endKey);
      // Remove range end key itself
      table.delete(endKey);
    }
  }

  /**
   * Helper method to delete DB keys in the snapshot scope (bucket)
   * from active DB's deletedTable.
   * @param omMetadataManager OMMetadataManager instance
   * @param volumeName volume name
   * @param bucketName bucket name
   */
  private static void deleteKeysFromDelKeyTableInSnapshotScope(
      OMMetadataManager omMetadataManager, String volumeName,
      String bucketName) throws IOException {

    // Range delete start key (inclusive)
    final String keyPrefix =
        omMetadataManager.getOzoneKey(volumeName, bucketName, "");

    try (TableIterator<String,
        ? extends Table.KeyValue<String, RepeatedOmKeyInfo>>
             iter = omMetadataManager.getDeletedTable().iterator(keyPrefix)) {
      performOperationOnKeys(iter, entry -> {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Removing key {} from DeletedTable", entry.getKey());
        }
        omMetadataManager.getDeletedTable().delete(entry.getKey());
        return null;
      });
    }

    // No need to invalidate deletedTable (or deletedDirectoryTable) table
    // cache since entries are not added to its table cache in the first place.
    // See OMKeyDeleteRequest and OMKeyPurgeRequest#validateAndUpdateCache.
    //
    // This makes the table clean up efficient as we only need one
    // deleteRange() operation. No need to invalidate cache entries
    // one by one.
  }

  // Get OmSnapshot if the keyName has ".snapshot" key indicator
  public ReferenceCounted<IOmMetadataReader, SnapshotCache> checkForSnapshot(
      String volumeName,
      String bucketName,
      String keyName,
      boolean skipActiveCheck) throws IOException {
    if (keyName == null || !ozoneManager.isFilesystemSnapshotEnabled()) {
      return ozoneManager.getOmMetadataReader();
    }

    // see if key is for a snapshot
    String[] keyParts = keyName.split(OM_KEY_PREFIX);
    if (isSnapshotKey(keyParts)) {
      String snapshotName = keyParts[1];
      if (snapshotName == null || snapshotName.isEmpty()) {
        // don't allow snapshot indicator without snapshot name
        throw new OMException(INVALID_KEY_NAME);
      }
      String snapshotTableKey = SnapshotInfo.getTableKey(volumeName,
          bucketName, snapshotName);

      // Block FS API reads when snapshot is not active.
      if (!skipActiveCheck) {
        checkSnapshotActive(ozoneManager, snapshotTableKey);
      }

      // Warn if actual cache size exceeds the soft limit already.
      if (snapshotCache.size() > softCacheSize) {
        LOG.warn("Snapshot cache size ({}) exceeds configured soft-limit ({}).",
            snapshotCache.size(), softCacheSize);
      }

      // retrieve the snapshot from the cache
      return snapshotCache.get(snapshotTableKey, skipActiveCheck);
    } else {
      return ozoneManager.getOmMetadataReader();
    }
  }

  /**
   * Returns true if the snapshot is in given status.
   * @param key DB snapshot table key
   * @param status SnapshotStatus
   * @return true if the snapshot is in given status, false otherwise
   */
  public boolean isSnapshotStatus(String key,
                                  SnapshotInfo.SnapshotStatus status)
      throws IOException {
    return getSnapshotInfo(key).getSnapshotStatus().equals(status);
  }

  public SnapshotInfo getSnapshotInfo(String key) throws IOException {
    return SnapshotUtils.getSnapshotInfo(ozoneManager, key);
  }

  public static String getSnapshotPrefix(String snapshotName) {
    return OM_SNAPSHOT_INDICATOR + OM_KEY_PREFIX +
        snapshotName + OM_KEY_PREFIX;
  }

  public static String getSnapshotPath(OzoneConfiguration conf,
                                       SnapshotInfo snapshotInfo) {
    return OMStorage.getOmDbDir(conf) +
        OM_KEY_PREFIX + OM_SNAPSHOT_CHECKPOINT_DIR + OM_KEY_PREFIX +
        OM_DB_NAME + snapshotInfo.getCheckpointDirName();
  }

  public static boolean isSnapshotKey(String[] keyParts) {
    return (keyParts.length > 1) &&
        (keyParts[0].compareTo(OM_SNAPSHOT_INDICATOR) == 0);
  }

  public CancelSnapshotDiffResponse cancelSnapshotDiff(
      final String volume,
      final String bucket,
      final String fromSnapshot,
      final String toSnapshot
  ) throws IOException {
    return snapshotDiffManager.cancelSnapshotDiff(volume, bucket, fromSnapshot,
        toSnapshot);
  }

  @SuppressWarnings("parameternumber")
  public SnapshotDiffResponse getSnapshotDiffReport(final String volume,
                                                    final String bucket,
                                                    final String fromSnapshot,
                                                    final String toSnapshot,
                                                    final String token,
                                                    int pageSize,
                                                    boolean forceFullDiff,
                                                    boolean disableNativeDiff)
      throws IOException {

    validateSnapshotsExistAndActive(volume, bucket, fromSnapshot, toSnapshot);

    // Check if fromSnapshot and toSnapshot are equal.
    if (Objects.equals(fromSnapshot, toSnapshot)) {
      SnapshotDiffReportOzone diffReport = new SnapshotDiffReportOzone(
          getSnapshotRootPath(volume, bucket).toString(), volume, bucket,
          fromSnapshot, toSnapshot, Collections.emptyList(), null);
      return new SnapshotDiffResponse(diffReport, DONE, 0L);
    }

    int index = getIndexFromToken(token);
    if (pageSize <= 0 || pageSize > maxPageSize) {
      pageSize = maxPageSize;
    }

    SnapshotDiffResponse snapshotDiffReport =
        snapshotDiffManager.getSnapshotDiffReport(volume, bucket,
            fromSnapshot, toSnapshot, index, pageSize, forceFullDiff,
            disableNativeDiff);

    // Check again to make sure that from and to snapshots are still active and
    // were not deleted in between response generation.
    // Ideally, snapshot diff and snapshot deletion should take an explicit lock
    // to achieve the synchronization, but it would be complex and expensive.
    // To avoid the complexity, we just check that snapshots are active
    // before returning the response. It is like an optimistic lock to achieve
    // similar behaviour and make sure client gets consistent response.
    validateSnapshotsExistAndActive(volume, bucket, fromSnapshot, toSnapshot);
    return snapshotDiffReport;
  }

  public List<SnapshotDiffJob> getSnapshotDiffList(final String volumeName,
                                                   final String bucketName,
                                                   final String jobStatus,
                                                   final boolean listAll)
      throws IOException {
    String volumeKey = ozoneManager.getMetadataManager()
        .getVolumeKey(volumeName);
    String bucketKey = ozoneManager.getMetadataManager()
        .getBucketKey(volumeName, bucketName);

    if (!ozoneManager.getMetadataManager()
            .getVolumeTable().isExist(volumeKey) ||
        !ozoneManager.getMetadataManager()
            .getBucketTable().isExist(bucketKey)) {
      throw new IOException("Provided volume name " + volumeName +
          " or bucket name " + bucketName + " doesn't exist");
    }
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl)
        ozoneManager.getMetadataManager();
    SnapshotChainManager snapshotChainManager =
        omMetadataManager.getSnapshotChainManager();
    String snapshotPath = volumeName + OM_KEY_PREFIX + bucketName;
    if (snapshotChainManager.getSnapshotChainPath(snapshotPath) == null) {
      // Return an empty ArrayList here to avoid
      // unnecessarily iterating the SnapshotDiffJob table.
      return new ArrayList<>();
    }

    return snapshotDiffManager.getSnapshotDiffJobList(
        volumeName, bucketName, jobStatus, listAll);
  }

  private void validateSnapshotsExistAndActive(final String volumeName,
                                               final String bucketName,
                                               final String fromSnapshotName,
                                               final String toSnapshotName)
      throws IOException {
    SnapshotInfo fromSnapInfo = SnapshotUtils.getSnapshotInfo(ozoneManager,
        volumeName, bucketName, fromSnapshotName);
    SnapshotInfo toSnapInfo = SnapshotUtils.getSnapshotInfo(ozoneManager,
        volumeName, bucketName, toSnapshotName);

    // Block SnapDiff if either of the snapshots is not active.
    checkSnapshotActive(fromSnapInfo, false);
    checkSnapshotActive(toSnapInfo, false);
  }

  private int getIndexFromToken(final String token) throws IOException {
    if (isBlank(token)) {
      return 0;
    }

    // Validate that token passed in the request is valid integer as of now.
    // Later we can change it if we migrate to encrypted or cursor token.
    try {
      int index = Integer.parseInt(token);
      if (index < 0) {
        throw new IOException("Passed token is invalid. Resend the request " +
            "with valid token returned in previous request.");
      }
      return index;
    } catch (NumberFormatException exception) {
      throw new IOException("Passed token is invalid. " +
          "Resend the request with valid token returned in previous request.");
    }
  }

  private ManagedRocksDB createRocksDbForSnapshotDiff(
      final ManagedDBOptions dbOptions, String dbPath,
      final List<ColumnFamilyDescriptor> familyDescriptors,
      final List<ColumnFamilyHandle> familyHandles
  ) {
    try {
      return ManagedRocksDB.open(dbOptions,
          dbPath,
          familyDescriptors,
          familyHandles);
    } catch (RocksDBException exception) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(exception);
    }
  }

  private String getDbPath(final OzoneConfiguration config) {
    File dbDirPath = ServerUtils.getDBPath(config,
        OZONE_OM_SNAPSHOT_DIFF_DB_DIR);
    return Paths.get(dbDirPath.toString(), OM_SNAPSHOT_DIFF_DB_NAME)
        .toFile().getAbsolutePath();
  }

  private List<ColumnFamilyDescriptor> getExitingColumnFamilyDescriptors(
      final String path) {
    try {
      return RocksDatabase.listColumnFamiliesEmptyOptions(path)
          .stream()
          .map(columnFamilyName ->
              new ColumnFamilyDescriptor(columnFamilyName,
                  new ManagedColumnFamilyOptions(columnFamilyOptions)
              ))
          .collect(Collectors.toList());
    } catch (RocksDBException exception) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(exception);
    }
  }

  /**
   * Return the column family from column family list if it was existing
   * column family, otherwise create new column family.
   * This is for backward and forward compatibility.
   * For backward compatibility, when column family doesn't exist. it will
   * create new one and return that.
   * For forward compatibility, it will return the existing one.
   */
  private ColumnFamilyHandle getOrCreateColumnFamily(
      final String columnFamilyName,
      final List<ColumnFamilyDescriptor> familyDescriptors,
      final List<ColumnFamilyHandle> familyHandles) {

    for (int i = 0; i < familyDescriptors.size(); i++) {
      String cfName = StringUtils.bytes2String(familyDescriptors.get(i)
          .getName());
      if (columnFamilyName.equals(cfName)) {
        return familyHandles.get(i);
      }
    }

    try {
      ColumnFamilyDescriptor columnFamilyDescriptor =
          new ColumnFamilyDescriptor(StringUtils.string2Bytes(columnFamilyName),
              columnFamilyOptions);
      ColumnFamilyHandle columnFamily = snapshotDiffDb.get()
          .createColumnFamily(columnFamilyDescriptor);

      // Add column family and descriptor so that they can be closed if needed.
      familyHandles.add(columnFamily);
      familyDescriptors.add(columnFamilyDescriptor);
      return columnFamily;
    } catch (RocksDBException exception) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(exception);
    }
  }

  /**
   * Drops the column families (intermediate snapDiff tables) which were
   * created by previously running snapDiff jobs and were not dropped because
   * OM crashed or some other error.
   */
  private void dropUnknownColumnFamilies(
      List<ColumnFamilyHandle> familyHandles
  ) {
    Set<String> allowedColumnFamilyOnStartUp = new HashSet<>(
        Arrays.asList(DEFAULT_COLUMN_FAMILY_NAME, SNAP_DIFF_JOB_TABLE_NAME,
            SNAP_DIFF_REPORT_TABLE_NAME, SNAP_DIFF_PURGED_JOB_TABLE_NAME));

    try {
      for (ColumnFamilyHandle columnFamilyHandle : familyHandles) {
        String columnFamilyName =
            StringUtils.bytes2String(columnFamilyHandle.getName());
        if (!allowedColumnFamilyOnStartUp.contains(columnFamilyName)) {
          dropColumnFamilyHandle(snapshotDiffDb, columnFamilyHandle);
        }
      }
    } catch (RocksDBException e) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(e);
    }
  }

  private void closeColumnFamilyOptions(
      final ManagedColumnFamilyOptions managedColumnFamilyOptions) {
    Preconditions.checkArgument(!managedColumnFamilyOptions.isReused());
    ManagedColumnFamilyOptions.closeDeeply(managedColumnFamilyOptions);
  }

  @Override
  public void close() {
    if (snapshotDiffManager != null) {
      snapshotDiffManager.close();
    }
    if (snapshotCache != null) {
      snapshotCache.invalidateAll();
    }
    if (snapshotDiffCleanupService != null) {
      snapshotDiffCleanupService.shutdown();
    }

    if (columnFamilyHandles != null) {
      columnFamilyHandles.forEach(ColumnFamilyHandle::close);
    }
    if (snapshotDiffDb != null) {
      snapshotDiffDb.close();
    }
    if (columnFamilyDescriptors != null) {
      columnFamilyDescriptors.forEach(columnFamilyDescriptor ->
          closeColumnFamilyOptions((ManagedColumnFamilyOptions)
              columnFamilyDescriptor.getOptions()));
    }
    if (columnFamilyOptions != null) {
      closeColumnFamilyOptions(columnFamilyOptions);
    }
    if (options != null) {
      options.close();
    }
  }

  public long getDiffCleanupServiceInterval() {
    return diffCleanupServiceInterval;
  }
}
