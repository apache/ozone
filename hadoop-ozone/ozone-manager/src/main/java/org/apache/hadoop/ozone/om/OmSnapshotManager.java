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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBStore;
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
import org.apache.hadoop.ozone.om.snapshot.SnapshotDiffJob;
import org.apache.hadoop.ozone.om.snapshot.SnapshotDiffManager;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.DEFAULT_COLUMN_FAMILY_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIFF_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_RUN_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_RUN_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_DB_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_REPORT_MAX_PAGE_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_REPORT_MAX_PAGE_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_KEY_NAME;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.checkSnapshotActive;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.dropColumnFamilyHandle;

/**
 * This class is used to manage/create OM snapshots.
 */
public final class OmSnapshotManager implements AutoCloseable {
  public static final String OM_HARDLINK_FILE = "hardLinkFile";
  private static final Logger LOG =
      LoggerFactory.getLogger(OmSnapshotManager.class);

  // Threshold for the table iterator loop in nanoseconds.
  private static final long DB_TABLE_ITER_LOOP_THRESHOLD_NS = 100000;

  private final OzoneManager ozoneManager;
  private final SnapshotDiffManager snapshotDiffManager;
  private final LoadingCache<String, OmSnapshot> snapshotCache;
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
  private static final String SNAP_DIFF_JOB_TABLE_NAME =
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
  private static final String SNAP_DIFF_REPORT_TABLE_NAME =
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
  private final ManagedColumnFamilyOptions columnFamilyOptions;
  private final ManagedDBOptions options;
  private final List<ColumnFamilyDescriptor> columnFamilyDescriptors;
  private final List<ColumnFamilyHandle> columnFamilyHandles;
  private final SnapshotDiffCleanupService snapshotDiffCleanupService;

  private final int maxPageSize;

  // Soft limit of the snapshot cache size.
  private final int softCacheSize;

  /**
   * TODO: [SNAPSHOT] HDDS-8529: Refactor the constructor in a way that when
   *  ozoneManager.isFilesystemSnapshotEnabled() returns false,
   *  no snapshot-related background job or initialization would run,
   *  except for applying previously committed Ratis transactions in e.g.:
   *  1. {@link OMKeyPurgeRequest#validateAndUpdateCache}
   *  2. {@link OMDirectoriesPurgeRequestWithFSO#validateAndUpdateCache}
   */
  public OmSnapshotManager(OzoneManager ozoneManager) {
    LOG.info("Ozone filesystem snapshot feature is {}.",
        ozoneManager.isFilesystemSnapshotEnabled() ? "enabled" : "disabled");

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

    // snapshot cache size, soft-limit
    this.softCacheSize = ozoneManager.getConfiguration().getInt(
        OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE,
        OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE_DEFAULT);

    CacheLoader<String, OmSnapshot> loader = createCacheLoader();

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

    // init LRU cache
    this.snapshotCache = CacheBuilder.newBuilder()
        // Indicating OmSnapshot instances are softly referenced from the cache.
        // If no thread is holding a strong reference to an OmSnapshot instance
        // (e.g. SnapDiff), the instance could be garbage collected by JVM at
        // its discretion.
        .softValues()
        .removalListener(removalListener)
        .build(loader);

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

  private CacheLoader<String, OmSnapshot> createCacheLoader() {
    return new CacheLoader<String, OmSnapshot>() {

      @Nonnull
      @Override
      public OmSnapshot load(@Nonnull String snapshotTableKey)
          throws IOException {
        // see if the snapshot exists
        SnapshotInfo snapshotInfo = getSnapshotInfo(snapshotTableKey);

        // Block snapshot from loading when it is no longer active
        // e.g. DELETED, unless this is called from SnapshotDeletingService.
        checkSnapshotActive(snapshotInfo);

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
              snapshotInfo.getCheckpointDirName(), isSnapshotInCache);
        } catch (IOException e) {
          LOG.error("Failed to retrieve snapshot: {}", snapshotTableKey);
          throw e;
        }

        try {
          // create the other manager instances based on snapshot
          // metadataManager
          PrefixManagerImpl pm = new PrefixManagerImpl(snapshotMetadataManager,
              false);
          KeyManagerImpl km = new KeyManagerImpl(null,
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

  private CodecRegistry createCodecRegistryForSnapDiff() {
    final CodecRegistry.Builder registry = CodecRegistry.newBuilder();
    // DiffReportEntry codec for Diff Report.
    registry.addCodec(SnapshotDiffReportOzone.DiffReportEntry.class,
        SnapshotDiffReportOzone.getDiffReportEntryCodec());
    registry.addCodec(SnapshotDiffJob.class,
        new SnapshotDiffJob.SnapshotDiffJobCodec());
    return registry.build();
  }

  /**
   * Get snapshot instance LRU cache.
   * @return LoadingCache
   */
  public LoadingCache<String, OmSnapshot> getSnapshotCache() {
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

    try {
      // Create DB checkpoint for snapshot
      dbCheckpoint = store.getSnapshot(snapshotInfo.getCheckpointDirName());
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

    if (dbCheckpoint != null) {
      LOG.info("Created checkpoint : {} for snapshot {}",
          dbCheckpoint.getCheckpointLocation(), snapshotInfo.getName());
    }

    final RocksDBCheckpointDiffer dbCpDiffer =
        store.getRocksDBCheckpointDiffer();

    if (dbCpDiffer != null) {
      final long dbLatestSequenceNumber = snapshotInfo.getDbTxSequenceNumber();

      // Write snapshot generation (latest sequence number) to compaction log.
      // This will be used for DAG reconstruction as snapshotGeneration.
      dbCpDiffer.appendSnapshotInfoToCompactionLog(dbLatestSequenceNumber,
          snapshotInfo.getSnapshotID(),
          snapshotInfo.getCreationTime());

      // Set compaction log filename to the latest DB sequence number
      // right after taking the RocksDB checkpoint for Ozone snapshot.
      //
      // Note it doesn't matter if sequence number hasn't increased (even though
      // it shouldn't happen), since the writer always appends the file.
      dbCpDiffer.setCurrentCompactionLog(dbLatestSequenceNumber);
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
      OMMetadataManager omMetadataManager,
      String volumeName,
      String bucketName) throws IOException {

    // Range delete start key (inclusive)
    final String beginKey = getOzonePathKeyWithVolumeBucketNames(
        omMetadataManager, volumeName, bucketName);
    // Range delete end key (exclusive). To be calculated
    String endKey;

    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
         iter = omMetadataManager.getDeletedDirTable().iterator()) {
      endKey = findEndKeyGivenPrefix(iter, beginKey);
    }

    // Clean up deletedDirectoryTable
    deleteRangeInclusive(omMetadataManager.getDeletedDirTable(),
        beginKey, endKey);
  }

  /**
   * Helper method to generate /volumeId/bucketId/ DB key prefix from given
   * volume name and bucket name as a prefix in FSO deletedDirectoryTable.
   * Follows:
   * {@link OmMetadataManagerImpl#getOzonePathKey(long, long, long, String)}.
   * <p>
   * Note: Currently, this is only intended to be a special use case in
   * {@link OmSnapshotManager}. If this is used elsewhere, consider moving this
   * to {@link OMMetadataManager}.
   *
   * @param volumeName volume name
   * @param bucketName bucket name
   * @return /volumeId/bucketId/
   *    e.g. /-9223372036854772480/-9223372036854771968/
   */
  @VisibleForTesting
  public static String getOzonePathKeyWithVolumeBucketNames(
      OMMetadataManager omMetadataManager,
      String volumeName,
      String bucketName) throws IOException {

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
    return OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX + bucketId + OM_KEY_PREFIX;
  }

  /**
   * Helper method to locate the end key with the given prefix and iterator.
   * @param keyIter TableIterator
   * @param keyPrefix DB key prefix String
   * @return endKey String, or null if no keys with such prefix is found
   */
  private static String findEndKeyGivenPrefix(
      TableIterator<String, ? extends Table.KeyValue<String, ?>> keyIter,
      String keyPrefix) throws IOException {

    String endKey;
    keyIter.seek(keyPrefix);
    // Continue only when there are entries of snapshot (bucket) scope
    // in deletedTable in the first place
    if (!keyIter.hasNext()) {
      // No key matching keyPrefix. No need to do delete or deleteRange at all.
      endKey = null;
    } else {
      // Remember the last key with a matching prefix
      endKey = keyIter.next().getKey();

      // Loop until prefix mismatches.
      // TODO: [SNAPSHOT] Try to seek to next predicted bucket name instead of
      //  the while-loop for a potential speed up?
      // Start performance tracking timer
      long startTime = System.nanoTime();
      while (keyIter.hasNext()) {
        Table.KeyValue<String, ?> entry = keyIter.next();
        String dbKey = entry.getKey();
        if (dbKey.startsWith(keyPrefix)) {
          endKey = dbKey;
        }
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
    return endKey;
  }

  /**
   * Helper method to do deleteRange on a table, including endKey.
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
      OMMetadataManager omMetadataManager,
      String volumeName,
      String bucketName) throws IOException {

    // Range delete start key (inclusive)
    final String beginKey =
        omMetadataManager.getOzoneKey(volumeName, bucketName, "");
    // Range delete end key (exclusive). To be found
    String endKey;

    try (TableIterator<String,
        ? extends Table.KeyValue<String, RepeatedOmKeyInfo>>
             iter = omMetadataManager.getDeletedTable().iterator()) {
      endKey = findEndKeyGivenPrefix(iter, beginKey);
    }

    // Clean up deletedTable
    deleteRangeInclusive(omMetadataManager.getDeletedTable(), beginKey, endKey);

    // No need to invalidate deletedTable (or deletedDirectoryTable) table
    // cache since entries are not added to its table cache in the first place.
    // See OMKeyDeleteRequest and OMKeyPurgeRequest#validateAndUpdateCache.
    //
    // This makes the table clean up efficient as we only need one
    // deleteRange() operation. No need to invalidate cache entries
    // one by one.
  }

  // Get OmSnapshot if the keyname has ".snapshot" key indicator
  public IOmMetadataReader checkForSnapshot(String volumeName,
                                            String bucketName, String keyname)
      throws IOException {
    if (keyname == null || !ozoneManager.isFilesystemSnapshotEnabled()) {
      return ozoneManager.getOmMetadataReader();
    }

    // see if key is for a snapshot
    String[] keyParts = keyname.split("/");
    if (isSnapshotKey(keyParts)) {
      String snapshotName = keyParts[1];
      if (snapshotName == null || snapshotName.isEmpty()) {
        // don't allow snapshot indicator without snapshot name
        throw new OMException(INVALID_KEY_NAME);
      }
      String snapshotTableKey = SnapshotInfo.getTableKey(volumeName,
          bucketName, snapshotName);

      // Block FS API reads when snapshot is not active.
      checkSnapshotActive(ozoneManager, snapshotTableKey);

      // Warn if actual cache size exceeds the soft limit already.
      if (snapshotCache.size() > softCacheSize) {
        LOG.warn("Snapshot cache size ({}) exceeds configured soft-limit ({}).",
            snapshotCache.size(), softCacheSize);
      }

      // retrieve the snapshot from the cache
      try {
        return snapshotCache.get(snapshotTableKey);
      } catch (ExecutionException e) {
        throw new IOException(e.getCause());
      }
    } else {
      return ozoneManager.getOmMetadataReader();
    }
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

  public SnapshotDiffResponse getSnapshotDiffReport(final String volume,
                                                    final String bucket,
                                                    final String fromSnapshot,
                                                    final String toSnapshot,
                                                    final String token,
                                                    int pageSize,
                                                    boolean forceFullDiff)
      throws IOException {

    validateSnapshotsExistAndActive(volume, bucket, fromSnapshot, toSnapshot);

    int index = getIndexFromToken(token);
    if (pageSize <= 0 || pageSize > maxPageSize) {
      pageSize = maxPageSize;
    }

    SnapshotDiffResponse snapshotDiffReport =
        snapshotDiffManager.getSnapshotDiffReport(volume, bucket,
            fromSnapshot, toSnapshot, index, pageSize, forceFullDiff);

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
    checkSnapshotActive(fromSnapInfo);
    checkSnapshotActive(toSnapInfo);

    // Check snapshot creation time
    if (fromSnapInfo.getCreationTime() > toSnapInfo.getCreationTime()) {
      throw new IOException("fromSnapshot:" + fromSnapInfo.getName() +
          " should be older than to toSnapshot:" + toSnapInfo.getName());
    }
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
