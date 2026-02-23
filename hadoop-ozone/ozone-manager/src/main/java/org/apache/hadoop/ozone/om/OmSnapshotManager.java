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

import static org.apache.commons.io.file.PathUtils.deleteDirectory;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.hadoop.hdds.StringUtils.getLexicographicallyHigherString;
import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.DEFAULT_COLUMN_FAMILY_NAME;
import static org.apache.hadoop.hdds.utils.db.RDBCheckpointUtils.POLL_INTERVAL_DURATION;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIFF_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_SEPARATOR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_FS_SNAPSHOT_MAX_LIMIT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_FS_SNAPSHOT_MAX_LIMIT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_CACHE_CLEANUP_SERVICE_RUN_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_CACHE_CLEANUP_SERVICE_RUN_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_COMPACT_NON_SNAPSHOT_DIFF_TABLES;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_COMPACT_NON_SNAPSHOT_DIFF_TABLES_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_RUN_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_RUN_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_DB_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_REPORT_MAX_PAGE_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_REPORT_MAX_PAGE_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_CHECKPOINT_DIR_CREATION_POLL_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_CHECKPOINT_DIR_CREATION_POLL_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_KEY_NAME;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TIMEOUT;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotDiffManager.getSnapshotRootPath;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.checkSnapshotActive;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.dropColumnFamilyHandle;
import static org.apache.hadoop.ozone.om.snapshot.db.SnapshotDiffDBDefinition.SNAP_DIFF_PURGED_JOB_TABLE_NAME;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotDiffJob;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.service.SnapshotDiffCleanupService;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.om.snapshot.SnapshotCache;
import org.apache.hadoop.ozone.om.snapshot.SnapshotDiffManager;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.snapshot.db.SnapshotDiffDBDefinition;
import org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.ListSnapshotDiffJobResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.apache.ratis.util.function.CheckedFunction;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to manage/create OM snapshots.
 */
public final class OmSnapshotManager implements AutoCloseable {
  public static final String OM_HARDLINK_FILE = "hardLinkFile";
  private static final Logger LOG =
      LoggerFactory.getLogger(OmSnapshotManager.class);

  private final OzoneManager ozoneManager;
  private final SnapshotDiffManager snapshotDiffManager;
  // Per-OM instance of snapshot cache map
  private final SnapshotCache snapshotCache;
  private final ManagedRocksDB snapshotDiffDb;

  public static final String DELIMITER = "-";

  /**
   * For snapshot compaction we need to capture SST files following column
   * families before compaction.
   */
  public static final Set<String> COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT =
      RocksDBCheckpointDiffer.COLUMN_FAMILIES_TO_TRACK_IN_DAG;

  private final long diffCleanupServiceInterval;
  private final int maxOpenSstFilesInSnapshotDb;
  private final ManagedColumnFamilyOptions columnFamilyOptions;
  private final ManagedDBOptions options;
  private final List<ColumnFamilyDescriptor> columnFamilyDescriptors;
  private final List<ColumnFamilyHandle> columnFamilyHandles;
  private final SnapshotDiffCleanupService snapshotDiffCleanupService;
  private final OmSnapshotLocalDataManager snapshotLocalDataManager;

  private final int maxPageSize;

  // Soft limit of the snapshot cache size.
  private final int softCacheSize;

  private int fsSnapshotMaxLimit;
  private final AtomicInteger inFlightSnapshotCount = new AtomicInteger(0);

  public OmSnapshotManager(OzoneManager ozoneManager) throws IOException {
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl) ozoneManager.getMetadataManager();
    boolean isFilesystemSnapshotEnabled = ozoneManager.isFilesystemSnapshotEnabled();
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
    CheckedFunction<SnapshotInfo, OmMetadataManagerImpl, IOException> defaultSnapDBProvider = snapshotInfo ->
        getSnapshotOmMetadataManager(snapshotInfo, 0, maxOpenSstFilesInSnapshotDb,
            ozoneManager.getConfiguration());
    this.snapshotLocalDataManager = new OmSnapshotLocalDataManager(ozoneManager.getMetadataManager(),
        omMetadataManager.getSnapshotChainManager(), ozoneManager.getVersionManager(), defaultSnapDBProvider,
        ozoneManager.getConfiguration());
    Preconditions.checkArgument(this.maxOpenSstFilesInSnapshotDb >= -1,
        OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES + " value should be larger than or equal to -1.");

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

      snapDiffJobCf = getOrCreateColumnFamily(SnapshotDiffDBDefinition.SNAP_DIFF_JOB_TABLE_NAME,
          columnFamilyDescriptors, columnFamilyHandles);
      snapDiffReportCf = getOrCreateColumnFamily(SnapshotDiffDBDefinition.SNAP_DIFF_REPORT_TABLE_NAME,
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

    // Soft-limit of lru cache size
    this.softCacheSize = ozoneManager.getConfiguration().getInt(
        OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE,
        OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE_DEFAULT);
      
    fsSnapshotMaxLimit = ozoneManager.getConfiguration().getInt(OZONE_OM_FS_SNAPSHOT_MAX_LIMIT,
        OZONE_OM_FS_SNAPSHOT_MAX_LIMIT_DEFAULT);

    CacheLoader<UUID, OmSnapshot> loader = createCacheLoader();

    // Init snapshot cache
    long cacheCleanupServiceInterval = ozoneManager.getConfiguration()
        .getTimeDuration(OZONE_OM_SNAPSHOT_CACHE_CLEANUP_SERVICE_RUN_INTERVAL,
            OZONE_OM_SNAPSHOT_CACHE_CLEANUP_SERVICE_RUN_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    boolean compactNonSnapshotDiffTables = ozoneManager.getConfiguration()
        .getBoolean(OZONE_OM_SNAPSHOT_COMPACT_NON_SNAPSHOT_DIFF_TABLES,
            OZONE_OM_SNAPSHOT_COMPACT_NON_SNAPSHOT_DIFF_TABLES_DEFAULT);
    this.snapshotCache = new SnapshotCache(loader, softCacheSize, ozoneManager.getMetrics(),
        cacheCleanupServiceInterval, compactNonSnapshotDiffTables, ozoneManager.getMetadataManager().getLock());

    this.snapshotDiffManager = new SnapshotDiffManager(snapshotDiffDb,
        ozoneManager, snapDiffJobCf, snapDiffReportCf,
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

  public static boolean isSnapshotPurged(SnapshotChainManager chainManager, OMMetadataManager omMetadataManager,
      UUID snapshotId, TransactionInfo transactionInfo) throws IOException {
    boolean purgeFlushed = transactionInfo != null &&
        isTransactionFlushedToDisk(omMetadataManager, transactionInfo);
    String tableKey = chainManager.getTableKey(snapshotId);
    if (tableKey == null) {
      // Snapshot chain is rebuilt from DB on every OM restart (loadFromSnapshotInfoTable),
      // but entries committed to the Raft log (but not yet flushed) after the restart
      // are applied in addToDBBatch (skipping validateAndUpdateCache), so they never call
      // addSnapshot(). This can affect any OM (leader or follower) after a restart.
      //
      // Need to fall back to transactionInfo. null means no purge has been recorded. Treat as active.
      LOG.debug("snapshotId {} has null tableKey in SnapshotChainManager. "
              + "transactionInfo={} purgeFlushed={}. Returning {}",
          snapshotId, transactionInfo, purgeFlushed, purgeFlushed);
      return purgeFlushed;
    }
    boolean inDB = omMetadataManager.getSnapshotInfoTable().isExist(tableKey);
    return !inDB && purgeFlushed;
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

  private static OmMetadataManagerImpl getSnapshotOmMetadataManager(SnapshotInfo snapshotInfo, int version,
      int maxOpenSstFilesInSnapshotDb, OzoneConfiguration conf) throws IOException {
    return new OmMetadataManagerImpl(conf, snapshotInfo.getCheckpointDirName(version),
        maxOpenSstFilesInSnapshotDb);
  }

  private CacheLoader<UUID, OmSnapshot> createCacheLoader() {
    return new CacheLoader<UUID, OmSnapshot>() {

      @Nonnull
      @Override
      public OmSnapshot load(@Nonnull UUID snapshotId) throws IOException {
        String snapshotTableKey = ((OmMetadataManagerImpl) ozoneManager.getMetadataManager())
            .getSnapshotChainManager()
            .getTableKey(snapshotId);

        // SnapshotChain maintains in-memory reverse mapping of snapshotId to snapshotName based on snapshotInfoTable.
        // So it should not happen ideally.
        // If it happens, then either snapshot has been purged in between or SnapshotChain is corrupted
        // and missing some entries which needs investigation.
        if (snapshotTableKey == null) {
          throw new OMException("Snapshot " + snapshotId +
              " is not found in the snapshot chain.", FILE_NOT_FOUND);
        }

        final SnapshotInfo snapshotInfo = getSnapshotInfo(snapshotTableKey);

        // read in the snapshot
        OzoneConfiguration conf = ozoneManager.getConfiguration();

        // Create the snapshot metadata manager by finding the corresponding
        // RocksDB instance, creating an OmMetadataManagerImpl instance based
        // on that.
        OMMetadataManager snapshotMetadataManager;
        try {
          // The check is only to prevent every snapshot read to perform a disk IO
          // and check if a checkpoint dir exists. If entry is present in cache,
          // it is most likely DB entries will get flushed in this wait time.
          Duration maxPollDuration =
              Duration.ofMillis(conf.getTimeDuration(
                  OZONE_SNAPSHOT_CHECKPOINT_DIR_CREATION_POLL_TIMEOUT,
                  OZONE_SNAPSHOT_CHECKPOINT_DIR_CREATION_POLL_TIMEOUT_DEFAULT,
                  TimeUnit.MILLISECONDS));
          boolean flushed = RatisHelper.attemptUntilTrue(() -> {
            try {
              return OmSnapshotManager.isSnapshotFlushedToDB(ozoneManager.getMetadataManager(), snapshotInfo);
            } catch (IOException e) {
              return false;
            }
          }, POLL_INTERVAL_DURATION, maxPollDuration);
          if (!flushed) {
            throw new OMException("Unable to load snapshot. " +
                "Create Snapshot Txn '" + snapshotInfo.getTableKey() +
                "' with txnId : '" + TransactionInfo.fromByteString(snapshotInfo.getCreateTransactionInfo()) +
                "' has not been flushed yet. Please wait a few more seconds before retrying", TIMEOUT);
          }
          try (OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataMetaProvider snapshotLocalDataProvider =
                   snapshotLocalDataManager.getOmSnapshotLocalDataMeta(snapshotInfo)) {
            snapshotMetadataManager = getSnapshotOmMetadataManager(snapshotInfo,
                snapshotLocalDataProvider.getMeta().getVersion(), maxOpenSstFilesInSnapshotDb, conf);
          }
        } catch (IOException e) {
          LOG.error("Failed to retrieve snapshot: {}", snapshotTableKey, e);
          throw e;
        }

        try {
          // create the other manager instances based on snapshot
          // metadataManager
          PrefixManagerImpl pm = new PrefixManagerImpl(ozoneManager, snapshotMetadataManager,
              false);
          KeyManagerImpl km = new KeyManagerImpl(ozoneManager,
              ozoneManager.getScmClient(), snapshotMetadataManager, conf,
              ozoneManager.getBlockTokenSecretManager(),
              ozoneManager.getKmsProvider(), ozoneManager.getPerfMetrics());

          return new OmSnapshot(km, pm, ozoneManager,
              snapshotInfo.getVolumeName(),
              snapshotInfo.getBucketName(),
              snapshotInfo.getName(),
              snapshotInfo.getSnapshotId());
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

  public int getMaxPageSize() {
    return maxPageSize;
  }

  /**
   * Get snapshot instance LRU cache size.
   * @return cache size.
   */
  @VisibleForTesting
  public int getSnapshotCacheSize() {
    return snapshotCache == null ? 0 : snapshotCache.size();
  }

  /**
   * Get snapshot cache instance.
   * @return snapshotCache.
   */
  public SnapshotCache getSnapshotCache() {
    return snapshotCache;
  }

  /**
   * Immediately invalidate all entries and close their DB instances in cache.
   */
  public void invalidateCache() {
    if (snapshotCache != null) {
      snapshotCache.invalidateAll();
    }
  }

  /**
   * Immediately invalidate an entry.
   *
   * @param key SnapshotId.
   */
  public void invalidateCacheEntry(UUID key) {
    if (snapshotCache != null) {
      snapshotCache.invalidate(key);
    }
  }

  /**
   * Deletes the snapshot checkpoint directories for a given snapshot ID up to
   * the specified maximum version. This method ensures that all directories
   * containing checkpoint data for the specified snapshot ID up to the max
   * version are removed in a controlled manner.
   *
   * @param snapshotId The unique identifier of the snapshot whose checkpoint
   *                   directories are to be deleted.
   * @param maxVersion The maximum version of checkpoint directories to delete.
   *                   If a value less than 0 is provided, it defaults to the
   *                   current maximum version of the snapshot.
   * @throws IOException If there is a failure acquiring the snapshot database
   *                     lock or while deleting directories.
   * @throws IllegalArgumentException If the specified maxVersion is greater
   *                                  than the current maximum version of the
   *                                  snapshot.
   */
  public void deleteSnapshotCheckpointDirectories(UUID snapshotId, int maxVersion) throws IOException {
    // Acquire Snapshot DBHandle lock before removing the older version to ensure all readers are done with the
    // snapshot db use.
    try (UncheckedAutoCloseableSupplier<OMLockDetails> lock = getSnapshotCache().lock(snapshotId)) {
      if (!lock.get().isLockAcquired()) {
        throw new IOException("Failed to acquire dbHandlelock on snapshot: " + snapshotId);
      }
      try (OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataMetaProvider snapMetaProvider =
               snapshotLocalDataManager.getOmSnapshotLocalDataMeta(snapshotId)) {
        if (maxVersion < 0) {
          maxVersion = snapMetaProvider.getMeta().getVersion();
        }
        if (maxVersion > snapMetaProvider.getMeta().getVersion()) {
          throw new IllegalArgumentException(
              String.format("Max Version to be deleted can never be greater than the existing " +
                  "version of the snapshot. Argument passed : %d and snapshotMaxVersion : %d", maxVersion,
                  snapMetaProvider.getMeta().getVersion()));
        }
        // Binary search the smallest existing version and delete the older versions starting from the smallest version.
        // This is to ensure efficient crash recovery.
        int smallestExistingVersion = 0;
        int largestExistingVersion = maxVersion;
        while (smallestExistingVersion <= largestExistingVersion) {
          int midVersion = smallestExistingVersion + (largestExistingVersion - smallestExistingVersion) / 2;
          Path path = OmSnapshotManager.getSnapshotPath(ozoneManager.getMetadataManager(), snapshotId, midVersion);
          if (path.toFile().exists()) {
            largestExistingVersion = midVersion - 1;
          } else {
            smallestExistingVersion = midVersion + 1;
          }
        }
        // Delete the older version directories. Always starting deletes from smallest version to largest version to
        // ensure binary search works correctly on a later basis.
        for (int version = smallestExistingVersion; version <= maxVersion; version++) {
          Path path = OmSnapshotManager.getSnapshotPath(ozoneManager.getMetadataManager(), snapshotId, version);
          LOG.info("Deleting snapshot checkpoint directory for snapshot {} version {} at path: {}", snapshotId,
              version, path);
          deleteDirectory(path);
        }
      }
    }
  }

  /**
   * Creates snapshot checkpoint that corresponds to snapshotInfo.
   * @param omMetadataManager the metadata manager
   * @param snapshotInfo The metadata of snapshot to be created
   * @return instance of DBCheckpoint
   */
  public static DBCheckpoint createOmSnapshotCheckpoint(
      OMMetadataManager omMetadataManager, SnapshotInfo snapshotInfo, BatchOperation batchOperation)
      throws IOException {
    RDBStore store = (RDBStore) omMetadataManager.getStore();

    final DBCheckpoint dbCheckpoint;

    boolean snapshotDirExist = false;
    // Create DB checkpoint for snapshot
    Path snapshotDirPath = getSnapshotPath(omMetadataManager, snapshotInfo, 0);
    if (Files.exists(snapshotDirPath)) {
      snapshotDirExist = true;
      dbCheckpoint = new RocksDBCheckpoint(snapshotDirPath);
    } else {
      dbCheckpoint = store.getSnapshot(snapshotInfo.getCheckpointDirName(0));
    }
    OmSnapshotManager omSnapshotManager =
        ((OmMetadataManagerImpl) omMetadataManager).getOzoneManager().getOmSnapshotManager();
    OmSnapshotLocalDataManager snapshotLocalDataManager = omSnapshotManager.getSnapshotLocalDataManager();
    OzoneConfiguration configuration = ((OmMetadataManagerImpl) omMetadataManager).getOzoneManager().getConfiguration();
    try (OmMetadataManagerImpl checkpointMetadataManager =
             OmMetadataManagerImpl.createCheckpointMetadataManager(configuration, dbCheckpoint)) {
      // Create the snapshot local property file.
      snapshotLocalDataManager.createNewOmSnapshotLocalDataFile(
          (RDBStore) checkpointMetadataManager.getStore(), snapshotInfo);
    }

    // Clean up active DB's deletedTable right after checkpoint is taken,
    // Snapshot create is processed as a single transaction and
    // transactions are flushed sequentially so, no need to take any lock as of now.
    for (Table<String, ?> table : ImmutableList.of(omMetadataManager.getDeletedTable(),
        omMetadataManager.getDeletedDirTable(), omMetadataManager.getSnapshotRenamedTable())) {
      deleteKeysFromTableWithBucketPrefix(omMetadataManager, table,
          snapshotInfo.getVolumeName(), snapshotInfo.getBucketName(), batchOperation);
    }

    if (snapshotDirExist) {
      LOG.info("Checkpoint: {} for snapshot {} already exists.",
          dbCheckpoint.getCheckpointLocation(), snapshotInfo.getTableKey());
      return dbCheckpoint;
    } else {
      LOG.info("Created checkpoint: {} for snapshot {}",
          dbCheckpoint.getCheckpointLocation(),
          snapshotInfo.getTableKey());
    }

    return dbCheckpoint;
  }

  /**
   * Helper method to perform batch delete range operation on a given key prefix.
   * @param metadataManager metadatManager instance
   * @param table table from which keys are to be deleted
   * @param volume volume corresponding to the bucket
   * @param bucket bucket corresponding to which keys need to be deleted from the table
   * @param batchOperation batch operation
   */
  private static void deleteKeysFromTableWithBucketPrefix(OMMetadataManager metadataManager,
      Table<String, ?> table, String volume, String bucket, BatchOperation batchOperation) throws IOException {
    String prefix = metadataManager.getTableBucketPrefix(table.getName(), volume, bucket);
    String endKey = getLexicographicallyHigherString(prefix);
    LOG.debug("Deleting key range from {} - startKey: {}, endKey: {}",
        table.getName(), prefix, endKey);
    try (TableIterator<String, String> itr = table.keyIterator(prefix)) {
      while (itr.hasNext()) {
        table.deleteWithBatch(batchOperation, itr.next());
      }
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

  // Get OmSnapshot if the keyName has ".snapshot" key indicator
  @SuppressWarnings("unchecked")
  public UncheckedAutoCloseableSupplier<IOmMetadataReader> getActiveFsMetadataOrSnapshot(
      String volumeName,
      String bucketName,
      String keyName) throws IOException {
    if (keyName == null || !ozoneManager.isFilesystemSnapshotEnabled()) {
      return ozoneManager.getOmMetadataReader();
    }

    // see if key is for a snapshot
    String[] keyParts = keyName.split(OM_KEY_PREFIX);
    if (isSnapshotKey(keyParts)) {
      String snapshotName = keyParts[1];
      // Updating the volumeName & bucketName in case the bucket is a linked bucket. We need to do this before a
      // permission check, since linked bucket permissions and source bucket permissions could be different.
      ResolvedBucket resolvedBucket = ozoneManager.resolveBucketLink(Pair.of(volumeName,
          bucketName), false, false);
      volumeName = resolvedBucket.realVolume();
      bucketName = resolvedBucket.realBucket();
      return (UncheckedAutoCloseableSupplier<IOmMetadataReader>) (UncheckedAutoCloseableSupplier<?>)
          getActiveSnapshot(volumeName, bucketName, snapshotName);
    } else {
      return ozoneManager.getOmMetadataReader();
    }
  }

  public UncheckedAutoCloseableSupplier<OmSnapshot> getActiveSnapshot(
      String volumeName,
      String bucketName,
      String snapshotName) throws IOException {
    return getSnapshot(volumeName, bucketName, snapshotName, false);
  }

  public UncheckedAutoCloseableSupplier<OmSnapshot> getSnapshot(
      String volumeName,
      String bucketName,
      String snapshotName) throws IOException {
    return getSnapshot(volumeName, bucketName, snapshotName, true);
  }

  public OmSnapshotLocalDataManager getSnapshotLocalDataManager() {
    return snapshotLocalDataManager;
  }

  private UncheckedAutoCloseableSupplier<OmSnapshot> getSnapshot(
      String volumeName,
      String bucketName,
      String snapshotName,
      boolean skipActiveCheck) throws IOException {

    if (snapshotName == null || snapshotName.isEmpty()) {
      // don't allow snapshot indicator without snapshot name
      throw new OMException(INVALID_KEY_NAME);
    }
    String snapshotTableKey = SnapshotInfo.getTableKey(volumeName,
        bucketName, snapshotName);

    return getSnapshot(snapshotTableKey, skipActiveCheck);
  }

  private UncheckedAutoCloseableSupplier<OmSnapshot> getSnapshot(String snapshotTableKey, boolean skipActiveCheck)
      throws IOException {
    SnapshotInfo snapshotInfo = SnapshotUtils.getSnapshotInfo(ozoneManager, snapshotTableKey);
    // Block FS API reads when snapshot is not active.
    if (!skipActiveCheck) {
      checkSnapshotActive(snapshotInfo, false);
    }

    // retrieve the snapshot from the cache
    return snapshotCache.get(snapshotInfo.getSnapshotId());
  }

  /**
   * Checks if the last transaction performed on the snapshot has been flushed to disk.
   * @param metadataManager Metadatamanager of Active OM.
   * @param snapshotTableKey table key corresponding to snapshot in snapshotInfoTable.
   * @return True if the changes have been flushed to DB otherwise false
   * @throws IOException
   */
  public static boolean areSnapshotChangesFlushedToDB(OMMetadataManager metadataManager, String snapshotTableKey)
      throws IOException {
    // Need this info from cache since the snapshot could have been updated only on cache and not on disk.
    SnapshotInfo snapshotInfo = metadataManager.getSnapshotInfoTable().get(snapshotTableKey);
    return areSnapshotChangesFlushedToDB(metadataManager, snapshotInfo);
  }

  /**
   * Checks if the last transaction performed on the snapshot has been flushed to disk.
   * @param metadataManager Metadatamanager of Active OM.
   * @param snapshotInfo SnapshotInfo value.
   * @return True if the changes have been flushed to DB otherwise false. It would return true if the snapshot
   * provided is null meaning the snapshot doesn't exist.
   * @throws IOException
   */
  public static boolean areSnapshotChangesFlushedToDB(OMMetadataManager metadataManager, SnapshotInfo snapshotInfo)
      throws IOException {
    if (snapshotInfo != null) {
      TransactionInfo snapshotTransactionInfo = snapshotInfo.getLastTransactionInfo() != null ?
          TransactionInfo.fromByteString(snapshotInfo.getLastTransactionInfo()) : null;
      return isTransactionFlushedToDisk(metadataManager, snapshotTransactionInfo);
    }
    return true;
  }

  public static boolean isSnapshotFlushedToDB(OMMetadataManager metadataManager, SnapshotInfo snapshotInfo)
      throws IOException {
    if (snapshotInfo != null) {
      TransactionInfo snapshotTransactionInfo = snapshotInfo.getCreateTransactionInfo() != null ?
          TransactionInfo.fromByteString(snapshotInfo.getCreateTransactionInfo()) : null;
      return isTransactionFlushedToDisk(metadataManager, snapshotTransactionInfo);
    }
    return true;
  }

  private static boolean isTransactionFlushedToDisk(OMMetadataManager metadataManager,
      TransactionInfo txnInfo) throws IOException {
    TransactionInfo omTransactionInfo = TransactionInfo.readTransactionInfo(metadataManager);
    // If transactionInfo field is null then return true to keep things backward compatible.
    return txnInfo == null || omTransactionInfo.compareTo(txnInfo) >= 0;
  }

  /**
   * Returns OmSnapshot object and skips active check.
   * This should only be used for API calls initiated by background service e.g. purgeKeys, purgeSnapshot,
   * snapshotMoveDeletedKeys, and SetSnapshotProperty.
   */
  public UncheckedAutoCloseableSupplier<OmSnapshot> getSnapshot(UUID snapshotId) throws IOException {
    return snapshotCache.get(snapshotId);
  }

  /**
   * Returns snapshotInfo from cache if it is present in cache, otherwise it checks RocksDB and return value from there.
   * #################################################
   * NOTE: THIS SHOULD BE USED BY SNAPSHOT CACHE ONLY.
   * #################################################
   * Sometimes, the follower OM node may be lagging that it gets purgeKeys or snapshotMoveDeletedKeys from a Snapshot,
   * and purgeSnapshot for the same Snapshot one after another. And purgeSnapshot's validateAndUpdateCache gets
   * executed before doubleBuffer flushes purgeKeys or snapshotMoveDeletedKeys from that Snapshot.
   * This should not be a case on the leader node because SnapshotDeletingService checks that deletedTable and
   * deletedDirectoryTable in DB don't have entries for the bucket before it sends a purgeSnapshot on a snapshot.
   * If that happens, and we just look into the cache, the addToBatch operation will fail when it tries to open
   * the DB and purgeKeys from the Snapshot because snapshot is already purged from the SnapshotInfoTable cache.
   * Hence, it is needed to look into the table to make sure that snapshot exists somewhere in cache or in DB.
   */
  private SnapshotInfo getSnapshotInfo(String snapshotKey) throws IOException {
    SnapshotInfo snapshotInfo = ozoneManager.getMetadataManager().getSnapshotInfoTable().get(snapshotKey);

    if (snapshotInfo == null) {
      snapshotInfo = ozoneManager.getMetadataManager().getSnapshotInfoTable().getSkipCache(snapshotKey);
    }
    if (snapshotInfo == null) {
      throw new OMException("Snapshot '" + snapshotKey + "' is not found.", FILE_NOT_FOUND);
    }
    return snapshotInfo;
  }

  public static String getSnapshotPrefix(String snapshotName) {
    return OM_SNAPSHOT_INDICATOR + OM_KEY_PREFIX +
        snapshotName + OM_KEY_PREFIX;
  }

  public static Path getSnapshotPath(OMMetadataManager omMetadataManager, SnapshotInfo snapshotInfo, int version) {
    return getSnapshotPath(omMetadataManager, snapshotInfo.getSnapshotId(), version);
  }

  public static Path getSnapshotPath(OMMetadataManager omMetadataManager, UUID snapshotId, int version) {
    RDBStore store = (RDBStore) omMetadataManager.getStore();
    String checkpointPrefix = store.getDbLocation().getName();
    return Paths.get(store.getSnapshotsParentDir(),
        checkpointPrefix + SnapshotInfo.getCheckpointDirName(snapshotId, version));
  }

  public static String getSnapshotPath(OzoneConfiguration conf,
      SnapshotInfo snapshotInfo, int version) {
    return getSnapshotPath(conf, snapshotInfo.getCheckpointDirName(version));
  }

  private static String getSnapshotPath(OzoneConfiguration conf, String checkpointDirName) {
    return OMStorage.getOmDbDir(conf) +
        OM_KEY_PREFIX + OM_SNAPSHOT_CHECKPOINT_DIR + OM_KEY_PREFIX +
        OM_DB_NAME + checkpointDirName;
  }

  public static String extractSnapshotIDFromCheckpointDirName(String snapshotPath) {
    // Find "om.db-" in the path and return whatever comes after
    int index = snapshotPath.lastIndexOf(OM_DB_NAME);
    if (index == -1 || index + OM_DB_NAME.length() + OM_SNAPSHOT_SEPARATOR.length() >= snapshotPath.length()) {
      throw new IllegalArgumentException("Invalid snapshot path " + snapshotPath);
    }
    return snapshotPath.substring(index + OM_DB_NAME.length() + OM_SNAPSHOT_SEPARATOR.length());
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

  public ListSnapshotDiffJobResponse getSnapshotDiffList(
      final String volumeName,
      final String bucketName,
      final String jobStatus,
      final boolean listAllStatus,
      final String prevSnapshotDiffJob,
      int maxListResult) throws IOException {
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
      return new ListSnapshotDiffJobResponse(Collections.emptyList(), null);
    }

    if (maxListResult <= 0 || maxListResult > maxPageSize) {
      maxListResult = maxPageSize;
    }

    return snapshotDiffManager.getSnapshotDiffJobList(volumeName, bucketName, jobStatus, listAllStatus,
        prevSnapshotDiffJob, maxListResult);
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

  public void snapshotLimitCheck() throws IOException, OMException {
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl) ozoneManager.getMetadataManager();
    SnapshotChainManager snapshotChainManager = omMetadataManager.getSnapshotChainManager();

    AtomicReference<IOException> exceptionRef = new AtomicReference<>(null);
    inFlightSnapshotCount.updateAndGet(count -> {
      int currentSnapshotNum = 0;
      try {
        currentSnapshotNum = snapshotChainManager.getGlobalSnapshotChain().size();
      } catch (IOException e) {
        exceptionRef.set(e);
        return count;
      }
      if (currentSnapshotNum + count >= fsSnapshotMaxLimit) {
        exceptionRef.set(new OMException(
            String.format("Snapshot limit of %d reached. Cannot create more snapshots. " +
                "Current snapshots: %d, In-flight creations: %d",
                fsSnapshotMaxLimit, currentSnapshotNum, count) +
                " If you already deleted some snapshots, " +
                "please wait for the background service to complete the cleanup.",
            OMException.ResultCodes.TOO_MANY_SNAPSHOTS));
        return count;
      }
      return count + 1;
    });
    if (exceptionRef.get() != null) {
      throw exceptionRef.get();
    }
  }

  public void decrementInFlightSnapshotCount() {
    // TODO this is a work around for the accounting logic of `inFlightSnapshotCount`.
    //    - It incorrectly assumes that LeaderReady means that there are no inflight snapshot requests.
    //    We may consider fixing it by waiting all the pending requests in notifyLeaderReady().
    //    - Also, it seems to have another bug that the PrepareState could disallow snapshot requests.
    //    In such case, `inFlightSnapshotCount` won't be decremented.
    int result = inFlightSnapshotCount.decrementAndGet();
    if (result < 0) {
      resetInFlightSnapshotCount();
    }
  }

  public void resetInFlightSnapshotCount() {
    inFlightSnapshotCount.set(0);
  }

  public int getInFlightSnapshotCount() {
    return inFlightSnapshotCount.get();
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
        Arrays.asList(DEFAULT_COLUMN_FAMILY_NAME, SnapshotDiffDBDefinition.SNAP_DIFF_JOB_TABLE_NAME,
            SnapshotDiffDBDefinition.SNAP_DIFF_REPORT_TABLE_NAME, SNAP_DIFF_PURGED_JOB_TABLE_NAME));

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
      snapshotCache.close();
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
    if (snapshotLocalDataManager != null) {
      snapshotLocalDataManager.close();
    }
  }

  public long getDiffCleanupServiceInterval() {
    return diffCleanupServiceInterval;
  }
}
