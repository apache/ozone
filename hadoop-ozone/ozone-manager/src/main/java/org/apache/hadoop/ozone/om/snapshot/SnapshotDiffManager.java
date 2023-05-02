/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.snapshot;

import com.google.common.cache.LoadingCache;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.NativeConstants;
import org.apache.hadoop.hdds.utils.NativeLibraryLoader;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.io.file.PathUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSSTDumpTool;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.WithObjectID;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.util.ClosableIterator;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;
import org.apache.ozone.rocksdb.util.ManagedSstFileReader;
import org.apache.ozone.rocksdb.util.RdbUtil;
import org.apache.ozone.rocksdiff.DifferSnapshotInfo;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.apache.ozone.rocksdiff.RocksDiffUtils;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.SynchronousQueue;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_JOB_DEFAULT_WAIT_TIME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_JOB_DEFAULT_WAIT_TIME_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_THREAD_POOL_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_THREAD_POOL_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF_DEFAULT;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.DELIMITER;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.dropColumnFamilyHandle;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.getSnapshotInfo;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.FAILED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.IN_PROGRESS;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.QUEUED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.REJECTED;

import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;

/**
 * Class to generate snapshot diff.
 */
public class SnapshotDiffManager implements AutoCloseable {
  private static final Logger LOG =
          LoggerFactory.getLogger(SnapshotDiffManager.class);
  private static final String FROM_SNAP_TABLE_SUFFIX = "-from-snap";
  private static final String TO_SNAP_TABLE_SUFFIX = "-to-snap";
  private static final String UNIQUE_IDS_TABLE_SUFFIX = "-unique-ids";
  private static final String DELETE_DIFF_TABLE_SUFFIX = "-delete-diff";
  private static final String RENAME_DIFF_TABLE_SUFFIX = "-rename-diff";
  private static final String CREATE_DIFF_TABLE_SUFFIX = "-create-diff";
  private static final String MODIFY_DIFF_TABLE_SUFFIX = "-modify-diff";

  private final ManagedRocksDB db;
  private final RocksDBCheckpointDiffer differ;
  private final OzoneManager ozoneManager;
  private final LoadingCache<String, OmSnapshot> snapshotCache;
  private final CodecRegistry codecRegistry;
  private final ManagedColumnFamilyOptions familyOptions;
  // TODO: [SNAPSHOT] Use different wait time based of job status.
  private final long defaultWaitTime;

  /**
   * Global table to keep the diff report. Each key is prefixed by the jobID
   * to improve look up and clean up.
   * Note that byte array is used to reduce the unnecessary serialization and
   * deserialization during intermediate steps.
   */
  private final PersistentMap<byte[], byte[]> snapDiffReportTable;

  /**
   * Contains all the snap diff jobs which are either queued, in_progress or
   * done. This table is used to make sure that there is only single job for
   * similar type of request at any point of time.
   */
  private final PersistentMap<String, SnapshotDiffJob> snapDiffJobTable;
  private final ExecutorService executorService;

  /**
   * Directory to keep hardlinks of SST files for a snapDiff job temporarily.
   * It is to make sure that SST files don't get deleted for the in_progress
   * job/s as part of compaction DAG and SST file pruning
   * {@link RocksDBCheckpointDiffer#pruneOlderSnapshotsWithCompactionHistory}.
   */
  private final String sstBackupDirForSnapDiffJobs;

  private boolean isNativeRocksToolsLoaded;

  private ManagedSSTDumpTool sstDumpTool;

  @SuppressWarnings("parameternumber")
  public SnapshotDiffManager(ManagedRocksDB db,
                             RocksDBCheckpointDiffer differ,
                             OzoneManager ozoneManager,
                             LoadingCache<String, OmSnapshot> snapshotCache,
                             ColumnFamilyHandle snapDiffJobCfh,
                             ColumnFamilyHandle snapDiffReportCfh,
                             ManagedColumnFamilyOptions familyOptions,
                             CodecRegistry codecRegistry) {
    this.db = db;
    this.differ = differ;
    this.ozoneManager = ozoneManager;
    this.snapshotCache = snapshotCache;
    this.familyOptions = familyOptions;
    this.codecRegistry = codecRegistry;
    this.defaultWaitTime = ozoneManager.getConfiguration().getTimeDuration(
        OZONE_OM_SNAPSHOT_DIFF_JOB_DEFAULT_WAIT_TIME,
        OZONE_OM_SNAPSHOT_DIFF_JOB_DEFAULT_WAIT_TIME_DEFAULT,
        TimeUnit.MILLISECONDS
    );

    int threadPoolSize = ozoneManager.getConfiguration().getInt(
        OZONE_OM_SNAPSHOT_DIFF_THREAD_POOL_SIZE,
        OZONE_OM_SNAPSHOT_DIFF_THREAD_POOL_SIZE_DEFAULT
    );

    this.snapDiffJobTable = new RocksDbPersistentMap<>(db,
        snapDiffJobCfh,
        codecRegistry,
        String.class,
        SnapshotDiffJob.class);

    this.snapDiffReportTable = new RocksDbPersistentMap<>(db,
        snapDiffReportCfh,
        codecRegistry,
        byte[].class,
        byte[].class);

    this.executorService = new ThreadPoolExecutor(threadPoolSize,
        threadPoolSize,
        0,
        TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(threadPoolSize)
    );

    Path path = Paths.get(differ.getMetadataDir(), "snapDiff");
    createEmptySnapDiffDir(path);
    this.sstBackupDirForSnapDiffJobs = path.toString();

    // Ideally, loadJobsOnStartUp should run only on OM node, since SnapDiff
    // is not HA currently and running this on all the nodes would be
    // inefficient. Especially, when OM node restarts and loses its leadership.
    // However, it is hard to determine if node is leader node because consensus
    // happens inside Ratis. We can add something like Awaitility.wait() here
    // but that is not foolproof either.
    // Hence, we decided that it is OK to let this run on all the OM nodes for
    // now knowing that it would be inefficient.
    // When SnapshotDiffManager loads for very first time, loadJobsOnStartUp
    // will be no-ops for all the nodes. In subsequent restarts or upgrades,
    // it would run on the current leader and most like on previous leader only.
    // When we build snapDiff HA aware, we will revisit this.
    // Details: https://github.com/apache/ozone/pull/4438#discussion_r1149788226
    this.loadJobsOnStartUp();
    isNativeRocksToolsLoaded = NativeLibraryLoader.getInstance()
            .loadLibrary(NativeConstants.ROCKS_TOOLS_NATIVE_LIBRARY_NAME);
    if (isNativeRocksToolsLoaded) {
      isNativeRocksToolsLoaded = initSSTDumpTool(
          ozoneManager.getConfiguration());
    }
  }

  private boolean initSSTDumpTool(OzoneConfiguration conf) {
    try {
      int threadPoolSize = conf.getInt(
              OMConfigKeys.OZONE_OM_SNAPSHOT_SST_DUMPTOOL_EXECUTOR_POOL_SIZE,
              OMConfigKeys
                  .OZONE_OM_SNAPSHOT_SST_DUMPTOOL_EXECUTOR_POOL_SIZE_DEFAULT);
      int bufferSize = (int) conf.getStorageSize(
          OMConfigKeys.OZONE_OM_SNAPSHOT_SST_DUMPTOOL_EXECUTOR_BUFFER_SIZE,
          OMConfigKeys
              .OZONE_OM_SNAPSHOT_SST_DUMPTOOL_EXECUTOR_BUFFER_SIZE_DEFAULT,
              StorageUnit.BYTES);
      ExecutorService execService = new ThreadPoolExecutor(0,
              threadPoolSize, 60, TimeUnit.SECONDS,
              new SynchronousQueue<>(), new ThreadFactoryBuilder()
              .setNameFormat("snapshot-diff-manager-sst-dump-tool-TID-%d")
              .build(),
              new ThreadPoolExecutor.DiscardPolicy());
      sstDumpTool = new ManagedSSTDumpTool(execService, bufferSize);
    } catch (NativeLibraryNotLoadedException e) {
      return false;
    }
    return true;
  }

  /**
   * Creates an empty dir. If directory exists, it deletes that and then
   * creates new one otherwise just create a new dir.
   * Throws IllegalStateException if, couldn't delete the existing
   * directory or fails to create it.
   * <p>
   * We delete existing dir is to remove all hardlinks and free up the space
   * if there were any created by previous snapDiff job and were not removed
   * because of any failure.
   */
  private void createEmptySnapDiffDir(Path path) {
    try {
      if (Files.exists(path)) {
        PathUtils.deleteDirectory(path);
      }
      Files.createDirectories(path);
    } catch (IOException e) {
      throw new IllegalStateException("Couldn't delete existing or create new" +
          " directory for:" + path, e);
    }

    // Create readme file.
    Path readmePath = Paths.get(path.toString(), "_README.txt");
    File readmeFile = new File(readmePath.toString());
    if (!readmeFile.exists()) {
      try (BufferedWriter bw = Files.newBufferedWriter(
          readmePath, StandardOpenOption.CREATE)) {
        bw.write("This directory is used to store SST files needed to" +
            " generate snap diff report for a particular job.\n" +
            " DO NOT add, change or delete any files in this directory" +
            " unless you know what you are doing.\n");
      } catch (IOException ignored) {
      }
    }
  }

  private void deleteDir(Path path) {
    if (path == null || Files.notExists(path)) {
      return;
    }

    try {
      PathUtils.deleteDirectory(path);
    } catch (IOException e) {
      // TODO: [SNAPSHOT] Fail gracefully
      throw new IllegalStateException(e);
    }
  }

  private Map<String, String> getTablePrefixes(
      OMMetadataManager omMetadataManager,
      String volumeName, String bucketName) throws IOException {
    // Copied straight from TestOMSnapshotDAG. TODO: Dedup. Move this to util.
    Map<String, String> tablePrefixes = new HashMap<>();
    String volumeId = String.valueOf(omMetadataManager.getVolumeId(volumeName));
    String bucketId = String.valueOf(
        omMetadataManager.getBucketId(volumeName, bucketName));
    tablePrefixes.put(OmMetadataManagerImpl.KEY_TABLE,
        OM_KEY_PREFIX + volumeName + OM_KEY_PREFIX + bucketName);
    tablePrefixes.put(OmMetadataManagerImpl.FILE_TABLE,
        OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX + bucketId);
    tablePrefixes.put(OmMetadataManagerImpl.DIRECTORY_TABLE,
        OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX + bucketId);
    return tablePrefixes;
  }

  /**
   * Convert from SnapshotInfo to DifferSnapshotInfo.
   */
  private DifferSnapshotInfo getDSIFromSI(SnapshotInfo snapshotInfo,
      OmSnapshot omSnapshot, final String volumeName, final String bucketName)
      throws IOException {

    final OMMetadataManager snapshotOMMM = omSnapshot.getMetadataManager();
    final String checkpointPath =
        snapshotOMMM.getStore().getDbLocation().getPath();
    final String snapshotId = snapshotInfo.getSnapshotID();
    final long dbTxSequenceNumber = snapshotInfo.getDbTxSequenceNumber();

    return new DifferSnapshotInfo(
        checkpointPath,
        snapshotId,
        dbTxSequenceNumber,
        getTablePrefixes(snapshotOMMM, volumeName, bucketName));
  }

  private Set<String> getSSTFileListForSnapshot(OmSnapshot snapshot,
          List<String> tablesToLookUp) throws RocksDBException {
    return RdbUtil.getSSTFilesForComparison(snapshot
        .getMetadataManager().getStore().getDbLocation()
        .getPath(), tablesToLookUp);
  }

  @SuppressWarnings("parameternumber")
  public SnapshotDiffResponse getSnapshotDiffReport(
      final String volume,
      final String bucket,
      final OmSnapshot fromSnapshot,
      final OmSnapshot toSnapshot,
      final SnapshotInfo fsInfo,
      final SnapshotInfo tsInfo,
      final int index,
      final int pageSize,
      final boolean forceFullDiff
  ) throws IOException {
    String snapDiffJobKey = fsInfo.getSnapshotID() + DELIMITER +
        tsInfo.getSnapshotID();

    SnapshotDiffJob snapDiffJob = getSnapDiffReportStatus(snapDiffJobKey,
        volume, bucket, fromSnapshot.getName(), toSnapshot.getName(),
        forceFullDiff);

    OFSPath snapshotRoot = getSnapshotRootPath(volume, bucket);

    switch (snapDiffJob.getStatus()) {
    case QUEUED:
      return submitSnapDiffJob(snapDiffJobKey, volume, bucket, fromSnapshot,
          toSnapshot, fsInfo, tsInfo, index, pageSize, forceFullDiff);
    case IN_PROGRESS:
      return new SnapshotDiffResponse(
          new SnapshotDiffReportOzone(snapshotRoot.toString(), volume, bucket,
              fromSnapshot.getName(), toSnapshot.getName(), new ArrayList<>(),
              null), IN_PROGRESS, defaultWaitTime);
    case FAILED:
      return new SnapshotDiffResponse(
          new SnapshotDiffReportOzone(snapshotRoot.toString(), volume, bucket,
              fromSnapshot.getName(), toSnapshot.getName(), new ArrayList<>(),
              null), FAILED, defaultWaitTime);
    case DONE:
      SnapshotDiffReportOzone report =
          createPageResponse(snapDiffJob.getJobId(), volume, bucket,
              fromSnapshot, toSnapshot, index, pageSize);
      return new SnapshotDiffResponse(report, DONE, 0L);
    default:
      throw new IllegalStateException("Unknown snapshot job status: " +
          snapDiffJob.getStatus());
    }
  }

  @NotNull
  private static OFSPath getSnapshotRootPath(String volume, String bucket) {
    org.apache.hadoop.fs.Path bucketPath = new org.apache.hadoop.fs.Path(
        OZONE_URI_DELIMITER + volume + OZONE_URI_DELIMITER + bucket);
    OFSPath path = new OFSPath(bucketPath, new OzoneConfiguration());
    return path;
  }

  private SnapshotDiffReportOzone createPageResponse(final String jobId,
                                                final String volume,
                                                final String bucket,
                                                final OmSnapshot fromSnapshot,
                                                final OmSnapshot toSnapshot,
                                                final int index,
                                                final int pageSize)
      throws IOException {
    List<DiffReportEntry> diffReportList = new ArrayList<>();

    OFSPath path = getSnapshotRootPath(volume, bucket);

    boolean hasMoreEntries = true;

    for (int idx = index; idx - index < pageSize; idx++) {
      byte[] rawKey = codecRegistry.asRawData(jobId + DELIMITER + idx);
      byte[] bytes = snapDiffReportTable.get(rawKey);
      if (bytes == null) {
        hasMoreEntries = false;
        break;
      }
      diffReportList.add(codecRegistry.asObject(bytes, DiffReportEntry.class));
    }

    String tokenString = hasMoreEntries ?
        String.valueOf(index + pageSize) : null;

    return new SnapshotDiffReportOzone(path.toString(), volume, bucket,
        fromSnapshot.getName(), toSnapshot.getName(), diffReportList,
        tokenString);
  }

  @SuppressWarnings("parameternumber")
  private synchronized SnapshotDiffResponse submitSnapDiffJob(
      final String jobKey,
      final String volume,
      final String bucket,
      final OmSnapshot fromSnapshot,
      final OmSnapshot toSnapshot,
      final SnapshotInfo fsInfo,
      final SnapshotInfo tsInfo,
      final int index,
      final int pageSize,
      final boolean forceFullDiff
  ) throws IOException {

    SnapshotDiffJob snapDiffJob = snapDiffJobTable.get(jobKey);

    OFSPath snapshotRoot = getSnapshotRootPath(volume, bucket);

    // This is only possible if another thread tired to submit the request,
    // and it got rejected. In this scenario, return the Rejected job status
    // with wait time.
    if (snapDiffJob == null) {
      LOG.info("Snap diff job has been removed for for volume: {}, " +
          "bucket: {}, fromSnapshot: {} and toSnapshot: {}.",
          volume, bucket, fromSnapshot.getName(), toSnapshot.getName());
      return new SnapshotDiffResponse(
          new SnapshotDiffReportOzone(snapshotRoot.toString(),
              volume, bucket, fromSnapshot.getName(), toSnapshot.getName(),
              new ArrayList<>(), null), REJECTED, defaultWaitTime);
    }

    // Check again that request is still in queued status. If it is not queued,
    // return the response accordingly for early return.
    if (snapDiffJob.getStatus() != QUEUED) {
      // Same request is submitted by another thread and already completed.
      if (snapDiffJob.getStatus() == DONE) {
        SnapshotDiffReportOzone report =
            createPageResponse(snapDiffJob.getJobId(), volume, bucket,
                fromSnapshot, toSnapshot, index, pageSize);
        return new SnapshotDiffResponse(report, DONE, 0L);
      } else {
        // Otherwise, return the same status as in DB with wait time.
        return new SnapshotDiffResponse(
            new SnapshotDiffReportOzone(snapshotRoot.toString(), volume, bucket,
                fromSnapshot.getName(), toSnapshot.getName(), new ArrayList<>(),
                null), snapDiffJob.getStatus(), defaultWaitTime);
      }
    }

    return submitSnapDiffJob(jobKey, snapDiffJob.getJobId(), volume, bucket,
        fromSnapshot, toSnapshot, fsInfo, tsInfo, forceFullDiff);
  }

  @SuppressWarnings("parameternumber")
  private synchronized SnapshotDiffResponse submitSnapDiffJob(
      final String jobKey,
      final String jobId,
      final String volume,
      final String bucket,
      final OmSnapshot fromSnapshot,
      final OmSnapshot toSnapshot,
      final SnapshotInfo fsInfo,
      final SnapshotInfo tsInfo,
      final boolean forceFullDiff
  ) {

    LOG.info("Submitting snap diff report generation request for" +
            " volume: {}, bucket: {}, fromSnapshot: {} and toSnapshot: {}",
        volume, bucket, fromSnapshot.getName(), toSnapshot.getName());

    OFSPath snapshotRoot = getSnapshotRootPath(volume, bucket);

    // Submit the request to the executor if job is still in queued status.
    // If executor cannot take any more job, remove the job form DB and return
    // the Rejected Job status with wait time.
    try {
      executorService.execute(() -> generateSnapshotDiffReport(jobKey, jobId,
          volume, bucket, fromSnapshot, toSnapshot, fsInfo, tsInfo,
          forceFullDiff));
      updateJobStatus(jobKey, QUEUED, IN_PROGRESS);
      return new SnapshotDiffResponse(
          new SnapshotDiffReportOzone(snapshotRoot.toString(), volume, bucket,
              fromSnapshot.getName(), toSnapshot.getName(), new ArrayList<>(),
              null), IN_PROGRESS, defaultWaitTime);
    } catch (RejectedExecutionException exception) {
      // Remove the entry from job table so that client can retry.
      // If entry is not removed, client has to wait till cleanup service
      // removes the entry even tho there are resources to execute the request
      // before the cleanup kicks in.
      snapDiffJobTable.remove(jobKey);
      LOG.info("Exceeded the snapDiff parallel requests progressing " +
          "limit. Please retry after {}.", defaultWaitTime);
      return new SnapshotDiffResponse(
          new SnapshotDiffReportOzone(snapshotRoot.toString(), volume, bucket,
              fromSnapshot.getName(), toSnapshot.getName(), new ArrayList<>(),
              null), REJECTED, defaultWaitTime);
    }
  }

  /**
   * Check if there is an existing request for the same `fromSnapshot` and
   * `toSnapshot`. If yes, then return that response otherwise adds a new entry
   * to the table for the future requests and returns that.
   */
  private synchronized SnapshotDiffJob getSnapDiffReportStatus(
      String snapDiffJobKey,
      String volume,
      String bucket,
      String fromSnapshot,
      String toSnapshot,
      boolean forceFullDiff
  ) {
    SnapshotDiffJob snapDiffJob = snapDiffJobTable.get(snapDiffJobKey);

    if (snapDiffJob == null) {
      String jobId = UUID.randomUUID().toString();
      snapDiffJob = new SnapshotDiffJob(System.currentTimeMillis(), jobId,
          QUEUED, volume, bucket, fromSnapshot, toSnapshot, forceFullDiff, 0L);
      snapDiffJobTable.put(snapDiffJobKey, snapDiffJob);
    }

    return snapDiffJob;
  }

  @SuppressWarnings({"checkstyle:Parameternumber", "checkstyle:MethodLength"})
  private void generateSnapshotDiffReport(final String jobKey,
                                          final String jobId,
                                          final String volume,
                                          final String bucket,
                                          final OmSnapshot fromSnapshot,
                                          final OmSnapshot toSnapshot,
                                          final SnapshotInfo fsInfo,
                                          final SnapshotInfo tsInfo,
                                          final boolean forceFullDiff) {
    ColumnFamilyHandle fromSnapshotColumnFamily = null;
    ColumnFamilyHandle toSnapshotColumnFamily = null;
    ColumnFamilyHandle objectIDsColumnFamily = null;

    // Creates temporary unique dir for the snapDiff job to keep SST files
    // hardlinks. JobId is used as dir name for uniqueness.
    // It is required to prevent that SST files get deleted for in_progress
    // job by RocksDBCheckpointDiffer#pruneOlderSnapshotsWithCompactionHistory.
    Path path = Paths.get(sstBackupDirForSnapDiffJobs + "/" + jobId);

    try {
      Files.createDirectories(path);
      // JobId is prepended to column families name to make them unique
      // for request.
      fromSnapshotColumnFamily =
          createColumnFamily(jobId + FROM_SNAP_TABLE_SUFFIX);
      toSnapshotColumnFamily =
          createColumnFamily(jobId + TO_SNAP_TABLE_SUFFIX);
      objectIDsColumnFamily =
          createColumnFamily(jobId + UNIQUE_IDS_TABLE_SUFFIX);

      // ObjectId to keyName map to keep key info for fromSnapshot.
      // objectIdToKeyNameMap is used to identify what keys were touched
      // in which snapshot and to know the difference if operation was
      // creation, deletion, modify or rename.
      // Stores only keyName instead of OmKeyInfo to reduce the memory
      // footprint.
      // Note: Store objectId and keyName as byte array to reduce unnecessary
      // serialization and deserialization.
      final PersistentMap<byte[], byte[]> objectIdToKeyNameMapForFromSnapshot =
          new RocksDbPersistentMap<>(db, fromSnapshotColumnFamily,
              codecRegistry, byte[].class, byte[].class);
      // ObjectId to keyName map to keep key info for toSnapshot.
      final PersistentMap<byte[], byte[]> objectIdToKeyNameMapForToSnapshot =
          new RocksDbPersistentMap<>(db, toSnapshotColumnFamily, codecRegistry,
              byte[].class, byte[].class);
      // Set of unique objectId between fromSnapshot and toSnapshot.
      final PersistentSet<byte[]> objectIDsToCheckMap =
          new RocksDbPersistentSet<>(db, objectIDsColumnFamily, codecRegistry,
              byte[].class);

      final BucketLayout bucketLayout = getBucketLayout(volume, bucket,
          fromSnapshot.getMetadataManager());
      final Table<String, OmKeyInfo> fsKeyTable =
          fromSnapshot.getMetadataManager().getKeyTable(bucketLayout);
      final Table<String, OmKeyInfo> tsKeyTable =
          toSnapshot.getMetadataManager().getKeyTable(bucketLayout);

      boolean useFullDiff = ozoneManager.getConfiguration().getBoolean(
          OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF,
          OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF_DEFAULT);
      if (forceFullDiff) {
        useFullDiff = true;
      }

      Map<String, String> tablePrefixes =
          getTablePrefixes(toSnapshot.getMetadataManager(), volume, bucket);
      List<String> tablesToLookUp =
              Collections.singletonList(fsKeyTable.getName());
      Set<String> deltaFilesForKeyOrFileTable = getDeltaFiles(fromSnapshot,
          toSnapshot, tablesToLookUp, fsInfo, tsInfo, useFullDiff,
          tablePrefixes, path.toString());

      // Workaround to handle deletes if native rocksDb tool for reading
      // tombstone is not loaded.
      // TODO: [SNAPSHOT] Update Rocksdb SSTFileIterator to read tombstone
      if (!isNativeRocksToolsLoaded) {
        deltaFilesForKeyOrFileTable.addAll(getSSTFileListForSnapshot(
                fromSnapshot, tablesToLookUp));
      }
      try {
        addToObjectIdMap(fsKeyTable, tsKeyTable,
            Pair.of(isNativeRocksToolsLoaded, deltaFilesForKeyOrFileTable),
            objectIdToKeyNameMapForFromSnapshot,
            objectIdToKeyNameMapForToSnapshot, objectIDsToCheckMap,
            tablePrefixes);
      } catch (NativeLibraryNotLoadedException e) {
        // Workaround to handle deletes if use of native rockstools for reading
        // tombstone fails.
        // TODO: [SNAPSHOT] Update Rocksdb SSTFileIterator to read tombstone
        deltaFilesForKeyOrFileTable.addAll(getSSTFileListForSnapshot(
                fromSnapshot, tablesToLookUp));
        try {
          addToObjectIdMap(fsKeyTable, tsKeyTable,
              Pair.of(false, deltaFilesForKeyOrFileTable),
              objectIdToKeyNameMapForFromSnapshot,
              objectIdToKeyNameMapForToSnapshot, objectIDsToCheckMap,
              tablePrefixes);
        } catch (NativeLibraryNotLoadedException ex) {
          // This code should be never executed.
          throw new IllegalStateException(ex);
        }
      }

      if (bucketLayout.isFileSystemOptimized()) {
        final Table<String, OmDirectoryInfo> fsDirTable =
            fromSnapshot.getMetadataManager().getDirectoryTable();
        final Table<String, OmDirectoryInfo> tsDirTable =
            toSnapshot.getMetadataManager().getDirectoryTable();
        tablesToLookUp = Collections.singletonList(fsDirTable.getName());
        final Set<String> deltaFilesForDirTable =
            getDeltaFiles(fromSnapshot, toSnapshot, tablesToLookUp, fsInfo,
                    tsInfo, useFullDiff, tablePrefixes, path.toString());
        if (!isNativeRocksToolsLoaded) {
          deltaFilesForDirTable.addAll(getSSTFileListForSnapshot(
                  fromSnapshot, tablesToLookUp));
        }
        try {
          addToObjectIdMap(fsDirTable, tsDirTable,
              Pair.of(isNativeRocksToolsLoaded, deltaFilesForDirTable),
              objectIdToKeyNameMapForFromSnapshot,
              objectIdToKeyNameMapForToSnapshot,
              objectIDsToCheckMap,
              tablePrefixes);
        } catch (NativeLibraryNotLoadedException e) {
          try {
            // Workaround to handle deletes if use of native rockstools for
            // reading tombstone fails.
            // TODO: [SNAPSHOT] Update Rocksdb SSTFileIterator to read tombstone
            deltaFilesForDirTable.addAll(getSSTFileListForSnapshot(
                    fromSnapshot, tablesToLookUp));
            addToObjectIdMap(fsDirTable, tsDirTable,
                Pair.of(false, deltaFilesForDirTable),
                objectIdToKeyNameMapForFromSnapshot,
                objectIdToKeyNameMapForToSnapshot, objectIDsToCheckMap,
                tablePrefixes);
          } catch (NativeLibraryNotLoadedException ex) {
            // This code should be never executed.
            throw new IllegalStateException(ex);
          }
        }
      }

      long totalDiffEntries = generateDiffReport(jobId,
          objectIDsToCheckMap,
          objectIdToKeyNameMapForFromSnapshot,
          objectIdToKeyNameMapForToSnapshot);
      updateJobStatusToDone(jobKey, totalDiffEntries);
    } catch (IOException | RocksDBException exception) {
      updateJobStatus(jobKey, IN_PROGRESS, FAILED);
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(exception.getCause());
    } finally {
      // Clean up: drop the intermediate column family and close them.
      dropAndCloseColumnFamilyHandle(fromSnapshotColumnFamily);
      dropAndCloseColumnFamilyHandle(toSnapshotColumnFamily);
      dropAndCloseColumnFamilyHandle(objectIDsColumnFamily);
      // Delete SST files backup directory.
      deleteDir(path);
    }
  }

  private void addToObjectIdMap(Table<String, ? extends WithObjectID> fsTable,
                                Table<String, ? extends WithObjectID> tsTable,
                                Pair<Boolean, Set<String>>
                                    isNativeRocksToolsLoadedDeltaFilesPair,
                                PersistentMap<byte[], byte[]> oldObjIdToKeyMap,
                                PersistentMap<byte[], byte[]> newObjIdToKeyMap,
                                PersistentSet<byte[]> objectIDsToCheck,
                                Map<String, String> tablePrefixes)
      throws IOException, NativeLibraryNotLoadedException {

    Set<String> deltaFiles = isNativeRocksToolsLoadedDeltaFilesPair.getRight();
    if (deltaFiles.isEmpty()) {
      return;
    }
    boolean nativeRocksToolsLoaded =
        isNativeRocksToolsLoadedDeltaFilesPair.getLeft();
    boolean isDirectoryTable =
        fsTable.getName().equals(OmMetadataManagerImpl.DIRECTORY_TABLE);
    ManagedSstFileReader sstFileReader = new ManagedSstFileReader(deltaFiles);
    try (Stream<String> keysToCheck = nativeRocksToolsLoaded
                 ? sstFileReader.getKeyStreamWithTombstone(sstDumpTool)
                 : sstFileReader.getKeyStream()) {
      keysToCheck.forEach(key -> {
        try {
          final WithObjectID oldKey = fsTable.get(key);
          final WithObjectID newKey = tsTable.get(key);
          if (areKeysEqual(oldKey, newKey) || !isKeyInBucket(key, tablePrefixes,
              fsTable.getName())) {
            // We don't have to do anything.
            return;
          }
          if (oldKey != null) {
            byte[] rawObjId = codecRegistry.asRawData(oldKey.getObjectID());
            byte[] rawValue = codecRegistry.asRawData(
                getKeyOrDirectoryName(isDirectoryTable, oldKey));
            oldObjIdToKeyMap.put(rawObjId, rawValue);
            objectIDsToCheck.add(rawObjId);
          }
          if (newKey != null) {
            byte[] rawObjId = codecRegistry.asRawData(newKey.getObjectID());
            byte[] rawValue = codecRegistry.asRawData(
                getKeyOrDirectoryName(isDirectoryTable, newKey));
            newObjIdToKeyMap.put(rawObjId, rawValue);
            objectIDsToCheck.add(rawObjId);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (RocksDBException rocksDBException) {
      // TODO: [SNAPSHOT] Gracefully handle exception
      //  e.g. when input files do not exist
      throw new RuntimeException(rocksDBException);
    }
  }

  private String getKeyOrDirectoryName(boolean isDirectory,
                                       WithObjectID object) {
    if (isDirectory) {
      OmDirectoryInfo directoryInfo = (OmDirectoryInfo) object;
      return directoryInfo.getName();
    }
    OmKeyInfo keyInfo = (OmKeyInfo) object;
    return keyInfo.getKeyName();
  }

  @NotNull
  @SuppressWarnings("parameternumber")
  private Set<String> getDeltaFiles(OmSnapshot fromSnapshot,
                                    OmSnapshot toSnapshot,
                                    List<String> tablesToLookUp,
                                    SnapshotInfo fsInfo, SnapshotInfo tsInfo,
                                    boolean useFullDiff,
                                    Map<String, String> tablePrefixes,
                                    String diffDir)
      throws RocksDBException, IOException {
    // TODO: [SNAPSHOT] Refactor the parameter list

    final Set<String> deltaFiles = new HashSet<>();

    // Check if compaction DAG is available, use that if so
    if (differ != null && fsInfo != null && tsInfo != null && !useFullDiff) {
      String volume = fsInfo.getVolumeName();
      String bucket = fsInfo.getBucketName();
      // Construct DifferSnapshotInfo
      final DifferSnapshotInfo fromDSI =
          getDSIFromSI(fsInfo, fromSnapshot, volume, bucket);
      final DifferSnapshotInfo toDSI =
          getDSIFromSI(tsInfo, toSnapshot, volume, bucket);

      LOG.debug("Calling RocksDBCheckpointDiffer");
      List<String> sstDiffList =
          differ.getSSTDiffListWithFullPath(toDSI, fromDSI, diffDir);
      deltaFiles.addAll(sstDiffList);
    }

    if (useFullDiff || deltaFiles.isEmpty()) {
      // If compaction DAG is not available (already cleaned up), fall back to
      //  the slower approach.
      if (!useFullDiff) {
        LOG.warn("RocksDBCheckpointDiffer is not available, falling back to" +
                " slow path");
      }

      Set<String> fromSnapshotFiles =
          RdbUtil.getSSTFilesForComparison(
              fromSnapshot.getMetadataManager().getStore()
                  .getDbLocation().getPath(),
              tablesToLookUp);
      Set<String> toSnapshotFiles =
          RdbUtil.getSSTFilesForComparison(
              toSnapshot.getMetadataManager().getStore()
                  .getDbLocation().getPath(),
              tablesToLookUp);

      deltaFiles.addAll(fromSnapshotFiles);
      deltaFiles.addAll(toSnapshotFiles);
      RocksDiffUtils.filterRelevantSstFiles(deltaFiles, tablePrefixes);
    }

    return deltaFiles;
  }

  private long generateDiffReport(
      final String jobId,
      final PersistentSet<byte[]> objectIDsToCheck,
      final PersistentMap<byte[], byte[]> oldObjIdToKeyMap,
      final PersistentMap<byte[], byte[]> newObjIdToKeyMap
  ) throws IOException {
    ColumnFamilyHandle deleteDiffColumnFamily = null;
    ColumnFamilyHandle renameDiffColumnFamily = null;
    ColumnFamilyHandle createDiffColumnFamily = null;
    ColumnFamilyHandle modifyDiffColumnFamily = null;

    // JobId is prepended to column family name to make it unique for request.
    try {
      deleteDiffColumnFamily =
          createColumnFamily(jobId + DELETE_DIFF_TABLE_SUFFIX);
      renameDiffColumnFamily =
          createColumnFamily(jobId + RENAME_DIFF_TABLE_SUFFIX);
      createDiffColumnFamily =
          createColumnFamily(jobId + CREATE_DIFF_TABLE_SUFFIX);
      modifyDiffColumnFamily =
          createColumnFamily(jobId + MODIFY_DIFF_TABLE_SUFFIX);

      // Keep byte array instead of storing as DiffReportEntry to avoid
      // unnecessary serialization and deserialization.
      final PersistentList<byte[]> deleteDiffs =
          createDiffReportPersistentList(deleteDiffColumnFamily);
      final PersistentList<byte[]> renameDiffs =
          createDiffReportPersistentList(renameDiffColumnFamily);
      final PersistentList<byte[]> createDiffs =
          createDiffReportPersistentList(createDiffColumnFamily);
      final PersistentList<byte[]> modifyDiffs =
          createDiffReportPersistentList(modifyDiffColumnFamily);

      try (ClosableIterator<byte[]>
               objectIdsIterator = objectIDsToCheck.iterator()) {
        while (objectIdsIterator.hasNext()) {
          byte[] id = objectIdsIterator.next();
          /*
           * This key can be
           * -> Created after the old snapshot was taken, which means it will be
           *    missing in oldKeyTable and present in newKeyTable.
           * -> Deleted after the old snapshot was taken, which means it will be
           *    present in oldKeyTable and missing in newKeyTable.
           * -> Modified after the old snapshot was taken, which means it will
           *    be present in oldKeyTable and present in newKeyTable with same
           *    Object ID but with different metadata.
           * -> Renamed after the old snapshot was taken, which means it will be
           *    present in oldKeyTable and present in newKeyTable but with
           *    different name and same Object ID.
           */

          byte[] oldKeyName = oldObjIdToKeyMap.get(id);
          byte[] newKeyName = newObjIdToKeyMap.get(id);

          if (oldKeyName == null && newKeyName == null) {
            // This cannot happen.
            throw new IllegalStateException(
                "Old and new key name both are null");
          } else if (oldKeyName == null) { // Key Created.
            String key = codecRegistry.asObject(newKeyName, String.class);
            DiffReportEntry entry =
                SnapshotDiffReportOzone.getDiffReportEntry(DiffType.CREATE,
                    key);
            createDiffs.add(codecRegistry.asRawData(entry));
          } else if (newKeyName == null) { // Key Deleted.
            String key = codecRegistry.asObject(oldKeyName, String.class);
            DiffReportEntry entry =
                SnapshotDiffReportOzone.getDiffReportEntry(DiffType.DELETE,
                    key);
            deleteDiffs.add(codecRegistry.asRawData(entry));
          } else if (Arrays.equals(oldKeyName, newKeyName)) { // Key modified.
            String key = codecRegistry.asObject(newKeyName, String.class);
            DiffReportEntry entry =
                SnapshotDiffReportOzone.getDiffReportEntry(DiffType.MODIFY,
                    key);
            modifyDiffs.add(codecRegistry.asRawData(entry));
          } else { // Key Renamed.
            String oldKey = codecRegistry.asObject(oldKeyName, String.class);
            String newKey = codecRegistry.asObject(newKeyName, String.class);
            renameDiffs.add(codecRegistry.asRawData(
                SnapshotDiffReportOzone.getDiffReportEntry(DiffType.RENAME,
                    oldKey, newKey)));
          }
        }
      }

      /*
       * The order in which snap-diff should be applied
       *
       *     1. Delete diffs
       *     2. Rename diffs
       *     3. Create diffs
       *     4. Modified diffs
       *
       * Consider the following scenario
       *
       *    1. File "A" is created.
       *    2. File "B" is created.
       *    3. File "C" is created.
       *    Snapshot "1" is taken.
       *
       * Case 1:
       *   1. File "A" is deleted.
       *   2. File "B" is renamed to "A".
       *   Snapshot "2" is taken.
       *
       *   Snapshot diff should be applied in the following order:
       *    1. Delete "A"
       *    2. Rename "B" to "A"
       *
       *
       * Case 2:
       *    1. File "B" is renamed to "C".
       *    2. File "B" is created.
       *    Snapshot "2" is taken.
       *
       *   Snapshot diff should be applied in the following order:
       *    1. Rename "B" to "C"
       *    2. Create "B"
       *
       */

      long index = 0;
      index = addToReport(jobId, index, deleteDiffs);
      index = addToReport(jobId, index, renameDiffs);
      index = addToReport(jobId, index, createDiffs);
      return addToReport(jobId, index, modifyDiffs);
    } catch (RocksDBException | IOException e) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(e);
    } finally {

      dropAndCloseColumnFamilyHandle(deleteDiffColumnFamily);
      dropAndCloseColumnFamilyHandle(renameDiffColumnFamily);
      dropAndCloseColumnFamilyHandle(createDiffColumnFamily);
      dropAndCloseColumnFamilyHandle(modifyDiffColumnFamily);
    }
  }

  private PersistentList<byte[]> createDiffReportPersistentList(
      ColumnFamilyHandle columnFamilyHandle
  ) {
    return new RocksDbPersistentList<>(db,
        columnFamilyHandle,
        codecRegistry,
        byte[].class);
  }

  private ColumnFamilyHandle createColumnFamily(String columnFamilyName)
      throws RocksDBException {
    return db.get().createColumnFamily(
        new ColumnFamilyDescriptor(
            StringUtils.string2Bytes(columnFamilyName),
            familyOptions));
  }

  private long addToReport(String jobId, long index,
                           PersistentList<byte[]> diffReportEntries)
      throws IOException {
    try (ClosableIterator<byte[]>
             diffReportIterator = diffReportEntries.iterator()) {
      while (diffReportIterator.hasNext()) {

        snapDiffReportTable.put(
            codecRegistry.asRawData(jobId + DELIMITER + index),
            diffReportIterator.next());
        index++;
      }
    }
    return index;
  }

  private void dropAndCloseColumnFamilyHandle(
      final ColumnFamilyHandle columnFamilyHandle) {

    if (columnFamilyHandle == null) {
      return;
    }

    dropColumnFamilyHandle(db, columnFamilyHandle);
    columnFamilyHandle.close();
  }

  private synchronized void updateJobStatus(String jobKey,
                                            JobStatus oldStatus,
                                            JobStatus newStatus) {
    SnapshotDiffJob snapshotDiffJob = snapDiffJobTable.get(jobKey);
    if (snapshotDiffJob.getStatus() != oldStatus) {
      throw new IllegalStateException("Invalid job status. Current job " +
          "status is '" + snapshotDiffJob.getStatus() + "', while '" +
          oldStatus + "' is expected.");
    }
    snapshotDiffJob.setStatus(newStatus);
    snapDiffJobTable.put(jobKey, snapshotDiffJob);
  }

  private synchronized void updateJobStatusToDone(String jobKey,
                                                  long totalNumberOfEntries) {
    SnapshotDiffJob snapshotDiffJob = snapDiffJobTable.get(jobKey);
    if (snapshotDiffJob.getStatus() != IN_PROGRESS) {
      throw new IllegalStateException("Invalid job status. Current job " +
          "status is '" + snapshotDiffJob.getStatus() + "', while '" +
          IN_PROGRESS + "' is expected.");
    }
    snapshotDiffJob.setStatus(DONE);
    snapshotDiffJob.setTotalDiffEntries(totalNumberOfEntries);
    snapDiffJobTable.put(jobKey, snapshotDiffJob);
  }

  private BucketLayout getBucketLayout(final String volume,
                                       final String bucket,
                                       final OMMetadataManager mManager)
      throws IOException {
    final String bucketTableKey = mManager.getBucketKey(volume, bucket);
    return mManager.getBucketTable().get(bucketTableKey).getBucketLayout();
  }

  private boolean areKeysEqual(WithObjectID oldKey, WithObjectID newKey) {
    if (oldKey == null && newKey == null) {
      return true;
    }
    if (oldKey != null) {
      return oldKey.equals(newKey);
    }
    return false;
  }

  /**
   * check if the given key is in the bucket specified by tablePrefix map.
   */
  private boolean isKeyInBucket(String key, Map<String, String> tablePrefixes,
      String tableName) {
    String volumeBucketDbPrefix;
    // In case of FSO - either File/Directory table
    // the key Prefix would be volumeId/bucketId and
    // in case of non-fso - volumeName/bucketName
    if (tableName.equals(
        OmMetadataManagerImpl.DIRECTORY_TABLE) || tableName.equals(
        OmMetadataManagerImpl.FILE_TABLE)) {
      volumeBucketDbPrefix =
          tablePrefixes.get(OmMetadataManagerImpl.DIRECTORY_TABLE);
    } else {
      volumeBucketDbPrefix = tablePrefixes.get(OmMetadataManagerImpl.KEY_TABLE);
    }
    return key.startsWith(volumeBucketDbPrefix);
  }

  /**
   * Loads the jobs which are in_progress and submits them to executor to start
   * processing.
   * This is needed to load previously running (in_progress) jobs to the
   * executor on service start up when OM restarts. If not done, these jobs
   * will never be completed if OM crashes when jobs were running.
   * Don't need to load queued jobs because responses for queued jobs were never
   * returned to client. In short, we don't return queued job status to client.
   * When client re-submits previously queued job, workflow will pick it and
   * execute it.
   */
  private void loadJobsOnStartUp() {

    try (ClosableIterator<Map.Entry<String, SnapshotDiffJob>> iterator =
             snapDiffJobTable.iterator()) {
      while (iterator.hasNext()) {
        Map.Entry<String, SnapshotDiffJob> next = iterator.next();
        String jobKey = next.getKey();
        SnapshotDiffJob snapshotDiffJob = next.getValue();
        if (snapshotDiffJob.getStatus() == IN_PROGRESS) {
          // This is done just to be in parity of the workflow.
          // If job status is not updated to QUEUED, workflow will fail when
          // job gets submitted to executor and its status is IN_PROGRESS.
          // Because according to workflow job can change its state from
          // QUEUED to IN_PROGRESS but not IN_PROGRESS to IN_PROGRESS.
          updateJobStatus(jobKey, IN_PROGRESS, QUEUED);

          loadJobOnStartUp(jobKey,
              snapshotDiffJob.getJobId(),
              snapshotDiffJob.getVolume(),
              snapshotDiffJob.getBucket(),
              snapshotDiffJob.getFromSnapshot(),
              snapshotDiffJob.getToSnapshot(),
              snapshotDiffJob.isForceFullDiff());
        }
      }
    } catch (IOException e) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(e.getCause());
    }
  }

  private synchronized void loadJobOnStartUp(final String jobKey,
                                             final String jobId,
                                             final String volume,
                                             final String bucket,
                                             final String fromSnapshot,
                                             final String toSnapshot,
                                             final boolean forceFullDiff)
      throws IOException {

    SnapshotInfo fsInfo = getSnapshotInfo(ozoneManager,
        volume, bucket, fromSnapshot);
    SnapshotInfo tsInfo = getSnapshotInfo(ozoneManager,
        volume, bucket, toSnapshot);

    String fsKey = SnapshotInfo.getTableKey(volume, bucket, fromSnapshot);
    String tsKey = SnapshotInfo.getTableKey(volume, bucket, toSnapshot);
    try {
      submitSnapDiffJob(jobKey, jobId, volume, bucket, snapshotCache.get(fsKey),
          snapshotCache.get(tsKey), fsInfo, tsInfo, forceFullDiff);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  @Override
  public void close() throws Exception {
    if (executorService != null) {
      executorService.shutdown();
    }
  }
}
