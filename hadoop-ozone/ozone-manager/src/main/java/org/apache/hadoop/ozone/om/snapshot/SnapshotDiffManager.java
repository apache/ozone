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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.commons.lang3.StringUtils.leftPad;
import static org.apache.hadoop.hdds.StringUtils.getLexicographicallyHigherString;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.CREATE;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.DELETE;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.MODIFY;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.RENAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_DISABLE_NATIVE_LIBS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_DISABLE_NATIVE_LIBS_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_JOB_DEFAULT_WAIT_TIME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_JOB_DEFAULT_WAIT_TIME_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_MAX_ALLOWED_KEYS_CHANGED_PER_DIFF_JOB;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_MAX_ALLOWED_KEYS_CHANGED_PER_DIFF_JOB_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_THREAD_POOL_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_THREAD_POOL_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF_DEFAULT;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.DELIMITER;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.checkSnapshotActive;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.dropColumnFamilyHandle;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.getSnapshotInfo;
import static org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse.CancelMessage.CANCEL_ALREADY_CANCELLED_JOB;
import static org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse.CancelMessage.CANCEL_ALREADY_DONE_JOB;
import static org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse.CancelMessage.CANCEL_ALREADY_FAILED_JOB;
import static org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse.CancelMessage.CANCEL_FAILED;
import static org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse.CancelMessage.CANCEL_JOB_NOT_EXIST;
import static org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse.CancelMessage.CANCEL_NON_CANCELLABLE;
import static org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse.CancelMessage.CANCEL_SUCCEEDED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.CANCELLED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.FAILED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.IN_PROGRESS;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.QUEUED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.REJECTED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus.DIFF_REPORT_GEN;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus.OBJECT_ID_MAP_GEN_FSO;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus.OBJECT_ID_MAP_GEN_OBS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jakarta.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.io.file.PathUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.SstFileSetReader;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotDiffJob;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.WithObjectID;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.om.snapshot.db.SnapshotDiffDBDefinition;
import org.apache.hadoop.ozone.om.snapshot.diff.delta.CompositeDeltaDiffComputer;
import org.apache.hadoop.ozone.om.snapshot.diff.delta.DeltaFileComputer;
import org.apache.hadoop.ozone.om.snapshot.util.TableMergeIterator;
import org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.ListSnapshotDiffJobResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus;
import org.apache.hadoop.ozone.util.ClosableIterator;
import org.apache.logging.log4j.util.Strings;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to generate snapshot diff.
 */
public class SnapshotDiffManager implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotDiffManager.class);
  private static final String DELETE_DIFF_TABLE_SUFFIX = "-delete-diff";
  private static final String RENAME_DIFF_TABLE_SUFFIX = "-rename-diff";
  private static final String CREATE_DIFF_TABLE_SUFFIX = "-create-diff";
  private static final String MODIFY_DIFF_TABLE_SUFFIX = "-modify-diff";

  private final ManagedRocksDB db;
  private final OzoneManager ozoneManager;
  private final OMMetadataManager activeOmMetadataManager;
  private final CodecRegistry codecRegistry;
  private final ManagedColumnFamilyOptions familyOptions;
  // TODO: [SNAPSHOT] Use different wait time based of job status.
  private final long defaultWaitTime;
  private final long maxAllowedKeyChangesForASnapDiff;

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
  private final ExecutorService snapDiffExecutor;

  /**
   * Directory to keep hardlinks of SST files for a snapDiff job temporarily.
   * It is to make sure that SST files don't get deleted for the in_progress
   * job/s as part of compaction DAG and SST file pruning
   * {@link RocksDBCheckpointDiffer#pruneOlderSnapshotsWithCompactionHistory}.
   */
  private final String sstBackupDirForSnapDiffJobs;

  private final boolean snapshotForceFullDiff;

  private final boolean diffDisableEfficientDiff;

  private final BiFunction<SnapshotInfo, SnapshotInfo, String>
      generateSnapDiffJobKey =
          (SnapshotInfo fromSnapshotInfo, SnapshotInfo toSnapshotInfo) ->
              fromSnapshotInfo.getSnapshotId() + DELIMITER +
                  toSnapshotInfo.getSnapshotId();

  @SuppressWarnings("parameternumber")
  public SnapshotDiffManager(ManagedRocksDB db,
                             OzoneManager ozoneManager,
                             ColumnFamilyHandle snapDiffJobCfh,
                             ColumnFamilyHandle snapDiffReportCfh,
                             ManagedColumnFamilyOptions familyOptions,
                             CodecRegistry codecRegistry) {
    this.db = db;
    this.ozoneManager = ozoneManager;
    this.activeOmMetadataManager = ozoneManager.getMetadataManager();
    this.familyOptions = familyOptions;
    this.codecRegistry = codecRegistry;
    this.defaultWaitTime = ozoneManager.getConfiguration().getTimeDuration(
        OZONE_OM_SNAPSHOT_DIFF_JOB_DEFAULT_WAIT_TIME,
        OZONE_OM_SNAPSHOT_DIFF_JOB_DEFAULT_WAIT_TIME_DEFAULT,
        TimeUnit.MILLISECONDS
    );

    this.snapshotForceFullDiff = ozoneManager.getConfiguration().getBoolean(
        OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF,
        OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF_DEFAULT);

    this.diffDisableEfficientDiff = ozoneManager.getConfiguration().getBoolean(
        OZONE_OM_SNAPSHOT_DIFF_DISABLE_NATIVE_LIBS,
        OZONE_OM_SNAPSHOT_DIFF_DISABLE_NATIVE_LIBS_DEFAULT);

    this.maxAllowedKeyChangesForASnapDiff = ozoneManager.getConfiguration()
        .getLong(
            OZONE_OM_SNAPSHOT_DIFF_MAX_ALLOWED_KEYS_CHANGED_PER_DIFF_JOB,
            OZONE_OM_SNAPSHOT_DIFF_MAX_ALLOWED_KEYS_CHANGED_PER_DIFF_JOB_DEFAULT
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

    this.snapDiffExecutor = new ThreadPoolExecutor(threadPoolSize,
        threadPoolSize,
        0,
        TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(threadPoolSize),
        new ThreadFactoryBuilder()
            .setNameFormat(ozoneManager.getThreadNamePrefix() +
                "snapshot-diff-job-thread-id-%d")
            .build()
    );

    RDBStore rdbStore = (RDBStore) ozoneManager.getMetadataManager().getStore();
    Objects.requireNonNull(rdbStore, "DBStore can't be null.");
    Path path = Paths.get(rdbStore.getSnapshotMetadataDir(), "snapDiff");
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
  }

  @VisibleForTesting
  public PersistentMap<String, SnapshotDiffJob> getSnapDiffJobTable() {
    return snapDiffJobTable;
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

  /**
   * Gets the report key for a particular index of snapshot diff job.
   */

  static String getReportKeyForIndex(String jobId, long index) {
    return jobId + DELIMITER + leftPad(String.valueOf(index), 20, '0');
  }

  public CancelSnapshotDiffResponse cancelSnapshotDiff(
      final String volumeName,
      final String bucketName,
      final String fromSnapshotName,
      final String toSnapshotName
  ) throws IOException {
    SnapshotInfo fsInfo =
        getSnapshotInfo(ozoneManager, volumeName, bucketName, fromSnapshotName);
    SnapshotInfo tsInfo =
        getSnapshotInfo(ozoneManager, volumeName, bucketName, toSnapshotName);

    String diffJobKey = generateSnapDiffJobKey.apply(fsInfo, tsInfo);
    SnapshotDiffJob diffJob = snapDiffJobTable.get(diffJobKey);

    if (diffJob == null) {
      return new CancelSnapshotDiffResponse(CANCEL_JOB_NOT_EXIST.getMessage());
    }

    String reason;
    switch (diffJob.getStatus()) {
    case  IN_PROGRESS:
      try {
        updateJobStatus(diffJobKey, IN_PROGRESS, CANCELLED);
        reason = CANCEL_SUCCEEDED.getMessage();
      } catch (IllegalStateException exception) {
        LOG.warn("Failed to update the job status.", exception);
        reason = CANCEL_FAILED.getMessage();
      }
      break;
    case DONE:
      reason = CANCEL_ALREADY_DONE_JOB.getMessage();
      break;
    case CANCELLED:
      reason = CANCEL_ALREADY_CANCELLED_JOB.getMessage();
      break;
    case FAILED:
      reason = CANCEL_ALREADY_FAILED_JOB.getMessage();
      break;
    default:
      reason = CANCEL_NON_CANCELLABLE.getMessage() +
          "Current status: " + diffJob.getStatus();
      break;
    }
    return new CancelSnapshotDiffResponse(reason);
  }

  public ListSnapshotDiffJobResponse getSnapshotDiffJobList(
      String volumeName,
      String bucketName,
      String jobStatus,
      boolean listAllStatus,
      String prevDiffJob,
      int maxEntries) throws IOException {
    List<SnapshotDiffJob> jobs = new ArrayList<>();
    String lastSnapshotDiffJob = null;

    try (ClosableIterator<Map.Entry<String, SnapshotDiffJob>> iterator =
             snapDiffJobTable.iterator(Optional.ofNullable(prevDiffJob), Optional.empty())) {
      Map.Entry<String, SnapshotDiffJob> entry = null;
      while (iterator.hasNext() && jobs.size() < maxEntries) {
        entry = iterator.next();
        SnapshotDiffJob snapshotDiffJob = entry.getValue();
        if (Objects.equals(prevDiffJob, entry.getKey())) {
          continue;
        }

        if (Objects.equals(snapshotDiffJob.getVolume(), volumeName) &&
            Objects.equals(snapshotDiffJob.getBucket(), bucketName)) {
          if (listAllStatus) {
            jobs.add(snapshotDiffJob);
          } else if (Objects.equals(snapshotDiffJob.getStatus(), getJobStatus(jobStatus))) {
            jobs.add(snapshotDiffJob);
          }
        }
      }

      // If maxEntries are populated and list still has more entries,
      // set the continuation token for the next page request otherwise null.
      if (iterator.hasNext()) {
        assert entry != null;
        lastSnapshotDiffJob = entry.getKey();
      }
    }

    return new ListSnapshotDiffJobResponse(jobs, lastSnapshotDiffJob);
  }

  private JobStatus getJobStatus(String jobStatus)
      throws IOException {
    try {
      return JobStatus.valueOf(jobStatus.toUpperCase());
    } catch (IllegalArgumentException ex) {
      LOG.info(ex.toString());
      throw new IOException("Invalid job status: " + jobStatus);
    }
  }

  @SuppressWarnings("parameternumber")
  public SnapshotDiffResponse getSnapshotDiffReport(
      final String volumeName,
      final String bucketName,
      final String fromSnapshotName,
      final String toSnapshotName,
      final int index,
      final int pageSize,
      final boolean forceFullDiff,
      final boolean disableNativeDiff
  ) throws IOException {

    SnapshotInfo fsInfo = getSnapshotInfo(ozoneManager,
        volumeName, bucketName, fromSnapshotName);
    SnapshotInfo tsInfo = getSnapshotInfo(ozoneManager,
        volumeName, bucketName, toSnapshotName);

    String snapDiffJobKey = generateSnapDiffJobKey.apply(fsInfo, tsInfo);

    SnapshotDiffJob snapDiffJob = getSnapDiffReportStatus(snapDiffJobKey,
        volumeName, bucketName, fromSnapshotName, toSnapshotName,
        forceFullDiff, disableNativeDiff);

    OFSPath snapshotRoot = getSnapshotRootPath(volumeName, bucketName);

    switch (snapDiffJob.getStatus()) {
    case QUEUED:
      return submitSnapDiffJob(snapDiffJobKey, volumeName, bucketName,
          fromSnapshotName, toSnapshotName, index, pageSize, forceFullDiff,
          disableNativeDiff);
    case IN_PROGRESS:
      SnapshotDiffResponse response = new SnapshotDiffResponse(
          new SnapshotDiffReportOzone(snapshotRoot.toString(), volumeName, bucketName,
              fromSnapshotName, toSnapshotName,
              new ArrayList<>(), null), IN_PROGRESS, defaultWaitTime);
      response.setSubStatus(snapDiffJob.getSubStatus());
      response.setProgressPercent(snapDiffJob.getKeysProcessedPct());
      return response;
    case FAILED:
      return new SnapshotDiffResponse(
          new SnapshotDiffReportOzone(snapshotRoot.toString(), volumeName,
              bucketName, fromSnapshotName, toSnapshotName, new ArrayList<>(),
              null),
          FAILED,
          // waitTime is equal to clean up internal. After that job will be
          // removed and client can retry.
          ozoneManager.getOmSnapshotManager().getDiffCleanupServiceInterval(),
          snapDiffJob.getReason());
    case DONE:
      SnapshotDiffReportOzone report = createPageResponse(snapDiffJob,
          volumeName, bucketName, fromSnapshotName, toSnapshotName, index,
          pageSize);
      return new SnapshotDiffResponse(report, DONE, 0L);
    case REJECTED:
      return new SnapshotDiffResponse(
          new SnapshotDiffReportOzone(snapshotRoot.toString(), volumeName,
              bucketName, fromSnapshotName, toSnapshotName, new ArrayList<>(),
              null),
          REJECTED, defaultWaitTime);
    case CANCELLED:
      return new SnapshotDiffResponse(
          new SnapshotDiffReportOzone(snapshotRoot.toString(), volumeName,
              bucketName, fromSnapshotName, toSnapshotName, new ArrayList<>(),
              null),
          CANCELLED, 0L);
    default:
      throw new IllegalStateException("Unknown snapshot job status: " +
          snapDiffJob.getStatus());
    }
  }

  @Nonnull
  public static OFSPath getSnapshotRootPath(String volume, String bucket) {
    org.apache.hadoop.fs.Path bucketPath = new org.apache.hadoop.fs.Path(
        OZONE_URI_DELIMITER + volume + OZONE_URI_DELIMITER + bucket);
    return new OFSPath(bucketPath, new OzoneConfiguration());
  }

  @VisibleForTesting
  SnapshotDiffReportOzone createPageResponse(
      final SnapshotDiffJob snapDiffJob,
      final String volumeName,
      final String bucketName,
      final String fromSnapshotName,
      final String toSnapshotName,
      final int index,
      final int pageSize
  ) throws IOException {
    if (index < 0 || index > snapDiffJob.getTotalDiffEntries()
        || pageSize <= 0) {
      throw new IOException(String.format(
          "Index (given: %d) should be a number >= 0 and < totalDiffEntries: " +
              "%d. Page size (given: %d) should be a positive number > 0.",
          index, snapDiffJob.getTotalDiffEntries(), pageSize));
    }

    OFSPath path = getSnapshotRootPath(volumeName, bucketName);

    Pair<List<DiffReportEntry>, String> pageResponse =
        createPageResponse(snapDiffJob, index, pageSize);
    List<DiffReportEntry> diffReportList = pageResponse.getLeft();
    String tokenString = pageResponse.getRight();

    return new SnapshotDiffReportOzone(path.toString(), volumeName, bucketName,
        fromSnapshotName, toSnapshotName, diffReportList, tokenString);
  }

  Pair<List<DiffReportEntry>, String> createPageResponse(
      final SnapshotDiffJob snapDiffJob,
      final int index,
      final int pageSize
  ) throws IOException {
    List<DiffReportEntry> diffReportList = new ArrayList<>();

    boolean hasMoreEntries = true;

    byte[] lowerIndex = codecRegistry.asRawData(getReportKeyForIndex(
        snapDiffJob.getJobId(), index));
    byte[] upperIndex = codecRegistry.asRawData(getReportKeyForIndex(
        snapDiffJob.getJobId(), index + pageSize));
    int idx = index;
    try (ClosableIterator<Map.Entry<byte[], byte[]>> iterator =
             snapDiffReportTable.iterator(Optional.of(lowerIndex),
                 Optional.of(upperIndex))) {
      int itemsFetched = 0;
      while (iterator.hasNext() && itemsFetched < pageSize) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        byte[] bytes = entry.getValue();
        diffReportList.add(codecRegistry.asObject(bytes,
            DiffReportEntry.class));
        idx += 1;
        itemsFetched += 1;
      }
      if (diffReportList.size() < pageSize) {
        hasMoreEntries = false;
      }
    }

    String nextTokenString = hasMoreEntries ? String.valueOf(idx) : null;

    checkReportsIntegrity(snapDiffJob, index, diffReportList.size());
    return Pair.of(diffReportList, nextTokenString);
  }

  /**
   * Check that total number of entries after creating the last page matches
   * that the total number of entries set after the diff report generation.
   * If check fails, it marks the job failed so that it is GC-ed by clean up
   * service and throws the exception to client.
   */
  @VisibleForTesting
  void checkReportsIntegrity(final SnapshotDiffJob diffJob,
                             final int pageStartIdx,
                             final int numberOfEntriesInPage)
      throws IOException {
    if ((pageStartIdx >= diffJob.getTotalDiffEntries() &&
        numberOfEntriesInPage != 0) || (pageStartIdx <
        diffJob.getTotalDiffEntries() && numberOfEntriesInPage == 0)) {
      LOG.error("Expected TotalDiffEntries: {} but found " +
              "TotalDiffEntries: {}",
          diffJob.getTotalDiffEntries(),
          pageStartIdx + numberOfEntriesInPage);
      updateJobStatus(diffJob.getJobId(), DONE, FAILED);
      throw new IOException("Report integrity check failed. Retry after: " +
          ozoneManager.getOmSnapshotManager().getDiffCleanupServiceInterval());
    }
  }

  @SuppressWarnings("parameternumber")
  private synchronized SnapshotDiffResponse submitSnapDiffJob(
      final String jobKey,
      final String volume,
      final String bucket,
      final String fromSnapshot,
      final String toSnapshot,
      final int index,
      final int pageSize,
      final boolean forceFullDiff,
      final boolean disableNativeDiff
  ) throws IOException {

    SnapshotDiffJob snapDiffJob = snapDiffJobTable.get(jobKey);

    OFSPath snapshotRoot = getSnapshotRootPath(volume, bucket);

    // This is only possible if another thread tried to submit the request,
    // and it got rejected. In this scenario, return the Rejected job status
    // with wait time.
    if (snapDiffJob == null) {
      LOG.info("Snap diff job has been removed for volume: {}, " +
          "bucket: {}, fromSnapshot: {} and toSnapshot: {}.",
          volume, bucket, fromSnapshot, toSnapshot);
      return new SnapshotDiffResponse(
          new SnapshotDiffReportOzone(snapshotRoot.toString(),
              volume, bucket, fromSnapshot, toSnapshot, new ArrayList<>(),
              null), REJECTED, defaultWaitTime);
    }

    // Check again that request is still in queued status. If it is not queued,
    // return the response accordingly for early return.
    if (snapDiffJob.getStatus() != QUEUED) {
      // Same request is submitted by another thread and already completed.
      if (snapDiffJob.getStatus() == DONE) {
        SnapshotDiffReportOzone report = createPageResponse(snapDiffJob, volume,
            bucket, fromSnapshot, toSnapshot, index, pageSize);
        return new SnapshotDiffResponse(report, DONE, 0L);
      } else {
        // Otherwise, return the same status as in DB with wait time.
        return new SnapshotDiffResponse(
            new SnapshotDiffReportOzone(snapshotRoot.toString(), volume, bucket,
                fromSnapshot, toSnapshot, new ArrayList<>(), null),
            snapDiffJob.getStatus(), defaultWaitTime);
      }
    }

    return submitSnapDiffJob(jobKey, snapDiffJob.getJobId(), volume, bucket,
        fromSnapshot, toSnapshot, forceFullDiff, disableNativeDiff);
  }

  @SuppressWarnings("parameternumber")
  private synchronized SnapshotDiffResponse submitSnapDiffJob(
      final String jobKey,
      final String jobId,
      final String volumeName,
      final String bucketName,
      final String fromSnapshotName,
      final String toSnapshotName,
      final boolean forceFullDiff,
      final boolean disableNativeDiff) {

    LOG.info("Submitting snap diff report generation request for" +
            " volume: {}, bucket: {}, fromSnapshot: {} and toSnapshot: {}",
        volumeName, bucketName, fromSnapshotName, toSnapshotName);

    OFSPath snapshotRoot = getSnapshotRootPath(volumeName, bucketName);

    // Submit the request to the executor if job is still in queued status.
    // If executor cannot take any more job, remove the job form DB and return
    // the Rejected Job status with wait time.
    try {
      updateJobStatus(jobKey, QUEUED, IN_PROGRESS);
      snapDiffExecutor.execute(() -> generateSnapshotDiffReport(jobKey, jobId,
          volumeName, bucketName, fromSnapshotName, toSnapshotName,
          forceFullDiff, disableNativeDiff));
      return new SnapshotDiffResponse(
          new SnapshotDiffReportOzone(snapshotRoot.toString(), volumeName,
              bucketName, fromSnapshotName, toSnapshotName, new ArrayList<>(),
              null),
          IN_PROGRESS, defaultWaitTime);
    } catch (RejectedExecutionException exception) {
      // Remove the entry from job table so that client can retry.
      // If entry is not removed, client has to wait till cleanup service
      // removes the entry even tho there are resources to execute the request
      // before the cleanup kicks in.
      snapDiffJobTable.remove(jobKey);
      LOG.info("Exceeded the snapDiff parallel requests progressing " +
          "limit. Removed the jobKey: {}. Please retry after {}.",
          jobKey, defaultWaitTime);
      return new SnapshotDiffResponse(
          new SnapshotDiffReportOzone(snapshotRoot.toString(), volumeName,
              bucketName, fromSnapshotName, toSnapshotName, new ArrayList<>(),
              null),
          REJECTED, defaultWaitTime);
    } catch (Exception exception) {
      // Remove the entry from job table as well.
      snapDiffJobTable.remove(jobKey);
      LOG.error("Failure in job submission to the executor. Removed the" +
              " jobKey: {}.", jobKey, exception);
      return new SnapshotDiffResponse(
          new SnapshotDiffReportOzone(snapshotRoot.toString(), volumeName,
              bucketName, fromSnapshotName, toSnapshotName, new ArrayList<>(),
              null),
          FAILED, defaultWaitTime);
    }
  }

  /**
   * Check if there is an existing request for the same `fromSnapshot` and
   * `toSnapshot`. If yes, then return that response otherwise adds a new entry
   * to the table for the future requests and returns that.
   */
  private synchronized SnapshotDiffJob getSnapDiffReportStatus(
      String jobKey,
      String volumeName,
      String bucketName,
      String fromSnapshotName,
      String toSnapshotName,
      boolean forceFullDiff,
      boolean disableNativeDiff) {
    SnapshotDiffJob snapDiffJob = snapDiffJobTable.get(jobKey);

    if (snapDiffJob == null) {
      String jobId = UUID.randomUUID().toString();
      snapDiffJob = new SnapshotDiffJob(System.currentTimeMillis(), jobId,
          QUEUED, volumeName, bucketName, fromSnapshotName, toSnapshotName, forceFullDiff,
          disableNativeDiff, 0L, null, 0.0);
      snapDiffJobTable.put(jobKey, snapDiffJob);
    }

    return snapDiffJob;
  }

  @VisibleForTesting
  boolean areDiffJobAndSnapshotsActive(
      final String volumeName, final String bucketName,
      final String fromSnapshotName, final String toSnapshotName)
      throws IOException {
    SnapshotInfo fromSnapInfo = getSnapshotInfo(ozoneManager, volumeName,
        bucketName, fromSnapshotName);
    SnapshotInfo toSnapInfo = getSnapshotInfo(ozoneManager, volumeName,
        bucketName, toSnapshotName);

    String jobKey = generateSnapDiffJobKey.apply(fromSnapInfo, toSnapInfo);
    SnapshotDiffJob diffJob = snapDiffJobTable.get(jobKey);
    if (diffJob == null || diffJob.getStatus() == CANCELLED) {
      return false;
    }
    checkSnapshotActive(fromSnapInfo, false);
    checkSnapshotActive(toSnapInfo, false);

    return true;
  }

  @SuppressWarnings({"methodlength", "parameternumber"})
  @VisibleForTesting
  void generateSnapshotDiffReport(final String jobKey,
                                  final String jobId,
                                  final String volumeName,
                                  final String bucketName,
                                  final String fromSnapshotName,
                                  final String toSnapshotName,
                                  final boolean forceFullDiff,
                                  final boolean disableNativeDiff) {
    LOG.info("Started snap diff report generation for volume: '{}', " +
            "bucket: '{}', fromSnapshot: '{}', toSnapshot: '{}'",
        volumeName, bucketName, fromSnapshotName, toSnapshotName);
    ozoneManager.getMetrics().incNumSnapshotDiffJobs();

    ColumnFamilyHandle fromSnapshotColumnFamily = null;
    ColumnFamilyHandle toSnapshotColumnFamily = null;
    ColumnFamilyHandle objectIDsColumnFamily = null;

    // Creates temporary unique dir for the snapDiff job to keep SST files
    // hardlinks. JobId is used as dir name for uniqueness.
    // It is required to prevent that SST files get deleted for in_progress
    // job by RocksDBCheckpointDiffer#pruneOlderSnapshotsWithCompactionHistory.
    Path diffJobPath = Paths.get(sstBackupDirForSnapDiffJobs).resolve(jobId);

    UncheckedAutoCloseableSupplier<OmSnapshot> rcFromSnapshot = null;
    UncheckedAutoCloseableSupplier<OmSnapshot> rcToSnapshot = null;

    boolean useFullDiff = snapshotForceFullDiff || forceFullDiff;
    boolean performNonNativeDiff = diffDisableEfficientDiff || disableNativeDiff;

    Consumer<SubStatus> activityReporter = (jobStatus) -> recordActivity(jobKey, jobStatus);
    try (DeltaFileComputer deltaFileComputer = new CompositeDeltaDiffComputer(ozoneManager.getOmSnapshotManager(),
        activeOmMetadataManager, diffJobPath, activityReporter, useFullDiff, performNonNativeDiff)) {
      if (!areDiffJobAndSnapshotsActive(volumeName, bucketName,
          fromSnapshotName, toSnapshotName)) {
        return;
      }
      rcFromSnapshot =
          ozoneManager.getOmSnapshotManager()
              .getActiveSnapshot(volumeName, bucketName, fromSnapshotName);
      rcToSnapshot =
          ozoneManager.getOmSnapshotManager()
              .getActiveSnapshot(volumeName, bucketName, toSnapshotName);

      OmSnapshot fromSnapshot = rcFromSnapshot.get();
      OmSnapshot toSnapshot = rcToSnapshot.get();
      SnapshotInfo fsInfo = getSnapshotInfo(ozoneManager,
          volumeName, bucketName, fromSnapshotName);
      SnapshotInfo tsInfo = getSnapshotInfo(ozoneManager,
          volumeName, bucketName, toSnapshotName);
      // JobId is prepended to column families name to make them unique
      // for request.
      fromSnapshotColumnFamily =
          createColumnFamily(jobId + SnapshotDiffDBDefinition.SNAP_DIFF_FROM_SNAP_OBJECT_TABLE_NAME);
      toSnapshotColumnFamily =
          createColumnFamily(jobId + SnapshotDiffDBDefinition.SNAP_DIFF_TO_SNAP_OBJECT_TABLE_NAME);
      objectIDsColumnFamily =
          createColumnFamily(jobId + SnapshotDiffDBDefinition.SNAP_DIFF_UNIQUE_IDS_TABLE_NAME);

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
      final PersistentMap<byte[], Boolean> objectIdToIsDirMap =
          new RocksDbPersistentMap<>(db, objectIDsColumnFamily, codecRegistry,
              byte[].class, Boolean.class);

      final BucketLayout bucketLayout = getBucketLayout(volumeName, bucketName,
          fromSnapshot.getMetadataManager());
      TablePrefixInfo tablePrefixes = toSnapshot.getMetadataManager().getTableBucketPrefix(volumeName, bucketName);

      if (!areDiffJobAndSnapshotsActive(volumeName, bucketName,
          fromSnapshotName, toSnapshotName)) {
        return;
      }
      Table<String, OmKeyInfo> fsKeyTable = fromSnapshot.getMetadataManager()
          .getKeyTable(bucketLayout);
      Table<String, OmKeyInfo> tsKeyTable = toSnapshot.getMetadataManager()
          .getKeyTable(bucketLayout);
      Table<String, OmDirectoryInfo> fsDirTable;
      Table<String, OmDirectoryInfo> tsDirTable;

      final Optional<Set<Long>> oldParentIds;
      final Optional<Set<Long>> newParentIds;
      if (bucketLayout.isFileSystemOptimized()) {
        oldParentIds = Optional.of(new HashSet<>());
        newParentIds = Optional.of(new HashSet<>());
        fsDirTable = fromSnapshot.getMetadataManager().getDirectoryTable();
        tsDirTable = toSnapshot.getMetadataManager().getDirectoryTable();
      } else {
        oldParentIds = Optional.empty();
        newParentIds = Optional.empty();
        fsDirTable = null;
        tsDirTable = null;
      }

      final Optional<Map<Long, Path>> oldParentIdPathMap;
      final Optional<Map<Long, Path>> newParentIdPathMap;
      if (bucketLayout.isFileSystemOptimized()) {
        oldParentIdPathMap = Optional.of(Maps.newHashMap());
        newParentIdPathMap = Optional.of(Maps.newHashMap());
      } else {
        oldParentIdPathMap = Optional.empty();
        newParentIdPathMap = Optional.empty();
      }
      // These are the most time and resource consuming method calls.
      // Split the calls into steps and store them in an array, to avoid
      // repetition while constantly checking if the job is cancelled.
      Callable<Void>[] methodCalls = new Callable[]{
          () -> {
            recordActivity(jobKey, OBJECT_ID_MAP_GEN_OBS);
            getDeltaFilesAndDiffKeysToObjectIdToKeyMap(fsKeyTable, tsKeyTable,
                fsInfo, tsInfo, performNonNativeDiff, tablePrefixes,
                objectIdToKeyNameMapForFromSnapshot,
                objectIdToKeyNameMapForToSnapshot, objectIdToIsDirMap,
                oldParentIds, newParentIds, deltaFileComputer, jobKey);
            return null;
          },
          () -> {
            if (bucketLayout.isFileSystemOptimized()) {
              recordActivity(jobKey, OBJECT_ID_MAP_GEN_FSO);
              getDeltaFilesAndDiffKeysToObjectIdToKeyMap(fsDirTable, tsDirTable,
                  fsInfo, tsInfo, performNonNativeDiff, tablePrefixes,
                  objectIdToKeyNameMapForFromSnapshot,
                  objectIdToKeyNameMapForToSnapshot, objectIdToIsDirMap,
                  oldParentIds, newParentIds, deltaFileComputer, jobKey);
            }
            return null;
          },
          () -> {
            if (bucketLayout.isFileSystemOptimized()) {
              long bucketId = toSnapshot.getMetadataManager()
                  .getBucketId(volumeName, bucketName);
              String tablePrefix = tablePrefixes.getTablePrefix(fromSnapshot.getMetadataManager()
                  .getDirectoryTable().getName());
              oldParentIdPathMap.get().putAll(new FSODirectoryPathResolver(
                  tablePrefix, bucketId,
                  fromSnapshot.getMetadataManager().getDirectoryTable())
                  .getAbsolutePathForObjectIDs(oldParentIds, true));
              newParentIdPathMap.get().putAll(new FSODirectoryPathResolver(
                  tablePrefix, bucketId,
                  toSnapshot.getMetadataManager().getDirectoryTable())
                  .getAbsolutePathForObjectIDs(newParentIds, true));
            }
            return null;
          },
          () -> {
            recordActivity(jobKey, DIFF_REPORT_GEN);
            long totalDiffEntries = generateDiffReport(jobId,
                fsKeyTable,
                tsKeyTable,
                fsDirTable,
                tsDirTable,
                objectIdToIsDirMap,
                objectIdToKeyNameMapForFromSnapshot,
                objectIdToKeyNameMapForToSnapshot,
                volumeName, bucketName,
                fromSnapshotName, toSnapshotName,
                bucketLayout.isFileSystemOptimized(), oldParentIdPathMap,
                newParentIdPathMap, tablePrefixes);
            // If job is cancelled, totalDiffEntries will be equal to -1.
            if (totalDiffEntries >= 0 &&
                areDiffJobAndSnapshotsActive(volumeName, bucketName,
                    fromSnapshotName, toSnapshotName)) {
              updateJobStatusToDone(jobKey, totalDiffEntries);
            }
            return null;
          }
      };

      // Check if the job is cancelled, before every method call.
      for (Callable<Void> methodCall : methodCalls) {
        if (!areDiffJobAndSnapshotsActive(volumeName, bucketName,
            fromSnapshotName, toSnapshotName)) {
          return;
        }
        methodCall.call();
      }
    } catch (IOException | RocksDBException exception) {
      updateJobStatusToFailed(jobKey, exception.getMessage());
      LOG.error("Caught checked exception during diff report generation for " +
              "volume: {} bucket: {}, fromSnapshot: {} and toSnapshot: {}",
          volumeName, bucketName, fromSnapshotName, toSnapshotName, exception);
      // TODO: [SNAPSHOT] Fail gracefully. Also check if it is even needed to
      //  throw this exception.
      throw new RuntimeException(exception);
    } catch (Exception exception) {
      updateJobStatusToFailed(jobKey, exception.getMessage());
      LOG.error("Caught unchecked exception during diff report generation " +
              "for volume: {} bucket: {}, fromSnapshot: {} and toSnapshot: {}",
          volumeName, bucketName, fromSnapshotName, toSnapshotName, exception);
      // TODO: [SNAPSHOT] Fail gracefully. Also check if it is even needed to
      //  throw this exception.
      throw new RuntimeException(exception);
    } finally {
      // Clean up: drop the intermediate column family and close them.
      dropAndCloseColumnFamilyHandle(fromSnapshotColumnFamily);
      dropAndCloseColumnFamilyHandle(toSnapshotColumnFamily);
      dropAndCloseColumnFamilyHandle(objectIDsColumnFamily);
      // Decrement ref counts
      if (rcFromSnapshot != null) {
        rcFromSnapshot.close();
      }
      if (rcToSnapshot != null) {
        rcToSnapshot.close();
      }
    }
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private void getDeltaFilesAndDiffKeysToObjectIdToKeyMap(
      final Table<String, ? extends WithParentObjectId> fsTable,
      final Table<String, ? extends WithParentObjectId> tsTable,
      final SnapshotInfo fsInfo, final SnapshotInfo tsInfo,
      boolean skipNativeDiff, final TablePrefixInfo tablePrefixes,
      final PersistentMap<byte[], byte[]> oldObjIdToKeyMap,
      final PersistentMap<byte[], byte[]> newObjIdToKeyMap,
      final PersistentMap<byte[], Boolean> objectIdToIsDirMap,
      final Optional<Set<Long>> oldParentIds, final Optional<Set<Long>> newParentIds,
      final DeltaFileComputer deltaFileComputer, final String jobKey) throws IOException, RocksDBException {

    Set<String> tablesToLookUp = Collections.singleton(fsTable.getName());
    Collection<Pair<Path, SstFileInfo>> deltaFiles = deltaFileComputer.getDeltaFiles(fsInfo, tsInfo,
        tablesToLookUp);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Computed Delta SST File Set, Total count = {} ", deltaFiles.size());
    }
    addToObjectIdMap(fsTable, tsTable, deltaFiles.stream().map(Pair::getLeft).collect(Collectors.toList()),
        !skipNativeDiff, oldObjIdToKeyMap, newObjIdToKeyMap, objectIdToIsDirMap, oldParentIds,
        newParentIds, tablePrefixes, jobKey);
  }

  @VisibleForTesting
  @SuppressWarnings("checkstyle:ParameterNumber")
  void addToObjectIdMap(Table<String, ? extends WithParentObjectId> fsTable,
      Table<String, ? extends WithParentObjectId> tsTable,
      Collection<Path> deltaFiles, boolean nativeRocksToolsLoaded,
      PersistentMap<byte[], byte[]> oldObjIdToKeyMap,
      PersistentMap<byte[], byte[]> newObjIdToKeyMap,
      PersistentMap<byte[], Boolean> objectIdToIsDirMap,
      Optional<Set<Long>> oldParentIds,
      Optional<Set<Long>> newParentIds,
      TablePrefixInfo tablePrefixes, String jobKey) throws IOException, RocksDBException {
    if (deltaFiles.isEmpty()) {
      return;
    }
    String tablePrefix = tablePrefixes.getTablePrefix(fsTable.getName());
    boolean isDirectoryTable = fsTable.getName().equals(DIRECTORY_TABLE);
    SstFileSetReader sstFileReader = new SstFileSetReader(deltaFiles);
    validateEstimatedKeyChangesAreInLimits(sstFileReader);
    long totalEstimatedKeysToProcess = sstFileReader.getEstimatedTotalKeys();
    String sstFileReaderLowerBound = tablePrefix;
    String sstFileReaderUpperBound = null;
    double stepIncreasePct = 0.1;
    double[] checkpoint = new double[1];
    checkpoint[0] = stepIncreasePct;
    if (Strings.isNotEmpty(tablePrefix)) {
      sstFileReaderUpperBound = getLexicographicallyHigherString(tablePrefix);
    }
    try (ClosableIterator<String> keysToCheck = nativeRocksToolsLoaded ?
        sstFileReader.getKeyStreamWithTombstone(sstFileReaderLowerBound, sstFileReaderUpperBound)
        : sstFileReader.getKeyStream(sstFileReaderLowerBound, sstFileReaderUpperBound);
         TableMergeIterator<String, WithParentObjectId> tableMergeIterator = new TableMergeIterator<>(keysToCheck,
             tablePrefix, (Table<String, WithParentObjectId>) fsTable, (Table<String, WithParentObjectId>) tsTable)) {
      AtomicLong keysProcessed = new AtomicLong(0);
      while (tableMergeIterator.hasNext()) {
        Table.KeyValue<String, List<WithParentObjectId>> kvs = tableMergeIterator.next();
        String key = kvs.getKey();
        if (totalEstimatedKeysToProcess > 0) {
          double progressPct = (double) keysProcessed.get() / totalEstimatedKeysToProcess;
          if (progressPct >= checkpoint[0]) {
            updateProgress(jobKey, progressPct);
            checkpoint[0] += stepIncreasePct;
          }
        }

        try {
          final WithParentObjectId fromObjectId = kvs.getValue().get(0);
          final WithParentObjectId toObjectId = kvs.getValue().get(1);
          if (areKeysEqual(fromObjectId, toObjectId)) {
            keysProcessed.getAndIncrement();
            continue;
          }
          if (fromObjectId != null) {
            byte[] rawObjId = codecRegistry.asRawData(fromObjectId.getObjectID());
            // Removing volume bucket info by removing the table bucket Prefix
            // from the key.
            // For FSO buckets will be left with the parent id/keyname.
            // For OBS buckets will be left with the complete path
            byte[] rawValue = codecRegistry.asRawData(
                key.substring(tablePrefix.length()));
            oldObjIdToKeyMap.put(rawObjId, rawValue);
            objectIdToIsDirMap.put(rawObjId, isDirectoryTable);
            oldParentIds.ifPresent(set -> set.add(
                fromObjectId.getParentObjectID()));
          }
          if (toObjectId != null) {
            byte[] rawObjId = codecRegistry.asRawData(toObjectId.getObjectID());
            byte[] rawValue = codecRegistry.asRawData(key.substring(tablePrefix.length()));
            newObjIdToKeyMap.put(rawObjId, rawValue);
            objectIdToIsDirMap.put(rawObjId, isDirectoryTable);
            newParentIds.ifPresent(set -> set.add(toObjectId
                .getParentObjectID()));
          }
          keysProcessed.getAndIncrement();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private void validateEstimatedKeyChangesAreInLimits(
      SstFileSetReader sstFileReader
  ) throws IOException {
    if (sstFileReader.getEstimatedTotalKeys() >
        maxAllowedKeyChangesForASnapDiff) {
      // TODO: [SNAPSHOT] HDDS-8202: Change it to custom snapshot exception.
      throw new IOException(
          String.format("Expected diff contains more than max allowed key " +
                  "changes for a snapDiff job. EstimatedTotalKeys: %s, " +
                  "AllowMaxTotalKeys: %s.",
              sstFileReader.getEstimatedTotalKeys(),
              maxAllowedKeyChangesForASnapDiff));
    }
  }

  private String resolveBucketRelativePath(boolean isFSOBucket,
      final Optional<Map<Long, Path>> parentIdMap, byte[] keyVal,
      boolean skipUnresolvedObjIds)
      throws IOException {
    String key = codecRegistry.asObject(keyVal, String.class);
    if (isFSOBucket) {
      String[] splitKey = key.split(OM_KEY_PREFIX, 2);
      Long parentId = Long.valueOf(splitKey[0]);
      if (parentIdMap.map(m -> !m.containsKey(parentId)).orElse(true)) {
        if (skipUnresolvedObjIds) {
          return null;
        } else {
          throw new IllegalStateException(String.format(
              "Cannot resolve path for key: %s with parent Id: %d", key,
              parentId));
        }

      }
      return parentIdMap.map(m -> m.get(parentId).resolve(splitKey[1]))
          .get().toString().substring(1);
    }
    return OzoneConsts.ROOT_PATH.resolve(key).toString()
        .substring(1);
  }

  @SuppressWarnings({"checkstyle:ParameterNumber", "checkstyle:MethodLength"})
  long generateDiffReport(
      final String jobId,
      final Table<String, OmKeyInfo> fsTable,
      final Table<String, OmKeyInfo> tsTable,
      final Table<String, OmDirectoryInfo> fsDirTable,
      final Table<String, OmDirectoryInfo> tsDirTable,
      final PersistentMap<byte[], Boolean> objectIdToIsDirMap,
      final PersistentMap<byte[], byte[]> oldObjIdToKeyMap,
      final PersistentMap<byte[], byte[]> newObjIdToKeyMap,
      final String volumeName,
      final String bucketName,
      final String fromSnapshotName,
      final String toSnapshotName,
      final boolean isFSOBucket,
      final Optional<Map<Long, Path>> oldParentIdPathMap,
      final Optional<Map<Long, Path>> newParentIdPathMap,
      final TablePrefixInfo tablePrefix) {
    LOG.info("Starting diff report generation for jobId: {}.", jobId);
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

      try (ClosableIterator<Map.Entry<byte[], Boolean>>
               iterator = objectIdToIsDirMap.iterator()) {
        // This counter is used, so that we can check every 100 elements
        // if the job is cancelled and snapshots are still active.
        int counter = 0;
        while (iterator.hasNext()) {
          if (counter % 100 == 0 &&
              !areDiffJobAndSnapshotsActive(volumeName, bucketName,
                  fromSnapshotName, toSnapshotName)) {
            return -1L;
          }

          Map.Entry<byte[], Boolean> nextEntry = iterator.next();
          byte[] id = nextEntry.getKey();
          boolean isDirectoryObject = nextEntry.getValue();

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
            String key = resolveBucketRelativePath(isFSOBucket,
                newParentIdPathMap, newKeyName, true);
            if (key != null) {
              DiffReportEntry entry =
                  SnapshotDiffReportOzone.getDiffReportEntry(CREATE, key);
              createDiffs.add(codecRegistry.asRawData(entry));
            }
          } else if (newKeyName == null) { // Key Deleted.
            String key = resolveBucketRelativePath(isFSOBucket,
                oldParentIdPathMap, oldKeyName, true);
            if (key != null) {
              DiffReportEntry entry =
                  SnapshotDiffReportOzone.getDiffReportEntry(DELETE, key);
              deleteDiffs.add(codecRegistry.asRawData(entry));
            }
          } else if (isDirectoryObject &&
              Arrays.equals(oldKeyName, newKeyName)) {
            String key = resolveBucketRelativePath(isFSOBucket,
                newParentIdPathMap, newKeyName, true);
            if (key != null) {
              DiffReportEntry entry =
                  SnapshotDiffReportOzone.getDiffReportEntry(MODIFY, key);
              modifyDiffs.add(codecRegistry.asRawData(entry));
            }
          } else {
            String keyPrefix = tablePrefix.getTablePrefix((isDirectoryObject ? fsDirTable : fsTable).getName());
            String oldKey = resolveBucketRelativePath(isFSOBucket,
                oldParentIdPathMap, oldKeyName, true);
            String newKey = resolveBucketRelativePath(isFSOBucket,
                newParentIdPathMap, newKeyName, true);
            if (oldKey == null && newKey == null) {
              // When both are unresolved then it means both keys are deleted. So no change for these objects.
              continue;
            } else if (oldKey == null) {
              // This should never happen where oldKey path is unresolved and new snapshot is resolved.
              throw new IllegalStateException(String.format("Old and new key resolved paths both are not null when " +
                      "oldKey is null for oldKey : %s newKey: %s", codecRegistry.asObject(oldKeyName, String.class),
                  codecRegistry.asObject(newKeyName, String.class)));
            } else if (newKey == null) {
              deleteDiffs.add(codecRegistry.asRawData(SnapshotDiffReportOzone
                  .getDiffReportEntry(DELETE, oldKey)));
            } else {
              // Check if block location is same or not. If it is not same,
              // key must have been overridden as well.
              boolean isObjectModified = isObjectModified(
                  keyPrefix + codecRegistry.asObject(oldKeyName, String.class),
                  keyPrefix + codecRegistry.asObject(newKeyName, String.class),
                  isDirectoryObject ? fsDirTable : fsTable,
                  isDirectoryObject ? tsDirTable : tsTable);
              if (isObjectModified) {
                // Here, oldKey name is returned as modified. Modified key name
                // is based on base snapshot (from snapshot).
                modifyDiffs.add(codecRegistry.asRawData(SnapshotDiffReportOzone
                    .getDiffReportEntry(MODIFY, oldKey)));
              }
              if (!isObjectModified || !Arrays.equals(oldKeyName, newKeyName)) {
                renameDiffs.add(codecRegistry.asRawData(
                    SnapshotDiffReportOzone.getDiffReportEntry(RENAME, oldKey,
                        newKey)));
              }
            }
          }
          counter++;
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

  /**
   * Checks if the key has been modified b/w snapshots.
   * @param fromKey Key info in source snapshot.
   * @param toKey Key info in target snapshot.
   * @return true if key is modified otherwise false.
   */
  private boolean isKeyModified(OmKeyInfo fromKey, OmKeyInfo toKey) {
    return !fromKey.isKeyInfoSame(toKey,
        false, false, false, false, true)
        || !SnapshotUtils.isBlockLocationInfoSame(fromKey, toKey);
  }

  private boolean isObjectModified(String fromObjectName, String toObjectName,
      final Table<String, ? extends WithObjectID> fromSnapshotTable,
      final Table<String, ? extends WithObjectID> toSnapshotTable)
      throws IOException {
    Objects.requireNonNull(fromObjectName, "fromObjectName is null.");
    Objects.requireNonNull(toObjectName, "toObjectName is null.");

    final WithObjectID fromObject = fromSnapshotTable.get(fromObjectName);
    final WithObjectID toObject = toSnapshotTable.get(toObjectName);
    if ((fromObject instanceof OmKeyInfo) && (toObject instanceof OmKeyInfo)) {
      return isKeyModified((OmKeyInfo) fromObject, (OmKeyInfo) toObject);
    } else if ((fromObject instanceof OmDirectoryInfo)
        && (toObject instanceof OmDirectoryInfo)) {
      return !areAclsSame((OmDirectoryInfo) fromObject,
          (OmDirectoryInfo) toObject);
    } else {
      throw new IllegalStateException("fromObject or toObject is not of " +
          "the expected type. fromObject type : " +
          fromObject.getClass().getName() + "toObject type: " +
          toObject.getClass().getName());
    }
  }

  private boolean areAclsSame(OmDirectoryInfo fromObject,
                              OmDirectoryInfo toObject) {
    return fromObject.getAcls().equals(toObject.getAcls());
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
        snapDiffReportTable.put(codecRegistry.asRawData(
            getReportKeyForIndex(jobId, index)), diffReportIterator.next());
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
      throw new IllegalStateException("Invalid job status for jobID: " +
          snapshotDiffJob.getJobId() + ". Job's current status is '" +
          snapshotDiffJob.getStatus() + "', while '" + oldStatus +
          "' is expected.");
    }
    snapshotDiffJob.setStatus(newStatus);
    snapDiffJobTable.put(jobKey, snapshotDiffJob);
  }

  synchronized void recordActivity(String jobKey,
      SubStatus subStatus) {
    SnapshotDiffJob snapshotDiffJob = snapDiffJobTable.get(jobKey);
    snapshotDiffJob.setSubStatus(subStatus);
    snapDiffJobTable.put(jobKey, snapshotDiffJob);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Snapshot Diff for jobKey = {} transitions to {} state", jobKey, subStatus);
    }
  }

  synchronized void updateProgress(String jobKey,
      double pct) {
    SnapshotDiffJob snapshotDiffJob = snapDiffJobTable.get(jobKey);
    snapshotDiffJob.setKeysProcessedPct(pct * 100);
    snapDiffJobTable.put(jobKey, snapshotDiffJob);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Completed processing {}% of keys for snapshot diff job {}", pct, jobKey);
    }
  }

  private synchronized void updateJobStatusToFailed(String jobKey,
                                                    String reason) {
    SnapshotDiffJob snapshotDiffJob = snapDiffJobTable.get(jobKey);
    if (snapshotDiffJob.getStatus() != IN_PROGRESS) {
      throw new IllegalStateException("Invalid job status for jobID: " +
          snapshotDiffJob.getJobId() + ". Job's current status is '" +
          snapshotDiffJob.getStatus() + "', while '" + IN_PROGRESS +
          "' is expected.");
    }
    snapshotDiffJob.setStatus(FAILED);
    if (org.apache.commons.lang3.StringUtils.isNotEmpty(reason)) {
      snapshotDiffJob.setReason(reason);
    } else {
      // TODO: [Snapshot] Revisit this when we have proper exception handling.
      snapshotDiffJob.setReason("Job failed due to unknown reason.");
    }
    ozoneManager.getMetrics().incNumSnapshotDiffJobFails();
    snapDiffJobTable.put(jobKey, snapshotDiffJob);
  }

  private synchronized void updateJobStatusToDone(String jobKey,
                                                  long totalNumberOfEntries) {
    SnapshotDiffJob snapshotDiffJob = snapDiffJobTable.get(jobKey);
    if (snapshotDiffJob.getStatus() != IN_PROGRESS) {
      throw new IllegalStateException("Invalid job status for jobID: " +
          snapshotDiffJob.getJobId() + ". Job's current status is '" +
          snapshotDiffJob.getStatus() + "', while '" + IN_PROGRESS +
          "' is expected.");
    }

    snapshotDiffJob.setStatus(DONE);
    snapshotDiffJob.setTotalDiffEntries(totalNumberOfEntries);
    snapDiffJobTable.put(jobKey, snapshotDiffJob);
  }

  @VisibleForTesting
  protected BucketLayout getBucketLayout(final String volume,
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
  boolean isKeyInBucket(String key, TablePrefixInfo tablePrefixInfo,
                        String tableName) {
    return key.startsWith(tablePrefixInfo.getTablePrefix(tableName));
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
  @VisibleForTesting
  void loadJobsOnStartUp() {

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

          submitSnapDiffJob(jobKey,
              snapshotDiffJob.getJobId(),
              snapshotDiffJob.getVolume(),
              snapshotDiffJob.getBucket(),
              snapshotDiffJob.getFromSnapshot(),
              snapshotDiffJob.getToSnapshot(),
              snapshotDiffJob.isForceFullDiff(),
              snapshotDiffJob.isNativeDiffDisabled());
        }
      }
    }
  }

  @Override
  public void close() {
    if (snapDiffExecutor != null) {
      closeExecutorService(snapDiffExecutor, "SnapDiffExecutor");
    }
  }

  private void closeExecutorService(ExecutorService executorService,
                                    String serviceName) {
    if (executorService != null) {
      LOG.info("Shutting down executorService: '{}'", serviceName);
      executorService.shutdownNow();
      try {
        if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        // Re-interrupt the thread while catching InterruptedException
        Thread.currentThread().interrupt();
        executorService.shutdownNow();
      }
    }
  }
}
