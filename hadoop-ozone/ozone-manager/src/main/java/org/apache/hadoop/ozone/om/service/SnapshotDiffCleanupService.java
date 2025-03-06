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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_JOB_REPORT_PERSISTENT_TIME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_JOB_REPORT_PERSISTENT_TIME_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_MAX_JOBS_PURGE_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_MAX_JOBS_PURGE_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.DELIMITER;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.CANCELLED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.FAILED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.REJECTED;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotDiffJob;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * Background service to clean-up snapDiff jobs which are stable and
 * corresponding reports.
 */
public class SnapshotDiffCleanupService extends BackgroundService {
  // Use only a single thread for Snapshot Diff cleanup.
  // Multiple threads would read from the same table and can send deletion
  // requests for same snapshot diff job multiple times.
  private static final int SNAPSHOT_DIFF_CLEANUP_CORE_POOL_SIZE = 1;

  private final AtomicBoolean suspended;
  private final AtomicLong runCount;
  private final AtomicLong successRunCount;
  private final ManagedRocksDB db;
  private final ColumnFamilyHandle snapDiffJobCfh;
  private final ColumnFamilyHandle snapDiffPurgedJobCfh;
  private final ColumnFamilyHandle snapDiffReportCfh;
  private final CodecRegistry codecRegistry;

  /**
   * Maximum numbers of snapDiff jobs to be purged per clean-up task run.
   */
  private final long maxJobToPurgePerTask;

  /**
   * Maximum time a snapDiff job and corresponding report will be persisted.
   */
  private final long maxAllowedTime;

  @SuppressWarnings("parameternumber")
  public SnapshotDiffCleanupService(long interval,
                                    long serviceTimeout,
                                    OzoneManager ozoneManager,
                                    ManagedRocksDB db,
                                    ColumnFamilyHandle snapDiffJobCfh,
                                    ColumnFamilyHandle snapDiffPurgedJobCfh,
                                    ColumnFamilyHandle snapDiffReportCfh,
                                    CodecRegistry codecRegistry) {
    super(SnapshotDiffCleanupService.class.getSimpleName(),
        interval,
        TimeUnit.MILLISECONDS, SNAPSHOT_DIFF_CLEANUP_CORE_POOL_SIZE,
        serviceTimeout, ozoneManager.getThreadNamePrefix());
    this.suspended = new AtomicBoolean(false);
    this.runCount = new AtomicLong(0);
    this.successRunCount = new AtomicLong(0);
    this.db = db;
    this.snapDiffJobCfh = snapDiffJobCfh;
    this.snapDiffPurgedJobCfh = snapDiffPurgedJobCfh;
    this.snapDiffReportCfh = snapDiffReportCfh;
    this.codecRegistry = codecRegistry;
    this.maxJobToPurgePerTask = ozoneManager.getConfiguration().getLong(
        OZONE_OM_SNAPSHOT_DIFF_MAX_JOBS_PURGE_PER_TASK,
        OZONE_OM_SNAPSHOT_DIFF_MAX_JOBS_PURGE_PER_TASK_DEFAULT
    );
    this.maxAllowedTime = ozoneManager.getConfiguration().getTimeDuration(
        OZONE_OM_SNAPSHOT_DIFF_JOB_REPORT_PERSISTENT_TIME,
        OZONE_OM_SNAPSHOT_DIFF_JOB_REPORT_PERSISTENT_TIME_DEFAULT,
        TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  public void run() {
    // Remove the job report first and then move snapDiff job from active
    // job table to purge job table. It is done this way, to not deal with
    // synchronization of snapDiff request and clean up. If entries are
    // moved from active job table to purge table first and then corresponding
    // report is removed, there could be a case when snapDiff request saw a
    // done job in active job table, but their report was deleted by clean up
    // service.
    // In clean report table first and them move jobs to purge table approach,
    // assumption is that by the next cleanup run, there is no purged snapDiff
    // job reading from report table.
    removeOlderJobReport();
    moveOldSnapDiffJobsToPurgeTable();
  }

  @VisibleForTesting
  public byte[] getEntryFromPurgedJobTable(String jobId) {
    try {
      return db.get().get(snapDiffPurgedJobCfh,
          codecRegistry.asRawData(jobId));
    } catch (IOException | RocksDBException e) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(e);
    }
  }

  /**
   * Move the snapDiff jobs from snapDiffJobTable to purge table which are
   * older than the allowed time or have FAILED or REJECTED status.
   * For jobs older than {@link SnapshotDiffCleanupService#maxAllowedTime},
   * we don't care if they are in QUEUED/IN_PROGRESS state.
   * Reason: there could be stale QUEUED/IN_PROGRESS jobs because of OM
   * crashing or leader node becoming follower.
   * The other assumption here is that no snapDiff job takes equal or more time
   * than the {@link SnapshotDiffCleanupService#maxAllowedTime}.
   * `maxAllowedTime` is the time, a snapDiff job and its report is persisted.
   */
  private void moveOldSnapDiffJobsToPurgeTable() {
    try (ManagedRocksIterator iterator =
             new ManagedRocksIterator(db.get().newIterator(snapDiffJobCfh));
         ManagedWriteBatch writeBatch = new ManagedWriteBatch();
         ManagedWriteOptions writeOptions = new ManagedWriteOptions()) {
      long currentTimeMillis = System.currentTimeMillis();
      long purgeJobCount = 0;
      iterator.get().seekToFirst();

      while (iterator.get().isValid() && purgeJobCount < maxJobToPurgePerTask) {
        byte[] keyBytes = iterator.get().key();
        byte[] snapInfoBytes = iterator.get().value();
        iterator.get().next();

        SnapshotDiffJob snapDiffJob = codecRegistry.asObject(snapInfoBytes,
            SnapshotDiffJob.class);

        if (currentTimeMillis - snapDiffJob.getCreationTime() > maxAllowedTime
            || snapDiffJob.getStatus() == FAILED
            || snapDiffJob.getStatus() == REJECTED
            || snapDiffJob.getStatus() == CANCELLED) {

          writeBatch.put(snapDiffPurgedJobCfh,
              codecRegistry.asRawData(snapDiffJob.getJobId()),
              codecRegistry.asRawData(snapDiffJob.getTotalDiffEntries()));
          writeBatch.delete(snapDiffJobCfh, keyBytes);
          purgeJobCount++;
        }
      }

      db.get().write(writeOptions, writeBatch);
    } catch (IOException | RocksDBException e) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(e);
    }
  }

  private void removeOlderJobReport() {
    try (ManagedRocksIterator rocksIterator = new ManagedRocksIterator(
        db.get().newIterator(snapDiffPurgedJobCfh));
         ManagedWriteBatch writeBatch = new ManagedWriteBatch();
         ManagedWriteOptions writeOptions = new ManagedWriteOptions()) {
      rocksIterator.get().seekToFirst();
      while (rocksIterator.get().isValid()) {
        byte[] key = rocksIterator.get().key();
        byte[] value = rocksIterator.get().value();
        rocksIterator.get().next();
        String prefix = codecRegistry.asObject(key, String.class);
        long totalNumberOfEntries = codecRegistry.asObject(value, Long.class);

        if (totalNumberOfEntries > 0) {
          byte[] beginKey = codecRegistry.asRawData(prefix + DELIMITER + 0);
          byte[] endKey = codecRegistry.asRawData(prefix + DELIMITER +
              (totalNumberOfEntries - 1));
          // Delete Range excludes the endKey.
          // Hence, we do two delete,
          //  1. deleteRange form beginKey(included) to endKey(excluded).
          //  2. delete endKey.
          writeBatch.deleteRange(snapDiffReportCfh, beginKey, endKey);
          writeBatch.delete(snapDiffReportCfh, endKey);
        }
        // Finally, remove the entry from the purged job table.
        writeBatch.delete(snapDiffPurgedJobCfh, key);
      }
      db.get().write(writeOptions, writeBatch);
    } catch (IOException | RocksDBException e) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(e);
    }
  }

  private class SnapshotDiffCleanUpTask implements BackgroundTask {

    @Override
    public BackgroundTaskResult call() {
      if (!shouldRun()) {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }

      runCount.incrementAndGet();
      run();
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new SnapshotDiffCleanupService.SnapshotDiffCleanUpTask());
    return queue;
  }

  private boolean shouldRun() {
    // TODO: [SNAPSHOT] Add OzoneManager.isLeaderReady() check along with
    //  suspended. `isLeaderReady` check was removed because some unit tests
    //  were failing due to Mockito limitation. Remove this once unit tests
    //  or mocking are fixed.
    return !suspended.get();
  }

  public long getRunCount() {
    return runCount.get();
  }

  public long getSuccessfulRunCount() {
    return successRunCount.get();
  }

  @VisibleForTesting
  void suspend() {
    suspended.set(true);
  }

  @VisibleForTesting
  void resume() {
    suspended.set(false);
  }
}
