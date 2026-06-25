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

import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_SST_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_SST_DELETING_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.BOOTSTRAP_LOCK;
import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.SNAPSHOT_DB_LOCK;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When snapshots are taken, an entire snapshot of the
 * OM RocksDB is captured and it will contain SST files corresponding
 * to all volumes/buckets and keys and also have data from
 * all the tables (columnFamilies) defined in the rocksdb
 * This is a background service which will cleanup and filter out
 * all the irrelevant and safe to delete sst files that don't correspond
 * to the bucket on which the snapshot was taken.
 */
public class SstFilteringService extends BackgroundService
    implements BootstrapStateHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(SstFilteringService.class);

  // Use only a single thread for SST deletion. Multiple threads would read
  // or write to same snapshots and can send deletion requests for same sst
  // multiple times.
  private static final int SST_FILTERING_CORE_POOL_SIZE = 1;

  public static final String SST_FILTERED_FILE = "sstFiltered";
  private static final byte[] SST_FILTERED_FILE_CONTENT = StringUtils.string2Bytes("This file holds information " +
      "if a particular snapshot has filtered out the relevant sst files or not.\nDO NOT add, change or delete " +
      "any files in this directory unless you know what you are doing.\n");
  private final OzoneManager ozoneManager;

  // Number of files to be batched in an iteration.
  private final long snapshotLimitPerTask;

  private AtomicLong snapshotFilteredCount;

  private AtomicBoolean running;

  private final BootstrapStateHandler.Lock lock;

  public static boolean isSstFiltered(OzoneConfiguration ozoneConfiguration, SnapshotInfo snapshotInfo) {
    Path sstFilteredFile = Paths.get(OmSnapshotManager.getSnapshotPath(ozoneConfiguration,
        snapshotInfo, 0), SST_FILTERED_FILE);
    return snapshotInfo.isSstFiltered() || sstFilteredFile.toFile().exists();
  }

  public SstFilteringService(long interval, TimeUnit unit, long serviceTimeout,
      OzoneManager ozoneManager, OzoneConfiguration configuration) {
    super("SstFilteringService", interval, unit, SST_FILTERING_CORE_POOL_SIZE,
        serviceTimeout, ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.snapshotLimitPerTask = configuration
        .getLong(SNAPSHOT_SST_DELETING_LIMIT_PER_TASK,
            SNAPSHOT_SST_DELETING_LIMIT_PER_TASK_DEFAULT);
    snapshotFilteredCount = new AtomicLong(0);
    running = new AtomicBoolean(false);
    IOzoneManagerLock ozoneManagerLock = ozoneManager.getMetadataManager().getLock();
    Function<Boolean, UncheckedAutoCloseable> lockSupplier = (readLock) ->
        ozoneManagerLock.acquireLock(BOOTSTRAP_LOCK, getServiceName(), readLock);
    this.lock = new BootstrapStateHandler.Lock(lockSupplier);
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

  private class SstFilteringTask implements BackgroundTask {

    private boolean isSnapshotDeleted(SnapshotInfo snapshotInfo) {
      return snapshotInfo == null || snapshotInfo.getSnapshotStatus() == SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED;
    }

    /**
     * Checks if the snapshot has been defragged.
     * @param snapshotInfo snapshotInfo
     * @return true if the snapshot has been defragged, false otherwise
     */
    private boolean isSnapshotDefragged(SnapshotInfo snapshotInfo) {
      try {
        OmSnapshotManager omSnapshotManager = ozoneManager.getOmSnapshotManager();
        if (omSnapshotManager == null) {
          return false;
        }
        OmSnapshotLocalDataManager localDataManager = omSnapshotManager.getSnapshotLocalDataManager();
        if (localDataManager == null) {
          return false;
        }
        try (OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataProvider provider =
                 localDataManager.getOmSnapshotLocalData(snapshotInfo)) {
          // If snapshot local data version is not 0, it means the snapshot has been defragged
          return provider.getVersion() > 0;
        }
      } catch (IOException e) {
        LOG.debug("Error checking if snapshot {} is defragged", snapshotInfo.getSnapshotId(), e);
        return false;
      }
    }

    /**
     * Marks the snapshot as SSTFiltered by creating a file in snapshot directory.
     * @param snapshotInfo snapshotInfo
     * @throws IOException
     */
    private void markSSTFilteredFlagForSnapshot(SnapshotInfo snapshotInfo) throws IOException {
      // Acquiring read lock to avoid race condition with the snapshot directory deletion occurring
      // in OmSnapshotPurgeResponse. Any operation apart from delete can run in parallel along with this operation.
      //TODO. Revisit other SNAPSHOT_LOCK and see if we can change write locks to read locks to further optimize it.
      OMLockDetails omLockDetails = ozoneManager.getMetadataManager().getLock()
          .acquireReadLock(SNAPSHOT_DB_LOCK, snapshotInfo.getSnapshotId().toString());
      boolean acquiredSnapshotLock = omLockDetails.isLockAcquired();
      if (acquiredSnapshotLock) {
        // Ensure snapshot is sstFiltered before defrag.
        String snapshotDir = OmSnapshotManager.getSnapshotPath(ozoneManager.getConfiguration(), snapshotInfo, 0);
        try {
          // mark the snapshot as filtered by creating a file.
          if (Files.exists(Paths.get(snapshotDir))) {
            Files.write(Paths.get(snapshotDir, SST_FILTERED_FILE), SST_FILTERED_FILE_CONTENT);
          }
        } finally {
          ozoneManager.getMetadataManager().getLock()
              .releaseReadLock(SNAPSHOT_DB_LOCK, snapshotInfo.getSnapshotId().toString());
        }
      }
    }

    @Override
    public BackgroundTaskResult call() throws Exception {

      Optional<OmSnapshotManager> snapshotManager = Optional.ofNullable(ozoneManager)
          .map(OzoneManager::getOmSnapshotManager);
      if (!snapshotManager.isPresent()) {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }
      Table<String, SnapshotInfo> snapshotInfoTable =
          ozoneManager.getMetadataManager().getSnapshotInfoTable();


      try (TableIterator<String, ? extends Table.KeyValue
              <String, SnapshotInfo>> iterator = snapshotInfoTable
              .iterator()) {
        iterator.seekToFirst();

        long snapshotLimit = snapshotLimitPerTask;

        while (iterator.hasNext() && snapshotLimit > 0 && running.get()) {
          Table.KeyValue<String, SnapshotInfo> keyValue = iterator.next();
          String snapShotTableKey = keyValue.getKey();
          SnapshotInfo snapshotInfo = keyValue.getValue();
          try {
            if (isSstFiltered(ozoneManager.getConfiguration(), snapshotInfo)) {
              continue;
            }

            // Skip defragged snapshots as defrag already performs filtering
            if (isSnapshotDefragged(snapshotInfo)) {
              LOG.debug("Skipping SST filtering for defragged snapshot: {}", snapShotTableKey);
              continue;
            }

            LOG.debug("Processing snapshot {} to filter relevant SST Files",
                snapShotTableKey);
            TablePrefixInfo bucketPrefixInfo =
                ozoneManager.getMetadataManager().getTableBucketPrefix(snapshotInfo.getVolumeName(),
                snapshotInfo.getBucketName());

            try (UncheckedAutoCloseable lock = getBootstrapStateLock().acquireReadLock();
                UncheckedAutoCloseableSupplier<OmSnapshot> snapshotMetadataReader =
                    snapshotManager.get().getActiveSnapshot(
                        snapshotInfo.getVolumeName(),
                        snapshotInfo.getBucketName(),
                        snapshotInfo.getName())) {
              OmSnapshot omSnapshot = snapshotMetadataReader.get();
              RDBStore rdbStore = (RDBStore) omSnapshot.getMetadataManager()
                  .getStore();
              RocksDatabase db = rdbStore.getDb();
              db.deleteFilesNotMatchingPrefix(bucketPrefixInfo);

              markSSTFilteredFlagForSnapshot(snapshotInfo);
              snapshotLimit--;
              snapshotFilteredCount.getAndIncrement();
            } catch (OMException ome) {
              // FILE_NOT_FOUND is obtained when the snapshot is deleted
              // In this case, get the snapshotInfo from the db, check if
              // it is deleted and if deleted mark it as sstFiltered.
              if (ome.getResult() == OMException.ResultCodes.FILE_NOT_FOUND) {
                SnapshotInfo snapshotInfoToCheck =
                    ozoneManager.getMetadataManager().getSnapshotInfoTable()
                        .get(snapShotTableKey);
                if (isSnapshotDeleted(snapshotInfoToCheck)) {
                  LOG.info("Snapshot with name: '{}', id: '{}' has been " +
                          "deleted.", snapshotInfo.getName(), snapshotInfo
                      .getSnapshotId());
                }
              }
            }
          } catch (IOException e) {
            if (isSnapshotDeleted(snapshotInfoTable.get(snapShotTableKey))) {
              LOG.info("Exception encountered while filtering a snapshot: {} since it was deleted midway",
                  snapShotTableKey, e);
            } else {
              LOG.error("Exception encountered while filtering a snapshot", e);
            }


          }
        }
      } catch (IOException e) {
        LOG.error("Error during Snapshot sst filtering ", e);
      }

      // nothing to return here
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new SstFilteringTask());
    return queue;
  }

  public AtomicLong getSnapshotFilteredCount() {
    return snapshotFilteredCount;
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
