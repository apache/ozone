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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om;


import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.BooleanTriFunction;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.om.snapshot.SnapshotCache;
import org.apache.ozone.rocksdiff.RocksDiffUtils;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_SST_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_SST_DELETING_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.SNAPSHOT_LOCK;

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

  private final OzoneManager ozoneManager;

  // Number of files to be batched in an iteration.
  private final long snapshotLimitPerTask;

  private AtomicLong snapshotFilteredCount;

  private AtomicBoolean running;

  // Note: This filter only works till snapshots are readable only.
  // In the future, if snapshots are changed to writable as well,
  // this will need to be revisited.
  static final BooleanTriFunction<String, String, String, Boolean>
      FILTER_FUNCTION =
          (first, last, prefix) -> {
            String firstBucketKey = RocksDiffUtils.constructBucketKey(first);
            String lastBucketKey = RocksDiffUtils.constructBucketKey(last);
            return RocksDiffUtils
                .isKeyWithPrefixPresent(prefix, firstBucketKey, lastBucketKey);
          };

  public SstFilteringService(long interval, TimeUnit unit, long serviceTimeout,
      OzoneManager ozoneManager, OzoneConfiguration configuration) {
    super("SstFilteringService", interval, unit, SST_FILTERING_CORE_POOL_SIZE,
        serviceTimeout);
    this.ozoneManager = ozoneManager;
    this.snapshotLimitPerTask = configuration
        .getLong(SNAPSHOT_SST_DELETING_LIMIT_PER_TASK,
            SNAPSHOT_SST_DELETING_LIMIT_PER_TASK_DEFAULT);
    snapshotFilteredCount = new AtomicLong(0);
    running = new AtomicBoolean(false);
  }

  private final BootstrapStateHandler.Lock lock =
      new BootstrapStateHandler.Lock();

  @Override
  public void start() {
    running.set(true);
    super.start();
  }

  private class SstFilteringTask implements BackgroundTask {


    /**
     * Marks the SSTFiltered flag corresponding to the snapshot.
     * @param volume Volume name of the snapshot
     * @param bucket Bucket name of the snapshot
     * @param snapshotName Snapshot name
     * @throws IOException
     */
    private void markSSTFilteredFlagForSnapshot(String volume, String bucket,
        String snapshotName) throws IOException {
      boolean acquiredSnapshotLock = ozoneManager.getMetadataManager().getLock()
              .acquireWriteLock(SNAPSHOT_LOCK, volume, bucket, snapshotName);
      if (acquiredSnapshotLock) {
        Table<String, SnapshotInfo> snapshotInfoTable =
            ozoneManager.getMetadataManager().getSnapshotInfoTable();
        try {
          // mark the snapshot as filtered by writing to the file
          String snapshotTableKey = SnapshotInfo.getTableKey(volume, bucket,
              snapshotName);
          SnapshotInfo snapshotInfo = snapshotInfoTable.get(snapshotTableKey);

          snapshotInfo.setSstFiltered(true);
          snapshotInfoTable.put(snapshotTableKey, snapshotInfo);
        } finally {
          ozoneManager.getMetadataManager().getLock()
              .releaseWriteLock(SNAPSHOT_LOCK, volume, bucket, snapshotName);
        }
      }
    }

    @Override
    public BackgroundTaskResult call() throws Exception {

      Optional<SnapshotCache> snapshotCache = Optional.ofNullable(ozoneManager)
          .map(OzoneManager::getOmSnapshotManager)
          .map(OmSnapshotManager::getSnapshotCache);
      if (!snapshotCache.isPresent()) {
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

          if (snapshotInfo.isSstFiltered()) {
            continue;
          }

          LOG.debug("Processing snapshot {} to filter relevant SST Files",
              snapShotTableKey);

          List<Pair<String, String>> prefixPairs =
              constructPrefixPairs(snapshotInfo);

          try (ReferenceCounted<IOmMetadataReader, SnapshotCache>
                   snapshotMetadataReader = snapshotCache.get()
              .get(snapshotInfo.getTableKey())) {
            OmSnapshot omSnapshot = (OmSnapshot) snapshotMetadataReader.get();
            RDBStore rdbStore = (RDBStore) omSnapshot.getMetadataManager()
                .getStore();
            RocksDatabase db = rdbStore.getDb();
            try (BootstrapStateHandler.Lock lock =
                getBootstrapStateLock().lock()) {
              db.deleteFilesNotMatchingPrefix(prefixPairs, FILTER_FUNCTION);
            }
          }
          markSSTFilteredFlagForSnapshot(snapshotInfo.getVolumeName(),
              snapshotInfo.getBucketName(), snapshotInfo.getName());
          snapshotLimit--;
          snapshotFilteredCount.getAndIncrement();
        }
      } catch (RocksDBException | IOException e) {
        LOG.error("Error during Snapshot sst filtering ", e);
      }

      // nothing to return here
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }

    /**
     * @param snapshotInfo
     * @return a list of pairs (tableName,keyPrefix).
     * @throws IOException
     */
    private List<Pair<String, String>> constructPrefixPairs(
        SnapshotInfo snapshotInfo) throws IOException {
      String volumeName = snapshotInfo.getVolumeName();
      String bucketName = snapshotInfo.getBucketName();

      long volumeId = ozoneManager.getMetadataManager().getVolumeId(volumeName);
      // TODO : HDDS-6984  buckets can be deleted via ofs
      //  handle deletion of bucket case.
      long bucketId =
          ozoneManager.getMetadataManager().getBucketId(volumeName, bucketName);

      String filterPrefix =
          OM_KEY_PREFIX + volumeName + OM_KEY_PREFIX + bucketName
              + OM_KEY_PREFIX;

      String filterPrefixFSO =
          OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX + bucketId
              + OM_KEY_PREFIX;

      List<Pair<String, String>> prefixPairs = new ArrayList<>();
      prefixPairs
          .add(Pair.of(OmMetadataManagerImpl.KEY_TABLE, filterPrefix));
      prefixPairs.add(
          Pair.of(OmMetadataManagerImpl.DIRECTORY_TABLE, filterPrefixFSO));
      prefixPairs
          .add(Pair.of(OmMetadataManagerImpl.FILE_TABLE, filterPrefixFSO));
      return prefixPairs;
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
