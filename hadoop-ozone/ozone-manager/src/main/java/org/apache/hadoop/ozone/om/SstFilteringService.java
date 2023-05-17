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
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.ozone.rocksdiff.RocksDiffUtils;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.ozone.OzoneConsts.FILTERED_SNAPSHOTS;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_SST_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_SST_DELETING_LIMIT_PER_TASK_DEFAULT;

/**
 * When snapshots are taken, an entire snapshot of the
 * OM RocksDB is captured and it will contain SST files corresponding
 * to all volumes/buckets and keys and also have data from
 * all the tables (columnFamilies) defined in the rocksdb
 * This is a background service which will cleanup and filter out
 * all the irrelevant and safe to delete sst files that don't correspond
 * to the bucket on which the snapshot was taken.
 */
public class SstFilteringService extends BackgroundService {

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

  private BooleanTriFunction<String, String, String, Boolean> filterFunction =
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
  }

  private class SstFilteringTask implements BackgroundTask {

    @Override
    public BackgroundTaskResult call() throws Exception {

      Table<String, SnapshotInfo> snapshotInfoTable =
          ozoneManager.getMetadataManager().getSnapshotInfoTable();
      try (
          TableIterator<String, ? extends Table.KeyValue
              <String, SnapshotInfo>> iterator = snapshotInfoTable
              .iterator()) {
        iterator.seekToFirst();

        long snapshotLimit = snapshotLimitPerTask;

        while (iterator.hasNext() && snapshotLimit > 0) {
          Table.KeyValue<String, SnapshotInfo> keyValue = iterator.next();
          String snapShotTableKey = keyValue.getKey();
          SnapshotInfo snapshotInfo = keyValue.getValue();

          File omMetadataDir =
              OMStorage.getOmDbDir(ozoneManager.getConfiguration());
          String snapshotDir = omMetadataDir + OM_KEY_PREFIX + OM_SNAPSHOT_DIR;
          Path filePath =
              Paths.get(snapshotDir + OM_KEY_PREFIX + FILTERED_SNAPSHOTS);

          // If entry for the snapshotID is present in this file,
          // it has already undergone filtering.
          if (Files.exists(filePath)) {
            List<String> processedSnapshotIds = Files.readAllLines(filePath);
            if (processedSnapshotIds.contains(snapshotInfo.getSnapshotID())) {
              continue;
            }
          }

          LOG.debug("Processing snapshot {} to filter relevant SST Files",
              snapShotTableKey);

          List<Pair<String, String>> prefixPairs =
              constructPrefixPairs(snapshotInfo);

          String dbName = OM_DB_NAME + snapshotInfo.getCheckpointDirName();

          String snapshotCheckpointDir = omMetadataDir + OM_KEY_PREFIX +
              OM_SNAPSHOT_CHECKPOINT_DIR;
          try (RDBStore rdbStore = (RDBStore) OmMetadataManagerImpl
              .loadDB(ozoneManager.getConfiguration(),
                      new File(snapshotCheckpointDir),
                      dbName, true, Optional.of(Boolean.TRUE), false, false)) {
            RocksDatabase db = rdbStore.getDb();
            db.deleteFilesNotMatchingPrefix(prefixPairs, filterFunction);
          }

          // mark the snapshot as filtered by writing to the file
          String content = snapshotInfo.getSnapshotID() + "\n";
          Files.write(filePath, content.getBytes(StandardCharsets.UTF_8),
              StandardOpenOption.CREATE, StandardOpenOption.APPEND);
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
          OM_KEY_PREFIX + volumeName + OM_KEY_PREFIX + bucketName;

      String filterPrefixFSO =
          OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX + bucketId;

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

}
