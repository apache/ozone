/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.service;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgePathRequest;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_PATH_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_PATH_DELETING_LIMIT_PER_TASK_DEFAULT;

/**
 * This is a background service to delete orphan directories and its
 * sub paths(sub-dirs and sub-files).
 *
 * <p>
 * This will scan the metadata of om periodically to get the orphan dirs from
 * DeletedDirectoryTable and find its sub paths. It will fetch all sub-files
 * from FileTable and move those to DeletedTable so that OM's
 * KeyDeletingService will cleanup those files later. It will fetch all
 * sub-directories from the DirectoryTable and move those to
 * DeletedDirectoryTable so that these will be visited in next iterations.
 *
 * <p>
 * After moving all sub-files and sub-dirs the parent orphan directory will be
 * deleted by this service. It will continue traversing until all the leaf path
 * components of an orphan directory is visited.
 */
public class DirectoryDeletingService extends AbstractKeyDeletingService {
  public static final Logger LOG =
      LoggerFactory.getLogger(DirectoryDeletingService.class);

  // Use only a single thread for DirDeletion. Multiple threads would read
  // or write to same tables and can send deletion requests for same key
  // multiple times.
  private static final int DIR_DELETING_CORE_POOL_SIZE = 1;
  private static final int MIN_ERR_LIMIT_PER_TASK = 1000;

  // Number of items(dirs/files) to be batched in an iteration.
  private final long pathLimitPerTask;
  private final int ratisByteLimit;
  private final AtomicBoolean suspended;

  public DirectoryDeletingService(long interval, TimeUnit unit,
      long serviceTimeout, OzoneManager ozoneManager,
      OzoneConfiguration configuration) {
    super(DirectoryDeletingService.class.getSimpleName(), interval, unit,
        DIR_DELETING_CORE_POOL_SIZE, serviceTimeout, ozoneManager, null);
    this.pathLimitPerTask = configuration
        .getInt(OZONE_PATH_DELETING_LIMIT_PER_TASK,
            OZONE_PATH_DELETING_LIMIT_PER_TASK_DEFAULT);
    int limit = (int) configuration.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    // always go to 90% of max limit for request as other header will be added
    this.ratisByteLimit = (int) (limit * 0.9);
    this.suspended = new AtomicBoolean(false);
  }

  private boolean shouldRun() {
    if (getOzoneManager() == null) {
      // OzoneManager can be null for testing
      return true;
    }
    return getOzoneManager().isLeaderReady() && !suspended.get();
  }

  /**
   * Suspend the service.
   */
  @VisibleForTesting
  public void suspend() {
    suspended.set(true);
  }

  /**
   * Resume the service if suspended.
   */
  @VisibleForTesting
  public void resume() {
    suspended.set(false);
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new DirectoryDeletingService.DirDeletingTask());
    return queue;
  }

  private class DirDeletingTask implements BackgroundTask {

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() {
      if (shouldRun()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Running DirectoryDeletingService");
        }
        getRunCount().incrementAndGet();
        long dirNum = 0L;
        long subDirNum = 0L;
        long subFileNum = 0L;
        long remainNum = pathLimitPerTask;
        int consumedSize = 0;
        List<PurgePathRequest> purgePathRequestList = new ArrayList<>();
        List<Pair<String, OmKeyInfo>> allSubDirList
            = new ArrayList<>((int) remainNum);

        // Acquire active DB deletedDirectoryTable write lock because of the
        // deletedDirTable read-write here to avoid interleaving with
        // the table range delete operation in createOmSnapshotCheckpoint()
        // that is called from OMSnapshotCreateResponse#addToDBBatch.
        getOzoneManager().getMetadataManager().getTableLock(
            OmMetadataManagerImpl.DELETED_DIR_TABLE).writeLock().lock();

        Table.KeyValue<String, OmKeyInfo> pendingDeletedDirInfo;
        try (TableIterator<String, ? extends KeyValue<String, OmKeyInfo>>
                 deleteTableIterator = getOzoneManager().getMetadataManager().
            getDeletedDirTable().iterator()) {

          long startTime = Time.monotonicNow();
          while (remainNum > 0 && deleteTableIterator.hasNext()) {
            pendingDeletedDirInfo = deleteTableIterator.next();
            // Do not reclaim if the directory is still being referenced by
            // the previous snapshot.
            if (previousSnapshotHasDir(pendingDeletedDirInfo)) {
              continue;
            }

            PurgePathRequest request = prepareDeleteDirRequest(
                remainNum, pendingDeletedDirInfo.getValue(),
                pendingDeletedDirInfo.getKey(), allSubDirList,
                getOzoneManager().getKeyManager());
            if (isBufferLimitCrossed(ratisByteLimit, consumedSize,
                request.getSerializedSize())) {
              if (purgePathRequestList.size() != 0) {
                // if message buffer reaches max limit, avoid sending further
                remainNum = 0;
                break;
              }
              // if directory itself is having a lot of keys / files,
              // reduce capacity to minimum level
              remainNum = MIN_ERR_LIMIT_PER_TASK;
              request = prepareDeleteDirRequest(
                  remainNum, pendingDeletedDirInfo.getValue(),
                  pendingDeletedDirInfo.getKey(), allSubDirList,
                  getOzoneManager().getKeyManager());
            }
            consumedSize += request.getSerializedSize();
            purgePathRequestList.add(request);
            // reduce remain count for self, sub-files, and sub-directories
            remainNum = remainNum - 1;
            remainNum = remainNum - request.getDeletedSubFilesCount();
            remainNum = remainNum - request.getMarkDeletedSubDirsCount();
            // Count up the purgeDeletedDir, subDirs and subFiles
            if (request.getDeletedDir() != null
                && !request.getDeletedDir().isEmpty()) {
              dirNum++;
            }
            subDirNum += request.getMarkDeletedSubDirsCount();
            subFileNum += request.getDeletedSubFilesCount();
          }

          optimizeDirDeletesAndSubmitRequest(
              remainNum, dirNum, subDirNum, subFileNum,
              allSubDirList, purgePathRequestList, null, startTime,
              ratisByteLimit - consumedSize,
              getOzoneManager().getKeyManager());

        } catch (IOException e) {
          LOG.error("Error while running delete directories and files " +
              "background task. Will retry at next run.", e);
        } finally {
          // Release deletedDirectoryTable write lock
          getOzoneManager().getMetadataManager().getTableLock(
              OmMetadataManagerImpl.DELETED_DIR_TABLE).writeLock().unlock();
        }
      }

      // place holder by returning empty results of this call back.
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }

    private boolean previousSnapshotHasDir(
        KeyValue<String, OmKeyInfo> pendingDeletedDirInfo) throws IOException {
      String key = pendingDeletedDirInfo.getKey();
      OmKeyInfo deletedDirInfo = pendingDeletedDirInfo.getValue();
      OmSnapshotManager omSnapshotManager =
          getOzoneManager().getOmSnapshotManager();
      OmMetadataManagerImpl metadataManager = (OmMetadataManagerImpl)
          getOzoneManager().getMetadataManager();

      try (ReferenceCounted<OmSnapshot> rcLatestSnapshot =
          metadataManager.getLatestActiveSnapshot(
              deletedDirInfo.getVolumeName(),
              deletedDirInfo.getBucketName(),
              omSnapshotManager)) {

        if (rcLatestSnapshot != null) {
          String dbRenameKey = metadataManager
              .getRenameKey(deletedDirInfo.getVolumeName(),
                  deletedDirInfo.getBucketName(), deletedDirInfo.getObjectID());
          Table<String, OmDirectoryInfo> prevDirTable =
              rcLatestSnapshot.get().getMetadataManager().getDirectoryTable();
          Table<String, OmKeyInfo> prevDeletedDirTable =
              rcLatestSnapshot.get().getMetadataManager().getDeletedDirTable();
          OmKeyInfo prevDeletedDirInfo = prevDeletedDirTable.get(key);
          if (prevDeletedDirInfo != null) {
            return true;
          }
          String prevDirTableDBKey = metadataManager.getSnapshotRenamedTable()
              .get(dbRenameKey);
          // In OMKeyDeleteResponseWithFSO OzonePathKey is converted to
          // OzoneDeletePathKey. Changing it back to check the previous DirTable
          String prevDbKey = prevDirTableDBKey == null ?
              metadataManager.getOzoneDeletePathDirKey(key) : prevDirTableDBKey;
          OmDirectoryInfo prevDirInfo = prevDirTable.get(prevDbKey);
          return prevDirInfo != null &&
              prevDirInfo.getObjectID() == deletedDirInfo.getObjectID();
        }
      }

      return false;
    }
  }

}
