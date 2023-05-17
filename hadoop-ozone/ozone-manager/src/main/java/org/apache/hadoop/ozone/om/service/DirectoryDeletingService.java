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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgePathRequest;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

  private static ClientId clientId = ClientId.randomId();

  // Use only a single thread for DirDeletion. Multiple threads would read
  // or write to same tables and can send deletion requests for same key
  // multiple times.
  private static final int DIR_DELETING_CORE_POOL_SIZE = 1;

  // Number of items(dirs/files) to be batched in an iteration.
  private final long pathLimitPerTask;

  public DirectoryDeletingService(long interval, TimeUnit unit,
      long serviceTimeout, OzoneManager ozoneManager,
      OzoneConfiguration configuration) {
    super(KeyDeletingService.class.getSimpleName(), interval, unit,
        DIR_DELETING_CORE_POOL_SIZE, serviceTimeout, ozoneManager, null);
    this.pathLimitPerTask = configuration
        .getInt(OZONE_PATH_DELETING_LIMIT_PER_TASK,
            OZONE_PATH_DELETING_LIMIT_PER_TASK_DEFAULT);
  }

  private boolean shouldRun() {
    if (getOzoneManager() == null) {
      // OzoneManager can be null for testing
      return true;
    }
    return getOzoneManager().isLeaderReady();
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
        List<PurgePathRequest> purgePathRequestList = new ArrayList<>();
        List<Pair<String, OmKeyInfo>> allSubDirList
            = new ArrayList<>((int) remainNum);

        // TODO: [SNAPSHOT] HDDS-8067. Acquire deletedDirectoryTable write lock

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
            purgePathRequestList.add(request);
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

          optimizeDirDeletesAndSubmitRequest(remainNum, dirNum, subDirNum,
              subFileNum, allSubDirList, purgePathRequestList, null, startTime);

        } catch (IOException e) {
          LOG.error("Error while running delete directories and files " +
              "background task. Will retry at next run.", e);
        }
        // TODO: [SNAPSHOT] HDDS-8067. Release deletedDirectoryTable write lock
        //  in finally block
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

      OmSnapshot latestSnapshot =
          metadataManager.getLatestSnapshot(deletedDirInfo.getVolumeName(),
              deletedDirInfo.getBucketName(), omSnapshotManager);

      if (latestSnapshot != null) {
        Table<String, OmDirectoryInfo> prevDirTable =
            latestSnapshot.getMetadataManager().getDirectoryTable();
        // In OMKeyDeleteResponseWithFSO OzonePathKey is converted to
        // OzoneDeletePathKey. Changing it back to check the previous DirTable.
        String prevDbKey = metadataManager.getOzoneDeletePathDirKey(key);
        OmDirectoryInfo prevDirInfo = prevDirTable.get(prevDbKey);
        return prevDirInfo != null &&
            prevDirInfo.getObjectID() == deletedDirInfo.getObjectID();
      }
      return false;
    }
  }

}
