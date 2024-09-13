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
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgePathRequest;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private OzoneManagerProtocolProtos.SetSnapshotPropertyRequest getSetSnapshotRequestUpdatingExclusiveSize(
        Map<String, Long> exclusiveSizeMap, Map<String, Long> exclusiveReplicatedSizeMap, String prevSnapshotKeyTable) {
      OzoneManagerProtocolProtos.SnapshotSize snapshotSize = OzoneManagerProtocolProtos.SnapshotSize.newBuilder()
          .setExclusiveSize(
              exclusiveSizeMap.getOrDefault(prevSnapshotKeyTable, 0L))
          .setExclusiveReplicatedSize(
              exclusiveReplicatedSizeMap.getOrDefault(
                  prevSnapshotKeyTable, 0L))
          .build();
      exclusiveSizeMap.remove(prevSnapshotKeyTable);
      exclusiveReplicatedSizeMap.remove(prevSnapshotKeyTable);

      return OzoneManagerProtocolProtos.SetSnapshotPropertyRequest.newBuilder()
          .setSnapshotKey(prevSnapshotKeyTable)
          .setSnapshotDirSize(snapshotSize)
          .build();
    }

    private OzoneManagerProtocolProtos.SetSnapshotPropertyRequest
        getSetSnapshotPropertyRequestupdatingDeepCleanSnapshotDir(String snapshotKeyTable) {
      return OzoneManagerProtocolProtos.SetSnapshotPropertyRequest.newBuilder()
          .setSnapshotKey(snapshotKeyTable)
          .setDeepCleanedDeletedDir(true)
          .build();
    }


    /**
     *
     * @param currentSnapshotInfo if null, deleted directories in AOS should be processed.
     * @param keyManager KeyManager of the underlying store.
     */
    private long processDeletedDirsForStore(SnapshotInfo currentSnapshotInfo, KeyManager keyManager,
                                            long remainNum) throws IOException {

      long dirNum = 0L;
      long subDirNum = 0L;
      long subFileNum = 0L;
      int consumedSize = 0;
      long initialRemainNum = remainNum;
      List<PurgePathRequest> purgePathRequestList = new ArrayList<>();
      List<Pair<String, OmKeyInfo>> allSubDirList
          = new ArrayList<>((int) remainNum);
      String volume = currentSnapshotInfo == null ? null : currentSnapshotInfo.getVolumeName();
      String bucket = currentSnapshotInfo == null ? null : currentSnapshotInfo.getBucketName();
      String snapshotTableKey = currentSnapshotInfo == null ? null : currentSnapshotInfo.getTableKey();
      Table.KeyValue<String, OmKeyInfo> pendingDeletedDirInfo;

      try (TableIterator<String, ? extends KeyValue<String, OmKeyInfo>>
               deletedDirsIterator = keyManager.getPendingDeletionDirs(volume, bucket)) {
        OmSnapshotManager omSnapshotManager = getOzoneManager().getOmSnapshotManager();
        SnapshotChainManager snapshotChainManager = ((OmMetadataManagerImpl)getOzoneManager().getMetadataManager())
            .getSnapshotChainManager();
        // This is to avoid race condition b/w purge request and snapshot chain updation. For AOS taking the global
        // snapshotId since AOS could process multiple buckets in one iteration. While using path previous snapshot
        // Id for a snapshot since it would process only one bucket.
        UUID expectedPreviousSnapshotId = currentSnapshotInfo == null ?
            snapshotChainManager.getLatestGlobalSnapshotId() :
            SnapshotUtils.getPreviousSnapshotId(currentSnapshotInfo, snapshotChainManager);
        IOzoneManagerLock lock = getOzoneManager().getMetadataManager().getLock();

        try (ReclaimableDirFilter reclaimableDirFilter = new ReclaimableDirFilter(omSnapshotManager,
            snapshotChainManager, currentSnapshotInfo, keyManager.getMetadataManager(), lock);
            ReclaimableKeyFilter reclaimableSubFileFilter = new ReclaimableKeyFilter(omSnapshotManager,
                snapshotChainManager, currentSnapshotInfo, keyManager.getMetadataManager(), lock)) {
          long startTime = Time.monotonicNow();
          while (remainNum > 0 && deletedDirsIterator.hasNext()) {
            pendingDeletedDirInfo = deletedDirsIterator.next();
            // Always perform listing on AOS.
            Optional<PurgePathRequest> request = prepareDeleteDirRequest(
                remainNum, pendingDeletedDirInfo.getValue(),
                pendingDeletedDirInfo.getKey(), allSubDirList,
                getOzoneManager().getKeyManager(), reclaimableDirFilter.apply(pendingDeletedDirInfo),
                reclaimableSubFileFilter);

            if (request.isPresent() && isBufferLimitCrossed(ratisByteLimit, consumedSize,
                request.get().getSerializedSize())) {
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
                  getOzoneManager().getKeyManager(), reclaimableDirFilter.apply(pendingDeletedDirInfo),
                  reclaimableSubFileFilter);
            }
            if (!request.isPresent()) {
              continue;
            }
            PurgePathRequest purgePathRequest = request.get();
            consumedSize += purgePathRequest.getSerializedSize();
            purgePathRequestList.add(purgePathRequest);
            remainNum = remainNum - purgePathRequest.getDeletedSubFilesCount();
            remainNum = remainNum - purgePathRequest.getMarkDeletedSubDirsCount();
            // Count up the purgeDeletedDir, subDirs and subFiles
            if (purgePathRequest.getDeletedDir() != null
                && !purgePathRequest.getDeletedDir().isEmpty()) {
              dirNum++;
            }
            subDirNum += purgePathRequest.getMarkDeletedSubDirsCount();
            subFileNum += purgePathRequest.getDeletedSubFilesCount();
          }

          Pair<Long, Optional<OzoneManagerProtocolProtos.OMResponse>> retVal = optimizeDirDeletesAndSubmitRequest(
              remainNum, dirNum, subDirNum, subFileNum,
              allSubDirList, purgePathRequestList, snapshotTableKey, startTime,
              ratisByteLimit - consumedSize,
              getOzoneManager().getKeyManager(), reclaimableDirFilter, reclaimableSubFileFilter,
              expectedPreviousSnapshotId);
          remainNum = retVal.getKey();

          if (remainNum == initialRemainNum &&
              retVal.getValue().map(OzoneManagerProtocolProtos.OMResponse::getSuccess).orElse(true)) {
            List<OzoneManagerProtocolProtos.SetSnapshotPropertyRequest> setSnapshotPropertyRequests = new ArrayList<>();
            Map<String, Long> exclusiveReplicatedSizeMap = reclaimableSubFileFilter.getExclusiveReplicatedSizeMap();
            Map<String, Long> exclusiveSizeMap = reclaimableSubFileFilter.getExclusiveSizeMap();
            for (String snapshot : Stream.of(exclusiveSizeMap.keySet(), exclusiveReplicatedSizeMap.keySet())
                .flatMap(Collection::stream).distinct().collect(Collectors.toList())) {
              setSnapshotPropertyRequests.add(getSetSnapshotRequestUpdatingExclusiveSize(exclusiveSizeMap,
                  exclusiveReplicatedSizeMap, snapshot));
            }
            //Updating directory deep clean flag of snapshot.
            if (currentSnapshotInfo != null) {
              setSnapshotPropertyRequests.add(getSetSnapshotPropertyRequestupdatingDeepCleanSnapshotDir(
                  currentSnapshotInfo.getTableKey()));
            }

            submitSetSnapshotRequest(setSnapshotPropertyRequests);
          }
        }

      } catch (IOException e) {
        throw e;
      }
      return remainNum;
    }

    @Override
    public BackgroundTaskResult call() {
      if (shouldRun()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Running DirectoryDeletingService");
        }
        getRunCount().incrementAndGet();
        long remainNum = pathLimitPerTask;
        try {
          remainNum = processDeletedDirsForStore(null, getOzoneManager().getKeyManager(),
              remainNum);
        } catch (IOException e) {
          LOG.error("Error while running delete directories and files " +
              "background task. Will retry at next run. on active object store", e);
        }

        if (remainNum > 0) {
          SnapshotChainManager chainManager =
              ((OmMetadataManagerImpl)getOzoneManager().getMetadataManager()).getSnapshotChainManager();
          OmSnapshotManager omSnapshotManager = getOzoneManager().getOmSnapshotManager();
          Iterator<UUID> iterator = null;
          try {
            iterator = chainManager.iterator(true);

          } catch (IOException e) {
            LOG.error("Error while initializing snapshot chain iterator.");
            return BackgroundTaskResult.EmptyTaskResult.newResult();
          }

          while (iterator.hasNext() && remainNum > 0) {
            UUID snapshotId =  iterator.next();
            try {
              SnapshotInfo snapInfo = SnapshotUtils.getSnapshotInfo(getOzoneManager(), chainManager, snapshotId);
              // Wait for snapshot changes to be flushed to disk.
              if (!OmSnapshotManager.areSnapshotChangesFlushedToDB(getOzoneManager().getMetadataManager(), snapInfo)) {
                LOG.info("Skipping snapshot processing since changes to snapshot {} have not been flushed to disk",
                    snapInfo);
                continue;
              }
              // Check if snapshot has been directory deep cleaned. Return if deep cleaning already done.
              if (snapInfo.getDeepCleanedDeletedDir()) {
                LOG.debug("Snapshot {} has already been directory deep cleaned", snapInfo);
                continue;
              }
              try (ReferenceCounted<OmSnapshot> omSnapshot = omSnapshotManager.getSnapshot(snapInfo.getVolumeName(),
                  snapInfo.getBucketName(), snapInfo.getName())) {
                remainNum = processDeletedDirsForStore(snapInfo, omSnapshot.get().getKeyManager(), remainNum);
              }

            } catch (IOException e) {
              LOG.error("Error while running delete directories and files " +
                  "background task for snapshot: {}. Will retry at next run. on active object store", snapshotId, e);
            }
          }
        }
      }
      // place holder by returning empty results of this call back.
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }

}
