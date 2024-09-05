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
import com.google.protobuf.ServiceException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetSnapshotPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotSize;
import org.apache.hadoop.ozone.util.CheckExceptionOperation;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.getOmKeyInfo;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.getOzonePathKeyForFso;

/**
 * Snapshot BG Service for deleted directory deep clean and exclusive size
 * calculation for deleted directories.
 */
public class SnapshotDirectoryCleaningService
    extends AbstractKeyDeletingService {
  // Use only a single thread for DirDeletion. Multiple threads would read
  // or write to same tables and can send deletion requests for same key
  // multiple times.
  private static final int SNAPSHOT_DIR_CORE_POOL_SIZE = 1;
  private static final int MIN_ERR_LIMIT_PER_TASK = 1000;
  private final AtomicBoolean suspended;
  private final Map<String, Long> exclusiveSizeMap;
  private final Map<String, Long> exclusiveReplicatedSizeMap;
  private final long keyLimitPerSnapshot;
  private final int  ratisByteLimit;

  public SnapshotDirectoryCleaningService(long interval, TimeUnit unit,
                                          long serviceTimeout,
                                          OzoneManager ozoneManager,
                                          ScmBlockLocationProtocol scmClient) {
    super(SnapshotDirectoryCleaningService.class.getSimpleName(),
        interval, unit, SNAPSHOT_DIR_CORE_POOL_SIZE, serviceTimeout,
        ozoneManager, scmClient);
    this.suspended = new AtomicBoolean(false);
    this.exclusiveSizeMap = new HashMap<>();
    this.exclusiveReplicatedSizeMap = new HashMap<>();
    this.keyLimitPerSnapshot = ozoneManager.getConfiguration().getLong(
        OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK,
        OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK_DEFAULT);
    int limit = (int) ozoneManager.getConfiguration().getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    // always go to 90% of max limit for request as other header will be added
    this.ratisByteLimit = (int) (limit * 0.9);
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
    queue.add(new SnapshotDirectoryCleaningService.SnapshotAOSDirCleanupTask());
    return queue;
  }

  private class SnapshotAOSDirCleanupTask implements BackgroundTask {

    //Expands deleted directory from active AOS if it is not referenced in the previous and breaks the dfs iteration.
    private Pair<Boolean, String> expandDirectoryAndPurgeIfDirNotReferenced(
        Table.KeyValue<String, OmKeyInfo> deletedDir, Table<String, OmDirectoryInfo> previousDirTable,
        Table<String, String> renamedTable) throws IOException {
      HddsProtos.KeyValue.Builder renamedEntry = HddsProtos.KeyValue.newBuilder();
      boolean isDirReclaimable = isDirReclaimable(deletedDir, previousDirTable, renamedTable, renamedEntry);
      return Pair.of(isDirReclaimable, renamedEntry.hasKey() ? renamedEntry.getKey() : null);
    }

    //Removes deleted file from AOS and moves if it is not referenced in the previous snapshot.
    // Returns True if it can be deleted and false if it cannot be deleted.
    private boolean deleteKeyIfNotReferencedInPreviousSnapshot(
        long volumeId, OmBucketInfo bucketInfo,
        OmKeyInfo deletedKeyInfo, SnapshotInfo prevSnapshotInfo, SnapshotInfo prevPrevSnapshotInfo,
        Table<String, String> renamedTable, Table<String, String> prevRenamedTable,
        Table<String, OmKeyInfo> previousKeyTable, Table<String, OmKeyInfo> previousPrevKeyTable) throws IOException {
      if (isKeyReclaimable(previousKeyTable, renamedTable, deletedKeyInfo, bucketInfo, volumeId, null)) {
        return true;
      }
      calculateExclusiveSize(prevSnapshotInfo, prevPrevSnapshotInfo, deletedKeyInfo, bucketInfo, volumeId,
          renamedTable, previousKeyTable, prevRenamedTable, previousPrevKeyTable, exclusiveSizeMap,
          exclusiveReplicatedSizeMap);
      return false;
    }

    @Override
    public BackgroundTaskResult call() {
      if (!shouldRun()) {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }
      LOG.debug("Running SnapshotDirectoryCleaningService");

      getRunCount().incrementAndGet();
      OmSnapshotManager omSnapshotManager =
          getOzoneManager().getOmSnapshotManager();
      Table<String, SnapshotInfo> snapshotInfoTable =
          getOzoneManager().getMetadataManager().getSnapshotInfoTable();
      OmMetadataManagerImpl metadataManager = (OmMetadataManagerImpl)
          getOzoneManager().getMetadataManager();
      SnapshotChainManager snapChainManager = metadataManager
          .getSnapshotChainManager();


      try (TableIterator<String, ? extends Table.KeyValue
          <String, SnapshotInfo>> iterator = snapshotInfoTable.iterator()) {
        while (iterator.hasNext()) {
          AtomicLong dirNum = new AtomicLong(0);
          AtomicLong subDirNum = new AtomicLong(0);
          AtomicLong subFileNum = new AtomicLong(0);
          AtomicLong remainNum = new AtomicLong(keyLimitPerSnapshot);
          AtomicInteger consumedSize = new AtomicInteger(0);
          // Getting the Snapshot info from cache.
          SnapshotInfo currSnapInfo = iterator.next().getValue();
          // Checking if all the transactions corresponding to the snapshot has been flushed by comparing the active
          // om's transaction. Since here we are just iterating through snapshots only on disk. There is a
          // possibility of missing deletedDirectory entries added from the previous run which has not been flushed.
          if (currSnapInfo == null || currSnapInfo.getDeepCleanedDeletedDir() ||
              !OmSnapshotManager.areSnapshotChangesFlushedToDB(metadataManager, currSnapInfo.getTableKey())) {
            continue;
          }

          ReferenceCounted<OmSnapshot> rcPrevOmSnapshot = null;
          ReferenceCounted<OmSnapshot> rcPrevToPrevOmSnapshot;
          try {
            long volumeId = metadataManager
                .getVolumeId(currSnapInfo.getVolumeName());
            // Get bucketInfo for the snapshot bucket to get bucket layout.
            String dbBucketKey = metadataManager
                .getBucketKey(currSnapInfo.getVolumeName(),
                    currSnapInfo.getBucketName());
            OmBucketInfo bucketInfo = metadataManager
                .getBucketTable().get(dbBucketKey);

            if (bucketInfo == null) {
              throw new IllegalStateException("Bucket " + "/" +
                  currSnapInfo.getVolumeName() + "/" + currSnapInfo
                  .getBucketName() +
                  " is not found. BucketInfo should not be " +
                  "null for snapshotted bucket. The OM is in " +
                  "unexpected state.");
            }

            SnapshotInfo previousSnapshot = getPreviousSnapshot(
                currSnapInfo, snapChainManager, omSnapshotManager);
            final SnapshotInfo previousToPrevSnapshot;

            final Table<String, OmKeyInfo> previousKeyTable;
            final Table<String, OmDirectoryInfo> previousDirTable;
            final Table<String, String> prevRenamedTable;

            final Table<String, OmKeyInfo> previousToPrevKeyTable;

            if (previousSnapshot != null) {
              rcPrevOmSnapshot = omSnapshotManager.getSnapshot(
                  previousSnapshot.getVolumeName(),
                  previousSnapshot.getBucketName(),
                  previousSnapshot.getName());
              OmSnapshot omPreviousSnapshot = rcPrevOmSnapshot.get();

              previousKeyTable = omPreviousSnapshot.getMetadataManager().getKeyTable(bucketInfo.getBucketLayout());
              prevRenamedTable = omPreviousSnapshot.getMetadataManager().getSnapshotRenamedTable();
              previousDirTable = omPreviousSnapshot.getMetadataManager().getDirectoryTable();
              previousToPrevSnapshot = getPreviousSnapshot(previousSnapshot, snapChainManager, omSnapshotManager);
            } else {
              previousKeyTable = null;
              previousDirTable = null;
              prevRenamedTable = null;
              previousToPrevSnapshot = null;
            }


            if (previousToPrevSnapshot != null) {
              rcPrevToPrevOmSnapshot = omSnapshotManager.getSnapshot(
                  previousToPrevSnapshot.getVolumeName(),
                  previousToPrevSnapshot.getBucketName(),
                  previousToPrevSnapshot.getName());
              OmSnapshot omPreviousToPrevSnapshot = rcPrevToPrevOmSnapshot.get();

              previousToPrevKeyTable = omPreviousToPrevSnapshot
                  .getMetadataManager()
                  .getKeyTable(bucketInfo.getBucketLayout());
            } else {
              previousToPrevKeyTable = null;
            }

            String dbBucketKeyForDir = getOzonePathKeyForFso(metadataManager,
                currSnapInfo.getVolumeName(), currSnapInfo.getBucketName());
            try (ReferenceCounted<OmSnapshot> rcCurrOmSnapshot = omSnapshotManager.getSnapshot(
                currSnapInfo.getVolumeName(),
                currSnapInfo.getBucketName(),
                currSnapInfo.getName())) {

              OmSnapshot currOmSnapshot = rcCurrOmSnapshot.get();
              Table<String, OmKeyInfo> snapDeletedDirTable =
                  currOmSnapshot.getMetadataManager().getDeletedDirTable();
              Table<String, String> snapRenameTable =
                  currOmSnapshot.getMetadataManager().getSnapshotRenamedTable();

              try (TableIterator<String, ? extends Table.KeyValue<String,
                  OmKeyInfo>> deletedDirIterator = snapDeletedDirTable
                  .iterator(dbBucketKeyForDir)) {
                long startTime = Time.monotonicNow();
                List<OzoneManagerProtocolProtos.PurgePathRequest> purgePathRequestList = new ArrayList<>();
                List<Pair<String, OmKeyInfo>> allSubDirList = new ArrayList<>();

                CheckExceptionOperation<OmKeyInfo, Boolean> checkExceptionOperationOnFile =
                    deletedKeyInfo -> deleteKeyIfNotReferencedInPreviousSnapshot(volumeId, bucketInfo, deletedKeyInfo,
                    previousSnapshot, previousToPrevSnapshot, snapRenameTable, prevRenamedTable,
                        previousKeyTable, previousToPrevKeyTable);

                while (deletedDirIterator.hasNext()) {
                  Table.KeyValue<String, OmKeyInfo> deletedDirInfo =
                      deletedDirIterator.next();

                  HddsProtos.KeyValue.Builder renamedEntry = HddsProtos.KeyValue.newBuilder();

                  // Check if the directory is reclaimable. If it is not we cannot delete the directory.
                  boolean isDirReclaimable = isDirReclaimable(deletedDirInfo, previousDirTable, snapRenameTable,
                      renamedEntry);
                  Optional<OzoneManagerProtocolProtos.PurgePathRequest> request = prepareDeleteDirRequest(
                      remainNum.get(), deletedDirInfo.getValue(), deletedDirInfo.getKey(),
                      allSubDirList, getOzoneManager().getKeyManager(), renamedEntry.hasKey() ?
                          renamedEntry.getKey() : null, isDirReclaimable, checkExceptionOperationOnFile);
                  if (!request.isPresent()) {
                    continue;
                  }
                  if (isBufferLimitCrossed(ratisByteLimit, consumedSize.get(),
                      request.get().getSerializedSize())) {
                    if (purgePathRequestList.size() != 0) {
                      // if message buffer reaches max limit, avoid sending further
                      remainNum.set(0);
                    }
                    // if directory itself is having a lot of keys / files,
                    // reduce capacity to minimum level
                    remainNum.set(MIN_ERR_LIMIT_PER_TASK);
                    request = prepareDeleteDirRequest(
                        remainNum.get(), deletedDirInfo.getValue(), deletedDirInfo.getKey(),
                        allSubDirList, getOzoneManager().getKeyManager(), renamedEntry.getKey(), isDirReclaimable,
                        checkExceptionOperationOnFile);
                  }
                  if (request.isPresent()) {
                    OzoneManagerProtocolProtos.PurgePathRequest requestVal = request.get();
                    consumedSize.addAndGet(requestVal.getSerializedSize());
                    purgePathRequestList.add(requestVal);
                    remainNum.addAndGet(-1 * requestVal.getDeletedSubFilesCount());
                    remainNum.addAndGet(-1 * requestVal.getMarkDeletedSubDirsCount());
                    // Count up the purgeDeletedDir, subDirs and subFiles
                    if (requestVal.hasDeletedDir() && !requestVal.getDeletedDir().isEmpty()) {
                      dirNum.incrementAndGet();
                    }
                    subDirNum.addAndGet(requestVal.getMarkDeletedSubDirsCount());
                    subFileNum.addAndGet(requestVal.getDeletedSubFilesCount());
                  }
                }

                boolean retVal = optimizeDirDeletesAndSubmitRequest(remainNum.get(), dirNum.get(), subDirNum.get(),
                    subFileNum.get(), allSubDirList, purgePathRequestList,
                    currSnapInfo.getTableKey(), startTime, 0, getOzoneManager().getKeyManager(),
                    (deletedDir) -> expandDirectoryAndPurgeIfDirNotReferenced(deletedDir, previousDirTable,
                        snapRenameTable), checkExceptionOperationOnFile).getRight()
                    .map(OzoneManagerProtocolProtos.OMResponse::getSuccess)
                    .orElse(true);

                List<SetSnapshotPropertyRequest> setSnapshotPropertyRequests = new ArrayList<>();
                if (retVal && remainNum.get() == keyLimitPerSnapshot) {
                  if (previousSnapshot != null) {
                    setSnapshotPropertyRequests.add(getSetSnapshotRequestUpdatingExclusiveSize(previousSnapshot.getTableKey()));
                  }
                  setSnapshotPropertyRequests.add(getSetSnapshotPropertyRequestupdatingDeepCleanSnapshotDir(
                      currSnapInfo.getTableKey()));
                  submitSetSnapshotRequest(setSnapshotPropertyRequests);
                }
              }
            }
          } finally {
            IOUtils.closeQuietly(rcPrevOmSnapshot);
          }
        }
      } catch (IOException ex) {
        LOG.error("Error while running directory deep clean on snapshots." +
            " Will retry at next run.", ex);
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }


  private SetSnapshotPropertyRequest getSetSnapshotRequestUpdatingExclusiveSize(String prevSnapshotKeyTable) {
    SnapshotSize snapshotSize = SnapshotSize.newBuilder()
            .setExclusiveSize(
                exclusiveSizeMap.getOrDefault(prevSnapshotKeyTable, 0L))
            .setExclusiveReplicatedSize(
                exclusiveReplicatedSizeMap.getOrDefault(
                    prevSnapshotKeyTable, 0L))
            .build();
    exclusiveSizeMap.remove(prevSnapshotKeyTable);
    exclusiveReplicatedSizeMap.remove(prevSnapshotKeyTable);

    return SetSnapshotPropertyRequest.newBuilder()
        .setSnapshotKey(prevSnapshotKeyTable)
        .setSnapshotSize(snapshotSize)
        .build();
  }

  private SetSnapshotPropertyRequest getSetSnapshotPropertyRequestupdatingDeepCleanSnapshotDir(String snapshotKeyTable) {
    return SetSnapshotPropertyRequest.newBuilder()
            .setSnapshotKey(snapshotKeyTable)
            .setDeepCleanedDeletedDir(true)
            .build();
  }

  private void submitSetSnapshotRequest(List<SetSnapshotPropertyRequest> setSnapshotPropertyRequests) {
    ClientId clientId = ClientId.randomId();
    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.SetSnapshotProperty)
        .addAllSetSnapshotPropertyRequests(setSnapshotPropertyRequests)
        .setClientId(clientId.toString())
        .build();
    submitRequest(omRequest, clientId);
  }

  public void submitRequest(OMRequest omRequest, ClientId clientId) {
    try {
      if (isRatisEnabled()) {
        OzoneManagerRatisServer server =
            getOzoneManager().getOmRatisServer();

        RaftClientRequest raftClientRequest = RaftClientRequest.newBuilder()
            .setClientId(clientId)
            .setServerId(server.getRaftPeerId())
            .setGroupId(server.getRaftGroupId())
            .setCallId(getRunCount().get())
            .setMessage(Message.valueOf(
                OMRatisHelper.convertRequestToByteString(omRequest)))
            .setType(RaftClientRequest.writeRequestType())
            .build();

        server.submitRequest(omRequest, raftClientRequest);
      } else {
        getOzoneManager().getOmServerProtocol()
            .submitRequest(null, omRequest);
      }
    } catch (ServiceException e) {
      LOG.error("Snapshot deep cleaning request failed. " +
          "Will retry at next run.", e);
    }
  }

  /**
   * Stack node data for directory deep clean for snapshot.
   */
  private static class StackNode {
    private String dirKey;
    private OmKeyInfo dirValue;
    private String subDirSeek;

    public String getDirKey() {
      return dirKey;
    }

    public void setDirKey(String dirKey) {
      this.dirKey = dirKey;
    }

    public OmKeyInfo getDirValue() {
      return dirValue;
    }

    public void setDirValue(OmKeyInfo dirValue) {
      this.dirValue = dirValue;
    }

    public String getSubDirSeek() {
      return subDirSeek;
    }

    public void setSubDirSeek(String subDirSeek) {
      this.subDirSeek = subDirSeek;
    }

    @Override
    public String toString() {
      return "StackNode{" +
          "dirKey='" + dirKey + '\'' +
          ", dirObjectId=" + dirValue.getObjectID() +
          ", subDirSeek='" + subDirSeek + '\'' +
          '}';
    }
  }
}
