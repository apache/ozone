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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
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
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetSnapshotPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotSize;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.getDirectoryInfo;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.getOmKeyInfo;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.setKeyNameAndFileName;
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
    private boolean expandDirectoryAndPurgeIfDirNotReferenced(
        Table.KeyValue<String, OmKeyInfo> deletedDir,
        Table<String, OmDirectoryInfo> previousDirTable,
        Table<String, String> renamedTable,
        AtomicLong remainNum, AtomicInteger consumedSize, AtomicLong dirNum,
        AtomicLong subDirNum, AtomicLong subFileNum,
        List<OzoneManagerProtocolProtos.PurgePathRequest> purgePathRequestList,
        List<Pair<String, OmKeyInfo>> allSubDirList,
        KeyManager keyManager) throws IOException {
      if (isDirReclaimable(deletedDir, previousDirTable, renamedTable)) {
        OzoneManagerProtocolProtos.PurgePathRequest request = prepareDeleteDirRequest(
            remainNum.get(), deletedDir.getValue(), deletedDir.getKey(),
            allSubDirList, keyManager);
        if (isBufferLimitCrossed(ratisByteLimit, consumedSize.get(),
            request.getSerializedSize())) {
          if (purgePathRequestList.size() != 0) {
            // if message buffer reaches max limit, avoid sending further
            remainNum.set(0);
            return false;
          }
          // if directory itself is having a lot of keys / files,
          // reduce capacity to minimum level
          remainNum.set(MIN_ERR_LIMIT_PER_TASK);
          request = prepareDeleteDirRequest(
              remainNum.get(), deletedDir.getValue(), deletedDir.getKey(),
              allSubDirList, keyManager);
        }
        consumedSize.addAndGet(request.getSerializedSize());
        purgePathRequestList.add(request);
        remainNum.addAndGet(-1 * request.getDeletedSubFilesCount());
        remainNum.addAndGet(-1 * request.getMarkDeletedSubDirsCount());
        // Count up the purgeDeletedDir, subDirs and subFiles
        boolean dirDeleted = false;
        if (request.getDeletedDir() != null
            && !request.getDeletedDir().isEmpty()) {
          dirNum.incrementAndGet();
          dirDeleted = true;
        }
        subDirNum.addAndGet(request.getMarkDeletedSubDirsCount());
        subFileNum.addAndGet(request.getDeletedSubFilesCount());
        // returning true if there are only files and all files along with the directory is removed. Otherwise this
        // directory needs another iteration to cleanup.
        return request.getMarkDeletedSubDirsCount() == 0 && dirDeleted;
      }
      return true;
    }

    //Removes deleted file from AOS and moves if it is not referenced in the previous snapshot.
    // Returns
    private boolean deleteKeyIfNotReferencedInPreviousSnapshot(
        long volumeId, OmBucketInfo bucketInfo,
        Table.KeyValue<String, OmKeyInfo> deletedKeyInfoEntry,
        SnapshotInfo prevSnapshotInfo, SnapshotInfo prevPrevSnapshotInfo,
        Table<String, String> renamedTable, Table<String, String> prevRenamedTable,
        Table<String, OmKeyInfo> previousKeyTable, Table<String, OmKeyInfo> previousPrevKeyTable,
        List<OzoneManagerProtocolProtos.KeyInfo> deletedKeyInfos,
        AtomicLong remainNum, AtomicInteger consumedSize) throws IOException {
      OmKeyInfo deletedKeyInfo = deletedKeyInfoEntry.getValue();
      if (isKeyReclaimable(previousKeyTable, renamedTable, deletedKeyInfo, bucketInfo, volumeId, null)) {
        OzoneManagerProtocolProtos.KeyInfo keyInfo = deletedKeyInfo.getProtobuf(true,
            ClientVersion.CURRENT_VERSION);
        if (isBufferLimitCrossed(ratisByteLimit, consumedSize.get(),
            keyInfo.getSerializedSize()) && consumedSize.get() > 0) {
          // if message buffer reaches max limit, avoid sending further
          remainNum.set(0);
          return false;
        }
        deletedKeyInfos.add(keyInfo);
        remainNum.decrementAndGet();
        consumedSize.addAndGet(keyInfo.getSerializedSize());
      } else {
        calculateExclusiveSize(prevSnapshotInfo, prevPrevSnapshotInfo, deletedKeyInfo, bucketInfo, volumeId,
            renamedTable, previousKeyTable, prevRenamedTable, previousPrevKeyTable, exclusiveSizeMap,
            exclusiveReplicatedSizeMap);
      }
      return true;
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
          ReferenceCounted<OmSnapshot> rcPrevToPrevOmSnapshot = null;
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
              rcPrevOmSnapshot = omSnapshotManager.getActiveSnapshot(
                  previousSnapshot.getVolumeName(),
                  previousSnapshot.getBucketName(),
                  previousSnapshot.getName());
              OmSnapshot omPreviousSnapshot = rcPrevOmSnapshot.get();

              previousKeyTable = omPreviousSnapshot.getMetadataManager()
                  .getKeyTable(bucketInfo.getBucketLayout());
              prevRenamedTable = omPreviousSnapshot
                  .getMetadataManager().getSnapshotRenamedTable();
              previousDirTable = omPreviousSnapshot.getMetadataManager().getDirectoryTable();
              previousToPrevSnapshot = getPreviousSnapshot(
                  previousSnapshot, snapChainManager, omSnapshotManager);
            } else {
              previousKeyTable = null;
              previousDirTable = null;
              prevRenamedTable = null;
              previousToPrevSnapshot = null;
            }


            if (previousToPrevSnapshot != null) {
              rcPrevToPrevOmSnapshot = omSnapshotManager.getActiveSnapshot(
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
                List<OzoneManagerProtocolProtos.KeyInfo> deletedSubKeyInfos = new ArrayList<>();
                Operation<Table.KeyValue<String, OmKeyInfo>, Boolean> operationOnDirectory =
                    deletedDirectoryEntry -> expandDirectoryAndPurgeIfDirNotReferenced(deletedDirectoryEntry, previousDirTable,
                    snapRenameTable, remainNum, consumedSize, dirNum, subDirNum, subFileNum,
                    purgePathRequestList, allSubDirList, getOzoneManager().getKeyManager());;

                Operation<Table.KeyValue<String, OmKeyInfo>, Boolean> operationOnFile =
                    deletedKeyInfo -> deleteKeyIfNotReferencedInPreviousSnapshot(volumeId, bucketInfo, deletedKeyInfo,
                    previousSnapshot, previousToPrevSnapshot, snapRenameTable, prevRenamedTable,
                        previousKeyTable, previousToPrevKeyTable, deletedSubKeyInfos, remainNum, consumedSize);;
                Supplier<Boolean> breakConditionSupplier = () -> remainNum.get() > 0;
                boolean retVal = true;
                while (deletedDirIterator.hasNext()) {
                  Table.KeyValue<String, OmKeyInfo> deletedDirInfo =
                      deletedDirIterator.next();
                  if (breakConditionSupplier.get()) {
                    retVal = false;
                    break;
                  }
                  // For each deleted directory we do an in-memory DFS and
                  // do a deep clean based on the previous snapshot.
                  retVal = retVal && iterateDirectoryTree(deletedDirInfo, volumeId, bucketInfo,
                      operationOnFile, operationOnDirectory,
                      breakConditionSupplier, metadataManager);
                }
                if (deletedSubKeyInfos.size() > 0) {
                  purgePathRequestList.add(wrapPurgeRequest(volumeId, bucketInfo.getObjectID(), deletedSubKeyInfos));
                }
                retVal = retVal && optimizeDirDeletesAndSubmitRequest(0, dirNum.get(), subDirNum.get(),
                    subFileNum.get(),
                    allSubDirList, purgePathRequestList, currSnapInfo.getTableKey(), startTime, 0,
                    getOzoneManager().getKeyManager()).getRight().map(OzoneManagerProtocolProtos.OMResponse::getSuccess)
                    .orElse(false);
                List<SetSnapshotPropertyRequest> setSnapshotPropertyRequests = new ArrayList<>();
                if (retVal && subDirNum.get() == 0) {
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

  /**
   * Performs a DFS iteration on a given deleted directory and performs operation on the sub deleted directories and
   * subfiles in the tree.
   * @param deletedDirInfo
   * @param volumeId
   * @param bucketInfo
   * @param operationOnFile
   * @param operationOnDirectory
   * @param breakConditionSupplier
   * @param metadataManager
   * @return True if complete directory was iterated and all operations returned a true otherwise false.
   * middle of iteration.
   * @throws IOException
   */
  private boolean iterateDirectoryTree(
      Table.KeyValue<String, OmKeyInfo> deletedDirInfo,
      long volumeId, OmBucketInfo bucketInfo, Operation<Table.KeyValue<String, OmKeyInfo>, Boolean> operationOnFile,
      Operation<Table.KeyValue<String, OmKeyInfo>, Boolean> operationOnDirectory,
      Supplier<Boolean> breakConditionSupplier,
      OMMetadataManager metadataManager) throws IOException {
    Table<String, OmDirectoryInfo> dirTable = metadataManager.getDirectoryTable();
    Table<String, OmKeyInfo> fileTable = metadataManager.getFileTable();
    Stack<StackNode> stackNodes = new Stack<>();
    OmDirectoryInfo omDeletedDirectoryInfo = getDirectoryInfo(deletedDirInfo.getValue());
    String dirPathDbKey = metadataManager.getOzonePathKey(volumeId, bucketInfo.getObjectID(),
        omDeletedDirectoryInfo);
    StackNode topLevelDir = new StackNode();
    topLevelDir.setDirKey(dirPathDbKey);
    topLevelDir.setDirValue(deletedDirInfo.getValue());
    stackNodes.push(topLevelDir);
    String bucketPrefixKeyFSO = getOzonePathKeyForFso(metadataManager, bucketInfo.getVolumeName(),
        bucketInfo.getBucketName());
    boolean retVal = true;
    try (TableIterator<String, ? extends Table.KeyValue<String, OmDirectoryInfo>>
             directoryIterator = dirTable.iterator(bucketPrefixKeyFSO);
         TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> fileIterator =
             fileTable.iterator(bucketPrefixKeyFSO)) {
      while (!stackNodes.isEmpty()) {
        StackNode stackTop = stackNodes.peek();
        // We are doing a pre order traversal here meaning, first process the current directory and all the files
        // and then do a DFS for directory. This is so that we can exit early avoiding unnecessary traversal.
        if (StringUtils.isEmpty(stackTop.getSubDirSeek())) {
          boolean directoryOpReturnValue = operationOnDirectory.apply(Table.newKeyValue(stackTop.getDirKey(),
              stackTop.getDirValue()));
          if (!directoryOpReturnValue) {
            retVal = false;
            stackNodes.pop();
          }
          if (breakConditionSupplier.get()) {
            return retVal;
          }
          String subFileSeekValue = metadataManager.getOzonePathKey(volumeId,
              bucketInfo.getObjectID(),
              stackTop.getDirValue().getObjectID(), "");
          fileIterator.seek(subFileSeekValue);
          while (fileIterator.hasNext()) {
            Table.KeyValue<String, OmKeyInfo> fileEntry = fileIterator.next();
            OmKeyInfo omKeyInfo = fileEntry.getValue();
            if (!OMFileRequest.isImmediateChild(omKeyInfo.getParentObjectID(),
                stackTop.getDirValue().getObjectID())) {
              break;
            }
            setKeyNameAndFileName(stackTop.getDirValue(), omKeyInfo);
            if(!operationOnFile.apply(fileEntry)) {
              retVal = false;
              stackNodes.pop();
              break;
            }
            if (breakConditionSupplier.get()) {
              return retVal;
            }
          }
          // Format : /volId/bucketId/parentId/
          String seekDirInDB = metadataManager
              .getOzonePathKey(volumeId, bucketInfo.getObjectID(),
                  stackTop.getDirValue().getObjectID(), "");
          stackTop.setSubDirSeek(seekDirInDB);
        } else {
          // Adding \0 to seek the next greater element.
          directoryIterator.seek(stackTop.getSubDirSeek() + "\0");
          if (directoryIterator.hasNext()) {
            Table.KeyValue<String, OmDirectoryInfo> deletedSubDirInfo = directoryIterator.next();
            String deletedSubDirKey = deletedSubDirInfo.getKey();
            String prefixCheck = metadataManager.getOzoneDeletePathDirKey(stackTop.getSubDirSeek());
            // Exit if it is out of the sub dir prefix scope.
            if (!deletedSubDirKey.startsWith(prefixCheck)) {
              stackNodes.pop();
            } else {
              stackTop.setSubDirSeek(deletedSubDirKey);
              StackNode nextSubDir = new StackNode();
              nextSubDir.setDirKey(deletedSubDirInfo.getKey());
              nextSubDir.setDirValue(getOmKeyInfo(stackTop.getDirValue(), deletedSubDirInfo.getValue()));
              stackNodes.push(nextSubDir);
            }
          } else {
            stackNodes.pop();
          }
        }
      }
    }
    return retVal;
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

  private interface Operation<T, R> {
    R apply(T t) throws IOException;

  }
}
