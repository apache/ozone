/**
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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.lock.OzoneManagerLock;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletedKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgePathRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.util.CheckedExceptionOperation;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.util.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConsts.OBJECT_ID_RECLAIM_BLOCKS;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Abstracts common code from KeyDeletingService and DirectoryDeletingService
 * which is now used by SnapshotDeletingService as well.
 */
public abstract class AbstractKeyDeletingService extends BackgroundService
    implements BootstrapStateHandler {
  private static final ClientId CLIENT_ID = ClientId.randomId();
  private final OzoneManager ozoneManager;
  private final ScmBlockLocationProtocol scmClient;
  private final AtomicLong deletedDirsCount;
  private final AtomicLong movedDirsCount;
  private final AtomicLong movedFilesCount;
  private final AtomicLong runCount;
  private final BootstrapStateHandler.Lock lock =
      new BootstrapStateHandler.Lock();

  public AbstractKeyDeletingService(String serviceName, long interval,
      TimeUnit unit, int threadPoolSize, long serviceTimeout,
      OzoneManager ozoneManager, ScmBlockLocationProtocol scmClient) {
    super(serviceName, interval, unit, threadPoolSize, serviceTimeout,
        ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.scmClient = scmClient;
    this.deletedDirsCount = new AtomicLong(0);
    this.movedDirsCount = new AtomicLong(0);
    this.movedFilesCount = new AtomicLong(0);
    this.runCount = new AtomicLong(0);
  }

  // TODO: Move this util class.
  public static boolean isBlockLocationInfoSame(OmKeyInfo prevKeyInfo,
                                                OmKeyInfo deletedKeyInfo) {

    if (prevKeyInfo == null && deletedKeyInfo == null) {
      LOG.debug("Both prevKeyInfo and deletedKeyInfo are null.");
      return true;
    }
    if (prevKeyInfo == null || deletedKeyInfo == null) {
      LOG.debug("prevKeyInfo: '{}' or deletedKeyInfo: '{}' is null.",
          prevKeyInfo, deletedKeyInfo);
      return false;
    }
    // For hsync, Though the blockLocationInfo of a key may not be same
    // at the time of snapshot and key deletion as blocks can be appended.
    // If the objectId is same then the key is same.
    if (prevKeyInfo.isHsync() && deletedKeyInfo.isHsync()) {
      return true;
    }

    if (prevKeyInfo.getKeyLocationVersions().size() !=
        deletedKeyInfo.getKeyLocationVersions().size()) {
      return false;
    }

    OmKeyLocationInfoGroup deletedOmKeyLocation =
        deletedKeyInfo.getLatestVersionLocations();
    OmKeyLocationInfoGroup prevOmKeyLocation =
        prevKeyInfo.getLatestVersionLocations();

    if (deletedOmKeyLocation == null || prevOmKeyLocation == null) {
      return false;
    }

    List<OmKeyLocationInfo> deletedLocationList =
        deletedOmKeyLocation.getLocationList();
    List<OmKeyLocationInfo> prevLocationList =
        prevOmKeyLocation.getLocationList();

    if (deletedLocationList.size() != prevLocationList.size()) {
      return false;
    }

    for (int idx = 0; idx < deletedLocationList.size(); idx++) {
      OmKeyLocationInfo deletedLocationInfo = deletedLocationList.get(idx);
      OmKeyLocationInfo prevLocationInfo = prevLocationList.get(idx);
      if (!deletedLocationInfo.hasSameBlockAs(prevLocationInfo)) {
        return false;
      }
    }

    return true;
  }

  protected Pair<Integer, Boolean> processKeyDeletes(List<BlockGroup> keyBlocksList,
      KeyManager manager,
      Map<String, RepeatedOmKeyInfo> keysToModify,
      List<String> renameEntries,
      String snapTableKey, UUID expectedPreviousSnapshotId) throws IOException {

    long startTime = Time.monotonicNow();
    Pair<Integer, Boolean> purgeResult = Pair.of(0, false);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Send {} key(s) to SCM: {}",
          keyBlocksList.size(), keyBlocksList);
    } else if (LOG.isInfoEnabled()) {
      int logSize = 10;
      if (keyBlocksList.size() < logSize) {
        logSize = keyBlocksList.size();
      }
      LOG.info("Send {} key(s) to SCM, first {} keys: {}",
          keyBlocksList.size(), logSize, keyBlocksList.subList(0, logSize));
    }
    List<DeleteBlockGroupResult> blockDeletionResults =
        scmClient.deleteKeyBlocks(keyBlocksList);
    LOG.info("{} BlockGroup deletion are acked by SCM in {} ms",
        keyBlocksList.size(), Time.monotonicNow() - startTime);
    if (blockDeletionResults != null) {
      startTime = Time.monotonicNow();
      if (isRatisEnabled()) {
        purgeResult = submitPurgeKeysRequest(blockDeletionResults, keysToModify, renameEntries,
            snapTableKey, expectedPreviousSnapshotId);
      } else {
        // TODO: Once HA and non-HA paths are merged, we should have
        //  only one code path here. Purge keys should go through an
        //  OMRequest model.
        purgeResult = deleteAllKeys(blockDeletionResults, manager);
      }
      LOG.info("Blocks for {} (out of {}) keys are deleted from DB in {} ms",
          purgeResult, blockDeletionResults.size(), Time.monotonicNow() - startTime);
    }
    return purgeResult;
  }

  /**
   * Deletes all the keys that SCM has acknowledged and queued for delete.
   *
   * @param results DeleteBlockGroups returned by SCM.
   * @throws IOException      on Error
   */
  private Pair<Integer, Boolean> deleteAllKeys(List<DeleteBlockGroupResult> results,
      KeyManager manager) throws IOException {
    Table<String, RepeatedOmKeyInfo> deletedTable =
        manager.getMetadataManager().getDeletedTable();
    DBStore store = manager.getMetadataManager().getStore();
    boolean purgeSuccess = true;
    // Put all keys to delete in a single transaction and call for delete.
    int deletedCount = 0;
    try (BatchOperation writeBatch = store.initBatchOperation()) {
      for (DeleteBlockGroupResult result : results) {
        if (result.isSuccess()) {
          // Purge key from OM DB.
          deletedTable.deleteWithBatch(writeBatch,
              result.getObjectKey());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Key {} deleted from OM DB", result.getObjectKey());
          }
          deletedCount++;
        } else {
          purgeSuccess = false;
        }
      }
      // Write a single transaction for delete.
      store.commitBatchOperation(writeBatch);
    }
    return Pair.of(deletedCount, purgeSuccess);
  }

  /**
   * Submits PurgeKeys request for the keys whose blocks have been deleted
   * by SCM.
   * @param results DeleteBlockGroups returned by SCM.
   * @param keysToModify Updated list of RepeatedOmKeyInfo
   */
  private Pair<Integer, Boolean> submitPurgeKeysRequest(List<DeleteBlockGroupResult> results,
      Map<String, RepeatedOmKeyInfo> keysToModify, List<String> renameEntriesToBeDeleted,
      String snapTableKey, UUID expectedPreviousSnapshotId) {
    Map<Pair<String, String>, List<String>> purgeKeysMapPerBucket = new HashMap<>();

    // Put all keys to be purged in a list
    int deletedCount = 0;
    boolean purgeSuccess = true;
    for (DeleteBlockGroupResult result : results) {
      if (result.isSuccess()) {
        // Add key to PurgeKeys list.
        String deletedKey = result.getObjectKey();
        if (keysToModify != null && !keysToModify.containsKey(deletedKey)) {
          // Parse Volume and BucketName
          addToMap(purgeKeysMapPerBucket, deletedKey);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Key {} set to be updated in OM DB, Other versions " +
                "of the key that are reclaimable are reclaimed.", deletedKey);
          }
        } else if (keysToModify == null) {
          addToMap(purgeKeysMapPerBucket, deletedKey);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Key {} set to be purged from OM DB", deletedKey);
          }
        }
        deletedCount++;
      } else {
        purgeSuccess = false;
      }
    }

    PurgeKeysRequest.Builder purgeKeysRequest = PurgeKeysRequest.newBuilder();
    if (snapTableKey != null) {
      purgeKeysRequest.setSnapshotTableKey(snapTableKey);
    }
    OzoneManagerProtocolProtos.NullableUUID.Builder expectedPreviousSnapshotNullableUUID =
        OzoneManagerProtocolProtos.NullableUUID.newBuilder();
    if (expectedPreviousSnapshotId != null) {
      expectedPreviousSnapshotNullableUUID.setUuid(HddsUtils.toProtobuf(expectedPreviousSnapshotId));
    }

    if (expectedPreviousSnapshotId != null) {
      purgeKeysRequest.setExpectedPreviousSnapshotID(expectedPreviousSnapshotNullableUUID);
    }

    // Add keys to PurgeKeysRequest bucket wise.
    for (Map.Entry<Pair<String, String>, List<String>> entry : purgeKeysMapPerBucket.entrySet()) {
      Pair<String, String> volumeBucketPair = entry.getKey();
      DeletedKeys deletedKeysInBucket = DeletedKeys.newBuilder()
          .setVolumeName(volumeBucketPair.getLeft())
          .setBucketName(volumeBucketPair.getRight())
          .addAllKeys(entry.getValue())
          .build();
      purgeKeysRequest.addDeletedKeys(deletedKeysInBucket);
    }
    // Adding rename entries to be purged.
    if (renameEntriesToBeDeleted != null) {
      purgeKeysRequest.addAllRenamedKeys(renameEntriesToBeDeleted);
    }


    List<SnapshotMoveKeyInfos> keysToUpdateList = new ArrayList<>();
    if (keysToModify != null) {
      for (Map.Entry<String, RepeatedOmKeyInfo> keyToModify :
          keysToModify.entrySet()) {

        SnapshotMoveKeyInfos.Builder keyToUpdate =
            SnapshotMoveKeyInfos.newBuilder();
        keyToUpdate.setKey(keyToModify.getKey());
        List<OzoneManagerProtocolProtos.KeyInfo> keyInfos =
            keyToModify.getValue().getOmKeyInfoList().stream()
                .map(k -> k.getProtobuf(ClientVersion.CURRENT_VERSION))
                .collect(Collectors.toList());
        keyToUpdate.addAllKeyInfos(keyInfos);
        keysToUpdateList.add(keyToUpdate.build());
      }

      if (keysToUpdateList.size() > 0) {
        purgeKeysRequest.addAllKeysToUpdate(keysToUpdateList);
      }
    }

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(Type.PurgeKeys)
        .setPurgeKeysRequest(purgeKeysRequest)
        .setClientId(CLIENT_ID.toString())
        .build();

    // Submit PurgeKeys request to OM
    try {
      OzoneManagerProtocolProtos.OMResponse omResponse = OzoneManagerRatisUtils.submitRequest(ozoneManager, omRequest,
          CLIENT_ID, runCount.get());
      if (omResponse != null) {
        purgeSuccess = purgeSuccess && omResponse.getSuccess();
      }
    } catch (ServiceException e) {
      LOG.error("PurgeKey request failed. Will retry at next run.");
      return Pair.of(0, false);
    }

    return Pair.of(deletedCount, purgeSuccess);
  }

  /**
   * Parse Volume and Bucket Name from ObjectKey and add it to given map of
   * keys to be purged per bucket.
   */
  private void addToMap(Map<Pair<String, String>, List<String>> map, String objectKey) {
    // Parse volume and bucket name
    String[] split = objectKey.split(OM_KEY_PREFIX);
    Preconditions.assertTrue(split.length >= 3, "Volume and/or Bucket Name " +
        "missing from Key Name " + objectKey);
    if (split.length == 3) {
      LOG.warn("{} missing Key Name", objectKey);
    }
    Pair<String, String> volumeBucketPair = Pair.of(split[1], split[2]);
    if (!map.containsKey(volumeBucketPair)) {
      map.put(volumeBucketPair, new ArrayList<>());
    }
    map.get(volumeBucketPair).add(objectKey);
  }

  protected OzoneManagerProtocolProtos.OMResponse submitPurgePaths(
      List<PurgePathRequest> requests, String snapTableKey, UUID expectedPreviousSnapshotId) {
    OzoneManagerProtocolProtos.PurgeDirectoriesRequest.Builder purgeDirRequest =
        OzoneManagerProtocolProtos.PurgeDirectoriesRequest.newBuilder();

    if (snapTableKey != null) {
      purgeDirRequest.setSnapshotTableKey(snapTableKey);
    }
    OzoneManagerProtocolProtos.NullableUUID.Builder expectedPreviousSnapshotNullableUUID =
        OzoneManagerProtocolProtos.NullableUUID.newBuilder();
    if (expectedPreviousSnapshotId != null) {
      expectedPreviousSnapshotNullableUUID.setUuid(HddsUtils.toProtobuf(expectedPreviousSnapshotId));
    }
    purgeDirRequest.setExpectedPreviousSnapshotID(expectedPreviousSnapshotNullableUUID.build());

    purgeDirRequest.addAllDeletedPath(requests);

    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.PurgeDirectories)
            .setPurgeDirectoriesRequest(purgeDirRequest)
            .setClientId(CLIENT_ID.toString())
            .build();

    // Submit Purge paths request to OM
    try {
      return OzoneManagerRatisUtils.submitRequest(ozoneManager, omRequest, CLIENT_ID, runCount.get());
    } catch (ServiceException e) {
      LOG.error("PurgePaths request failed. Will retry at next run.");
    }
    return null;
  }

  private OzoneManagerProtocolProtos.PurgePathRequest wrapPurgeRequest(
      final long volumeId,
      final long bucketId,
      final String purgeDeletedDir,
      final List<OmKeyInfo> purgeDeletedFiles,
      final List<OmKeyInfo> markDirsAsDeleted) {
    // Put all keys to be purged in a list
    PurgePathRequest.Builder purgePathsRequest = PurgePathRequest.newBuilder();
    purgePathsRequest.setVolumeId(volumeId);
    purgePathsRequest.setBucketId(bucketId);

    if (purgeDeletedDir != null) {
      purgePathsRequest.setDeletedDir(purgeDeletedDir);
    }

    for (OmKeyInfo purgeFile : purgeDeletedFiles) {
      purgePathsRequest.addDeletedSubFiles(
          purgeFile.getProtobuf(true, ClientVersion.CURRENT_VERSION));
    }

    // Add these directories to deletedDirTable, so that its sub-paths will be
    // traversed in next iteration to ensure cleanup all sub-children.
    for (OmKeyInfo dir : markDirsAsDeleted) {
      purgePathsRequest.addMarkDeletedSubDirs(
          dir.getProtobuf(ClientVersion.CURRENT_VERSION));
    }

    return purgePathsRequest.build();
  }

  protected Optional<PurgePathRequest> prepareDeleteDirRequest(
      long remainNum, OmKeyInfo pendingDeletedDirInfo, String delDirName,
      List<Pair<String, OmKeyInfo>> subDirList,
      KeyManager keyManager, boolean deleteDir,
      CheckedExceptionOperation<Table.KeyValue<String, OmKeyInfo>, Boolean, IOException> fileDeletionChecker)
      throws IOException {
    // step-0: Get one pending deleted directory
    if (LOG.isDebugEnabled()) {
      LOG.debug("Pending deleted dir name: {}",
          pendingDeletedDirInfo.getKeyName());
    }

    final String[] keys = delDirName.split(OM_KEY_PREFIX);
    final long volumeId = Long.parseLong(keys[1]);
    final long bucketId = Long.parseLong(keys[2]);

    // step-1: get all sub directories under the deletedDir. Always expand all sub directories irrespective of
    // reference of sub-directory in previous snapshot.
    List<OmKeyInfo> subDirs = keyManager
        .getPendingDeletionSubDirs(volumeId, bucketId,
            pendingDeletedDirInfo, (keyInfo) -> true, remainNum);
    remainNum = remainNum - subDirs.size();

    OMMetadataManager omMetadataManager = keyManager.getMetadataManager();
    for (OmKeyInfo dirInfo : subDirs) {
      String ozoneDbKey = omMetadataManager.getOzonePathKey(volumeId,
          bucketId, dirInfo.getParentObjectID(), dirInfo.getFileName());
      String ozoneDeleteKey = omMetadataManager.getOzoneDeletePathKey(
          dirInfo.getObjectID(), ozoneDbKey);
      subDirList.add(Pair.of(ozoneDeleteKey, dirInfo));
      LOG.debug("Moved sub dir name: {}", dirInfo.getKeyName());
    }

    // step-2: get all sub files under the deletedDir
    // Only remove sub files if the parent directory is going to be deleted or can be reclaimed.
    List<OmKeyInfo> subFiles = new ArrayList<>();
    for (OmKeyInfo omKeyInfo : keyManager
        .getPendingDeletionSubFiles(volumeId, bucketId,
            pendingDeletedDirInfo, (keyInfo) -> deleteDir || fileDeletionChecker.apply(keyInfo), remainNum)) {
      subFiles.add(omKeyInfo);
    }
    remainNum = remainNum - subFiles.size();

    if (LOG.isDebugEnabled()) {
      for (OmKeyInfo fileInfo : subFiles) {
        LOG.debug("Moved sub file name: {}", fileInfo.getKeyName());
      }
    }

    // step-3: Since there is a boundary condition of 'numEntries' in
    // each batch, check whether the sub paths count reached batch size
    // limit. If count reached limit then there can be some more child
    // paths to be visited and will keep the parent deleted directory
    // for one more pass.
    // If there are no subpaths to expand and the directory itself cannot be reclaimed then skip purge processing for
    // this dir, since this would be a noop.
    String purgeDeletedDir = deleteDir && remainNum > 0 ? delDirName : null;
    if (purgeDeletedDir == null && subFiles.isEmpty() && subDirs.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(wrapPurgeRequest(volumeId, bucketId,
        purgeDeletedDir, subFiles, subDirs));
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public Pair<Long, Optional<OzoneManagerProtocolProtos.OMResponse>>  optimizeDirDeletesAndSubmitRequest(
      long remainNum, long dirNum, long subDirNum, long subFileNum, List<Pair<String, OmKeyInfo>> allSubDirList,
      List<PurgePathRequest> purgePathRequestList, String snapTableKey, long startTime, int remainingBufLimit,
      KeyManager keyManager,
      CheckedExceptionOperation<Table.KeyValue<String, OmKeyInfo>, Boolean, IOException> subDirPurgeChecker,
      CheckedExceptionOperation<Table.KeyValue<String, OmKeyInfo>, Boolean, IOException> fileDeletionChecker,
      UUID expectedPreviousSnapshotId) {

    // Optimization to handle delete sub-dir and keys to remove quickly
    // This case will be useful to handle when depth of directory is high
    int subdirDelNum = 0;
    int subDirRecursiveCnt = 0;
    int consumedSize = 0;
    while (remainNum > 0 && subDirRecursiveCnt < allSubDirList.size()) {
      try {
        Pair<String, OmKeyInfo> stringOmKeyInfoPair = allSubDirList.get(subDirRecursiveCnt);
        Boolean result = subDirPurgeChecker.apply(Table.newKeyValue(stringOmKeyInfoPair.getKey(),
                stringOmKeyInfoPair.getValue()));
        Optional<PurgePathRequest> request = prepareDeleteDirRequest(
            remainNum, stringOmKeyInfoPair.getValue(),
            stringOmKeyInfoPair.getKey(), allSubDirList,
            keyManager, result, fileDeletionChecker);
        if (!request.isPresent()) {
          continue;
        }
        if (isBufferLimitCrossed(remainingBufLimit, consumedSize,
            request.get().getSerializedSize())) {
          // ignore further add request
          break;
        }
        PurgePathRequest requestVal = request.get();
        consumedSize += requestVal.getSerializedSize();
        purgePathRequestList.add(requestVal);
        remainNum = remainNum - requestVal.getDeletedSubFilesCount();
        remainNum = remainNum - requestVal.getMarkDeletedSubDirsCount();
        // Count up the purgeDeletedDir, subDirs and subFiles
        requestVal.getDeletedDir();
        if (requestVal.hasDeletedDir() && !requestVal.getDeletedDir().isEmpty()) {
          subdirDelNum++;
        }
        subDirNum += requestVal.getMarkDeletedSubDirsCount();
        subFileNum += requestVal.getDeletedSubFilesCount();
        subDirRecursiveCnt++;
      } catch (IOException e) {
        LOG.error("Error while running delete directories and files " +
            "background task. Will retry at next run for subset.", e);
        break;
      }
    }
    OzoneManagerProtocolProtos.OMResponse response = null;
    if (!purgePathRequestList.isEmpty()) {
      response = submitPurgePaths(purgePathRequestList, snapTableKey, expectedPreviousSnapshotId);
    }

    if (dirNum != 0 || subDirNum != 0 || subFileNum != 0) {
      deletedDirsCount.addAndGet(dirNum + subdirDelNum);
      movedDirsCount.addAndGet(subDirNum - subdirDelNum);
      movedFilesCount.addAndGet(subFileNum);
      LOG.info("Number of dirs deleted: {}, Number of sub-dir " +
              "deleted: {}, Number of sub-files moved:" +
              " {} to DeletedTable, Number of sub-dirs moved {} to " +
              "DeletedDirectoryTable, iteration elapsed: {}ms," +
              " totalRunCount: {}",
          dirNum, subdirDelNum, subFileNum, (subDirNum - subdirDelNum),
          Time.monotonicNow() - startTime, getRunCount());
    }
    return Pair.of(remainNum, Optional.ofNullable(response));
  }

  /**
   * To calculate Exclusive Size for current snapshot, Check
   * the next snapshot deletedTable if the deleted key is
   * referenced in current snapshot and not referenced in the
   * previous snapshot then that key is exclusive to the current
   * snapshot. Here since we are only iterating through
   * deletedTable we can check the previous and previous to
   * previous snapshot to achieve the same.
   * previousSnapshot - Snapshot for which exclusive size is
   *                    getting calculating.
   * currSnapshot - Snapshot's deletedTable is used to calculate
   *                previousSnapshot snapshot's exclusive size.
   * previousToPrevSnapshot - Snapshot which is used to check
   *                 if key is exclusive to previousSnapshot.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public void calculateExclusiveSize(
      SnapshotInfo previousSnapshot,
      SnapshotInfo previousToPrevSnapshot,
      OmKeyInfo keyInfo,
      OmBucketInfo bucketInfo, long volumeId,
      Table<String, String> snapRenamedTable,
      Table<String, OmKeyInfo> previousKeyTable,
      Table<String, String> prevRenamedTable,
      Table<String, OmKeyInfo> previousToPrevKeyTable,
      Map<String, Long> exclusiveSizeMap,
      Map<String, Long> exclusiveReplicatedSizeMap) throws IOException {
    String prevSnapKey = previousSnapshot.getTableKey();
    long exclusiveReplicatedSize =
        exclusiveReplicatedSizeMap.getOrDefault(
            prevSnapKey, 0L) + keyInfo.getReplicatedSize();
    long exclusiveSize = exclusiveSizeMap.getOrDefault(
        prevSnapKey, 0L) + keyInfo.getDataSize();

    // If there is no previous to previous snapshot, then
    // the previous snapshot is the first snapshot.
    if (previousToPrevSnapshot == null) {
      exclusiveSizeMap.put(prevSnapKey, exclusiveSize);
      exclusiveReplicatedSizeMap.put(prevSnapKey,
          exclusiveReplicatedSize);
    } else {
      OmKeyInfo keyInfoPrevSnapshot = getPreviousSnapshotKeyName(
          keyInfo, bucketInfo, volumeId,
          snapRenamedTable, previousKeyTable);
      OmKeyInfo keyInfoPrevToPrevSnapshot = getPreviousSnapshotKeyName(
          keyInfoPrevSnapshot, bucketInfo, volumeId,
          prevRenamedTable, previousToPrevKeyTable);
      // If the previous to previous snapshot doesn't
      // have the key, then it is exclusive size for the
      // previous snapshot.
      if (keyInfoPrevToPrevSnapshot == null) {
        exclusiveSizeMap.put(prevSnapKey, exclusiveSize);
        exclusiveReplicatedSizeMap.put(prevSnapKey,
            exclusiveReplicatedSize);
      }
    }
  }

  private OmKeyInfo getPreviousSnapshotKeyName(
      OmKeyInfo keyInfo, OmBucketInfo bucketInfo, long volumeId,
      Table<String, String> snapRenamedTable,
      Table<String, OmKeyInfo> previousKeyTable) throws IOException {

    if (keyInfo == null) {
      return null;
    }

    String dbKeyPrevSnap;
    if (bucketInfo.getBucketLayout().isFileSystemOptimized()) {
      dbKeyPrevSnap = getOzoneManager().getMetadataManager().getOzonePathKey(
          volumeId,
          bucketInfo.getObjectID(),
          keyInfo.getParentObjectID(),
          keyInfo.getFileName());
    } else {
      dbKeyPrevSnap = getOzoneManager().getMetadataManager().getOzoneKey(
          keyInfo.getVolumeName(),
          keyInfo.getBucketName(),
          keyInfo.getKeyName());
    }

    String dbRenameKey = getOzoneManager().getMetadataManager().getRenameKey(
        keyInfo.getVolumeName(),
        keyInfo.getBucketName(),
        keyInfo.getObjectID());

    String renamedKey = snapRenamedTable.getIfExist(dbRenameKey);
    OmKeyInfo prevKeyInfo = renamedKey != null ?
        previousKeyTable.get(renamedKey) :
        previousKeyTable.get(dbKeyPrevSnap);

    if (prevKeyInfo == null ||
        prevKeyInfo.getObjectID() != keyInfo.getObjectID()) {
      return null;
    }

    return isBlockLocationInfoSame(prevKeyInfo, keyInfo) ?
        prevKeyInfo : null;
  }

  protected boolean isBufferLimitCrossed(
      int maxLimit, int cLimit, int increment) {
    return cLimit + increment >= maxLimit;
  }

  protected boolean isKeyReclaimable(
      Table<String, OmKeyInfo> previousKeyTable,
      Table<String, String> renamedTable,
      OmKeyInfo deletedKeyInfo, OmBucketInfo bucketInfo,
      long volumeId, HddsProtos.KeyValue.Builder renamedKeyBuilder)
      throws IOException {

    String dbKey;
    // Handle case when the deleted snapshot is the first snapshot.
    if (previousKeyTable == null) {
      return true;
    }

    // These are uncommitted blocks wrapped into a pseudo KeyInfo
    if (deletedKeyInfo.getObjectID() == OBJECT_ID_RECLAIM_BLOCKS) {
      return true;
    }

    // Construct keyTable or fileTable DB key depending on the bucket type
    if (bucketInfo.getBucketLayout().isFileSystemOptimized()) {
      dbKey = ozoneManager.getMetadataManager().getOzonePathKey(
          volumeId,
          bucketInfo.getObjectID(),
          deletedKeyInfo.getParentObjectID(),
          deletedKeyInfo.getFileName());
    } else {
      dbKey = ozoneManager.getMetadataManager().getOzoneKey(
          deletedKeyInfo.getVolumeName(),
          deletedKeyInfo.getBucketName(),
          deletedKeyInfo.getKeyName());
    }

    /*
     snapshotRenamedTable:
     1) /volumeName/bucketName/objectID ->
                 /volumeId/bucketId/parentId/fileName (FSO)
     2) /volumeName/bucketName/objectID ->
                /volumeName/bucketName/keyName (non-FSO)
    */
    String dbRenameKey = ozoneManager.getMetadataManager().getRenameKey(
        deletedKeyInfo.getVolumeName(), deletedKeyInfo.getBucketName(),
        deletedKeyInfo.getObjectID());

    // Condition: key should not exist in snapshotRenamedTable
    // of the current snapshot and keyTable of the previous snapshot.
    // Check key exists in renamedTable of the Snapshot
    String renamedKey = renamedTable.getIfExist(dbRenameKey);

    if (renamedKey != null && renamedKeyBuilder != null) {
      renamedKeyBuilder.setKey(dbRenameKey).setValue(renamedKey);
    }
    // previousKeyTable is fileTable if the bucket is FSO,
    // otherwise it is the keyTable.
    OmKeyInfo prevKeyInfo = renamedKey != null ? previousKeyTable
        .get(renamedKey) : previousKeyTable.get(dbKey);

    if (prevKeyInfo == null ||
        prevKeyInfo.getObjectID() != deletedKeyInfo.getObjectID()) {
      return true;
    }

    // For key overwrite the objectID will remain the same, In this
    // case we need to check if OmKeyLocationInfo is also same.
    return !isBlockLocationInfoSame(prevKeyInfo, deletedKeyInfo);
  }

  protected boolean isDirReclaimable(
      Table.KeyValue<String, OmKeyInfo> deletedDir,
      Table<String, OmDirectoryInfo> previousDirTable,
      Table<String, String> renamedTable) throws IOException {

    if (previousDirTable == null) {
      return true;
    }

    String deletedDirDbKey = deletedDir.getKey();
    OmKeyInfo deletedDirInfo = deletedDir.getValue();
    String dbRenameKey = ozoneManager.getMetadataManager().getRenameKey(
        deletedDirInfo.getVolumeName(), deletedDirInfo.getBucketName(),
        deletedDirInfo.getObjectID());

      /*
      snapshotRenamedTable: /volumeName/bucketName/objectID ->
          /volumeId/bucketId/parentId/dirName
       */
    String dbKeyBeforeRename = renamedTable.getIfExist(dbRenameKey);
    String prevDbKey = null;

    if (dbKeyBeforeRename != null) {
      prevDbKey = dbKeyBeforeRename;
    } else {
      // In OMKeyDeleteResponseWithFSO OzonePathKey is converted to
      // OzoneDeletePathKey. Changing it back to check the previous DirTable.
      prevDbKey = ozoneManager.getMetadataManager()
          .getOzoneDeletePathDirKey(deletedDirDbKey);
    }

    OmDirectoryInfo prevDirectoryInfo = previousDirTable.get(prevDbKey);
    if (prevDirectoryInfo == null) {
      return true;
    }

    return prevDirectoryInfo.getObjectID() != deletedDirInfo.getObjectID();
  }

  protected boolean isRenameEntryReclaimable(Table.KeyValue<String, String> renameEntry,
                                             Table<String, OmDirectoryInfo> previousDirTable,
                                             Table<String, OmKeyInfo> prevKeyInfoTable) throws IOException {

    if (previousDirTable == null && prevKeyInfoTable == null) {
      return true;
    }
    String prevDbKey = renameEntry.getValue();


    if (previousDirTable != null) {
      OmDirectoryInfo prevDirectoryInfo = previousDirTable.getIfExist(prevDbKey);
      if (prevDirectoryInfo != null) {
        return false;
      }
    }

    if (prevKeyInfoTable != null) {
      OmKeyInfo omKeyInfo = prevKeyInfoTable.getIfExist(prevDbKey);
      return omKeyInfo == null;
    }
    return true;
  }



  public boolean isRatisEnabled() {
    if (ozoneManager == null) {
      return false;
    }
    return ozoneManager.isRatisEnabled();
  }

  public OzoneManager getOzoneManager() {
    return ozoneManager;
  }

  public ScmBlockLocationProtocol getScmClient() {
    return scmClient;
  }

  /**
   * Returns the number of times this Background service has run.
   *
   * @return Long, run count.
   */
  @VisibleForTesting
  public AtomicLong getRunCount() {
    return runCount;
  }

  /**
   * Returns the number of dirs deleted by the background service.
   *
   * @return Long count.
   */
  @VisibleForTesting
  public long getDeletedDirsCount() {
    return deletedDirsCount.get();
  }

  /**
   * Returns the number of sub-dirs deleted by the background service.
   *
   * @return Long count.
   */
  @VisibleForTesting
  public long getMovedDirsCount() {
    return movedDirsCount.get();
  }

  /**
   * Returns the number of files moved to DeletedTable by the background
   * service.
   *
   * @return Long count.
   */
  @VisibleForTesting
  public long getMovedFilesCount() {
    return movedFilesCount.get();
  }

  public BootstrapStateHandler.Lock getBootstrapStateLock() {
    return lock;
  }

  /**
   * Submits SetSnapsnapshotPropertyRequest to OM.
   * @param setSnapshotPropertyRequests request to be sent to OM
   */
  protected void submitSetSnapshotRequest(
      List<OzoneManagerProtocolProtos.SetSnapshotPropertyRequest> setSnapshotPropertyRequests) {
    OzoneManagerProtocolProtos.OMRequest omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.SetSnapshotProperty)
        .addAllSetSnapshotPropertyRequests(setSnapshotPropertyRequests)
        .setClientId(CLIENT_ID.toString())
        .build();
    try {
      OzoneManagerRatisUtils.submitRequest(getOzoneManager(), omRequest, CLIENT_ID, getRunCount().get());
    } catch (ServiceException e) {
      LOG.error("Failed to submit set snapshot property request", e);
    }
  }

  /**
   * Class to take multiple locks on a resource.
   */
  protected static class MultiLocks<T> {
    private final Queue<T> objectLocks;
    private final IOzoneManagerLock lock;
    private final OzoneManagerLock.Resource resource;
    private final boolean writeLock;
    public MultiLocks(IOzoneManagerLock lock, OzoneManagerLock.Resource resource, boolean writeLock) {
      this.writeLock = writeLock;
      this.resource = resource;
      this.lock = lock;
      this.objectLocks = new LinkedList<>();
    }

    public OMLockDetails acquireLock(Collection<T> objects) throws OMException {
      if (!objectLocks.isEmpty()) {
        throw new OMException("More locks cannot be acquired when locks have been already acquired. Locks acquired : "
            + objectLocks, OMException.ResultCodes.INTERNAL_ERROR);
      }
      OMLockDetails omLockDetails = OMLockDetails.EMPTY_DETAILS_LOCK_ACQUIRED;
      for (T object : objects) {
        if (object != null) {
          omLockDetails = this.writeLock ? lock.acquireWriteLock(resource, object.toString())
              : lock.acquireReadLock(resource, object.toString());
          objectLocks.add(object);
          if (!omLockDetails.isLockAcquired()) {
            break;
          }
        }
      }
      if (!omLockDetails.isLockAcquired()) {
        releaseLock();
      }
      return omLockDetails;
    }

    public void releaseLock() {
      while (!objectLocks.isEmpty()) {
        T object = objectLocks.poll();
        OMLockDetails lockDetails = this.writeLock ? lock.releaseWriteLock(resource, object.toString())
            : lock.releaseReadLock(resource, object.toString());
      }
    }
  }

  /**
   * This class is responsible for opening last N snapshot given snapshot or AOS metadata manager by acquiring a lock.
   */
  private abstract class ReclaimableFilter<V> implements CheckedExceptionOperation<Table.KeyValue<String, V>,
        Boolean, IOException>, Closeable {

    private final SnapshotInfo currentSnapshotInfo;
    private final OmSnapshotManager omSnapshotManager;
    private final SnapshotChainManager snapshotChainManager;

    private final List<SnapshotInfo> previousSnapshotInfos;
    private final List<ReferenceCounted<OmSnapshot>> previousOmSnapshots;
    private final MultiLocks<UUID> snapshotIdLocks;
    private Long volumeId;
    private OmBucketInfo bucketInfo;
    private final OMMetadataManager metadataManager;
    private final int numberOfPreviousSnapshotsFromChain;

    /**
     * Filter to return deleted keys/directories which are reclaimable based on their presence in previous snapshot in
     * the snapshot chain.
     * @param omSnapshotManager
     * @param snapshotChainManager
     * @param currentSnapshotInfo : If null the deleted keys in AOS needs to be processed, hence the latest snapshot
     *                            in the snapshot chain corresponding to bucket key needs to be processed.
     * @param metadataManager : MetadataManager corresponding to snapshot or AOS.
     * @param lock : Lock for Active OM.
     */
    private ReclaimableFilter(OmSnapshotManager omSnapshotManager, SnapshotChainManager snapshotChainManager,
                             SnapshotInfo currentSnapshotInfo, OMMetadataManager metadataManager,
                             IOzoneManagerLock lock,
                             int numberOfPreviousSnapshotsFromChain) {
      this.omSnapshotManager = omSnapshotManager;
      this.currentSnapshotInfo = currentSnapshotInfo;
      this.snapshotChainManager = snapshotChainManager;
      this.snapshotIdLocks = new MultiLocks<>(lock, OzoneManagerLock.Resource.SNAPSHOT_GC_LOCK, false);
      this.metadataManager = metadataManager;
      this.numberOfPreviousSnapshotsFromChain = numberOfPreviousSnapshotsFromChain;
      this.previousOmSnapshots = new ArrayList<>(numberOfPreviousSnapshotsFromChain);
      this.previousSnapshotInfos = new ArrayList<>(numberOfPreviousSnapshotsFromChain);
    }

    private List<SnapshotInfo> getLastNSnapshotInChain(String volume, String bucket) throws IOException {
      if (currentSnapshotInfo != null && (!currentSnapshotInfo.getVolumeName().equals(volume) ||
          !currentSnapshotInfo.getBucketName().equals(bucket))) {
        throw new IOException("Volume & Bucket name for snapshot : " + currentSnapshotInfo + " not matching for " +
            "key in volume: " + volume + " bucket: " + bucket);
      }
      SnapshotInfo expectedPreviousSnapshotInfo = currentSnapshotInfo == null
          ? SnapshotUtils.getLatestSnapshotInfo(volume, bucket, ozoneManager, snapshotChainManager)
          : SnapshotUtils.getPreviousSnapshot(ozoneManager, snapshotChainManager, currentSnapshotInfo);
      List<SnapshotInfo> snapshotInfos = new ArrayList<>();
      snapshotInfos.add(expectedPreviousSnapshotInfo);
      SnapshotInfo snapshotInfo = expectedPreviousSnapshotInfo;
      while (snapshotInfos.size() < numberOfPreviousSnapshotsFromChain) {
        snapshotInfo = snapshotInfo == null ? null
            : SnapshotUtils.getPreviousSnapshot(ozoneManager, snapshotChainManager, expectedPreviousSnapshotInfo);
        snapshotInfos.add(snapshotInfo);
        // If changes made to the snapshot have not been flushed to disk, throw exception immediately, next run of
        // garbage collection would process the snapshot.
        if (!OmSnapshotManager.areSnapshotChangesFlushedToDB(getOzoneManager().getMetadataManager(), snapshotInfo)) {
          throw new IOException("Changes made to the snapshot " + snapshotInfo + " have not been flushed to the disk ");
        }
      }

      // Reversing list to get the correct order in chain. To ensure locking order is as per the chain ordering.
      Collections.reverse(snapshotInfos);
      return snapshotInfos;
    }

    private boolean validateExistingLastNSnapshotsInChain(String volume, String bucket) throws IOException {
      List<SnapshotInfo> expectedLastNSnapshotsInChain = getLastNSnapshotInChain(volume, bucket);
      List<UUID> expectedSnapshotIds = expectedLastNSnapshotsInChain.stream()
          .map(snapshotInfo -> snapshotInfo == null ? null : snapshotInfo.getSnapshotId())
          .collect(Collectors.toList());
      List<UUID> existingSnapshotIds = previousOmSnapshots.stream()
          .map(omSnapshotReferenceCounted -> omSnapshotReferenceCounted == null ? null :
              omSnapshotReferenceCounted.get().getSnapshotID()).collect(Collectors.toList());
      return expectedSnapshotIds.equals(existingSnapshotIds);
    }

    // Initialize the last N snapshots in the chain by acquiring locks. Throw IOException if it fails.
    private void initializePreviousSnapshotsFromChain(String volume, String bucket) throws IOException {
      // If existing snapshotIds don't match then close all snapshots and reopen the previous N snapshots.
      if (!validateExistingLastNSnapshotsInChain(volume, bucket)) {
        close();
        try {
          // Acquire lock only on last N-1 snapshot & current snapshot(AOS if it is null).
          List<SnapshotInfo> expectedLastNSnapshotsInChain = getLastNSnapshotInChain(volume, bucket);
          List<UUID> expectedSnapshotIds = expectedLastNSnapshotsInChain.stream()
              .map(snapshotInfo -> snapshotInfo == null ? null : snapshotInfo.getSnapshotId())
              .collect(Collectors.toList());
          List<UUID> lockIds = new ArrayList<>(expectedSnapshotIds.subList(1, expectedSnapshotIds.size()));
          lockIds.add(currentSnapshotInfo == null ? null : currentSnapshotInfo.getSnapshotId());

          if (snapshotIdLocks.acquireLock(lockIds).isLockAcquired()) {
            for (SnapshotInfo snapshotInfo : expectedLastNSnapshotsInChain) {
              if (snapshotInfo != null) {
                // For AOS fail operation if any of the previous snapshots are not active. currentSnapshotInfo for
                // AOS will be null.
                previousOmSnapshots.add(currentSnapshotInfo == null
                    ? omSnapshotManager.getActiveSnapshot(snapshotInfo.getVolumeName(), snapshotInfo.getBucketName(),
                    snapshotInfo.getName())
                    : omSnapshotManager.getSnapshot(snapshotInfo.getVolumeName(), snapshotInfo.getBucketName(),
                    snapshotInfo.getName()));
                previousSnapshotInfos.add(snapshotInfo);
              } else {
                previousOmSnapshots.add(null);
                previousSnapshotInfos.add(null);
              }

              // TODO: Getting volumeId and bucket from active OM. This would be wrong on volume & bucket renames
              //  support.
              volumeId = getOzoneManager().getMetadataManager().getVolumeId(volume);
              String dbBucketKey = getOzoneManager().getMetadataManager().getBucketKey(volume, bucket);
              bucketInfo = getOzoneManager().getMetadataManager().getBucketTable().get(dbBucketKey);
            }
          } else {
            throw new IOException("Lock acquisition failed for last N snapshots : " +
                expectedLastNSnapshotsInChain + " " + currentSnapshotInfo);
          }
        } catch (IOException e) {
          this.close();
          throw e;
        }
      }
    }

    @Override
    public Boolean apply(Table.KeyValue<String, V> keyValue) throws IOException {
      String volume = getVolumeName(keyValue);
      String bucket = getBucketName(keyValue);
      initializePreviousSnapshotsFromChain(volume, bucket);
      boolean isReclaimable = isReclaimable(keyValue);
      // This is to ensure the reclamation ran on the same previous snapshot and no change occurred in the chain
      // while processing the entry.
      return isReclaimable && validateExistingLastNSnapshotsInChain(volume, bucket);
    }

    protected abstract String getVolumeName(Table.KeyValue<String, V> keyValue) throws IOException;
    protected abstract String getBucketName(Table.KeyValue<String, V> keyValue) throws IOException;

    protected abstract Boolean isReclaimable(Table.KeyValue<String, V> omKeyInfo) throws IOException;

    @Override
    public void close() throws IOException {
      this.snapshotIdLocks.releaseLock();
      for (ReferenceCounted<OmSnapshot> previousOmSnapshot : previousOmSnapshots) {
        previousOmSnapshot.close();
      }
      previousOmSnapshots.clear();
      previousSnapshotInfos.clear();
    }

    public ReferenceCounted<OmSnapshot> getPreviousOmSnapshot(int index) {
      return previousOmSnapshots.get(index);
    }

    public OMMetadataManager getMetadataManager() {
      return metadataManager;
    }

    public Long getVolumeId() {
      return volumeId;
    }

    public OmBucketInfo getBucketInfo() {
      return bucketInfo;
    }

    public SnapshotInfo getPreviousSnapshotInfo(int index) {
      return previousSnapshotInfos.get(index);
    }
  }

  protected class ReclaimableKeyFilter extends ReclaimableFilter<OmKeyInfo> {
    private final Map<String, Long> exclusiveSizeMap;
    private final Map<String, Long> exclusiveReplicatedSizeMap;

    /**
     * Filter to return deleted keys which are reclaimable based on their presence in previous snapshot in
     * the snapshot chain.
     * @param omSnapshotManager
     * @param snapshotChainManager
     * @param currentSnapshotInfo : If null the deleted keys in AOS needs to be processed, hence the latest snapshot
     *                            in the snapshot chain corresponding to bucket key needs to be processed.
     * @param metadataManager : MetadataManager corresponding to snapshot or AOS.
     * @param lock : Lock for Active OM.
     */
    public ReclaimableKeyFilter(OmSnapshotManager omSnapshotManager, SnapshotChainManager snapshotChainManager,
                                SnapshotInfo currentSnapshotInfo, OMMetadataManager metadataManager,
                                IOzoneManagerLock lock) {
      super(omSnapshotManager, snapshotChainManager, currentSnapshotInfo, metadataManager, lock, 2);
      this.exclusiveSizeMap = new HashMap<>();
      this.exclusiveReplicatedSizeMap = new HashMap<>();
    }

    @Override
    protected String getVolumeName(Table.KeyValue<String, OmKeyInfo> keyValue) throws IOException {
      return keyValue.getValue().getVolumeName();
    }

    @Override
    protected String getBucketName(Table.KeyValue<String, OmKeyInfo> keyValue) throws IOException {
      return keyValue.getValue().getBucketName();
    }

    @Override
    protected Boolean isReclaimable(Table.KeyValue<String, OmKeyInfo> deletedKeyInfo) throws IOException {
      ReferenceCounted<OmSnapshot> previousSnapshot = getPreviousOmSnapshot(1);
      ReferenceCounted<OmSnapshot> previousToPreviousSnapshot = getPreviousOmSnapshot(0);

      Table<String, OmKeyInfo> previousKeyTable = null;
      Table<String, OmKeyInfo> previousPrevKeyTable = null;

      Table<String, String> renamedTable = getMetadataManager().getSnapshotRenamedTable();
      Table<String, String> prevRenamedTable = null;

      SnapshotInfo previousSnapshotInfo = getPreviousSnapshotInfo(1);
      SnapshotInfo prevPrevSnapshotInfo = getPreviousSnapshotInfo(0);

      if (previousSnapshot != null) {
        previousKeyTable = previousSnapshot.get().getMetadataManager().getKeyTable(getBucketInfo().getBucketLayout());
        prevRenamedTable = previousSnapshot.get().getMetadataManager().getSnapshotRenamedTable();
      }
      if (previousToPreviousSnapshot != null) {
        previousPrevKeyTable = previousToPreviousSnapshot.get().getMetadataManager()
            .getKeyTable(getBucketInfo().getBucketLayout());
      }
      if (isKeyReclaimable(previousKeyTable, renamedTable, deletedKeyInfo.getValue(), getBucketInfo(), getVolumeId(),
          null)) {
        return true;
      }
      calculateExclusiveSize(previousSnapshotInfo, prevPrevSnapshotInfo, deletedKeyInfo.getValue(), getBucketInfo(),
          getVolumeId(), renamedTable, previousKeyTable, prevRenamedTable, previousPrevKeyTable, exclusiveSizeMap,
          exclusiveReplicatedSizeMap);
      return false;
    }


    public Map<String, Long> getExclusiveSizeMap() {
      return exclusiveSizeMap;
    }

    public Map<String, Long> getExclusiveReplicatedSizeMap() {
      return exclusiveReplicatedSizeMap;
    }
  }

  protected class ReclaimableDirFilter extends ReclaimableFilter<OmKeyInfo> {

    /**
     * Filter to return deleted directories which are reclaimable based on their presence in previous snapshot in
     * the snapshot chain.
     * @param omSnapshotManager
     * @param snapshotChainManager
     * @param currentSnapshotInfo : If null the deleted keys in AOS needs to be processed, hence the latest snapshot
     *                            in the snapshot chain corresponding to bucket key needs to be processed.
     * @param metadataManager : MetadataManager corresponding to snapshot or AOS.
     * @param lock : Lock for Active OM.
     */
    public ReclaimableDirFilter(OmSnapshotManager omSnapshotManager, SnapshotChainManager snapshotChainManager,
                                SnapshotInfo currentSnapshotInfo, OMMetadataManager metadataManager,
                                IOzoneManagerLock lock) {
      super(omSnapshotManager, snapshotChainManager, currentSnapshotInfo, metadataManager, lock, 1);
    }

    @Override
    protected String getVolumeName(Table.KeyValue<String, OmKeyInfo> keyValue) throws IOException {
      return keyValue.getValue().getVolumeName();
    }

    @Override
    protected String getBucketName(Table.KeyValue<String, OmKeyInfo> keyValue) throws IOException {
      return keyValue.getValue().getBucketName();
    }

    @Override
    protected Boolean isReclaimable(Table.KeyValue<String, OmKeyInfo> deletedDirInfo) throws IOException {
      ReferenceCounted<OmSnapshot> previousSnapshot = getPreviousOmSnapshot(0);
      Table<String, OmDirectoryInfo> prevDirTable = previousSnapshot == null ? null :
          previousSnapshot.get().getMetadataManager().getDirectoryTable();
      return isDirReclaimable(deletedDirInfo, prevDirTable, getMetadataManager().getSnapshotRenamedTable());
    }
  }

  protected class ReclaimableRenameEntryFilter extends ReclaimableFilter<String> {

    /**
     * Filter to return rename table entries which are reclaimable based on the key presence in previous snapshot's
     * keyTable/DirectoryTable in the snapshot chain.
     * @param omSnapshotManager
     * @param snapshotChainManager
     * @param currentSnapshotInfo : If null the deleted keys in AOS needs to be processed, hence the latest snapshot
     *                            in the snapshot chain corresponding to bucket key needs to be processed.
     * @param metadataManager : MetadataManager corresponding to snapshot or AOS.
     * @param lock : Lock for Active OM.
     */
    public ReclaimableRenameEntryFilter(OmSnapshotManager omSnapshotManager, SnapshotChainManager snapshotChainManager,
                                        SnapshotInfo currentSnapshotInfo, OMMetadataManager metadataManager,
                                        IOzoneManagerLock lock) {
      super(omSnapshotManager, snapshotChainManager, currentSnapshotInfo, metadataManager, lock, 1);
    }

    @Override
    protected Boolean isReclaimable(Table.KeyValue<String, String> renameEntry) throws IOException {
      ReferenceCounted<OmSnapshot> previousSnapshot = getPreviousOmSnapshot(0);
      Table<String, OmKeyInfo> previousKeyTable = null;
      Table<String, OmDirectoryInfo> prevDirTable = null;
      if (previousSnapshot != null) {
        previousKeyTable = previousSnapshot.get().getMetadataManager().getKeyTable(getBucketInfo().getBucketLayout());
        prevDirTable = previousSnapshot.get().getMetadataManager().getDirectoryTable();
      }
      return isRenameEntryReclaimable(renameEntry, prevDirTable, previousKeyTable);
    }

    @Override
    protected String getVolumeName(Table.KeyValue<String, String> keyValue) throws IOException {
      return getMetadataManager().splitRenameKey(keyValue.getKey())[0];
    }

    @Override
    protected String getBucketName(Table.KeyValue<String, String> keyValue) throws IOException {
      return getMetadataManager().splitRenameKey(keyValue.getKey())[1];
    }
  }
}
