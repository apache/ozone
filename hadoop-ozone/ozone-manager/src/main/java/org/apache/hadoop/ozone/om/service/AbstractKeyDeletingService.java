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

import static org.apache.hadoop.ozone.OzoneConsts.OBJECT_ID_RECLAIM_BLOCKS;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.isBlockLocationInfoSame;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.ozone.common.DeletedBlockGroup;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.DeleteKeysResult;
import org.apache.hadoop.ozone.om.DeletingServiceMetrics;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletedKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgePathRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;

/**
 * Abstracts common code from KeyDeletingService and DirectoryDeletingService
 * which is now used by SnapshotDeletingService as well.
 */
public abstract class AbstractKeyDeletingService extends BackgroundService
    implements BootstrapStateHandler {

  private final OzoneManager ozoneManager;
  private final DeletingServiceMetrics metrics;
  private final OMPerformanceMetrics perfMetrics;
  private final ScmBlockLocationProtocol scmClient;
  private final ClientId clientId = ClientId.randomId();
  private final AtomicLong deletedDirsCount;
  private final AtomicLong movedDirsCount;
  private final AtomicLong movedFilesCount;
  private final AtomicLong runCount;
  private final AtomicLong callId;
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
    this.metrics = ozoneManager.getDeletionMetrics();
    this.perfMetrics = ozoneManager.getPerfMetrics();
    this.callId = new AtomicLong(0);
  }

  protected Pair<Integer, Boolean> processKeyDeletes(List<DeletedBlockGroup> keyBlocksList,
      Map<String, RepeatedOmKeyInfo> keysToModify, List<String> renameEntries,
      String snapTableKey, UUID expectedPreviousSnapshotId) throws IOException, InterruptedException {

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
      long purgeStartTime = Time.monotonicNow();
      purgeResult = submitPurgeKeysRequest(blockDeletionResults,
          keysToModify, renameEntries, snapTableKey, expectedPreviousSnapshotId);
      int limit = ozoneManager.getConfiguration().getInt(OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK,
          OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK_DEFAULT);
      LOG.info("Blocks for {} (out of {}) keys are deleted from DB in {} ms. Limit per task is {}.",
          purgeResult, blockDeletionResults.size(), Time.monotonicNow() - purgeStartTime, limit);
    }
    perfMetrics.setKeyDeletingServiceLatencyMs(Time.monotonicNow() - startTime);
    return purgeResult;
  }

  /**
   * Submits PurgeKeys request for the keys whose blocks have been deleted
   * by SCM.
   * @param results DeleteBlockGroups returned by SCM.
   * @param keysToModify Updated list of RepeatedOmKeyInfo
   */
  private Pair<Integer, Boolean> submitPurgeKeysRequest(List<DeleteBlockGroupResult> results,
      Map<String, RepeatedOmKeyInfo> keysToModify,  List<String> renameEntriesToBeDeleted,
      String snapTableKey, UUID expectedPreviousSnapshotId) throws InterruptedException {
    List<String> purgeKeys = new ArrayList<>();

    // Put all keys to be purged in a list
    int deletedCount = 0;
    Set<String> failedDeletedKeys = new HashSet<>();
    boolean purgeSuccess = true;
    for (DeleteBlockGroupResult result : results) {
      String deletedKey = result.getObjectKey();
      if (result.isSuccess()) {
        // Add key to PurgeKeys list.
        if (keysToModify != null && !keysToModify.containsKey(deletedKey)) {
          // Parse Volume and BucketName
          purgeKeys.add(deletedKey);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Key {} set to be updated in OM DB, Other versions " +
                "of the key that are reclaimable are reclaimed.", deletedKey);
          }
        } else if (keysToModify == null) {
          purgeKeys.add(deletedKey);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Key {} set to be purged from OM DB", deletedKey);
          }
        }
        deletedCount++;
      } else {
        // If the block deletion failed, then the deleted keys should also not be modified.
        failedDeletedKeys.add(deletedKey);
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
    purgeKeysRequest.setExpectedPreviousSnapshotID(expectedPreviousSnapshotNullableUUID.build());
    DeletedKeys deletedKeys = DeletedKeys.newBuilder()
        .setVolumeName("")
        .setBucketName("")
        .addAllKeys(purgeKeys)
        .build();
    purgeKeysRequest.addDeletedKeys(deletedKeys);
    // Adding rename entries to be purged.
    if (renameEntriesToBeDeleted != null) {
      purgeKeysRequest.addAllRenamedKeys(renameEntriesToBeDeleted);
    }
    List<SnapshotMoveKeyInfos> keysToUpdateList = new ArrayList<>();
    if (keysToModify != null) {
      for (Map.Entry<String, RepeatedOmKeyInfo> keyToModify :
          keysToModify.entrySet()) {
        if (failedDeletedKeys.contains(keyToModify.getKey())) {
          continue;
        }
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

      if (!keysToUpdateList.isEmpty()) {
        purgeKeysRequest.addAllKeysToUpdate(keysToUpdateList);
      }
    }

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(Type.PurgeKeys)
        .setPurgeKeysRequest(purgeKeysRequest)
        .setClientId(clientId.toString())
        .build();

    // Submit PurgeKeys request to OM
    try (BootstrapStateHandler.Lock lock = snapTableKey != null ? getBootstrapStateLock().lock() : null) {
      OzoneManagerProtocolProtos.OMResponse omResponse = submitRequest(omRequest);
      if (omResponse != null) {
        purgeSuccess = purgeSuccess && omResponse.getSuccess();
      }
    } catch (ServiceException e) {
      LOG.error("PurgeKey request failed. Will retry at next run.", e);
      return Pair.of(0, false);
    }

    return Pair.of(deletedCount, purgeSuccess);
  }

  protected OzoneManagerProtocolProtos.OMResponse submitRequest(OMRequest omRequest) throws ServiceException {
    return OzoneManagerRatisUtils.submitRequest(ozoneManager, omRequest, clientId, callId.incrementAndGet());
  }

  protected void submitPurgePaths(List<PurgePathRequest> requests,
                                  String snapTableKey,
                                  UUID expectedPreviousSnapshotId) {
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
            .setClientId(clientId.toString())
            .build();

    // Submit Purge paths request to OM
    try {
      submitRequest(omRequest);
    } catch (ServiceException e) {
      LOG.error("PurgePaths request failed. Will retry at next run.", e);
    }
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

  protected PurgePathRequest prepareDeleteDirRequest(
      OmKeyInfo pendingDeletedDirInfo, String delDirName,
      List<Pair<String, OmKeyInfo>> subDirList,
      KeyManager keyManager, long remainingBufLimit) throws IOException {
    // step-0: Get one pending deleted directory
    if (LOG.isDebugEnabled()) {
      LOG.debug("Pending deleted dir name: {}",
          pendingDeletedDirInfo.getKeyName());
    }

    final String[] keys = delDirName.split(OM_KEY_PREFIX);
    final long volumeId = Long.parseLong(keys[1]);
    final long bucketId = Long.parseLong(keys[2]);

    // step-1: get all sub directories under the deletedDir
    DeleteKeysResult subDirDeleteResult =
        keyManager.getPendingDeletionSubDirs(volumeId, bucketId,
            pendingDeletedDirInfo, remainingBufLimit);
    List<OmKeyInfo> subDirs = subDirDeleteResult.getKeysToDelete();
    remainingBufLimit -= subDirDeleteResult.getConsumedSize();

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
    DeleteKeysResult subFileDeleteResult =
        keyManager.getPendingDeletionSubFiles(volumeId, bucketId,
            pendingDeletedDirInfo, remainingBufLimit);
    List<OmKeyInfo> subFiles = subFileDeleteResult.getKeysToDelete();

    if (LOG.isDebugEnabled()) {
      for (OmKeyInfo fileInfo : subFiles) {
        LOG.debug("Moved sub file name: {}", fileInfo.getKeyName());
      }
    }

    // step-3: If both sub-dirs and sub-files are exhausted under a parent
    // directory, only then delete the parent.
    String purgeDeletedDir = subDirDeleteResult.isProcessedKeys() &&
        subFileDeleteResult.isProcessedKeys() ? delDirName :  null;
    return wrapPurgeRequest(volumeId, bucketId,
        purgeDeletedDir, subFiles, subDirs);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public void optimizeDirDeletesAndSubmitRequest(
      long dirNum, long subDirNum, long subFileNum,
      List<Pair<String, OmKeyInfo>> allSubDirList,
      List<PurgePathRequest> purgePathRequestList,
      String snapTableKey, long startTime,
      long remainingBufLimit, KeyManager keyManager,
      UUID expectedPreviousSnapshotId, long rnCnt) {

    // Optimization to handle delete sub-dir and keys to remove quickly
    // This case will be useful to handle when depth of directory is high
    int subdirDelNum = 0;
    int subDirRecursiveCnt = 0;
    int consumedSize = 0;
    while (subDirRecursiveCnt < allSubDirList.size() && remainingBufLimit > 0) {
      try {
        Pair<String, OmKeyInfo> stringOmKeyInfoPair
            = allSubDirList.get(subDirRecursiveCnt);
        PurgePathRequest request = prepareDeleteDirRequest(
            stringOmKeyInfoPair.getValue(),
            stringOmKeyInfoPair.getKey(), allSubDirList, keyManager,
            remainingBufLimit);
        consumedSize += request.getSerializedSize();
        remainingBufLimit -= consumedSize;
        purgePathRequestList.add(request);
        // Count up the purgeDeletedDir, subDirs and subFiles
        if (request.getDeletedDir() != null
            && !request.getDeletedDir().isEmpty()) {
          subdirDelNum++;
        }
        subDirNum += request.getMarkDeletedSubDirsCount();
        subFileNum += request.getDeletedSubFilesCount();
        subDirRecursiveCnt++;
      } catch (IOException e) {
        LOG.error("Error while running delete directories and files " +
            "background task. Will retry at next run for subset.", e);
        break;
      }
    }

    if (!purgePathRequestList.isEmpty()) {
      submitPurgePaths(purgePathRequestList, snapTableKey, expectedPreviousSnapshotId);
    }

    if (dirNum != 0 || subDirNum != 0 || subFileNum != 0) {
      long subdirMoved = subDirNum - subdirDelNum;
      deletedDirsCount.addAndGet(dirNum + subdirDelNum);
      movedDirsCount.addAndGet(subdirMoved);
      movedFilesCount.addAndGet(subFileNum);
      long timeTakenInIteration = Time.monotonicNow() - startTime;
      LOG.info("Number of dirs deleted: {}, Number of sub-dir " +
              "deleted: {}, Number of sub-files moved:" +
              " {} to DeletedTable, Number of sub-dirs moved {} to " +
              "DeletedDirectoryTable, iteration elapsed: {}ms, " +
              " totalRunCount: {}",
          dirNum, subdirDelNum, subFileNum, (subDirNum - subdirDelNum),
          timeTakenInIteration, rnCnt);
      metrics.incrementDirectoryDeletionTotalMetrics(dirNum + subdirDelNum, subDirNum, subFileNum);
      perfMetrics.setDirectoryDeletingServiceLatencyMs(timeTakenInIteration);
    }
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

  public AtomicLong getCallId() {
    return callId;
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

  @Override
  public BootstrapStateHandler.Lock getBootstrapStateLock() {
    return lock;
  }

  /**
   * Submits SetSnapsnapshotPropertyRequest to OM.
   * @param setSnapshotPropertyRequests request to be sent to OM
   */
  protected void submitSetSnapshotRequests(
      List<OzoneManagerProtocolProtos.SetSnapshotPropertyRequest> setSnapshotPropertyRequests) {
    if (setSnapshotPropertyRequests.isEmpty()) {
      return;
    }
    OzoneManagerProtocolProtos.OMRequest omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.SetSnapshotProperty)
        .addAllSetSnapshotPropertyRequests(setSnapshotPropertyRequests)
        .setClientId(clientId.toString())
        .build();
    try {
      submitRequest(omRequest);
    } catch (ServiceException e) {
      LOG.error("Failed to submit set snapshot property request", e);
    }
  }
}
