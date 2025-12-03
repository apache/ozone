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

package org.apache.hadoop.ozone.om.request.key;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_ALREADY_CLOSED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_UNDER_LEASE_RECOVERY;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmFSOFile;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithMetadata;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmKeyHSyncUtil;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles CommitKey request - prefix layout.
 */
public class OMKeyCommitRequestWithFSO extends OMKeyCommitRequest {

  @VisibleForTesting
  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyCommitRequestWithFSO.class);

  public OMKeyCommitRequestWithFSO(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    CommitKeyRequest commitKeyRequest = getOmRequest().getCommitKeyRequest();

    KeyArgs commitKeyArgs = commitKeyRequest.getKeyArgs();

    String volumeName = commitKeyArgs.getVolumeName();
    String bucketName = commitKeyArgs.getBucketName();
    String keyName = commitKeyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    Map<String, String> auditMap = buildKeyArgsAuditMap(commitKeyArgs);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
            getOmRequest());

    Exception exception = null;
    OmKeyInfo omKeyInfo = null;
    OmBucketInfo omBucketInfo;
    OMClientResponse omClientResponse = null;
    boolean bucketLockAcquired = false;
    Result result;
    boolean isHSync = commitKeyRequest.hasHsync() && commitKeyRequest.getHsync();
    boolean isRecovery = commitKeyRequest.hasRecovery() && commitKeyRequest.getRecovery();
    // isHsync = true, a commit request as a result of client side hsync call
    // isRecovery = true, a commit request as a result of client side recoverLease call
    // none of isHsync and isRecovery is true, a commit request as a result of client side normal
    // outputStream#close call.
    if (isHSync) {
      omMetrics.incNumKeyHSyncs();
    } else {
      omMetrics.incNumKeyCommits();
    }

    LOG.debug("isHSync = {}, isRecovery = {}, volumeName = {}, bucketName = {}, keyName = {}",
        isHSync, isRecovery, volumeName, bucketName, keyName);

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    try {
      String dbOpenFileKey = null;

      List<OmKeyLocationInfo>
          locationInfoList = getOmKeyLocationInfos(ozoneManager, commitKeyArgs);

      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      bucketLockAcquired = getOmLockDetails().isLockAcquired();

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);

      String errMsg = "Cannot create file : " + keyName
              + " as parent directory doesn't exist";
      OmFSOFile fsoFile =  new OmFSOFile.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName(keyName)
          .setOmMetadataManager(omMetadataManager)
          .setErrMsg(errMsg)
          .build();

      String fileName = fsoFile.getFileName();
      long volumeId = fsoFile.getVolumeId();
      String dbFileKey = fsoFile.getOzonePathKey();
      OmKeyInfo keyToDelete =
          omMetadataManager.getKeyTable(getBucketLayout()).get(dbFileKey);
      long writerClientId = commitKeyRequest.getClientID();
      boolean isSameHsyncKey = false;
      boolean isOverwrittenHsyncKey = false;
      final String clientIdString = String.valueOf(writerClientId);
      if (null != keyToDelete) {
        isSameHsyncKey = java.util.Optional.of(keyToDelete)
            .map(WithMetadata::getMetadata)
            .map(meta -> meta.get(OzoneConsts.HSYNC_CLIENT_ID))
            .filter(id -> id.equals(clientIdString))
            .isPresent();
        if (!isSameHsyncKey) {
          isOverwrittenHsyncKey = java.util.Optional.of(keyToDelete)
              .map(WithMetadata::getMetadata)
              .map(meta -> meta.get(OzoneConsts.HSYNC_CLIENT_ID))
              .filter(id -> !id.equals(clientIdString))
              .isPresent() && !isRecovery;
        }
      }

      if (isRecovery && keyToDelete != null) {
        String clientId = keyToDelete.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
        if (clientId == null) {
          throw new OMException("Failed to recovery key, as " +
              dbFileKey + " is already closed", KEY_ALREADY_CLOSED);
        }
        writerClientId = Long.parseLong(clientId);
      }
      dbOpenFileKey = fsoFile.getOpenFileName(writerClientId);
      omKeyInfo = OMFileRequest.getOmKeyInfoFromFileTable(true,
              omMetadataManager, dbOpenFileKey, keyName);
      if (omKeyInfo == null) {
        String action = isRecovery ? "recovery" : isHSync ? "hsync" : "commit";
        throw new OMException("Failed to " + action + " key, as " +
            dbOpenFileKey + " entry is not found in the OpenKey table", KEY_NOT_FOUND);
      } else if (omKeyInfo.getMetadata().containsKey(OzoneConsts.DELETED_HSYNC_KEY) ||
          omKeyInfo.getMetadata().containsKey(OzoneConsts.OVERWRITTEN_HSYNC_KEY)) {
        throw new OMException("Open Key " + keyName + " is already deleted/overwritten",
            KEY_NOT_FOUND);
      }

      if (omKeyInfo.getMetadata().containsKey(OzoneConsts.LEASE_RECOVERY) &&
          omKeyInfo.getMetadata().containsKey(OzoneConsts.HSYNC_CLIENT_ID)) {
        if (!isRecovery) {
          throw new OMException("Cannot commit key " + dbOpenFileKey + " with " + OzoneConsts.LEASE_RECOVERY +
              " metadata while recovery flag is not set in request", KEY_UNDER_LEASE_RECOVERY);
        }
      }

      OmKeyInfo openKeyToDelete = null;
      String dbOpenKeyToDeleteKey = null;
      if (isOverwrittenHsyncKey) {
        // find the overwritten openKey and add OVERWRITTEN_HSYNC_KEY to it.
        dbOpenKeyToDeleteKey = fsoFile.getOpenFileName(
            Long.parseLong(keyToDelete.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID)));
        openKeyToDelete = OMFileRequest.getOmKeyInfoFromFileTable(true,
            omMetadataManager, dbOpenKeyToDeleteKey, keyName);
        openKeyToDelete = openKeyToDelete.toBuilder()
            .addMetadata(OzoneConsts.OVERWRITTEN_HSYNC_KEY, "true")
            .setUpdateID(trxnLogIndex)
            .build();
        openKeyToDelete.setModificationTime(Time.now());
        OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager,
            dbOpenKeyToDeleteKey, openKeyToDelete, keyName, trxnLogIndex);
      }

      omKeyInfo.setModificationTime(commitKeyArgs.getModificationTime());
      // non-null indicates it is necessary to update the open key
      OmKeyInfo newOpenKeyInfo = null;

      if (isHSync) {
        if (!OmKeyHSyncUtil.isHSyncedPreviously(omKeyInfo, clientIdString, dbOpenFileKey)) {
          // Update open key as well if it is the first hsync of this key
          omKeyInfo = omKeyInfo.withMetadataMutations(
              metadata -> metadata.put(OzoneConsts.HSYNC_CLIENT_ID, clientIdString));
          newOpenKeyInfo = omKeyInfo.copyObject();
        }
      }

      // Set the new metadata from the request and UpdateID to current
      // transactionLogIndex
      omKeyInfo = omKeyInfo.toBuilder()
          .addAllMetadata(KeyValueUtil.getFromProtobuf(
              commitKeyArgs.getMetadataList()))
          .setDataSize(commitKeyArgs.getDataSize())
          .setUpdateID(trxnLogIndex)
          .build();

      List<OmKeyLocationInfo> uncommitted =
          omKeyInfo.updateLocationInfoList(locationInfoList, false);

      // If bucket versioning is turned on during the update, between key
      // creation and key commit, old versions will be just overwritten and
      // not kept. Bucket versioning will be effective from the first key
      // creation after the knob turned on.
      Map<String, RepeatedOmKeyInfo> oldKeyVersionsToDeleteMap = null;

      validateAtomicRewrite(keyToDelete, omKeyInfo, auditMap);
      // Optimistic locking validation has passed. Now set the rewrite fields to null so they are
      // not persisted in the key table.
      omKeyInfo.setExpectedDataGeneration(null);

      long correctedSpace = omKeyInfo.getReplicatedSize();
      // if keyToDelete isn't null, usedNamespace shouldn't check and increase.
      if (keyToDelete != null && isSameHsyncKey) {
        correctedSpace -= keyToDelete.getReplicatedSize();
        checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
            correctedSpace);
      } else if (keyToDelete != null && !omBucketInfo.getIsVersionEnabled()) {
        RepeatedOmKeyInfo oldVerKeyInfo = getOldVersionsToCleanUp(
            keyToDelete, omBucketInfo.getObjectID(), trxnLogIndex);
        String delKeyName = omMetadataManager
            .getOzoneKey(volumeName, bucketName, fileName);
        // using pseudoObjId as objectId can be same in case of overwrite key
        long pseudoObjId = ozoneManager.getObjectIdFromTxId(trxnLogIndex);
        delKeyName = omMetadataManager.getOzoneDeletePathKey(
            pseudoObjId, delKeyName);
        if (null == oldKeyVersionsToDeleteMap) {
          oldKeyVersionsToDeleteMap = new HashMap<>();
        }

        // Remove any block from oldVerKeyInfo that share the same container ID
        // and local ID with omKeyInfo blocks'.
        // Otherwise, it causes data loss once those shared blocks are added
        // to deletedTable and processed by KeyDeletingService for deletion.
        Pair<Map<OmKeyInfo, List<OmKeyLocationInfo>>, Integer> filteredUsedBlockCnt =
            filterOutBlocksStillInUse(omKeyInfo, oldVerKeyInfo);
        Map<OmKeyInfo, List<OmKeyLocationInfo>> blocks = filteredUsedBlockCnt.getLeft();
        correctedSpace -= blocks.entrySet().stream().mapToLong(filteredKeyBlocks ->
            filteredKeyBlocks.getValue().stream().mapToLong(block -> QuotaUtil.getReplicatedSize(
                block.getLength(), filteredKeyBlocks.getKey().getReplicationConfig())).sum()).sum();
        long totalSize = 0;
        long totalNamespace = 0;
        if (!oldVerKeyInfo.getOmKeyInfoList().isEmpty()) {
          oldKeyVersionsToDeleteMap.put(delKeyName, oldVerKeyInfo);
          List<OmKeyInfo> oldKeys = oldVerKeyInfo.getOmKeyInfoList();
          for (int i = 0; i < oldKeys.size(); i++) {
            OmKeyInfo updatedOlderKeyVersions =
                oldKeys.get(i).withCommittedKeyDeletedFlag(true);
            oldKeys.set(i, updatedOlderKeyVersions);
            totalSize += sumBlockLengths(updatedOlderKeyVersions);
            totalNamespace += 1;
          }
        }
        // Subtract the size of blocks to be overwritten.
        checkBucketQuotaInNamespace(omBucketInfo, 1L);
        checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
            correctedSpace);
        // Subtract the size of blocks to be overwritten.
        omBucketInfo.decrUsedNamespace(totalNamespace, true);
        omBucketInfo.decrUsedNamespace(filteredUsedBlockCnt.getRight(), false);
        omBucketInfo.decrUsedBytes(totalSize, true);
      } else {
        checkBucketQuotaInNamespace(omBucketInfo, 1L);
        checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
            correctedSpace);
      }
      omBucketInfo.incrUsedNamespace(1L);

      // let the uncommitted blocks pretend as key's old version blocks
      // which will be deleted as RepeatedOmKeyInfo
      final OmKeyInfo pseudoKeyInfo = isHSync ? null
          : wrapUncommittedBlocksAsPseudoKey(uncommitted, omKeyInfo);
      if (pseudoKeyInfo != null) {
        String delKeyName = omMetadataManager
            .getOzoneKey(volumeName, bucketName, fileName);
        long pseudoObjId = ozoneManager.getObjectIdFromTxId(trxnLogIndex);
        delKeyName = omMetadataManager.getOzoneDeletePathKey(
            pseudoObjId, delKeyName);
        if (null == oldKeyVersionsToDeleteMap) {
          oldKeyVersionsToDeleteMap = new HashMap<>();
        }
        oldKeyVersionsToDeleteMap.computeIfAbsent(delKeyName,
            key -> new RepeatedOmKeyInfo(omBucketInfo.getObjectID())).addOmKeyInfo(pseudoKeyInfo);
      }

      // Add to cache of open key table and key table.
      if (!isHSync) {
        // If isHSync = false, put a tombstone in OpenKeyTable cache,
        // indicating the key is removed from OpenKeyTable.
        // So that this key can't be committed again.
        OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager,
            dbOpenFileKey, null, keyName, trxnLogIndex);

        // Prevent hsync metadata from getting committed to the final key
        omKeyInfo = omKeyInfo.withMetadataMutations(
            metadata -> metadata.remove(OzoneConsts.HSYNC_CLIENT_ID));
        if (isRecovery) {
          omKeyInfo = omKeyInfo.withMetadataMutations(
              metadata -> metadata.remove(OzoneConsts.LEASE_RECOVERY));
        }
      } else if (newOpenKeyInfo != null) {
        // isHSync is true and newOpenKeyInfo is set, update OpenKeyTable
        OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager,
            dbOpenFileKey, newOpenKeyInfo, keyName, trxnLogIndex);
      }

      OMFileRequest.addFileTableCacheEntry(omMetadataManager, dbFileKey,
              omKeyInfo, fileName, trxnLogIndex);

      omBucketInfo.incrUsedBytes(correctedSpace);

      omClientResponse = new OMKeyCommitResponseWithFSO(omResponse.build(),
          omKeyInfo, dbFileKey, dbOpenFileKey, omBucketInfo.copyObject(),
          oldKeyVersionsToDeleteMap, volumeId, isHSync, newOpenKeyInfo, dbOpenKeyToDeleteKey, openKeyToDelete);

      result = Result.SUCCESS;
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyCommitResponseWithFSO(createErrorOMResponse(
              omResponse, exception), getBucketLayout());
    } finally {
      if (bucketLockAcquired) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // Debug logging for any key commit operation, successful or not
    LOG.debug("Key commit {} with isHSync = {}, omKeyInfo = {}",
        result == Result.SUCCESS ? "succeeded" : "failed", isHSync, omKeyInfo);

    if (!isHSync) {
      markForAudit(auditLogger, buildAuditMessage(OMAction.COMMIT_KEY, auditMap,
              exception, getOmRequest().getUserInfo()));
      processResult(commitKeyRequest, volumeName, bucketName, keyName,
          omMetrics, exception, omKeyInfo, result);
    }

    return omClientResponse;
  }
}
