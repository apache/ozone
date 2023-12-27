/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.key;

import java.nio.file.InvalidPathException;
import java.util.HashMap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithMetadata;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_ALREADY_CLOSED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_UNDER_LEASE_RECOVERY;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles CommitKey request - prefix layout.
 */
public class OMKeyCommitRequestWithFSO extends OMKeyCommitRequest {

  @VisibleForTesting
  public static final Logger LOG =
      LoggerFactory.getLogger(OMKeyCommitRequestWithFSO.class);

  public OMKeyCommitRequestWithFSO(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

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
    OmBucketInfo omBucketInfo = null;
    OMClientResponse omClientResponse = null;
    boolean bucketLockAcquired = false;
    Result result;
    boolean isHSync = commitKeyRequest.hasHsync() && commitKeyRequest.getHsync();
    boolean isRecovery = commitKeyRequest.hasRecovery() && commitKeyRequest.getRecovery();
    boolean realCommit = (!isHSync) || (isHSync && isRecovery);
    if (!realCommit) {
      omMetrics.incNumKeyHSyncs();
    } else {
      omMetrics.incNumKeyCommits();
    }

    LOG.debug("isHSync = {}, isRecovery = {}, volumeName = {}, bucketName = {}, keyName = {}",
        isHSync, isRecovery, volumeName, bucketName, keyName);

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    try {
      commitKeyArgs = resolveBucketLink(ozoneManager, commitKeyArgs, auditMap);
      volumeName = commitKeyArgs.getVolumeName();
      bucketName = commitKeyArgs.getBucketName();

      // check Acl
      checkKeyAclsInOpenKeyTable(ozoneManager, volumeName, bucketName,
              keyName, IAccessAuthorizer.ACLType.WRITE,
              commitKeyRequest.getClientID());

      Iterator<Path> pathComponents = Paths.get(keyName).iterator();
      String dbOpenFileKey = null;

      List<OmKeyLocationInfo>
          locationInfoList = getOmKeyLocationInfos(ozoneManager, commitKeyArgs);

      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      bucketLockAcquired = getOmLockDetails().isLockAcquired();

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      String fileName = OzoneFSUtils.getFileName(keyName);
      omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);
      final long volumeId = omMetadataManager.getVolumeId(volumeName);
      final long bucketId = omMetadataManager.getBucketId(
              volumeName, bucketName);
      long parentID = OMFileRequest.getParentID(volumeId, bucketId,
              pathComponents, keyName, omMetadataManager,
              "Cannot create file : " + keyName
              + " as parent directory doesn't exist");
      String dbFileKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
              parentID, fileName);
      OmKeyInfo keyToDelete =
          omMetadataManager.getKeyTable(getBucketLayout()).get(dbFileKey);
      long writerClientId = commitKeyRequest.getClientID();
      if (isRecovery && keyToDelete != null) {
        String clientId = keyToDelete.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
        if (clientId == null) {
          throw new OMException("Failed to recovery key, as " +
              dbFileKey + " is already closed", KEY_ALREADY_CLOSED);
        }
        writerClientId = Long.parseLong(clientId);
      }

      dbOpenFileKey = omMetadataManager.getOpenFileName(volumeId, bucketId,
          parentID, fileName, writerClientId);
      omKeyInfo = OMFileRequest.getOmKeyInfoFromFileTable(true,
              omMetadataManager, dbOpenFileKey, keyName);
      if (omKeyInfo == null) {
        String action = isRecovery ? "recovery" : isHSync ? "hsync" : "commit";
        throw new OMException("Failed to " + action + " key, as " +
            dbOpenFileKey + " entry is not found in the OpenKey table", KEY_NOT_FOUND);
      }

      if (omKeyInfo.getMetadata().containsKey(OzoneConsts.LEASE_RECOVERY) &&
          omKeyInfo.getMetadata().containsKey(OzoneConsts.HSYNC_CLIENT_ID)) {
        if (!isRecovery) {
          throw new OMException("Cannot commit key " + dbOpenFileKey + " with " + OzoneConsts.LEASE_RECOVERY +
              " metadata while recovery flag is not set in request", KEY_UNDER_LEASE_RECOVERY);
        }
      }

      omKeyInfo.getMetadata().putAll(KeyValueUtil.getFromProtobuf(
          commitKeyArgs.getMetadataList()));
      if (!realCommit) {
        omKeyInfo.getMetadata().put(OzoneConsts.HSYNC_CLIENT_ID, String.valueOf(writerClientId));
      } else if (isRecovery) {
        omKeyInfo.getMetadata().remove(OzoneConsts.HSYNC_CLIENT_ID);
        omKeyInfo.getMetadata().remove(OzoneConsts.LEASE_RECOVERY);
      }

      omKeyInfo.setDataSize(commitKeyArgs.getDataSize());
      omKeyInfo.setModificationTime(commitKeyArgs.getModificationTime());

      List<OmKeyLocationInfo> uncommitted =
          omKeyInfo.updateLocationInfoList(locationInfoList, false);

      // Set the UpdateID to current transactionLogIndex
      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      // If bucket versioning is turned on during the update, between key
      // creation and key commit, old versions will be just overwritten and
      // not kept. Bucket versioning will be effective from the first key
      // creation after the knob turned on.
      boolean isPreviousCommitHsync = false;
      Map<String, RepeatedOmKeyInfo> oldKeyVersionsToDeleteMap = null;
      if (null != keyToDelete) {
        final String clientIdString = String.valueOf(writerClientId);
        isPreviousCommitHsync = java.util.Optional.ofNullable(keyToDelete)
            .map(WithMetadata::getMetadata)
            .map(meta -> meta.get(OzoneConsts.HSYNC_CLIENT_ID))
            .filter(id -> id.equals(clientIdString))
            .isPresent();
      }

      long correctedSpace = omKeyInfo.getReplicatedSize();

      // if keyToDelete isn't null, usedNamespace shouldn't check and
      // increase.
      if (keyToDelete != null && (!realCommit || isPreviousCommitHsync)) {
        correctedSpace -= keyToDelete.getReplicatedSize();
        checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
            correctedSpace);
      } else if (keyToDelete != null && !omBucketInfo.getIsVersionEnabled()) {
        // Subtract the size of blocks to be overwritten.
        correctedSpace -= keyToDelete.getReplicatedSize();
        RepeatedOmKeyInfo oldVerKeyInfo = getOldVersionsToCleanUp(
            keyToDelete, trxnLogIndex, ozoneManager.isRatisEnabled());
        checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
            correctedSpace);
        String delKeyName = omMetadataManager
            .getOzoneKey(volumeName, bucketName, fileName);
        delKeyName = omMetadataManager.getOzoneDeletePathKey(
            keyToDelete.getObjectID(), delKeyName);
        if (null == oldKeyVersionsToDeleteMap) {
          oldKeyVersionsToDeleteMap = new HashMap<>();
        }

        // Remove any block from oldVerKeyInfo that share the same container ID
        // and local ID with omKeyInfo blocks'.
        // Otherwise, it causes data loss once those shared blocks are added
        // to deletedTable and processed by KeyDeletingService for deletion.
        filterOutBlocksStillInUse(omKeyInfo, oldVerKeyInfo);

        if (!oldVerKeyInfo.getOmKeyInfoList().isEmpty()) {
          oldKeyVersionsToDeleteMap.put(delKeyName, oldVerKeyInfo);
        }
      } else {
        checkBucketQuotaInNamespace(omBucketInfo, 1L);
        checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
            correctedSpace);
        omBucketInfo.incrUsedNamespace(1L);
      }

      // let the uncommitted blocks pretend as key's old version blocks
      // which will be deleted as RepeatedOmKeyInfo
      final OmKeyInfo pseudoKeyInfo = !realCommit ? null
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
        oldKeyVersionsToDeleteMap.put(delKeyName,
            new RepeatedOmKeyInfo(pseudoKeyInfo));
      }

      // Add to cache of open key table and key table.
      if (realCommit) {
        // If isHSync = false, put a tombstone in OpenKeyTable cache,
        // indicating the key is removed from OpenKeyTable.
        // So that this key can't be committed again.
        OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager,
            dbOpenFileKey, null, fileName, trxnLogIndex);
      }

      OMFileRequest.addFileTableCacheEntry(omMetadataManager, dbFileKey,
              omKeyInfo, fileName, trxnLogIndex);

      omBucketInfo.incrUsedBytes(correctedSpace);

      omClientResponse = new OMKeyCommitResponseWithFSO(omResponse.build(),
              omKeyInfo, dbFileKey, dbOpenFileKey, omBucketInfo.copyObject(),
          oldKeyVersionsToDeleteMap, volumeId, realCommit);

      result = Result.SUCCESS;
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyCommitResponseWithFSO(createErrorOMResponse(
              omResponse, exception), getBucketLayout());
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
              omDoubleBufferHelper);

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

    if (realCommit) {
      auditLog(auditLogger, buildAuditMessage(OMAction.COMMIT_KEY, auditMap,
              exception, getOmRequest().getUserInfo()));
      processResult(commitKeyRequest, volumeName, bucketName, keyName,
          omMetrics, exception, omKeyInfo, result);
    }

    return omClientResponse;
  }
}
