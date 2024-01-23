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

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithMetadata;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles CommitKey request.
 */
public class OMKeyCommitRequest extends OMKeyRequest {

  @VisibleForTesting
  public static final Logger LOG =
      LoggerFactory.getLogger(OMKeyCommitRequest.class);

  public OMKeyCommitRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    OMRequest request = super.preExecute(ozoneManager);
    CommitKeyRequest commitKeyRequest = request.getCommitKeyRequest();
    Preconditions.checkNotNull(commitKeyRequest);

    KeyArgs keyArgs = commitKeyRequest.getKeyArgs();

    // Verify key name
    final boolean checkKeyNameEnabled = ozoneManager.getConfiguration()
         .getBoolean(OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_KEY,
                 OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_DEFAULT);
    if (checkKeyNameEnabled) {
      OmUtils.validateKeyName(StringUtils.removeEnd(keyArgs.getKeyName(),
              OzoneConsts.FS_FILE_COPYING_TEMP_SUFFIX));
    }
    boolean isHsync = commitKeyRequest.hasHsync() &&
        commitKeyRequest.getHsync();
    boolean enableHsync = ozoneManager.getConfiguration().getBoolean(
        OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED,
        OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED_DEFAULT);
    if (isHsync && !enableHsync) {
      throw new OMException("Hsync is not enabled. To enable, " +
          "set ozone.fs.hsync.enabled = true", NOT_SUPPORTED_OPERATION);
    }

    String keyPath = keyArgs.getKeyName();
    keyPath = validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(),
        keyPath, getBucketLayout());

    KeyArgs.Builder newKeyArgs =
        keyArgs.toBuilder().setModificationTime(Time.now())
            .setKeyName(keyPath);

    KeyArgs resolvedKeyArgs =
        resolveBucketAndCheckOpenKeyAcls(newKeyArgs.build(), ozoneManager,
            IAccessAuthorizer.ACLType.WRITE, commitKeyRequest.getClientID());

    return request.toBuilder()
        .setCommitKeyRequest(commitKeyRequest.toBuilder()
            .setKeyArgs(resolvedKeyArgs)).build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, TermIndex termIndex) {
    final long trxnLogIndex = termIndex.getIndex();

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

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    boolean isHSync = commitKeyRequest.hasHsync() &&
            commitKeyRequest.getHsync();

    if (isHSync) {
      omMetrics.incNumKeyHSyncs();
    } else {
      omMetrics.incNumKeyCommits();
    }

    LOG.debug("isHSync = {}, volumeName = {}, bucketName = {}, keyName = {}",
        isHSync, volumeName, bucketName, keyName);

    try {
      String dbOzoneKey =
          omMetadataManager.getOzoneKey(volumeName, bucketName,
              keyName);
      String dbOpenKey = omMetadataManager.getOpenKey(volumeName, bucketName,
          keyName, commitKeyRequest.getClientID());

      List<OmKeyLocationInfo>
          locationInfoList = getOmKeyLocationInfos(ozoneManager, commitKeyArgs);

      mergeOmLockDetails(
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
              bucketName));
      bucketLockAcquired = getOmLockDetails().isLockAcquired();

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);

      // Check for directory exists with same name, if it exists throw error.
      if (LOG.isDebugEnabled()) {
        LOG.debug("BucketName: {}, BucketLayout: {}",
            omBucketInfo.getBucketName(), omBucketInfo.getBucketLayout());
      }
      if (omBucketInfo.getBucketLayout()
          .shouldNormalizePaths(ozoneManager.getEnableFileSystemPaths())) {
        if (checkDirectoryAlreadyExists(volumeName, bucketName, keyName,
            omMetadataManager)) {
          throw new OMException("Can not create file: " + keyName +
              " as there is already directory in the given path", NOT_A_FILE);
        }
        // Ensure the parent exist.
        if (!"".equals(OzoneFSUtils.getParent(keyName))
            && !checkDirectoryAlreadyExists(volumeName, bucketName,
            OzoneFSUtils.getParent(keyName), omMetadataManager)) {
          throw new OMException("Cannot create file : " + keyName
              + " as parent directory doesn't exist",
              OMException.ResultCodes.DIRECTORY_NOT_FOUND);
        }
      }

      // If bucket versioning is turned on during the update, between key
      // creation and key commit, old versions will be just overwritten and
      // not kept. Bucket versioning will be effective from the first key
      // creation after the knob turned on.
      boolean isPreviousCommitHsync = false;
      Map<String, RepeatedOmKeyInfo> oldKeyVersionsToDeleteMap = null;
      OmKeyInfo keyToDelete =
          omMetadataManager.getKeyTable(getBucketLayout()).get(dbOzoneKey);
      if (null != keyToDelete) {
        final String clientIdString
            = String.valueOf(commitKeyRequest.getClientID());
        isPreviousCommitHsync = java.util.Optional.ofNullable(keyToDelete)
            .map(WithMetadata::getMetadata)
            .map(meta -> meta.get(OzoneConsts.HSYNC_CLIENT_ID))
            .filter(id -> id.equals(clientIdString))
            .isPresent();
      }

      omKeyInfo =
          omMetadataManager.getOpenKeyTable(getBucketLayout()).get(dbOpenKey);
      if (omKeyInfo == null) {
        String action = "commit";
        if (isHSync) {
          action = "hsync";
        }
        throw new OMException("Failed to " + action + " key, as " + dbOpenKey +
            "entry is not found in the OpenKey table", KEY_NOT_FOUND);
      }
      omKeyInfo.getMetadata().putAll(KeyValueUtil.getFromProtobuf(
          commitKeyArgs.getMetadataList()));
      if (isHSync) {
        omKeyInfo.getMetadata().put(OzoneConsts.HSYNC_CLIENT_ID,
            String.valueOf(commitKeyRequest.getClientID()));
      }
      omKeyInfo.setDataSize(commitKeyArgs.getDataSize());
      omKeyInfo.setModificationTime(commitKeyArgs.getModificationTime());
      // Update the block length for each block, return the allocated but
      // uncommitted blocks
      List<OmKeyLocationInfo> uncommitted =
          omKeyInfo.updateLocationInfoList(locationInfoList, false);

      // Set the UpdateID to current transactionLogIndex
      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      long correctedSpace = omKeyInfo.getReplicatedSize();
      // if keyToDelete isn't null, usedNamespace needn't check and
      // increase.
      if (keyToDelete != null && (isHSync || isPreviousCommitHsync)) {
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
        String delKeyName = omMetadataManager.getOzoneDeletePathKey(
            keyToDelete.getObjectID(), dbOzoneKey);
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
      final OmKeyInfo pseudoKeyInfo = isHSync ? null
          : wrapUncommittedBlocksAsPseudoKey(uncommitted, omKeyInfo);
      if (pseudoKeyInfo != null) {
        long pseudoObjId = ozoneManager.getObjectIdFromTxId(trxnLogIndex);
        String delKeyName = omMetadataManager.getOzoneDeletePathKey(
            pseudoObjId, dbOzoneKey);
        if (null == oldKeyVersionsToDeleteMap) {
          oldKeyVersionsToDeleteMap = new HashMap<>();
        }
        oldKeyVersionsToDeleteMap.put(delKeyName,
            new RepeatedOmKeyInfo(pseudoKeyInfo));
      }

      // Add to cache of open key table and key table.
      if (!isHSync) {
        // If isHSync = false, put a tombstone in OpenKeyTable cache,
        // indicating the key is removed from OpenKeyTable.
        // So that this key can't be committed again.
        omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
            dbOpenKey, trxnLogIndex);
      }

      omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
          dbOzoneKey, omKeyInfo, trxnLogIndex);

      omBucketInfo.incrUsedBytes(correctedSpace);

      omClientResponse = new OMKeyCommitResponse(omResponse.build(),
          omKeyInfo, dbOzoneKey, dbOpenKey, omBucketInfo.copyObject(),
          oldKeyVersionsToDeleteMap, isHSync);

      result = Result.SUCCESS;
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyCommitResponse(createErrorOMResponse(
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
      auditLog(auditLogger, buildAuditMessage(OMAction.COMMIT_KEY, auditMap,
              exception, getOmRequest().getUserInfo()));
      processResult(commitKeyRequest, volumeName, bucketName, keyName,
          omMetrics, exception, omKeyInfo, result);
    }

    return omClientResponse;
  }

  @NotNull
  protected List<OmKeyLocationInfo> getOmKeyLocationInfos(
      OzoneManager ozoneManager, KeyArgs commitKeyArgs) {
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    for (KeyLocation keyLocation : commitKeyArgs.getKeyLocationsList()) {
      OmKeyLocationInfo locationInfo =
          OmKeyLocationInfo.getFromProtobuf(keyLocation);

      // Strip out tokens before adding to cache.
      // This way during listStatus token information does not pass on to
      // client when returning from cache.
      if (ozoneManager.isGrpcBlockTokenEnabled()) {
        locationInfo.setToken(null);
      }
      locationInfoList.add(locationInfo);
    }
    return locationInfoList;
  }

  /**
   * Process result of om request execution.
   *
   * @param commitKeyRequest commit key request
   * @param volumeName       volume name
   * @param bucketName       bucket name
   * @param keyName          key name
   * @param omMetrics        om metrics
   * @param exception        exception trace
   * @param omKeyInfo        omKeyInfo
   * @param result           result
   * @param result           stores the result of the execution
   */
  @SuppressWarnings("parameternumber")
  protected void processResult(CommitKeyRequest commitKeyRequest,
                               String volumeName, String bucketName,
                               String keyName, OMMetrics omMetrics,
                               Exception exception, OmKeyInfo omKeyInfo,
                               Result result) {
    switch (result) {
    case SUCCESS:
      // As when we commit the key, then it is visible in ozone, so we should
      // increment here.
      // As key also can have multiple versions, we need to increment keys
      // only if version is 0. Currently we have not complete support of
      // versioning of keys. So, this can be revisited later.
      if (omKeyInfo.getKeyLocationVersions().size() == 1) {
        omMetrics.incNumKeys();
      }
      if (commitKeyRequest.getKeyArgs().hasEcReplicationConfig()) {
        omMetrics.incEcKeysTotal();
      }
      omMetrics.incDataCommittedBytes(omKeyInfo.getDataSize());
      LOG.debug("Key committed. Volume:{}, Bucket:{}, Key:{}", volumeName,
              bucketName, keyName);
      break;
    case FAILURE:
      LOG.error("Key committed failed. Volume:{}, Bucket:{}, Key:{}. " +
          "Exception:{}", volumeName, bucketName, keyName, exception);
      if (commitKeyRequest.getKeyArgs().hasEcReplicationConfig()) {
        omMetrics.incEcKeyCreateFailsTotal();
      }
      omMetrics.incNumKeyCommitFails();
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyCommitRequest: {}",
              commitKeyRequest);
    }
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.CLUSTER_NEEDS_FINALIZATION,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CommitKey
  )
  public static OMRequest disallowCommitKeyWithECReplicationConfig(
      OMRequest req, ValidationContext ctx) throws OMException {
    if (!ctx.versionManager()
        .isAllowed(OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT)) {
      if (req.getCommitKeyRequest().getKeyArgs().hasEcReplicationConfig()) {
        throw new OMException("Cluster does not have the Erasure Coded"
            + " Storage support feature finalized yet, but the request contains"
            + " an Erasure Coded replication type. Rejecting the request,"
            + " please finalize the cluster upgrade and then try again.",
            OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
      }
    }
    return req;
  }

  /**
   * Validates key commit requests.
   * We do not want to allow older clients to commit keys associated with
   * buckets which use non LEGACY layouts.
   *
   * @param req - the request to validate
   * @param ctx - the validation context
   * @return the validated request
   * @throws OMException if the request is invalid
   */
  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CommitKey
  )
  public static OMRequest blockCommitKeyWithBucketLayoutFromOldClient(
      OMRequest req, ValidationContext ctx) throws IOException {
    if (req.getCommitKeyRequest().hasKeyArgs()) {
      KeyArgs keyArgs = req.getCommitKeyRequest().getKeyArgs();

      if (keyArgs.hasVolumeName() && keyArgs.hasBucketName()) {
        BucketLayout bucketLayout = ctx.getBucketLayout(
            keyArgs.getVolumeName(), keyArgs.getBucketName());
        bucketLayout.validateSupportedOperation();
      }
    }
    return req;
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.CLUSTER_NEEDS_FINALIZATION,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CommitKey
  )
  public static OMRequest disallowHsync(
      OMRequest req, ValidationContext ctx) throws OMException {
    if (!ctx.versionManager()
        .isAllowed(OMLayoutFeature.HSYNC)) {
      CommitKeyRequest commitKeyRequest = req.getCommitKeyRequest();
      boolean isHSync = commitKeyRequest.hasHsync() &&
          commitKeyRequest.getHsync();
      if (isHSync) {
        throw new OMException("Cluster does not have the hsync support "
            + "feature finalized yet, but the request contains"
            + " an hsync field. Rejecting the request,"
            + " please finalize the cluster upgrade and then try again.",
            OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
      }
    }
    return req;
  }
}
