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
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneManagerVersion;
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
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithMetadata;
import org.apache.hadoop.ozone.om.request.util.OmKeyHSyncUtil;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles CommitKey request.
 */
public class OMKeyCommitRequest extends OMKeyRequest {

  @VisibleForTesting
  private static final Logger LOG = LoggerFactory.getLogger(OMKeyCommitRequest.class);

  public OMKeyCommitRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    OMRequest request = super.preExecute(ozoneManager);
    CommitKeyRequest commitKeyRequest = request.getCommitKeyRequest();
    Objects.requireNonNull(commitKeyRequest, "commitKeyRequest == null");

    KeyArgs keyArgs = commitKeyRequest.getKeyArgs();

    if (keyArgs.hasExpectedDataGeneration()) {
      ozoneManager.checkFeatureEnabled(OzoneManagerVersion.ATOMIC_REWRITE_KEY);
    }

    if (ozoneManager.getConfig().isKeyNameCharacterCheckEnabled()) {
      OmUtils.validateKeyName(keyArgs.getKeyName());
    }

    boolean isHsync = commitKeyRequest.hasHsync() && commitKeyRequest.getHsync();
    boolean isRecovery = commitKeyRequest.hasRecovery() && commitKeyRequest.getRecovery();
    boolean enableHsync = OzoneFSUtils.canEnableHsync(ozoneManager.getConfiguration(), false);

    // If hsynced is called for a file, then this file is hsynced, otherwise it's not hsynced.
    // Currently, file lease recovery by design only supports recover hsynced file
    if ((isHsync || isRecovery) && !enableHsync) {
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
    OmBucketInfo omBucketInfo = null;
    OMClientResponse omClientResponse = null;
    boolean bucketLockAcquired = false;
    Result result;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

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

    try {
      String dbOzoneKey =
          omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);

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
      OmKeyInfo keyToDelete =
          omMetadataManager.getKeyTable(getBucketLayout()).get(dbOzoneKey);
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
              dbOzoneKey + " is already closed", KEY_ALREADY_CLOSED);
        }
        writerClientId = Long.parseLong(clientId);
      }
      String dbOpenKey = omMetadataManager.getOpenKey(volumeName, bucketName,
          keyName, writerClientId);
      omKeyInfo =
          omMetadataManager.getOpenKeyTable(getBucketLayout()).get(dbOpenKey);
      if (omKeyInfo == null) {
        String action = isRecovery ? "recovery" : isHSync ? "hsync" : "commit";
        throw new OMException("Failed to " + action + " key, as " + dbOpenKey +
            " entry is not found in the OpenKey table", KEY_NOT_FOUND);
      } else if (omKeyInfo.getMetadata().containsKey(OzoneConsts.DELETED_HSYNC_KEY) ||
          omKeyInfo.getMetadata().containsKey(OzoneConsts.OVERWRITTEN_HSYNC_KEY)) {
        throw new OMException("Open Key " + keyName + " is already deleted/overwritten",
            KEY_NOT_FOUND);
      }

      if (omKeyInfo.getMetadata().containsKey(OzoneConsts.LEASE_RECOVERY) &&
          omKeyInfo.getMetadata().containsKey(OzoneConsts.HSYNC_CLIENT_ID)) {
        if (!isRecovery) {
          throw new OMException("Cannot commit key " + dbOpenKey + " with " + OzoneConsts.LEASE_RECOVERY +
              " metadata while recovery flag is not set in request", KEY_UNDER_LEASE_RECOVERY);
        }
      }

      OmKeyInfo openKeyToDelete = null;
      String dbOpenKeyToDeleteKey = null;
      if (isOverwrittenHsyncKey) {
        // find the overwritten openKey and add OVERWRITTEN_HSYNC_KEY to it.
        dbOpenKeyToDeleteKey = omMetadataManager.getOpenKey(volumeName, bucketName,
            keyName, Long.parseLong(keyToDelete.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID)));
        openKeyToDelete = omMetadataManager.getOpenKeyTable(getBucketLayout()).get(dbOpenKeyToDeleteKey);
        openKeyToDelete = openKeyToDelete.toBuilder()
            .addMetadata(OzoneConsts.OVERWRITTEN_HSYNC_KEY, "true")
            .setUpdateID(trxnLogIndex)
            .build();
        openKeyToDelete.setModificationTime(Time.now());
        omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
            dbOpenKeyToDeleteKey, openKeyToDelete, trxnLogIndex);
      }

      omKeyInfo.setModificationTime(commitKeyArgs.getModificationTime());
      // non-null indicates it is necessary to update the open key
      OmKeyInfo newOpenKeyInfo = null;

      if (isHSync) {
        if (!OmKeyHSyncUtil.isHSyncedPreviously(omKeyInfo, clientIdString, dbOpenKey)) {
          // Update open key as well if it is the first hsync of this key
          omKeyInfo = omKeyInfo.withMetadataMutations(
              metadata -> metadata.put(OzoneConsts.HSYNC_CLIENT_ID, clientIdString));
          newOpenKeyInfo = omKeyInfo.copyObject();
        }
      }

      validateAtomicRewrite(keyToDelete, omKeyInfo, auditMap);
      // Optimistic locking validation has passed. Now set the rewrite fields to null so they are
      // not persisted in the key table.
      // Combination
      // Set the UpdateID to current transactionLogIndex
      omKeyInfo = omKeyInfo.toBuilder()
          .setExpectedDataGeneration(null)
          .addAllMetadata(KeyValueUtil.getFromProtobuf(
                commitKeyArgs.getMetadataList()))
          .setUpdateID(trxnLogIndex)
          .setDataSize(commitKeyArgs.getDataSize())
          .build();

      // Update the block length for each block, return the allocated but
      // uncommitted blocks
      List<OmKeyLocationInfo> uncommitted =
          omKeyInfo.updateLocationInfoList(locationInfoList, false);

      Map<String, RepeatedOmKeyInfo> oldKeyVersionsToDeleteMap = null;
      long correctedSpace = omKeyInfo.getReplicatedSize();
      // if keyToDelete isn't null, usedNamespace needn't check and
      // increase.
      if (keyToDelete != null && (isSameHsyncKey)) {
        correctedSpace -= keyToDelete.getReplicatedSize();
        checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
            correctedSpace);
      } else if (keyToDelete != null && !omBucketInfo.getIsVersionEnabled()) {
        RepeatedOmKeyInfo oldVerKeyInfo = getOldVersionsToCleanUp(
            keyToDelete, omBucketInfo.getObjectID(), trxnLogIndex);
        // using pseudoObjId as objectId can be same in case of overwrite key
        long pseudoObjId = ozoneManager.getObjectIdFromTxId(trxnLogIndex);
        String delKeyName = omMetadataManager.getOzoneDeletePathKey(
            pseudoObjId, dbOzoneKey);
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
        checkBucketQuotaInNamespace(omBucketInfo, 1L);
        checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
            correctedSpace);
        // Subtract the size of blocks to be overwritten.
        omBucketInfo.decrUsedNamespace(totalNamespace, true);
        // Subtract the used namespace of empty overwritten keys.
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
      oldKeyVersionsToDeleteMap = addKeyInfoToDeleteMap(ozoneManager, trxnLogIndex, dbOzoneKey,
          omBucketInfo.getObjectID(), pseudoKeyInfo, oldKeyVersionsToDeleteMap);

      // Add to cache of open key table and key table.
      if (!isHSync) {
        // If !isHSync = true, put a tombstone in OpenKeyTable cache,
        // indicating the key is removed from OpenKeyTable.
        // So that this key can't be committed again.
        omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
            dbOpenKey, trxnLogIndex);

        // Prevent hsync metadata from getting committed to the final key
        omKeyInfo = omKeyInfo.withMetadataMutations(metadata -> {
          metadata.remove(OzoneConsts.HSYNC_CLIENT_ID);
          if (isRecovery) {
            metadata.remove(OzoneConsts.LEASE_RECOVERY);
          }
        });
      } else if (newOpenKeyInfo != null) {
        // isHSync is true and newOpenKeyInfo is set, update OpenKeyTable
        omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
            dbOpenKey, newOpenKeyInfo, trxnLogIndex);
      }

      omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
          dbOzoneKey, omKeyInfo, trxnLogIndex);

      omBucketInfo.incrUsedBytes(correctedSpace);

      omClientResponse = new OMKeyCommitResponse(omResponse.build(),
          omKeyInfo, dbOzoneKey, dbOpenKey, omBucketInfo.copyObject(),
          oldKeyVersionsToDeleteMap, isHSync, newOpenKeyInfo, dbOpenKeyToDeleteKey, openKeyToDelete);

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
    LOG.debug("Key commit {} with isHSync = {}, isRecovery = {}, omKeyInfo = {}",
        result == Result.SUCCESS ? "succeeded" : "failed", isHSync, isRecovery, omKeyInfo);

    if (!isHSync) {
      markForAudit(auditLogger, buildAuditMessage(OMAction.COMMIT_KEY, auditMap,
              exception, getOmRequest().getUserInfo()));
      processResult(commitKeyRequest, volumeName, bucketName, keyName,
          omMetrics, exception, omKeyInfo, result);
    }

    return omClientResponse;
  }

  @Nonnull
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
        .isAllowed(OMLayoutFeature.HBASE_SUPPORT)) {
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

  /**
   * Validates key commit requests.
   * We do not want to allow clients to perform lease recovery requests
   * until the cluster has finalized the HBase support feature.
   *
   * @param req - the request to validate
   * @param ctx - the validation context
   * @return the validated request
   * @throws OMException if the request is invalid
   */
  @RequestFeatureValidator(
      conditions = ValidationCondition.CLUSTER_NEEDS_FINALIZATION,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CommitKey
  )

  public static OMRequest disallowRecovery(
      OMRequest req, ValidationContext ctx) throws OMException {
    if (!ctx.versionManager()
        .isAllowed(OMLayoutFeature.HBASE_SUPPORT)) {
      CommitKeyRequest commitKeyRequest = req.getCommitKeyRequest();
      boolean isRecovery = commitKeyRequest.hasRecovery() &&
          commitKeyRequest.getRecovery();
      if (isRecovery) {
        throw new OMException("Cluster does not have the HBase support "
            + "feature finalized yet, but the request contains"
            + " an recovery field. Rejecting the request,"
            + " please finalize the cluster upgrade and then try again.",
            OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
      }
    }
    return req;
  }

  protected void validateAtomicRewrite(OmKeyInfo existing, OmKeyInfo toCommit, Map<String, String> auditMap)
      throws OMException {
    if (toCommit.getExpectedDataGeneration() != null) {
      // These values are not passed in the request keyArgs, so add them into the auditMap if they are present
      // in the open key entry.
      auditMap.put(OzoneConsts.REWRITE_GENERATION, String.valueOf(toCommit.getExpectedDataGeneration()));
      if (existing == null) {
        throw new OMException("Atomic rewrite is not allowed for a new key", KEY_NOT_FOUND);
      }
      if (!toCommit.getExpectedDataGeneration().equals(existing.getUpdateID())) {
        throw new OMException("Cannot commit as current generation (" + existing.getUpdateID() +
            ") does not match the expected generation to rewrite (" + toCommit.getExpectedDataGeneration() + ")",
            KEY_NOT_FOUND);
      }
    }
  }

}
