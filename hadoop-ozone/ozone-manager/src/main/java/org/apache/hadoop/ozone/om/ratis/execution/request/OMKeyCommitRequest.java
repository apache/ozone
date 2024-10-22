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

package org.apache.hadoop.ozone.om.ratis.execution.request;

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithMetadata;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmKeyHSyncUtil;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.DummyOMClientResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.apache.ratis.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_ALREADY_CLOSED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_UNDER_LEASE_RECOVERY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

/**
 * Handles CommitKey request.
 */
public class OMKeyCommitRequest extends OmRequestBase {
  private static final Logger LOG = LoggerFactory.getLogger(OMKeyCommitRequest.class);

  public OMKeyCommitRequest(OMRequest omRequest, OmBucketInfo bucketInfo) {
    super(omRequest, bucketInfo);
  }

  @Override
  public OMRequest preProcess(OzoneManager ozoneManager) throws IOException {
    OMRequest request = super.preProcess(ozoneManager);
    CommitKeyRequest commitKeyRequest = request.getCommitKeyRequest();
    Preconditions.checkNotNull(commitKeyRequest);

    KeyArgs keyArgs = commitKeyRequest.getKeyArgs();

    if (keyArgs.hasExpectedDataGeneration()) {
      ozoneManager.checkFeatureEnabled(OzoneManagerVersion.ATOMIC_REWRITE_KEY);
    }

    // Verify key name
    final boolean checkKeyNameEnabled = ozoneManager.getConfiguration().getBoolean(
        OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_KEY,
        OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_DEFAULT);
    if (checkKeyNameEnabled) {
      OmUtils.validateKeyName(StringUtils.removeEnd(keyArgs.getKeyName(), OzoneConsts.FS_FILE_COPYING_TEMP_SUFFIX));
    }
    boolean isHsync = commitKeyRequest.hasHsync() && commitKeyRequest.getHsync();
    boolean isRecovery = commitKeyRequest.hasRecovery() && commitKeyRequest.getRecovery();
    boolean enableHsync = OzoneFSUtils.canEnableHsync(ozoneManager.getConfiguration(), false);

    // If hsynced is called for a file, then this file is hsynced, otherwise it's not hsynced.
    // Currently, file lease recovery by design only supports recover hsynced file
    if ((isHsync || isRecovery) && !enableHsync) {
      throw new OMException("Hsync is not enabled. To enable, set ozone.fs.hsync.enabled = true",
          NOT_SUPPORTED_OPERATION);
    }

    String keyPath = keyArgs.getKeyName();
    keyPath = OMClientRequest.validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(), keyPath,
        getBucketLayout());

    KeyArgs.Builder newKeyArgs = keyArgs.toBuilder().setVolumeName(getBucketInfo().getVolumeName())
        .setBucketName(getBucketInfo().getBucketName()).setModificationTime(Time.now()).setKeyName(keyPath);
    return request.toBuilder().setCommitKeyRequest(commitKeyRequest.toBuilder().setKeyArgs(newKeyArgs)).build();
  }
  public void authorize(OzoneManager ozoneManager) throws IOException {
    KeyArgs keyArgs = getOmRequest().getCommitKeyRequest().getKeyArgs();
    OmKeyUtils.checkOpenKeyAcls(ozoneManager, keyArgs.getVolumeName(), keyArgs.getBucketName(), keyArgs.getKeyName(),
        IAccessAuthorizer.ACLType.WRITE, OzoneObj.ResourceType.KEY, getOmRequest().getCommitKeyRequest().getClientID(),
        getOmRequest());
  }
  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse process(OzoneManager ozoneManager, TermIndex termIndex) throws IOException {
    final long trxnLogIndex = termIndex.getIndex();
    CommitKeyRequest commitKeyRequest = getOmRequest().getCommitKeyRequest();
    KeyArgs commitKeyArgs = commitKeyRequest.getKeyArgs();

    String volumeName = commitKeyArgs.getVolumeName();
    String bucketName = commitKeyArgs.getBucketName();
    String keyName = commitKeyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    Map<String, String> auditMap = buildKeyArgsAuditMap(commitKeyArgs);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());

    Exception exception = null;
    OmKeyInfo omKeyInfo = null;
    OmBucketInfo omBucketInfo = null;
    OMClientResponse omClientResponse = null;
    OMClientRequest.Result result;

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
      String dbOzoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);

      List<OmKeyLocationInfo> locationInfoList = getOmKeyLocationInfos(ozoneManager, commitKeyArgs);
      omBucketInfo = resolveBucket(ozoneManager, commitKeyArgs.getVolumeName(), commitKeyArgs.getBucketName());

      // Check for directory exists with same name, if it exists throw error.
      if (LOG.isDebugEnabled()) {
        LOG.debug("BucketName: {}, BucketLayout: {}", omBucketInfo.getBucketName(), omBucketInfo.getBucketLayout());
      }

      // If bucket versioning is turned on during the update, between key
      // creation and key commit, old versions will be just overwritten and
      // not kept. Bucket versioning will be effective from the first key
      // creation after the knob turned on.
      OmKeyInfo keyToDelete = null;
      if (ozoneManager.getConfiguration().getBoolean("ozone.om.leader.commit.request.old.key.get", true)) {
        keyToDelete = captureLatencyNs(omMetrics.getKeyCommitGetKeyRate(),
            () -> omMetadataManager.getKeyTable(getBucketLayout()).get(dbOzoneKey));
      }
      // OmKeyInfo keyToDelete = omMetadataManager.getKeyTable(getBucketLayout()).get(dbOzoneKey);
      long writerClientId = commitKeyRequest.getClientID();
      boolean isSameHsyncKey = false;
      boolean isOverwrittenHsyncKey = false;
      final String clientIdString = String.valueOf(writerClientId);
      if (null != keyToDelete) {
        isSameHsyncKey = java.util.Optional.of(keyToDelete).map(WithMetadata::getMetadata)
            .map(meta -> meta.get(OzoneConsts.HSYNC_CLIENT_ID)).filter(id -> id.equals(clientIdString)).isPresent();
        if (!isSameHsyncKey) {
          isOverwrittenHsyncKey = java.util.Optional.of(keyToDelete).map(WithMetadata::getMetadata)
              .map(meta -> meta.get(OzoneConsts.HSYNC_CLIENT_ID)).isPresent() && !isRecovery;
        }
      }

      if (isRecovery && keyToDelete != null) {
        String clientId = keyToDelete.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
        if (clientId == null) {
          throw new OMException("Failed to recovery key, as " + dbOzoneKey + " is already closed", KEY_ALREADY_CLOSED);
        }
        writerClientId = Long.parseLong(clientId);
      }
      String dbOpenKey = omMetadataManager.getOpenKey(volumeName, bucketName, keyName, writerClientId);
      omKeyInfo = captureLatencyNs(omMetrics.getKeyCommitGetOpenKeyRate(),
          () -> omMetadataManager.getOpenKeyTable(getBucketLayout()).get(dbOpenKey));
      if (omKeyInfo == null) {
        String action = isRecovery ? "recovery" : isHSync ? "hsync" : "commit";
        throw new OMException("Failed to " + action + " key, as " + dbOpenKey +
            " entry is not found in the OpenKey table", KEY_NOT_FOUND);
      } else if (omKeyInfo.getMetadata().containsKey(OzoneConsts.DELETED_HSYNC_KEY) ||
          omKeyInfo.getMetadata().containsKey(OzoneConsts.OVERWRITTEN_HSYNC_KEY)) {
        throw new OMException("Open Key " + keyName + " is already deleted/overwritten", KEY_NOT_FOUND);
      }

      if (omKeyInfo.getMetadata().containsKey(OzoneConsts.LEASE_RECOVERY) &&
          omKeyInfo.getMetadata().containsKey(OzoneConsts.HSYNC_CLIENT_ID)) {
        if (!isRecovery) {
          throw new OMException("Cannot commit key " + dbOpenKey + " with " + OzoneConsts.LEASE_RECOVERY +
              " metadata while recovery flag is not set in request", KEY_UNDER_LEASE_RECOVERY);
        }
      }

      if (isOverwrittenHsyncKey) {
        // find the overwritten openKey and add OVERWRITTEN_HSYNC_KEY to it.
        String dbOpenKeyName = omMetadataManager.getOpenKey(volumeName, bucketName,
            keyName, Long.parseLong(keyToDelete.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID)));
        OmKeyInfo dbOpenKeyUpdate = omMetadataManager.getOpenKeyTable(getBucketLayout()).get(dbOpenKeyName);
        dbOpenKeyUpdate.getMetadata().put(OzoneConsts.OVERWRITTEN_HSYNC_KEY, "true");
        dbOpenKeyUpdate.setModificationTime(Time.now());
        dbOpenKeyUpdate.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());
        CodecBuffer openKeyBuf = OmKeyInfo.getCodec(true).toDirectCodecBuffer(dbOpenKeyUpdate);
        changeRecorder().add(omMetadataManager.getOpenKeyTable(getBucketLayout()).getName(), dbOpenKeyName, openKeyBuf);
      }

      omKeyInfo.setModificationTime(commitKeyArgs.getModificationTime());
      // non-null indicates it is necessary to update the open key
      OmKeyInfo newOpenKeyInfo = null;
      if (isHSync) {
        if (!OmKeyHSyncUtil.isHSyncedPreviously(omKeyInfo, clientIdString, dbOpenKey)) {
          // Update open key as well if it is the first hsync of this key
          omKeyInfo.getMetadata().put(OzoneConsts.HSYNC_CLIENT_ID, clientIdString);
          newOpenKeyInfo = omKeyInfo.copyObject();
        }
      }

      validateAtomicRewrite(keyToDelete, omKeyInfo, auditMap);
      // Optimistic locking validation has passed. Now set the rewrite fields to null so they are
      // not persisted in the key table.
      omKeyInfo.setExpectedDataGeneration(null);

      omKeyInfo.getMetadata().putAll(KeyValueUtil.getFromProtobuf(commitKeyArgs.getMetadataList()));
      omKeyInfo.setDataSize(commitKeyArgs.getDataSize());
      // Update the block length for each block, return the allocated but uncommitted blocks
      List<OmKeyLocationInfo> uncommitted = omKeyInfo.updateLocationInfoList(locationInfoList, false);

      // Set the UpdateID to current transactionLogIndex
      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());
      if (keyToDelete != null) {
        omKeyInfo.setObjectID(0);
        omKeyInfo.setObjectID(keyToDelete.getObjectID());
      }

      RepeatedOmKeyInfo blocksToRemove = new RepeatedOmKeyInfo();
      long correctedSpace = omKeyInfo.getReplicatedSize();
      long nameSpaceUsed = 0;
      if (keyToDelete != null && (isSameHsyncKey)) {
        correctedSpace -= keyToDelete.getReplicatedSize();
      } else if (keyToDelete != null && !omBucketInfo.getIsVersionEnabled()) {
        // Subtract the size of blocks to be overwritten.
        correctedSpace -= keyToDelete.getReplicatedSize();
        blocksToRemove.addOmKeyInfo(OmUtils.prepareKeyForDelete(keyToDelete, trxnLogIndex,
            ozoneManager.isRatisEnabled()).getOmKeyInfoList().get(0));
      } else {
        // if keyToDelete isn't null, usedNamespace needn't check and increase.
        nameSpaceUsed = 1;
      }

      // let the uncommitted blocks pretend as key's old version blocks which will be deleted as RepeatedOmKeyInfo
      final OmKeyInfo pseudoKeyInfo = isHSync ? null
          : OmKeyUtils.wrapUncommittedBlocksAsPseudoKey(uncommitted, omKeyInfo);
      if (pseudoKeyInfo != null) {
        blocksToRemove.addOmKeyInfo(pseudoKeyInfo);
      }
      if (blocksToRemove.getOmKeyInfoList().size() > 0) {
        long pseudoObjId = ozoneManager.getObjectIdFromTxId(trxnLogIndex);
        String delKeyName = omMetadataManager.getOzoneDeletePathKey(pseudoObjId, dbOzoneKey);
        // filterOutBlocksStillInUse(omKeyInfo, blocksToRemove);
        CodecBuffer oldVerInfoBuf = RepeatedOmKeyInfo.getCodec(true).toDirectCodecBuffer(blocksToRemove);
        changeRecorder().add(omMetadataManager.getDeletedTable().getName(), delKeyName, oldVerInfoBuf);
      }

      omBucketInfo.incrUsedNamespace(nameSpaceUsed);
      omBucketInfo.incrUsedBytes(correctedSpace);
      OmKeyUtils.checkUpdateBucketQuota(omBucketInfo, correctedSpace, nameSpaceUsed);
      changeRecorder().add(omMetadataManager.getBucketTable().getName(), correctedSpace, nameSpaceUsed);

      // Add to cache of open key table and key table.
      if (!isHSync) {
        changeRecorder().add(omMetadataManager.getKeyTable(getBucketLayout()).getName(), dbOpenKey, null);
        // Prevent hsync metadata from getting committed to the final key
        omKeyInfo.getMetadata().remove(OzoneConsts.HSYNC_CLIENT_ID);
        if (isRecovery) {
          omKeyInfo.getMetadata().remove(OzoneConsts.LEASE_RECOVERY);
        }
      } else if (newOpenKeyInfo != null) {
        // isHSync is true and newOpenKeyInfo is set, update OpenKeyTable
        CodecBuffer newOpenKeyBuf = OmKeyInfo.getCodec(true).toDirectCodecBuffer(newOpenKeyInfo);
        changeRecorder().add(omMetadataManager.getOpenKeyTable(getBucketLayout()).getName(), dbOpenKey, newOpenKeyBuf);
      }

      CodecBuffer omKeyBuf = OmKeyInfo.getCodec(true).toDirectCodecBuffer(omKeyInfo);
      changeRecorder().add(omMetadataManager.getKeyTable(getBucketLayout()).getName(), dbOzoneKey, omKeyBuf);

      omClientResponse = new DummyOMClientResponse(omResponse.build());
      result = OMClientRequest.Result.SUCCESS;
    } catch (IOException | InvalidPathException ex) {
      result = OMClientRequest.Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyCommitResponse(OmKeyUtils.createErrorOMResponse(
          omResponse, exception), getBucketLayout());
    }

    // Debug logging for any key commit operation, successful or not
    LOG.debug("Key commit {} with isHSync = {}, isRecovery = {}, omKeyInfo = {}",
        result == OMClientRequest.Result.SUCCESS ? "succeeded" : "failed", isHSync, isRecovery, omKeyInfo);

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
                               OMClientRequest.Result result) {
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
