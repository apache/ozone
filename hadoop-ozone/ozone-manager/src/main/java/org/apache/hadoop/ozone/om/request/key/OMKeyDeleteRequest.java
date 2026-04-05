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

import static org.apache.hadoop.ozone.OzoneConsts.DELETED_HSYNC_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequestUtils;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles DeleteKey request.
 */
public class OMKeyDeleteRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyDeleteRequest.class);

  public OMKeyDeleteRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    DeleteKeyRequest deleteKeyRequest = super.preExecute(ozoneManager)
        .getDeleteKeyRequest();
    Objects.requireNonNull(deleteKeyRequest, "deleteKeyRequest == null");

    OzoneManagerProtocolProtos.KeyArgs keyArgs = deleteKeyRequest.getKeyArgs();
    String keyPath = keyArgs.getKeyName();

    OmUtils.verifyKeyNameWithSnapshotReservedWordForDeletion(keyPath);
    keyPath = normalizeKeyPath(ozoneManager.getEnableFileSystemPaths(), keyPath, getBucketLayout());

    OzoneManagerProtocolProtos.KeyArgs.Builder newKeyArgs =
        keyArgs.toBuilder().setModificationTime(Time.now()).setKeyName(keyPath);

    KeyArgs resolvedArgs = resolveBucketAndCheckAcls(ozoneManager, newKeyArgs);
    return getOmRequest().toBuilder()
        .setDeleteKeyRequest(deleteKeyRequest.toBuilder()
            .setKeyArgs(resolvedArgs))
        .setUserInfo(getUserIfNotExists(ozoneManager)).build();
  }

  protected KeyArgs resolveBucketAndCheckAcls(OzoneManager ozoneManager,
      KeyArgs.Builder newKeyArgs) throws IOException {
    return captureLatencyNs(
          ozoneManager.getPerfMetrics().getDeleteKeyResolveBucketAndAclCheckLatencyNs(),
          () -> resolveBucketAndCheckKeyAcls(newKeyArgs.build(), ozoneManager, ACLType.DELETE));
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();
    DeleteKeyRequest deleteKeyRequest = getOmRequest().getDeleteKeyRequest();

    OzoneManagerProtocolProtos.KeyArgs keyArgs = deleteKeyRequest.getKeyArgs();
    Map<String, String> auditMap = buildLightKeyArgsAuditMap(keyArgs);

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyDeletes();
    OMPerformanceMetrics perfMetrics = ozoneManager.getPerfMetrics();
    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    Exception exception = null;
    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    Result result = null;
    long startNanos = Time.monotonicNowNanos();
    try {
      String objectKey =
          omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);

      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      // Validate bucket and volume exists or not.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      OmKeyInfo omKeyInfo =
          omMetadataManager.getKeyTable(getBucketLayout()).get(objectKey);
      if (omKeyInfo == null) {
        throw new OMException("Key not found", KEY_NOT_FOUND);
      }

      // Set the UpdateID to current transactionLogIndex
      omKeyInfo = omKeyInfo.toBuilder()
          .setUpdateID(trxnLogIndex)
          .build();

      // Update table cache. Put a tombstone entry
      omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
          new CacheKey<>(
              omMetadataManager.getOzoneKey(volumeName, bucketName, keyName)),
          CacheValue.get(trxnLogIndex));

      OmBucketInfo omBucketInfo =
          getBucketInfo(omMetadataManager, volumeName, bucketName);

      long quotaReleased = sumBlockLengths(omKeyInfo);
      // Empty entries won't be added to deleted table so this key shouldn't get added to snapshotUsed space.
      boolean isKeyNonEmpty = !OmKeyInfo.isKeyEmpty(omKeyInfo);
      omBucketInfo.decrUsedBytes(quotaReleased, isKeyNonEmpty);
      omBucketInfo.decrUsedNamespace(1L, isKeyNonEmpty);
      OmKeyInfo deletedOpenKeyInfo = null;

      // If omKeyInfo has hsync metadata, delete its corresponding open key as well
      String dbOpenKey = null;
      String hsyncClientId = omKeyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
      if (hsyncClientId != null) {
        Table<String, OmKeyInfo> openKeyTable = omMetadataManager.getOpenKeyTable(getBucketLayout());
        dbOpenKey = omMetadataManager.getOpenKey(volumeName, bucketName, keyName, hsyncClientId);
        OmKeyInfo openKeyInfo = openKeyTable.get(dbOpenKey);
        if (openKeyInfo != null) {
          openKeyInfo = openKeyInfo.withMetadataMutations(
              metadata -> metadata.put(DELETED_HSYNC_KEY, "true"));
          openKeyTable.addCacheEntry(dbOpenKey, openKeyInfo, trxnLogIndex);
          deletedOpenKeyInfo = openKeyInfo;
        } else {
          LOG.warn("Potentially inconsistent DB state: open key not found with dbOpenKey '{}'", dbOpenKey);
        }
      }

      omClientResponse = new OMKeyDeleteResponse(
          omResponse.setDeleteKeyResponse(DeleteKeyResponse.newBuilder())
              .build(), omKeyInfo,
          omBucketInfo.copyObject(), deletedOpenKeyInfo);
      if (omKeyInfo.isFile()) {
        auditMap.put(OzoneConsts.DATA_SIZE, String.valueOf(omKeyInfo.getDataSize()));
        auditMap.put(OzoneConsts.REPLICATION_CONFIG, omKeyInfo.getReplicationConfig().toString());
      }

      result = Result.SUCCESS;
      long endNanosDeleteKeySuccessLatencyNs = Time.monotonicNowNanos();
      perfMetrics.setDeleteKeySuccessLatencyNs(endNanosDeleteKeySuccessLatencyNs - startNanos);
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse =
          new OMKeyDeleteResponse(createErrorOMResponse(omResponse, exception),
              getBucketLayout());
      long endNanosDeleteKeyFailureLatencyNs = Time.monotonicNowNanos();
      perfMetrics.setDeleteKeyFailureLatencyNs(endNanosDeleteKeyFailureLatencyNs - startNanos);
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // Performing audit logging outside of the lock.
    markForAudit(auditLogger,
        buildAuditMessage(OMAction.DELETE_KEY, auditMap, exception, userInfo));

    switch (result) {
    case SUCCESS:
      omMetrics.decNumKeys();
      LOG.debug("Key deleted. Volume:{}, Bucket:{}, Key:{}", volumeName,
          bucketName, keyName);
      break;
    case FAILURE:
      omMetrics.incNumKeyDeleteFails();
      LOG.error("Key delete failed. Volume:{}, Bucket:{}, Key:{}.", volumeName,
          bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyDeleteRequest: {}",
          deleteKeyRequest);
    }

    return omClientResponse;
  }

  /**
   * Validates key delete requests.
   * We do not want to allow older clients to delete keys in buckets which use
   * non LEGACY layouts.
   *
   * @param req - the request to validate
   * @param ctx - the validation context
   * @return the validated request
   * @throws OMException if the request is invalid
   */
  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.DeleteKey
  )
  public static OMRequest blockDeleteKeyWithBucketLayoutFromOldClient(
      OMRequest req, ValidationContext ctx) throws IOException {

    final KeyArgs keyArgs = req.getDeleteKeyRequest().getKeyArgs();

    OMClientRequestUtils.validateVolumeName(keyArgs.getVolumeName());
    OMClientRequestUtils.validateBucketName(keyArgs.getBucketName());

    final BucketLayout bucketLayout = ctx.getBucketLayout(
        keyArgs.getVolumeName(), keyArgs.getBucketName());
    bucketLayout.validateSupportedOperation();

    return req;
  }
}
