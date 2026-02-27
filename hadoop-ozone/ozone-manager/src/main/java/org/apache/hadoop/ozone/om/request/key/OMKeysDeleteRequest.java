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

import static org.apache.hadoop.ozone.OzoneConsts.BUCKET;
import static org.apache.hadoop.ozone.OzoneConsts.DATA_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.DELETED_HSYNC_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.DELETED_KEYS_LIST;
import static org.apache.hadoop.ozone.OzoneConsts.KEY;
import static org.apache.hadoop.ozone.OzoneConsts.REPLICATION_CONFIG;
import static org.apache.hadoop.ozone.OzoneConsts.UNDELETED_KEYS_LIST;
import static org.apache.hadoop.ozone.OzoneConsts.VOLUME;
import static org.apache.hadoop.ozone.audit.OMAction.DELETE_KEYS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.PARTIAL_DELETE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.ErrorInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.OMClientRequestUtils;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeysDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles DeleteKey request.
 */
public class OMKeysDeleteRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeysDeleteRequest.class);

  public OMKeysDeleteRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();
    DeleteKeysRequest deleteKeyRequest = getOmRequest().getDeleteKeysRequest();

    OzoneManagerProtocolProtos.DeleteKeyArgs deleteKeyArgs =
        deleteKeyRequest.getDeleteKeys();

    List<String> deleteKeys = new ArrayList<>(deleteKeyArgs.getKeysList());
    List<OmKeyInfo> deleteKeysInfo = new ArrayList<>();

    Exception exception = null;
    OMClientResponse omClientResponse = null;
    Result result = null;
    Map<String, ErrorInfo> keyToError = new HashMap<>();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyDeletes();
    OMPerformanceMetrics perfMetrics = ozoneManager.getPerfMetrics();
    String volumeName = deleteKeyArgs.getVolumeName();
    String bucketName = deleteKeyArgs.getBucketName();
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(VOLUME, volumeName);
    auditMap.put(BUCKET, bucketName);
    List<OmKeyInfo> omKeyInfoList = new ArrayList<>();
    // dirList is applicable for FSO implementation
    List<OmKeyInfo> dirList = new ArrayList<>();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    boolean acquiredLock = false;

    int indexFailed = 0;
    int length = deleteKeys.size();
    OzoneManagerProtocolProtos.DeleteKeyArgs.Builder unDeletedKeys =
        OzoneManagerProtocolProtos.DeleteKeyArgs.newBuilder()
            .setVolumeName(volumeName).setBucketName(bucketName);

    boolean deleteStatus = true;
    long startNanos = Time.monotonicNowNanos();
    try {
      long startNanosDeleteKeysResolveBucketLatency = Time.monotonicNowNanos();
      ResolvedBucket bucket = ozoneManager.resolveBucketLink(Pair.of(volumeName, bucketName), this);
      perfMetrics.setDeleteKeysResolveBucketLatencyNs(
              Time.monotonicNowNanos() - startNanosDeleteKeysResolveBucketLatency);
      bucket.audit(auditMap);
      volumeName = bucket.realVolume();
      bucketName = bucket.realBucket();

      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();
      // Validate bucket and volume exists or not.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      String volumeOwner = getVolumeOwner(omMetadataManager, volumeName);

      for (indexFailed = 0; indexFailed < length; indexFailed++) {
        String keyName = deleteKeyArgs.getKeys(indexFailed);
        String objectKey =
            omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
        OmKeyInfo omKeyInfo = getOmKeyInfo(ozoneManager, omMetadataManager,
            volumeName, bucketName, keyName);

        if (omKeyInfo == null) {
          deleteStatus = false;
          LOG.error("Received a request to delete a Key does not exist {}",
              objectKey);
          deleteKeys.remove(keyName);
          unDeletedKeys.addKeys(keyName);
          keyToError.put(keyName, new ErrorInfo(OMException.ResultCodes.KEY_NOT_FOUND.name(), "Key does not exist"));
          continue;
        }

        try {
          // check Acl
          long startNanosDeleteKeysAclCheckLatency = Time.monotonicNowNanos();
          checkKeyAcls(ozoneManager, volumeName, bucketName, keyName,
              IAccessAuthorizer.ACLType.DELETE, OzoneObj.ResourceType.KEY,
              volumeOwner);
          perfMetrics.setDeleteKeysAclCheckLatencyNs(Time.monotonicNowNanos() - startNanosDeleteKeysAclCheckLatency);
          OzoneFileStatus fileStatus = getOzoneKeyStatus(
              ozoneManager, omMetadataManager, volumeName, bucketName, keyName);
          addKeyToAppropriateList(omKeyInfoList, omKeyInfo, dirList,
              fileStatus);
          deleteKeysInfo.add(omKeyInfo);
        } catch (Exception ex) {
          deleteStatus = false;
          LOG.error("Acl check failed for Key: {}", objectKey, ex);
          deleteKeys.remove(keyName);
          unDeletedKeys.addKeys(keyName);
          keyToError.put(keyName, new ErrorInfo(OMException.ResultCodes.ACCESS_DENIED.name(), "ACL check failed"));
        }
      }

      OmBucketInfo omBucketInfo =
          getBucketInfo(omMetadataManager, volumeName, bucketName);

      Map<String, OmKeyInfo> openKeyInfoMap = new HashMap<>();
      // Mark all keys which can be deleted, in cache as deleted.
      Pair<Long, Integer> quotaReleasedEmptyKeys =
          markKeysAsDeletedInCache(ozoneManager, trxnLogIndex, omKeyInfoList,
              dirList, omMetadataManager, openKeyInfoMap);
      omBucketInfo.decrUsedBytes(quotaReleasedEmptyKeys.getKey(), true);
      // For empty keyInfos the quota should be released and not added to namespace.
      omBucketInfo.decrUsedNamespace(omKeyInfoList.size() + dirList.size() -
              quotaReleasedEmptyKeys.getValue(), true);
      omBucketInfo.decrUsedNamespace(quotaReleasedEmptyKeys.getValue(), false);

      final long volumeId = omMetadataManager.getVolumeId(volumeName);
      omClientResponse =
          getOmClientResponse(ozoneManager, omKeyInfoList, dirList, omResponse,
              unDeletedKeys, keyToError, deleteStatus, omBucketInfo, volumeId, openKeyInfoMap);

      result = Result.SUCCESS;
      long endNanosDeleteKeySuccessLatencyNs = Time.monotonicNowNanos();
      perfMetrics.setDeleteKeySuccessLatencyNs(endNanosDeleteKeySuccessLatencyNs - startNanos);
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      createErrorOMResponse(omResponse, exception);

      // reset deleteKeys as request failed.
      deleteKeys = new ArrayList<>();
      deleteKeysInfo.clear();
      // Add all keys which are failed due to any other exception .
      for (int i = indexFailed; i < length; i++) {
        unDeletedKeys.addKeys(deleteKeyArgs.getKeys(i));
        keyToError.put(deleteKeyArgs.getKeys(i), new ErrorInfo(OMException.ResultCodes.INTERNAL_ERROR.name(),
            ex.getMessage()));
      }

      omResponse.setDeleteKeysResponse(
          DeleteKeysResponse.newBuilder().setStatus(false)
              .setUnDeletedKeys(unDeletedKeys).build()).build();
      omClientResponse =
          new OMKeysDeleteResponse(omResponse.build(), getBucketLayout());
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

    addDeletedKeys(auditMap, deleteKeysInfo, unDeletedKeys.getKeysList());

    markForAudit(auditLogger,
        buildAuditMessage(DELETE_KEYS, auditMap, exception, userInfo));

    switch (result) {
    case SUCCESS:
      omMetrics.decNumKeys(deleteKeys.size());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Keys delete success. Volume:{}, Bucket:{}, Keys:{}",
            volumeName, bucketName, auditMap.get(DELETED_KEYS_LIST));
      }
      break;
    case FAILURE:
      omMetrics.incNumKeyDeleteFails();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Keys delete failed. Volume:{}, Bucket:{}, DeletedKeys:{}, "
                + "UnDeletedKeys:{}", volumeName, bucketName,
            auditMap.get(DELETED_KEYS_LIST), auditMap.get(UNDELETED_KEYS_LIST),
            exception);
      }
      break;
    default:
      LOG.error("Unrecognized Result for OMKeysDeleteRequest: {}",
          deleteKeyRequest);
    }

    return omClientResponse;
  }

  protected OzoneFileStatus getOzoneKeyStatus(
      OzoneManager ozoneManager, OMMetadataManager omMetadataManager,
      String volumeName, String bucketName, String keyName) throws IOException {
    // implemented in child class
    return null;
  }

  @Nonnull
  @SuppressWarnings("parameternumber")
  protected OMClientResponse getOmClientResponse(OzoneManager ozoneManager,
      List<OmKeyInfo> omKeyInfoList, List<OmKeyInfo> dirList,
      OMResponse.Builder omResponse,
      OzoneManagerProtocolProtos.DeleteKeyArgs.Builder unDeletedKeys,
      Map<String, ErrorInfo> keyToErrors,
      boolean deleteStatus, OmBucketInfo omBucketInfo, long volumeId, Map<String, OmKeyInfo> openKeyInfoMap) {
    OMClientResponse omClientResponse;
    List<OzoneManagerProtocolProtos.DeleteKeyError> deleteKeyErrors = new ArrayList<>();
    for (Map.Entry<String, ErrorInfo>  key : keyToErrors.entrySet()) {
      deleteKeyErrors.add(OzoneManagerProtocolProtos.DeleteKeyError.newBuilder().setKey(key.getKey())
          .setErrorCode(key.getValue().getCode()).setErrorMsg(key.getValue().getMessage()).build());
    }
    omClientResponse = new OMKeysDeleteResponse(omResponse
        .setDeleteKeysResponse(
            DeleteKeysResponse.newBuilder().setStatus(deleteStatus)
                .setUnDeletedKeys(unDeletedKeys).addAllErrors(deleteKeyErrors))
        .setStatus(deleteStatus ? OK : PARTIAL_DELETE).setSuccess(deleteStatus)
        .build(), omKeyInfoList,
        omBucketInfo.copyObject(), openKeyInfoMap);
    return omClientResponse;
  }

  protected Pair<Long, Integer> markKeysAsDeletedInCache(OzoneManager ozoneManager,
      long trxnLogIndex, List<OmKeyInfo> omKeyInfoList, List<OmKeyInfo> dirList,
      OMMetadataManager omMetadataManager, Map<String, OmKeyInfo> openKeyInfoMap)
          throws IOException {
    int emptyKeys = 0;
    long quotaReleased = 0;
    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      String volumeName = omKeyInfo.getVolumeName();
      String bucketName = omKeyInfo.getBucketName();
      String keyName = omKeyInfo.getKeyName();
      omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
          new CacheKey<>(omMetadataManager.getOzoneKey(volumeName, bucketName, keyName)),
          CacheValue.get(trxnLogIndex));

      omKeyInfo = omKeyInfo.toBuilder()
          .setUpdateID(trxnLogIndex)
          .build();
      quotaReleased += sumBlockLengths(omKeyInfo);
      emptyKeys += OmKeyInfo.isKeyEmpty(omKeyInfo) ? 1 : 0;

      // If omKeyInfo has hsync metadata, delete its corresponding open key as well
      String hsyncClientId = omKeyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
      if (hsyncClientId != null) {
        Table<String, OmKeyInfo> openKeyTable = omMetadataManager.getOpenKeyTable(getBucketLayout());
        String dbOpenKey = omMetadataManager.getOpenKey(volumeName, bucketName, keyName, hsyncClientId);
        OmKeyInfo openKeyInfo = openKeyTable.get(dbOpenKey);
        if (openKeyInfo != null) {
          openKeyInfo = openKeyInfo.withMetadataMutations(
              metadata -> metadata.put(DELETED_HSYNC_KEY, "true"));
          openKeyTable.addCacheEntry(dbOpenKey, openKeyInfo, trxnLogIndex);
          // Add to the map of open keys to be deleted.
          openKeyInfoMap.put(dbOpenKey, openKeyInfo);
        } else {
          LOG.warn("Potentially inconsistent DB state: open key not found with dbOpenKey '{}'", dbOpenKey);
        }
      }
    }
    return Pair.of(quotaReleased, emptyKeys);
  }

  protected void addKeyToAppropriateList(List<OmKeyInfo> omKeyInfoList,
      OmKeyInfo omKeyInfo, List<OmKeyInfo> dirList, OzoneFileStatus keyStatus) {
    omKeyInfoList.add(omKeyInfo);
  }

  protected OmKeyInfo getOmKeyInfo(
      OzoneManager ozoneManager, OMMetadataManager omMetadataManager,
      String volume, String bucket, String key) throws IOException {
    String objectKey = omMetadataManager.getOzoneKey(volume, bucket, key);
    return omMetadataManager.getKeyTable(getBucketLayout()).get(objectKey);
  }

  /**
   * Add key info to audit map for DeleteKeys request.
   */
  protected static void addDeletedKeys(Map<String, String> auditMap,
      List<OmKeyInfo> deletedKeyInfos, List<String> unDeletedKeys) {
    StringBuilder keys = new StringBuilder();
    for (int i = 0; i < deletedKeyInfos.size(); i++) {
      OmKeyInfo key = deletedKeyInfos.get(i);
      keys.append('{').append(KEY).append('=').append(key.getKeyName()).append(", ");
      keys.append(DATA_SIZE).append('=').append(key.getDataSize()).append(", ");
      keys.append(REPLICATION_CONFIG).append('=').append(key.getReplicationConfig()).append('}');
      if (i < deletedKeyInfos.size() - 1) {
        keys.append(", ");
      }
    }
    auditMap.put(DELETED_KEYS_LIST, keys.toString());
    auditMap.put(UNDELETED_KEYS_LIST, String.join(",", unDeletedKeys));
  }

  /**
   * Validates delete key requests.
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
      requestType = Type.DeleteKeys
  )
  public static OMRequest blockDeleteKeysWithBucketLayoutFromOldClient(
      OMRequest req, ValidationContext ctx) throws IOException {
    if (req.getDeleteKeysRequest().hasDeleteKeys()) {
      final DeleteKeyArgs keyArgs = req.getDeleteKeysRequest().getDeleteKeys();

      OMClientRequestUtils.validateVolumeName(keyArgs.getVolumeName());
      OMClientRequestUtils.validateBucketName(keyArgs.getBucketName());

      final BucketLayout bucketLayout = ctx.getBucketLayout(
          keyArgs.getVolumeName(), keyArgs.getBucketName());
      bucketLayout.validateSupportedOperation();
    }
    return req;
  }
}
