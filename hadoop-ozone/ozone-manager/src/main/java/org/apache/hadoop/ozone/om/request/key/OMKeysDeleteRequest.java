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

import com.google.common.base.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeysDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.OzoneConsts.BUCKET;
import static org.apache.hadoop.ozone.OzoneConsts.DELETED_KEYS_LIST;
import static org.apache.hadoop.ozone.OzoneConsts.UNDELETED_KEYS_LIST;
import static org.apache.hadoop.ozone.OzoneConsts.VOLUME;
import static org.apache.hadoop.ozone.audit.OMAction.DELETE_KEYS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.PARTIAL_DELETE;

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
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    DeleteKeysRequest deleteKeyRequest = getOmRequest().getDeleteKeysRequest();

    OzoneManagerProtocolProtos.DeleteKeyArgs deleteKeyArgs =
        deleteKeyRequest.getDeleteKeys();

    List<String> deleteKeys = new ArrayList<>(deleteKeyArgs.getKeysList());

    IOException exception = null;
    OMClientResponse omClientResponse = null;
    Result result = null;

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyDeletes();
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
    try {
      ResolvedBucket bucket =
          ozoneManager.resolveBucketLink(Pair.of(volumeName, bucketName), this);
      bucket.audit(auditMap);
      volumeName = bucket.realVolume();
      bucketName = bucket.realBucket();

      acquiredLock = omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName);
      // Validate bucket and volume exists or not.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      String volumeOwner = getVolumeOwner(omMetadataManager, volumeName);

      for (indexFailed = 0; indexFailed < length; indexFailed++) {
        String keyName = deleteKeyArgs.getKeys(indexFailed);
        String objectKey =
            omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
        OmKeyInfo omKeyInfo =
            getOmKeyInfo(omMetadataManager, volumeName, bucketName, keyName);

        if (omKeyInfo == null) {
          deleteStatus = false;
          LOG.error("Received a request to delete a Key does not exist {}",
              objectKey);
          deleteKeys.remove(keyName);
          unDeletedKeys.addKeys(keyName);
          continue;
        }

        try {
          // check Acl
          checkKeyAcls(ozoneManager, volumeName, bucketName, keyName,
              IAccessAuthorizer.ACLType.DELETE, OzoneObj.ResourceType.KEY,
              volumeOwner);
          addKeyToAppropriateList(omKeyInfoList, omKeyInfo, dirList,
              getOzoneKeyStatus(omMetadataManager, volumeName, bucketName,
                  keyName));
        } catch (Exception ex) {
          deleteStatus = false;
          LOG.error("Acl check failed for Key: {}", objectKey, ex);
          deleteKeys.remove(keyName);
          unDeletedKeys.addKeys(keyName);
        }
      }

      long quotaReleased = 0;
      OmBucketInfo omBucketInfo =
          getBucketInfo(omMetadataManager, volumeName, bucketName);

      // Mark all keys which can be deleted, in cache as deleted.
      quotaReleased =
          markKeysAsDeletedInCache(ozoneManager, trxnLogIndex, omKeyInfoList,
              dirList, omMetadataManager, quotaReleased);
      omBucketInfo.incrUsedBytes(-quotaReleased);
      omBucketInfo.incrUsedNamespace(-1L * omKeyInfoList.size());

      final long volumeId = omMetadataManager.getVolumeId(volumeName);
      omClientResponse =
          getOmClientResponse(ozoneManager, omKeyInfoList, dirList, omResponse,
              unDeletedKeys, deleteStatus, omBucketInfo, volumeId);

      result = Result.SUCCESS;

    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      createErrorOMResponse(omResponse, ex);

      // reset deleteKeys as request failed.
      deleteKeys = new ArrayList<>();
      // Add all keys which are failed due to any other exception .
      for (int i = indexFailed; i < length; i++) {
        unDeletedKeys.addKeys(deleteKeyArgs.getKeys(i));
      }

      omResponse.setDeleteKeysResponse(
          DeleteKeysResponse.newBuilder().setStatus(false)
              .setUnDeletedKeys(unDeletedKeys).build()).build();
      omClientResponse =
          new OMKeysDeleteResponse(omResponse.build(), getBucketLayout());

    } finally {
      if (acquiredLock) {
        omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName);
      }
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
    }

    addDeletedKeys(auditMap, deleteKeys, unDeletedKeys.getKeysList());

    auditLog(auditLogger,
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
      OMMetadataManager omMetadataManager, String volumeName, String bucketName,
      String keyName) throws IOException {
    // implemented in child class
    return null;
  }

  @NotNull
  @SuppressWarnings("parameternumber")
  protected OMClientResponse getOmClientResponse(OzoneManager ozoneManager,
      List<OmKeyInfo> omKeyInfoList, List<OmKeyInfo> dirList,
      OMResponse.Builder omResponse,
      OzoneManagerProtocolProtos.DeleteKeyArgs.Builder unDeletedKeys,
      boolean deleteStatus, OmBucketInfo omBucketInfo, long volumeId) {
    OMClientResponse omClientResponse;
    omClientResponse = new OMKeysDeleteResponse(omResponse
        .setDeleteKeysResponse(
            DeleteKeysResponse.newBuilder().setStatus(deleteStatus)
                .setUnDeletedKeys(unDeletedKeys))
        .setStatus(deleteStatus ? OK : PARTIAL_DELETE).setSuccess(deleteStatus)
        .build(), omKeyInfoList, ozoneManager.isRatisEnabled(),
        omBucketInfo.copyObject());
    return omClientResponse;
  }

  protected long markKeysAsDeletedInCache(OzoneManager ozoneManager,
      long trxnLogIndex, List<OmKeyInfo> omKeyInfoList, List<OmKeyInfo> dirList,
      OMMetadataManager omMetadataManager, long quotaReleased)
          throws IOException {
    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
          new CacheKey<>(omMetadataManager
              .getOzoneKey(omKeyInfo.getVolumeName(), omKeyInfo.getBucketName(),
                  omKeyInfo.getKeyName())),
          new CacheValue<>(Optional.absent(), trxnLogIndex));

      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());
      quotaReleased += sumBlockLengths(omKeyInfo);
    }
    return quotaReleased;
  }

  protected void addKeyToAppropriateList(List<OmKeyInfo> omKeyInfoList,
      OmKeyInfo omKeyInfo, List<OmKeyInfo> dirList, OzoneFileStatus keyStatus) {
    omKeyInfoList.add(omKeyInfo);
  }

  protected OmKeyInfo getOmKeyInfo(OMMetadataManager omMetadataManager,
      String volume, String bucket, String key) throws IOException {
    String objectKey = omMetadataManager.getOzoneKey(volume, bucket, key);
    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable(getBucketLayout()).get(objectKey);
    return omKeyInfo;
  }

  /**
   * Add key info to audit map for DeleteKeys request.
   */
  protected static void addDeletedKeys(Map<String, String> auditMap,
      List<String> deletedKeys, List<String> unDeletedKeys) {
    auditMap.put(DELETED_KEYS_LIST, String.join(",", deletedKeys));
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
      DeleteKeyArgs keyArgs = req.getDeleteKeysRequest().getDeleteKeys();

      if (keyArgs.hasVolumeName() && keyArgs.hasBucketName()) {
        BucketLayout bucketLayout = ctx.getBucketLayout(
            keyArgs.getVolumeName(), keyArgs.getBucketName());
        bucketLayout.validateSupportedOperation();
      }
    }
    return req;
  }
}
