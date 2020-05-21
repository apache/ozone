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
import java.util.Map;

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMReplayException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles DeleteKey request.
 */
public class OMKeyDeleteRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyDeleteRequest.class);

  public OMKeyDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    DeleteKeyRequest deleteKeyRequest = getOmRequest().getDeleteKeyRequest();
    Preconditions.checkNotNull(deleteKeyRequest);

    OzoneManagerProtocolProtos.KeyArgs keyArgs = deleteKeyRequest.getKeyArgs();

    OzoneManagerProtocolProtos.KeyArgs.Builder newKeyArgs =
        keyArgs.toBuilder().setModificationTime(Time.now());

    return getOmRequest().toBuilder()
        .setDeleteKeyRequest(deleteKeyRequest.toBuilder()
            .setKeyArgs(newKeyArgs)).setUserInfo(getUserInfo()).build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    DeleteKeyRequest deleteKeyRequest = getOmRequest().getDeleteKeyRequest();

    OzoneManagerProtocolProtos.KeyArgs deleteKeyArgs =
        deleteKeyRequest.getKeyArgs();

    String volumeName = deleteKeyArgs.getVolumeName();
    String bucketName = deleteKeyArgs.getBucketName();
    String keyName = deleteKeyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyDeletes();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    Map<String, String> auditMap = buildKeyArgsAuditMap(deleteKeyArgs);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    IOException exception = null;
    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    Result result = null;
    try {
      // check Acl
      checkKeyAcls(ozoneManager, volumeName, bucketName, keyName,
          IAccessAuthorizer.ACLType.DELETE, OzoneObj.ResourceType.KEY);

      String objectKey = omMetadataManager.getOzoneKey(
          volumeName, bucketName, keyName);

      acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
          volumeName, bucketName);

      // Validate bucket and volume exists or not.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(objectKey);
      if (omKeyInfo == null) {
        throw new OMException("Key not found", KEY_NOT_FOUND);
      }

      // Check if this transaction is a replay of ratis logs.
      if (isReplay(ozoneManager, omKeyInfo, trxnLogIndex)) {
        // Replay implies the response has already been returned to
        // the client. So take no further action and return a dummy
        // OMClientResponse.
        throw new OMReplayException();
      }

      // Set the UpdateID to current transactionLogIndex
      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      // Update table cache.
      omMetadataManager.getKeyTable().addCacheEntry(
          new CacheKey<>(omMetadataManager.getOzoneKey(volumeName, bucketName,
              keyName)),
          new CacheValue<>(Optional.absent(), trxnLogIndex));

      // Check recover-setting to update cache of trashTable.
      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
      boolean trashEnabled = omMetadataManager.getBucketTable()
          .getCacheValue(new CacheKey<>(bucketKey)).getCacheValue()
          .getTrashEnabled();
      if (trashEnabled) {
        RepeatedOmKeyInfo repeatedOmKeyInfo = omMetadataManager
            .getTrashTable().get(objectKey);
        repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(omKeyInfo,
            repeatedOmKeyInfo, trxnLogIndex, ozoneManager.isRatisEnabled());

        omMetadataManager.getTrashTable().addCacheEntry(
            new CacheKey<>(objectKey),
            new CacheValue<>(Optional.of(repeatedOmKeyInfo), trxnLogIndex));
      }

      // No need to add cache entries to delete table. As delete table will
      // be used by DeleteKeyService only, not used for any client response
      // validation, so we don't need to add to cache.
      // TODO: Revisit if we need it later.

      omClientResponse = new OMKeyDeleteResponse(omResponse
          .setDeleteKeyResponse(DeleteKeyResponse.newBuilder()).build(),
          omKeyInfo, ozoneManager.isRatisEnabled());

      result = Result.SUCCESS;
    } catch (IOException ex) {
      if (ex instanceof OMReplayException) {
        result = Result.REPLAY;
        omClientResponse = new OMKeyDeleteResponse(createReplayOMResponse(
            omResponse));
      } else {
        result = Result.FAILURE;
        exception = ex;
        omClientResponse = new OMKeyDeleteResponse(createErrorOMResponse(
            omResponse, exception));
      }
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
            omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    // Performing audit logging outside of the lock.
    if (result != Result.REPLAY) {
      auditLog(auditLogger, buildAuditMessage(OMAction.DELETE_KEY, auditMap,
          exception, userInfo));
    }

    switch (result) {
    case SUCCESS:
      omMetrics.decNumKeys();
      LOG.debug("Key deleted. Volume:{}, Bucket:{}, Key:{}", volumeName,
          bucketName, keyName);
      break;
    case REPLAY:
      LOG.debug("Replayed Transaction {} ignored. Request: {}", trxnLogIndex,
          deleteKeyRequest);
      break;
    case FAILURE:
      omMetrics.incNumKeyDeleteFails();
      LOG.error("Key delete failed. Volume:{}, Bucket:{}, Key{}. Exception:{}",
          volumeName, bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyDeleteRequest: {}",
          deleteKeyRequest);
    }

    return omClientResponse;
  }
}
