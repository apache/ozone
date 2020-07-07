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
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeysDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles DeleteKey request.
 */
public class OMKeysDeleteRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeysDeleteRequest.class);

  public OMKeysDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    DeleteKeysRequest deleteKeyRequest =
        getOmRequest().getDeleteKeysRequest();
    Preconditions.checkNotNull(deleteKeyRequest);
    List<KeyArgs> newKeyArgsList = new ArrayList<>();
    for (KeyArgs keyArgs : deleteKeyRequest.getKeyArgsList()) {
      newKeyArgsList.add(
          keyArgs.toBuilder().setModificationTime(Time.now()).build());
    }
    DeleteKeysRequest newDeleteKeyRequest = DeleteKeysRequest
        .newBuilder().addAllKeyArgs(newKeyArgsList).build();

    return getOmRequest().toBuilder()
        .setDeleteKeysRequest(newDeleteKeyRequest)
        .setUserInfo(getUserInfo()).build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    DeleteKeysRequest deleteKeyRequest =
        getOmRequest().getDeleteKeysRequest();

    List<KeyArgs> deleteKeyArgsList = deleteKeyRequest.getKeyArgsList();

    IOException exception = null;
    OMClientResponse omClientResponse = null;
    Result result = null;

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyDeletes();
    Map<String, String> auditMap = null;
    String volumeName = "";
    String bucketName = "";
    String keyName = "";
    List<OmKeyInfo> omKeyInfoList = new ArrayList<>();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo =
        getOmRequest().getUserInfo();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    // As right now, only client exposed API is for a single volume and
    // bucket. So, all entries will have same volume name and bucket name.
    // So, we can validate once.
    if (deleteKeyArgsList.size() > 0) {
      volumeName = deleteKeyArgsList.get(0).getVolumeName();
      bucketName = deleteKeyArgsList.get(0).getBucketName();
    }

    boolean acquiredLock =
        omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
            bucketName);

    int indexFailed = 0;
    try {

      // Validate bucket and volume exists or not.
      if (deleteKeyArgsList.size() > 0) {
        validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      }


      // Check if any of the key in the batch cannot be deleted. If exists the
      // batch delete will be failed.

      for (indexFailed = 0; indexFailed < deleteKeyArgsList.size();
           indexFailed++) {
        KeyArgs deleteKeyArgs = deleteKeyArgsList.get(0);
        auditMap = buildKeyArgsAuditMap(deleteKeyArgs);
        volumeName = deleteKeyArgs.getVolumeName();
        bucketName = deleteKeyArgs.getBucketName();
        keyName = deleteKeyArgs.getKeyName();
        String objectKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
            keyName);
        OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(objectKey);


        // Do we need to fail the batch if one of the key does not exist?
        // For now following the previous code behavior. If this code changes
        // behavior, this will be incompatible change across upgrades, and we
        // need to version the Requests and do logic accordingly.

        if (omKeyInfo == null) {
          LOG.error("Key does not exist {}", objectKey);
          throw new OMException("Key Not Found " + objectKey, KEY_NOT_FOUND);
        }

        // check Acl
        checkKeyAcls(ozoneManager, volumeName, bucketName, keyName,
            IAccessAuthorizer.ACLType.DELETE, OzoneObj.ResourceType.KEY);

        omKeyInfoList.add(omKeyInfo);
      }


      // Mark all keys in cache as deleted.
      for (KeyArgs deleteKeyArgs : deleteKeyArgsList) {
        volumeName = deleteKeyArgs.getVolumeName();
        bucketName = deleteKeyArgs.getBucketName();
        keyName = deleteKeyArgs.getKeyName();
        omMetadataManager.getKeyTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getOzoneKey(volumeName, bucketName,
                keyName)),
            new CacheValue<>(Optional.absent(), trxnLogIndex));
      }


      omClientResponse = new OMKeysDeleteResponse(omResponse
          .setDeleteKeysResponse(DeleteKeysResponse.newBuilder()
              .setStatus(true)).build(), omKeyInfoList, trxnLogIndex,
          ozoneManager.isRatisEnabled());
      result = Result.SUCCESS;

    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      createErrorOMResponse(omResponse, ex);
      omResponse.setDeleteKeysResponse(DeleteKeysResponse.newBuilder()
          .setStatus(false).build()).build();
      omClientResponse = new OMKeysDeleteResponse(omResponse.build());

    } finally {
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
    }

    // When we get any error during iteration build the remaining audit map
    // from deleteKeyArgsList.
    for (int i = indexFailed; i < deleteKeyArgsList.size(); i++) {
      buildKeyArgsAuditMap(deleteKeyArgsList.get(i));
    }

    auditLog(auditLogger, buildAuditMessage(
        OMAction.DELETE_KEYS, auditMap, exception, userInfo));


    switch (result) {
    case SUCCESS:
      omMetrics.decNumKeys();
      LOG.debug("Key deleted. Volume:{}, Bucket:{}, Key:{}", volumeName,
          bucketName, keyName);
      break;
    case FAILURE:
      omMetrics.incNumKeyDeleteFails();
      LOG.error("Key delete failed. Volume:{}, Bucket:{}, Key{}." +
          " Exception:{}", volumeName, bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyDeleteRequest: {}",
          deleteKeyRequest);
    }

    return omClientResponse;
  }
}
