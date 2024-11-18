/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.request.s3.tagging;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequestUtils;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tagging.S3PutObjectTaggingResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PutObjectTaggingRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PutObjectTaggingResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.ratis.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles put object tagging request.
 */
public class S3PutObjectTaggingRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3PutObjectTaggingRequest.class);

  public S3PutObjectTaggingRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    PutObjectTaggingRequest putObjectTaggingRequest =
        super.preExecute(ozoneManager).getPutObjectTaggingRequest();
    Preconditions.checkNotNull(putObjectTaggingRequest);

    KeyArgs keyArgs = putObjectTaggingRequest.getKeyArgs();

    String keyPath = keyArgs.getKeyName();
    keyPath = validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(),
        keyPath, getBucketLayout());

    KeyArgs.Builder newKeyArgs =
        keyArgs.toBuilder()
            .setKeyName(keyPath);

    KeyArgs resolvedArgs = resolveBucketAndCheckKeyAcls(newKeyArgs.build(),
        ozoneManager, ACLType.WRITE);
    return getOmRequest().toBuilder()
        .setUserInfo(getUserInfo())
        .setPutObjectTaggingRequest(
            putObjectTaggingRequest.toBuilder().setKeyArgs(resolvedArgs))
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, TermIndex termIndex) {
    final long trxnLogIndex = termIndex.getIndex();

    PutObjectTaggingRequest putObjectTaggingRequest = getOmRequest().getPutObjectTaggingRequest();

    KeyArgs keyArgs = putObjectTaggingRequest.getKeyArgs();
    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumPutObjectTagging();

    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    IOException exception = null;
    Result result = null;
    try {
      mergeOmLockDetails(
          omMetadataManager.getLock()
              .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName)
      );
      acquiredLock = getOmLockDetails().isLockAcquired();

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      String dbOzoneKey =
          omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);

      OmKeyInfo omKeyInfo =
          omMetadataManager.getKeyTable(getBucketLayout()).get(dbOzoneKey);
      if (omKeyInfo == null) {
        throw new OMException("Key not found", KEY_NOT_FOUND);
      }

      // Set the tags
      omKeyInfo.getTags().clear();
      omKeyInfo.getTags().putAll(KeyValueUtil.getFromProtobuf(keyArgs.getTagsList()));
      // Set the UpdateID to the current transactionLogIndex
      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      // Note: Key modification time is not changed because S3 last modified
      // time only changes when there are changes in the object content

      // Update table cache
      omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
          new CacheKey<>(dbOzoneKey),
          CacheValue.get(trxnLogIndex, omKeyInfo)
      );

      omClientResponse = new S3PutObjectTaggingResponse(
          omResponse.setPutObjectTaggingResponse(PutObjectTaggingResponse.newBuilder()).build(),
          omKeyInfo
      );

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new S3PutObjectTaggingResponse(
          createErrorOMResponse(omResponse, exception),
          getBucketLayout()
      );
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.PUT_OBJECT_TAGGING, auditMap, exception, getOmRequest().getUserInfo()
    ));

    switch (result) {
    case SUCCESS:
      LOG.debug("Put object tagging success. Volume:{}, Bucket:{}, Key:{}.", volumeName,
          bucketName, keyName);
      break;
    case FAILURE:
      omMetrics.incNumPutObjectTaggingFails();
      if (OMClientRequestUtils.shouldLogClientRequestFailure(exception)) {
        LOG.error("Put object tagging failed. Volume:{}, Bucket:{}, Key:{}.", volumeName,
            bucketName, keyName, exception);
      }
      break;
    default:
      LOG.error("Unrecognized Result for S3PutObjectTaggingRequest: {}",
          putObjectTaggingRequest);
    }

    return omClientResponse;
  }
}
