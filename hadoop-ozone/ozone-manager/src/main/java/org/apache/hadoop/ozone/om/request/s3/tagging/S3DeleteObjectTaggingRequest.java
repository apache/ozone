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

package org.apache.hadoop.ozone.om.request.s3.tagging;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequestUtils;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tagging.S3DeleteObjectTaggingResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteObjectTaggingRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteObjectTaggingResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles delete object tagging request.
 */
public class S3DeleteObjectTaggingRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3DeleteObjectTaggingRequest.class);

  public S3DeleteObjectTaggingRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    DeleteObjectTaggingRequest deleteObjectTaggingRequest =
        super.preExecute(ozoneManager).getDeleteObjectTaggingRequest();
    Objects.requireNonNull(deleteObjectTaggingRequest, "deleteObjectTaggingRequest == null");

    KeyArgs keyArgs = deleteObjectTaggingRequest.getKeyArgs();

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
        .setDeleteObjectTaggingRequest(
            deleteObjectTaggingRequest.toBuilder().setKeyArgs(resolvedArgs))
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    DeleteObjectTaggingRequest deleteObjectTaggingRequest = getOmRequest().getDeleteObjectTaggingRequest();

    KeyArgs keyArgs = deleteObjectTaggingRequest.getKeyArgs();
    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumDeleteObjectTagging();

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

      // Clear / delete the tags
      // Set the UpdateID to the current transactionLogIndex
      omKeyInfo = omKeyInfo.toBuilder()
          .setTags(Collections.emptyMap())
          .setUpdateID(trxnLogIndex)
          .build();

      // Note: Key modification time is not changed because S3 last modified
      // time only changes when there are changes in the object content

      // Update table cache
      omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
          new CacheKey<>(dbOzoneKey),
          CacheValue.get(trxnLogIndex, omKeyInfo)
      );

      omClientResponse = new S3DeleteObjectTaggingResponse(
          omResponse.setDeleteObjectTaggingResponse(DeleteObjectTaggingResponse.newBuilder()).build(),
          omKeyInfo
      );

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new S3DeleteObjectTaggingResponse(
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
        OMAction.DELETE_OBJECT_TAGGING, auditMap, exception, getOmRequest().getUserInfo()
    ));

    switch (result) {
    case SUCCESS:
      LOG.debug("Delete object tagging success. Volume:{}, Bucket:{}, Key:{}.", volumeName,
          bucketName, keyName);
      break;
    case FAILURE:
      omMetrics.incNumDeleteObjectTaggingFails();
      if (OMClientRequestUtils.shouldLogClientRequestFailure(exception)) {
        LOG.error("Delete object tagging failed. Volume:{}, Bucket:{}, Key:{}.", volumeName,
            bucketName, keyName, exception);
      }
      break;
    default:
      LOG.error("Unrecognized Result for S3DeleteObjectTaggingRequest: {}",
          deleteObjectTaggingRequest);
    }

    return omClientResponse;
  }
}
