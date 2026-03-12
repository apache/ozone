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

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tagging.S3PutObjectTaggingResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PutObjectTaggingRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PutObjectTaggingResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles put object tagging request for FSO bucket.
 */
public class S3PutObjectTaggingRequestWithFSO extends S3PutObjectTaggingRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3PutObjectTaggingRequestWithFSO.class);

  public S3PutObjectTaggingRequestWithFSO(OMRequest omRequest,
                                           BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

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

      OzoneFileStatus keyStatus = OMFileRequest.getOMKeyInfoIfExists(
          omMetadataManager, volumeName, bucketName, keyName, 0,
          ozoneManager.getDefaultReplicationConfig());

      if (keyStatus == null) {
        throw new OMException("Key not found. Key: " + keyName, ResultCodes.KEY_NOT_FOUND);
      }

      boolean isDirectory = keyStatus.isDirectory();

      if (isDirectory) {
        throw new OMException("PutObjectTagging is not currently supported for FSO directory",
            ResultCodes.NOT_SUPPORTED_OPERATION);
      }

      OmKeyInfo omKeyInfo = keyStatus.getKeyInfo();
      // Reverting back the full path to key name
      // Eg: a/b/c/d/e/file1 -> file1
      omKeyInfo.setKeyName(OzoneFSUtils.getFileName(keyName));
      final long volumeId = omMetadataManager.getVolumeId(volumeName);
      final long bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
      final String dbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
          omKeyInfo.getParentObjectID(), omKeyInfo.getFileName());

      omKeyInfo = omKeyInfo.toBuilder()
          .setTags(KeyValueUtil.getFromProtobuf(keyArgs.getTagsList()))
          .setUpdateID(trxnLogIndex)
          .build();

      // Note: Key modification time is not changed because S3 last modified
      // time only changes when there are changes in the object content

      // Update table cache for file table. No need to check directory table since
      // PutObjectTagging rejects operations on FSO directory
      omMetadataManager.getKeyTable(getBucketLayout())
          .addCacheEntry(new CacheKey<>(dbKey),
              CacheValue.get(trxnLogIndex, omKeyInfo));

      omClientResponse = new S3PutObjectTaggingResponseWithFSO(
          omResponse.setPutObjectTaggingResponse(PutObjectTaggingResponse.newBuilder()).build(),
          omKeyInfo, volumeId, bucketId
      );

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new S3PutObjectTaggingResponseWithFSO(
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

    switch (result) {
    case SUCCESS:
      LOG.debug("Put object tagging success. Volume:{}, Bucket:{}, Key:{}.", volumeName,
          bucketName, keyName);
      break;
    case FAILURE:
      omMetrics.incNumPutObjectTaggingFails();
      LOG.error("Put object tagging failed. Volume:{}, Bucket:{}, Key:{}.", volumeName,
          bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for S3PutObjectTaggingRequest: {}",
          putObjectTaggingRequest);
    }

    return omClientResponse;
  }
}
