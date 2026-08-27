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

package org.apache.hadoop.ozone.om.request.s3.metadata;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.OMAction;
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
import org.apache.hadoop.ozone.om.request.OMClientRequestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.metadata.S3SetObjectMetadataResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetObjectMetadataRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetObjectMetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles set object metadata request for FSO bucket.
 */
public class S3SetObjectMetadataRequestWithFSO extends S3SetObjectMetadataRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3SetObjectMetadataRequestWithFSO.class);

  public S3SetObjectMetadataRequestWithFSO(OMRequest omRequest,
                                           BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    SetObjectMetadataRequest setObjectMetadataRequest = getOmRequest().getSetObjectMetadataRequest();

    KeyArgs keyArgs = setObjectMetadataRequest.getKeyArgs();
    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumSetObjectMetadata();

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

      if (keyStatus.isDirectory()) {
        throw new OMException("SetObjectMetadata is not currently supported for FSO directory",
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
          .setMetadata(replaceMetadata(omKeyInfo.getMetadata(),
              KeyValueUtil.getFromProtobuf(keyArgs.getMetadataList())))
          .setTags(KeyValueUtil.getFromProtobuf(keyArgs.getTagsList()))
          .setModificationTime(keyArgs.getModificationTime())
          .setUpdateID(trxnLogIndex)
          .build();

      // Note: unlike object tagging, the key modification time is updated
      // because AWS CopyObject updates the object's LastModified.

      // Update table cache for file table. No need to check directory table since
      // SetObjectMetadata rejects operations on FSO directory
      omMetadataManager.getKeyTable(getBucketLayout())
          .addCacheEntry(new CacheKey<>(dbKey),
              CacheValue.get(trxnLogIndex, omKeyInfo));

      omClientResponse = new S3SetObjectMetadataResponseWithFSO(
          omResponse.setSetObjectMetadataResponse(SetObjectMetadataResponse.newBuilder()).build(),
          omKeyInfo, volumeId, bucketId
      );

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new S3SetObjectMetadataResponseWithFSO(
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
        OMAction.SET_OBJECT_METADATA, auditMap, exception, getOmRequest().getUserInfo()
    ));

    switch (result) {
    case SUCCESS:
      LOG.debug("Set object metadata success. Volume:{}, Bucket:{}, Key:{}.", volumeName,
          bucketName, keyName);
      break;
    case FAILURE:
      omMetrics.incNumSetObjectMetadataFails();
      if (OMClientRequestUtils.shouldLogClientRequestFailure(exception)) {
        LOG.error("Set object metadata failed. Volume:{}, Bucket:{}, Key:{}.", volumeName,
            bucketName, keyName, exception);
      }
      break;
    default:
      LOG.error("Unrecognized Result for S3SetObjectMetadataRequest: {}",
          setObjectMetadataRequest);
    }

    return omClientResponse;
  }
}
