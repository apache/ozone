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

package org.apache.hadoop.ozone.om.request.s3.multipart;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart
    .S3MultipartUploadAbortResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadAbortRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadAbortResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles Abort of multipart upload request.
 */
public class S3MultipartUploadAbortRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3MultipartUploadAbortRequest.class);

  public S3MultipartUploadAbortRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    KeyArgs keyArgs =
        getOmRequest().getAbortMultiPartUploadRequest().getKeyArgs();

    return getOmRequest().toBuilder().setAbortMultiPartUploadRequest(
        getOmRequest().getAbortMultiPartUploadRequest().toBuilder()
            .setKeyArgs(keyArgs.toBuilder().setModificationTime(Time.now())
                .setKeyName(validateAndNormalizeKey(
                    ozoneManager.getEnableFileSystemPaths(),
                    keyArgs.getKeyName()))))
        .setUserInfo(getUserInfo()).build();

  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    MultipartUploadAbortRequest multipartUploadAbortRequest = getOmRequest()
        .getAbortMultiPartUploadRequest();
    OzoneManagerProtocolProtos.KeyArgs keyArgs = multipartUploadAbortRequest
        .getKeyArgs();
    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    final String requestedVolume = volumeName;
    final String requestedBucket = bucketName;
    String keyName = keyArgs.getKeyName();

    ozoneManager.getMetrics().incNumAbortMultipartUploads();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquiredLock = false;
    IOException exception = null;
    OmMultipartKeyInfo multipartKeyInfo = null;
    String multipartKey = null;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;
    Result result = null;
    OmBucketInfo omBucketInfo = null;
    try {
      keyArgs = resolveBucketLink(ozoneManager, keyArgs, auditMap);
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();

      // TODO to support S3 ACL later.
      acquiredLock =
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
              volumeName, bucketName);

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      multipartKey = omMetadataManager.getMultipartKey(
          volumeName, bucketName, keyName, keyArgs.getMultipartUploadID());

      OmKeyInfo omKeyInfo =
          omMetadataManager.getOpenKeyTable().get(multipartKey);
      omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);

      // If there is no entry in openKeyTable, then there is no multipart
      // upload initiated for this key.
      if (omKeyInfo == null) {
        throw new OMException("Abort Multipart Upload Failed: volume: " +
            requestedVolume + "bucket: " + requestedBucket + "key: " + keyName,
            OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
      }

      multipartKeyInfo = omMetadataManager.getMultipartInfoTable()
          .get(multipartKey);
      multipartKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      // When abort uploaded key, we need to subtract the PartKey length from
      // the volume usedBytes.
      long quotaReleased = 0;
      int keyFactor = omKeyInfo.getFactor().getNumber();
      Iterator iter =
          multipartKeyInfo.getPartKeyInfoMap().entrySet().iterator();
      while(iter.hasNext()) {
        Map.Entry entry = (Map.Entry)iter.next();
        PartKeyInfo iterPartKeyInfo = (PartKeyInfo)entry.getValue();
        quotaReleased +=
            iterPartKeyInfo.getPartKeyInfo().getDataSize() * keyFactor;
      }
      omBucketInfo.incrUsedBytes(-quotaReleased);

      // Update cache of openKeyTable and multipartInfo table.
      // No need to add the cache entries to delete table, as the entries
      // in delete table are not used by any read/write operations.
      omMetadataManager.getOpenKeyTable().addCacheEntry(
          new CacheKey<>(multipartKey),
          new CacheValue<>(Optional.absent(), trxnLogIndex));
      omMetadataManager.getMultipartInfoTable().addCacheEntry(
          new CacheKey<>(multipartKey),
          new CacheValue<>(Optional.absent(), trxnLogIndex));

      omClientResponse = new S3MultipartUploadAbortResponse(
          omResponse.setAbortMultiPartUploadResponse(
              MultipartUploadAbortResponse.newBuilder()).build(),
          multipartKey, multipartKeyInfo, ozoneManager.isRatisEnabled(),
          omBucketInfo.copyObject());

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse =
          new S3MultipartUploadAbortResponse(createErrorOMResponse(omResponse,
              exception));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK,
            volumeName, bucketName);
      }
    }

    // audit log
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.ABORT_MULTIPART_UPLOAD, auditMap,
        exception, getOmRequest().getUserInfo()));

    switch (result) {
    case SUCCESS:
      LOG.debug("Abort Multipart request is successfully completed for " +
              "KeyName {} in VolumeName/Bucket {}/{}", keyName, volumeName,
          bucketName);
      break;
    case FAILURE:
      ozoneManager.getMetrics().incNumAbortMultipartUploadFails();
      LOG.error("Abort Multipart request is failed for KeyName {} in " +
              "VolumeName/Bucket {}/{}", keyName, volumeName, bucketName,
          exception);
      break;
    default:
      LOG.error("Unrecognized Result for S3MultipartUploadAbortRequest: {}",
          multipartUploadAbortRequest);
    }

    return omClientResponse;
  }
}
