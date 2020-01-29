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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMReplayException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3InitiateMultipartUploadResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartInfoInitiateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartInfoInitiateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.UniqueId;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles initiate multipart upload request.
 */
public class S3InitiateMultipartUploadRequest extends OMKeyRequest {


  private static final Logger LOG =
      LoggerFactory.getLogger(S3InitiateMultipartUploadRequest.class);

  private enum Result {
    SUCCESS,
    REPLAY,
    FAILURE
  }

  public S3InitiateMultipartUploadRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) {
    MultipartInfoInitiateRequest multipartInfoInitiateRequest =
        getOmRequest().getInitiateMultiPartUploadRequest();
    Preconditions.checkNotNull(multipartInfoInitiateRequest);

    OzoneManagerProtocolProtos.KeyArgs.Builder newKeyArgs =
        multipartInfoInitiateRequest.getKeyArgs().toBuilder()
            .setMultipartUploadID(UUID.randomUUID().toString() + "-" +
                UniqueId.next()).setModificationTime(Time.now());

    return getOmRequest().toBuilder()
        .setUserInfo(getUserInfo())
        .setInitiateMultiPartUploadRequest(
            multipartInfoInitiateRequest.toBuilder().setKeyArgs(newKeyArgs))
        .build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    MultipartInfoInitiateRequest multipartInfoInitiateRequest =
        getOmRequest().getInitiateMultiPartUploadRequest();

    OzoneManagerProtocolProtos.KeyArgs keyArgs =
        multipartInfoInitiateRequest.getKeyArgs();

    Preconditions.checkNotNull(keyArgs.getMultipartUploadID());

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    ozoneManager.getMetrics().incNumInitiateMultipartUploads();
    boolean acquiredBucketLock = false;
    IOException exception = null;
    OmMultipartKeyInfo multipartKeyInfo = null;
    OmKeyInfo omKeyInfo = null;
    Result result = null;

    OMResponse.Builder omResponse = OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.InitiateMultiPartUpload)
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setSuccess(true);
    OMClientResponse omClientResponse = null;
    try {
      // TODO to support S3 ACL later.
      acquiredBucketLock =
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
              bucketName);

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      // Check if this transaction is a replay.
      // We check only the KeyTable here and not the OpenKeyTable. In case
      // this transaction is a replay but the transaction was not committed
      // to the KeyTable, then we recreate the key in OpenKey table. This is
      // okay as all the subsequent transactions would also be replayed and
      // the openKey table would eventually reach the same state.
      // The reason we do not check the OpenKey table is to avoid a DB read
      // in regular non-replay scenario.
      String dbKeyName = omMetadataManager.getOzoneKey(volumeName, bucketName,
          keyName);
      OmKeyInfo dbKeyInfo = omMetadataManager.getKeyTable().get(dbKeyName);
      if (dbKeyInfo != null) {
        // Check if this transaction is a replay of ratis logs.
        if (isReplay(ozoneManager, dbKeyInfo.getUpdateID(),
            transactionLogIndex)) {
          // Replay implies the response has already been returned to
          // the client. So take no further action and return a dummy
          // OMClientResponse.
          throw new OMReplayException();
        }
      }

      // We are adding uploadId to key, because if multiple users try to
      // perform multipart upload on the same key, each will try to upload, who
      // ever finally commit the key, we see that key in ozone. Suppose if we
      // don't add id, and use the same key /volume/bucket/key, when multiple
      // users try to upload the key, we update the parts of the key's from
      // multiple users to same key, and the key output can be a mix of the
      // parts from multiple users.

      // So on same key if multiple time multipart upload is initiated we
      // store multiple entries in the openKey Table.
      // Checked AWS S3, when we try to run multipart upload, each time a
      // new uploadId is returned. And also even if a key exist when initiate
      // multipart upload request is received, it returns multipart upload id
      // for the key.

      String multipartKey = omMetadataManager.getMultipartKey(volumeName,
          bucketName, keyName, keyArgs.getMultipartUploadID());

      // Even if this key already exists in the KeyTable, it would be taken
      // care of in the final complete multipart upload. AWS S3 behavior is
      // also like this, even when key exists in a bucket, user can still
      // initiate MPU.

      multipartKeyInfo = new OmMultipartKeyInfo.Builder()
          .setUploadID(keyArgs.getMultipartUploadID())
          .setCreationTime(keyArgs.getModificationTime())
          .setReplicationType(keyArgs.getType())
          .setReplicationFactor(keyArgs.getFactor())
          .setObjectID(transactionLogIndex)
          .setUpdateID(transactionLogIndex)
          .build();

      omKeyInfo = new OmKeyInfo.Builder()
          .setVolumeName(keyArgs.getVolumeName())
          .setBucketName(keyArgs.getBucketName())
          .setKeyName(keyArgs.getKeyName())
          .setCreationTime(keyArgs.getModificationTime())
          .setModificationTime(keyArgs.getModificationTime())
          .setReplicationType(keyArgs.getType())
          .setReplicationFactor(keyArgs.getFactor())
          .setOmKeyLocationInfos(Collections.singletonList(
              new OmKeyLocationInfoGroup(0, new ArrayList<>())))
          .setAcls(OzoneAclUtil.fromProtobuf(keyArgs.getAclsList()))
          .setObjectID(transactionLogIndex)
          .setUpdateID(transactionLogIndex)
          .build();

      // Add to cache
      omMetadataManager.getOpenKeyTable().addCacheEntry(
          new CacheKey<>(multipartKey),
          new CacheValue<>(Optional.of(omKeyInfo), transactionLogIndex));
      omMetadataManager.getMultipartInfoTable().addCacheEntry(
          new CacheKey<>(multipartKey),
          new CacheValue<>(Optional.of(multipartKeyInfo),
              transactionLogIndex));

      omClientResponse =
          new S3InitiateMultipartUploadResponse(
              omResponse.setInitiateMultiPartUploadResponse(
                  MultipartInfoInitiateResponse.newBuilder()
                      .setVolumeName(volumeName)
                      .setBucketName(bucketName)
                      .setKeyName(keyName)
                      .setMultipartUploadID(keyArgs.getMultipartUploadID()))
                  .build(), multipartKeyInfo, omKeyInfo);

      result = Result.SUCCESS;
    } catch (IOException ex) {
      if (ex instanceof  OMReplayException) {
        result = Result.REPLAY;
        omClientResponse = new S3InitiateMultipartUploadResponse(
            createReplayOMResponse(omResponse));
      } else {
        result = Result.FAILURE;
        exception = ex;
        omClientResponse = new S3InitiateMultipartUploadResponse(
            createErrorOMResponse(omResponse, exception));
      }
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
      if (acquiredBucketLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    // audit log
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.INITIATE_MULTIPART_UPLOAD, buildKeyArgsAuditMap(keyArgs),
        exception, getOmRequest().getUserInfo()));

    switch (result) {
    case SUCCESS:
      LOG.debug("S3 InitiateMultipart Upload request for Key {} in " +
              "Volume/Bucket {}/{} is successfully completed", keyName,
          volumeName, bucketName);
      break;
    case REPLAY:
      LOG.debug("Replayed Transaction {} ignored. Request: {}",
          transactionLogIndex, multipartInfoInitiateRequest);
      break;
    case FAILURE:
      ozoneManager.getMetrics().incNumInitiateMultipartUploadFails();
      LOG.error("S3 InitiateMultipart Upload request for Key {} in " +
              "Volume/Bucket {}/{} is failed", keyName, volumeName, bucketName,
          exception);
    default:
      LOG.error("Unrecognized Result for S3InitiateMultipartUploadRequest: {}",
          multipartInfoInitiateRequest);
    }

    return omClientResponse;
  }
}
