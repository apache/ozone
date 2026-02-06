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

package org.apache.hadoop.ozone.om.request.s3.multipart;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OMMultipartUploadUtils;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.OMClientVersionValidator;
import org.apache.hadoop.ozone.om.request.validation.OMLayoutVersionValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3MultipartUploadAbortResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadAbortRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadAbortResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles Abort of multipart upload request.
 */
public class S3MultipartUploadAbortRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3MultipartUploadAbortRequest.class);

  public S3MultipartUploadAbortRequest(OMRequest omRequest) {
    super(omRequest);
  }

  public S3MultipartUploadAbortRequest(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    KeyArgs keyArgs = super.preExecute(ozoneManager)
        .getAbortMultiPartUploadRequest().getKeyArgs();
    String keyPath = keyArgs.getKeyName();
    keyPath = validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(),
        keyPath, getBucketLayout());

    KeyArgs newKeyArgs =
        keyArgs.toBuilder().setModificationTime(Time.now())
            .setKeyName(keyPath).build();

    KeyArgs resolvedArgs = resolveBucketAndCheckKeyAcls(newKeyArgs,
        ozoneManager, ACLType.WRITE);
    return getOmRequest().toBuilder().setAbortMultiPartUploadRequest(
        getOmRequest().getAbortMultiPartUploadRequest().toBuilder().setKeyArgs(
            resolvedArgs)).setUserInfo(getUserInfo()).build();

  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    MultipartUploadAbortRequest multipartUploadAbortRequest = getOmRequest()
        .getAbortMultiPartUploadRequest();
    OzoneManagerProtocolProtos.KeyArgs keyArgs = multipartUploadAbortRequest
        .getKeyArgs();
    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
    auditMap.put(OzoneConsts.UPLOAD_ID, keyArgs.getMultipartUploadID());

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    final String requestedVolume = volumeName;
    final String requestedBucket = bucketName;
    String keyName = keyArgs.getKeyName();

    ozoneManager.getMetrics().incNumAbortMultipartUploads();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquiredLock = false;
    Exception exception = null;
    OmMultipartKeyInfo multipartKeyInfo = null;
    String multipartKey = null;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;
    Result result = null;
    OmBucketInfo omBucketInfo = null;
    try {
      mergeOmLockDetails(
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
              bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      multipartKey = omMetadataManager.getMultipartKey(
          volumeName, bucketName, keyName, keyArgs.getMultipartUploadID());

      String multipartOpenKey;
      try {
        multipartOpenKey =
            getMultipartOpenKey(keyArgs.getMultipartUploadID(), volumeName,
                bucketName, keyName, omMetadataManager);
      } catch (OMException ome) {
        throw new OMException(
            "Abort Multipart Upload Failed: volume: " + requestedVolume
                + ", bucket: " + requestedBucket + ", key: " + keyName, ome,
            OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
      }

      OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable(getBucketLayout())
          .get(multipartOpenKey);
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
      multipartKeyInfo = multipartKeyInfo.toBuilder()
          .setUpdateID(trxnLogIndex)
          .build();

      // When abort uploaded key, we need to subtract the PartKey length from
      // the volume usedBytes.
      long quotaReleased = 0;
      for (PartKeyInfo iterPartKeyInfo : multipartKeyInfo.getPartKeyInfoMap()) {
        quotaReleased += QuotaUtil.getReplicatedSize(
            iterPartKeyInfo.getPartKeyInfo().getDataSize(),
            omKeyInfo.getReplicationConfig());
      }
      omBucketInfo.incrUsedBytes(-quotaReleased);

      // Update cache of openKeyTable and multipartInfo table.
      // No need to add the cache entries to delete table, as the entries
      // in delete table are not used by any read/write operations.
      omMetadataManager.getOpenKeyTable(getBucketLayout())
          .addCacheEntry(new CacheKey<>(multipartOpenKey),
              CacheValue.get(trxnLogIndex));
      omMetadataManager.getMultipartInfoTable()
          .addCacheEntry(new CacheKey<>(multipartKey),
              CacheValue.get(trxnLogIndex));

      omClientResponse = getOmClientResponse(ozoneManager, multipartKeyInfo,
          multipartKey, multipartOpenKey, omResponse, omBucketInfo);

      result = Result.SUCCESS;
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = getOmClientResponse(exception, omResponse);
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // audit log
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
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

  protected OMClientResponse getOmClientResponse(Exception exception,
      OMResponse.Builder omResponse) {

    return new S3MultipartUploadAbortResponse(createErrorOMResponse(omResponse,
            exception), getBucketLayout());
  }

  protected OMClientResponse getOmClientResponse(OzoneManager ozoneManager,
      OmMultipartKeyInfo multipartKeyInfo, String multipartKey,
      String multipartOpenKey, OMResponse.Builder omResponse,
      OmBucketInfo omBucketInfo) {

    OMClientResponse omClientResponse = new S3MultipartUploadAbortResponse(
        omResponse.setAbortMultiPartUploadResponse(
            MultipartUploadAbortResponse.newBuilder()).build(), multipartKey,
        multipartOpenKey, multipartKeyInfo,
        omBucketInfo.copyObject(), getBucketLayout());
    return omClientResponse;
  }

  protected String getMultipartOpenKey(String multipartUploadID,
      String volumeName, String bucketName, String keyName,
      OMMetadataManager omMetadataManager) throws IOException {
    return OMMultipartUploadUtils.getMultipartOpenKey(
        volumeName, bucketName, keyName, multipartUploadID, omMetadataManager,
        getBucketLayout());
  }

  @OMLayoutVersionValidator(
      applyBefore = OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.AbortMultiPartUpload
  )
  public static OMRequest disallowAbortMultiPartUploadWithECReplicationConfig(
      OMRequest req, ValidationContext ctx) throws OMException {
    if (!ctx.versionManager().isAllowed(
        OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT)) {
      if (req.getAbortMultiPartUploadRequest().getKeyArgs()
          .hasEcReplicationConfig()) {
        throw new OMException("Cluster does not have the Erasure Coded"
            + " Storage support feature finalized yet, but the request contains"
            + " an Erasure Coded replication type. Rejecting the request,"
            + " please finalize the cluster upgrade and then try again.",
            OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
      }
    }
    return req;
  }

  /**
   * Validates S3 MPU abort requests.
   * We do not want to allow older clients to abort MPU operations in
   * buckets which use non LEGACY layouts.
   *
   * @param req - the request to validate
   * @param ctx - the validation context
   * @return the validated request
   * @throws OMException if the request is invalid
   */
  @OMClientVersionValidator(
      applyBefore = ClientVersion.BUCKET_LAYOUT_SUPPORT,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.AbortMultiPartUpload
  )
  public static OMRequest blockMPUAbortWithBucketLayoutFromOldClient(
      OMRequest req, ValidationContext ctx) throws IOException {
    if (req.getAbortMultiPartUploadRequest().hasKeyArgs()) {
      KeyArgs keyArgs = req.getAbortMultiPartUploadRequest().getKeyArgs();

      if (keyArgs.hasVolumeName() && keyArgs.hasBucketName()) {
        BucketLayout bucketLayout = ctx.getBucketLayout(
            keyArgs.getVolumeName(), keyArgs.getBucketName());
        bucketLayout.validateSupportedOperation();
      }
    }
    return req;
  }
}
