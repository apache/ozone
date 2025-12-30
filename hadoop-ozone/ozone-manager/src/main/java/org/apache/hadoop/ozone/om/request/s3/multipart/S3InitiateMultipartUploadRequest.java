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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneConfigUtil;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OMMultipartUploadUtils;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3InitiateMultipartUploadResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartInfoInitiateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartInfoInitiateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles initiate multipart upload request.
 */
public class S3InitiateMultipartUploadRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3InitiateMultipartUploadRequest.class);

  public S3InitiateMultipartUploadRequest(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    MultipartInfoInitiateRequest multipartInfoInitiateRequest =
        super.preExecute(ozoneManager).getInitiateMultiPartUploadRequest();
    Objects.requireNonNull(multipartInfoInitiateRequest, "multipartInfoInitiateRequest == null");

    KeyArgs keyArgs = multipartInfoInitiateRequest.getKeyArgs();

    String keyPath = keyArgs.getKeyName();
    keyPath = validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(),
        keyPath, getBucketLayout());

    KeyArgs.Builder newKeyArgs = keyArgs.toBuilder()
            .setMultipartUploadID(
                OMMultipartUploadUtils.getMultipartUploadId())
            .setModificationTime(Time.now())
            .setKeyName(keyPath);

    generateRequiredEncryptionInfo(keyArgs, newKeyArgs, ozoneManager);

    KeyArgs resolvedArgs = resolveBucketAndCheckKeyAcls(newKeyArgs.build(),
        ozoneManager, ACLType.CREATE);
    return getOmRequest().toBuilder()
        .setUserInfo(getUserInfo())
        .setInitiateMultiPartUploadRequest(
            multipartInfoInitiateRequest.toBuilder().setKeyArgs(resolvedArgs))
        .build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();
    MultipartInfoInitiateRequest multipartInfoInitiateRequest =
        getOmRequest().getInitiateMultiPartUploadRequest();

    KeyArgs keyArgs =
        multipartInfoInitiateRequest.getKeyArgs();

    Objects.requireNonNull(keyArgs.getMultipartUploadID(), "multipartUploadID == null");

    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
    auditMap.put(OzoneConsts.UPLOAD_ID, keyArgs.getMultipartUploadID());

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    final String requestedVolume = volumeName;
    final String requestedBucket = bucketName;
    String keyName = keyArgs.getKeyName();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    ozoneManager.getMetrics().incNumInitiateMultipartUploads();
    boolean acquiredBucketLock = false;
    Exception exception = null;
    OmMultipartKeyInfo multipartKeyInfo = null;
    OmKeyInfo omKeyInfo = null;
    Result result = null;
    long objectID = ozoneManager.getObjectIdFromTxId(transactionLogIndex);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;
    try {
      mergeOmLockDetails(
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
              bucketName));
      acquiredBucketLock = getOmLockDetails().isLockAcquired();

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

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

      String multipartKey = omMetadataManager.getMultipartKey(
          volumeName, bucketName, keyName,
          keyArgs.getMultipartUploadID());

      // Even if this key already exists in the KeyTable, it would be taken
      // care of in the final complete multipart upload. AWS S3 behavior is
      // also like this, even when key exists in a bucket, user can still
      // initiate MPU.
      final OmBucketInfo bucketInfo = omMetadataManager.getBucketTable()
          .get(omMetadataManager.getBucketKey(volumeName, bucketName));

      OMFileRequest.OMPathInfo pathInfo = null;
      if (bucketInfo != null && bucketInfo.getBucketLayout()
          .shouldNormalizePaths(ozoneManager.getEnableFileSystemPaths())) {
        pathInfo = OMFileRequest.verifyFilesInPath(omMetadataManager,
            volumeName, bucketName, keyName, Paths.get(keyName));
      }
      final ReplicationConfig replicationConfig = OzoneConfigUtil
          .resolveReplicationConfigPreference(keyArgs.getType(),
              keyArgs.getFactor(), keyArgs.getEcReplicationConfig(),
              bucketInfo != null ?
                  bucketInfo.getDefaultReplicationConfig() :
                  null, ozoneManager);

      multipartKeyInfo = new OmMultipartKeyInfo.Builder()
          .setUploadID(keyArgs.getMultipartUploadID())
          .setCreationTime(keyArgs.getModificationTime())
          .setReplicationConfig(
              replicationConfig)
          .setObjectID(objectID)
          .setUpdateID(transactionLogIndex)
          .build();

      omKeyInfo = new OmKeyInfo.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName(keyArgs.getKeyName())
          .setCreationTime(keyArgs.getModificationTime())
          .setModificationTime(keyArgs.getModificationTime())
          .setReplicationConfig(replicationConfig)
          .setOmKeyLocationInfos(Collections.singletonList(
              new OmKeyLocationInfoGroup(0, new ArrayList<>(), true)))
          .setAcls(getAclsForKey(keyArgs, bucketInfo, pathInfo,
              ozoneManager.getPrefixManager(), ozoneManager.getConfig()))
          .setObjectID(objectID)
          .setUpdateID(transactionLogIndex)
          .setFileEncryptionInfo(keyArgs.hasFileEncryptionInfo() ?
              OMPBHelper.convert(keyArgs.getFileEncryptionInfo()) : null)
          .addAllMetadata(KeyValueUtil.getFromProtobuf(keyArgs.getMetadataList()))
          .setOwnerName(keyArgs.getOwnerName())
          .addAllTags(KeyValueUtil.getFromProtobuf(keyArgs.getTagsList()))
          .build();

      // Add to cache
      omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
          new CacheKey<>(multipartKey),
          CacheValue.get(transactionLogIndex, omKeyInfo));
      omMetadataManager.getMultipartInfoTable().addCacheEntry(
          new CacheKey<>(multipartKey),
          CacheValue.get(transactionLogIndex, multipartKeyInfo));

      omClientResponse =
          new S3InitiateMultipartUploadResponse(
              omResponse.setInitiateMultiPartUploadResponse(
                  MultipartInfoInitiateResponse.newBuilder()
                      .setVolumeName(requestedVolume)
                      .setBucketName(requestedBucket)
                      .setKeyName(keyName)
                      .setMultipartUploadID(keyArgs.getMultipartUploadID()))
                  .build(), multipartKeyInfo, omKeyInfo, getBucketLayout());

      result = Result.SUCCESS;
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new S3InitiateMultipartUploadResponse(
          createErrorOMResponse(omResponse, exception), getBucketLayout());
    } finally {
      if (acquiredBucketLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }
    logResult(ozoneManager, multipartInfoInitiateRequest, auditMap, volumeName,
            bucketName, keyName, exception, result);

    return omClientResponse;
  }

  @SuppressWarnings("parameternumber")
  protected void logResult(OzoneManager ozoneManager,
      MultipartInfoInitiateRequest multipartInfoInitiateRequest,
      Map<String, String> auditMap, String volumeName, String bucketName,
      String keyName, Exception exception, Result result) {
    // audit log
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.INITIATE_MULTIPART_UPLOAD, auditMap,
        exception, getOmRequest().getUserInfo()));

    switch (result) {
    case SUCCESS:
      LOG.debug("S3 InitiateMultipart Upload request for Key {} in " +
              "Volume/Bucket {}/{} is successfully completed", keyName,
          volumeName, bucketName);
      break;
    case FAILURE:
      ozoneManager.getMetrics().incNumInitiateMultipartUploadFails();
      LOG.error("S3 InitiateMultipart Upload request for Key {} in " +
              "Volume/Bucket {}/{} is failed", keyName, volumeName, bucketName,
          exception);
      break;
    default:
      LOG.error("Unrecognized Result for S3InitiateMultipartUploadRequest: {}",
          multipartInfoInitiateRequest);
    }
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.CLUSTER_NEEDS_FINALIZATION,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.InitiateMultiPartUpload
  )
  public static OMRequest
      disallowInitiateMultiPartUploadWithECReplicationConfig(
      OMRequest req, ValidationContext ctx) throws OMException {
    if (!ctx.versionManager()
        .isAllowed(OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT)) {
      if (req.getInitiateMultiPartUploadRequest().getKeyArgs()
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
   * Validates S3 initiate MPU requests.
   * We do not want to allow older clients to initiate MPU to buckets which
   * use non LEGACY layouts.
   *
   * @param req - the request to validate
   * @param ctx - the validation context
   * @return the validated request
   * @throws OMException if the request is invalid
   */
  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.InitiateMultiPartUpload
  )
  public static OMRequest blockInitiateMPUWithBucketLayoutFromOldClient(
      OMRequest req, ValidationContext ctx) throws IOException {
    if (req.getInitiateMultiPartUploadRequest().hasKeyArgs()) {
      KeyArgs keyArgs = req.getInitiateMultiPartUploadRequest().getKeyArgs();

      if (keyArgs.hasVolumeName() && keyArgs.hasBucketName()) {
        BucketLayout bucketLayout = ctx.getBucketLayout(
            keyArgs.getVolumeName(), keyArgs.getBucketName());
        bucketLayout.validateSupportedOperation();
      }
    }
    return req;
  }
}
