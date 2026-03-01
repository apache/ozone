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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.OMClientVersionValidator;
import org.apache.hadoop.ozone.om.request.validation.OMLayoutVersionValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3MultipartUploadCommitPartResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartCommitUploadPartRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartCommitUploadPartResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle Multipart upload commit upload part file.
 */
public class S3MultipartUploadCommitPartRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3MultipartUploadCommitPartRequest.class);

  public S3MultipartUploadCommitPartRequest(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    MultipartCommitUploadPartRequest multipartCommitUploadPartRequest =
        super.preExecute(ozoneManager).getCommitMultiPartUploadRequest();

    KeyArgs keyArgs = multipartCommitUploadPartRequest.getKeyArgs();
    String keyPath = keyArgs.getKeyName();
    keyPath = validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(),
        keyPath, getBucketLayout());

    KeyArgs newKeyArgs =
        keyArgs.toBuilder().setModificationTime(Time.now())
            .setKeyName(keyPath).build();

    KeyArgs resolvedArgs = resolveBucketAndCheckOpenKeyAcls(newKeyArgs,
        ozoneManager, ACLType.WRITE,
        multipartCommitUploadPartRequest.getClientID());
    return getOmRequest().toBuilder().setCommitMultiPartUploadRequest(
        multipartCommitUploadPartRequest.toBuilder().setKeyArgs(
            resolvedArgs)).setUserInfo(getUserInfo()).build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();
    MultipartCommitUploadPartRequest multipartCommitUploadPartRequest =
        getOmRequest().getCommitMultiPartUploadRequest();

    KeyArgs keyArgs = multipartCommitUploadPartRequest.getKeyArgs();
    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
    auditMap.put(OzoneConsts.UPLOAD_ID, keyArgs.getMultipartUploadID());

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    ozoneManager.getMetrics().incNumCommitMultipartUploadParts();

    boolean acquiredLock = false;

    Exception exception = null;
    String partName = null;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;
    OzoneManagerProtocolProtos.PartKeyInfo oldPartKeyInfo = null;
    String openKey = null;
    OmKeyInfo omKeyInfo = null;
    String multipartKey = null;
    OmMultipartKeyInfo multipartKeyInfo = null;
    Result result = null;
    OmBucketInfo omBucketInfo = null;
    OmBucketInfo copyBucketInfo = null;
    long bucketId = 0;
    try {
      long clientID = multipartCommitUploadPartRequest.getClientID();

      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
      String uploadID = keyArgs.getMultipartUploadID();
      multipartKey = getMultipartKey(volumeName, bucketName, keyName,
              omMetadataManager, uploadID);

      multipartKeyInfo = omMetadataManager.getMultipartInfoTable()
          .get(multipartKey);

      openKey = getOpenKey(volumeName, bucketName, keyName, omMetadataManager,
              clientID);

      String ozoneKey = omMetadataManager.getOzoneKey(
          volumeName, bucketName, keyName);

      omKeyInfo = getOmKeyInfo(omMetadataManager, openKey, keyName);

      if (omKeyInfo == null) {
        throw new OMException("Failed to commit Multipart Upload key, as " +
            openKey + "entry is not found in the openKey table",
            KEY_NOT_FOUND);
      }
      // Add/Update user defined metadata.
      // Set the UpdateID to current transactionLogIndex
      omKeyInfo = omKeyInfo.toBuilder()
          .addAllMetadata(KeyValueUtil.getFromProtobuf(
              keyArgs.getMetadataList()))
          .setUpdateID(trxnLogIndex)
          .build();

      // set the data size and location info list
      omKeyInfo.setDataSize(keyArgs.getDataSize());
      List<OmKeyLocationInfo> uncommitted = omKeyInfo.updateLocationInfoList(
          keyArgs.getKeyLocationsList().stream()
          .map(OmKeyLocationInfo::getFromProtobuf)
          .collect(Collectors.toList()), true);
      // Set Modification time
      omKeyInfo.setModificationTime(keyArgs.getModificationTime());

      int partNumber = keyArgs.getMultipartNumber();
      partName = getPartName(ozoneKey, uploadID, partNumber);

      if (multipartKeyInfo == null) {
        // This can occur when user started uploading part by the time commit
        // of that part happens, in between the user might have requested
        // abort multipart upload. If we just throw exception, then the data
        // will not be garbage collected, so move this part to delete table
        // and throw error
        // Move this part to delete table.
        throw new OMException("No such Multipart upload is with specified " +
            "uploadId " + uploadID,
            OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
      }

      oldPartKeyInfo = multipartKeyInfo.getPartKeyInfo(partNumber);

      // Build this multipart upload part info.
      OzoneManagerProtocolProtos.PartKeyInfo.Builder partKeyInfo =
          OzoneManagerProtocolProtos.PartKeyInfo.newBuilder();
      partKeyInfo.setPartName(partName);
      partKeyInfo.setPartNumber(partNumber);
      partKeyInfo.setPartKeyInfo(omKeyInfo.getProtobuf(
          getOmRequest().getVersion()));

      // Add this part information in to multipartKeyInfo.
      multipartKeyInfo.addPartKeyInfo(partKeyInfo.build());

      // Set the UpdateID to current transactionLogIndex
      multipartKeyInfo = multipartKeyInfo.toBuilder()
          .setUpdateID(trxnLogIndex)
          .build();

      // OldPartKeyInfo will be deleted. Its updateID will be set in
      // S3MultipartUploadCommitPartResponse before being added to
      // DeletedKeyTable.

      // Add to cache.

      // Delete from open key table and add it to multipart info table.
      // No need to add cache entries to delete table, as no
      // read/write requests that info for validation.
      omMetadataManager.getMultipartInfoTable().addCacheEntry(
          new CacheKey<>(multipartKey),
          CacheValue.get(trxnLogIndex, multipartKeyInfo));

      omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
          new CacheKey<>(openKey),
          CacheValue.get(trxnLogIndex));

      omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);

      // This map should contain maximum of two entries
      // 1. Overwritten part
      // 2. Uncommitted pseudo part key
      Map<String, RepeatedOmKeyInfo> keyVersionsToDeleteMap = null;

      long correctedSpace = omKeyInfo.getReplicatedSize();
      if (null != oldPartKeyInfo) {
        OmKeyInfo partKeyToBeDeleted =
            OmKeyInfo.getFromProtobuf(oldPartKeyInfo.getPartKeyInfo());
        correctedSpace -= partKeyToBeDeleted.getReplicatedSize();
        RepeatedOmKeyInfo oldVerKeyInfo = getOldVersionsToCleanUp(partKeyToBeDeleted, omBucketInfo.getObjectID(),
            trxnLogIndex);
        // Unlike normal key commit, we can reuse the objectID for MPU part key because MPU part key
        // always use a new object ID regardless whether there is an existing key.
        String delKeyName = omMetadataManager.getOzoneDeletePathKey(
            partKeyToBeDeleted.getObjectID(), multipartKey);

        if (!oldVerKeyInfo.getOmKeyInfoList().isEmpty()) {
          keyVersionsToDeleteMap = new HashMap<>();
          keyVersionsToDeleteMap.put(delKeyName, oldVerKeyInfo);
        }
      }
      checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
          correctedSpace);
      omBucketInfo.incrUsedBytes(correctedSpace);

      // let the uncommitted blocks pretend as key's old version blocks
      // which will be deleted as RepeatedOmKeyInfo
      final OmKeyInfo pseudoKeyInfo = wrapUncommittedBlocksAsPseudoKey(uncommitted, omKeyInfo);
      keyVersionsToDeleteMap = addKeyInfoToDeleteMap(ozoneManager, trxnLogIndex, ozoneKey, omBucketInfo.getObjectID(),
          pseudoKeyInfo, keyVersionsToDeleteMap);

      MultipartCommitUploadPartResponse.Builder commitResponseBuilder = MultipartCommitUploadPartResponse.newBuilder()
          .setPartName(partName);
      String eTag = omKeyInfo.getMetadata().get(OzoneConsts.ETAG);
      if (eTag != null) {
        commitResponseBuilder.setETag(eTag);
      }
      omResponse.setCommitMultiPartUploadResponse(commitResponseBuilder);
      omClientResponse =
          getOmClientResponse(ozoneManager, keyVersionsToDeleteMap, openKey,
              omKeyInfo, multipartKey, multipartKeyInfo, omResponse.build(),
              omBucketInfo.copyObject(), bucketId);

      result = Result.SUCCESS;
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse =
          getOmClientResponse(ozoneManager, null, openKey,
              omKeyInfo, multipartKey, multipartKeyInfo,
              createErrorOMResponse(omResponse, exception), copyBucketInfo, bucketId);
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    logResult(ozoneManager, multipartCommitUploadPartRequest, keyArgs,
            auditMap, volumeName, bucketName, keyName, exception, partName,
            result);

    return omClientResponse;
  }

  @VisibleForTesting
  public static String getPartName(String ozoneKey, String uploadID,
      long partNumber) {
    return ozoneKey + "-" + uploadID + "-" + partNumber;
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  protected S3MultipartUploadCommitPartResponse getOmClientResponse(
      OzoneManager ozoneManager, Map<String, RepeatedOmKeyInfo> keyToDeleteMap,
      String openKey, OmKeyInfo omKeyInfo, String multipartKey,
      OmMultipartKeyInfo multipartKeyInfo, OMResponse build,
      OmBucketInfo omBucketInfo, long bucketId) {

    return new S3MultipartUploadCommitPartResponse(build, multipartKey, openKey,
        multipartKeyInfo, keyToDeleteMap, omKeyInfo,
        omBucketInfo, bucketId, getBucketLayout());
  }

  protected OmKeyInfo getOmKeyInfo(OMMetadataManager omMetadataManager,
      String openKey, String keyName) throws IOException {

    return omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);
  }

  protected String getOpenKey(String volumeName, String bucketName,
      String keyName, OMMetadataManager omMetadataManager, long clientID)
      throws IOException {

    return omMetadataManager
        .getOpenKey(volumeName, bucketName, keyName, clientID);
  }

  @SuppressWarnings("parameternumber")
  private void logResult(OzoneManager ozoneManager,
      MultipartCommitUploadPartRequest multipartCommitUploadPartRequest,
      KeyArgs keyArgs, Map<String, String> auditMap, String volumeName,
      String bucketName, String keyName, Exception exception,
      String partName, Result result) {
    // audit log
    // Add MPU related information.
    auditMap.put(OzoneConsts.MULTIPART_UPLOAD_PART_NUMBER,
        String.valueOf(keyArgs.getMultipartNumber()));
    auditMap.put(OzoneConsts.MULTIPART_UPLOAD_PART_NAME, partName);
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.COMMIT_MULTIPART_UPLOAD_PARTKEY,
        auditMap, exception,
        getOmRequest().getUserInfo()));

    switch (result) {
    case SUCCESS:
      LOG.debug("MultipartUpload Commit is successfully for Key:{} in " +
          "Volume/Bucket {}/{}", keyName, volumeName, bucketName);
      break;
    case FAILURE:
      ozoneManager.getMetrics().incNumCommitMultipartUploadPartFails();
      LOG.error("MultipartUpload Commit is failed for Key:{} in " +
          "Volume/Bucket {}/{}", keyName, volumeName, bucketName,
          exception);
      break;
    default:
      LOG.error("Unrecognized Result for S3MultipartUploadCommitPartRequest: " +
          "{}", multipartCommitUploadPartRequest);
    }
  }

  private String getMultipartKey(String volumeName, String bucketName,
      String keyName, OMMetadataManager omMetadataManager, String uploadID) {
    return omMetadataManager.getMultipartKey(volumeName, bucketName,
        keyName, uploadID);
  }

  @OMLayoutVersionValidator(
      applyBefore = OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CommitMultiPartUpload
  )
  public static OMRequest disallowCommitMultiPartUploadWithECReplicationConfig(
      OMRequest req, ValidationContext ctx) throws OMException {
    if (!ctx.versionManager().isAllowed(
        OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT)) {
      if (req.getCommitMultiPartUploadRequest().getKeyArgs()
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
   * Validates S3 MPU commit part requests.
   * We do not want to allow older clients to commit MPU keys to buckets which
   * use non LEGACY layouts.
   *
   * @param req - the request to validate
   * @param ctx - the validation context
   * @return the validated request
   * @throws OMException if the request is invalid
   */
  @OMClientVersionValidator(
      applyBefore = ClientVersion.BUCKET_LAYOUT_SUPPORT,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CommitMultiPartUpload
  )
  public static OMRequest blockMPUCommitWithBucketLayoutFromOldClient(
      OMRequest req, ValidationContext ctx) throws IOException {
    if (req.getCommitMultiPartUploadRequest().hasKeyArgs()) {
      KeyArgs keyArgs = req.getCommitMultiPartUploadRequest().getKeyArgs();

      if (keyArgs.hasVolumeName() && keyArgs.hasBucketName()) {
        BucketLayout bucketLayout = ctx.getBucketLayout(
            keyArgs.getVolumeName(), keyArgs.getBucketName());
        bucketLayout.validateSupportedOperation();
      }
    }
    return req;
  }
}
