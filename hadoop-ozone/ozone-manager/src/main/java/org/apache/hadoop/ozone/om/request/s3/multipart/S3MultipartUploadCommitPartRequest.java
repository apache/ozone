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
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart
    .S3MultipartUploadCommitPartResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartCommitUploadPartRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartCommitUploadPartResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handle Multipart upload commit upload part file.
 */
public class S3MultipartUploadCommitPartRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3MultipartUploadCommitPartRequest.class);

  public S3MultipartUploadCommitPartRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    MultipartCommitUploadPartRequest multipartCommitUploadPartRequest =
        getOmRequest().getCommitMultiPartUploadRequest();

    KeyArgs keyArgs = multipartCommitUploadPartRequest.getKeyArgs();
    return getOmRequest().toBuilder().setCommitMultiPartUploadRequest(
        multipartCommitUploadPartRequest.toBuilder()
            .setKeyArgs(keyArgs.toBuilder().setModificationTime(Time.now())
                .setKeyName(validateAndNormalizeKey(
                    ozoneManager.getEnableFileSystemPaths(),
                    keyArgs.getKeyName()))))
        .setUserInfo(getUserInfo()).build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    MultipartCommitUploadPartRequest multipartCommitUploadPartRequest =
        getOmRequest().getCommitMultiPartUploadRequest();

    KeyArgs keyArgs = multipartCommitUploadPartRequest.getKeyArgs();
    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    ozoneManager.getMetrics().incNumCommitMultipartUploadParts();

    boolean acquiredLock = false;

    IOException exception = null;
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
    try {
      keyArgs = resolveBucketLink(ozoneManager, keyArgs, auditMap);
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();

      // TODO to support S3 ACL later.
      acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
          volumeName, bucketName);

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      String uploadID = keyArgs.getMultipartUploadID();
      multipartKey = omMetadataManager.getMultipartKey(volumeName, bucketName,
          keyName, uploadID);

      multipartKeyInfo = omMetadataManager.getMultipartInfoTable()
          .get(multipartKey);

      long clientID = multipartCommitUploadPartRequest.getClientID();

      openKey = omMetadataManager.getOpenKey(
          volumeName, bucketName, keyName, clientID);

      String ozoneKey = omMetadataManager.getOzoneKey(
          volumeName, bucketName, keyName);

      omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

      if (omKeyInfo == null) {
        throw new OMException("Failed to commit Multipart Upload key, as " +
            openKey + "entry is not found in the openKey table",
            KEY_NOT_FOUND);
      }

      // set the data size and location info list
      omKeyInfo.setDataSize(keyArgs.getDataSize());
      omKeyInfo.updateLocationInfoList(keyArgs.getKeyLocationsList().stream()
          .map(OmKeyLocationInfo::getFromProtobuf)
          .collect(Collectors.toList()));
      // Set Modification time
      omKeyInfo.setModificationTime(keyArgs.getModificationTime());
      // Set the UpdateID to current transactionLogIndex
      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      partName = ozoneKey + clientID;

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

      int partNumber = keyArgs.getMultipartNumber();
      oldPartKeyInfo = multipartKeyInfo.getPartKeyInfo(partNumber);

      // Build this multipart upload part info.
      OzoneManagerProtocolProtos.PartKeyInfo.Builder partKeyInfo =
          OzoneManagerProtocolProtos.PartKeyInfo.newBuilder();
      partKeyInfo.setPartName(partName);
      partKeyInfo.setPartNumber(partNumber);
      partKeyInfo.setPartKeyInfo(omKeyInfo.getProtobuf(
          getOmRequest().getVersion()));

      // Add this part information in to multipartKeyInfo.
      multipartKeyInfo.addPartKeyInfo(partNumber, partKeyInfo.build());

      // Set the UpdateID to current transactionLogIndex
      multipartKeyInfo.setUpdateID(trxnLogIndex,
          ozoneManager.isRatisEnabled());

      // OldPartKeyInfo will be deleted. Its updateID will be set in
      // S3MultipartUplodaCommitPartResponse before being added to
      // DeletedKeyTable.

      // Add to cache.

      // Delete from open key table and add it to multipart info table.
      // No need to add cache entries to delete table, as no
      // read/write requests that info for validation.
      omMetadataManager.getMultipartInfoTable().addCacheEntry(
          new CacheKey<>(multipartKey),
          new CacheValue<>(Optional.of(multipartKeyInfo),
              trxnLogIndex));

      omMetadataManager.getOpenKeyTable().addCacheEntry(
          new CacheKey<>(openKey),
          new CacheValue<>(Optional.absent(), trxnLogIndex));

      long scmBlockSize = ozoneManager.getScmBlockSize();
      int factor = omKeyInfo.getFactor().getNumber();
      omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);
      // Block was pre-requested and UsedBytes updated when createKey and
      // AllocatedBlock. The space occupied by the Key shall be based on
      // the actual Key size, and the total Block size applied before should
      // be subtracted.
      long correctedSpace = omKeyInfo.getDataSize() * factor -
          keyArgs.getKeyLocationsList().size() * scmBlockSize * factor;
      omBucketInfo.incrUsedBytes(correctedSpace);

      omResponse.setCommitMultiPartUploadResponse(
          MultipartCommitUploadPartResponse.newBuilder()
              .setPartName(partName));
      omClientResponse = new S3MultipartUploadCommitPartResponse(
          omResponse.build(), multipartKey, openKey,
          multipartKeyInfo, oldPartKeyInfo, omKeyInfo,
          ozoneManager.isRatisEnabled(),
          omBucketInfo.copyObject());

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new S3MultipartUploadCommitPartResponse(
          createErrorOMResponse(omResponse, exception), multipartKey, openKey,
          multipartKeyInfo, oldPartKeyInfo, omKeyInfo,
          ozoneManager.isRatisEnabled(), copyBucketInfo);
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK,
            volumeName, bucketName);
      }
    }

    // audit log
    // Add MPU related information.
    auditMap.put(OzoneConsts.MULTIPART_UPLOAD_PART_NUMBER,
        String.valueOf(keyArgs.getMultipartNumber()));
    auditMap.put(OzoneConsts.MULTIPART_UPLOAD_PART_NAME, partName);
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
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

    return omClientResponse;
  }

}

