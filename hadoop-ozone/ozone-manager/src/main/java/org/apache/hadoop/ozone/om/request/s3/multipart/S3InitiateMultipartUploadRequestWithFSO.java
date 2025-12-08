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
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneConfigUtil;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3InitiateMultipartUploadResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartInfoInitiateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartInfoInitiateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;

/**
 * Handles initiate multipart upload request.
 */
public class S3InitiateMultipartUploadRequestWithFSO
        extends S3InitiateMultipartUploadRequest {

  public S3InitiateMultipartUploadRequestWithFSO(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
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
    List<OmDirectoryInfo> missingParentInfos;
    Result result = null;

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;
    try {

      mergeOmLockDetails(
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
              bucketName));
      acquiredBucketLock = getOmLockDetails().isLockAcquired();

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      OMFileRequest.OMPathInfoWithFSO pathInfoFSO = OMFileRequest
          .verifyDirectoryKeysInPath(omMetadataManager, volumeName, bucketName,
              keyName, Paths.get(keyName));

      // check if the directory already existed in OM
      checkDirectoryResult(keyName, pathInfoFSO.getDirectoryResult());

      final OmBucketInfo bucketInfo = getBucketInfo(omMetadataManager,
          volumeName, bucketName);

      // add all missing parents to dir table
      missingParentInfos = getAllMissingParentDirInfo(ozoneManager, keyArgs, bucketInfo,
              pathInfoFSO, transactionLogIndex);

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

      final long volumeId = omMetadataManager.getVolumeId(volumeName);
      final long bucketId = omMetadataManager.getBucketId(volumeName,
              bucketName);

      String multipartOpenKey = omMetadataManager
          .getMultipartKey(volumeId, bucketId,
                  pathInfoFSO.getLastKnownParentId(),
                  pathInfoFSO.getLeafNodeName(),
                  keyArgs.getMultipartUploadID());

      // Even if this key already exists in the KeyTable, it would be taken
      // care of in the final complete multipart upload. AWS S3 behavior is
      // also like this, even when key exists in a bucket, user can still
      // initiate MPU.
      final ReplicationConfig replicationConfig = OzoneConfigUtil
          .resolveReplicationConfigPreference(keyArgs.getType(),
              keyArgs.getFactor(), keyArgs.getEcReplicationConfig(),
              bucketInfo != null ?
                  bucketInfo.getDefaultReplicationConfig() :
                  null, ozoneManager);

      multipartKeyInfo = new OmMultipartKeyInfo.Builder()
          .setUploadID(keyArgs.getMultipartUploadID())
          .setCreationTime(keyArgs.getModificationTime())
          .setReplicationConfig(replicationConfig)
          .setObjectID(pathInfoFSO.getLeafNodeObjectId())
          .setUpdateID(transactionLogIndex)
          .setParentID(pathInfoFSO.getLastKnownParentId())
          .build();

      omKeyInfo = new OmKeyInfo.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName(keyArgs.getKeyName())
          .setOwnerName(keyArgs.getOwnerName())
          .setCreationTime(keyArgs.getModificationTime())
          .setModificationTime(keyArgs.getModificationTime())
          .setReplicationConfig(replicationConfig)
          .setOmKeyLocationInfos(Collections.singletonList(
              new OmKeyLocationInfoGroup(0, new ArrayList<>(), true)))
          .setAcls(getAclsForKey(keyArgs, bucketInfo, pathInfoFSO,
              ozoneManager.getPrefixManager(), ozoneManager.getConfig()))
          .setObjectID(pathInfoFSO.getLeafNodeObjectId())
          .setUpdateID(transactionLogIndex)
          .setFileEncryptionInfo(keyArgs.hasFileEncryptionInfo() ?
              OMPBHelper.convert(keyArgs.getFileEncryptionInfo()) : null)
          .setParentObjectID(pathInfoFSO.getLastKnownParentId())
          .addAllMetadata(KeyValueUtil.getFromProtobuf(keyArgs.getMetadataList()))
          .addAllTags(KeyValueUtil.getFromProtobuf(keyArgs.getTagsList()))
          .build();
      
      // validate and update namespace for missing parent directory
      if (null != missingParentInfos) {
        checkBucketQuotaInNamespace(bucketInfo, missingParentInfos.size());
        bucketInfo.incrUsedNamespace(missingParentInfos.size());
      }

      // Add cache entries for the prefix directories.
      // Skip adding for the file key itself, until Key Commit.
      OMFileRequest.addDirectoryTableCacheEntries(omMetadataManager,
              volumeId, bucketId, transactionLogIndex,
              missingParentInfos, null);

      OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager,
          multipartOpenKey, omKeyInfo, keyName,
              transactionLogIndex);

      // Add to cache
      omMetadataManager.getMultipartInfoTable().addCacheEntry(
          multipartKey, multipartKeyInfo, transactionLogIndex);

      omClientResponse =
          new S3InitiateMultipartUploadResponseWithFSO(
              omResponse.setInitiateMultiPartUploadResponse(
                  MultipartInfoInitiateResponse.newBuilder()
                      .setVolumeName(requestedVolume)
                      .setBucketName(requestedBucket)
                      .setKeyName(keyName)
                      .setMultipartUploadID(keyArgs.getMultipartUploadID()))
                  .build(), multipartKeyInfo, omKeyInfo, multipartKey,
              missingParentInfos, getBucketLayout(), volumeId, bucketId,
              bucketInfo.copyObject());

      result = Result.SUCCESS;
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new S3InitiateMultipartUploadResponseWithFSO(
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

  /**
   * Verify om directory result.
   *
   * @param keyName           key name
   * @param omDirectoryResult directory result
   * @throws OMException if file or directory or file exists in the given path
   */
  private void checkDirectoryResult(String keyName,
      OMFileRequest.OMDirectoryResult omDirectoryResult) throws OMException {
    if (omDirectoryResult == DIRECTORY_EXISTS) {
      throw new OMException("Can not write to directory: " + keyName,
              OMException.ResultCodes.NOT_A_FILE);
    }
  }
}
