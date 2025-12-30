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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.getParentId;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3MultipartUploadCompleteResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3MultipartUploadCompleteResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

/**
 * Handle Multipart upload complete request.
 */
public class S3MultipartUploadCompleteRequestWithFSO
        extends S3MultipartUploadCompleteRequest {

  public S3MultipartUploadCompleteRequestWithFSO(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  protected void checkDirectoryAlreadyExists(OzoneManager ozoneManager,
      OmBucketInfo omBucketInfo, String keyName,
      OMMetadataManager omMetadataManager) throws IOException {

    Path keyPath = Paths.get(keyName);
    OMFileRequest.OMPathInfoWithFSO pathInfoFSO =
        OMFileRequest.verifyDirectoryKeysInPath(omMetadataManager,
            omBucketInfo.getVolumeName(), omBucketInfo.getBucketName(),
            keyName, keyPath);
    // Check for directory exists with same name, if it exists throw error.
    if (pathInfoFSO.getDirectoryResult() == DIRECTORY_EXISTS) {
      throw new OMException("Can not Complete MPU for file: " + keyName +
          " as there is already directory in the given path",
          NOT_A_FILE);
    }
  }

  @Override
  protected void addMissingParentsToCache(OmBucketInfo omBucketInfo,
      List<OmDirectoryInfo> missingParentInfos,
      OMMetadataManager omMetadataManager, long volumeId, long bucketId,
      long transactionLogIndex) throws IOException {

    // validate and update namespace for missing parent directory.
    checkBucketQuotaInNamespace(omBucketInfo, missingParentInfos.size());
    omBucketInfo.incrUsedNamespace(missingParentInfos.size());

    // Add cache entries for the missing parent directories.
    OMFileRequest.addDirectoryTableCacheEntries(omMetadataManager,
        volumeId, bucketId, transactionLogIndex,
        missingParentInfos, null);
  }

  @Override
  protected void addMultiPartToCache(
      OMMetadataManager omMetadataManager, String multipartOpenKey,
      OMFileRequest.OMPathInfoWithFSO pathInfoFSO, OmKeyInfo omKeyInfo,
      String keyName, long transactionLogIndex
  ) throws IOException {

    // Add multi part to cache
    OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager,
        multipartOpenKey, omKeyInfo,
        keyName, transactionLogIndex);

  }

  @Override
  protected OmKeyInfo getOmKeyInfoFromKeyTable(String dbOzoneFileKey,
      String keyName, OMMetadataManager omMetadataManager) throws IOException {
    return OMFileRequest.getOmKeyInfoFromFileTable(false,
            omMetadataManager, dbOzoneFileKey, keyName);
  }

  @Override
  protected OmKeyInfo getOmKeyInfoFromOpenKeyTable(String dbMultipartKey,
      String keyName, OMMetadataManager omMetadataManager) throws IOException {
    return OMFileRequest.getOmKeyInfoFromFileTable(true,
            omMetadataManager, dbMultipartKey, keyName);
  }

  @Override
  protected void addKeyTableCacheEntry(OMMetadataManager omMetadataManager,
      String ozoneKey, OmKeyInfo omKeyInfo, long transactionLogIndex)
      throws IOException {

    // Add key entry to file table.
    OMFileRequest.addFileTableCacheEntry(omMetadataManager, ozoneKey, omKeyInfo,
        omKeyInfo.getFileName(), transactionLogIndex);
  }

  @Override
  protected String getDBOzoneKey(OMMetadataManager omMetadataManager,
      String volumeName, String bucketName, String keyName)throws IOException {

    long parentId =
        getParentId(omMetadataManager, volumeName, bucketName, keyName);

    String fileName = keyName;
    Path filePath = Paths.get(keyName).getFileName();
    if (filePath != null) {
      fileName = filePath.toString();
    }

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    return omMetadataManager.getOzonePathKey(volumeId, bucketId,
            parentId, fileName);
  }

  @Override
  protected String getDBMultipartOpenKey(String volumeName, String bucketName,
      String keyName, String uploadID, OMMetadataManager omMetadataManager)
      throws IOException {
    return omMetadataManager.getMultipartKeyFSO(volumeName, bucketName, keyName, uploadID);
  }

  @Override
  protected S3MultipartUploadCompleteResponse getOmClientResponse(
      OzoneManagerProtocolProtos.OMResponse.Builder omResponse,
      Exception exception) {

    return new S3MultipartUploadCompleteResponseWithFSO(
        createErrorOMResponse(omResponse, exception), getBucketLayout());
  }

  @Override
  protected OMClientResponse getOmClientResponse(String multipartKey,
      OzoneManagerProtocolProtos.OMResponse.Builder omResponse,
      String dbMultipartOpenKey, OmKeyInfo omKeyInfo,
      List<OmKeyInfo> allKeyInfoToRemove, OmBucketInfo omBucketInfo,
      long volumeId, long bucketId, List<OmDirectoryInfo> missingParentInfos,
      OmMultipartKeyInfo multipartKeyInfo) {

    return new S3MultipartUploadCompleteResponseWithFSO(omResponse.build(),
        multipartKey, dbMultipartOpenKey, omKeyInfo, allKeyInfoToRemove,
        getBucketLayout(), omBucketInfo, volumeId, bucketId,
        missingParentInfos, multipartKeyInfo);
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}

