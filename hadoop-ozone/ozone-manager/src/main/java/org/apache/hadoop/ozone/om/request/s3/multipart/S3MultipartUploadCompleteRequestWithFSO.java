/*
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

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3MultipartUploadCompleteResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3MultipartUploadCompleteResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS;

/**
 * Handle Multipart upload complete request.
 */
public class S3MultipartUploadCompleteRequestWithFSO
        extends S3MultipartUploadCompleteRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3MultipartUploadCompleteRequestWithFSO.class);

  public S3MultipartUploadCompleteRequestWithFSO(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  protected void checkDirectoryAlreadyExists(OzoneManager ozoneManager,
      String volumeName, String bucketName, String keyName,
      OMMetadataManager omMetadataManager) throws IOException {

    Path keyPath = Paths.get(keyName);
    OMFileRequest.OMPathInfoWithFSO pathInfoFSO =
        OMFileRequest.verifyDirectoryKeysInPath(omMetadataManager,
            volumeName, bucketName, keyName, keyPath);
    // Check for directory exists with same name, if it exists throw error.
    if (pathInfoFSO.getDirectoryResult() == DIRECTORY_EXISTS) {
      throw new OMException("Can not Complete MPU for file: " + keyName +
          " as there is already directory in the given path",
          NOT_A_FILE);
    }
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
      String ozoneKey, OmKeyInfo omKeyInfo, long transactionLogIndex) {

    // Add key entry to file table.
    OMFileRequest.addFileTableCacheEntry(omMetadataManager, ozoneKey, omKeyInfo,
        omKeyInfo.getFileName(), transactionLogIndex);
  }

  @Override
  protected void updatePrefixFSOInfo(OmKeyInfo dbOpenKeyInfo,
                                     OmKeyInfo.Builder builder) {
    // updates parentID and fileName
    builder.setParentObjectID(dbOpenKeyInfo.getParentObjectID());
    builder.setFileName(dbOpenKeyInfo.getFileName());
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

    return omMetadataManager.getOzonePathKey(parentId, fileName);
  }

  @Override
  protected String getDBMultipartOpenKey(String volumeName, String bucketName,
      String keyName, String uploadID, OMMetadataManager omMetadataManager)
      throws IOException {

    long parentId =
        getParentId(omMetadataManager, volumeName, bucketName, keyName);

    String fileName = keyName;
    Path filePath = Paths.get(keyName).getFileName();
    if (filePath != null) {
      fileName = filePath.toString();
    }

    return omMetadataManager.getMultipartKey(parentId, fileName, uploadID);
  }

  @Override
  protected S3MultipartUploadCompleteResponse getOmClientResponse(
      OzoneManagerProtocolProtos.OMResponse.Builder omResponse,
      IOException exception) {

    return new S3MultipartUploadCompleteResponseWithFSO(
        createErrorOMResponse(omResponse, exception));
  }

  @Override
  protected OMClientResponse getOmClientResponse(String multipartKey,
      OzoneManagerProtocolProtos.OMResponse.Builder omResponse,
      String dbMultipartOpenKey, OmKeyInfo omKeyInfo,
      List<OmKeyInfo> unUsedParts) {

    return new S3MultipartUploadCompleteResponseWithFSO(omResponse.build(),
        multipartKey, dbMultipartOpenKey, omKeyInfo, unUsedParts);
  }

  private long getParentId(OMMetadataManager omMetadataManager,
      String volumeName, String bucketName, String keyName) throws IOException {

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
        omMetadataManager.getBucketTable().get(bucketKey);
    long bucketId = omBucketInfo.getObjectID();
    Iterator<Path> pathComponents = Paths.get(keyName).iterator();
    return OMFileRequest
        .getParentID(bucketId, pathComponents, keyName, omMetadataManager);
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}

