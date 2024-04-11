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

package org.apache.hadoop.ozone.om.response.s3.multipart;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.MULTIPARTINFO_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;

/**
 * Response for Multipart Upload Complete request.
 *
 * This performs:
 * 1) Delete multipart key from OpenFileTable, MPUTable,
 * 2) Add file to FileTable,
 * 3) Delete unused parts.
 */
@CleanupTableInfo(cleanupTables = {OPEN_FILE_TABLE, FILE_TABLE, DELETED_TABLE,
    MULTIPARTINFO_TABLE})
public class S3MultipartUploadCompleteResponseWithFSO
        extends S3MultipartUploadCompleteResponse {

  private long volumeId;
  private long bucketId;

  private List<OmDirectoryInfo> missingParentInfos;

  private OmMultipartKeyInfo multipartKeyInfo;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public S3MultipartUploadCompleteResponseWithFSO(
      @Nonnull OMResponse omResponse,
      @Nonnull String multipartKey,
      @Nonnull String multipartOpenKey,
      @Nonnull OmKeyInfo omKeyInfo,
      @Nonnull List<OmKeyInfo> allKeyInfoToRemove,
      @Nonnull BucketLayout bucketLayout,
      OmBucketInfo omBucketInfo,
      @Nonnull long volumeId, @Nonnull long bucketId,
      List<OmDirectoryInfo> missingParentInfos,
      OmMultipartKeyInfo multipartKeyInfo) {
    super(omResponse, multipartKey, multipartOpenKey, omKeyInfo,
        allKeyInfoToRemove, bucketLayout, omBucketInfo);
    this.volumeId = volumeId;
    this.bucketId = bucketId;
    this.missingParentInfos = missingParentInfos;
    this.multipartKeyInfo = multipartKeyInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public S3MultipartUploadCompleteResponseWithFSO(
      @Nonnull OMResponse omResponse, @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
                           BatchOperation batchOperation) throws IOException {
    if (missingParentInfos != null) {
      // Create missing parent directory entries.
      for (OmDirectoryInfo parentDirInfo : missingParentInfos) {
        final String parentKey = omMetadataManager.getOzonePathKey(
            volumeId, bucketId, parentDirInfo.getParentObjectID(),
            parentDirInfo.getName());
        omMetadataManager.getDirectoryTable().putWithBatch(batchOperation,
            parentKey, parentDirInfo);
      }

      // namespace quota changes for parent directory
      String bucketKey = omMetadataManager.getBucketKey(
          getOmBucketInfo().getVolumeName(),
          getOmBucketInfo().getBucketName());
      omMetadataManager.getBucketTable().putWithBatch(batchOperation,
          bucketKey, getOmBucketInfo());

      if (OMFileRequest.getOmKeyInfoFromFileTable(true,
          omMetadataManager, getMultiPartKey(), getOmKeyInfo().getKeyName())
          != null) {
        // Add multi part to open key table.
        OMFileRequest.addToOpenFileTableForMultipart(omMetadataManager,
            batchOperation,
            getOmKeyInfo(), multipartKeyInfo.getUploadID(), volumeId,
            bucketId);
      }
    }
    super.addToDBBatch(omMetadataManager, batchOperation);
  }

  @Override
  protected String addToKeyTable(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String ozoneKey = omMetadataManager
        .getOzoneKey(getOmKeyInfo().getVolumeName(),
            getOmKeyInfo().getBucketName(), getOmKeyInfo().getKeyName());

    OMFileRequest
        .addToFileTable(omMetadataManager, batchOperation, getOmKeyInfo(),
            volumeId, bucketId);

    return ozoneKey;

  }

}

