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

package org.apache.hadoop.ozone.om.response.s3.multipart;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.MULTIPART_INFO_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_FILE_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
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

/**
 * Response for S3 Initiate Multipart Upload request for prefix layout.
 */
@CleanupTableInfo(cleanupTables = {DIRECTORY_TABLE, OPEN_FILE_TABLE,
    MULTIPART_INFO_TABLE})
public class S3InitiateMultipartUploadResponseWithFSO extends
        S3InitiateMultipartUploadResponse {
  private List<OmDirectoryInfo> parentDirInfos;
  private String mpuDBKey;
  private long volumeId;
  private long bucketId;
  private OmBucketInfo omBucketInfo;

  @SuppressWarnings("parameternumber")
  public S3InitiateMultipartUploadResponseWithFSO(
      @Nonnull OMResponse omResponse,
      @Nonnull OmMultipartKeyInfo omMultipartKeyInfo,
      @Nonnull OmKeyInfo omKeyInfo, @Nonnull String mpuDBKey,
      @Nonnull List<OmDirectoryInfo> parentDirInfos,
      @Nonnull BucketLayout bucketLayout, @Nonnull long volumeId,
      @Nonnull long bucketId, @Nonnull OmBucketInfo omBucketInfo) {
    super(omResponse, omMultipartKeyInfo, omKeyInfo, bucketLayout);
    this.parentDirInfos = parentDirInfos;
    this.mpuDBKey = mpuDBKey;
    this.volumeId = volumeId;
    this.bucketId = bucketId;
    this.omBucketInfo = omBucketInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public S3InitiateMultipartUploadResponseWithFSO(
      @Nonnull OMResponse omResponse, @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    /**
     * Create parent directory entries during MultiPartFileKey Create - do not
     * wait for File Commit request.
     */
    if (parentDirInfos != null) {
      for (OmDirectoryInfo parentDirInfo : parentDirInfos) {
        final String parentKey = omMetadataManager.getOzonePathKey(
                volumeId, bucketId, parentDirInfo.getParentObjectID(),
                parentDirInfo.getName());
        omMetadataManager.getDirectoryTable().putWithBatch(batchOperation,
                parentKey, parentDirInfo);
      }

      // namespace quota changes for parent directory
      String bucketKey = omMetadataManager.getBucketKey(
          omBucketInfo.getVolumeName(),
          omBucketInfo.getBucketName());
      omMetadataManager.getBucketTable().putWithBatch(batchOperation,
          bucketKey, omBucketInfo);
    }

    OMFileRequest.addToOpenFileTableForMultipart(omMetadataManager,
        batchOperation,
        getOmKeyInfo(), getOmMultipartKeyInfo().getUploadID(), volumeId,
        bucketId);

    omMetadataManager.getMultipartInfoTable().putWithBatch(batchOperation,
        mpuDBKey, getOmMultipartKeyInfo());
  }
}
