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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.MULTIPART_INFO_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.key.OmKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for Multipart Upload Complete request.
 *
 * This performs:
 * 1) Delete multipart key from OpenKeyTable, MPUTable,
 * 2) Add key to KeyTable,
 * 3) Delete unused parts.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, KEY_TABLE, DELETED_TABLE,
    MULTIPART_INFO_TABLE, BUCKET_TABLE})
public class S3MultipartUploadCompleteResponse extends OmKeyResponse {
  private String multipartKey;
  private String multipartOpenKey;
  private OmKeyInfo omKeyInfo;
  private List<OmKeyInfo> allKeyInfoToRemove;
  private OmBucketInfo omBucketInfo;
  private long bucketId;

  @SuppressWarnings("parameternumber")
  public S3MultipartUploadCompleteResponse(
      @Nonnull OMResponse omResponse,
      @Nonnull String multipartKey,
      @Nonnull String multipartOpenKey,
      @Nonnull OmKeyInfo omKeyInfo,
      @Nonnull List<OmKeyInfo> allKeyInfoToRemove,
      @Nonnull BucketLayout bucketLayout,
      OmBucketInfo omBucketInfo,
      long bucketId) {
    super(omResponse, bucketLayout);
    this.allKeyInfoToRemove = allKeyInfoToRemove;
    this.multipartKey = multipartKey;
    this.multipartOpenKey = multipartOpenKey;
    this.omKeyInfo = omKeyInfo;
    this.omBucketInfo = omBucketInfo;
    this.bucketId = bucketId;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public S3MultipartUploadCompleteResponse(@Nonnull OMResponse omResponse,
                                           @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // 1. Delete multipart key from OpenKeyTable, MPUTable
    omMetadataManager.getOpenKeyTable(getBucketLayout())
        .deleteWithBatch(batchOperation, multipartOpenKey);
    omMetadataManager.getMultipartInfoTable().deleteWithBatch(batchOperation,
        multipartKey);

    // 2. Add key to KeyTable
    addToKeyTable(omMetadataManager, batchOperation);

    // 3. Delete unused parts
    if (!allKeyInfoToRemove.isEmpty()) {
      // Add unused parts to deleted key table.
      for (OmKeyInfo keyInfoToRemove : allKeyInfoToRemove) {
        String deleteKey = omMetadataManager.getOzoneDeletePathKey(
            keyInfoToRemove.getObjectID(), multipartKey);
        omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
            deleteKey, new RepeatedOmKeyInfo(keyInfoToRemove, bucketId));
      }
    }

    // update bucket usedBytes, only when total bucket size has changed
    // due to unused parts cleanup or an overwritten version.
    if (omBucketInfo != null) {
      omMetadataManager.getBucketTable().putWithBatch(batchOperation,
              omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
                      omBucketInfo.getBucketName()), omBucketInfo);
    }
  }

  protected String addToKeyTable(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String ozoneKey = omMetadataManager.getOzoneKey(omKeyInfo.getVolumeName(),
        omKeyInfo.getBucketName(), omKeyInfo.getKeyName());
    omMetadataManager.getKeyTable(getBucketLayout())
        .putWithBatch(batchOperation, ozoneKey, omKeyInfo);
    return ozoneKey;
  }

  protected OmKeyInfo getOmKeyInfo() {
    return omKeyInfo;
  }

  @Nullable
  protected OmBucketInfo getOmBucketInfo() {
    return omBucketInfo;
  }

  protected String getMultiPartKey() {
    return multipartKey;
  }
}
