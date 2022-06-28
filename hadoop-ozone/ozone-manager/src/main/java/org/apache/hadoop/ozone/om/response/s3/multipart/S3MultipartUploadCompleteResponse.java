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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.key.OmKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.MULTIPARTINFO_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;

/**
 * Response for Multipart Upload Complete request.
 *
 * This performs:
 * 1) Delete multipart key from OpenKeyTable, MPUTable,
 * 2) Add key to KeyTable,
 * 3) Delete unused parts.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, KEY_TABLE, DELETED_TABLE,
    MULTIPARTINFO_TABLE, BUCKET_TABLE})
public class S3MultipartUploadCompleteResponse extends OmKeyResponse {
  private String multipartKey;
  private String multipartOpenKey;
  private OmKeyInfo omKeyInfo;
  private List<OmKeyInfo> partsUnusedList;
  private OmBucketInfo omBucketInfo;
  private RepeatedOmKeyInfo keyVersionsToDelete;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public S3MultipartUploadCompleteResponse(
      @Nonnull OMResponse omResponse,
      @Nonnull String multipartKey,
      @Nonnull String multipartOpenKey,
      @Nonnull OmKeyInfo omKeyInfo,
      @Nonnull List<OmKeyInfo> unUsedParts,
      @Nonnull BucketLayout bucketLayout,
      @Nonnull OmBucketInfo omBucketInfo,
      RepeatedOmKeyInfo keyVersionsToDelete) {
    super(omResponse, bucketLayout);
    this.partsUnusedList = unUsedParts;
    this.multipartKey = multipartKey;
    this.multipartOpenKey = multipartOpenKey;
    this.omKeyInfo = omKeyInfo;
    this.omBucketInfo = omBucketInfo;
    this.keyVersionsToDelete = keyVersionsToDelete;
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
    String ozoneKey = addToKeyTable(omMetadataManager, batchOperation);

    // 3. Delete unused parts
    if (!partsUnusedList.isEmpty()) {
      // Add unused parts to deleted key table.
      if (keyVersionsToDelete == null) {
        keyVersionsToDelete = new RepeatedOmKeyInfo(partsUnusedList);
      } else {
        for (OmKeyInfo unusedParts : partsUnusedList) {
          keyVersionsToDelete.addOmKeyInfo(unusedParts);
        }
      }
    }
    if (keyVersionsToDelete != null) {
      omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
          ozoneKey, keyVersionsToDelete);
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

  protected String getMultipartKey() {
    return multipartKey;
  }

  protected OmKeyInfo getOmKeyInfo() {
    return omKeyInfo;
  }

  protected List<OmKeyInfo> getPartsUnusedList() {
    return partsUnusedList;
  }

  public OmBucketInfo getOmBucketInfo() {
    return omBucketInfo;
  }
}
