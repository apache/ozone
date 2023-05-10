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

package org.apache.hadoop.ozone.om.response.s3.multipart;

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.key.OmKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.MULTIPARTINFO_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .Status.NO_SUCH_MULTIPART_UPLOAD_ERROR;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .Status.OK;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Response for S3MultipartUploadCommitPart request.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, DELETED_TABLE,
    MULTIPARTINFO_TABLE, BUCKET_TABLE})
public class S3MultipartUploadCommitPartResponse extends OmKeyResponse {

  private String multipartKey;
  private String openKey;
  private OmMultipartKeyInfo omMultipartKeyInfo;
  private OzoneManagerProtocolProtos.PartKeyInfo oldPartKeyInfo;
  private OmKeyInfo openPartKeyInfoToBeDeleted;
  private boolean isRatisEnabled;
  private OmBucketInfo omBucketInfo;

  /**
   * Regular response.
   * 1. Update MultipartKey in MultipartInfoTable with new PartKeyInfo
   * 2. Delete openKey from OpenKeyTable
   * 3. If old PartKeyInfo exists, put it in DeletedKeyTable
   * @param omResponse
   * @param multipartKey
   * @param openKey
   * @param omMultipartKeyInfo
   * @param oldPartKeyInfo
   * @param openPartKeyInfoToBeDeleted
   * @param isRatisEnabled
   * @param omBucketInfo
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public S3MultipartUploadCommitPartResponse(@Nonnull OMResponse omResponse,
      String multipartKey, String openKey,
      @Nullable OmMultipartKeyInfo omMultipartKeyInfo,
      @Nullable OzoneManagerProtocolProtos.PartKeyInfo oldPartKeyInfo,
      @Nullable OmKeyInfo openPartKeyInfoToBeDeleted,
      boolean isRatisEnabled, @Nonnull OmBucketInfo omBucketInfo,
      @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    this.multipartKey = multipartKey;
    this.openKey = openKey;
    this.omMultipartKeyInfo = omMultipartKeyInfo;
    this.oldPartKeyInfo = oldPartKeyInfo;
    this.openPartKeyInfoToBeDeleted = openPartKeyInfoToBeDeleted;
    this.isRatisEnabled = isRatisEnabled;
    this.omBucketInfo = omBucketInfo;
  }

  @Override
  public void checkAndUpdateDB(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    if (getOMResponse().getStatus() == NO_SUCH_MULTIPART_UPLOAD_ERROR) {
      // Means by the time we try to commit part, some one has aborted this
      // multipart upload. So, delete this part information.

      RepeatedOmKeyInfo repeatedOmKeyInfo =
          OmUtils.prepareKeyForDelete(openPartKeyInfoToBeDeleted,
              openPartKeyInfoToBeDeleted.getUpdateID(),
              isRatisEnabled);
      // multi-part key format is volumeName/bucketName/keyName/uploadId
      String deleteKey = omMetadataManager.getOzoneDeletePathKey(
          openPartKeyInfoToBeDeleted.getObjectID(), multipartKey);

      omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
          deleteKey, repeatedOmKeyInfo);
    }

    if (getOMResponse().getStatus() == OK) {
      addToDBBatch(omMetadataManager, batchOperation);
    }
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // If we have old part info:
    // Need to do 3 steps:
    //   0. Strip GDPR related metadata from multipart info
    //   1. add old part to delete table
    //   2. Commit multipart info which has information about this new part.
    //   3. delete this new part entry from open key table.

    // This means for this multipart upload part upload, we have an old
    // part information, so delete it.
    if (oldPartKeyInfo != null) {
      OmKeyInfo partKeyToBeDeleted =
          OmKeyInfo.getFromProtobuf(oldPartKeyInfo.getPartKeyInfo());

      RepeatedOmKeyInfo repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(
          partKeyToBeDeleted,
          omMultipartKeyInfo.getUpdateID(), isRatisEnabled);
      // multi-part key format is volumeName/bucketName/keyName/uploadId
      String deleteKey = omMetadataManager.getOzoneDeletePathKey(
          partKeyToBeDeleted.getObjectID(), multipartKey);

      omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
          deleteKey, repeatedOmKeyInfo);
    }

    omMetadataManager.getMultipartInfoTable().putWithBatch(batchOperation,
        multipartKey, omMultipartKeyInfo);

    //  This information has been added to multipartKeyInfo. So, we can
    //  safely delete part key info from open key table.
    omMetadataManager.getOpenKeyTable(getBucketLayout())
        .deleteWithBatch(batchOperation, openKey);

    // update bucket usedBytes.
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
            omBucketInfo.getBucketName()), omBucketInfo);
  }
}

