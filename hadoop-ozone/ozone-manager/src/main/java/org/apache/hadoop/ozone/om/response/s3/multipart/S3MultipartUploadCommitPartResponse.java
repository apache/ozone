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
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.MULTIPART_INFO_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.NO_SUCH_MULTIPART_UPLOAD_ERROR;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.key.OmKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for S3MultipartUploadCommitPart request.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, DELETED_TABLE,
    MULTIPART_INFO_TABLE, BUCKET_TABLE})
public class S3MultipartUploadCommitPartResponse extends OmKeyResponse {

  private final String multipartKey;
  private final String openKey;
  private final OmMultipartKeyInfo omMultipartKeyInfo;
  private final Map<String, RepeatedOmKeyInfo> keyToDeleteMap;
  private final OmKeyInfo openPartKeyInfoToBeDeleted;
  private final OmBucketInfo omBucketInfo;
  private final long bucketId;

  /**
   * Regular response.
   * 1. Update MultipartKey in MultipartInfoTable with new PartKeyInfo
   * 2. Delete openKey from OpenKeyTable
   * 3. If old key or uncommitted (pseudo) key exists, put it in DeletedTable
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public S3MultipartUploadCommitPartResponse(@Nonnull OMResponse omResponse,
      String multipartKey, String openKey,
      @Nullable OmMultipartKeyInfo omMultipartKeyInfo,
      @Nullable Map<String, RepeatedOmKeyInfo> keyToDeleteMap,
      @Nullable OmKeyInfo openPartKeyInfoToBeDeleted,
      @Nonnull OmBucketInfo omBucketInfo,
      long bucketId,
      @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    this.multipartKey = multipartKey;
    this.openKey = openKey;
    this.omMultipartKeyInfo = omMultipartKeyInfo;
    this.keyToDeleteMap = keyToDeleteMap;
    this.openPartKeyInfoToBeDeleted = openPartKeyInfoToBeDeleted;
    this.omBucketInfo = omBucketInfo;
    this.bucketId = bucketId;
  }

  @Override
  public void checkAndUpdateDB(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    if (getOMResponse().getStatus() == NO_SUCH_MULTIPART_UPLOAD_ERROR) {
      // Means by the time we try to commit part, some one has aborted this
      // multipart upload. So, delete this part information.

      RepeatedOmKeyInfo repeatedOmKeyInfo =
          OmUtils.prepareKeyForDelete(bucketId, openPartKeyInfoToBeDeleted,
              openPartKeyInfoToBeDeleted.getUpdateID());
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
    // Delete old (overwritten) part upload and uncommitted parts
    if (keyToDeleteMap != null) {
      for (Map.Entry<String, RepeatedOmKeyInfo> entry : keyToDeleteMap.entrySet()) {
        omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
            entry.getKey(), entry.getValue());
      }
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

  @VisibleForTesting
  public Map<String, RepeatedOmKeyInfo> getKeyToDelete() {
    return keyToDeleteMap;
  }
}

