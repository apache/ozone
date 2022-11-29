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

package org.apache.hadoop.ozone.om.response.key;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.DeleteTablePrefix;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;

/**
 * Response for DeleteKey request.
 */
@CleanupTableInfo(cleanupTables = {FILE_TABLE, DIRECTORY_TABLE,
    DELETED_TABLE, DELETED_DIR_TABLE, BUCKET_TABLE})
public class OMKeyDeleteResponseWithFSO extends OMKeyDeleteResponse {

  private OmKeyInfo omKeyInfo; // in case when it's a directory
  private boolean isDeleteDirectory;
  private String keyName;
  private long volumeId;

  @SuppressWarnings("parameternumber")
  public OMKeyDeleteResponseWithFSO(@Nonnull OMResponse omResponse,
          @Nonnull String keyName, @Nonnull DeleteTablePrefix prefix,
          @Nonnull OmKeyInfo omKeyInfo, @Nonnull OmBucketInfo omBucketInfo,
          @Nonnull boolean isDeleteDirectory, @Nonnull long volumeId) {
    super(omResponse, prefix, Arrays.asList(omKeyInfo),
        omBucketInfo);

    this.omKeyInfo = omKeyInfo;
    this.keyName = keyName;
    this.isDeleteDirectory = isDeleteDirectory;
    this.volumeId = volumeId;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMKeyDeleteResponseWithFSO(@Nonnull OMResponse omResponse,
                                    @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // For OmResponse with failure, this should do nothing. This method is
    // not called in failure scenario in OM code.
    String ozoneDbKey = omMetadataManager.getOzonePathKey(
        volumeId, getOmBucketInfo().getObjectID(),
        omKeyInfo.getParentObjectID(), omKeyInfo.getFileName());

    if (isDeleteDirectory) {
      omMetadataManager.getDirectoryTable().deleteWithBatch(batchOperation,
              ozoneDbKey);
      // Sets full absolute key name to OmKeyInfo, which is
      // required for moving the sub-files to KeyDeletionService.
      omKeyInfo.setKeyName(keyName);
      String ozoneDeleteKey = omMetadataManager.getOzoneDeletePathKey(
          omKeyInfo.getObjectID(), ozoneDbKey);
      omMetadataManager.getDeletedDirTable().putWithBatch(
          batchOperation, ozoneDeleteKey, omKeyInfo);
    } else {
      // UpdateID is already set, so clearing GDPR data is enough
      deleteFromKeyTable(omMetadataManager, batchOperation);
      insertToDeleteTable(omMetadataManager, batchOperation);
    }

    // update bucket usedBytes.
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
            omMetadataManager.getBucketKey(getOmBucketInfo().getVolumeName(),
                    getOmBucketInfo().getBucketName()), getOmBucketInfo());
  }

  @Override
  protected String getKeyToDelete(OMMetadataManager omMetadataManager,
      OmKeyInfo omKeyInfo1) throws IOException {
    String ozoneDBKey = omMetadataManager.getOzonePathKey(
            volumeId, getOmBucketInfo().getObjectID(),
            omKeyInfo1.getParentObjectID(), omKeyInfo1.getFileName());
    return ozoneDBKey;
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
