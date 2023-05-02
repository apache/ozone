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

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import java.io.IOException;
import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;

/**
 * Response for CommitKey request.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, KEY_TABLE, DELETED_TABLE,
    BUCKET_TABLE})
public class OMKeyCommitResponse extends OmKeyResponse {

  private OmKeyInfo omKeyInfo;
  private String ozoneKeyName;
  private String openKeyName;
  private OmBucketInfo omBucketInfo;
  private Map<String, RepeatedOmKeyInfo> keyToDeleteMap;

  private boolean isHSync;

  public OMKeyCommitResponse(
      @Nonnull OMResponse omResponse,
      @Nonnull OmKeyInfo omKeyInfo, String ozoneKeyName, String openKeyName,
      @Nonnull OmBucketInfo omBucketInfo,
      Map<String, RepeatedOmKeyInfo> keyToDeleteMap,
      boolean isHSync) {
    super(omResponse, omBucketInfo.getBucketLayout());
    this.omKeyInfo = omKeyInfo;
    this.ozoneKeyName = ozoneKeyName;
    this.openKeyName = openKeyName;
    this.omBucketInfo = omBucketInfo;
    this.keyToDeleteMap = keyToDeleteMap;
    this.isHSync = isHSync;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMKeyCommitResponse(@Nonnull OMResponse omResponse, @Nonnull
      BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // Delete from OpenKey table
    if (!isHSync()) {
      omMetadataManager.getOpenKeyTable(getBucketLayout())
              .deleteWithBatch(batchOperation, openKeyName);
    }

    omMetadataManager.getKeyTable(getBucketLayout())
        .putWithBatch(batchOperation, ozoneKeyName, omKeyInfo);

    updateDeletedTable(omMetadataManager, batchOperation);

    // update bucket usedBytes.
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
            omBucketInfo.getBucketName()), omBucketInfo);
  }

  protected String getOpenKeyName() {
    return openKeyName;
  }

  protected OmKeyInfo getOmKeyInfo() {
    return omKeyInfo;
  }

  protected OmBucketInfo getOmBucketInfo() {
    return omBucketInfo;
  }

  protected String getOzoneKeyName() {
    return ozoneKeyName;
  }

  @VisibleForTesting
  public Map<String, RepeatedOmKeyInfo> getKeysToDelete() {
    return keyToDeleteMap;
  }

  protected void updateDeletedTable(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    if (this.keyToDeleteMap != null) {
      for (Map.Entry<String, RepeatedOmKeyInfo> entry : 
          keyToDeleteMap.entrySet()) {
        omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
            entry.getKey(), entry.getValue());
      }
    }
  }

  protected boolean isHSync() {
    return isHSync;
  }
}
