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

package org.apache.hadoop.ozone.om.response.key;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.HasCompletedRequestInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

/**
 * Response for CommitKey request.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, KEY_TABLE, DELETED_TABLE,
    BUCKET_TABLE})
public class OMKeyCommitResponse extends OmKeyResponse implements HasCompletedRequestInfo {

  private OmKeyInfo omKeyInfo;
  private String ozoneKeyName;
  private String openKeyName;
  private OmBucketInfo omBucketInfo;
  private Map<String, RepeatedOmKeyInfo> keyToDeleteMap;
  private boolean isHSync;
  private OmKeyInfo newOpenKeyInfo;
  private OmKeyInfo openKeyToUpdate;
  private String openKeyNameToUpdate;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public OMKeyCommitResponse(
      @Nonnull OMResponse omResponse,
      @Nonnull OmKeyInfo omKeyInfo, String ozoneKeyName, String openKeyName,
      @Nonnull OmBucketInfo omBucketInfo,
      Map<String, RepeatedOmKeyInfo> keyToDeleteMap,
      boolean isHSync,
      OmKeyInfo newOpenKeyInfo, String openKeyNameToUpdate, OmKeyInfo openKeyToUpdate) {
    super(omResponse, omBucketInfo.getBucketLayout());
    this.omKeyInfo = omKeyInfo;
    this.ozoneKeyName = ozoneKeyName;
    this.openKeyName = openKeyName;
    this.omBucketInfo = omBucketInfo;
    this.keyToDeleteMap = keyToDeleteMap;
    this.isHSync = isHSync;
    this.newOpenKeyInfo = newOpenKeyInfo;
    this.openKeyNameToUpdate = openKeyNameToUpdate;
    this.openKeyToUpdate = openKeyToUpdate;
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
    } else if (newOpenKeyInfo != null) {
      omMetadataManager.getOpenKeyTable(getBucketLayout()).putWithBatch(
          batchOperation, openKeyName, newOpenKeyInfo);
    }

    omMetadataManager.getKeyTable(getBucketLayout())
        .putWithBatch(batchOperation, ozoneKeyName, omKeyInfo);

    updateDeletedTable(omMetadataManager, batchOperation);
    handleOpenKeyToUpdate(omMetadataManager, batchOperation);

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

  protected void handleOpenKeyToUpdate(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    if (this.openKeyToUpdate != null) {
      omMetadataManager.getOpenKeyTable(getBucketLayout()).putWithBatch(
          batchOperation, openKeyNameToUpdate, openKeyToUpdate);
    }
  }

  protected boolean isHSync() {
    return isHSync;
  }

  public OmKeyInfo getNewOpenKeyInfo() {
    return newOpenKeyInfo;
  }

  @Override
  public OmCompletedRequestInfo getCompletedRequestInfo(long trxnLogIndex) {
    return OmCompletedRequestInfo.newBuilder()
        .setTrxLogIndex(trxnLogIndex)
        .setCmdType(Type.CommitKey)
        .setCreationTime(System.currentTimeMillis())
        .setVolumeName(omKeyInfo.getVolumeName())
        .setBucketName(omKeyInfo.getBucketName())
        .setKeyName(omKeyInfo.getKeyName())
        .setOpArgs(new OmCompletedRequestInfo.OperationArgs.NoArgs())
        .build();
  }
}
