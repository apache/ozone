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

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.HasCompletedRequestInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

/**
 * Response for DeleteKey request.
 */
@CleanupTableInfo(cleanupTables = {KEY_TABLE, OPEN_KEY_TABLE, DELETED_TABLE, BUCKET_TABLE})
public class OMKeyDeleteResponse extends AbstractOMKeyDeleteResponse implements HasCompletedRequestInfo {

  private OmKeyInfo omKeyInfo;
  private OmBucketInfo omBucketInfo;
  // If not null, this key will be deleted from OpenKeyTable
  private OmKeyInfo deletedOpenKeyInfo;

  public OMKeyDeleteResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmKeyInfo omKeyInfo,
      @Nonnull OmBucketInfo omBucketInfo, OmKeyInfo deletedOpenKeyInfo) {
    super(omResponse, omBucketInfo.getBucketLayout());
    this.omKeyInfo = omKeyInfo;
    this.omBucketInfo = omBucketInfo;
    this.deletedOpenKeyInfo = deletedOpenKeyInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMKeyDeleteResponse(@Nonnull OMResponse omResponse, @Nonnull
                             BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // For OmResponse with failure, this should do nothing. This method is
    // not called in failure scenario in OM code.
    String ozoneKey = omMetadataManager.getOzoneKey(omKeyInfo.getVolumeName(),
        omKeyInfo.getBucketName(), omKeyInfo.getKeyName());
    Table<String, OmKeyInfo> keyTable =
        omMetadataManager.getKeyTable(getBucketLayout());
    addDeletionToBatch(omMetadataManager, batchOperation, keyTable, ozoneKey,
        omKeyInfo, omBucketInfo.getObjectID(), true);

    // update bucket usedBytes.
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
            omBucketInfo.getBucketName()), omBucketInfo);

    // necessary when the file is hsync'ed but not committed
    // Update metadata which will be used to cleanup openKey in openKeyCleanupService
    if (deletedOpenKeyInfo != null) {
      String hsyncClientId = deletedOpenKeyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
      if (hsyncClientId != null) {
        String dbOpenKey = omMetadataManager.getOpenKey(deletedOpenKeyInfo.getVolumeName(),
            deletedOpenKeyInfo.getBucketName(), deletedOpenKeyInfo.getKeyName(), hsyncClientId);
        omMetadataManager.getOpenKeyTable(getBucketLayout()).putWithBatch(
            batchOperation, dbOpenKey, deletedOpenKeyInfo);
      }
    }
  }

  protected OmKeyInfo getOmKeyInfo() {
    return omKeyInfo;
  }

  protected OmKeyInfo getDeletedOpenKeyInfo() {
    return deletedOpenKeyInfo;
  }

  protected OmBucketInfo getOmBucketInfo() {
    return omBucketInfo;
  }

  @Override
  public OmCompletedRequestInfo getCompletedRequestInfo(long trxnLogIndex) {
    return OmCompletedRequestInfo.newBuilder()
        .setTrxLogIndex(trxnLogIndex)
        .setCmdType(Type.DeleteKey)
        .setCreationTime(System.currentTimeMillis())
        .setVolumeName(omKeyInfo.getVolumeName())
        .setBucketName(omKeyInfo.getBucketName())
        .setKeyName(omKeyInfo.getKeyName())
        .setOpArgs(new OmCompletedRequestInfo.OperationArgs.NoArgs())
        .build();
  }
}
