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
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VERSIONED_KEY_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for {@code DELETE ?versionId=}: the version leaves the table that
 * held it and its blocks go to the deletedTable, which is the single path
 * through which version blocks are reclaimed. Removing the current version
 * promotes the newest remaining one into the keyTable in the same batch, so the
 * key never lacks a current version; if none remains the key disappears.
 */
@CleanupTableInfo(cleanupTables = {KEY_TABLE, VERSIONED_KEY_TABLE, DELETED_TABLE, BUCKET_TABLE})
public class OMKeyVersionDeleteResponse extends AbstractOMKeyDeleteResponse {

  private final OmKeyInfo deletedVersion;
  private final String deletedKeyName;
  private final boolean deletedCurrent;
  private final String promotedKeyName;
  private final OmKeyInfo promoted;
  private final OmBucketInfo omBucketInfo;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public OMKeyVersionDeleteResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmKeyInfo deletedVersion, @Nonnull String deletedKeyName,
      boolean deletedCurrent, String promotedKeyName, OmKeyInfo promoted,
      @Nonnull OmBucketInfo omBucketInfo) {
    super(omResponse, omBucketInfo.getBucketLayout());
    this.deletedVersion = deletedVersion;
    this.deletedKeyName = deletedKeyName;
    this.deletedCurrent = deletedCurrent;
    this.promotedKeyName = promotedKeyName;
    this.promoted = promoted;
    this.omBucketInfo = omBucketInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMKeyVersionDeleteResponse(@Nonnull OMResponse omResponse,
      @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    this.deletedVersion = null;
    this.deletedKeyName = null;
    this.deletedCurrent = false;
    this.promotedKeyName = null;
    this.promoted = null;
    this.omBucketInfo = null;
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    // The version leaves whichever table held it and its blocks go to the
    // deletedTable; when it was the current version the successor takes its
    // place in the keyTable, so the delete and the promotion land in one batch.
    addDeletionToBatch(omMetadataManager, batchOperation,
        deletedCurrent ? omMetadataManager.getKeyTable(getBucketLayout())
            : omMetadataManager.getVersionedKeyTable(),
        deletedKeyName, deletedVersion, omBucketInfo.getObjectID(), true);

    if (promoted != null) {
      omMetadataManager.getKeyTable(getBucketLayout())
          .putWithBatch(batchOperation, deletedKeyName, promoted);
      omMetadataManager.getVersionedKeyTable()
          .deleteWithBatch(batchOperation, promotedKeyName);
    }

    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
            omBucketInfo.getBucketName()), omBucketInfo);
  }
}
