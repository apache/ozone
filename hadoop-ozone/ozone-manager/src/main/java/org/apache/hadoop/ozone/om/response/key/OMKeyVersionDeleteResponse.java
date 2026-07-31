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
 * Response for {@code DELETE ?versionId=} against a noncurrent version: the
 * version is removed from the versionedKeyTable and its blocks go to the
 * deletedTable, which is the single path through which version blocks are
 * reclaimed. The key's current version is untouched.
 */
@CleanupTableInfo(cleanupTables = {VERSIONED_KEY_TABLE, DELETED_TABLE, BUCKET_TABLE})
public class OMKeyVersionDeleteResponse extends AbstractOMKeyDeleteResponse {

  private final OmKeyInfo deletedVersion;
  private final String versionedKeyName;
  private final OmBucketInfo omBucketInfo;

  public OMKeyVersionDeleteResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmKeyInfo deletedVersion, @Nonnull String versionedKeyName,
      @Nonnull OmBucketInfo omBucketInfo) {
    super(omResponse, omBucketInfo.getBucketLayout());
    this.deletedVersion = deletedVersion;
    this.versionedKeyName = versionedKeyName;
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
    this.versionedKeyName = null;
    this.omBucketInfo = null;
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    addDeletionToBatch(omMetadataManager, batchOperation,
        omMetadataManager.getVersionedKeyTable(), versionedKeyName,
        deletedVersion, omBucketInfo.getObjectID(), true);

    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
            omBucketInfo.getBucketName()), omBucketInfo);
  }
}
