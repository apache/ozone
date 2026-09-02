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
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SHARED_BLOCK_GROUP_TABLE;

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
 * Response for a server side key copy. The destination key, the source key's
 * new shared block group tag and the group's sharer count are persisted in one
 * batch, so a key never becomes visible without the count that protects its
 * blocks from being released while the other sharer is still alive.
 */
@CleanupTableInfo(cleanupTables = {KEY_TABLE, SHARED_BLOCK_GROUP_TABLE, BUCKET_TABLE})
public class OMKeyCopyResponse extends OmKeyResponse {

  private OmKeyInfo destinationKeyInfo;
  private String destinationDbKey;
  private OmKeyInfo sourceKeyInfo;
  private String sourceDbKey;
  private long sharedBlockGroupId;
  private long sharerCount;
  private OmBucketInfo omBucketInfo;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public OMKeyCopyResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmKeyInfo destinationKeyInfo, @Nonnull String destinationDbKey,
      OmKeyInfo sourceKeyInfo, @Nonnull String sourceDbKey,
      long sharedBlockGroupId, long sharerCount,
      @Nonnull OmBucketInfo omBucketInfo, @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    this.destinationKeyInfo = destinationKeyInfo;
    this.destinationDbKey = destinationDbKey;
    this.sourceKeyInfo = sourceKeyInfo;
    this.sourceDbKey = sourceDbKey;
    this.sharedBlockGroupId = sharedBlockGroupId;
    this.sharerCount = sharerCount;
    this.omBucketInfo = omBucketInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMKeyCopyResponse(@Nonnull OMResponse omResponse,
      @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    omMetadataManager.getKeyTable(getBucketLayout())
        .putWithBatch(batchOperation, destinationDbKey, destinationKeyInfo);

    // Only set on the first copy, when the source has to be tagged as sharing.
    if (sourceKeyInfo != null) {
      omMetadataManager.getKeyTable(getBucketLayout())
          .putWithBatch(batchOperation, sourceDbKey, sourceKeyInfo);
    }

    omMetadataManager.getSharedBlockGroupTable()
        .putWithBatch(batchOperation, sharedBlockGroupId, sharerCount);

    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
            omBucketInfo.getBucketName()), omBucketInfo);
  }
}
