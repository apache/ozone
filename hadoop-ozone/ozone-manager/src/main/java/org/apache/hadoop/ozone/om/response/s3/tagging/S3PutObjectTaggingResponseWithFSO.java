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

package org.apache.hadoop.ozone.om.response.s3.tagging;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for put object tagging request for FSO bucket.
 */
@CleanupTableInfo(cleanupTables = {FILE_TABLE})
public class S3PutObjectTaggingResponseWithFSO extends S3PutObjectTaggingResponse {

  private long volumeId;
  private long bucketId;

  public S3PutObjectTaggingResponseWithFSO(@Nonnull OMResponse omResponse,
                                           @Nonnull OmKeyInfo omKeyInfo,
                                           @Nonnull long volumeId,
                                           @Nonnull long bucketId) {
    super(omResponse, omKeyInfo);
    this.volumeId = volumeId;
    this.bucketId = bucketId;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public S3PutObjectTaggingResponseWithFSO(@Nonnull OMResponse omResponse,
                                           @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
                              BatchOperation batchOperation) throws IOException {
    String ozoneDbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
        getOmKeyInfo().getParentObjectID(), getOmKeyInfo().getFileName());
    omMetadataManager.getKeyTable(getBucketLayout())
        .putWithBatch(batchOperation, ozoneDbKey, getOmKeyInfo());
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
