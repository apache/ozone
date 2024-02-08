/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.response.s3.multipart;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartAbortInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.MULTIPARTINFO_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;

/**
 * Handles response to abort expired MPUs.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, OPEN_FILE_TABLE,
    DELETED_TABLE, MULTIPARTINFO_TABLE, BUCKET_TABLE})
public class S3ExpiredMultipartUploadsAbortResponse extends
    AbstractS3MultipartAbortResponse {

  private Map<OmBucketInfo, List<OmMultipartAbortInfo>> mpusToDelete;

  public S3ExpiredMultipartUploadsAbortResponse(
      @Nonnull OMResponse omResponse,
      @Nonnull Map<OmBucketInfo, List<OmMultipartAbortInfo>> mpusToDelete,
      boolean isRatisEnabled) {
    super(omResponse, isRatisEnabled);
    this.mpusToDelete = mpusToDelete;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public S3ExpiredMultipartUploadsAbortResponse(
      @Nonnull OMResponse omResponse) {
    // Set BucketLayout.DEFAULT just as a placeholder
    // OmMultipartAbortInfo already contains the bucket layout info
    super(omResponse, BucketLayout.DEFAULT);
  }

  @Override
  protected void addToDBBatch(
      OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    for (Map.Entry<OmBucketInfo, List<OmMultipartAbortInfo>> mpuInfoPair :
        mpusToDelete.entrySet()) {
      addAbortToBatch(omMetadataManager, batchOperation,
          mpuInfoPair.getKey(), mpuInfoPair.getValue());
    }
  }
}
