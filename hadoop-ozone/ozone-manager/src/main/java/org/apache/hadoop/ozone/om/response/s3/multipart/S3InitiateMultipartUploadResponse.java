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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.MULTIPART_INFO_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.key.OmKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for S3 Initiate Multipart Upload request.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, MULTIPART_INFO_TABLE})
public class S3InitiateMultipartUploadResponse extends OmKeyResponse {
  private OmMultipartKeyInfo omMultipartKeyInfo;
  private OmKeyInfo omKeyInfo;

  public S3InitiateMultipartUploadResponse(
      @Nonnull OMResponse omResponse,
      @Nonnull OmMultipartKeyInfo omMultipartKeyInfo,
      @Nonnull OmKeyInfo omKeyInfo, @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    this.omMultipartKeyInfo = omMultipartKeyInfo;
    this.omKeyInfo = omKeyInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public S3InitiateMultipartUploadResponse(@Nonnull OMResponse omResponse,
                                           @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String multipartKey =
        omMetadataManager.getMultipartKey(omKeyInfo.getVolumeName(),
            omKeyInfo.getBucketName(), omKeyInfo.getKeyName(),
            omMultipartKeyInfo.getUploadID());

    omMetadataManager.getOpenKeyTable(getBucketLayout())
        .putWithBatch(batchOperation, multipartKey, omKeyInfo);
    omMetadataManager.getMultipartInfoTable().putWithBatch(batchOperation,
        multipartKey, omMultipartKeyInfo);
  }

  @VisibleForTesting
  public OmMultipartKeyInfo getOmMultipartKeyInfo() {
    return omMultipartKeyInfo;
  }

  @VisibleForTesting
  public OmKeyInfo getOmKeyInfo() {
    return omKeyInfo;
  }
}
