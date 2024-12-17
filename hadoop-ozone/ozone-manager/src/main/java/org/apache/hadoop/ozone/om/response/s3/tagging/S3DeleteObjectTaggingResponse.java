/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.response.s3.tagging;

import jakarta.annotation.Nonnull;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.key.OmKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

/**
 * Response for delete object tagging request.
 */
@CleanupTableInfo(cleanupTables = {KEY_TABLE})
public class S3DeleteObjectTaggingResponse extends OmKeyResponse {

  private OmKeyInfo omKeyInfo;

  public S3DeleteObjectTaggingResponse(@Nonnull OMResponse omResponse,
                                       @Nonnull OmKeyInfo omKeyInfo) {
    super(omResponse);
    this.omKeyInfo = omKeyInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public S3DeleteObjectTaggingResponse(@Nonnull OMResponse omResponse,
                                       @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
                              BatchOperation batchOperation) throws IOException {
    omMetadataManager.getKeyTable(getBucketLayout()).putWithBatch(batchOperation,
        omMetadataManager.getOzoneKey(
            omKeyInfo.getVolumeName(),
            omKeyInfo.getBucketName(),
            omKeyInfo.getKeyName()),
        omKeyInfo
    );
  }

  protected OmKeyInfo getOmKeyInfo() {
    return omKeyInfo;
  }

}
