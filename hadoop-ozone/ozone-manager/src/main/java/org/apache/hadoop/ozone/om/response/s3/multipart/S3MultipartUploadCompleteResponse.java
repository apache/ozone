/*
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

package org.apache.hadoop.ozone.om.response.s3.multipart;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.MULTIPARTINFO_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;

/**
 * Response for Multipart Upload Complete request.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, KEY_TABLE, DELETED_TABLE,
    MULTIPARTINFO_TABLE})
public class S3MultipartUploadCompleteResponse extends OMClientResponse {
  private String multipartKey;
  private OmKeyInfo omKeyInfo;
  private List<OmKeyInfo> partsUnusedList;

  public S3MultipartUploadCompleteResponse(
      @Nonnull OMResponse omResponse,
      @Nonnull String multipartKey,
      @Nonnull OmKeyInfo omKeyInfo,
      @Nonnull List<OmKeyInfo> unUsedParts) {
    super(omResponse);
    this.partsUnusedList = unUsedParts;
    this.multipartKey = multipartKey;
    this.omKeyInfo = omKeyInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public S3MultipartUploadCompleteResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    omMetadataManager.getOpenKeyTable().deleteWithBatch(batchOperation,
        multipartKey);
    omMetadataManager.getMultipartInfoTable().deleteWithBatch(batchOperation,
        multipartKey);

    String ozoneKey = omMetadataManager.getOzoneKey(omKeyInfo.getVolumeName(),
        omKeyInfo.getBucketName(), omKeyInfo.getKeyName());
    omMetadataManager.getKeyTable().putWithBatch(batchOperation, ozoneKey,
        omKeyInfo);

    if (!partsUnusedList.isEmpty()) {
      // Add unused parts to deleted key table.
      RepeatedOmKeyInfo repeatedOmKeyInfo = omMetadataManager.getDeletedTable()
          .get(ozoneKey);
      if (repeatedOmKeyInfo == null) {
        repeatedOmKeyInfo = new RepeatedOmKeyInfo(partsUnusedList);
      } else {
        repeatedOmKeyInfo.addOmKeyInfo(omKeyInfo);
      }

      omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
          ozoneKey, repeatedOmKeyInfo);
    }
  }

}