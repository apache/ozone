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

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.MULTIPARTFILEINFO_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;

/**
 * Response for Multipart Upload Complete request.
 */
@CleanupTableInfo(cleanupTables = {OPEN_FILE_TABLE, FILE_TABLE, DELETED_TABLE,
    MULTIPARTFILEINFO_TABLE})
public class S3MultipartUploadCompleteResponseWithFSO
        extends S3MultipartUploadCompleteResponse {

  public S3MultipartUploadCompleteResponseWithFSO(
      @Nonnull OMResponse omResponse,
      @Nonnull String multipartKey,
      @Nonnull OmKeyInfo omKeyInfo,
      @Nonnull List<OmKeyInfo> unUsedParts) {
    super(omResponse, multipartKey, omKeyInfo, unUsedParts);
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public S3MultipartUploadCompleteResponseWithFSO(
      @Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }


  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    omMetadataManager.getOpenKeyTable().deleteWithBatch(batchOperation,
            getMultipartKey());
    omMetadataManager.getMultipartInfoTable().deleteWithBatch(batchOperation,
            getMultipartKey());

    String dbFileKey = OMFileRequest.addToFileTable(omMetadataManager,
            batchOperation, getOmKeyInfo());

    if (!getPartsUnusedList().isEmpty()) {
      // Add unused parts to deleted key table.
      RepeatedOmKeyInfo repeatedOmKeyInfo = omMetadataManager.getDeletedTable()
              .get(dbFileKey);
      if (repeatedOmKeyInfo == null) {
        repeatedOmKeyInfo = new RepeatedOmKeyInfo(getPartsUnusedList());
      } else {
        repeatedOmKeyInfo.addOmKeyInfo(getOmKeyInfo());
      }

      omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
              dbFileKey, repeatedOmKeyInfo);
    }
  }
}

