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

package org.apache.hadoop.ozone.om.response.key.acl;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Response for Bucket acl request for prefix layout.
 */
@CleanupTableInfo(cleanupTables = { FILE_TABLE, DIRECTORY_TABLE })
public class OMKeyAclResponseWithFSO extends OMKeyAclResponse {

  private boolean isDirectory;
  private long volumeId;
  private long bucketId;

  public OMKeyAclResponseWithFSO(
      @Nonnull OzoneManagerProtocolProtos.OMResponse omResponse,
      @Nonnull OmKeyInfo omKeyInfo, boolean isDirectory,
      @Nonnull BucketLayout bucketLayout, @Nonnull long volumeId,
      @Nonnull long bucketId) {
    super(omResponse, omKeyInfo, bucketLayout);
    this.isDirectory = isDirectory;
    this.volumeId = volumeId;
    this.bucketId = bucketId;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   *
   * @param omResponse
   */
  public OMKeyAclResponseWithFSO(
      @Nonnull OzoneManagerProtocolProtos.OMResponse omResponse,
      BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
  }

  @Override public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String ozoneDbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
        getOmKeyInfo().getParentObjectID(), getOmKeyInfo().getFileName());
    if (isDirectory) {
      OmDirectoryInfo dirInfo = OMFileRequest.getDirectoryInfo(getOmKeyInfo());
      omMetadataManager.getDirectoryTable()
          .putWithBatch(batchOperation, ozoneDbKey, dirInfo);
    } else {
      omMetadataManager.getKeyTable(getBucketLayout())
          .putWithBatch(batchOperation, ozoneDbKey, getOmKeyInfo());
    }
  }
}
