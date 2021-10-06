/**
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

package org.apache.hadoop.ozone.om.response.key;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;

/**
 * Response for AllocateBlock request - prefix layout.
 */
@CleanupTableInfo(cleanupTables = {OPEN_FILE_TABLE})
public class OMAllocateBlockResponseWithFSO extends OMAllocateBlockResponse {

  public OMAllocateBlockResponseWithFSO(@Nonnull OMResponse omResponse,
      @Nonnull OmKeyInfo omKeyInfo, long clientID,
      @Nonnull OmBucketInfo omBucketInfo) {
    super(omResponse, omKeyInfo, clientID, omBucketInfo);
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMAllocateBlockResponseWithFSO(@Nonnull OMResponse omResponse) {
    super(omResponse);
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    OMFileRequest.addToOpenFileTable(omMetadataManager, batchOperation,
            getOmKeyInfo(), getClientID());

    // update bucket usedBytes.
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
            omMetadataManager.getBucketKey(getOmKeyInfo().getVolumeName(),
                    getOmKeyInfo().getBucketName()), getOmBucketInfo());
  }
}

