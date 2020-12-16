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

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import java.io.IOException;
import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;

/**
 * Response for CommitKey request.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, KEY_TABLE})
public class OMKeyCommitResponse extends OMClientResponse {

  private OmKeyInfo omKeyInfo;
  private String ozoneKeyName;
  private String openKeyName;
  private OmBucketInfo omBucketInfo;

  public OMKeyCommitResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmKeyInfo omKeyInfo, String ozoneKeyName, String openKeyName,
      @Nonnull OmBucketInfo omBucketInfo) {
    super(omResponse);
    this.omKeyInfo = omKeyInfo;
    this.ozoneKeyName = ozoneKeyName;
    this.openKeyName = openKeyName;
    this.omBucketInfo = omBucketInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMKeyCommitResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // Delete from OpenKey table
    omMetadataManager.getOpenKeyTable().deleteWithBatch(batchOperation,
        openKeyName);

    omMetadataManager.getKeyTable().putWithBatch(batchOperation, ozoneKeyName,
        omKeyInfo);

    // update bucket usedBytes.
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
            omBucketInfo.getBucketName()), omBucketInfo);
  }

}
