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
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;

/**
 * Response for CommitKey request layout version V1.
 */
@CleanupTableInfo(cleanupTables = {OPEN_FILE_TABLE, FILE_TABLE})
public class OMKeyCommitResponseV1 extends OMKeyCommitResponse {

  public OMKeyCommitResponseV1(@Nonnull OMResponse omResponse,
                               @Nonnull OmKeyInfo omKeyInfo,
                               String ozoneKeyName, String openKeyName,
                               @Nonnull OmVolumeArgs omVolumeArgs,
                               @Nonnull OmBucketInfo omBucketInfo) {
    super(omResponse, omKeyInfo, ozoneKeyName, openKeyName, omVolumeArgs,
            omBucketInfo);
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMKeyCommitResponseV1(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
                           BatchOperation batchOperation) throws IOException {

    // Delete from OpenKey table
    omMetadataManager.getOpenKeyTable().deleteWithBatch(batchOperation,
            getOpenKeyName());

    OMFileRequest.addToFileTable(omMetadataManager, batchOperation,
            getOmKeyInfo());

    // update volume usedBytes.
    omMetadataManager.getVolumeTable().putWithBatch(batchOperation,
            omMetadataManager.getVolumeKey(getOmVolumeArgs().getVolume()),
            getOmVolumeArgs());
    // update bucket usedBytes.
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
            omMetadataManager.getBucketKey(getOmVolumeArgs().getVolume(),
                    getOmBucketInfo().getBucketName()), getOmBucketInfo());
  }
}
