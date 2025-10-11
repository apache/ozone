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

package org.apache.hadoop.ozone.om.response.key;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for AllocateBlock request.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, BUCKET_TABLE})
public class OMAllocateBlockResponse extends OmKeyResponse {

  private OmKeyInfo omKeyInfo;
  private long clientID;

  public OMAllocateBlockResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmKeyInfo omKeyInfo, long clientID,
      @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    this.omKeyInfo = omKeyInfo;
    this.clientID = clientID;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMAllocateBlockResponse(@Nonnull OMResponse omResponse,
                                 @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String openKey = omMetadataManager.getOpenKey(omKeyInfo.getVolumeName(),
        omKeyInfo.getBucketName(), omKeyInfo.getKeyName(), clientID);
    omMetadataManager.getOpenKeyTable(getBucketLayout())
        .putWithBatch(batchOperation, openKey, omKeyInfo);
  }

  protected OmKeyInfo getOmKeyInfo() {
    return omKeyInfo;
  }

  protected long getClientID() {
    return clientID;
  }
}
