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
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import java.io.IOException;
import javax.annotation.Nonnull;

/**
 * Response for CommitKey request.
 */
public class OMKeyCommitResponse extends OMClientResponse {

  private OmKeyInfo omKeyInfo;
  private String ozoneKeyName;
  private String openKeyName;

  public OMKeyCommitResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmKeyInfo omKeyInfo, String ozoneKeyName, String openKeyName) {
    super(omResponse);
    this.omKeyInfo = omKeyInfo;
    this.ozoneKeyName = ozoneKeyName;
    this.openKeyName = openKeyName;
  }

  /**
   * When the KeyCommit request is a replay but the openKey should be deleted
   * from the OpenKey table.
   * Note that this response will result in openKey deletion only. Key will
   * not be added to Key table.
   * @param openKeyName openKey to be deleted from OpenKey table
   */
  public OMKeyCommitResponse(@Nonnull OMResponse omResponse,
      String openKeyName) {
    super(omResponse);
    this.omKeyInfo = null;
    this.openKeyName = openKeyName;
  }

  /**
   * For when the request is not successful or it is a replay transaction.
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

    // Add entry to Key table if omKeyInfo is available i.e. it is not a
    // replayed transaction.
    if (omKeyInfo != null) {
      omMetadataManager.getKeyTable().putWithBatch(batchOperation, ozoneKeyName,
          omKeyInfo);
    }
  }
}
