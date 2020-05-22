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
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .Status;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import java.io.IOException;
import javax.annotation.Nullable;
import javax.annotation.Nonnull;

/**
 * Response for RecoverTrash request.
 */
public class OMTrashRecoverResponse extends OMClientResponse {
  private OmKeyInfo omKeyInfo;
  private RepeatedOmKeyInfo trashRepeatedKeyInfo;

  public OMTrashRecoverResponse(
      @Nullable RepeatedOmKeyInfo trashRepeatedKeyInfo,
      @Nullable OmKeyInfo omKeyInfo,
      @Nonnull OMResponse omResponse) {
    super(omResponse);
    this.trashRepeatedKeyInfo = trashRepeatedKeyInfo;
    this.omKeyInfo = omKeyInfo;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    // For omResponse with non-OK status, we do nothing.
    if (getOMResponse().getStatus() == Status.OK) {
      String trashTableKey = omMetadataManager.getOzoneKey(
          omKeyInfo.getVolumeName(), omKeyInfo.getBucketName(),
          omKeyInfo.getKeyName());

      // Update keyTable in OMDB.
      omMetadataManager.getKeyTable()
          .putWithBatch(batchOperation, trashTableKey, omKeyInfo);

      // Update trashTable in OMDB
      omMetadataManager.getTrashTable()
          .putWithBatch(batchOperation, trashTableKey, trashRepeatedKeyInfo);

      // Update deletedTable in OMDB
      omMetadataManager.getDeletedTable()
          .putWithBatch(batchOperation, trashTableKey, trashRepeatedKeyInfo);
    }
  }

}
