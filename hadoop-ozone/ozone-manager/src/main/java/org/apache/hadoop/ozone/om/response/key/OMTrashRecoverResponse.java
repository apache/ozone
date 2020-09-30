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

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import java.io.IOException;
import javax.annotation.Nullable;
import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

/**
 * Response for RecoverTrash request.
 */
@CleanupTableInfo(cleanupTables = {KEY_TABLE})
public class OMTrashRecoverResponse extends OMClientResponse {

  private OmKeyInfo omKeyInfo;

  public OMTrashRecoverResponse(@Nullable OmKeyInfo omKeyInfo,
      @Nonnull OMResponse omResponse) {
    super(omResponse);
    this.omKeyInfo = omKeyInfo;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

      /* TODO: HDDS-2425. HDDS-2426. */
    String trashKey = omMetadataManager
        .getOzoneKey(omKeyInfo.getVolumeName(),
            omKeyInfo.getBucketName(), omKeyInfo.getKeyName());
    RepeatedOmKeyInfo repeatedOmKeyInfo = omMetadataManager
        .getDeletedTable().get(trashKey);
    omKeyInfo = OmUtils.prepareKeyForRecover(omKeyInfo, repeatedOmKeyInfo);
    omMetadataManager.getDeletedTable()
        .deleteWithBatch(batchOperation, omKeyInfo.getKeyName());
    /* TODO: trashKey should be updated to destinationBucket. */
    omMetadataManager.getKeyTable()
        .putWithBatch(batchOperation, trashKey, omKeyInfo);
  }

}
