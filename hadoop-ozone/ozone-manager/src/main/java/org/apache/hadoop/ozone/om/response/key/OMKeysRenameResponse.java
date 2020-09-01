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
import org.apache.hadoop.ozone.om.helpers.OmRenameKeys;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

/**
 * Response for RenameKeys request.
 */
@CleanupTableInfo(cleanupTables = {KEY_TABLE})
public class OMKeysRenameResponse extends OMClientResponse {

  private OmRenameKeys omRenameKeys;

  public OMKeysRenameResponse(@Nonnull OMResponse omResponse,
                              OmRenameKeys omRenameKeys) {
    super(omResponse);
    this.omRenameKeys = omRenameKeys;
  }


  /**
   * For when the request is not successful or it is a replay transaction.
   * For a successful request, the other constructor should be used.
   */
  public OMKeysRenameResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
                           BatchOperation batchOperation) throws IOException {
    String volumeName = omRenameKeys.getVolume();
    String bucketName = omRenameKeys.getBucket();

    for (Map.Entry< String, OmKeyInfo> entry :
        omRenameKeys.getFromKeyAndToKeyInfo().entrySet()) {
      String fromKeyName = entry.getKey();
      OmKeyInfo newKeyInfo = entry.getValue();
      String toKeyName = newKeyInfo.getKeyName();

      omMetadataManager.getKeyTable().deleteWithBatch(batchOperation,
          omMetadataManager
              .getOzoneKey(volumeName, bucketName, fromKeyName));
      omMetadataManager.getKeyTable().putWithBatch(batchOperation,
          omMetadataManager.getOzoneKey(volumeName, bucketName, toKeyName),
          newKeyInfo);
    }
  }

}
