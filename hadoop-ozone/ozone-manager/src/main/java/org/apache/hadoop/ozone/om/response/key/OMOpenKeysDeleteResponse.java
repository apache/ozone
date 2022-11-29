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
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.DeleteTablePrefix;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;

/**
 * Handles responses to move open keys from the open key table to the delete
 * table. Modifies the open key table and delete table databases.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, DELETED_TABLE, BUCKET_TABLE})
public class OMOpenKeysDeleteResponse extends AbstractOMKeyDeleteResponse {

  private Map<String, OmKeyInfo> keysToDelete;

  public OMOpenKeysDeleteResponse(
          @Nonnull OMResponse omResponse,
          @Nonnull DeleteTablePrefix prefix,
          @Nonnull Map<String, OmKeyInfo> keysToDelete,
          @Nonnull BucketLayout bucketLayout) {

    super(omResponse, prefix, new ArrayList<>(), bucketLayout);
    this.keysToDelete = keysToDelete;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMOpenKeysDeleteResponse(
      @Nonnull OMResponse omResponse,
      @Nonnull BucketLayout bucketLayout) {

    super(omResponse, bucketLayout);
  }

  @Override
  protected String getKeyToDelete(OMMetadataManager omMetadataManager,
      OmKeyInfo omKeyInfo) {
    // This method will never be used, because addToDBBatch()
    // is already implemented.
    throw new IllegalStateException("BUG: key to delete cannot be retrieved");
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    Table<String, OmKeyInfo> openKeyTable =
        omMetadataManager.getOpenKeyTable(getBucketLayout());

    for (Map.Entry<String, OmKeyInfo> entry : keysToDelete.entrySet()) {
      openKeyTable.deleteWithBatch(batchOperation, entry.getKey());
      if (!isKeyEmpty(entry.getValue())) {
        addToOmKeyInfoList(entry.getValue());
      }
    }
    if (!getOmKeyInfoList().isEmpty()) {
      insertToDeleteTable(omMetadataManager, batchOperation);
    }
  }
}
