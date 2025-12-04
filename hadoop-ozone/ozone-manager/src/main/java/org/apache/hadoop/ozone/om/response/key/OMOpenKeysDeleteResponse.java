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
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Handles responses to move open keys from the open key table to the delete
 * table. Modifies the open key table and delete table databases.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, OPEN_FILE_TABLE,
    DELETED_TABLE, BUCKET_TABLE})
public class OMOpenKeysDeleteResponse extends AbstractOMKeyDeleteResponse {

  private Map<String, Pair<Long, OmKeyInfo>> keysToDelete;

  public OMOpenKeysDeleteResponse(
      @Nonnull OMResponse omResponse,
      @Nonnull Map<String, Pair<Long, OmKeyInfo>> keysToDelete,
      @Nonnull BucketLayout bucketLayout) {

    super(omResponse, bucketLayout);
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
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    Table<String, OmKeyInfo> openKeyTable =
        omMetadataManager.getOpenKeyTable(getBucketLayout());

    for (Map.Entry<String, Pair<Long, OmKeyInfo>> keyInfoPair : keysToDelete.entrySet()) {
      addDeletionToBatch(omMetadataManager, batchOperation, openKeyTable,
          keyInfoPair.getKey(), keyInfoPair.getValue().getValue(), keyInfoPair.getValue().getKey(), false);
    }
  }
}
