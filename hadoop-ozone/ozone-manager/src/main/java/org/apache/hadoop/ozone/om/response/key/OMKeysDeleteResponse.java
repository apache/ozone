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
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.PARTIAL_DELETE;

/**
 * Response for DeleteKey request.
 */
@CleanupTableInfo(cleanupTables = KEY_TABLE)
public class OMKeysDeleteResponse extends AbstractOMKeyDeleteResponse {
  private List<OmKeyInfo> omKeyInfoList;
  private OmBucketInfo omBucketInfo;

  public OMKeysDeleteResponse(@Nonnull OMResponse omResponse,
      @Nonnull List<OmKeyInfo> keyDeleteList,
      boolean isRatisEnabled, @Nonnull OmBucketInfo omBucketInfo) {
    super(omResponse, isRatisEnabled);
    this.omKeyInfoList = keyDeleteList;
    this.omBucketInfo = omBucketInfo;
  }

  /**
   * For when the request is not successful or it is a replay transaction.
   * For a successful request, the other constructor should be used.
   */
  public OMKeysDeleteResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
  }

  @Override
  public void checkAndUpdateDB(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    if (getOMResponse().getStatus() == OK ||
        getOMResponse().getStatus() == PARTIAL_DELETE) {
      addToDBBatch(omMetadataManager, batchOperation);
    }
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
                           BatchOperation batchOperation) throws IOException {
    String volumeName = "";
    String bucketName = "";
    String keyName = "";
    Table<String, OmKeyInfo> keyTable = omMetadataManager.getKeyTable();
    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      volumeName = omKeyInfo.getVolumeName();
      bucketName = omKeyInfo.getBucketName();
      keyName = omKeyInfo.getKeyName();

      String deleteKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
          keyName);

      addDeletionToBatch(omMetadataManager, batchOperation, keyTable,
          deleteKey, omKeyInfo);
    }

    // update bucket usedBytes.
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
            omBucketInfo.getBucketName()), omBucketInfo);
  }
}