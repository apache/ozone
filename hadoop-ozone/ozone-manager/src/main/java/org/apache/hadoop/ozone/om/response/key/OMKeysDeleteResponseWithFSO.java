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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

/**
 * Response for DeleteKeys request.
 */
@CleanupTableInfo(cleanupTables = { KEY_TABLE, DIRECTORY_TABLE,
    DELETED_DIR_TABLE, DELETED_TABLE, BUCKET_TABLE })
public class OMKeysDeleteResponseWithFSO extends OMKeysDeleteResponse {

  private List<OmKeyInfo> dirsList;

  public OMKeysDeleteResponseWithFSO(
      @NotNull OzoneManagerProtocolProtos.OMResponse omResponse,
      @NotNull List<OmKeyInfo> keyDeleteList,
      @NotNull List<OmKeyInfo> dirDeleteList, boolean isRatisEnabled,
      @NotNull OmBucketInfo omBucketInfo) {
    super(omResponse, keyDeleteList, isRatisEnabled, omBucketInfo);
    this.dirsList = dirDeleteList;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    Table<String, OmKeyInfo> keyTable =
        omMetadataManager.getKeyTable(getBucketLayout());

    // remove dirs from DirTable and add to DeletedDirTable
    for (OmKeyInfo omKeyInfo : dirsList) {
      String ozoneDbKey = omMetadataManager.getOzonePathKey(
          omKeyInfo.getParentObjectID(), omKeyInfo.getFileName());
      omMetadataManager.getDirectoryTable().deleteWithBatch(batchOperation,
          ozoneDbKey);
      omMetadataManager.getDeletedDirTable().putWithBatch(
          batchOperation, ozoneDbKey, omKeyInfo);
    }

    // remove keys from FileTable and add to DeletedTable
    for (OmKeyInfo omKeyInfo : getOmKeyInfoList()) {
      String ozoneDbKey = omMetadataManager.getOzonePathKey(
          omKeyInfo.getParentObjectID(), omKeyInfo.getFileName());
      String deletedKey = omMetadataManager
          .getOzoneKey(omKeyInfo.getVolumeName(), omKeyInfo.getBucketName(),
              omKeyInfo.getKeyName());
      addDeletionToBatch(omMetadataManager, batchOperation, keyTable,
          ozoneDbKey, deletedKey, omKeyInfo);
    }

    // update bucket usedBytes.
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(getOmBucketInfo().getVolumeName(),
            getOmBucketInfo().getBucketName()), getOmBucketInfo());
  }
}
