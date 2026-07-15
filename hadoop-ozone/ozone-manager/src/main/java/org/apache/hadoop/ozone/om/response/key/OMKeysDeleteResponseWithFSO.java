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
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_FILE_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Response for DeleteKeys request.
 */
@CleanupTableInfo(cleanupTables = { FILE_TABLE, OPEN_FILE_TABLE, DIRECTORY_TABLE,
    DELETED_DIR_TABLE, DELETED_TABLE, BUCKET_TABLE })
public class OMKeysDeleteResponseWithFSO extends OMKeysDeleteResponse {

  private List<OmKeyInfo> dirsList;
  private long volumeId;

  public OMKeysDeleteResponseWithFSO(
      @Nonnull OzoneManagerProtocolProtos.OMResponse omResponse,
      @Nonnull List<OmKeyInfo> keyDeleteList,
      @Nonnull List<OmKeyInfo> dirDeleteList,
      @Nonnull OmBucketInfo omBucketInfo, @Nonnull long volId,
      @Nonnull Map<String, OmKeyInfo> openKeyInfoMap) {
    super(omResponse, keyDeleteList, omBucketInfo, openKeyInfoMap);
    this.dirsList = dirDeleteList;
    this.volumeId = volId;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    Table<String, OmKeyInfo> keyTable =
        omMetadataManager.getKeyTable(getBucketLayout());

    final long bucketId = getOmBucketInfo().getObjectID();
    // remove dirs from DirTable and add to DeletedDirTable
    for (OmKeyInfo omKeyInfo : dirsList) {
      String ozoneDbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
          omKeyInfo.getParentObjectID(), omKeyInfo.getFileName());
      omMetadataManager.getDirectoryTable().deleteWithBatch(batchOperation,
          ozoneDbKey);
      String ozoneDeleteKey = omMetadataManager.getOzoneDeletePathKey(
          omKeyInfo.getObjectID(), ozoneDbKey);
      omMetadataManager.getDeletedDirTable().putWithBatch(
          batchOperation, ozoneDeleteKey, omKeyInfo);
    }

    // remove keys from FileTable and add to DeletedTable
    for (OmKeyInfo omKeyInfo : getOmKeyInfoList()) {
      String ozoneDbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
          omKeyInfo.getParentObjectID(), omKeyInfo.getFileName());
      String deletedKey = omMetadataManager
          .getOzoneKey(omKeyInfo.getVolumeName(), omKeyInfo.getBucketName(),
              omKeyInfo.getKeyName());
      deletedKey = omMetadataManager.getOzoneDeletePathKey(
          omKeyInfo.getObjectID(), deletedKey);
      addDeletionToBatch(omMetadataManager, batchOperation, keyTable,
          ozoneDbKey, deletedKey, omKeyInfo, bucketId, true);
    }

    // update bucket usedBytes.
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(getOmBucketInfo().getVolumeName(),
            getOmBucketInfo().getBucketName()), getOmBucketInfo());

    if (!getOpenKeyInfoMap().isEmpty()) {
      for (Map.Entry<String, OmKeyInfo> entry : getOpenKeyInfoMap().entrySet()) {
        omMetadataManager.getOpenKeyTable(getBucketLayout()).putWithBatch(
            batchOperation, entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
