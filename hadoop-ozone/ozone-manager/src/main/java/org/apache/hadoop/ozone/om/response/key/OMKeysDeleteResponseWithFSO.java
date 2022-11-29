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
import org.apache.hadoop.ozone.om.DeleteTablePrefix;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;

/**
 * Response for DeleteKeys request.
 */
@CleanupTableInfo(cleanupTables = { FILE_TABLE, DIRECTORY_TABLE,
    DELETED_DIR_TABLE, DELETED_TABLE, BUCKET_TABLE })
public class OMKeysDeleteResponseWithFSO extends OMKeysDeleteResponse {

  private List<OmKeyInfo> dirsList;
  private long volumeId;

  public OMKeysDeleteResponseWithFSO(
          @NotNull OzoneManagerProtocolProtos.OMResponse omResponse,
          @NotNull DeleteTablePrefix deleteTablePrefix,
          @NotNull List<OmKeyInfo> keyDeleteList,
          @NotNull List<OmKeyInfo> dirDeleteList,
          @NotNull OmBucketInfo omBucketInfo, @Nonnull long volId) {
    super(omResponse, deleteTablePrefix, keyDeleteList, omBucketInfo);
    this.dirsList = dirDeleteList;
    this.volumeId = volId;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

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
    deleteFromKeyTable(omMetadataManager, batchOperation);
    insertToDeleteTable(omMetadataManager, batchOperation);

    // update bucket usedBytes.
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(getOmBucketInfo().getVolumeName(),
            getOmBucketInfo().getBucketName()), getOmBucketInfo());
  }

  @Override
  protected String getKeyToDelete(OMMetadataManager omMetadataManager,
         OmKeyInfo omKeyInfo1) throws IOException {
    String ozoneDBKey = omMetadataManager.getOzonePathKey(
            volumeId, getOmBucketInfo().getObjectID(),
            omKeyInfo1.getParentObjectID(), omKeyInfo1.getFileName());
    return ozoneDBKey;
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
