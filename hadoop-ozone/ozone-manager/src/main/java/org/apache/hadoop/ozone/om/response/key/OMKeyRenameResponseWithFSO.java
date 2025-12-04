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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SNAPSHOT_RENAMED_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for RenameKey request - prefix layout.
 */
@CleanupTableInfo(cleanupTables = {FILE_TABLE, DIRECTORY_TABLE,
    SNAPSHOT_RENAMED_TABLE})
public class OMKeyRenameResponseWithFSO extends OMKeyRenameResponse {

  private boolean isRenameDirectory;
  private OmKeyInfo fromKeyParent;
  private OmKeyInfo toKeyParent;
  private OmBucketInfo bucketInfo;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public OMKeyRenameResponseWithFSO(@Nonnull OMResponse omResponse,
      String fromDBKey, String toDBKey, OmKeyInfo fromKeyParent,
      OmKeyInfo toKeyParent, @Nonnull OmKeyInfo renameKeyInfo,
      OmBucketInfo bucketInfo,
      boolean isRenameDirectory, BucketLayout bucketLayout) {
    super(omResponse, fromDBKey, toDBKey, renameKeyInfo, bucketLayout);
    this.isRenameDirectory = isRenameDirectory;
    this.fromKeyParent = fromKeyParent;
    this.toKeyParent = toKeyParent;
    this.bucketInfo = bucketInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMKeyRenameResponseWithFSO(@Nonnull OMResponse omResponse,
                                    @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
                           BatchOperation batchOperation) throws IOException {
    long volumeId = omMetadataManager.getVolumeId(
        getRenameKeyInfo().getVolumeName());
    long bucketId = omMetadataManager.getBucketId(
        getRenameKeyInfo().getVolumeName(), getRenameKeyInfo().getBucketName());
    if (isRenameDirectory) {
      omMetadataManager.getDirectoryTable().deleteWithBatch(batchOperation,
              getFromKeyName());

      OmDirectoryInfo renameDirInfo =
              OMFileRequest.getDirectoryInfo(getRenameKeyInfo());
      omMetadataManager.getDirectoryTable().putWithBatch(batchOperation,
              getToKeyName(), renameDirInfo);

    } else {
      omMetadataManager.getKeyTable(getBucketLayout())
          .deleteWithBatch(batchOperation, getFromKeyName());
      omMetadataManager.getKeyTable(getBucketLayout())
          .putWithBatch(batchOperation, getToKeyName(), getRenameKeyInfo());
    }

    boolean isSnapshotBucket = OMClientRequestUtils.
        isSnapshotBucket(omMetadataManager, getRenameKeyInfo());
    String renameDbKey = omMetadataManager.getRenameKey(
        getRenameKeyInfo().getVolumeName(),
        getRenameKeyInfo().getBucketName(),
        getRenameKeyInfo().getObjectID());
    String renamedKey = omMetadataManager.getSnapshotRenamedTable()
        .get(renameDbKey);
    if (isSnapshotBucket && renamedKey == null) {
      omMetadataManager.getSnapshotRenamedTable().putWithBatch(
          batchOperation, renameDbKey, getFromKeyName());
    }

    if (fromKeyParent != null) {
      addDirToDBBatch(omMetadataManager, fromKeyParent,
          volumeId, bucketId, batchOperation);
    }
    if (toKeyParent != null) {
      addDirToDBBatch(omMetadataManager, toKeyParent,
          volumeId, bucketId, batchOperation);
    }
    if (bucketInfo != null) {
      String dbBucketKey =
          omMetadataManager.getBucketKey(bucketInfo.getVolumeName(),
              bucketInfo.getBucketName());
      omMetadataManager.getBucketTable().putWithBatch(batchOperation,
          dbBucketKey, bucketInfo);
    }
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  private void addDirToDBBatch(OMMetadataManager metadataManager,
      OmKeyInfo keyInfo, long volumeId, long bucketId, BatchOperation batch)
      throws IOException {
    String dbKey = metadataManager.getOzonePathKey(volumeId, bucketId,
        keyInfo.getParentObjectID(), keyInfo.getFileName());
    OmDirectoryInfo keyDirInfo =
        OMFileRequest.getDirectoryInfo(keyInfo);
    metadataManager.getDirectoryTable().putWithBatch(batch,
        dbKey, keyDirInfo);
  }
}
