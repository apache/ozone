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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SNAPSHOT_INFO_TABLE;
import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.SNAPSHOT_DB_CONTENT_LOCK;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.request.key.OMDirectoriesPurgeRequestWithFSO;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Response for {@link OMDirectoriesPurgeRequestWithFSO} request.
 */
@CleanupTableInfo(cleanupTables = {DELETED_TABLE, DELETED_DIR_TABLE,
    DIRECTORY_TABLE, FILE_TABLE, SNAPSHOT_INFO_TABLE})
public class OMDirectoriesPurgeResponseWithFSO extends OmKeyResponse {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMDirectoriesPurgeResponseWithFSO.class);

  private List<OzoneManagerProtocolProtos.PurgePathRequest> paths;
  private Map<Pair<String, String>, OmBucketInfo> volBucketInfoMap;
  private SnapshotInfo fromSnapshotInfo;
  private Map<String, OmKeyInfo> openKeyInfoMap;

  public OMDirectoriesPurgeResponseWithFSO(@Nonnull OMResponse omResponse,
      @Nonnull List<OzoneManagerProtocolProtos.PurgePathRequest> paths,
      @Nonnull BucketLayout bucketLayout,
      Map<Pair<String, String>, OmBucketInfo> volBucketInfoMap,
      SnapshotInfo fromSnapshotInfo, Map<String, OmKeyInfo> openKeyInfoMap) {
    super(omResponse, bucketLayout);
    this.paths = paths;
    this.volBucketInfoMap = volBucketInfoMap;
    this.fromSnapshotInfo = fromSnapshotInfo;
    this.openKeyInfoMap = openKeyInfoMap;
  }

  public OMDirectoriesPurgeResponseWithFSO(OMResponse omResponse) {
    super(omResponse);
  }

  @Override
  public void addToDBBatch(OMMetadataManager metadataManager,
      BatchOperation batchOp) throws IOException {
    if (fromSnapshotInfo != null) {
      OmSnapshotManager omSnapshotManager =
          ((OmMetadataManagerImpl) metadataManager)
              .getOzoneManager().getOmSnapshotManager();
      IOzoneManagerLock lock = metadataManager.getLock();
      UUID fromSnapshotId = fromSnapshotInfo.getSnapshotId();
      OMLockDetails lockDetails = lock.acquireReadLock(SNAPSHOT_DB_CONTENT_LOCK, fromSnapshotId.toString());
      if (!lockDetails.isLockAcquired()) {
        throw new OMException("Unable to acquire read lock on " + SNAPSHOT_DB_CONTENT_LOCK + " for snapshot: " +
            fromSnapshotId, OMException.ResultCodes.INTERNAL_ERROR);
      }
      try (UncheckedAutoCloseableSupplier<OmSnapshot>
          rcFromSnapshotInfo = omSnapshotManager.getSnapshot(fromSnapshotId)) {
        OmSnapshot fromSnapshot = rcFromSnapshotInfo.get();
        DBStore fromSnapshotStore = fromSnapshot.getMetadataManager()
            .getStore();
        // Init Batch Operation for snapshot db.
        try (BatchOperation writeBatch =
            fromSnapshotStore.initBatchOperation()) {
          processPaths(metadataManager, fromSnapshot.getMetadataManager(), batchOp, writeBatch);
          fromSnapshotStore.commitBatchOperation(writeBatch);
        }
      } finally {
        lock.releaseReadLock(SNAPSHOT_DB_CONTENT_LOCK, fromSnapshotId.toString());
      }
      metadataManager.getSnapshotInfoTable().putWithBatch(batchOp, fromSnapshotInfo.getTableKey(), fromSnapshotInfo);
    } else {
      processPaths(metadataManager, metadataManager, batchOp, batchOp);
    }

    // update bucket quota in active db
    for (OmBucketInfo omBucketInfo : volBucketInfoMap.values()) {
      metadataManager.getBucketTable().putWithBatch(batchOp,
          metadataManager.getBucketKey(omBucketInfo.getVolumeName(),
              omBucketInfo.getBucketName()), omBucketInfo);
    }
  }

  public void processPaths(
      OMMetadataManager keySpaceOmMetadataManager, OMMetadataManager deletedSpaceOmMetadataManager,
      BatchOperation keySpaceBatchOperation, BatchOperation deletedSpaceBatchOperation) throws IOException {
    for (OzoneManagerProtocolProtos.PurgePathRequest path : paths) {
      final long volumeId = path.getVolumeId();
      final long bucketId = path.getBucketId();

      final List<OzoneManagerProtocolProtos.KeyInfo> deletedSubFilesList =
          path.getDeletedSubFilesList();
      final List<OzoneManagerProtocolProtos.KeyInfo> markDeletedSubDirsList =
          path.getMarkDeletedSubDirsList();

      // Add all sub-directories to deleted directory table.
      for (OzoneManagerProtocolProtos.KeyInfo key : markDeletedSubDirsList) {
        OmKeyInfo keyInfo = OmKeyInfo.getFromProtobuf(key);
        String ozoneDbKey = keySpaceOmMetadataManager.getOzonePathKey(volumeId,
            bucketId, keyInfo.getParentObjectID(), keyInfo.getFileName());
        String ozoneDeleteKey = deletedSpaceOmMetadataManager.getOzoneDeletePathKey(
            key.getObjectID(), ozoneDbKey);
        deletedSpaceOmMetadataManager.getDeletedDirTable().putWithBatch(deletedSpaceBatchOperation,
            ozoneDeleteKey, keyInfo);

        keySpaceOmMetadataManager.getDirectoryTable().deleteWithBatch(keySpaceBatchOperation,
            ozoneDbKey);

        if (LOG.isDebugEnabled()) {
          LOG.debug("markDeletedDirList KeyName: {}, DBKey: {}",
              keyInfo.getKeyName(), ozoneDbKey);
        }
      }

      for (OzoneManagerProtocolProtos.KeyInfo key : deletedSubFilesList) {
        OmKeyInfo keyInfo = OmKeyInfo.getFromProtobuf(key)
            .withCommittedKeyDeletedFlag(true);
        String ozoneDbKey = keySpaceOmMetadataManager.getOzonePathKey(volumeId,
            bucketId, keyInfo.getParentObjectID(), keyInfo.getFileName());
        keySpaceOmMetadataManager.getKeyTable(getBucketLayout())
            .deleteWithBatch(keySpaceBatchOperation, ozoneDbKey);

        if (LOG.isDebugEnabled()) {
          LOG.info("Move keyName:{} to DeletedTable DBKey: {}",
              keyInfo.getKeyName(), ozoneDbKey);
        }

        RepeatedOmKeyInfo repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(bucketId,
            keyInfo, keyInfo.getUpdateID());

        String deletedKey = keySpaceOmMetadataManager
            .getOzoneKey(keyInfo.getVolumeName(), keyInfo.getBucketName(),
                keyInfo.getKeyName());
        deletedKey = deletedSpaceOmMetadataManager.getOzoneDeletePathKey(
            keyInfo.getObjectID(), deletedKey);

        deletedSpaceOmMetadataManager.getDeletedTable().putWithBatch(deletedSpaceBatchOperation,
            deletedKey, repeatedOmKeyInfo);
      }

      if (!openKeyInfoMap.isEmpty()) {
        for (Map.Entry<String, OmKeyInfo> entry : openKeyInfoMap.entrySet()) {
          keySpaceOmMetadataManager.getOpenKeyTable(getBucketLayout()).putWithBatch(
              keySpaceBatchOperation, entry.getKey(), entry.getValue());
        }
      }

      // Delete the visited directory from deleted directory table
      if (path.hasDeletedDir()) {
        deletedSpaceOmMetadataManager.getDeletedDirTable().deleteWithBatch(deletedSpaceBatchOperation,
            path.getDeletedDir());

        if (LOG.isDebugEnabled()) {
          LOG.info("Purge Deleted Directory DBKey: {}", path.getDeletedDir());
        }
      }
    }
  }

  @VisibleForTesting
  public Map<Pair<String, String>, OmBucketInfo> getVolBucketInfoMap() {
    return volBucketInfoMap;
  }
}
