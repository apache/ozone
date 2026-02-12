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

package org.apache.hadoop.ozone.om.response.snapshot;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SNAPSHOT_INFO_TABLE;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager.WritableOmSnapshotLocalDataProvider;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Response for OMSnapshotPurgeRequest.
 */
@CleanupTableInfo(cleanupTables = {SNAPSHOT_INFO_TABLE})
public class OMSnapshotPurgeResponse extends OMClientResponse {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMSnapshotPurgeResponse.class);
  private final List<String> snapshotDbKeys;
  private final Map<String, SnapshotInfo> updatedSnapInfos;
  private final TransactionInfo transactionInfo;

  public OMSnapshotPurgeResponse(
      @Nonnull OMResponse omResponse,
      @Nonnull List<String> snapshotDbKeys,
      Map<String, SnapshotInfo> updatedSnapInfos,
      TransactionInfo transactionInfo
  ) {
    super(omResponse);
    this.snapshotDbKeys = snapshotDbKeys;
    this.updatedSnapInfos = updatedSnapInfos;
    this.transactionInfo = transactionInfo;
  }

  /**
   * Constructor for failed request.
   * It should not be used for successful request.
   */
  public OMSnapshotPurgeResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
    this.snapshotDbKeys = null;
    this.updatedSnapInfos = null;
    this.transactionInfo = null;
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    OmMetadataManagerImpl metadataManager = (OmMetadataManagerImpl)
        omMetadataManager;
    updateSnapInfo(metadataManager, batchOperation, updatedSnapInfos);
    for (String dbKey: snapshotDbKeys) {
      // Skip the cache here because snapshot is purged from cache in OMSnapshotPurgeRequest.
      SnapshotInfo snapshotInfo = omMetadataManager
          .getSnapshotInfoTable().getSkipCache(dbKey);
      // Even though snapshot existed when SnapshotDeletingService
      // was running. It might be deleted in the previous run and
      // the DB might not have been updated yet. So snapshotInfo
      // can be null.
      if (snapshotInfo == null) {
        continue;
      }
      OmSnapshotManager omSnapshotManager = metadataManager.getOzoneManager().getOmSnapshotManager();
      // Remove and close snapshot's RocksDB instance from SnapshotCache.
      omSnapshotManager.invalidateCacheEntry(snapshotInfo.getSnapshotId());
      // Remove the snapshot from snapshotId to snapshotTableKey map.
      ((OmMetadataManagerImpl) omMetadataManager).getSnapshotChainManager()
          .removeFromSnapshotIdToTable(snapshotInfo.getSnapshotId());

      OmSnapshotLocalDataManager snapshotLocalDataManager = ((OmMetadataManagerImpl) omMetadataManager)
          .getOzoneManager().getOmSnapshotManager().getSnapshotLocalDataManager();
      // Update snapshot local data to update purge transaction info. This would be used to check whether the
      // snapshot purged txn is flushed to rocksdb.
      updateLocalData(snapshotLocalDataManager, snapshotInfo);
      // Delete Snapshot checkpoint directory.

      omSnapshotManager.deleteSnapshotCheckpointDirectories(snapshotInfo.getSnapshotId(), -1);
      // Delete snapshotInfo from the table.
      omMetadataManager.getSnapshotInfoTable().deleteWithBatch(batchOperation, dbKey);
    }
  }

  private void updateSnapInfo(OmMetadataManagerImpl metadataManager,
                              BatchOperation batchOp,
                              Map<String, SnapshotInfo> snapshotInfos)
      throws IOException {
    for (Map.Entry<String, SnapshotInfo> entry : snapshotInfos.entrySet()) {
      metadataManager.getSnapshotInfoTable().putWithBatch(batchOp,
          entry.getKey(), entry.getValue());
    }
  }

  private void updateLocalData(OmSnapshotLocalDataManager localDataManager, SnapshotInfo snapshotInfo)
      throws IOException {
    try (WritableOmSnapshotLocalDataProvider snap = localDataManager.getWritableOmSnapshotLocalData(snapshotInfo)) {
      snap.setTransactionInfo(this.transactionInfo);
      snap.commit();
    }
  }

  @VisibleForTesting
  public Map<String, SnapshotInfo> getUpdatedSnapInfos() {
    return updatedSnapInfos;
  }
}
