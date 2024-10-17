/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.response.snapshot;

import jakarta.annotation.Nonnull;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.SNAPSHOT_INFO_TABLE;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.createMergedRepeatedOmKeyInfoFromDeletedTableEntry;

/**
 * Response for OMSnapshotMoveDeletedKeysRequest.
 */
@CleanupTableInfo(cleanupTables = {SNAPSHOT_INFO_TABLE})
public class OMSnapshotMoveTableKeysResponse extends OMClientResponse {

  private SnapshotInfo fromSnapshot;
  private SnapshotInfo nextSnapshot;
  private List<SnapshotMoveKeyInfos> deletedKeys;
  private List<HddsProtos.KeyValue> renameKeysList;
  private List<SnapshotMoveKeyInfos> deletedDirs;

  public OMSnapshotMoveTableKeysResponse(OMResponse omResponse,
                                         @Nonnull SnapshotInfo fromSnapshot, SnapshotInfo nextSnapshot,
                                         List<SnapshotMoveKeyInfos> deletedKeys,
                                         List<SnapshotMoveKeyInfos> deletedDirs,
                                         List<HddsProtos.KeyValue> renamedKeys) {
    super(omResponse);
    this.fromSnapshot = fromSnapshot;
    this.nextSnapshot = nextSnapshot;
    this.deletedKeys = deletedKeys;
    this.renameKeysList = renamedKeys;
    this.deletedDirs = deletedDirs;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMSnapshotMoveTableKeysResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager, BatchOperation batchOperation) throws IOException {
    OmSnapshotManager omSnapshotManager = ((OmMetadataManagerImpl) omMetadataManager)
        .getOzoneManager().getOmSnapshotManager();

    try (ReferenceCounted<OmSnapshot> rcOmFromSnapshot =
             omSnapshotManager.getSnapshot(fromSnapshot.getSnapshotId())) {

      OmSnapshot fromOmSnapshot = rcOmFromSnapshot.get();

      if (nextSnapshot != null) {
        try (ReferenceCounted<OmSnapshot>
            rcOmNextSnapshot = omSnapshotManager.getSnapshot(nextSnapshot.getSnapshotId())) {

          OmSnapshot nextOmSnapshot = rcOmNextSnapshot.get();
          RDBStore nextSnapshotStore = (RDBStore) nextOmSnapshot.getMetadataManager().getStore();
          // Init Batch Operation for snapshot db.
          try (BatchOperation writeBatch = nextSnapshotStore.initBatchOperation()) {
            addKeysToNextSnapshot(writeBatch, nextOmSnapshot.getMetadataManager());
            nextSnapshotStore.commitBatchOperation(writeBatch);
            nextSnapshotStore.getDb().flushWal(true);
            nextSnapshotStore.getDb().flush();
          }
        }
      } else {
        // Handle the case where there is no next Snapshot.
        addKeysToNextSnapshot(batchOperation, omMetadataManager);
      }

      // Update From Snapshot Deleted Table.
      RDBStore fromSnapshotStore = (RDBStore) fromOmSnapshot.getMetadataManager().getStore();
      try (BatchOperation fromSnapshotBatchOp = fromSnapshotStore.initBatchOperation()) {
        deleteKeysFromSnapshot(fromSnapshotBatchOp, fromOmSnapshot.getMetadataManager());
        fromSnapshotStore.commitBatchOperation(fromSnapshotBatchOp);
        fromSnapshotStore.getDb().flushWal(true);
        fromSnapshotStore.getDb().flush();
      }
    }

    // Flush snapshot info to rocksDB.
    omMetadataManager.getSnapshotInfoTable().putWithBatch(batchOperation, fromSnapshot.getTableKey(), fromSnapshot);
    if (nextSnapshot != null) {
      omMetadataManager.getSnapshotInfoTable().putWithBatch(batchOperation, nextSnapshot.getTableKey(), nextSnapshot);
    }
  }

  private void deleteKeysFromSnapshot(BatchOperation batchOp, OMMetadataManager fromSnapshotMetadataManager)
      throws IOException {
    for (SnapshotMoveKeyInfos deletedOmKeyInfo : deletedKeys) {
      // Delete keys from current snapshot that are moved to next snapshot.
      fromSnapshotMetadataManager.getDeletedTable().deleteWithBatch(batchOp, deletedOmKeyInfo.getKey());
    }

    // Delete rename keys from current snapshot that are moved to next snapshot.
    for (HddsProtos.KeyValue renameEntry : renameKeysList) {
      fromSnapshotMetadataManager.getSnapshotRenamedTable().deleteWithBatch(batchOp, renameEntry.getKey());
    }

    // Delete deletedDir from current snapshot that are moved to next snapshot.
    for (SnapshotMoveKeyInfos deletedDirInfo : deletedDirs) {
      fromSnapshotMetadataManager.getDeletedDirTable().deleteWithBatch(batchOp, deletedDirInfo.getKey());
    }

  }

  private void addKeysToNextSnapshot(BatchOperation batchOp, OMMetadataManager metadataManager) throws IOException {

    // Add renamed keys to the next snapshot or active DB.
    for (HddsProtos.KeyValue renameEntry : renameKeysList) {
      metadataManager.getSnapshotRenamedTable().putWithBatch(batchOp, renameEntry.getKey(), renameEntry.getValue());
    }
    // Add deleted keys to the next snapshot or active DB.
    for (SnapshotMoveKeyInfos deletedKeyInfo : deletedKeys) {
      RepeatedOmKeyInfo omKeyInfos = createMergedRepeatedOmKeyInfoFromDeletedTableEntry(deletedKeyInfo,
          metadataManager);
      metadataManager.getDeletedTable().putWithBatch(batchOp, deletedKeyInfo.getKey(), omKeyInfos);
    }
    // Add deleted dir keys to the next snapshot or active DB.
    for (SnapshotMoveKeyInfos deletedDirInfo : deletedDirs) {
      metadataManager.getDeletedDirTable().putWithBatch(batchOp, deletedDirInfo.getKey(),
          OmKeyInfo.getFromProtobuf(deletedDirInfo.getKeyInfosList().get(0)));
    }
  }
}

