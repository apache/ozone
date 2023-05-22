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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.SNAPSHOT_INFO_TABLE;

/**
 * Response for OMSnapshotMoveDeletedKeysRequest.
 */
@CleanupTableInfo(cleanupTables = {SNAPSHOT_INFO_TABLE})
public class OMSnapshotMoveDeletedKeysResponse extends OMClientResponse {

  private OmSnapshot fromSnapshot;
  private OmSnapshot nextSnapshot;
  private List<SnapshotMoveKeyInfos> nextDBKeysList;
  private List<SnapshotMoveKeyInfos> reclaimKeysList;
  private List<HddsProtos.KeyValue> renamedKeysList;
  private List<String> movedDirs;

  public OMSnapshotMoveDeletedKeysResponse(OMResponse omResponse,
       @Nonnull OmSnapshot omFromSnapshot, OmSnapshot omNextSnapshot,
       List<SnapshotMoveKeyInfos> nextDBKeysList,
       List<SnapshotMoveKeyInfos> reclaimKeysList,
       List<HddsProtos.KeyValue> renamedKeysList,
       List<String> movedDirs) {
    super(omResponse);
    this.fromSnapshot = omFromSnapshot;
    this.nextSnapshot = omNextSnapshot;
    this.nextDBKeysList = nextDBKeysList;
    this.reclaimKeysList = reclaimKeysList;
    this.renamedKeysList = renamedKeysList;
    this.movedDirs = movedDirs;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMSnapshotMoveDeletedKeysResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    if (nextSnapshot != null) {
      RDBStore nextSnapshotStore =
          (RDBStore) nextSnapshot.getMetadataManager().getStore();
      // Init Batch Operation for snapshot db.
      try (BatchOperation writeBatch = nextSnapshotStore.initBatchOperation()) {
        processKeys(writeBatch, nextSnapshot.getMetadataManager(),
            nextDBKeysList, true);
        processDirs(writeBatch, nextSnapshot.getMetadataManager());
        nextSnapshotStore.commitBatchOperation(writeBatch);
        nextSnapshotStore.getDb().flushWal(true);
        nextSnapshotStore.getDb().flush();
      }
    } else {
      // Handle the case where there is no next Snapshot.
      processKeys(batchOperation, omMetadataManager, nextDBKeysList, true);
      processDirs(batchOperation, omMetadataManager);
    }

    // Update From Snapshot Deleted Table.
    RDBStore fromSnapshotStore =
        (RDBStore) fromSnapshot.getMetadataManager().getStore();
    try (BatchOperation fromSnapshotBatchOp =
             fromSnapshotStore.initBatchOperation()) {
      processKeys(fromSnapshotBatchOp, fromSnapshot.getMetadataManager(),
          reclaimKeysList, false);
      fromSnapshotStore.commitBatchOperation(fromSnapshotBatchOp);
      fromSnapshotStore.getDb().flushWal(true);
      fromSnapshotStore.getDb().flush();
    }
  }

  private void processDirs(BatchOperation batchOp,
                           OMMetadataManager omMetadataManager)
      throws IOException {
    for (String movedDirsKey : movedDirs) {
      OmKeyInfo keyInfo = fromSnapshot.getMetadataManager().getDeletedDirTable()
          .get(movedDirsKey);
      // Move deleted dirs to next snapshot or active DB
      omMetadataManager.getDeletedDirTable().putWithBatch(
          batchOp, movedDirsKey, keyInfo);
      // Delete dirs from current snapshot that are moved to next snapshot.
      fromSnapshot.getMetadataManager().getDeletedDirTable()
          .deleteWithBatch(batchOp, movedDirsKey);
    }
  }

  private void processKeys(BatchOperation batchOp,
      OMMetadataManager metadataManager,
      List<SnapshotMoveKeyInfos> keyList,
      boolean isNextDB) throws IOException {

    // Move renamed keys to only the next snapshot or active DB.
    if (isNextDB) {
      for (HddsProtos.KeyValue renamedKey: renamedKeysList) {
        metadataManager.getSnapshotRenamedTable()
            .putWithBatch(batchOp, renamedKey.getKey(), renamedKey.getValue());
      }
    }

    for (SnapshotMoveKeyInfos dBKey : keyList) {
      RepeatedOmKeyInfo omKeyInfos =
          createRepeatedOmKeyInfo(dBKey.getKeyInfosList());
      if (omKeyInfos == null) {
        continue;
      }
      metadataManager.getDeletedTable().putWithBatch(batchOp,
              dBKey.getKey(), omKeyInfos);
    }
  }

  private RepeatedOmKeyInfo createRepeatedOmKeyInfo(List<KeyInfo> keyInfoList)
      throws IOException {
    RepeatedOmKeyInfo result = null;

    for (KeyInfo keyInfo: keyInfoList) {
      if (result == null) {
        result = new RepeatedOmKeyInfo(OmKeyInfo.getFromProtobuf(keyInfo));
      } else {
        result.addOmKeyInfo(OmKeyInfo.getFromProtobuf(keyInfo));
      }
    }

    return result;
  }
}

