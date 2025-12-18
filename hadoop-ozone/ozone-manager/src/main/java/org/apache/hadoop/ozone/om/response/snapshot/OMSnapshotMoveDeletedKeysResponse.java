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
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.createMergedRepeatedOmKeyInfoFromDeletedTableEntry;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;

/**
 * Response for OMSnapshotMoveDeletedKeysRequest.
 */
@CleanupTableInfo(cleanupTables = {SNAPSHOT_INFO_TABLE})
public class OMSnapshotMoveDeletedKeysResponse extends OMClientResponse {

  private SnapshotInfo fromSnapshot;
  private SnapshotInfo nextSnapshot;
  private List<SnapshotMoveKeyInfos> nextDBKeysList;
  private List<SnapshotMoveKeyInfos> reclaimKeysList;
  private List<HddsProtos.KeyValue> renamedKeysList;
  private List<String> movedDirs;
  private long bucketId;

  public OMSnapshotMoveDeletedKeysResponse(Builder builder) {
    super(builder.omResponse);
    this.fromSnapshot = builder.fromSnapshot;
    this.nextSnapshot = builder.nextSnapshot;
    this.nextDBKeysList = builder.nextDBKeysList;
    this.reclaimKeysList = builder.reclaimKeysList;
    this.renamedKeysList = builder.renamedKeysList;
    this.movedDirs = builder.movedDirs;
    this.bucketId = builder.bucketId;
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

    // Note: a trick to get
    // To do this properly, refactor OzoneManagerDoubleBuffer#addToBatch and
    // add OmSnapshotManager as a parameter.
    OmSnapshotManager omSnapshotManager =
        ((OmMetadataManagerImpl) omMetadataManager)
            .getOzoneManager().getOmSnapshotManager();

    try (UncheckedAutoCloseableSupplier<OmSnapshot> rcOmFromSnapshot =
             omSnapshotManager.getSnapshot(fromSnapshot.getSnapshotId())) {

      OmSnapshot fromOmSnapshot = rcOmFromSnapshot.get();

      if (nextSnapshot != null) {
        try (UncheckedAutoCloseableSupplier<OmSnapshot>
            rcOmNextSnapshot = omSnapshotManager.getSnapshot(nextSnapshot.getSnapshotId())) {

          OmSnapshot nextOmSnapshot = rcOmNextSnapshot.get();
          RDBStore nextSnapshotStore =
              (RDBStore) nextOmSnapshot.getMetadataManager().getStore();
          // Init Batch Operation for snapshot db.
          try (RDBBatchOperation writeBatch =
              nextSnapshotStore.initBatchOperation()) {
            processKeys(writeBatch, nextOmSnapshot.getMetadataManager());
            processDirs(writeBatch, nextOmSnapshot.getMetadataManager(),
                fromOmSnapshot);
            nextSnapshotStore.commitBatchOperation(writeBatch);
            nextSnapshotStore.getDb().flushWal(true);
            nextSnapshotStore.getDb().flush();
          }
        }
      } else {
        // Handle the case where there is no next Snapshot.
        processKeys(batchOperation, omMetadataManager);
        processDirs(batchOperation, omMetadataManager, fromOmSnapshot);
      }

      // Update From Snapshot Deleted Table.
      RDBStore fromSnapshotStore =
          (RDBStore) fromOmSnapshot.getMetadataManager().getStore();
      try (RDBBatchOperation fromSnapshotBatchOp =
          fromSnapshotStore.initBatchOperation()) {
        processReclaimKeys(fromSnapshotBatchOp,
            fromOmSnapshot.getMetadataManager());
        deleteDirsFromSnapshot(fromSnapshotBatchOp, fromOmSnapshot);
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

  private void deleteDirsFromSnapshot(BatchOperation batchOp,
      OmSnapshot fromOmSnapshot)
      throws IOException {
    for (String movedDirsKey : movedDirs) {
      // Delete dirs from current snapshot that are moved to next snapshot.
      fromOmSnapshot
          .getMetadataManager()
          .getDeletedDirTable()
          .deleteWithBatch(batchOp, movedDirsKey);
    }
  }

  private void processReclaimKeys(BatchOperation batchOp,
                                  OMMetadataManager metadataManager)
      throws IOException {
    for (SnapshotMoveKeyInfos dBKey : reclaimKeysList) {
      RepeatedOmKeyInfo omKeyInfos = createRepeatedOmKeyInfo(dBKey.getKeyInfosList(), bucketId);
      // omKeyInfos can be null, because everything from RepeatedOmKeyInfo
      // is moved to next snapshot which means this key can be deleted in
      // the current snapshot processed by SDS. The reclaim key here indicates
      // the key can be removed from the deleted current snapshot
      if (omKeyInfos == null) {
        metadataManager.getDeletedTable().deleteWithBatch(batchOp,
            dBKey.getKey());
        continue;
      }
      metadataManager.getDeletedTable().putWithBatch(batchOp,
          dBKey.getKey(), omKeyInfos);
    }
  }

  private void processDirs(BatchOperation batchOp,
                           OMMetadataManager omMetadataManager,
                           OmSnapshot fromOmSnapshot)
      throws IOException {
    for (String movedDirsKey : movedDirs) {
      OmKeyInfo keyInfo = fromOmSnapshot.getMetadataManager()
          .getDeletedDirTable()
          .get(movedDirsKey);
      if (keyInfo == null) {
        continue;
      }
      // Move deleted dirs to next snapshot or active DB
      omMetadataManager.getDeletedDirTable().putWithBatch(
          batchOp, movedDirsKey, keyInfo);
    }
  }

  private void processKeys(BatchOperation batchOp,
      OMMetadataManager metadataManager) throws IOException {

    // Move renamed keys to only the next snapshot or active DB.
    for (HddsProtos.KeyValue renamedKey: renamedKeysList) {
      metadataManager.getSnapshotRenamedTable()
          .putWithBatch(batchOp, renamedKey.getKey(), renamedKey.getValue());
    }

    for (SnapshotMoveKeyInfos dBKey : nextDBKeysList) {
      RepeatedOmKeyInfo omKeyInfos = createMergedRepeatedOmKeyInfoFromDeletedTableEntry(dBKey, bucketId,
          metadataManager);
      if (omKeyInfos == null) {
        continue;
      }
      metadataManager.getDeletedTable().putWithBatch(batchOp,
              dBKey.getKey(), omKeyInfos);
    }
  }

  public static RepeatedOmKeyInfo createRepeatedOmKeyInfo(
      List<KeyInfo> keyInfoList, long bucketId) throws IOException {
    RepeatedOmKeyInfo result = null;

    for (KeyInfo keyInfo: keyInfoList) {
      if (result == null) {
        result = new RepeatedOmKeyInfo(OmKeyInfo.getFromProtobuf(keyInfo), bucketId);
      } else {
        result.addOmKeyInfo(OmKeyInfo.getFromProtobuf(keyInfo));
      }
    }

    return result;
  }

  /**
   * Builder for OMSnapshotMoveDeletedKeysResponse.
   */
  public static class Builder {
    private OMResponse omResponse;
    private SnapshotInfo fromSnapshot;
    private SnapshotInfo nextSnapshot;
    private List<SnapshotMoveKeyInfos> nextDBKeysList;
    private List<SnapshotMoveKeyInfos> reclaimKeysList;
    private List<HddsProtos.KeyValue> renamedKeysList;
    private List<String> movedDirs;
    private long bucketId;

    public Builder setOmResponse(OMResponse omResponse) {
      this.omResponse = omResponse;
      return this;
    }

    public Builder setFromSnapshot(SnapshotInfo fromSnapshot) {
      this.fromSnapshot = fromSnapshot;
      return this;
    }

    public Builder setNextSnapshot(SnapshotInfo nextSnapshot) {
      this.nextSnapshot = nextSnapshot;
      return this;
    }

    public Builder setNextDBKeysList(List<SnapshotMoveKeyInfos> nextDBKeysList) {
      this.nextDBKeysList = nextDBKeysList;
      return this;
    }

    public Builder setReclaimKeysList(List<SnapshotMoveKeyInfos> reclaimKeysList) {
      this.reclaimKeysList = reclaimKeysList;
      return this;
    }

    public Builder setRenamedKeysList(List<HddsProtos.KeyValue> renamedKeysList) {
      this.renamedKeysList = renamedKeysList;
      return this;
    }

    public Builder setMovedDirs(List<String> movedDirs) {
      this.movedDirs = movedDirs;
      return this;
    }

    public Builder setBucketId(long bucketId) {
      this.bucketId = bucketId;
      return this;
    }

    public OMSnapshotMoveDeletedKeysResponse build() {
      Objects.requireNonNull(fromSnapshot, "fromSnapshot must not be null");
      return new OMSnapshotMoveDeletedKeysResponse(this);
    }
  }
}

