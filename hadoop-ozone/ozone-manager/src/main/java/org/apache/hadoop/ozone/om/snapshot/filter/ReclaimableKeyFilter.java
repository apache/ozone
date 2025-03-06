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

package org.apache.hadoop.ozone.om.snapshot.filter;

import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.isBlockLocationInfoSame;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.ratis.util.MemoizedCheckedSupplier;

/**
 * Filter to return deleted keys which are reclaimable based on their presence in previous snapshot in
 * the snapshot chain.
 */
public class ReclaimableKeyFilter extends ReclaimableFilter<OmKeyInfo> {
  private final OzoneManager ozoneManager;
  private final Map<UUID, Long> exclusiveSizeMap;
  private final Map<UUID, Long> exclusiveReplicatedSizeMap;

  /**
   * @param omSnapshotManager
   * @param snapshotChainManager
   * @param currentSnapshotInfo  : If null the deleted keys in AOS needs to be processed, hence the latest snapshot
   *                             in the snapshot chain corresponding to bucket key needs to be processed.
   * @param metadataManager      : MetadataManager corresponding to snapshot or AOS.
   * @param lock                 : Lock for Active OM.
   */
  public ReclaimableKeyFilter(OzoneManager ozoneManager,
                              OmSnapshotManager omSnapshotManager, SnapshotChainManager snapshotChainManager,
                              SnapshotInfo currentSnapshotInfo, OMMetadataManager metadataManager,
                              IOzoneManagerLock lock) {
    super(ozoneManager, omSnapshotManager, snapshotChainManager, currentSnapshotInfo, metadataManager, lock, 2);
    this.ozoneManager = ozoneManager;
    this.exclusiveSizeMap = new HashMap<>();
    this.exclusiveReplicatedSizeMap = new HashMap<>();
  }

  @Override
  protected String getVolumeName(Table.KeyValue<String, OmKeyInfo> keyValue) throws IOException {
    return keyValue.getValue().getVolumeName();
  }

  @Override
  protected String getBucketName(Table.KeyValue<String, OmKeyInfo> keyValue) throws IOException {
    return keyValue.getValue().getBucketName();
  }

  @Override
  protected Boolean isReclaimable(Table.KeyValue<String, OmKeyInfo> deletedKeyInfo) throws IOException {
    ReferenceCounted<OmSnapshot> previousSnapshot = getPreviousOmSnapshot(1);
    ReferenceCounted<OmSnapshot> previousToPreviousSnapshot = getPreviousOmSnapshot(0);

    AtomicReference<Table<String, OmKeyInfo>> previousKeyTable = new AtomicReference<>();

    Table<String, String> renamedTable = getMetadataManager().getSnapshotRenamedTable();
    AtomicReference<Table<String, String>> prevRenamedTable = new AtomicReference<>();

    if (previousSnapshot != null) {
      previousKeyTable.set(previousSnapshot.get().getMetadataManager().getKeyTable(getBucketInfo().getBucketLayout()));
      prevRenamedTable.set(previousSnapshot.get().getMetadataManager().getSnapshotRenamedTable());
    }

    // Getting keyInfo from prev snapshot's keyTable/fileTable
    MemoizedCheckedSupplier<Optional<OmKeyInfo>, IOException> previousKeyInfo =
        MemoizedCheckedSupplier.valueOf(() -> getPreviousSnapshotKey(deletedKeyInfo.getValue(), getBucketInfo(),
            getVolumeId(), renamedTable, previousKeyTable.get()));
    // If file not present in previous snapshot then it won't be present in previous to previous snapshot either.
    if (!previousKeyInfo.get().isPresent()) {
      return true;
    }

    AtomicReference<Table<String, OmKeyInfo>> previousPrevKeyTable = new AtomicReference<>();
    if (previousToPreviousSnapshot != null) {
      previousPrevKeyTable.set(previousToPreviousSnapshot.get().getMetadataManager()
          .getKeyTable(getBucketInfo().getBucketLayout()));
    }
    // Getting keyInfo from prev to prev snapshot's keyTable/fileTable based on keyInfo of prev keyTable
    MemoizedCheckedSupplier<Optional<OmKeyInfo>, IOException> previousPrevKeyInfo =
        MemoizedCheckedSupplier.valueOf(() -> getPreviousSnapshotKey(previousKeyInfo.get().orElse(null),
            getBucketInfo(), getVolumeId(), prevRenamedTable.get(), previousPrevKeyTable.get()));
    SnapshotInfo prevToPrevSnapshotInfo = getPreviousSnapshotInfo(0);
    calculateExclusiveSize(prevToPrevSnapshotInfo, previousPrevKeyInfo.get().orElse(null),
        exclusiveSizeMap, exclusiveReplicatedSizeMap);
    return false;
  }


  public Map<UUID, Long> getExclusiveSizeMap() {
    return exclusiveSizeMap;
  }

  public Map<UUID, Long> getExclusiveReplicatedSizeMap() {
    return exclusiveReplicatedSizeMap;
  }

  /**
   * To calculate Exclusive Size for current snapshot, Check
   * the next snapshot deletedTable if the deleted key is
   * referenced in current snapshot and not referenced in the
   * previous snapshot then that key is exclusive to the current
   * snapshot. Here since we are only iterating through
   * deletedTable we can check the previous and previous to
   * previous snapshot to achieve the same.
   * previousSnapshot - Snapshot for which exclusive size is
   *                    getting calculating.
   * currSnapshot - Snapshot's deletedTable is used to calculate
   *                previousSnapshot snapshot's exclusive size.
   * previousToPrevSnapshot - Snapshot which is used to check
   *                 if key is exclusive to previousSnapshot.
   */
  private void calculateExclusiveSize(SnapshotInfo prevToPrevSnapKey, OmKeyInfo keyInfoPrevToPrevSnapshot,
                                      Map<UUID, Long> exclusiveSizes, Map<UUID, Long> exclusiveReplicatedSizes) {
    if (keyInfoPrevToPrevSnapshot == null) {
      return;
    }
    exclusiveSizes.compute(prevToPrevSnapKey.getSnapshotId(),
        (k, v) -> (v == null ? 0 : v) + keyInfoPrevToPrevSnapshot.getDataSize());
    exclusiveReplicatedSizes.compute(prevToPrevSnapKey.getSnapshotId(),
        (k, v) -> (v == null ? 0 : v) + keyInfoPrevToPrevSnapshot.getReplicatedSize());
  }

  private Optional<OmKeyInfo> getPreviousSnapshotKey(OmKeyInfo keyInfo, OmBucketInfo bucketInfo, long volumeId,
                                                     Table<String, String> snapRenamedTable,
                                                     Table<String, OmKeyInfo> previousKeyTable) throws IOException {

    if (keyInfo == null || previousKeyTable == null) {
      return Optional.empty();
    }
    String dbRenameKey = ozoneManager.getMetadataManager().getRenameKey(
        keyInfo.getVolumeName(),
        keyInfo.getBucketName(),
        keyInfo.getObjectID());

    String renamedKey = snapRenamedTable.getIfExist(dbRenameKey);
    OmKeyInfo prevKeyInfo;

    if (renamedKey == null) {
      String dbKeyPrevSnap;
      if (bucketInfo.getBucketLayout().isFileSystemOptimized()) {
        dbKeyPrevSnap = ozoneManager.getMetadataManager().getOzonePathKey(
            volumeId,
            bucketInfo.getObjectID(),
            keyInfo.getParentObjectID(),
            keyInfo.getFileName());
      } else {
        dbKeyPrevSnap = ozoneManager.getMetadataManager().getOzoneKey(
            keyInfo.getVolumeName(),
            keyInfo.getBucketName(),
            keyInfo.getKeyName());
      }
      prevKeyInfo = previousKeyTable.get(dbKeyPrevSnap);
    } else {
      prevKeyInfo = previousKeyTable.get(renamedKey);
    }

    if (prevKeyInfo == null || prevKeyInfo.getObjectID() != keyInfo.getObjectID()) {
      return Optional.empty();
    }
    return isBlockLocationInfoSame(prevKeyInfo, keyInfo) ? Optional.of(prevKeyInfo) : Optional.empty();
  }
}
