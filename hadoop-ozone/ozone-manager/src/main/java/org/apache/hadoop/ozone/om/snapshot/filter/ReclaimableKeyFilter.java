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
package org.apache.hadoop.ozone.om.snapshot.filter;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.OzoneConsts.OBJECT_ID_RECLAIM_BLOCKS;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.isBlockLocationInfoSame;

/**
 * Filter to return deleted keys which are reclaimable based on their presence in previous snapshot in
 * the snapshot chain.
 */
public class ReclaimableKeyFilter extends ReclaimableFilter<OmKeyInfo> {
  private final OzoneManager ozoneManager;
  private final Map<String, Long> exclusiveSizeMap;
  private final Map<String, Long> exclusiveReplicatedSizeMap;

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

    Table<String, OmKeyInfo> previousKeyTable = null;
    Table<String, OmKeyInfo> previousPrevKeyTable = null;

    Table<String, String> renamedTable = getMetadataManager().getSnapshotRenamedTable();
    Table<String, String> prevRenamedTable = null;

    SnapshotInfo previousSnapshotInfo = getPreviousSnapshotInfo(1);
    SnapshotInfo prevPrevSnapshotInfo = getPreviousSnapshotInfo(0);

    if (previousSnapshot != null) {
      previousKeyTable = previousSnapshot.get().getMetadataManager().getKeyTable(getBucketInfo().getBucketLayout());
      prevRenamedTable = previousSnapshot.get().getMetadataManager().getSnapshotRenamedTable();
    }
    if (previousToPreviousSnapshot != null) {
      previousPrevKeyTable = previousToPreviousSnapshot.get().getMetadataManager()
          .getKeyTable(getBucketInfo().getBucketLayout());
    }
    if (isKeyReclaimable(previousKeyTable, renamedTable, deletedKeyInfo.getValue(),
        getBucketInfo(), getVolumeId(),
        null)) {
      return true;
    }
    calculateExclusiveSize(previousSnapshotInfo, prevPrevSnapshotInfo, deletedKeyInfo.getValue(), getBucketInfo(),
        getVolumeId(), renamedTable, previousKeyTable, prevRenamedTable, previousPrevKeyTable, exclusiveSizeMap,
        exclusiveReplicatedSizeMap);
    return false;
  }


  public Map<String, Long> getExclusiveSizeMap() {
    return exclusiveSizeMap;
  }

  public Map<String, Long> getExclusiveReplicatedSizeMap() {
    return exclusiveReplicatedSizeMap;
  }

  private boolean isKeyReclaimable(
      Table<String, OmKeyInfo> previousKeyTable,
      Table<String, String> renamedTable,
      OmKeyInfo deletedKeyInfo, OmBucketInfo bucketInfo,
      long volumeId, HddsProtos.KeyValue.Builder renamedKeyBuilder)
      throws IOException {

    String dbKey;
    // Handle case when the deleted snapshot is the first snapshot.
    if (previousKeyTable == null) {
      return true;
    }

    // These are uncommitted blocks wrapped into a pseudo KeyInfo
    if (deletedKeyInfo.getObjectID() == OBJECT_ID_RECLAIM_BLOCKS) {
      return true;
    }

    // Construct keyTable or fileTable DB key depending on the bucket type
    if (bucketInfo.getBucketLayout().isFileSystemOptimized()) {
      dbKey = ozoneManager.getMetadataManager().getOzonePathKey(
          volumeId,
          bucketInfo.getObjectID(),
          deletedKeyInfo.getParentObjectID(),
          deletedKeyInfo.getFileName());
    } else {
      dbKey = ozoneManager.getMetadataManager().getOzoneKey(
          deletedKeyInfo.getVolumeName(),
          deletedKeyInfo.getBucketName(),
          deletedKeyInfo.getKeyName());
    }

    /*
     snapshotRenamedTable:
     1) /volumeName/bucketName/objectID ->
                 /volumeId/bucketId/parentId/fileName (FSO)
     2) /volumeName/bucketName/objectID ->
                /volumeName/bucketName/keyName (non-FSO)
    */
    String dbRenameKey = ozoneManager.getMetadataManager().getRenameKey(
        deletedKeyInfo.getVolumeName(), deletedKeyInfo.getBucketName(),
        deletedKeyInfo.getObjectID());

    // Condition: key should not exist in snapshotRenamedTable
    // of the current snapshot and keyTable of the previous snapshot.
    // Check key exists in renamedTable of the Snapshot
    String renamedKey = renamedTable.getIfExist(dbRenameKey);

    if (renamedKey != null && renamedKeyBuilder != null) {
      renamedKeyBuilder.setKey(dbRenameKey).setValue(renamedKey);
    }
    // previousKeyTable is fileTable if the bucket is FSO,
    // otherwise it is the keyTable.
    OmKeyInfo prevKeyInfo = renamedKey != null ? previousKeyTable
        .get(renamedKey) : previousKeyTable.get(dbKey);

    if (prevKeyInfo == null ||
        prevKeyInfo.getObjectID() != deletedKeyInfo.getObjectID()) {
      return true;
    }

    // For key overwrite the objectID will remain the same, In this
    // case we need to check if OmKeyLocationInfo is also same.
    return !isBlockLocationInfoSame(prevKeyInfo, deletedKeyInfo);
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
  @SuppressWarnings("checkstyle:ParameterNumber")
  public void calculateExclusiveSize(
      SnapshotInfo previousSnapshot,
      SnapshotInfo previousToPrevSnapshot,
      OmKeyInfo keyInfo,
      OmBucketInfo bucketInfo, long volumeId,
      Table<String, String> snapRenamedTable,
      Table<String, OmKeyInfo> previousKeyTable,
      Table<String, String> prevRenamedTable,
      Table<String, OmKeyInfo> previousToPrevKeyTable,
      Map<String, Long> exclusiveSizes,
      Map<String, Long> exclusiveReplicatedSizes) throws IOException {
    String prevSnapKey = previousSnapshot.getTableKey();
    long exclusiveReplicatedSize = exclusiveReplicatedSizes.getOrDefault(
            prevSnapKey, 0L) + keyInfo.getReplicatedSize();
    long exclusiveSize = exclusiveSizes.getOrDefault(prevSnapKey, 0L) + keyInfo.getDataSize();

    // If there is no previous to previous snapshot, then
    // the previous snapshot is the first snapshot.
    if (previousToPrevSnapshot == null) {
      exclusiveSizes.put(prevSnapKey, exclusiveSize);
      exclusiveReplicatedSizes.put(prevSnapKey,
          exclusiveReplicatedSize);
    } else {
      OmKeyInfo keyInfoPrevSnapshot = getPreviousSnapshotKeyName(
          keyInfo, bucketInfo, volumeId,
          snapRenamedTable, previousKeyTable);
      OmKeyInfo keyInfoPrevToPrevSnapshot = getPreviousSnapshotKeyName(
          keyInfoPrevSnapshot, bucketInfo, volumeId,
          prevRenamedTable, previousToPrevKeyTable);
      // If the previous to previous snapshot doesn't
      // have the key, then it is exclusive size for the
      // previous snapshot.
      if (keyInfoPrevToPrevSnapshot == null) {
        exclusiveSizes.put(prevSnapKey, exclusiveSize);
        exclusiveReplicatedSizes.put(prevSnapKey,
            exclusiveReplicatedSize);
      }
    }
  }

  private OmKeyInfo getPreviousSnapshotKeyName(OmKeyInfo keyInfo, OmBucketInfo bucketInfo, long volumeId,
      Table<String, String> snapRenamedTable, Table<String, OmKeyInfo> previousKeyTable) throws IOException {

    if (keyInfo == null) {
      return null;
    }

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

    String dbRenameKey = ozoneManager.getMetadataManager().getRenameKey(
        keyInfo.getVolumeName(),
        keyInfo.getBucketName(),
        keyInfo.getObjectID());

    String renamedKey = snapRenamedTable.getIfExist(dbRenameKey);
    OmKeyInfo prevKeyInfo = renamedKey != null ? previousKeyTable.get(renamedKey) : previousKeyTable.get(dbKeyPrevSnap);

    if (prevKeyInfo == null || prevKeyInfo.getObjectID() != keyInfo.getObjectID()) {
      return null;
    }

    return isBlockLocationInfoSame(prevKeyInfo, keyInfo) ? prevKeyInfo : null;
  }
}
