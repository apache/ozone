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
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.DeletingServiceMetrics;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.ratis.util.MemoizedCheckedSupplier;
import org.apache.ratis.util.function.CheckedSupplier;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;

/**
 * Filter to return deleted keys which are reclaimable based on their presence in previous snapshot in
 * the snapshot chain.
 */
public class ReclaimableKeyFilter extends ReclaimableFilter<OmKeyInfo> {
  private final Map<UUID, Long> exclusiveSizeMap;
  private final Map<UUID, Long> exclusiveReplicatedSizeMap;
  private final boolean intervalOptimizationEnabled;
  private final DeletingServiceMetrics metrics;
  // Decoded once per bucket rather than once per deleted key version.
  private UUID cachedPreviousSnapshotId;
  private Long cachedPreviousSnapshotCreateIndex;

  /**
   * @param currentSnapshotInfo  : If null the deleted keys in AOS needs to be processed, hence the latest snapshot
   *                             in the snapshot chain corresponding to bucket key needs to be processed.
   * @param keyManager      : keyManager corresponding to snapshot or AOS.
   * @param lock                 : Lock for Active OM.
   */
  public ReclaimableKeyFilter(OzoneManager ozoneManager,
                              OmSnapshotManager omSnapshotManager, SnapshotChainManager snapshotChainManager,
                              SnapshotInfo currentSnapshotInfo, KeyManager keyManager,
                              IOzoneManagerLock lock) {
    super(ozoneManager, omSnapshotManager, snapshotChainManager, currentSnapshotInfo, keyManager, lock, 2);
    this.exclusiveSizeMap = new HashMap<>();
    this.exclusiveReplicatedSizeMap = new HashMap<>();
    this.intervalOptimizationEnabled =
        ozoneManager.getVersionManager().isAllowed(OMLayoutFeature.SNAPSHOT_RECLAIM_SEQ_NUM);
    this.metrics = ozoneManager.getDeletionMetrics();
  }

  @Override
  protected String getVolumeName(Table.KeyValue<String, OmKeyInfo> keyValue) throws IOException {
    return keyValue.getValue().getVolumeName();
  }

  @Override
  protected String getBucketName(Table.KeyValue<String, OmKeyInfo> keyValue) throws IOException {
    return keyValue.getValue().getBucketName();
  }

  /**
   * Determines whether a deleted key version is reclaimable, preferring its visibility interval and
   * falling back to reading the previous snapshot when the interval is absent.
   *
   * @return {@code true} if no snapshot references the version, {@code false} otherwise.
   */
  @Override
  protected Boolean isReclaimable(Table.KeyValue<String, OmKeyInfo> deletedKeyInfo) throws IOException {
    if (Boolean.TRUE.equals(isReclaimableByVisibilityInterval(deletedKeyInfo.getValue()))) {
      metrics.incrNumReclaimDecisionsFromInterval();
      return true;
    }
    metrics.incrNumReclaimDecisionsFromSnapshotLookup();
    return isReclaimableByPreviousSnapshotLookup(deletedKeyInfo);
  }

  /**
   * Determines reclaimability by looking the key up in the previous snapshot's key table or file
   * table. A key absent there is reclaimable; a key present there is not, and is additionally
   * checked against the previous-to-previous snapshot to attribute it to the previous snapshot's
   * exclusive size.
   */
  private Boolean isReclaimableByPreviousSnapshotLookup(Table.KeyValue<String, OmKeyInfo> deletedKeyInfo)
      throws IOException {
    UncheckedAutoCloseableSupplier<OmSnapshot> previousSnapshot = getPreviousOmSnapshot(1);
    KeyManager previousKeyManager = Optional.ofNullable(previousSnapshot)
        .map(i -> i.get().getKeyManager()).orElse(null);

    // Getting keyInfo from prev snapshot's keyTable/fileTable
    CheckedSupplier<Optional<OmKeyInfo>, IOException> previousKeyInfo =
        MemoizedCheckedSupplier.valueOf(() -> getPreviousSnapshotKeyInfo(getVolumeId(), getBucketInfo(),
            deletedKeyInfo.getValue(), getKeyManager(), previousKeyManager));
    // If file not present in previous snapshot then it won't be present in previous to previous snapshot either.
    if (!previousKeyInfo.get().isPresent()) {
      return true;
    }

    UncheckedAutoCloseableSupplier<OmSnapshot> previousToPreviousSnapshot = getPreviousOmSnapshot(0);
    KeyManager previousToPreviousKeyManager = Optional.ofNullable(previousToPreviousSnapshot)
        .map(i -> i.get().getKeyManager()).orElse(null);

    // Getting keyInfo from prev to prev snapshot's keyTable/fileTable based on keyInfo of prev keyTable
    CheckedSupplier<Optional<OmKeyInfo>, IOException> previousPrevKeyInfo =
        MemoizedCheckedSupplier.valueOf(() -> getPreviousSnapshotKeyInfo(
            getVolumeId(), getBucketInfo(), previousKeyInfo.get().orElse(null), previousKeyManager,
            previousToPreviousKeyManager));
    calculateExclusiveSize(getPreviousSnapshotInfo(1), previousKeyInfo, previousPrevKeyInfo,
        exclusiveSizeMap, exclusiveReplicatedSizeMap);
    return false;
  }

  /**
   * Decides reclaimability from the version's visibility interval, reading no snapshot DB.
   *
   * <p>Only the previous snapshot needs checking: {@link OmSnapshotManager#createOmSnapshotCheckpoint}
   * drains the bucket's deletedTable from the active DB when a checkpoint is taken, so every entry
   * here has {@code seqNumMax} above the previous snapshot's create index. Any older snapshot inside
   * the interval therefore implies the previous one is too. That invariant is asserted rather than
   * assumed: an entry violating it falls back instead of being reclaimed.
   *
   * @return TRUE if no snapshot can see this version, FALSE if the previous snapshot can, null if
   *         the interval is absent and the caller must fall back to a snapshot lookup.
   */
  private Boolean isReclaimableByVisibilityInterval(OmKeyInfo deletedKeyInfo) throws IOException {
    Long seqNumMin = deletedKeyInfo.getSeqNumMin();
    Long seqNumMax = deletedKeyInfo.getSeqNumMax();
    if (!intervalOptimizationEnabled || seqNumMin == null || seqNumMax == null) {
      return null;
    }
    SnapshotInfo previousSnapshotInfo = getPreviousSnapshotInfo(1);
    if (previousSnapshotInfo == null) {
      // No previous snapshot, so nothing can reference this version.
      return true;
    }
    Long previousSnapshotCreateIndex = getPreviousSnapshotCreateIndex(previousSnapshotInfo);
    if (previousSnapshotCreateIndex == null) {
      // Snapshot predates createTransactionInfo being recorded.
      return null;
    }
    if (seqNumMax <= previousSnapshotCreateIndex) {
      // The invariant above says this cannot happen. If it ever does, an older snapshot could sit
      // inside the interval without being checked, so fall back rather than reclaim on our own.
      return null;
    }
    return seqNumMin > previousSnapshotCreateIndex;
  }

  private Long getPreviousSnapshotCreateIndex(SnapshotInfo previousSnapshotInfo) throws IOException {
    if (!previousSnapshotInfo.getSnapshotId().equals(cachedPreviousSnapshotId)) {
      cachedPreviousSnapshotId = previousSnapshotInfo.getSnapshotId();
      cachedPreviousSnapshotCreateIndex = previousSnapshotInfo.getCreateTransactionInfo() == null ? null
          : TransactionInfo.fromByteString(previousSnapshotInfo.getCreateTransactionInfo()).getTransactionIndex();
    }
    return cachedPreviousSnapshotCreateIndex;
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
  private void calculateExclusiveSize(SnapshotInfo previousSnapshotInfo,
                                      CheckedSupplier<Optional<OmKeyInfo>, IOException> keyInfoPrevSnapshot,
                                      CheckedSupplier<Optional<OmKeyInfo>, IOException> keyInfoPrevToPrevSnapshot,
                                      Map<UUID, Long> exclusiveSizes, Map<UUID, Long> exclusiveReplicatedSizes)
      throws IOException {
    if (keyInfoPrevSnapshot.get().isPresent() && !keyInfoPrevToPrevSnapshot.get().isPresent()) {
      OmKeyInfo keyInfo = keyInfoPrevSnapshot.get().get();
      exclusiveSizes.compute(previousSnapshotInfo.getSnapshotId(),
          (k, v) -> (v == null ? 0 : v) + keyInfo.getDataSize());
      exclusiveReplicatedSizes.compute(previousSnapshotInfo.getSnapshotId(),
          (k, v) -> (v == null ? 0 : v) + keyInfo.getReplicatedSize());
    }
  }

  private Optional<OmKeyInfo> getPreviousSnapshotKeyInfo(long volumeId, OmBucketInfo bucketInfo,
                                                         OmKeyInfo keyInfo, KeyManager keyManager,
                                                         KeyManager previousKeyManager) throws IOException {
    if (keyInfo == null || previousKeyManager == null) {
      return Optional.empty();
    }
    OmKeyInfo prevKeyInfo = keyManager.getPreviousSnapshotOzoneKeyInfo(volumeId, bucketInfo, keyInfo)
        .apply(previousKeyManager);

    // Check if objectIds are matching then the keys are the same.
    if (prevKeyInfo == null || prevKeyInfo.getObjectID() != keyInfo.getObjectID()) {
      return Optional.empty();
    }
    return isBlockLocationInfoSame(prevKeyInfo, keyInfo) ? Optional.of(prevKeyInfo) : Optional.empty();
  }
}
