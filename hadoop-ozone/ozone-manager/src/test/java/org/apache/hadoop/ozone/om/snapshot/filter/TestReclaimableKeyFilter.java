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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.rocksdb.RocksDBException;

/**
 * Test class for verifying the behavior of the ReclaimableKeyFilter.
 *
 * <p>The ReclaimableKeyFilter is responsible for determining if a deleted key entry
 * can be reclaimed based on its presence in previous snapshots. In essence, if a key
 * does not appear in a previous snapshot—or if the snapshot chain's state indicates that
 * the key is no longer valid—the key is considered reclaimable. These tests simulate a
 * variety of snapshot chain scenarios to validate that:
 * <ul>
 *   <li>Keys are not reclaimed if they exist in prior snapshots.</li>
 *   <li>Keys are reclaimed if they are removed from the previous snapshot.</li>
 *   <li>Exclusive size metrics, including data and replicated sizes, are correctly updated
 *       for snapshots where keys are not reclaimed.</li>
 * </ul>
 *
 * <p>This class extends {@link AbstractReclaimableFilterTest} to leverage common setup, snapshot
 * chain preparation, and utility methods.
 */
public class TestReclaimableKeyFilter extends AbstractReclaimableFilterTest {
  @Override
  protected ReclaimableFilter initializeFilter(OzoneManager om, OmSnapshotManager snapshotManager,
                                               SnapshotChainManager chainManager, SnapshotInfo currentSnapshotInfo,
                                               KeyManager km, IOzoneManagerLock lock,
                                               int numberOfPreviousSnapshotsFromChain) {
    return new ReclaimableKeyFilter(om, snapshotManager, chainManager, currentSnapshotInfo, km, lock);
  }

  List<Arguments> testReclaimableFilterArguments() {
    List<Arguments> arguments = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 5; j++) {
        arguments.add(Arguments.of(i, j));
      }
    }
    return arguments;
  }

  private KeyManager mockOmSnapshot(UncheckedAutoCloseableSupplier<OmSnapshot> snapshot) {
    if (snapshot != null) {
      OmSnapshot omSnapshot = snapshot.get();
      KeyManager keyManager = mock(KeyManager.class);
      when(omSnapshot.getKeyManager()).thenReturn(keyManager);
      return keyManager;
    }
    return null;
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private void testReclaimableKeyFilter(String volume, String bucket, int index,
                                        OmKeyInfo keyInfo, OmKeyInfo prevKeyInfo, OmKeyInfo prevPrevKeyInfo,
                                        Boolean expectedValue,
                                        Optional<AtomicLong> size, Optional<AtomicLong> replicatedSize)
      throws IOException {
    List<SnapshotInfo> snapshotInfos = getLastSnapshotInfos(volume, bucket, 2, index);
    SnapshotInfo previousToPreviousSapshotInfo = snapshotInfos.get(0);
    SnapshotInfo prevSnapshotInfo = snapshotInfos.get(1);
    OmBucketInfo bucketInfo = getOzoneManager().getBucketManager().getBucketInfo(volume, bucket);
    long volumeId = getOzoneManager().getMetadataManager().getVolumeId(volume);

    UncheckedAutoCloseableSupplier<OmSnapshot> prevSnap = Optional.ofNullable(prevSnapshotInfo)
        .map(info -> {
          try {
            return getOmSnapshotManager().getActiveSnapshot(volume, bucket, info.getName());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).orElse(null);
    UncheckedAutoCloseableSupplier<OmSnapshot> prevToPrevSnap = Optional.ofNullable(previousToPreviousSapshotInfo)
        .map(info -> {
          try {
            return getOmSnapshotManager().getActiveSnapshot(volume, bucket, info.getName());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).orElse(null);

    KeyManager keyManager = getKeyManager();
    KeyManager prevKeyManager = mockOmSnapshot(prevSnap);
    KeyManager prevToPrevKeyManager = mockOmSnapshot(prevToPrevSnap);
    if (prevKeyManager != null) {
      when(keyManager.getPreviousSnapshotOzoneKeyInfo(eq(volumeId),
          eq(bucketInfo), eq(keyInfo)))
          .thenReturn((km) -> prevKeyInfo);
    }
    if (prevKeyInfo != null && prevKeyManager != null && prevToPrevKeyManager != null) {
      when(prevKeyManager.getPreviousSnapshotOzoneKeyInfo(eq(volumeId),
          eq(bucketInfo), eq(prevKeyInfo))).thenReturn((km) -> prevPrevKeyInfo);
    }
    when(keyInfo.getVolumeName()).thenReturn(volume);
    when(keyInfo.getBucketName()).thenReturn(bucket);
    assertEquals(expectedValue, getReclaimableFilter().apply(Table.newKeyValue("key", keyInfo)));
    ReclaimableKeyFilter keyFilter = (ReclaimableKeyFilter) getReclaimableFilter();
    if (prevSnap != null) {
      assertEquals(size.map(AtomicLong::get).orElse(null),
          keyFilter.getExclusiveSizeMap().get(prevSnap.get().getSnapshotID()));
      assertEquals(replicatedSize.map(AtomicLong::get).orElse(null),
          keyFilter.getExclusiveReplicatedSizeMap().get(prevSnap.get().getSnapshotID()));
    } else {
      assertTrue(keyFilter.getExclusiveReplicatedSizeMap().isEmpty());
      assertTrue(keyFilter.getExclusiveSizeMap().isEmpty());
    }

  }

  private OmKeyInfo getMockedOmKeyInfo(long objectId, long size, long replicatedSize) {
    OmKeyInfo keyInfo = mock(OmKeyInfo.class);
    when(keyInfo.getObjectID()).thenReturn(objectId);
    when(keyInfo.getDataSize()).thenReturn(size);
    when(keyInfo.getReplicatedSize()).thenReturn(replicatedSize);
    return keyInfo;
  }

  private OmKeyInfo getMockedOmKeyInfo(long objectId) {
    return getMockedOmKeyInfo(objectId, 0, 0);
  }

  /**
   * Tests that a key present in prior snapshots is not reclaimable.
   *
   * @param actualNumberOfSnapshots the total number of snapshots in the chain.
   * @param index the snapshot chain index used for testing.
   * @throws IOException if an I/O error occurs during the test.
   * @throws RocksDBException if RocksDB encounters an error.
   */
  @ParameterizedTest
  @MethodSource("testReclaimableFilterArguments")
  public void testNonReclaimableKey(int actualNumberOfSnapshots, int index) throws IOException, RocksDBException {
    setup(2, actualNumberOfSnapshots, index, 4, 2);
    String volume = getVolumes().get(3);
    String bucket = getBuckets().get(1);
    index = Math.min(index, actualNumberOfSnapshots);
    OmKeyInfo keyInfo = getMockedOmKeyInfo(1);
    OmKeyInfo prevKeyInfo = index - 1 >= 0 ? getMockedOmKeyInfo(1) : null;
    OmKeyInfo prevPrevKeyInfo = index - 2 >= 0 ? getMockedOmKeyInfo(3) : null;
    if (prevKeyInfo != null) {
      getMockedSnapshotUtils().when(() -> SnapshotUtils.isBlockLocationInfoSame(eq(prevKeyInfo), eq(keyInfo)))
          .thenReturn(true);
    }
    if (prevPrevKeyInfo != null) {
      getMockedSnapshotUtils().when(() -> SnapshotUtils.isBlockLocationInfoSame(eq(prevPrevKeyInfo), eq(prevKeyInfo)))
          .thenReturn(true);
    }
    Optional<AtomicLong> size = Optional.ofNullable(prevKeyInfo).map(i -> new AtomicLong());
    testReclaimableKeyFilter(volume, bucket, index, keyInfo, prevKeyInfo, prevPrevKeyInfo,
        prevKeyInfo == null, size, size);
  }

  /**
   * Tests the filter when the object IDs differ between current and previous snapshot key entries.
   *
   * @param actualNumberOfSnapshots the total number of snapshots in the chain.
   * @param index the snapshot chain index used for testing.
   * @throws IOException if an I/O error occurs during the test.
   * @throws RocksDBException if RocksDB encounters an error.
   */
  @ParameterizedTest
  @MethodSource("testReclaimableFilterArguments")
  public void testReclaimableKeyWithDifferentObjId(int actualNumberOfSnapshots, int index)
      throws IOException, RocksDBException {
    setup(2, actualNumberOfSnapshots, index, 4, 2);
    String volume = getVolumes().get(3);
    String bucket = getBuckets().get(1);
    index = Math.min(index, actualNumberOfSnapshots);
    OmKeyInfo keyInfo = getMockedOmKeyInfo(1);
    OmKeyInfo prevKeyInfo = index - 1 >= 0 ? getMockedOmKeyInfo(2) : null;
    OmKeyInfo prevPrevKeyInfo = index - 2 >= 0 ? getMockedOmKeyInfo(3) : null;
    if (prevKeyInfo != null) {
      getMockedSnapshotUtils().when(() -> SnapshotUtils.isBlockLocationInfoSame(eq(prevKeyInfo), eq(keyInfo)))
          .thenReturn(true);
    }
    testReclaimableKeyFilter(volume, bucket, index, keyInfo, prevKeyInfo, prevPrevKeyInfo,
        true, Optional.empty(), Optional.empty());
  }

  /**
   * Tests the filter behavior when block location information differs between snapshots.
   *
   * @param actualNumberOfSnapshots the total number of snapshots in the chain.
   * @param index the snapshot chain index used for testing.
   * @throws IOException if an I/O error occurs during the test.
   * @throws RocksDBException if RocksDB encounters an error.
   */
  @ParameterizedTest
  @MethodSource("testReclaimableFilterArguments")
  public void testReclaimableKeyWithDifferentBlockIds(int actualNumberOfSnapshots, int index)
      throws IOException, RocksDBException {
    setup(2, actualNumberOfSnapshots, index, 4, 2);
    String volume = getVolumes().get(3);
    String bucket = getBuckets().get(1);
    index = Math.min(index, actualNumberOfSnapshots);
    OmKeyInfo keyInfo = getMockedOmKeyInfo(1);
    OmKeyInfo prevKeyInfo = index - 1 >= 0 ? getMockedOmKeyInfo(1) : null;
    OmKeyInfo prevPrevKeyInfo = index - 2 >= 0 ? getMockedOmKeyInfo(3) : null;
    if (prevKeyInfo != null) {
      getMockedSnapshotUtils().when(() -> SnapshotUtils.isBlockLocationInfoSame(eq(prevKeyInfo), eq(keyInfo)))
          .thenReturn(false);
    }
    testReclaimableKeyFilter(volume, bucket, index, keyInfo, prevKeyInfo, prevPrevKeyInfo,
        true, Optional.empty(), Optional.empty());
  }

  /**
   * Tests the exclusive size calculation for a non-reclaimable key.
   *
   * This test verifies that for a key which is not reclaimable,
   * the filter correctly updates the exclusive size and exclusive replicated size
   * stored in the filter's maps.
   *
   * @param actualNumberOfSnapshots the total number of snapshots in the chain.
   * @param index the snapshot chain index used for testing size calculations.
   * @throws IOException if an I/O error occurs during the test.
   * @throws RocksDBException if RocksDB encounters an error.
   */
  @ParameterizedTest
  @MethodSource("testReclaimableFilterArguments")
  public void testExclusiveSizeCalculationWithNonReclaimableKey(int actualNumberOfSnapshots, int index)
      throws IOException, RocksDBException {
    setup(2, actualNumberOfSnapshots, index, 4, 2);
    String volume = getVolumes().get(3);
    String bucket = getBuckets().get(1);
    index = Math.min(index, actualNumberOfSnapshots);
    OmKeyInfo keyInfo = getMockedOmKeyInfo(1, 1, 4);
    OmKeyInfo prevKeyInfo = index - 1 >= 0 ? getMockedOmKeyInfo(1, 2, 5) : null;
    OmKeyInfo prevPrevKeyInfo = index - 2 >= 0 ? getMockedOmKeyInfo(1, 3, 6) : null;
    if (prevKeyInfo != null) {
      getMockedSnapshotUtils().when(() -> SnapshotUtils.isBlockLocationInfoSame(eq(prevKeyInfo), eq(keyInfo)))
          .thenReturn(true);
    }
    if (prevPrevKeyInfo != null) {
      getMockedSnapshotUtils().when(() -> SnapshotUtils.isBlockLocationInfoSame(eq(prevPrevKeyInfo), eq(prevKeyInfo)))
          .thenReturn(true);
    }

    Optional<AtomicLong> size = Optional.ofNullable(prevKeyInfo)
        .map(i -> prevPrevKeyInfo == null ? new AtomicLong(2) : null);
    Optional<AtomicLong> replicatedSize = Optional.ofNullable(prevKeyInfo)
        .map(i -> prevPrevKeyInfo == null ? new AtomicLong(5) : null);

    testReclaimableKeyFilter(volume, bucket, index, keyInfo, prevKeyInfo, prevPrevKeyInfo,
        prevKeyInfo == null, size, replicatedSize);
    if (prevPrevKeyInfo != null) {
      getMockedSnapshotUtils().when(() -> SnapshotUtils.isBlockLocationInfoSame(eq(prevPrevKeyInfo), eq(prevKeyInfo)))
          .thenReturn(false);
    }
    if (prevKeyInfo != null) {
      size = Optional.of(size.orElse(new AtomicLong()));
      replicatedSize = Optional.of(replicatedSize.orElse(new AtomicLong()));
      size.get().addAndGet(2L);
      replicatedSize.get().addAndGet(5L);
    }
    testReclaimableKeyFilter(volume, bucket, index, keyInfo, prevKeyInfo, prevPrevKeyInfo,
        prevKeyInfo == null, size, replicatedSize);
    OmKeyInfo prevPrevKeyInfo1;
    if (prevPrevKeyInfo != null) {
      prevPrevKeyInfo1 = getMockedOmKeyInfo(2, 3, 4);
      getMockedSnapshotUtils().when(() -> SnapshotUtils.isBlockLocationInfoSame(eq(prevPrevKeyInfo1), eq(prevKeyInfo)))
          .thenReturn(true);
    } else {
      prevPrevKeyInfo1 = null;
    }

    if (prevKeyInfo != null) {
      size = Optional.of(size.orElse(new AtomicLong()));
      replicatedSize = Optional.of(replicatedSize.orElse(new AtomicLong()));
      size.get().addAndGet(2L);
      replicatedSize.get().addAndGet(5L);
    }
    testReclaimableKeyFilter(volume, bucket, index, keyInfo, prevKeyInfo, prevPrevKeyInfo1,
        prevKeyInfo == null, size, replicatedSize);
  }

  /**
   * Boundary of the flush-lag reclamation window: the purge of the last path snapshot has been APPLIED
   * (the in-memory chain is empty) but is NOT yet flushed, so the on-disk snapshotInfoTable still holds the
   * snapshot's row. Reclamation proceeding here is safe ONLY because the row can never be ACTIVE on disk:
   * SnapshotDeletingService submits a purge only after the snapshot's deletion is flushed
   * (shouldIgnoreSnapshot -> areSnapshotChangesFlushedToDB, which relies on OMSnapshotDeleteRequest stamping
   * lastTransactionInfo). A DELETED-on-disk row is not user-readable, and re-processing it after a
   * restore-from-backup re-runs moveTableKeys/purge idempotently. If a chain-removal path that bypasses that
   * gate is ever added, this assumption breaks and reclamation here would resurrect an ACTIVE snapshot with
   * physically deleted blocks.
   */
  @Test
  public void testKeyReclaimableWhenChainEmptyingPurgeUnflushedButDeleteFlushed()
      throws IOException, RocksDBException {
    setup(2, 1, 1, 1, 1);
    String volume = getVolumes().get(0);
    String bucket = getBuckets().get(0);
    SnapshotInfo purgedSnapshot = getSnapshotInfos().get(getKey(volume, bucket)).get(0);
    purgedSnapshot.setSnapshotStatus(SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED);

    // SnapshotPurge has been applied: the in-memory chain no longer has the snapshot...
    getSnapshotInfos().get(getKey(volume, bucket)).clear();

    // ReclaimableKeyFilter determines that the chain is empty without consulting snapshotInfoTable. These stubs
    // document the invariant guaranteed by the SnapshotDeletingService flush gate: after the applied purge removes
    // the snapshot from the in-memory chain, the unflushed on-disk row can only be DELETED.
    Table<String, SnapshotInfo> snapshotInfoTable = mock(Table.class);
    OMMetadataManager metadataManager = getOzoneManager().getMetadataManager();
    when(metadataManager.getSnapshotInfoTable()).thenReturn(snapshotInfoTable);
    when(snapshotInfoTable.get(eq(purgedSnapshot.getTableKey()))).thenReturn(null);
    when(snapshotInfoTable.getSkipCache(eq(purgedSnapshot.getTableKey()))).thenReturn(purgedSnapshot);

    OmKeyInfo keyInfo = getMockedOmKeyInfo(1);
    when(keyInfo.getVolumeName()).thenReturn(volume);
    when(keyInfo.getBucketName()).thenReturn(bucket);

    assertTrue(getReclaimableFilter().apply(Table.newKeyValue("deletedKey", keyInfo)),
        "with the chain empty and the unflushed purge's on-disk row at worst DELETED, the AOS deleted key "
            + "is reclaimable");
  }

  /**
   * Control for the flush-lag repro above: when the chain is empty AND durably so (the on-disk
   * snapshotInfoTable has no row either), reclamation must proceed.
   */
  @Test
  public void testKeyReclaimableWhenChainDurablyEmpty() throws IOException, RocksDBException {
    setup(2, 1, 1, 1, 1);
    String volume = getVolumes().get(0);
    String bucket = getBuckets().get(0);
    SnapshotInfo purgedSnapshot = getSnapshotInfos().get(getKey(volume, bucket)).get(0);

    getSnapshotInfos().get(getKey(volume, bucket)).clear();

    // As above, these stubs document the durable-empty invariant but do not affect the filter result.
    Table<String, SnapshotInfo> snapshotInfoTable = mock(Table.class);
    OMMetadataManager metadataManager = getOzoneManager().getMetadataManager();
    when(metadataManager.getSnapshotInfoTable()).thenReturn(snapshotInfoTable);
    when(snapshotInfoTable.get(eq(purgedSnapshot.getTableKey()))).thenReturn(null);
    when(snapshotInfoTable.getSkipCache(eq(purgedSnapshot.getTableKey()))).thenReturn(null);

    OmKeyInfo keyInfo = getMockedOmKeyInfo(1);
    when(keyInfo.getVolumeName()).thenReturn(volume);
    when(keyInfo.getBucketName()).thenReturn(bucket);

    assertTrue(getReclaimableFilter().apply(Table.newKeyValue("deletedKey", keyInfo)),
        "with no snapshot in the chain and none on disk, the AOS deleted key is reclaimable");
  }
}
