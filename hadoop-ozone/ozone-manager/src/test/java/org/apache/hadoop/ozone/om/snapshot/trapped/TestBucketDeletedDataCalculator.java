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

package org.apache.hadoop.ozone.om.snapshot.trapped;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.DeleteKeysResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.BucketManager;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.snapshot.SnapshotRequestAndResponseTests;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link BucketDeletedDataCalculator}.
 */
public class TestBucketDeletedDataCalculator extends SnapshotRequestAndResponseTests {

  private KeyManager mockActiveKeyManager(String volume, String bucket) throws IOException {
    KeyManager keyManager = mock(KeyManager.class);
    when(keyManager.getMetadataManager()).thenReturn(getOmMetadataManager());
    when(keyManager.getDeletedDirEntries(volume, bucket)).thenAnswer(invocation -> {
      Table<String, OmKeyInfo> deletedDirTable = getOmMetadataManager().getDeletedDirTable();
      String prefix = getOmMetadataManager().getTableBucketPrefix(
          deletedDirTable.getName(), volume, bucket);
      return deletedDirTable.iterator(prefix);
    });
    when(getOzoneManager().getKeyManager()).thenReturn(keyManager);
    return keyManager;
  }

  private void mockBucketManager(String volume, String bucket) throws IOException {
    BucketManager bucketManager = mock(BucketManager.class);
    String bucketDbKey = getOmMetadataManager().getBucketKey(volume, bucket);
    OmBucketInfo bucketInfo = getOmMetadataManager().getBucketTable().get(bucketDbKey);
    when(bucketManager.getBucketInfo(volume, bucket)).thenReturn(bucketInfo);
    when(getOzoneManager().getBucketManager()).thenReturn(bucketManager);
  }

  private OmKeyInfo newOmKeyInfo(String volume, String bucket, String keyName, long objectId) {
    return OMRequestTestUtils.createOmKeyInfo(
            volume, bucket, keyName,
            RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE))
        .setObjectID(objectId)
        .setUpdateID(objectId)
        .build();
  }

  private void putDeletedKey(
      OMMetadataManager metadataManager, String volume, String bucket, OmKeyInfo keyInfo) throws Exception {
    long bucketId = getOmMetadataManager().getBucketId(volume, bucket);
    RepeatedOmKeyInfo repeated = new RepeatedOmKeyInfo(keyInfo, bucketId);
    String deletedDbKey = metadataManager.getOzoneKey(volume, bucket, keyInfo.getKeyName());
    try (BatchOperation batch = metadataManager.getStore().initBatchOperation()) {
      metadataManager.getDeletedTable().putWithBatch(batch, deletedDbKey, repeated);
      metadataManager.getStore().commitBatchOperation(batch);
    }
  }

  private SnapshotInfo setDeepCleanedDeletedDir(
      String volume, String bucket, String snapshotName, boolean deepCleanedDeletedDir) throws Exception {
    String snapshotTableKey = SnapshotInfo.getTableKey(volume, bucket, snapshotName);
    SnapshotInfo snapshotInfo = getOmMetadataManager().getSnapshotInfoTable().get(snapshotTableKey);
    snapshotInfo.setDeepCleanedDeletedDir(deepCleanedDeletedDir);
    getOmMetadataManager().getSnapshotInfoTable()
        .addCacheEntry(snapshotTableKey, snapshotInfo, System.currentTimeMillis());
    return snapshotInfo;
  }

  private void ensureBucketContextInSnapshot(
      OMMetadataManager snapshotMetadataManager, String volume, String bucket) throws Exception {
    OmVolumeArgs volumeArgs = getOmMetadataManager().getVolumeTable().get(getOmMetadataManager().getVolumeKey(volume));
    OmBucketInfo bucketInfo = getOmMetadataManager().getBucketTable().get(getOmMetadataManager().getBucketKey(volume, bucket));
    if (volumeArgs != null) {
      snapshotMetadataManager.getVolumeTable().put(snapshotMetadataManager.getVolumeKey(volume), volumeArgs);
    }
    if (bucketInfo != null) {
      snapshotMetadataManager.getBucketTable().put(snapshotMetadataManager.getBucketKey(volume, bucket), bucketInfo);
    }
  }

  @Test
  public void testAosDeletedKeyReportedAsPurgeable() throws Exception {
    String volume = getVolumeName();
    String bucket = getBucketName();
    mockActiveKeyManager(volume, bucket);
    mockBucketManager(volume, bucket);
    OmKeyInfo keyInfo = newOmKeyInfo(volume, bucket, "key-a", 101L);
    putDeletedKey(getOmMetadataManager(), volume, bucket, keyInfo);

    BucketDeletedDataCalculator.BucketDeletedBytesStats stats =
        new BucketDeletedDataCalculator(getOzoneManager())
            .calculate(volume, bucket);

    assertEquals(0L, stats.getSnapshotTrappedBytes());
    assertEquals(0L, stats.getSnapshotTrappedKeys());
    assertEquals(0L, stats.getSnapshotTrappedDirs());
    assertTrue(stats.getPurgeableBytes() > 0L);
    assertEquals(1L, stats.getPurgeableKeys());
    assertEquals(0L, stats.getPurgeableDirs());
  }

  @Test
  public void testSnapshotDeletedKeysSkippedUntilDeletedDirDeepClean() throws Exception {
    String volume = getVolumeName();
    String bucket = getBucketName();
    mockActiveKeyManager(volume, bucket);
    mockBucketManager(volume, bucket);

    String snapshotName = "snap-skip";
    createSnapshotCheckpoint(volume, bucket, snapshotName);
    setDeepCleanedDeletedDir(volume, bucket, snapshotName, false);

    OmKeyInfo keyInfo = newOmKeyInfo(volume, bucket, "key-in-snapshot", 201L);
    try (UncheckedAutoCloseableSupplier<OmSnapshot> snapshot =
             getOmSnapshotManager().getActiveSnapshot(volume, bucket, snapshotName)) {
      putDeletedKey(snapshot.get().getMetadataManager(), volume, bucket, keyInfo);
    }

    BucketDeletedDataCalculator.BucketDeletedBytesStats stats =
        new BucketDeletedDataCalculator(getOzoneManager()).calculate(volume, bucket);

    assertEquals(0L, stats.getSnapshotTrappedBytes());
    assertEquals(0L, stats.getPurgeableBytes());
    assertEquals(0L, stats.getSnapshotTrappedKeys());
    assertEquals(0L, stats.getPurgeableKeys());
    assertEquals(0L, stats.getSnapshotTrappedDirs());
    assertEquals(0L, stats.getPurgeableDirs());
  }

  @Test
  public void testSnapshotDeletedKeyReportedAsSnapshotTrapped() throws Exception {
    String volume = getVolumeName();
    String bucket = getBucketName();
    mockActiveKeyManager(volume, bucket);
    mockBucketManager(volume, bucket);

    String snapshotOne = "snap-1";
    String snapshotTwo = "snap-2";
    createSnapshotCheckpoint(volume, bucket, snapshotOne);
    createSnapshotCheckpoint(volume, bucket, snapshotTwo);
    setDeepCleanedDeletedDir(volume, bucket, snapshotOne, true);
    setDeepCleanedDeletedDir(volume, bucket, snapshotTwo, true);

    OmKeyInfo keyInfo = newOmKeyInfo(volume, bucket, "key-trapped", 301L);
    try (UncheckedAutoCloseableSupplier<OmSnapshot> snapshot =
             getOmSnapshotManager().getActiveSnapshot(volume, bucket, snapshotOne)) {
      OMMetadataManager snapshotMetadataManager = snapshot.get().getMetadataManager();
      ensureBucketContextInSnapshot(snapshotMetadataManager, volume, bucket);
      String keyDbKey = snapshotMetadataManager.getOzoneKey(volume, bucket, keyInfo.getKeyName());
      snapshotMetadataManager.getKeyTable(BucketLayout.LEGACY).put(keyDbKey, keyInfo);
    }
    try (UncheckedAutoCloseableSupplier<OmSnapshot> snapshot =
             getOmSnapshotManager().getActiveSnapshot(volume, bucket, snapshotTwo)) {
      OMMetadataManager snapshotMetadataManager = snapshot.get().getMetadataManager();
      ensureBucketContextInSnapshot(snapshotMetadataManager, volume, bucket);
      putDeletedKey(snapshotMetadataManager, volume, bucket, keyInfo);
    }

    BucketDeletedDataCalculator.BucketDeletedBytesStats stats =
        new BucketDeletedDataCalculator(getOzoneManager()).calculate(volume, bucket);

    assertEquals(keyInfo.getReplicatedSize(), stats.getSnapshotTrappedBytes());
    assertEquals(0L, stats.getPurgeableBytes());
    assertEquals(1L, stats.getSnapshotTrappedKeys());
    assertEquals(0L, stats.getPurgeableKeys());
    assertEquals(0L, stats.getSnapshotTrappedDirs());
    assertEquals(0L, stats.getPurgeableDirs());
  }

  @Test
  public void testAosDeletedDirTraversalReportedAsPurgeable() throws Exception {
    String volume = getVolumeName();
    String bucket = getBucketName();
    KeyManager keyManager = mockActiveKeyManager(volume, bucket);
    mockBucketManager(volume, bucket);

    Table<String, OmKeyInfo> deletedDirTable = getOmMetadataManager().getDeletedDirTable();
    String deletedDirPrefix = getOmMetadataManager().getTableBucketPrefix(
        deletedDirTable.getName(), volume, bucket);
    OmKeyInfo rootDirInfo = newOmKeyInfo(volume, bucket, "dir-root", 401L);
    try (BatchOperation batch = getOmMetadataManager().getStore().initBatchOperation()) {
      deletedDirTable.putWithBatch(batch, deletedDirPrefix + "/dir-root", rootDirInfo);
      getOmMetadataManager().getStore().commitBatchOperation(batch);
    }

    OmKeyInfo subFileInfo = newOmKeyInfo(volume, bucket, "dir-root/file-a", 402L);
    when(keyManager.getPendingDeletionSubFiles(anyLong(), anyLong(), any(OmKeyInfo.class), any(), anyInt()))
        .thenReturn(new DeleteKeysResult(Collections.singletonList(subFileInfo), true));
    when(keyManager.getPendingDeletionSubDirs(anyLong(), anyLong(), any(OmKeyInfo.class), any(), anyInt()))
        .thenReturn(new DeleteKeysResult(Collections.emptyList(), true));

    BucketDeletedDataCalculator.BucketDeletedBytesStats stats =
        new BucketDeletedDataCalculator(getOzoneManager()).calculate(volume, bucket);

    assertEquals(0L, stats.getSnapshotTrappedBytes());
    assertEquals(subFileInfo.getReplicatedSize(), stats.getPurgeableBytes());
    assertEquals(0L, stats.getSnapshotTrappedKeys());
    assertEquals(1L, stats.getPurgeableKeys());
    assertEquals(0L, stats.getSnapshotTrappedDirs());
    assertEquals(1L, stats.getPurgeableDirs());
  }
}

