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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.rocksdb.RocksDBException;

/**
 * Test class for ReclaimableRenameEntryFilter.
 */
public class TestReclaimableRenameEntryFilter extends AbstractReclaimableFilterTest {
  @Override
  protected ReclaimableFilter initializeFilter(OzoneManager om, OmSnapshotManager snapshotManager,
                                               SnapshotChainManager chainManager, SnapshotInfo currentSnapshotInfo,
                                               KeyManager km, IOzoneManagerLock lock,
                                               int numberOfPreviousSnapshotsFromChain) {
    return new ReclaimableRenameEntryFilter(om, snapshotManager, chainManager, currentSnapshotInfo, km, lock);
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

  private void testReclaimableRenameEntryFilter(String volume, String bucket, int index,
                                                String value,
                                                Table<String, OmKeyInfo> keyTable,
                                                Table<String, OmDirectoryInfo> dirTable,
                                                Boolean expectedValue)
      throws IOException {
    List<SnapshotInfo> snapshotInfos = getLastSnapshotInfos(volume, bucket, 1, index);
    SnapshotInfo prevSnapshotInfo = snapshotInfos.get(0);
    OmBucketInfo bucketInfo = getOzoneManager().getBucketManager().getBucketInfo(volume, bucket);
    if (prevSnapshotInfo != null) {
      UncheckedAutoCloseableSupplier<OmSnapshot> prevSnap = Optional.ofNullable(prevSnapshotInfo)
          .map(info -> {
            try {
              return getOmSnapshotManager().getActiveSnapshot(volume, bucket, info.getName());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }).orElse(null);
      mockOmSnapshot(prevSnap, bucketInfo, keyTable, dirTable);
    }
    String key = bucketInfo.getVolumeName() + "/" + bucketInfo.getBucketName() + "/" + 1;
    String[] keySplit = key.split("/");
    KeyManager km = getKeyManager();
    OMMetadataManager omMetadataManager = mock(OMMetadataManager.class);
    when(km.getMetadataManager()).thenReturn(omMetadataManager);
    when(omMetadataManager.splitRenameKey(eq(key))).thenReturn(keySplit);
    assertEquals(expectedValue, getReclaimableFilter().apply(Table.newKeyValue(key, value)));
  }

  private <T> Table<String, T> getMockedTable(Map<String, T> map) throws IOException {
    Table<String, T> table = mock(Table.class);
    when(table.get(anyString())).thenAnswer(i -> map.get(i.getArgument(0)));
    when(table.getIfExist(anyString())).thenAnswer(i -> map.get(i.getArgument(0)));
    return table;
  }

  private <T> Table<String, T> getFailingMockedTable() throws IOException {
    Table<String, T> table = mock(Table.class);
    when(table.get(anyString())).thenThrow(new RocksDatabaseException());
    when(table.getIfExist(anyString())).thenThrow(new RocksDatabaseException());
    return table;
  }

  private void mockOmSnapshot(UncheckedAutoCloseableSupplier<OmSnapshot> snapshot,
                              OmBucketInfo bucketInfo, Table<String, OmKeyInfo> keyTable,
                              Table<String, OmDirectoryInfo> dirTable) {
    if (snapshot != null) {
      OmSnapshot omSnapshot = snapshot.get();
      OMMetadataManager omMetadataManager = mock(OMMetadataManager.class);
      when(omSnapshot.getMetadataManager()).thenReturn(omMetadataManager);
      when(omMetadataManager.getKeyTable(eq(bucketInfo.getBucketLayout()))).thenReturn(keyTable);
      when(omMetadataManager.getDirectoryTable()).thenReturn(dirTable);
    }
  }

  @ParameterizedTest
  @MethodSource("testReclaimableFilterArguments")
  public void testNonReclaimableRenameEntryWithKeyNonFSO(int actualNumberOfSnapshots, int index)
      throws IOException, RocksDBException {
    setup(1, actualNumberOfSnapshots, index, 4, 2,
        BucketLayout.OBJECT_STORE);
    String volume = getVolumes().get(3);
    String bucket = getBuckets().get(1);
    index = Math.min(index, actualNumberOfSnapshots);
    String value = UUID.randomUUID().toString();
    Table<String, OmKeyInfo> keyTable = getMockedTable(ImmutableMap.of(value, mock(OmKeyInfo.class)));
    Table<String, OmDirectoryInfo> directoryTable = getFailingMockedTable();
    testReclaimableRenameEntryFilter(volume, bucket, index, value, keyTable, directoryTable, index == 0);
  }

  @ParameterizedTest
  @MethodSource("testReclaimableFilterArguments")
  public void testReclaimableRenameEntryWithKeyNonFSO(int actualNumberOfSnapshots, int index)
      throws IOException, RocksDBException {
    setup(1, actualNumberOfSnapshots, index, 4, 2,
        BucketLayout.OBJECT_STORE);
    String volume = getVolumes().get(3);
    String bucket = getBuckets().get(1);
    index = Math.min(index, actualNumberOfSnapshots);
    String value = UUID.randomUUID().toString();
    Table<String, OmKeyInfo> keyTable = getMockedTable(Collections.emptyMap());
    Table<String, OmDirectoryInfo> directoryTable = getFailingMockedTable();
    testReclaimableRenameEntryFilter(volume, bucket, index, value, keyTable, directoryTable, true);
  }

  @ParameterizedTest
  @MethodSource("testReclaimableFilterArguments")
  public void testReclaimableRenameEntryWithFSO(int actualNumberOfSnapshots, int index)
      throws IOException, RocksDBException {
    setup(1, actualNumberOfSnapshots, index, 4, 2,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    String volume = getVolumes().get(3);
    String bucket = getBuckets().get(1);
    index = Math.min(index, actualNumberOfSnapshots);
    String value = UUID.randomUUID().toString();
    Table<String, OmKeyInfo> keyTable = getMockedTable(Collections.emptyMap());
    Table<String, OmDirectoryInfo> directoryTable = getMockedTable(Collections.emptyMap());
    testReclaimableRenameEntryFilter(volume, bucket, index, value, keyTable, directoryTable, true);
  }

  @ParameterizedTest
  @MethodSource("testReclaimableFilterArguments")
  public void testNonReclaimableRenameEntryWithFileFSO(int actualNumberOfSnapshots, int index)
      throws IOException, RocksDBException {
    setup(1, actualNumberOfSnapshots, index, 4, 2,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    String volume = getVolumes().get(3);
    String bucket = getBuckets().get(1);
    index = Math.min(index, actualNumberOfSnapshots);
    String value = UUID.randomUUID().toString();
    Table<String, OmKeyInfo> keyTable = getMockedTable(ImmutableMap.of(value, mock(OmKeyInfo.class)));
    Table<String, OmDirectoryInfo> directoryTable = getMockedTable(Collections.emptyMap());
    testReclaimableRenameEntryFilter(volume, bucket, index, value, keyTable, directoryTable, index == 0);
  }

  @ParameterizedTest
  @MethodSource("testReclaimableFilterArguments")
  public void testNonReclaimableRenameEntryWithDirFSO(int actualNumberOfSnapshots, int index)
      throws IOException, RocksDBException {
    setup(1, actualNumberOfSnapshots, index, 4, 2,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    String volume = getVolumes().get(3);
    String bucket = getBuckets().get(1);
    index = Math.min(index, actualNumberOfSnapshots);
    String value = UUID.randomUUID().toString();
    Table<String, OmKeyInfo> keyTable = getMockedTable(Collections.emptyMap());
    Table<String, OmDirectoryInfo> directoryTable = getMockedTable(ImmutableMap.of(value, mock(OmDirectoryInfo.class)));
    testReclaimableRenameEntryFilter(volume, bucket, index, value, keyTable, directoryTable, index == 0);
  }
}
