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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
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
 * Test class for ReclaimableDirFilter.
 */
public class TestReclaimableDirFilter extends AbstractReclaimableFilterTest {
  @Override
  protected ReclaimableFilter initializeFilter(OzoneManager om, OmSnapshotManager snapshotManager,
                                               SnapshotChainManager chainManager, SnapshotInfo currentSnapshotInfo,
                                               KeyManager km, IOzoneManagerLock lock,
                                               int numberOfPreviousSnapshotsFromChain) {
    return new ReclaimableDirFilter(om, snapshotManager, chainManager, currentSnapshotInfo, km, lock);
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

  private void testReclaimableDirFilter(String volume, String bucket, int index,
                                        OmKeyInfo dirInfo, OmDirectoryInfo prevDirInfo,
                                        Boolean expectedValue)
      throws IOException {
    List<SnapshotInfo> snapshotInfos = getLastSnapshotInfos(volume, bucket, 1, index);
    assertEquals(snapshotInfos.size(), 1);
    SnapshotInfo prevSnapshotInfo = snapshotInfos.get(0);
    OmBucketInfo bucketInfo = getOzoneManager().getBucketManager().getBucketInfo(volume, bucket);
    long volumeId = getOzoneManager().getMetadataManager().getVolumeId(volume);
    KeyManager keyManager = getKeyManager();
    if (prevSnapshotInfo != null) {
      UncheckedAutoCloseableSupplier<OmSnapshot> prevSnap = Optional.ofNullable(prevSnapshotInfo)
          .map(info -> {
            try {
              return getOmSnapshotManager().getActiveSnapshot(volume, bucket, info.getName());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }).orElse(null);
      mockOmSnapshot(prevSnap);
      when(keyManager.getPreviousSnapshotOzoneDirInfo(eq(volumeId), eq(bucketInfo), eq(dirInfo)))
          .thenReturn((km) -> prevDirInfo);
    }

    when(dirInfo.getVolumeName()).thenReturn(volume);
    when(dirInfo.getBucketName()).thenReturn(bucket);
    assertEquals(expectedValue, getReclaimableFilter().apply(Table.newKeyValue("key", dirInfo)));
  }

  private OmKeyInfo getMockedOmKeyInfo(long objectId) {
    OmKeyInfo keyInfo = mock(OmKeyInfo.class);
    when(keyInfo.getObjectID()).thenReturn(objectId);
    return keyInfo;
  }

  private OmDirectoryInfo getMockedOmDirInfo(long objectId) {
    OmDirectoryInfo keyInfo = mock(OmDirectoryInfo.class);
    when(keyInfo.getObjectID()).thenReturn(objectId);
    return keyInfo;
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

  @ParameterizedTest
  @MethodSource("testReclaimableFilterArguments")
  public void testNonReclaimableDirectory(int actualNumberOfSnapshots, int index) throws IOException, RocksDBException {
    setup(1, actualNumberOfSnapshots, index, 4, 2);
    String volume = getVolumes().get(3);
    String bucket = getBuckets().get(1);
    index = Math.min(index, actualNumberOfSnapshots);
    OmKeyInfo dirInfo = getMockedOmKeyInfo(1);
    OmDirectoryInfo prevDirectoryInfo = index - 1 >= 0 ? getMockedOmDirInfo(1) : null;
    testReclaimableDirFilter(volume, bucket, index, dirInfo, prevDirectoryInfo, prevDirectoryInfo == null);
  }

  @ParameterizedTest
  @MethodSource("testReclaimableFilterArguments")
  public void testReclaimableDirectoryWithDifferentObjId(int actualNumberOfSnapshots, int index)
      throws IOException, RocksDBException {
    setup(1, actualNumberOfSnapshots, index, 4, 2);
    String volume = getVolumes().get(3);
    String bucket = getBuckets().get(1);
    index = Math.min(index, actualNumberOfSnapshots);
    OmKeyInfo dirInfo = getMockedOmKeyInfo(1);
    OmDirectoryInfo prevDirectoryInfo = index - 1 >= 0 ? getMockedOmDirInfo(2) : null;
    testReclaimableDirFilter(volume, bucket, index, dirInfo, prevDirectoryInfo, true);
  }
}
