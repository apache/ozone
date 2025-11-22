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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SNAPSHOT_RENAMED_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.MapBackedTableIterator;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.ratis.util.function.CheckedFunction;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * Test class for unit tests KeyManagerImpl.
 */
public class TestKeyManagerImpl {
  private static Stream<Arguments> getTableIteratorParameters() {
    return Stream.of(
        Arguments.argumentSet("Fetch first 50 entries for volume 0, bucket 0",
            5, 10, 100, 0, 0, 0, 0, 0, 50, null),
        Arguments.argumentSet("Fetch first 50 entries for any volume/bucket", 5, 10, 100, null, null, 0, 0, 0, 50,
            null),
        Arguments.argumentSet("Fetch first 30 entries for volume 1, bucket 1", 5, 10, 100, 1, 1, 0, 0, 0, 30, null),
        Arguments.argumentSet("Fetch 20 entries from offset (2,2,10) for volume 2, bucket 2", 5, 10, 100, 2, 2, 2, 2,
            10, 20, null),
        Arguments.argumentSet("Fetch 40 entries from offset (2,2,50) for volume 3, bucket 3", 5, 10, 100, 3, 3, 3, 3,
            50, 40, null),
        Arguments.argumentSet("Fetch 200 entries from the very beginning (null start offsets)", 5, 10, 100, null,
            null, null, null, null, 200, null),
        Arguments.argumentSet("Fetch 200 entries starting from bucket 3, key 50, spanning 3 buckets", 5, 10, 100,
            null, null, 0, 3, 50, 200, null),
        Arguments.argumentSet("Invalid: bucket is set but volume is null", 5, 10, 100, null, 1, 0, 0, 0, 10,
            IOException.class),
        Arguments.argumentSet("Invalid: volume is set but bucket is null", 5, 10, 100, 1, null, 0, 0, 0, 10,
            IOException.class),
        Arguments.argumentSet("Fetch 50 entries from volume 2, bucket 5, but only 31 exist", 5, 10, 100, 2, 5, 2, 5,
            70, 50, null),
        Arguments.argumentSet("Start from last volume (4), second-last bucket (8), key 80 but only 131 entries exist",
            5, 10, 100, null, null, 4, 8, 80, 200, null)
    );
  }

  @SuppressWarnings({"checkstyle:ParameterNumber"})
  private <V> List<Table.KeyValue<String, V>> mockTableIterator(
      Class<V> valueClass, Table<String, V> table, int numberOfVolumes, int numberOfBucketsPerVolume,
      int numberOfKeysPerBucket, String volumeNamePrefix, String bucketNamePrefix, String keyPrefix,
      Integer volumeNumberFilter, Integer bucketNumberFilter, Integer startVolumeNumber, Integer startBucketNumber,
      Integer startKeyNumber, CheckedFunction<Table.KeyValue<String, V>, Boolean, IOException> filter,
      int numberOfEntries) throws IOException {
    TreeMap<String, V> values = new TreeMap<>();
    List<Table.KeyValue<String, V>> keyValues = new ArrayList<>();
    String startKey = startVolumeNumber == null || startBucketNumber == null || startKeyNumber == null ? null
        : (String.format("/%s%010d/%s%010d/%s%010d", volumeNamePrefix, startVolumeNumber, bucketNamePrefix,
        startBucketNumber, keyPrefix, startKeyNumber));
    for (int i = 0; i < numberOfVolumes; i++) {
      for (int j = 0; j < numberOfBucketsPerVolume; j++) {
        for (int k = 0; k < numberOfKeysPerBucket; k++) {
          String key = String.format("/%s%010d/%s%010d/%s%010d", volumeNamePrefix, i, bucketNamePrefix, j,
              keyPrefix, k);
          V value = valueClass == String.class ? (V) key : mock(valueClass);
          values.put(key, value);

          if ((volumeNumberFilter == null || i == volumeNumberFilter) &&
              (bucketNumberFilter == null || j == bucketNumberFilter) &&
              (startKey == null || startKey.compareTo(key) <= 0)) {
            keyValues.add(Table.newKeyValue(key, value));
          }
        }
      }
    }

    when(table.iterator(anyString())).thenAnswer(i -> new MapBackedTableIterator<>(values, i.getArgument(0)));
    return keyValues.stream().filter(kv -> {
      try {
        return filter.apply(kv);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).limit(numberOfEntries).collect(Collectors.toList());
  }

  @ParameterizedTest
  @MethodSource("getTableIteratorParameters")
  @SuppressWarnings({"checkstyle:ParameterNumber"})
  public void testGetDeletedKeyEntries(int numberOfVolumes, int numberOfBucketsPerVolume, int numberOfKeysPerBucket,
                                       Integer volumeNumber, Integer bucketNumber,
                                       Integer startVolumeNumber, Integer startBucketNumber, Integer startKeyNumber,
                                       int numberOfEntries, Class<? extends Exception> expectedException)
      throws IOException {
    String volumeNamePrefix = "volume";
    String bucketNamePrefix = "bucket";
    String keyPrefix = "key";
    OzoneConfiguration configuration = new OzoneConfiguration();
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    KeyManagerImpl km = new KeyManagerImpl(null, null, metadataManager, configuration, null, null, null);
    Table<String, RepeatedOmKeyInfo> mockedDeletedTable = Mockito.mock(Table.class);
    when(mockedDeletedTable.getName()).thenReturn(DELETED_TABLE);
    when(metadataManager.getDeletedTable()).thenReturn(mockedDeletedTable);
    when(metadataManager.getTableBucketPrefix(eq(DELETED_TABLE), anyString(), anyString()))
        .thenAnswer(i -> "/" + i.getArguments()[1] + "/" + i.getArguments()[2] + "/");
    CheckedFunction<Table.KeyValue<String, RepeatedOmKeyInfo>, Boolean, IOException> filter =
        (kv) -> Long.parseLong(kv.getKey().split(keyPrefix)[1]) % 2 == 0;
    List<Table.KeyValue<String, List<OmKeyInfo>>> expectedEntries = mockTableIterator(
        RepeatedOmKeyInfo.class, mockedDeletedTable, numberOfVolumes, numberOfBucketsPerVolume, numberOfKeysPerBucket,
        volumeNamePrefix, bucketNamePrefix, keyPrefix, volumeNumber, bucketNumber, startVolumeNumber, startBucketNumber,
        startKeyNumber, filter, numberOfEntries).stream()
        .map(kv -> {
          String key = kv.getKey();
          RepeatedOmKeyInfo value = kv.getValue();
          List<OmKeyInfo> omKeyInfos = Collections.singletonList(Mockito.mock(OmKeyInfo.class));
          when(value.cloneOmKeyInfoList()).thenReturn(omKeyInfos);
          return Table.newKeyValue(key, omKeyInfos);
        }).collect(Collectors.toList());
    String volumeName = volumeNumber == null ? null : (String.format("%s%010d", volumeNamePrefix, volumeNumber));
    String bucketName = bucketNumber == null ? null : (String.format("%s%010d", bucketNamePrefix, bucketNumber));
    String startKey = startVolumeNumber == null || startBucketNumber == null || startKeyNumber == null ? null
        : (String.format("/%s%010d/%s%010d/%s%010d", volumeNamePrefix, startVolumeNumber, bucketNamePrefix,
        startBucketNumber, keyPrefix, startKeyNumber));
    if (expectedException != null) {
      assertThrows(expectedException, () -> km.getDeletedKeyEntries(volumeName, bucketName, startKey, filter,
          numberOfEntries));
    } else {
      assertEquals(expectedEntries, km.getDeletedKeyEntries(volumeName, bucketName, startKey, filter, numberOfEntries));
    }
  }

  @ParameterizedTest
  @MethodSource("getTableIteratorParameters")
  @SuppressWarnings({"checkstyle:ParameterNumber"})
  public void testGetRenameKeyEntries(int numberOfVolumes, int numberOfBucketsPerVolume, int numberOfKeysPerBucket,
                                      Integer volumeNumber, Integer bucketNumber,
                                      Integer startVolumeNumber, Integer startBucketNumber, Integer startKeyNumber,
                                      int numberOfEntries, Class<? extends Exception> expectedException)
      throws IOException {
    String volumeNamePrefix = "volume";
    String bucketNamePrefix = "bucket";
    String keyPrefix = "";
    OzoneConfiguration configuration = new OzoneConfiguration();
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    KeyManagerImpl km = new KeyManagerImpl(null, null, metadataManager, configuration, null, null, null);
    Table<String, String> mockedRenameTable = Mockito.mock(Table.class);
    when(mockedRenameTable.getName()).thenReturn(SNAPSHOT_RENAMED_TABLE);
    when(metadataManager.getSnapshotRenamedTable()).thenReturn(mockedRenameTable);
    when(metadataManager.getTableBucketPrefix(eq(SNAPSHOT_RENAMED_TABLE), anyString(), anyString()))
        .thenAnswer(i -> "/" + i.getArguments()[1] + "/" + i.getArguments()[2] + "/");
    CheckedFunction<Table.KeyValue<String, String>, Boolean, IOException> filter =
        (kv) -> Long.parseLong(kv.getKey().split("/")[3]) % 2 == 0;
    List<Table.KeyValue<String, String>> expectedEntries = mockTableIterator(
        String.class, mockedRenameTable, numberOfVolumes, numberOfBucketsPerVolume, numberOfKeysPerBucket,
        volumeNamePrefix, bucketNamePrefix, keyPrefix, volumeNumber, bucketNumber, startVolumeNumber, startBucketNumber,
        startKeyNumber, filter, numberOfEntries);
    String volumeName = volumeNumber == null ? null : (String.format("%s%010d", volumeNamePrefix, volumeNumber));
    String bucketName = bucketNumber == null ? null : (String.format("%s%010d", bucketNamePrefix, bucketNumber));
    String startKey = startVolumeNumber == null || startBucketNumber == null || startKeyNumber == null ? null
        : (String.format("/%s%010d/%s%010d/%s%010d", volumeNamePrefix, startVolumeNumber, bucketNamePrefix,
        startBucketNumber, keyPrefix, startKeyNumber));
    if (expectedException != null) {
      assertThrows(expectedException, () -> km.getRenamesKeyEntries(volumeName, bucketName, startKey,
          filter, numberOfEntries));
    } else {
      assertEquals(expectedEntries, km.getRenamesKeyEntries(volumeName, bucketName, startKey, filter, numberOfEntries));
    }
  }

  @ParameterizedTest
  @MethodSource("getTableIteratorParameters")
  @SuppressWarnings({"checkstyle:ParameterNumber"})
  public void testGetDeletedDirEntries(int numberOfVolumes, int numberOfBucketsPerVolume, int numberOfKeysPerBucket,
                                       Integer volumeNumber, Integer bucketNumber,
                                       Integer startVolumeNumber, Integer startBucketNumber, Integer startKeyNumber,
                                       int numberOfEntries, Class<? extends Exception> expectedException)
      throws IOException {
    String volumeNamePrefix = "";
    String bucketNamePrefix = "";
    String keyPrefix = "key";
    startVolumeNumber = null;
    OzoneConfiguration configuration = new OzoneConfiguration();
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    KeyManagerImpl km = new KeyManagerImpl(null, null, metadataManager, configuration, null, null, null);
    Table<String, OmKeyInfo> mockedDeletedDirTable = Mockito.mock(Table.class);
    when(mockedDeletedDirTable.getName()).thenReturn(DELETED_DIR_TABLE);
    when(metadataManager.getDeletedDirTable()).thenReturn(mockedDeletedDirTable);
    when(metadataManager.getTableBucketPrefix(eq(DELETED_DIR_TABLE), anyString(), anyString()))
        .thenAnswer(i -> "/" + i.getArguments()[1] + "/" + i.getArguments()[2] + "/");
    List<Table.KeyValue<String, OmKeyInfo>> expectedEntries = mockTableIterator(
        OmKeyInfo.class, mockedDeletedDirTable, numberOfVolumes, numberOfBucketsPerVolume, numberOfKeysPerBucket,
        volumeNamePrefix, bucketNamePrefix, keyPrefix, volumeNumber, bucketNumber, startVolumeNumber, startBucketNumber,
        startKeyNumber, (kv) -> true, numberOfEntries);
    String volumeName = volumeNumber == null ? null : (String.format("%s%010d", volumeNamePrefix, volumeNumber));
    String bucketName = bucketNumber == null ? null : (String.format("%s%010d", bucketNamePrefix, bucketNumber));
    if (expectedException != null) {
      assertThrows(expectedException, () -> km.getDeletedDirEntries(volumeName, bucketName, numberOfEntries));
    } else {
      assertEquals(expectedEntries, km.getDeletedDirEntries(volumeName, bucketName, numberOfEntries));
    }
  }
}
