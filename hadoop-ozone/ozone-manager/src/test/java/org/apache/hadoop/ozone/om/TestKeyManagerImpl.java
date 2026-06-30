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
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * Test class for unit tests KeyManagerImpl.
 */
public class TestKeyManagerImpl {
  private static Stream<TableIteratorTestCase> getSuccessfulTableIteratorParameters() {
    return Stream.of(
        TableIteratorTestCase.newBuilder("Fetch first 50 entries for volume 0, bucket 0")
            .volumeBucket(0, 0)
            .start(0, 0, 0)
            .entries(50)
            .build(),
        TableIteratorTestCase.newBuilder("Fetch first 50 entries for any volume/bucket")
            .start(0, 0, 0)
            .entries(50)
            .build(),
        TableIteratorTestCase.newBuilder("Fetch first 30 entries for volume 1, bucket 1")
            .volumeBucket(1, 1)
            .start(0, 0, 0)
            .entries(30)
            .build(),
        TableIteratorTestCase.newBuilder("Fetch 20 entries from offset (2,2,10) for volume 2, bucket 2")
            .volumeBucket(2, 2)
            .start(2, 2, 10)
            .entries(20)
            .build(),
        TableIteratorTestCase.newBuilder("Fetch 40 entries from offset (2,2,50) for volume 3, bucket 3")
            .volumeBucket(3, 3)
            .start(3, 3, 50)
            .entries(40)
            .build(),
        TableIteratorTestCase.newBuilder("Fetch 200 entries from the very beginning (null start offsets)")
            .start(null, null, null)
            .entries(200)
            .build(),
        TableIteratorTestCase.newBuilder("Fetch 200 entries starting from bucket 3, key 50, spanning 3 buckets")
            .start(0, 3, 50)
            .entries(200)
            .build(),
        TableIteratorTestCase.newBuilder("Fetch 50 entries from volume 2, bucket 5, but only 31 exist")
            .volumeBucket(2, 5)
            .start(2, 5, 70)
            .entries(50)
            .build(),
        TableIteratorTestCase.newBuilder("Start from last volume (4), second-last bucket (8), key 80 "
            + "but only 131 entries exist")
            .start(4, 8, 80)
            .entries(200)
            .build()
    );
  }

  private static Stream<TableIteratorTestCase> getInvalidTableIteratorParameters() {
    return Stream.of(
        TableIteratorTestCase.newBuilder("Invalid: bucket is set but volume is null")
            .volumeBucket(null, 1)
            .start(0, 0, 0)
            .entries(10)
            .build(),
        TableIteratorTestCase.newBuilder("Invalid: volume is set but bucket is null")
            .volumeBucket(1, null)
            .start(0, 0, 0)
            .entries(10)
            .build()
    );
  }

  @SuppressWarnings("unchecked")
  private <V> List<Table.KeyValue<String, V>> mockTableIterator(
      Class<V> valueClass, Table<String, V> table, TableIteratorTestCase testCase, String volumeNamePrefix,
      String bucketNamePrefix, String keyPrefix,
      CheckedFunction<Table.KeyValue<String, V>, Boolean, IOException> filter) throws IOException {
    TreeMap<String, V> values = new TreeMap<>();
    List<Table.KeyValue<String, V>> keyValues = new ArrayList<>();
    String startKey = getTableKey(volumeNamePrefix, testCase.getStartVolumeNumber(), bucketNamePrefix,
        testCase.getStartBucketNumber(), keyPrefix, testCase.getStartKeyNumber());
    for (int i = 0; i < testCase.getNumberOfVolumes(); i++) {
      for (int j = 0; j < testCase.getNumberOfBucketsPerVolume(); j++) {
        for (int k = 0; k < testCase.getNumberOfKeysPerBucket(); k++) {
          String key = getTableKey(volumeNamePrefix, i, bucketNamePrefix, j, keyPrefix, k);
          V value = valueClass == String.class ? (V) key : mock(valueClass);
          values.put(key, value);

          if ((testCase.getVolumeNumber() == null || i == testCase.getVolumeNumber()) &&
              (testCase.getBucketNumber() == null || j == testCase.getBucketNumber()) &&
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
    }).limit(testCase.getNumberOfEntries()).collect(Collectors.toList());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getSuccessfulTableIteratorParameters")
  public void testGetDeletedKeyEntries(TableIteratorTestCase testCase) throws IOException {
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
        .thenAnswer(i -> getBucketPrefix(i.getArguments()));
    CheckedFunction<Table.KeyValue<String, RepeatedOmKeyInfo>, Boolean, IOException> filter =
        (kv) -> getKeyIndex(kv.getKey(), keyPrefix) % 2 == 0;
    List<Table.KeyValue<String, List<OmKeyInfo>>> expectedEntries = mockTableIterator(
        RepeatedOmKeyInfo.class, mockedDeletedTable, testCase, volumeNamePrefix, bucketNamePrefix, keyPrefix,
        filter).stream()
        .map(kv -> {
          String key = kv.getKey();
          RepeatedOmKeyInfo value = kv.getValue();
          List<OmKeyInfo> omKeyInfos = Collections.singletonList(Mockito.mock(OmKeyInfo.class));
          when(value.cloneOmKeyInfoList()).thenReturn(omKeyInfos);
          return Table.newKeyValue(key, omKeyInfos);
        }).collect(Collectors.toList());
    String volumeName = getObjectName(volumeNamePrefix, testCase.getVolumeNumber());
    String bucketName = getObjectName(bucketNamePrefix, testCase.getBucketNumber());
    String startKey = getTableKey(volumeNamePrefix, testCase.getStartVolumeNumber(), bucketNamePrefix,
        testCase.getStartBucketNumber(), keyPrefix, testCase.getStartKeyNumber());
    assertEquals(expectedEntries,
        km.getDeletedKeyEntries(volumeName, bucketName, startKey, filter, testCase.getNumberOfEntries()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getInvalidTableIteratorParameters")
  public void testGetDeletedKeyEntriesFails(TableIteratorTestCase testCase) throws IOException {
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
        .thenAnswer(i -> getBucketPrefix(i.getArguments()));
    CheckedFunction<Table.KeyValue<String, RepeatedOmKeyInfo>, Boolean, IOException> filter =
        (kv) -> getKeyIndex(kv.getKey(), keyPrefix) % 2 == 0;
    String volumeName = getObjectName(volumeNamePrefix, testCase.getVolumeNumber());
    String bucketName = getObjectName(bucketNamePrefix, testCase.getBucketNumber());
    String startKey = getTableKey(volumeNamePrefix, testCase.getStartVolumeNumber(), bucketNamePrefix,
        testCase.getStartBucketNumber(), keyPrefix, testCase.getStartKeyNumber());

    assertThrows(IOException.class,
        () -> km.getDeletedKeyEntries(volumeName, bucketName, startKey, filter, testCase.getNumberOfEntries()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getSuccessfulTableIteratorParameters")
  public void testGetRenameKeyEntries(TableIteratorTestCase testCase) throws IOException {
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
        .thenAnswer(i -> getBucketPrefix(i.getArguments()));
    CheckedFunction<Table.KeyValue<String, String>, Boolean, IOException> filter =
        (kv) -> getRenameKeyIndex(kv.getKey()) % 2 == 0;
    List<Table.KeyValue<String, String>> expectedEntries = mockTableIterator(
        String.class, mockedRenameTable, testCase, volumeNamePrefix, bucketNamePrefix, keyPrefix, filter);
    String volumeName = getObjectName(volumeNamePrefix, testCase.getVolumeNumber());
    String bucketName = getObjectName(bucketNamePrefix, testCase.getBucketNumber());
    String startKey = getTableKey(volumeNamePrefix, testCase.getStartVolumeNumber(), bucketNamePrefix,
        testCase.getStartBucketNumber(), keyPrefix, testCase.getStartKeyNumber());
    assertEquals(expectedEntries,
        km.getRenamesKeyEntries(volumeName, bucketName, startKey, filter, testCase.getNumberOfEntries()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getInvalidTableIteratorParameters")
  public void testGetRenameKeyEntriesFails(TableIteratorTestCase testCase) throws IOException {
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
        .thenAnswer(i -> getBucketPrefix(i.getArguments()));
    CheckedFunction<Table.KeyValue<String, String>, Boolean, IOException> filter =
        (kv) -> getRenameKeyIndex(kv.getKey()) % 2 == 0;
    String volumeName = getObjectName(volumeNamePrefix, testCase.getVolumeNumber());
    String bucketName = getObjectName(bucketNamePrefix, testCase.getBucketNumber());
    String startKey = getTableKey(volumeNamePrefix, testCase.getStartVolumeNumber(), bucketNamePrefix,
        testCase.getStartBucketNumber(), keyPrefix, testCase.getStartKeyNumber());

    assertThrows(IOException.class,
        () -> km.getRenamesKeyEntries(volumeName, bucketName, startKey, filter, testCase.getNumberOfEntries()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getSuccessfulTableIteratorParameters")
  public void testGetDeletedDirEntries(TableIteratorTestCase testCase) throws IOException {
    String volumeNamePrefix = "";
    String bucketNamePrefix = "";
    String keyPrefix = "key";
    OzoneConfiguration configuration = new OzoneConfiguration();
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    KeyManagerImpl km = new KeyManagerImpl(null, null, metadataManager, configuration, null, null, null);
    Table<String, OmKeyInfo> mockedDeletedDirTable = Mockito.mock(Table.class);
    when(mockedDeletedDirTable.getName()).thenReturn(DELETED_DIR_TABLE);
    when(metadataManager.getDeletedDirTable()).thenReturn(mockedDeletedDirTable);
    when(metadataManager.getTableBucketPrefix(eq(DELETED_DIR_TABLE), anyString(), anyString()))
        .thenAnswer(i -> getBucketPrefix(i.getArguments()));
    List<Table.KeyValue<String, OmKeyInfo>> expectedEntries = mockTableIterator(
        OmKeyInfo.class, mockedDeletedDirTable, testCase.withoutStartKey(), volumeNamePrefix, bucketNamePrefix,
        keyPrefix, (kv) -> true);
    String volumeName = getObjectName(volumeNamePrefix, testCase.getVolumeNumber());
    String bucketName = getObjectName(bucketNamePrefix, testCase.getBucketNumber());
    assertEquals(expectedEntries, km.getDeletedDirEntries(volumeName, bucketName, testCase.getNumberOfEntries()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getInvalidTableIteratorParameters")
  public void testGetDeletedDirEntriesFails(TableIteratorTestCase testCase) throws IOException {
    String volumeNamePrefix = "";
    String bucketNamePrefix = "";
    OzoneConfiguration configuration = new OzoneConfiguration();
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    KeyManagerImpl km = new KeyManagerImpl(null, null, metadataManager, configuration, null, null, null);
    Table<String, OmKeyInfo> mockedDeletedDirTable = Mockito.mock(Table.class);
    when(mockedDeletedDirTable.getName()).thenReturn(DELETED_DIR_TABLE);
    when(metadataManager.getDeletedDirTable()).thenReturn(mockedDeletedDirTable);
    when(metadataManager.getTableBucketPrefix(eq(DELETED_DIR_TABLE), anyString(), anyString()))
        .thenAnswer(i -> getBucketPrefix(i.getArguments()));
    String volumeName = getObjectName(volumeNamePrefix, testCase.getVolumeNumber());
    String bucketName = getObjectName(bucketNamePrefix, testCase.getBucketNumber());

    assertThrows(IOException.class,
        () -> km.getDeletedDirEntries(volumeName, bucketName, testCase.getNumberOfEntries()));
  }

  private static String getObjectName(String prefix, Integer number) {
    return number == null ? null : String.format("%s%010d", prefix, number);
  }

  private static String getTableKey(String volumeNamePrefix, Integer volumeNumber, String bucketNamePrefix,
      Integer bucketNumber, String keyPrefix, Integer keyNumber) {
    if (volumeNumber == null || bucketNumber == null || keyNumber == null) {
      return null;
    }
    return String.format("/%s%010d/%s%010d/%s%010d", volumeNamePrefix, volumeNumber, bucketNamePrefix, bucketNumber,
        keyPrefix, keyNumber);
  }

  private static String getBucketPrefix(Object[] arguments) {
    return "/" + arguments[1] + "/" + arguments[2] + "/";
  }

  private static long getKeyIndex(String key, String keyPrefix) {
    return Long.parseLong(key.split(keyPrefix)[1]);
  }

  private static long getRenameKeyIndex(String key) {
    return Long.parseLong(key.split("/")[3]);
  }

  private static final class TableIteratorTestCase {
    private final String name;
    private final int numberOfVolumes;
    private final int numberOfBucketsPerVolume;
    private final int numberOfKeysPerBucket;
    private final Integer volumeNumber;
    private final Integer bucketNumber;
    private final Integer startVolumeNumber;
    private final Integer startBucketNumber;
    private final Integer startKeyNumber;
    private final int numberOfEntries;

    private TableIteratorTestCase(Builder builder) {
      name = builder.name;
      numberOfVolumes = builder.numberOfVolumes;
      numberOfBucketsPerVolume = builder.numberOfBucketsPerVolume;
      numberOfKeysPerBucket = builder.numberOfKeysPerBucket;
      volumeNumber = builder.volumeNumber;
      bucketNumber = builder.bucketNumber;
      startVolumeNumber = builder.startVolumeNumber;
      startBucketNumber = builder.startBucketNumber;
      startKeyNumber = builder.startKeyNumber;
      numberOfEntries = builder.numberOfEntries;
    }

    static Builder newBuilder(String name) {
      return new Builder(name);
    }

    TableIteratorTestCase withoutStartKey() {
      return newBuilder(name)
          .tableSize(numberOfVolumes, numberOfBucketsPerVolume, numberOfKeysPerBucket)
          .volumeBucket(volumeNumber, bucketNumber)
          .start(null, startBucketNumber, startKeyNumber)
          .entries(numberOfEntries)
          .build();
    }

    int getNumberOfVolumes() {
      return numberOfVolumes;
    }

    int getNumberOfBucketsPerVolume() {
      return numberOfBucketsPerVolume;
    }

    int getNumberOfKeysPerBucket() {
      return numberOfKeysPerBucket;
    }

    Integer getVolumeNumber() {
      return volumeNumber;
    }

    Integer getBucketNumber() {
      return bucketNumber;
    }

    Integer getStartVolumeNumber() {
      return startVolumeNumber;
    }

    Integer getStartBucketNumber() {
      return startBucketNumber;
    }

    Integer getStartKeyNumber() {
      return startKeyNumber;
    }

    int getNumberOfEntries() {
      return numberOfEntries;
    }

    @Override
    public String toString() {
      return name;
    }

    private static final class Builder {
      private final String name;
      private int numberOfVolumes = 5;
      private int numberOfBucketsPerVolume = 10;
      private int numberOfKeysPerBucket = 100;
      private Integer volumeNumber;
      private Integer bucketNumber;
      private Integer startVolumeNumber;
      private Integer startBucketNumber;
      private Integer startKeyNumber;
      private int numberOfEntries;

      private Builder(String testName) {
        this.name = testName;
      }

      Builder tableSize(int volumes, int bucketsPerVolume, int keysPerBucket) {
        numberOfVolumes = volumes;
        numberOfBucketsPerVolume = bucketsPerVolume;
        numberOfKeysPerBucket = keysPerBucket;
        return this;
      }

      Builder volumeBucket(Integer volume, Integer bucket) {
        volumeNumber = volume;
        bucketNumber = bucket;
        return this;
      }

      Builder start(Integer volume, Integer bucket, Integer key) {
        startVolumeNumber = volume;
        startBucketNumber = bucket;
        startKeyNumber = key;
        return this;
      }

      Builder entries(int entries) {
        numberOfEntries = entries;
        return this;
      }

      TableIteratorTestCase build() {
        return new TableIteratorTestCase(this);
      }
    }
  }
}
