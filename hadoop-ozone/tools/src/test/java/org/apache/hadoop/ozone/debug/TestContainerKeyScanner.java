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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.debug;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for {@link ContainerKeyScanner}.
 */
@ExtendWith(MockitoExtension.class)
public class TestContainerKeyScanner {

  /*
  private ContainerKeyScanner containerKeyScanner;
  @Mock
  private ContainerKeyInfoWrapper containerKeyInfoWrapper;

  @BeforeEach
  void setup() {
    containerKeyScanner = new ContainerKeyScanner();
    containerKeyScanner.setContainerIds(
        Stream.of(1L, 2L, 3L).collect(Collectors.toSet()));
  }

  @Test
  void testOutputWhenContainerKeyInfosEmpty() {
    when(containerKeyInfoWrapper.getContainerKeyInfos())
        .thenReturn(new ArrayList<>());
    long processedKeys = new Random().nextLong();
    when(containerKeyInfoWrapper.getKeysProcessed()).thenReturn(processedKeys);

    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));

    containerKeyScanner.printOutput(containerKeyInfoWrapper);

    String expectedOutput = "No keys were found for container IDs: " +
        containerKeyScanner.getContainerIds() + "\n" +
        "Keys processed: " + processedKeys + "\n";
    assertEquals(expectedOutput, outContent.toString());
  }

  @Test
  void testOutputWhenContainerKeyInfosNotEmptyAndKeyMatchesContainerId() {
    List<ContainerKeyInfo> containerKeyInfos = Stream.of(
        new ContainerKeyInfo(1L, "vol1", "bucket1", "key1"),
        new ContainerKeyInfo(2L, "vol2", "bucket2", "key2"),
        new ContainerKeyInfo(3L, "vol3", "bucket3", "key3")
    ).collect(Collectors.toList());
    when(containerKeyInfoWrapper.getContainerKeyInfos())
        .thenReturn(containerKeyInfos);
    long processedKeys = containerKeyInfos.size();
    when(containerKeyInfoWrapper.getKeysProcessed()).thenReturn(processedKeys);

    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));

    containerKeyScanner.printOutput(containerKeyInfoWrapper);

    String expectedOutput = "{\n" +
        "  \"keysProcessed\": 3,\n" +
        "  \"containerKeys\": {\n" +
        "    \"1\": [\n" +
        "      {\n" +
        "        \"containerID\": 1,\n" +
        "        \"volumeName\": \"vol1\",\n" +
        "        \"bucketName\": \"bucket1\",\n" +
        "        \"keyName\": \"key1\"\n" +
        "      }\n" +
        "    ],\n" +
        "    \"2\": [\n" +
        "      {\n" +
        "        \"containerID\": 2,\n" +
        "        \"volumeName\": \"vol2\",\n" +
        "        \"bucketName\": \"bucket2\",\n" +
        "        \"keyName\": \"key2\"\n" +
        "      }\n" +
        "    ],\n" +
        "    \"3\": [\n" +
        "      {\n" +
        "        \"containerID\": 3,\n" +
        "        \"volumeName\": \"vol3\",\n" +
        "        \"bucketName\": \"bucket3\",\n" +
        "        \"keyName\": \"key3\"\n" +
        "      }\n" +
        "    ]\n" +
        "  }\n" +
        "}\n";
    assertEquals(expectedOutput, outContent.toString());
  }

  @Test
  void testOutputWhenContainerKeyInfosNotEmptyAndKeysDoNotMatchContainersId() {
    List<ContainerKeyInfo> containerKeyInfos = Stream.of(
        new ContainerKeyInfo(4L, "vol1", "bucket1", "key1"),
        new ContainerKeyInfo(5L, "vol2", "bucket2", "key2"),
        new ContainerKeyInfo(6L, "vol3", "bucket3", "key3")
    ).collect(Collectors.toList());
    when(containerKeyInfoWrapper.getContainerKeyInfos())
        .thenReturn(containerKeyInfos);
    long processedKeys = containerKeyInfos.size();
    when(containerKeyInfoWrapper.getKeysProcessed()).thenReturn(processedKeys);

    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));

    containerKeyScanner.printOutput(containerKeyInfoWrapper);

    String expectedOutput = "{\n" +
        "  \"keysProcessed\": 3,\n" +
        "  \"containerKeys\": {\n" +
        "    \"1\": [],\n" +
        "    \"2\": [],\n" +
        "    \"3\": []\n" +
        "  }\n" +
        "}\n";
    assertEquals(expectedOutput, outContent.toString());
  }

  @Test
  public void testExceptionThrownWhenColumnFamilyDefinitionIsNull() {
    DBDefinition dbDefinition = mock(DBDefinition.class);
    String tableName = "tableName";
    when(dbDefinition.getColumnFamily(tableName)).thenReturn(null);

    Exception e = Assertions.assertThrows(IllegalStateException.class,
        () -> containerKeyScanner.processTable(dbDefinition, null, null, null,
            tableName));
    Assertions.assertEquals("Table with name" + tableName + " not found",
        e.getMessage());
  }

  @Test
  public void testExceptionThrownWhenColumnFamilyHandleIsNull()
      throws IOException {
    ContainerKeyScanner containerKeyScannerMock =
        mock(ContainerKeyScanner.class);
    DBDefinition dbDefinition = mock(DBDefinition.class);
    DBColumnFamilyDefinition dbColumnFamilyDefinition =
        mock(DBColumnFamilyDefinition.class);
    when(dbDefinition.getColumnFamily(any())).thenReturn(
        dbColumnFamilyDefinition);
    when(dbColumnFamilyDefinition.getName()).thenReturn("name");
    when(
        containerKeyScannerMock.processTable(any(), isNull(), isNull(),
            isNull(), isNull())).thenCallRealMethod();

    Exception e = Assertions.assertThrows(IllegalStateException.class,
        () -> containerKeyScannerMock.processTable(dbDefinition, null, null,
            null, null));
    Assertions.assertEquals("columnFamilyHandle is null", e.getMessage());
  }

  @Test
  public void testNoKeysProcessedWhenTableProcessed()
      throws IOException {
    ContainerKeyScanner containerKeyScannerMock =
        mock(ContainerKeyScanner.class);
    DBDefinition dbDefinition = mock(DBDefinition.class);
    DBColumnFamilyDefinition dbColumnFamilyDefinition =
        mock(DBColumnFamilyDefinition.class);
    when(dbDefinition.getColumnFamily(any())).thenReturn(
        dbColumnFamilyDefinition);
    ColumnFamilyHandle columnFamilyHandle = mock(ColumnFamilyHandle.class);
    when(dbColumnFamilyDefinition.getName()).thenReturn("name");
    when(containerKeyScannerMock.getColumnFamilyHandle(any(),
        isNull())).thenReturn(columnFamilyHandle);
    ManagedRocksDB db = mock(ManagedRocksDB.class);
    RocksIterator iterator = mock(RocksIterator.class);
    RocksDB rocksDB = mock(RocksDB.class);
    when(db.get()).thenReturn(rocksDB);
    when(rocksDB.newIterator(columnFamilyHandle)).thenReturn(iterator);
    doNothing().when(iterator).seekToFirst();
    when(
        containerKeyScannerMock.processTable(any(), isNull(), any(),
            isNull(), isNull())).thenCallRealMethod();

    long keysProcessed =
        containerKeyScannerMock.processTable(dbDefinition, null, db, null,
            null);
    assertEquals(0, keysProcessed);
  }

  @Test
  public void testKeysProcessedWhenTableProcessedButNoContainerIdMatch()
      throws IOException {
    ContainerKeyScanner containerKeyScannerMock =
        mock(ContainerKeyScanner.class);
    DBDefinition dbDefinition = mock(DBDefinition.class);
    DBColumnFamilyDefinition dbColumnFamilyDefinition =
        mock(DBColumnFamilyDefinition.class);
    when(dbDefinition.getColumnFamily(any())).thenReturn(
        dbColumnFamilyDefinition);
    ColumnFamilyHandle columnFamilyHandle = mock(ColumnFamilyHandle.class);
    when(dbColumnFamilyDefinition.getName()).thenReturn("name");
    when(containerKeyScannerMock.getColumnFamilyHandle(any(),
        isNull())).thenReturn(columnFamilyHandle);
    ManagedRocksDB db = mock(ManagedRocksDB.class);
    RocksIterator iterator = mock(RocksIterator.class);
    RocksDB rocksDB = mock(RocksDB.class);
    when(db.get()).thenReturn(rocksDB);
    when(rocksDB.newIterator(columnFamilyHandle)).thenReturn(iterator);
    when(iterator.isValid())
        .thenReturn(true)
        .thenReturn(false);
    Codec codec = mock(Codec.class);
    when(dbColumnFamilyDefinition.getValueCodec()).thenReturn(codec);
    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder().build();
    omKeyInfo.setKeyLocationVersions(null);
    when(iterator.value()).thenReturn(new byte[1]);
    when(codec.fromPersistedFormat(any())).thenReturn(omKeyInfo);
    when(
        containerKeyScannerMock.processTable(any(), isNull(), any(),
            isNull(), isNull())).thenCallRealMethod();

    long keysProcessed =
        containerKeyScannerMock.processTable(dbDefinition, null, db, null,
            null);
    assertEquals(1, keysProcessed);
  }

  @Test
  public void testKeysProcessedAndKeyInfoWhenTableProcessedButContainerIdMatch()
      throws IOException {
    ContainerKeyScanner containerKeyScannerMock =
        mock(ContainerKeyScanner.class);
    DBDefinition dbDefinition = mock(DBDefinition.class);
    DBColumnFamilyDefinition dbColumnFamilyDefinition =
        mock(DBColumnFamilyDefinition.class);
    when(dbDefinition.getColumnFamily(any())).thenReturn(
        dbColumnFamilyDefinition);
    ColumnFamilyHandle columnFamilyHandle = mock(ColumnFamilyHandle.class);
    when(dbColumnFamilyDefinition.getName()).thenReturn("name");
    when(containerKeyScannerMock.getColumnFamilyHandle(any(),
        isNull())).thenReturn(columnFamilyHandle);
    ManagedRocksDB db = mock(ManagedRocksDB.class);
    RocksIterator iterator = mock(RocksIterator.class);
    RocksDB rocksDB = mock(RocksDB.class);
    when(db.get()).thenReturn(rocksDB);
    when(rocksDB.newIterator(columnFamilyHandle)).thenReturn(iterator);
    when(iterator.isValid())
        .thenReturn(true)
        .thenReturn(false);
    Codec codec = mock(Codec.class);
    when(dbColumnFamilyDefinition.getValueCodec()).thenReturn(codec);
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = "key1";
    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
    List<OmKeyLocationInfoGroup> keyLocationVersions = new ArrayList<>();
    List<OmKeyLocationInfo> keyLocationInfos = new ArrayList<>();
    long containerID = 1L;
    keyLocationInfos.add(
        new OmKeyLocationInfo.Builder().setBlockID(new BlockID(containerID, 1L))
            .build());
    keyLocationVersions.add(new OmKeyLocationInfoGroup(1L, keyLocationInfos));
    omKeyInfo.setKeyLocationVersions(keyLocationVersions);
    when(iterator.value()).thenReturn(new byte[1]);
    when(codec.fromPersistedFormat(any())).thenReturn(omKeyInfo);
    when(
        containerKeyScannerMock.processTable(any(), isNull(), any(),
            anyList(), isNull())).thenCallRealMethod();
    List<ContainerKeyInfo> containerKeyInfos = new ArrayList<>();
    ContainerKeyInfo expectedContainerKeyInfo =
        new ContainerKeyInfo(containerID, volumeName, bucketName, keyName);
    doCallRealMethod().when(containerKeyScannerMock).setContainerIds(anySet());
    containerKeyScannerMock.setContainerIds(
        containerKeyScanner.getContainerIds());

    long keysProcessed =
        containerKeyScannerMock.processTable(dbDefinition, null, db,
            containerKeyInfos, null);
    assertEquals(1, keysProcessed);
    assertEquals(expectedContainerKeyInfo, containerKeyInfos.get(0));
  }
   */

}
