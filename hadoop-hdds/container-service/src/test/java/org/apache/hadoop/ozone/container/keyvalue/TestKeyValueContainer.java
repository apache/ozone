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

package org.apache.hadoop.ozone.container.keyvalue;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V2;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V3;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.buildTestTree;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.verifyAllDataChecksumsMatch;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED;
import static org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil.isSameSchemaVersion;
import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.utils.db.DatanodeDBProfile;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.metadata.AbstractDatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.replication.CopyContainerCompression;
import org.apache.hadoop.util.DiskChecker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.LiveFileMetaData;

/**
 * Class to test KeyValue Container operations.
 */
public class TestKeyValueContainer {

  @TempDir
  private File folder;

  private String scmId = UUID.randomUUID().toString();
  private VolumeSet volumeSet;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private KeyValueContainerData keyValueContainerData;
  private KeyValueContainer keyValueContainer;
  private UUID datanodeId;

  private ContainerLayoutVersion layout;
  private String schemaVersion;
  private List<HddsVolume> hddsVolumes;

  // Use one configuration object across parameterized runs of tests.
  // This preserves the column family options in the container options
  // cache for testContainersShareColumnFamilyOptions.
  private static final OzoneConfiguration CONF = new OzoneConfiguration();

  private void setVersionInfo(ContainerTestVersionInfo versionInfo) {
    this.layout = versionInfo.getLayout();
    this.schemaVersion = versionInfo.getSchemaVersion();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, CONF);
  }

  private void init(ContainerTestVersionInfo versionInfo) throws Exception {
    setVersionInfo(versionInfo);
    CodecBuffer.enableLeakDetection();

    DatanodeConfiguration dc = CONF.getObject(DatanodeConfiguration.class);
    dc.setAutoCompactionSmallSstFile(true);
    dc.setAutoCompactionSmallSstFileNum(100);
    dc.setRocksdbDeleteObsoleteFilesPeriod(5000);
    CONF.setFromObject(dc);

    datanodeId = UUID.randomUUID();

    hddsVolumes = new ArrayList<>();

    hddsVolumes.add(new HddsVolume.Builder(folder.toString())
        .conf(CONF).datanodeUuid(datanodeId
            .toString()).build());
    StorageVolumeUtil.checkVolume(hddsVolumes.get(0), scmId, scmId, CONF,
        null, null);

    volumeSet = mock(MutableVolumeSet.class);
    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);
    when(volumeSet.getVolumesList())
        .thenAnswer(i -> hddsVolumes.stream()
            .map(v -> (StorageVolume) v)
            .collect(Collectors.toList()));
    when(volumeChoosingPolicy
        .chooseVolume(anyList(), anyLong())).thenAnswer(
            invocation -> {
              List<HddsVolume> volumes = invocation.getArgument(0);
              return volumes.get(0);
            });

    keyValueContainerData = new KeyValueContainerData(1L,
        layout,
        (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
        datanodeId.toString());

    keyValueContainer = new KeyValueContainer(keyValueContainerData, CONF);
  }

  @AfterEach
  public void after() {
    CodecBuffer.assertNoLeaks();
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testCreateContainer(ContainerTestVersionInfo versionInfo)
      throws Exception {
    init(versionInfo);
    testCreateContainer();
  }

  private void testCreateContainer() throws StorageContainerException {
    createContainer();

    String containerMetaDataPath = keyValueContainerData.getMetadataPath();
    String chunksPath = keyValueContainerData.getChunksPath();

    // Check whether containerMetaDataPath and chunksPath exists or not.
    assertNotNull(containerMetaDataPath);
    assertNotNull(chunksPath);
    // Check whether container file and container db file exists or not.
    assertTrue(keyValueContainer.getContainerFile().exists(),
        ".Container File does not exist");
    assertTrue(keyValueContainer.getContainerDBFile().exists(), "Container " +
        "DB does not exist");
  }

  /**
   * Tests repair of containers affected by the bug reported in HDDS-6235.
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testMissingChunksDirCreated(ContainerTestVersionInfo versionInfo)
      throws Exception {
    init(versionInfo);
    // Create an empty container and delete its chunks directory.
    createContainer();
    closeContainer();
    // Sets the checksum.
    populate(0);
    KeyValueContainerData data = keyValueContainer.getContainerData();
    File chunksDir = new File(data.getChunksPath());
    assertTrue(chunksDir.delete());

    // When the container is loaded, the missing chunks directory should
    // be created.
    KeyValueContainerUtil.parseKVContainerData(data, CONF);
    assertTrue(chunksDir.exists());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testNextVolumeTriedOnWriteFailure(
      ContainerTestVersionInfo versionInfo) throws Exception {
    init(versionInfo);
    String volumeDirPath =
        Files.createDirectory(folder.toPath().resolve("volumeDir"))
            .toFile().getAbsolutePath();
    HddsVolume newVolume = new HddsVolume.Builder(volumeDirPath)
        .conf(CONF).datanodeUuid(datanodeId.toString()).build();
    StorageVolumeUtil.checkVolume(newVolume, scmId, scmId, CONF, null, null);
    hddsVolumes.add(newVolume);

    // Override the class, so that the first time we call it, it throws
    // simulating a disk or write failure. The second time it should be ok

    final AtomicInteger callCount = new AtomicInteger(0);
    keyValueContainer = new KeyValueContainer(keyValueContainerData, CONF) {

      @Override
      protected void createContainerMetaData(File containerMetaDataPath,
          File chunksPath, File dbFile, String schemaVers,
          ConfigurationSource configuration) throws IOException {
        if (callCount.get() == 0) {
          callCount.incrementAndGet();
          throw new IOException("Injected failure");
        } else {
          callCount.incrementAndGet();
          super.createContainerMetaData(containerMetaDataPath, chunksPath,
              dbFile, schemaVers, configuration);
        }
      }
    };
    testCreateContainer();
    // We should have called the mocked class twice. Once to get an error and
    // then retry without a failure.
    assertEquals(2, callCount.get());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testEmptyContainerImportExport(
      ContainerTestVersionInfo versionInfo) throws Exception {
    init(versionInfo);
    createContainer();
    closeContainer();

    KeyValueContainerData data = keyValueContainer.getContainerData();

    // Check state of original container.
    checkContainerFilesPresent(data, 0);

    //destination path
    File exportTar = Files.createFile(
        folder.toPath().resolve("export.tar")).toFile();
    TarContainerPacker packer = new TarContainerPacker(NO_COMPRESSION);
    //export the container
    try (OutputStream fos = Files.newOutputStream(exportTar.toPath())) {
      keyValueContainer.exportContainerData(fos, packer);
    }

    KeyValueContainerUtil.removeContainer(
        keyValueContainer.getContainerData(), CONF);
    keyValueContainer.delete();

    // import container.
    try (InputStream fis = Files.newInputStream(exportTar.toPath())) {
      keyValueContainer.importContainerData(fis, packer);
    }

    // Make sure empty chunks dir was unpacked.
    checkContainerFilesPresent(data, 0);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testEmptyMerkleTreeImportExport(ContainerTestVersionInfo versionInfo) throws Exception {
    init(versionInfo);
    createContainer();
    closeContainer();

    KeyValueContainerData data = keyValueContainer.getContainerData();
    // Create an empty checksum file that exists but has no valid merkle tree
    File checksumFile = ContainerChecksumTreeManager.getContainerChecksumFile(data);
    ContainerProtos.ContainerChecksumInfo emptyContainerInfo = ContainerProtos.ContainerChecksumInfo
        .newBuilder().build();
    try (OutputStream tmpOutputStream = Files.newOutputStream(checksumFile.toPath())) {
      emptyContainerInfo.writeTo(tmpOutputStream);
    }

    // Check state of original container.
    checkContainerFilesPresent(data, 0);

    //destination path
    File exportTar = Files.createFile(
        folder.toPath().resolve("export.tar")).toFile();
    TarContainerPacker packer = new TarContainerPacker(NO_COMPRESSION);
    //export the container
    try (OutputStream fos = Files.newOutputStream(exportTar.toPath())) {
      keyValueContainer.exportContainerData(fos, packer);
    }

    KeyValueContainerUtil.removeContainer(
        keyValueContainer.getContainerData(), CONF);
    keyValueContainer.delete();

    // import container.
    try (InputStream fis = Files.newInputStream(exportTar.toPath())) {
      keyValueContainer.importContainerData(fis, packer);
    }

    // Make sure empty chunks dir was unpacked.
    checkContainerFilesPresent(data, 0);
    data = keyValueContainer.getContainerData();
    ContainerProtos.ContainerChecksumInfo checksumInfo = ContainerChecksumTreeManager.readChecksumInfo(data);
    assertFalse(checksumInfo.hasContainerMerkleTree());
    // The import should not fail and the checksum should be 0
    assertEquals(0, data.getDataChecksum());
    // The checksum is not stored in rocksDB as the container merkle tree doesn't exist.
    verifyAllDataChecksumsMatch(data, CONF);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testUnhealthyContainerImportExport(
      ContainerTestVersionInfo versionInfo) throws Exception {
    init(versionInfo);
    createContainer();
    long numberOfKeysToWrite = 12;
    populate(numberOfKeysToWrite);
    keyValueContainerData.setState(
        ContainerProtos.ContainerDataProto.State.UNHEALTHY);
    KeyValueContainerData data = keyValueContainer.getContainerData();
    keyValueContainer.update(data.getMetadata(), true);

    //destination path
    File exportTar = Files.createFile(folder.toPath().resolve("export.tar")).toFile();
    TarContainerPacker packer = new TarContainerPacker(NO_COMPRESSION);
    //export the container
    try (OutputStream fos = Files.newOutputStream(exportTar.toPath())) {
      keyValueContainer.exportContainerData(fos, packer);
    }

    KeyValueContainerUtil.removeContainer(
        keyValueContainer.getContainerData(), CONF);
    keyValueContainer.delete();

    // import container.
    try (InputStream fis = Files.newInputStream(exportTar.toPath())) {
      keyValueContainer.importContainerData(fis, packer);
    }

    // Re-load container data after import
    data = keyValueContainer.getContainerData();
    // Make sure empty chunks dir was unpacked.
    checkContainerFilesPresent(data, 0);
    // Ensure the unhealthy state is preserved
    assertEquals(ContainerProtos.ContainerDataProto.State.UNHEALTHY,
        data.getState());
    assertEquals(numberOfKeysToWrite, keyValueContainerData.getBlockCount());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testContainerImportExport(ContainerTestVersionInfo versionInfo)
      throws Exception {
    init(versionInfo);
    long containerId = keyValueContainer.getContainerData().getContainerID();
    createContainer();
    long numberOfKeysToWrite = 12;
    closeContainer();
    populate(numberOfKeysToWrite);

    // Create merkle tree and set data checksum to simulate actual key value container.
    File checksumFile = ContainerChecksumTreeManager.getContainerChecksumFile(
        keyValueContainer.getContainerData());
    ContainerProtos.ContainerMerkleTree containerMerkleTreeWriterProto = buildTestTree(CONF).toProto();
    keyValueContainerData.setDataChecksum(containerMerkleTreeWriterProto.getDataChecksum());
    ContainerProtos.ContainerChecksumInfo containerInfo = ContainerProtos.ContainerChecksumInfo.newBuilder()
        .setContainerID(containerId)
        .setContainerMerkleTree(containerMerkleTreeWriterProto).build();
    try (OutputStream tmpOutputStream = Files.newOutputStream(checksumFile.toPath())) {
      containerInfo.writeTo(tmpOutputStream);
    }

    //destination path
    File folderToExport = Files.createFile(
        folder.toPath().resolve("export.tar")).toFile();
    for (CopyContainerCompression compr : CopyContainerCompression.values()) {
      TarContainerPacker packer = new TarContainerPacker(compr);

      //export the container
      try (OutputStream fos = Files.newOutputStream(folderToExport.toPath())) {
        keyValueContainer
            .exportContainerData(fos, packer);
      }

      //delete the original one
      KeyValueContainerUtil.removeContainer(
          keyValueContainer.getContainerData(), CONF);
      keyValueContainer.delete();

      //create a new one
      KeyValueContainerData containerData =
          new KeyValueContainerData(containerId,
              keyValueContainerData.getLayoutVersion(),
              keyValueContainerData.getMaxSize(), UUID.randomUUID().toString(),
              datanodeId.toString());
      containerData.setSchemaVersion(keyValueContainerData.getSchemaVersion());
      KeyValueContainer container = new KeyValueContainer(containerData, CONF);

      HddsVolume containerVolume = volumeChoosingPolicy.chooseVolume(
          StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList()), 1);

      container.populatePathFields(scmId, containerVolume);
      try (InputStream fis = Files.newInputStream(folderToExport.toPath())) {
        container.importContainerData(fis, packer);
      }

      assertEquals("value1", containerData.getMetadata().get("key1"));
      assertEquals(keyValueContainerData.getContainerDBType(),
          containerData.getContainerDBType());
      assertEquals(keyValueContainerData.getState(),
          containerData.getState());
      assertEquals(numberOfKeysToWrite,
          containerData.getBlockCount());
      assertEquals(keyValueContainerData.getLayoutVersion(),
          containerData.getLayoutVersion());
      assertEquals(keyValueContainerData.getMaxSize(),
          containerData.getMaxSize());
      assertEquals(keyValueContainerData.getBytesUsed(),
          containerData.getBytesUsed());
      assertEquals(keyValueContainerData.getDataChecksum(), containerData.getDataChecksum());
      verifyAllDataChecksumsMatch(containerData, CONF);

      assertNotNull(containerData.getContainerFileChecksum());
      assertNotEquals(containerData.ZERO_CHECKSUM, container.getContainerData().getContainerFileChecksum());

      //Can't overwrite existing container
      KeyValueContainer finalContainer = container;
      assertThrows(IOException.class, () -> {
        try (InputStream fis = Files.newInputStream(folderToExport.toPath())) {
          finalContainer.importContainerData(fis, packer);
        }
      }, "Container is imported twice. Previous files are overwritten");
      //all good
      assertTrue(container.getContainerFile().exists());

      //Import failure should cleanup the container directory
      containerData =
          new KeyValueContainerData(containerId + 1,
              keyValueContainerData.getLayoutVersion(),
              keyValueContainerData.getMaxSize(), UUID.randomUUID().toString(),
              datanodeId.toString());
      containerData.setSchemaVersion(keyValueContainerData.getSchemaVersion());
      container = new KeyValueContainer(containerData, CONF);

      containerVolume = volumeChoosingPolicy.chooseVolume(
          StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList()), 1);
      container.populatePathFields(scmId, containerVolume);
      KeyValueContainer finalContainer1 = container;
      assertThrows(IOException.class, () -> {
        try {
          InputStream fis = Files.newInputStream(folderToExport.toPath());
          fis.close();
          finalContainer1.importContainerData(fis, packer);
        } finally {
          File directory =
              new File(finalContainer1.getContainerData().getContainerPath());
          assertFalse(directory.exists());
        }
      });
    }
  }

  private void checkContainerFilesPresent(KeyValueContainerData data,
      long expectedNumFilesInChunksDir) throws IOException {
    File chunksDir = new File(data.getChunksPath());
    assertTrue(Files.isDirectory(chunksDir.toPath()));
    try (Stream<Path> stream = Files.list(chunksDir.toPath())) {
      assertEquals(expectedNumFilesInChunksDir, stream.count());
    }
    assertTrue(data.getDbFile().exists());
    assertTrue(KeyValueContainer.getContainerFile(
        data.getMetadataPath(), data.getContainerID()).exists());
  }

  /**
   * Create the container on disk.
   */
  private void createContainer() throws StorageContainerException {
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    keyValueContainerData = keyValueContainer.getContainerData();
  }

  /**
   * Add some keys to the container.
   */
  private void populate(KeyValueContainer container, long numberOfKeysToWrite)
      throws IOException {
    KeyValueContainerData cData = container.getContainerData();
    try (DBHandle metadataStore = BlockUtils.getDB(cData, CONF)) {
      Table<String, BlockData> blockDataTable =
              metadataStore.getStore().getBlockDataTable();

      for (long i = 0; i < numberOfKeysToWrite; i++) {
        blockDataTable.put(cData.getBlockKey(i),
            new BlockData(new BlockID(i, i)));
      }

      // As now when we put blocks, we increment block count and update in DB.
      // As for test, we are doing manually so adding key count to DB.
      metadataStore.getStore().getMetadataTable()
              .put(cData.getBlockCountKey(), numberOfKeysToWrite);
    }

    Map<String, String> metadata = new HashMap<>();
    metadata.put("key1", "value1");
    container.update(metadata, true);
  }

  /**
   * Add some keys to the container.
   */
  private void populate(long numberOfKeysToWrite) throws IOException {
    populate(keyValueContainer, numberOfKeysToWrite);
  }

  private void populateWithoutBlock(KeyValueContainer container,
                                    long numberOfKeysToWrite)
      throws IOException {
    KeyValueContainerData cData = container.getContainerData();
    try (DBHandle metadataStore = BlockUtils.getDB(cData, CONF)) {
      // Just update metdata, and don't insert in block table
      // As for test, we are doing manually so adding key count to DB.
      metadataStore.getStore().getMetadataTable()
          .put(cData.getBlockCountKey(), numberOfKeysToWrite);
    }

    Map<String, String> metadata = new HashMap<>();
    metadata.put("key1", "value1");
    container.update(metadata, true);
  }

  /**
   * Set container state to CLOSED.
   */
  private void closeContainer() {
    keyValueContainerData.setState(
        ContainerProtos.ContainerDataProto.State.CLOSED);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void concurrentExport(ContainerTestVersionInfo versionInfo)
      throws Exception {
    init(versionInfo);
    createContainer();
    populate(100);
    closeContainer();

    AtomicReference<String> failed = new AtomicReference<>();

    TarContainerPacker packer = new TarContainerPacker(NO_COMPRESSION);
    List<Thread> threads = IntStream.range(0, 20)
        .mapToObj(i -> new Thread(() -> {
          try {
            File file = Files.createFile(
                folder.toPath().resolve("concurrent" + i + ".tar")).toFile();
            try (OutputStream out = Files.newOutputStream(file.toPath())) {
              keyValueContainer.exportContainerData(out, packer);
            }
          } catch (Exception e) {
            failed.compareAndSet(null, e.getMessage());
          }
        }))
        .collect(Collectors.toList());

    threads.forEach(Thread::start);
    for (Thread thread : threads) {
      thread.join();
    }

    assertNull(failed.get());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testDuplicateContainer(ContainerTestVersionInfo versionInfo) throws Exception {
    init(versionInfo);

    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    StorageContainerException exception = assertThrows(StorageContainerException.class, () ->
        keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId));
    assertEquals(ContainerProtos.Result.CONTAINER_ALREADY_EXISTS, exception.getResult());
    assertThat(exception).hasMessage("Container creation failed because ContainerFile already exists");
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testDiskFullExceptionCreateContainer(
      ContainerTestVersionInfo versionInfo) throws Exception {
    init(versionInfo);
    reset(volumeChoosingPolicy);
    when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenThrow(DiskChecker.DiskOutOfSpaceException.class);

    StorageContainerException exception = assertThrows(StorageContainerException.class, () ->
        keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId));
    assertEquals(ContainerProtos.Result.DISK_OUT_OF_SPACE, exception.getResult());
    assertThat(exception).hasMessage("Container creation failed, due to disk out of space");
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testDeleteContainer(ContainerTestVersionInfo versionInfo)
      throws Exception {
    init(versionInfo);
    closeContainer();
    keyValueContainer = new KeyValueContainer(
        keyValueContainerData, CONF);
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    KeyValueContainerUtil.removeContainer(
        keyValueContainer.getContainerData(), CONF);
    keyValueContainer.delete();

    String containerMetaDataPath = keyValueContainerData
        .getMetadataPath();
    File containerMetaDataLoc = new File(containerMetaDataPath);

    assertFalse(containerMetaDataLoc
        .getParentFile().exists(), "Container directory still exists");

    assertFalse(keyValueContainer.getContainerFile().exists(),
        "Container File still exists");

    if (isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V3)) {
      assertTrue(keyValueContainer.getContainerDBFile().exists());
    } else {
      assertFalse(keyValueContainer.getContainerDBFile().exists(),
          "Container DB file still exists");
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testCloseContainer(ContainerTestVersionInfo versionInfo)
      throws Exception {
    init(versionInfo);
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    keyValueContainer.close();

    keyValueContainerData = keyValueContainer
        .getContainerData();

    assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
        keyValueContainerData.getState());

    //Check state in the .container file
    File containerFile = keyValueContainer.getContainerFile();

    keyValueContainerData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
        keyValueContainerData.getState());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testReportOfUnhealthyContainer(
      ContainerTestVersionInfo versionInfo) throws Exception {
    init(versionInfo);
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    assertNotNull(keyValueContainer.getContainerReport());
    keyValueContainer.markContainerUnhealthy();
    File containerFile = keyValueContainer.getContainerFile();
    keyValueContainerData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertEquals(ContainerProtos.ContainerDataProto.State.UNHEALTHY,
        keyValueContainerData.getState());
    assertNotNull(keyValueContainer.getContainerReport());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testUpdateContainer(ContainerTestVersionInfo versionInfo)
      throws Exception {
    init(versionInfo);
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    Map<String, String> metadata = new HashMap<>();
    metadata.put(OzoneConsts.VOLUME, OzoneConsts.OZONE);
    metadata.put(OzoneConsts.OWNER, OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    keyValueContainer.update(metadata, true);

    keyValueContainerData = keyValueContainer
        .getContainerData();

    assertEquals(2, keyValueContainerData.getMetadata().size());

    //Check metadata in the .container file
    File containerFile = keyValueContainer.getContainerFile();

    keyValueContainerData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertEquals(2, keyValueContainerData.getMetadata().size());

  }

  @ContainerTestVersionInfo.ContainerTest
  public void testUpdateContainerUnsupportedRequest(
      ContainerTestVersionInfo versionInfo) throws Exception {
    init(versionInfo);

    closeContainer();

    StorageContainerException exception = assertThrows(StorageContainerException.class, () -> {
      keyValueContainer = new KeyValueContainer(keyValueContainerData, CONF);
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
      Map<String, String> metadata = new HashMap<>();
      metadata.put(OzoneConsts.VOLUME, OzoneConsts.OZONE);
      keyValueContainer.update(metadata, false);
    });

    assertEquals(ContainerProtos.Result.UNSUPPORTED_REQUEST, exception.getResult());
    assertThat(exception).hasMessageContaining(keyValueContainerData.toString());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testContainerRocksDB(ContainerTestVersionInfo versionInfo)
      throws Exception {
    init(versionInfo);
    closeContainer();
    keyValueContainer = new KeyValueContainer(
        keyValueContainerData, CONF);
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);

    try (DBHandle db = BlockUtils.getDB(keyValueContainerData, CONF)) {
      RDBStore store = (RDBStore) db.getStore().getStore();
      long defaultCacheSize = OzoneConsts.GB;
      long cacheSize = Long.parseLong(store
          .getProperty("rocksdb.block-cache-capacity"));
      assertEquals(defaultCacheSize, cacheSize);
      for (ColumnFamily handle : store.getColumnFamilies()) {
        cacheSize = Long.parseLong(
            store.getProperty(handle, "rocksdb.block-cache-capacity"));
        assertEquals(defaultCacheSize, cacheSize);
      }
    }
  }

  @Test
  public void testContainersShareColumnFamilyOptions() {
    ConfigurationSource conf = new OzoneConfiguration();

    // Make sure ColumnFamilyOptions are same for a particular db profile
    for (Supplier<DatanodeDBProfile> dbProfileSupplier : new Supplier[] {
        DatanodeDBProfile.Disk::new, DatanodeDBProfile.SSD::new }) {
      // ColumnFamilyOptions should be same across configurations
      ColumnFamilyOptions columnFamilyOptions1 = dbProfileSupplier.get()
          .getColumnFamilyOptions(new OzoneConfiguration());
      ColumnFamilyOptions columnFamilyOptions2 = dbProfileSupplier.get()
          .getColumnFamilyOptions(new OzoneConfiguration());
      assertEquals(columnFamilyOptions1, columnFamilyOptions2);

      // ColumnFamilyOptions should be same when queried multiple times
      // for a particulat configuration
      columnFamilyOptions1 = dbProfileSupplier.get()
          .getColumnFamilyOptions(conf);
      columnFamilyOptions2 = dbProfileSupplier.get()
          .getColumnFamilyOptions(conf);
      assertEquals(columnFamilyOptions1, columnFamilyOptions2);
    }

    // Make sure ColumnFamilyOptions are different for different db profile
    DatanodeDBProfile diskProfile = new DatanodeDBProfile.Disk();
    DatanodeDBProfile ssdProfile = new DatanodeDBProfile.SSD();
    assertNotEquals(
        diskProfile.getColumnFamilyOptions(new OzoneConfiguration()),
        ssdProfile.getColumnFamilyOptions(new OzoneConfiguration()));
    assertNotEquals(diskProfile.getColumnFamilyOptions(conf),
        ssdProfile.getColumnFamilyOptions(conf));
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testDBProfileAffectsDBOptions(
      ContainerTestVersionInfo versionInfo) throws Exception {
    init(versionInfo);
    // Create Container 1
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);

    DatanodeDBProfile outProfile1;
    try (DBHandle db1 =
             BlockUtils.getDB(keyValueContainer.getContainerData(), CONF)) {
      DatanodeStore store1 = db1.getStore();
      assertInstanceOf(AbstractDatanodeStore.class, store1);
      outProfile1 = ((AbstractDatanodeStore) store1).getDbProfile();
    }

    // Create Container 2 with different DBProfile in otherConf
    OzoneConfiguration otherConf = new OzoneConfiguration();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, otherConf);
    // Use a dedicated profile for test
    otherConf.setEnum(HDDS_DB_PROFILE, DBProfile.SSD);

    keyValueContainerData = new KeyValueContainerData(2L,
        layout,
        (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
        datanodeId.toString());
    keyValueContainer = new KeyValueContainer(keyValueContainerData, otherConf);
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);

    DatanodeDBProfile outProfile2;
    try (DBHandle db2 =
        BlockUtils.getDB(keyValueContainer.getContainerData(), otherConf)) {
      DatanodeStore store2 = db2.getStore();
      assertInstanceOf(AbstractDatanodeStore.class, store2);
      outProfile2 = ((AbstractDatanodeStore) store2).getDbProfile();
    }

    // DBOtions should be different, except SCHEMA-V3
    if (isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V3)) {
      assertEquals(
          outProfile1.getDBOptions().compactionReadaheadSize(),
          outProfile2.getDBOptions().compactionReadaheadSize());
    } else {
      assertNotEquals(
          outProfile1.getDBOptions().compactionReadaheadSize(),
          outProfile2.getDBOptions().compactionReadaheadSize());
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testKeyValueDataProtoBufMsg(ContainerTestVersionInfo versionInfo)
      throws Exception {
    init(versionInfo);
    createContainer();
    populate(10);
    closeContainer();
    ContainerProtos.ContainerDataProto proto =
        keyValueContainerData.getProtoBufMessage();

    assertEquals(keyValueContainerData.getContainerID(),
        proto.getContainerID());
    assertEquals(keyValueContainerData.getContainerType(),
        proto.getContainerType());
    assertEquals(keyValueContainerData.getContainerPath(),
        proto.getContainerPath());
    assertEquals(keyValueContainerData.getBlockCount(),
        proto.getBlockCount());
    assertEquals(keyValueContainerData.getBytesUsed(),
        proto.getBytesUsed());
    assertEquals(keyValueContainerData.getState(),
        proto.getState());

    for (ContainerProtos.KeyValue kv : proto.getMetadataList()) {
      assertEquals(keyValueContainerData.getMetadata().get(kv.getKey()),
          kv.getValue());
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  void testAutoCompactionSmallSstFile(
      ContainerTestVersionInfo versionInfo) throws Exception {
    init(versionInfo);
    assumeTrue(isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V3));
    // Create a new HDDS volume
    String volumeDirPath =
        Files.createDirectory(folder.toPath().resolve("volumeDir")).toFile()
            .getAbsolutePath();
    HddsVolume newVolume = new HddsVolume.Builder(volumeDirPath)
        .conf(CONF).datanodeUuid(datanodeId.toString()).build();
    StorageVolumeUtil.checkVolume(newVolume, scmId, scmId, CONF, null, null);
    List<HddsVolume> volumeList = new ArrayList<>();
    HddsVolume hddsVolume = hddsVolumes.get(0);
    volumeList.add(hddsVolume);
    volumeList.add(newVolume);

    long startContainerId =
        keyValueContainer.getContainerData().getContainerID() + 1;
    long containerId = startContainerId;

    // Create containers on each volume and export all container on new volume
    int count = 200;
    long numberOfKeysToWrite = 500;
    KeyValueContainerData containerData;
    KeyValueContainer container;
    List<File> exportFiles = new ArrayList<>();
    for (HddsVolume volume: volumeList) {
      reset(volumeChoosingPolicy);
      when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
          .thenReturn(volume);
      for (int index = 0; index < count; index++, containerId++) {
        // Create new container
        containerData = new KeyValueContainerData(containerId, layout,
            (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
            datanodeId.toString());
        container = new KeyValueContainer(containerData, CONF);
        container.create(volumeSet, volumeChoosingPolicy, scmId);
        containerData = container.getContainerData();
        containerData.setState(ContainerProtos.ContainerDataProto.State.CLOSED);
        populate(container, numberOfKeysToWrite);

        //destination path
        if (volume == newVolume) {
          File folderToExport =
              Files.createFile(
                  folder.toPath().resolve(containerId + "_exported.tar.gz")).toFile();
          TarContainerPacker packer = new TarContainerPacker(NO_COMPRESSION);
          //export the container
          try (OutputStream fos = Files.newOutputStream(folderToExport.toPath())) {
            container.exportContainerData(fos, packer);
          }
          exportFiles.add(folderToExport);
          // delete the original one
          container.delete();
        }
      }
    }

    //Import container one by one to old HDDS volume
    List<KeyValueContainer> containerList = new ArrayList<>();
    try {
      for (int index = 0; index < count; index++) {
        containerData =
            new KeyValueContainerData(containerId - count + index,
                keyValueContainerData.getLayoutVersion(),
                keyValueContainerData.getMaxSize(),
                UUID.randomUUID().toString(),
                datanodeId.toString());
        containerData.setSchemaVersion(schemaVersion);
        container = new KeyValueContainer(containerData, CONF);
        container.populatePathFields(scmId, hddsVolume);
        try (InputStream fis =
                 Files.newInputStream(exportFiles.get(index).toPath())) {
          TarContainerPacker packer = new TarContainerPacker(NO_COMPRESSION);
          container.importContainerData(fis, packer);
          containerList.add(container);
          assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED, container.getContainerData().getState());
        }
      }

      // Check sst files
      DatanodeStore dnStore = DatanodeStoreCache.getInstance().getDB(
          hddsVolume.getDbParentDir() + "/" + OzoneConsts.CONTAINER_DB_NAME,
              CONF).getStore();
      List<LiveFileMetaData> fileMetaDataList1 =
          ((RDBStore)(dnStore.getStore())).getDb().getLiveFilesMetaData();
      // When using Table.loadFromFile() in loadKVContainerData(),
      // there were as many SST files generated as the number of imported containers
      // After moving away from using Table.loadFromFile(), no SST files are generated unless the db is force flushed
      assertEquals(0, fileMetaDataList1.size());
      hddsVolume.compactDb();
      // Sleep a while to wait for compaction to complete
      Thread.sleep(7000);
      List<LiveFileMetaData> fileMetaDataList2 =
          ((RDBStore)(dnStore.getStore())).getDb().getLiveFilesMetaData();
      assertThat(fileMetaDataList2).hasSizeLessThanOrEqualTo(fileMetaDataList1.size());
    } finally {
      // clean up
      for (KeyValueContainer c : containerList) {
        File directory =
            new File(c.getContainerData().getContainerPath());
        FileUtils.deleteDirectory(directory);
      }
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testIsEmptyContainerStateWhileImport(
      ContainerTestVersionInfo versionInfo) throws Exception {
    init(versionInfo);
    long containerId = keyValueContainer.getContainerData().getContainerID();
    createContainer();
    long numberOfKeysToWrite = 1;
    closeContainer();
    populate(numberOfKeysToWrite);

    //destination path
    File folderToExport = Files.createFile(
        folder.toPath().resolve("export.tar")).toFile();
    for (CopyContainerCompression compr : CopyContainerCompression.values()) {
      TarContainerPacker packer = new TarContainerPacker(compr);

      //export the container
      try (OutputStream fos = Files.newOutputStream(folderToExport.toPath())) {
        keyValueContainer
            .exportContainerData(fos, packer);
      }

      //delete the original one
      KeyValueContainerUtil.removeContainer(
          keyValueContainer.getContainerData(), CONF);
      keyValueContainer.delete();

      //create a new one
      KeyValueContainerData containerData =
          new KeyValueContainerData(containerId,
              keyValueContainerData.getLayoutVersion(),
              keyValueContainerData.getMaxSize(), UUID.randomUUID().toString(),
              datanodeId.toString());
      containerData.setSchemaVersion(keyValueContainerData.getSchemaVersion());
      KeyValueContainer container = new KeyValueContainer(containerData, CONF);

      HddsVolume containerVolume = volumeChoosingPolicy.chooseVolume(
          StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList()), 1);

      container.populatePathFields(scmId, containerVolume);
      try (InputStream fis = Files.newInputStream(folderToExport.toPath())) {
        container.importContainerData(fis, packer);
      }

      // After import check whether isEmpty flag is false
      assertFalse(container.getContainerData().isEmpty());
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testIsEmptyContainerStateWhileImportWithoutBlock(
      ContainerTestVersionInfo versionInfo) throws Exception {
    init(versionInfo);
    long containerId = keyValueContainer.getContainerData().getContainerID();
    createContainer();
    long numberOfKeysToWrite = 1;
    closeContainer();
    populateWithoutBlock(keyValueContainer, numberOfKeysToWrite);

    //destination path
    File folderToExport = Files.createFile(
        folder.toPath().resolve("export.tar")).toFile();
    for (CopyContainerCompression compr : CopyContainerCompression.values()) {
      TarContainerPacker packer = new TarContainerPacker(compr);

      //export the container
      try (OutputStream fos = Files.newOutputStream(folderToExport.toPath())) {
        keyValueContainer
            .exportContainerData(fos, packer);
      }

      //delete the original one
      KeyValueContainerUtil.removeContainer(
          keyValueContainer.getContainerData(), CONF);
      keyValueContainer.delete();
      //create a new one
      KeyValueContainerData containerData =
          new KeyValueContainerData(containerId,
              keyValueContainerData.getLayoutVersion(),
              keyValueContainerData.getMaxSize(), UUID.randomUUID().toString(),
              datanodeId.toString());
      containerData.setSchemaVersion(keyValueContainerData.getSchemaVersion());
      KeyValueContainer container = new KeyValueContainer(containerData, CONF);

      HddsVolume containerVolume = volumeChoosingPolicy.chooseVolume(
          StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList()), 1);

      container.populatePathFields(scmId, containerVolume);
      try (InputStream fis = Files.newInputStream(folderToExport.toPath())) {
        container.importContainerData(fis, packer);
      }

      // After import check whether isEmpty flag is true
      // since there are no blocks in rocksdb
      assertTrue(container.getContainerData().isEmpty());
    }
  }

  /**
   * Test import schema V2 replica to V3 enabled HddsVolume.
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testImportV2ReplicaToV3HddsVolume(
      ContainerTestVersionInfo versionInfo) throws Exception {
    init(versionInfo);
    final String testDir = folder.getPath();
    testMixedSchemaImport(testDir, false);
  }

  /**
   * Test import schema V3 replica to V3 disabled HddsVolume.
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testImportV3ReplicaToV2HddsVolume(
      ContainerTestVersionInfo versionInfo) throws Exception {
    init(versionInfo);
    final String testDir = folder.getPath();
    testMixedSchemaImport(testDir, true);
  }

  private void testMixedSchemaImport(String dir,
      boolean schemaV3Enabled) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    final String dir1 = dir + (schemaV3Enabled ? "/v3" : "/v2");

    // create HddsVolume
    HddsVolume hddsVolume1 = new HddsVolume.Builder(dir1)
        .conf(conf).datanodeUuid(datanodeId.toString()).build();
    conf.setBoolean(CONTAINER_SCHEMA_V3_ENABLED, schemaV3Enabled);
    StorageVolumeUtil.checkVolume(hddsVolume1, scmId, scmId, conf, null, null);
    hddsVolumes.clear();
    hddsVolumes.add(hddsVolume1);

    // create container
    long containerId = 1;
    KeyValueContainerData data = new KeyValueContainerData(containerId,
        ContainerLayoutVersion.FILE_PER_BLOCK,
        ContainerTestHelper.CONTAINER_MAX_SIZE, UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
    KeyValueContainer container = new KeyValueContainer(data, conf);
    container.create(volumeSet, volumeChoosingPolicy, scmId);
    long pendingDeleteBlockCount = 20;
    try (DBHandle meta = BlockUtils.getDB(data, conf)) {
      Table<String, Long> metadataTable = meta.getStore().getMetadataTable();
      metadataTable.put(data.getPendingDeleteBlockCountKey(),
          pendingDeleteBlockCount);
      metadataTable.put(data.getPendingDeleteBlockBytesKey(),
          pendingDeleteBlockCount * 256);
    }
    container.close();

    // verify container schema
    if (schemaV3Enabled) {
      assertEquals(SCHEMA_V3,
          container.getContainerData().getSchemaVersion());
    } else {
      assertEquals(SCHEMA_V2,
          container.getContainerData().getSchemaVersion());
    }

    //export container
    TarContainerPacker packer = new TarContainerPacker(NO_COMPRESSION);
    File file1 = new File(dir1 + "/" + containerId);
    if (!file1.createNewFile()) {
      fail("Failed to create file " + file1.getAbsolutePath());
    }
    try (OutputStream fos = Files.newOutputStream(file1.toPath())) {
      container.exportContainerData(fos, packer);
    }

    // create new HddsVolume
    conf.setBoolean(CONTAINER_SCHEMA_V3_ENABLED, !schemaV3Enabled);
    final String dir2 = dir + (schemaV3Enabled ? "/v2" : "/v3");
    HddsVolume hddsVolume2 = new HddsVolume.Builder(dir2)
        .conf(conf).datanodeUuid(datanodeId.toString()).build();
    StorageVolumeUtil.checkVolume(hddsVolume2, scmId, scmId, conf, null, null);
    hddsVolumes.clear();
    hddsVolumes.add(hddsVolume2);

    // import container to new HddsVolume
    KeyValueContainer importedContainer = new KeyValueContainer(data, conf);
    importedContainer.populatePathFields(scmId, hddsVolume2);
    try (InputStream fio = Files.newInputStream(file1.toPath())) {
      importedContainer.importContainerData(fio, packer);
    }

    assertEquals(schemaV3Enabled ? SCHEMA_V3 : SCHEMA_V2,
        importedContainer.getContainerData().getSchemaVersion());
    assertEquals(pendingDeleteBlockCount,
        importedContainer.getContainerData().getNumPendingDeletionBlocks());
    assertEquals(pendingDeleteBlockCount * 256,
        importedContainer.getContainerData().getBlockPendingDeletionBytes());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testContainerCreationCommitSpaceReserve(
      ContainerTestVersionInfo versionInfo) throws Exception {
    init(versionInfo);
    keyValueContainerData = spy(keyValueContainerData);
    keyValueContainer = new KeyValueContainer(keyValueContainerData, CONF);
    keyValueContainer = spy(keyValueContainer);

    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);

    // verify that
    verify(volumeChoosingPolicy).chooseVolume(anyList(), anyLong()); // this would reserve commit space
    assertTrue(keyValueContainerData.isCommittedSpace());
  }
}
