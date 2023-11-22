/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;

import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.utils.db.DatanodeDBProfile;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume
    .RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.metadata.AbstractDatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.replication.CopyContainerCompression;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.hadoop.util.DiskChecker;

import org.assertj.core.api.Fail;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.Assume;
import org.junit.jupiter.api.BeforeEach;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TemporaryFolder;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.LiveFileMetaData;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V2;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V3;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED;
import static org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil.isSameSchemaVersion;
import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;
import static org.apache.ratis.util.Preconditions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

/**
 * Class to test KeyValue Container operations.
 */
@RunWith(Parameterized.class)
public class TestKeyValueContainer {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private String scmId = UUID.randomUUID().toString();
  private VolumeSet volumeSet;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private KeyValueContainerData keyValueContainerData;
  private KeyValueContainer keyValueContainer;
  private UUID datanodeId;

  private final ContainerLayoutVersion layout;
  private String schemaVersion;
  private List<HddsVolume> hddsVolumes;

  // Use one configuration object across parameterized runs of tests.
  // This preserves the column family options in the container options
  // cache for testContainersShareColumnFamilyOptions.
  private static final OzoneConfiguration CONF = new OzoneConfiguration();

  public TestKeyValueContainer(ContainerTestVersionInfo versionInfo) {
    this.layout = versionInfo.getLayout();
    this.schemaVersion = versionInfo.getSchemaVersion();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, CONF);
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return ContainerTestVersionInfo.versionParameters();
  }

  @BeforeEach
  public void setUp() throws Exception {
    CodecBuffer.enableLeakDetection();

    DatanodeConfiguration dc = CONF.getObject(DatanodeConfiguration.class);
    dc.setAutoCompactionSmallSstFileNum(100);
    dc.setRocksdbDeleteObsoleteFilesPeriod(5000);
    CONF.setFromObject(dc);

    datanodeId = UUID.randomUUID();

    hddsVolumes = new ArrayList<>();

    hddsVolumes.add(new HddsVolume.Builder(folder.getRoot()
        .getAbsolutePath()).conf(CONF).datanodeUuid(datanodeId
        .toString()).build());
    StorageVolumeUtil.checkVolume(hddsVolumes.get(0), scmId, scmId, CONF,
        null, null);

    volumeSet = mock(MutableVolumeSet.class);
    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);
    Mockito.when(volumeSet.getVolumesList())
        .thenAnswer(i -> hddsVolumes.stream()
            .map(v -> (StorageVolume) v)
            .collect(Collectors.toList()));
    Mockito.when(volumeChoosingPolicy
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

  @Test
  public void testCreateContainer() throws Exception {
    createContainer();

    String containerMetaDataPath = keyValueContainerData.getMetadataPath();
    String chunksPath = keyValueContainerData.getChunksPath();

    // Check whether containerMetaDataPath and chunksPath exists or not.
    assertTrue(containerMetaDataPath != null);
    assertTrue(chunksPath != null);
    // Check whether container file and container db file exists or not.
    assertTrue(keyValueContainer.getContainerFile().exists(),
        ".Container File does not exist");
    assertTrue(keyValueContainer.getContainerDBFile().exists(), "Container " +
        "DB does not exist");
  }

  /**
   * Tests repair of containers affected by the bug reported in HDDS-6235.
   */
  @Test
  public void testMissingChunksDirCreated() throws Exception {
    // Create an empty container and delete its chunks directory.
    createContainer();
    closeContainer();
    // Sets the checksum.
    populate(0);
    KeyValueContainerData data = keyValueContainer.getContainerData();
    File chunksDir = new File(data.getChunksPath());
    Assertions.assertTrue(chunksDir.delete());

    // When the container is loaded, the missing chunks directory should
    // be created.
    KeyValueContainerUtil.parseKVContainerData(data, CONF);
    Assertions.assertTrue(chunksDir.exists());
  }

  @Test
  public void testNextVolumeTriedOnWriteFailure() throws Exception {
    HddsVolume newVolume = new HddsVolume.Builder(
        folder.newFolder().getAbsolutePath())
        .conf(CONF).datanodeUuid(datanodeId.toString()).build();
    StorageVolumeUtil.checkVolume(newVolume, scmId, scmId, CONF, null, null);
    hddsVolumes.add(newVolume);

    // Override the class, so that the first time we call it, it throws
    // simulating a disk or write failure. The second time it should be ok

    final AtomicInteger callCount = new AtomicInteger(0);
    keyValueContainer = new KeyValueContainer(keyValueContainerData, CONF)  {

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

  @Test
  public void testEmptyContainerImportExport() throws Exception {
    createContainer();
    closeContainer();

    KeyValueContainerData data = keyValueContainer.getContainerData();

    // Check state of original container.
    checkContainerFilesPresent(data, 0);

    //destination path
    File exportTar = folder.newFile("exported.tar");
    TarContainerPacker packer = new TarContainerPacker(NO_COMPRESSION);
    //export the container
    try (FileOutputStream fos = new FileOutputStream(exportTar)) {
      keyValueContainer.exportContainerData(fos, packer);
    }

    KeyValueContainerUtil.removeContainer(
        keyValueContainer.getContainerData(), CONF);
    keyValueContainer.delete();

    // import container.
    try (FileInputStream fis = new FileInputStream(exportTar)) {
      keyValueContainer.importContainerData(fis, packer);
    }

    // Make sure empty chunks dir was unpacked.
    checkContainerFilesPresent(data, 0);
  }

  @Test
  public void testUnhealthyContainerImportExport() throws Exception {
    createContainer();
    long numberOfKeysToWrite = 12;
    populate(numberOfKeysToWrite);
    keyValueContainerData.setState(
        ContainerProtos.ContainerDataProto.State.UNHEALTHY);
    KeyValueContainerData data = keyValueContainer.getContainerData();
    keyValueContainer.update(data.getMetadata(), true);

    //destination path
    File exportTar = folder.newFile("exported.tar");
    TarContainerPacker packer = new TarContainerPacker(NO_COMPRESSION);
    //export the container
    try (FileOutputStream fos = new FileOutputStream(exportTar)) {
      keyValueContainer.exportContainerData(fos, packer);
    }

    KeyValueContainerUtil.removeContainer(
        keyValueContainer.getContainerData(), CONF);
    keyValueContainer.delete();

    // import container.
    try (FileInputStream fis = new FileInputStream(exportTar)) {
      keyValueContainer.importContainerData(fis, packer);
    }

    // Re-load container data after import
    data = keyValueContainer.getContainerData();
    // Make sure empty chunks dir was unpacked.
    checkContainerFilesPresent(data, 0);
    // Ensure the unhealthy state is preserved
    Assertions.assertEquals(ContainerProtos.ContainerDataProto.State.UNHEALTHY,
        data.getState());
    assertEquals(numberOfKeysToWrite, keyValueContainerData.getBlockCount());
  }

  @Test
  public void testContainerImportExport() throws Exception {
    long containerId = keyValueContainer.getContainerData().getContainerID();
    createContainer();
    long numberOfKeysToWrite = 12;
    closeContainer();
    populate(numberOfKeysToWrite);

    //destination path
    File folderToExport = folder.newFile("exported.tar");
    for (CopyContainerCompression compr : CopyContainerCompression.values()) {
      TarContainerPacker packer = new TarContainerPacker(compr);

      //export the container
      try (FileOutputStream fos = new FileOutputStream(folderToExport)) {
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
      try (FileInputStream fis = new FileInputStream(folderToExport)) {
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

      //Can't overwrite existing container
      try {
        try (FileInputStream fis = new FileInputStream(folderToExport)) {
          container.importContainerData(fis, packer);
        }
        fail("Container is imported twice. Previous files are overwritten");
      } catch (IOException ex) {
        //all good
        assertTrue(container.getContainerFile().exists());
      }

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
      try {
        FileInputStream fis = new FileInputStream(folderToExport);
        fis.close();
        container.importContainerData(fis, packer);
        fail("Container import should fail");
      } catch (Exception ex) {
        assertTrue(ex instanceof IOException);
      } finally {
        File directory =
            new File(container.getContainerData().getContainerPath());
        assertFalse(directory.exists());
      }
    }
  }

  private void checkContainerFilesPresent(KeyValueContainerData data,
      long expectedNumFilesInChunksDir) throws IOException {
    File chunksDir = new File(data.getChunksPath());
    Assertions.assertTrue(Files.isDirectory(chunksDir.toPath()));
    try (Stream<Path> stream = Files.list(chunksDir.toPath())) {
      Assertions.assertEquals(expectedNumFilesInChunksDir, stream.count());
    }
    Assertions.assertTrue(data.getDbFile().exists());
    Assertions.assertTrue(KeyValueContainer.getContainerFile(data.getMetadataPath(),
        data.getContainerID()).exists());
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

  @Test
  public void concurrentExport() throws Exception {
    createContainer();
    populate(100);
    closeContainer();

    AtomicReference<String> failed = new AtomicReference<>();

    TarContainerPacker packer = new TarContainerPacker(NO_COMPRESSION);
    List<Thread> threads = IntStream.range(0, 20)
        .mapToObj(i -> new Thread(() -> {
          try {
            File file = folder.newFile("concurrent" + i + ".tar");
            try (OutputStream out = new FileOutputStream(file)) {
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

  @Test
  public void testDuplicateContainer() throws Exception {
    try {
      // Create Container.
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
      fail("testDuplicateContainer failed");
    } catch (StorageContainerException ex) {
      GenericTestUtils.assertExceptionContains("ContainerFile already " +
          "exists", ex);
      assertEquals(ContainerProtos.Result.CONTAINER_ALREADY_EXISTS, ex
          .getResult());
    }
  }

  @Test
  public void testDiskFullExceptionCreateContainer() throws Exception {

    Mockito.reset(volumeChoosingPolicy);
    Mockito.when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenThrow(DiskChecker.DiskOutOfSpaceException.class);
    try {
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
      fail("testDiskFullExceptionCreateContainer failed");
    } catch (StorageContainerException ex) {
      GenericTestUtils.assertExceptionContains("disk out of space",
          ex);
      assertEquals(ContainerProtos.Result.DISK_OUT_OF_SPACE, ex.getResult());
    }
  }

  @Test
  public void testDeleteContainer() throws Exception {
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

  @Test
  public void testCloseContainer() throws Exception {
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

  @Test
  public void testReportOfUnhealthyContainer() throws Exception {
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    Assertions.assertNotNull(keyValueContainer.getContainerReport());
    keyValueContainer.markContainerUnhealthy();
    File containerFile = keyValueContainer.getContainerFile();
    keyValueContainerData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertEquals(ContainerProtos.ContainerDataProto.State.UNHEALTHY,
        keyValueContainerData.getState());
    Assertions.assertNotNull(keyValueContainer.getContainerReport());
  }

  @Test
  public void testUpdateContainer() throws IOException {
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

  @Test
  public void testUpdateContainerUnsupportedRequest() throws Exception {
    try {
      closeContainer();
      keyValueContainer = new KeyValueContainer(keyValueContainerData, CONF);
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
      Map<String, String> metadata = new HashMap<>();
      metadata.put(OzoneConsts.VOLUME, OzoneConsts.OZONE);
      keyValueContainer.update(metadata, false);
      fail("testUpdateContainerUnsupportedRequest failed");
    } catch (StorageContainerException ex) {
      GenericTestUtils.assertExceptionContains("Updating a closed container " +
          "without force option is not allowed", ex);
      assertEquals(ContainerProtos.Result.UNSUPPORTED_REQUEST, ex
          .getResult());
    }
  }

  @Test
  public void testContainerRocksDB() throws IOException {
    closeContainer();
    keyValueContainer = new KeyValueContainer(
        keyValueContainerData, CONF);
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);

    try (DBHandle db = BlockUtils.getDB(keyValueContainerData, CONF)) {
      RDBStore store = (RDBStore) db.getStore().getStore();
      long defaultCacheSize = 64 * OzoneConsts.MB;
      long cacheSize = Long.parseLong(store
          .getProperty("rocksdb.block-cache-capacity"));
      Assertions.assertEquals(defaultCacheSize, cacheSize);
      for (ColumnFamily handle : store.getColumnFamilies()) {
        cacheSize = Long.parseLong(
            store.getProperty(handle, "rocksdb.block-cache-capacity"));
        Assertions.assertEquals(defaultCacheSize, cacheSize);
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
      Assertions.assertEquals(columnFamilyOptions1, columnFamilyOptions2);

      // ColumnFamilyOptions should be same when queried multiple times
      // for a particulat configuration
      columnFamilyOptions1 = dbProfileSupplier.get()
          .getColumnFamilyOptions(conf);
      columnFamilyOptions2 = dbProfileSupplier.get()
          .getColumnFamilyOptions(conf);
      Assertions.assertEquals(columnFamilyOptions1, columnFamilyOptions2);
    }

    // Make sure ColumnFamilyOptions are different for different db profile
    DatanodeDBProfile diskProfile = new DatanodeDBProfile.Disk();
    DatanodeDBProfile ssdProfile = new DatanodeDBProfile.SSD();
    Assertions.assertNotEquals(
        diskProfile.getColumnFamilyOptions(new OzoneConfiguration()),
        ssdProfile.getColumnFamilyOptions(new OzoneConfiguration()));
    Assertions.assertNotEquals(diskProfile.getColumnFamilyOptions(conf),
        ssdProfile.getColumnFamilyOptions(conf));
  }


  @Test
  public void testDBProfileAffectsDBOptions() throws Exception {
    // Create Container 1
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);

    DatanodeDBProfile outProfile1;
    try (DBHandle db1 =
        BlockUtils.getDB(keyValueContainer.getContainerData(), CONF)) {
      DatanodeStore store1 = db1.getStore();
      Assertions.assertTrue(store1 instanceof AbstractDatanodeStore);
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
      Assertions.assertTrue(store2 instanceof AbstractDatanodeStore);
      outProfile2 = ((AbstractDatanodeStore) store2).getDbProfile();
    }

    // DBOtions should be different, except SCHEMA-V3
    if (isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V3)) {
      Assertions.assertEquals(
          outProfile1.getDBOptions().compactionReadaheadSize(),
          outProfile2.getDBOptions().compactionReadaheadSize());
    } else {
      Assertions.assertNotEquals(
          outProfile1.getDBOptions().compactionReadaheadSize(),
          outProfile2.getDBOptions().compactionReadaheadSize());
    }
  }

  @Test
  public void testKeyValueDataProtoBufMsg() throws Exception {
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

  @Test
  public void testAutoCompactionSmallSstFile() throws IOException {
    Assume.assumeTrue(
        isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V3));
    // Create a new HDDS volume
    HddsVolume newVolume = new HddsVolume.Builder(
        folder.newFolder().getAbsolutePath())
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
      Mockito.reset(volumeChoosingPolicy);
      Mockito.when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
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
              folder.newFile(containerId + "_exported.tar.gz");
          TarContainerPacker packer = new TarContainerPacker(NO_COMPRESSION);
          //export the container
          try (FileOutputStream fos = new FileOutputStream(folderToExport)) {
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
        try (FileInputStream fis =
                 new FileInputStream(exportFiles.get(index))) {
          TarContainerPacker packer = new TarContainerPacker(NO_COMPRESSION);
          container.importContainerData(fis, packer);
          containerList.add(container);
        }
      }

      // Check sst files
      DatanodeStore dnStore = DatanodeStoreCache.getInstance().getDB(
          hddsVolume.getDbParentDir() + "/" + OzoneConsts.CONTAINER_DB_NAME,
              CONF).getStore();
      List<LiveFileMetaData> fileMetaDataList1 =
          ((RDBStore)(dnStore.getStore())).getDb().getLiveFilesMetaData();
      hddsVolume.check(true);
      // Sleep a while to wait for compaction to complete
      Thread.sleep(7000);
      List<LiveFileMetaData> fileMetaDataList2 =
          ((RDBStore)(dnStore.getStore())).getDb().getLiveFilesMetaData();
      Assertions.assertTrue(fileMetaDataList2.size() < fileMetaDataList1.size());
    } catch (Exception e) {
      Fail.fail("TestAutoCompactionSmallSstFile failed");
    } finally {
      // clean up
      for (KeyValueContainer c : containerList) {
        File directory =
            new File(c.getContainerData().getContainerPath());
        FileUtils.deleteDirectory(directory);
      }
    }
  }

  @Test
  public void testIsEmptyContainerStateWhileImport() throws Exception {
    long containerId = keyValueContainer.getContainerData().getContainerID();
    createContainer();
    long numberOfKeysToWrite = 1;
    closeContainer();
    populate(numberOfKeysToWrite);

    //destination path
    File folderToExport = folder.newFile("export.tar");
    for (CopyContainerCompression compr : CopyContainerCompression.values()) {
      TarContainerPacker packer = new TarContainerPacker(compr);

      //export the container
      try (FileOutputStream fos = new FileOutputStream(folderToExport)) {
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
      try (FileInputStream fis = new FileInputStream(folderToExport)) {
        container.importContainerData(fis, packer);
      }

      // After import check whether isEmpty flag is false
      Assertions.assertFalse(container.getContainerData().isEmpty());
    }
  }

  @Test
  public void testIsEmptyContainerStateWhileImportWithoutBlock()
      throws Exception {
    long containerId = keyValueContainer.getContainerData().getContainerID();
    createContainer();
    long numberOfKeysToWrite = 1;
    closeContainer();
    populateWithoutBlock(keyValueContainer, numberOfKeysToWrite);

    //destination path
    File folderToExport = folder.newFile("export.tar");
    for (CopyContainerCompression compr : CopyContainerCompression.values()) {
      TarContainerPacker packer = new TarContainerPacker(compr);

      //export the container
      try (FileOutputStream fos = new FileOutputStream(folderToExport)) {
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
      try (FileInputStream fis = new FileInputStream(folderToExport)) {
        container.importContainerData(fis, packer);
      }

      // After import check whether isEmpty flag is true
      // since there are no blocks in rocksdb
      Assertions.assertTrue(container.getContainerData().isEmpty());
    }
  }

  /**
   * Test import schema V2 replica to V3 enabled HddsVolume.
   */
  @Test
  public void testImportV2ReplicaToV3HddsVolume() throws Exception {
    final String testDir = GenericTestUtils.getTempPath(
        TestKeyValueContainer.class.getSimpleName() + "-"
            + UUID.randomUUID());
    try {
      testMixedSchemaImport(testDir, false);
    } finally {
      FileUtils.deleteDirectory(new File(testDir));
    }
  }

  /**
   * Test import schema V3 replica to V3 disabled HddsVolume.
   */
  @Test
  public void testImportV3ReplicaToV2HddsVolume() throws Exception {
    final String testDir = GenericTestUtils.getTempPath(
        TestKeyValueContainer.class.getSimpleName() + "-"
            + UUID.randomUUID());
    try {
      testMixedSchemaImport(testDir, true);
    } finally {
      FileUtils.deleteDirectory(new File(testDir));
    }
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
    }
    container.close();

    // verify container schema
    if (schemaV3Enabled) {
      Assertions.assertEquals(SCHEMA_V3,
          container.getContainerData().getSchemaVersion());
    } else {
      Assertions.assertEquals(SCHEMA_V2,
          container.getContainerData().getSchemaVersion());
    }

    //export container
    TarContainerPacker packer = new TarContainerPacker(NO_COMPRESSION);
    File file1 = new File(dir1 + "/" + containerId);
    if (!file1.createNewFile()) {
      Assertions.fail("Failed to create file " + file1.getAbsolutePath());
    }
    try (FileOutputStream fos = new FileOutputStream(file1)) {
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
    try (FileInputStream fio = new FileInputStream(file1)) {
      importedContainer.importContainerData(fio, packer);
    }

    Assertions.assertEquals(schemaV3Enabled ? SCHEMA_V3 : SCHEMA_V2,
        importedContainer.getContainerData().getSchemaVersion());
    Assertions.assertEquals(pendingDeleteBlockCount,
        importedContainer.getContainerData().getNumPendingDeletionBlocks());
  }
}
