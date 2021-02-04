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

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;

import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume
    .RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.metadata.AbstractDatanodeStore;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyOptions;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.ratis.util.Preconditions.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
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

  private final ChunkLayOutVersion layout;

  // Use one configuration object across parameterized runs of tests.
  // This preserves the column family options in the container options
  // cache for testContainersShareColumnFamilyOptions.
  private static final OzoneConfiguration CONF = new OzoneConfiguration();

  public TestKeyValueContainer(ChunkLayOutVersion layout) {
    this.layout = layout;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return ChunkLayoutTestInfo.chunkLayoutParameters();
  }

  @Before
  public void setUp() throws Exception {
    datanodeId = UUID.randomUUID();
    HddsVolume hddsVolume = new HddsVolume.Builder(folder.getRoot()
        .getAbsolutePath()).conf(CONF).datanodeUuid(datanodeId
        .toString()).build();

    volumeSet = mock(MutableVolumeSet.class);
    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);
    Mockito.when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume);

    keyValueContainerData = new KeyValueContainerData(1L,
        layout,
        (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
        datanodeId.toString());

    keyValueContainer = new KeyValueContainer(keyValueContainerData, CONF);
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

  @Test
  public void testContainerImportExport() throws Exception {
    long containerId = keyValueContainer.getContainerData().getContainerID();
    createContainer();
    long numberOfKeysToWrite = 12;
    closeContainer();
    populate(numberOfKeysToWrite);

    //destination path
    File folderToExport = folder.newFile("exported.tar.gz");

    TarContainerPacker packer = new TarContainerPacker();

    //export the container
    try (FileOutputStream fos = new FileOutputStream(folderToExport)) {
      keyValueContainer
          .exportContainerData(fos, packer);
    }

    //delete the original one
    keyValueContainer.delete();

    //create a new one
    KeyValueContainerData containerData =
        new KeyValueContainerData(containerId,
            keyValueContainerData.getLayOutVersion(),
            keyValueContainerData.getMaxSize(), UUID.randomUUID().toString(),
            datanodeId.toString());
    KeyValueContainer container = new KeyValueContainer(containerData, CONF);

    HddsVolume containerVolume = volumeChoosingPolicy.chooseVolume(volumeSet
        .getVolumesList(), 1);
    String hddsVolumeDir = containerVolume.getHddsRootDir().toString();

    container.populatePathFields(scmId, containerVolume, hddsVolumeDir);
    try (FileInputStream fis = new FileInputStream(folderToExport)) {
      container.importContainerData(fis, packer);
    }

    assertEquals("value1", containerData.getMetadata().get("key1"));
    assertEquals(keyValueContainerData.getContainerDBType(),
        containerData.getContainerDBType());
    assertEquals(keyValueContainerData.getState(),
        containerData.getState());
    assertEquals(numberOfKeysToWrite,
        containerData.getKeyCount());
    assertEquals(keyValueContainerData.getLayOutVersion(),
        containerData.getLayOutVersion());
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
    }

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
  private void populate(long numberOfKeysToWrite) throws IOException {
    try (ReferenceCountedDB metadataStore =
        BlockUtils.getDB(keyValueContainer.getContainerData(), CONF)) {
      Table<String, BlockData> blockDataTable =
              metadataStore.getStore().getBlockDataTable();

      for (long i = 0; i < numberOfKeysToWrite; i++) {
        blockDataTable.put("test" + i, new BlockData(new BlockID(i, i)));
      }

      // As now when we put blocks, we increment block count and update in DB.
      // As for test, we are doing manually so adding key count to DB.
      metadataStore.getStore().getMetadataTable()
              .put(OzoneConsts.BLOCK_COUNT, numberOfKeysToWrite);
    }

    Map<String, String> metadata = new HashMap<>();
    metadata.put("key1", "value1");
    keyValueContainer.update(metadata, true);
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

    TarContainerPacker packer = new TarContainerPacker();
    List<Thread> threads = IntStream.range(0, 20)
        .mapToObj(i -> new Thread(() -> {
          try {
            File file = folder.newFile("concurrent" + i + ".tar.gz");
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
    keyValueContainer.delete();

    String containerMetaDataPath = keyValueContainerData
        .getMetadataPath();
    File containerMetaDataLoc = new File(containerMetaDataPath);

    assertFalse("Container directory still exists", containerMetaDataLoc
        .getParentFile().exists());

    assertFalse("Container File still exists",
        keyValueContainer.getContainerFile().exists());
    assertFalse("Container DB file still exists",
        keyValueContainer.getContainerDBFile().exists());
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
    Assert.assertNotNull(keyValueContainer.getContainerReport());
    keyValueContainer.markContainerUnhealthy();
    File containerFile = keyValueContainer.getContainerFile();
    keyValueContainerData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertEquals(ContainerProtos.ContainerDataProto.State.UNHEALTHY,
        keyValueContainerData.getState());
    Assert.assertNotNull(keyValueContainer.getContainerReport());
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
  public void testContainersShareColumnFamilyOptions() throws Exception {
    // Get a read only view (not a copy) of the options cache.
    Map<ConfigurationSource, ColumnFamilyOptions> cachedOptions =
        AbstractDatanodeStore.getColumnFamilyOptionsCache();

    // Create Container 1
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    Assert.assertEquals(1, cachedOptions.size());
    ColumnFamilyOptions options1 = cachedOptions.get(CONF);
    Assert.assertNotNull(options1);

    // Create Container 2
    keyValueContainerData = new KeyValueContainerData(2L,
        layout,
        (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
        datanodeId.toString());
    keyValueContainer = new KeyValueContainer(keyValueContainerData, CONF);
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);

    Assert.assertEquals(1, cachedOptions.size());
    ColumnFamilyOptions options2 = cachedOptions.get(CONF);
    Assert.assertNotNull(options2);

    // Column family options object should be reused.
    Assert.assertSame(options1, options2);
  }
}
