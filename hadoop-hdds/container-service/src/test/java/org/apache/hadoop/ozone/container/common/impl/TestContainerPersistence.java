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

package org.apache.hadoop.ozone.container.common.impl;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.BCSID_MISMATCH;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNKNOWN_BCSID;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getChunk;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getData;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.setDataChecksum;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil.isSameSchemaVersion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.ChunkBufferToByteString;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.keyvalue.impl.BlockManagerImpl;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerFactory;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple tests to verify that container persistence works as expected. Some of
 * these tests are specific to {@link KeyValueContainer}. If a new {@link
 * ContainerProtos.ContainerType} is added, the tests need to be modified.
 */
public class TestContainerPersistence {
  private static final String DATANODE_UUID = UUID.randomUUID().toString();
  private static final String SCM_ID = UUID.randomUUID().toString();
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TestContainerPersistence.class);
  @TempDir
  private static File hddsFile;
  private static String hddsPath;
  private static OzoneConfiguration conf;
  private static VolumeChoosingPolicy volumeChoosingPolicy;

  private ContainerSet containerSet;
  private MutableVolumeSet volumeSet;
  private BlockManager blockManager;
  private ChunkManager chunkManager;

  /**
   * Set the timeout for every test.
   */

  private ContainerLayoutVersion layout;
  private String schemaVersion;

  private void initSchemaAndVersionInfo(ContainerTestVersionInfo versionInfo) {
    this.layout = versionInfo.getLayout();
    this.schemaVersion = versionInfo.getSchemaVersion();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
  }

  @BeforeAll
  public static void init() {
    conf = new OzoneConfiguration();
    hddsPath = hddsFile.getPath();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, hddsPath);
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, hddsPath);
    volumeChoosingPolicy = new RoundRobinVolumeChoosingPolicy();
  }

  @AfterAll
  public static void shutdown() throws IOException {
    FileUtils.deleteDirectory(new File(hddsPath));
  }

  @BeforeEach
  public void setupPaths() throws IOException {
    containerSet = newContainerSet();
    volumeSet = new MutableVolumeSet(DATANODE_UUID, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    // Initialize volume directories.
    for (HddsVolume volume : StorageVolumeUtil.getHddsVolumesList(
        volumeSet.getVolumesList())) {
      boolean success = StorageVolumeUtil.checkVolume(volume, SCM_ID, SCM_ID,
          conf, null, null);
      assertTrue(success);
    }
    blockManager = new BlockManagerImpl(conf);
    chunkManager = ChunkManagerFactory.createChunkManager(conf, blockManager,
        null);

    for (String dir : conf.getStrings(ScmConfigKeys.HDDS_DATANODE_DIR_KEY)) {
      StorageLocation location = StorageLocation.parse(dir);
      FileUtils.forceMkdir(new File(location.getNormalizedUri()));
    }
  }

  @AfterEach
  public void cleanupDir() throws IOException {
    // Cleanup cache
    BlockUtils.shutdownCache(conf);

    // Clean up SCM metadata
    LOGGER.info("Deleting {}", hddsPath);
    FileUtils.deleteDirectory(new File(hddsPath));

    // Clean up SCM datanode container metadata/data
    for (String dir : conf.getStrings(ScmConfigKeys.HDDS_DATANODE_DIR_KEY)) {
      StorageLocation location = StorageLocation.parse(dir);
      FileUtils.deleteDirectory(new File(location.getNormalizedUri()));
    }
  }

  private long getTestContainerID() {
    return ContainerTestHelper.getTestContainerID();
  }

  private KeyValueContainer addContainer(ContainerSet cSet, long cID)
      throws IOException {
    long commitBytesBefore = 0;
    long commitBytesAfter = 0;
    long commitIncrement = 0;
    KeyValueContainerData data = new KeyValueContainerData(cID,
        layout,
        ContainerTestHelper.CONTAINER_MAX_SIZE, UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    KeyValueContainer container = new KeyValueContainer(data, conf);
    commitBytesBefore = StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList()).get(0).getCommittedBytes();

    container.create(volumeSet, volumeChoosingPolicy, SCM_ID);
    cSet.addContainer(container);

    commitBytesAfter = container.getContainerData()
        .getVolume().getCommittedBytes();
    commitIncrement = commitBytesAfter - commitBytesBefore;
    // did we commit space for the new container?
    assertEquals(commitIncrement,
        ContainerTestHelper.CONTAINER_MAX_SIZE);
    return container;
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testCreateContainer(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initSchemaAndVersionInfo(versionInfo);
    long testContainerID = getTestContainerID();
    addContainer(containerSet, testContainerID);
    assertThat(containerSet.getContainerMapCopy()).containsKey(testContainerID);
    KeyValueContainerData kvData =
        (KeyValueContainerData) containerSet.getContainer(testContainerID)
            .getContainerData();

    assertNotNull(kvData);
    assertTrue(new File(kvData.getMetadataPath()).exists());
    assertTrue(new File(kvData.getChunksPath()).exists());
    assertTrue(kvData.getDbFile().exists());

    Path meta = kvData.getDbFile().toPath().getParent();
    assertTrue(meta != null && Files.exists(meta));

    try (DBHandle store = BlockUtils.getDB(kvData, conf)) {
      assertNotNull(store);
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testCreateDuplicateContainer(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initSchemaAndVersionInfo(versionInfo);
    long testContainerID = getTestContainerID();

    Container container = addContainer(containerSet, testContainerID);
    IOException ex = assertThrows(IOException.class, () -> containerSet.addContainer(container));
    assertNotNull(ex);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testAddingBlockToDeletedContainer(
      ContainerTestVersionInfo versionInfo) throws Exception {
    initSchemaAndVersionInfo(versionInfo);
    // With schema v3, we don't have a container dedicated db,
    // so skip check the behaviors related to it.
    assumeFalse(isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V3));

    long testContainerID = getTestContainerID();
    Thread.sleep(100);

    Container<KeyValueContainerData> container =
        addContainer(containerSet, testContainerID);
    container.close();

    assertThat(containerSet.getContainerMapCopy())
        .containsKey(testContainerID);

    KeyValueContainerUtil.removeContainer(container.getContainerData(), conf);
    container.delete();
    containerSet.removeContainer(testContainerID);
    assertThat(containerSet.getContainerMapCopy()).doesNotContainKey(testContainerID);

    // Adding block to a deleted container should fail.
    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    BlockData someKey = new BlockData(blockID);
    someKey.setChunks(new LinkedList<>());
    Exception exception = assertThrows(StorageContainerException.class,
        () -> blockManager.putBlock(container, someKey));
    assertThat(exception.getMessage())
        .contains("Error opening DB.");
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testDeleteNonEmptyContainer(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initSchemaAndVersionInfo(versionInfo);
    long testContainerID = getTestContainerID();
    Container<KeyValueContainerData> container =
        addContainer(containerSet, testContainerID);
    container.close();

    assertThat(containerSet.getContainerMapCopy())
        .containsKey(testContainerID);

    // Deleting a non-empty container should fail.
    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    BlockData someKey = new BlockData(blockID);
    someKey.setChunks(new LinkedList<>());
    blockManager.putBlock(container, someKey);

    // KeyValueHandler setup
    String datanodeId = UUID.randomUUID().toString();

    ContainerMetrics metrics = ContainerMetrics.create(conf);
    AtomicInteger icrReceived = new AtomicInteger(0);

    KeyValueHandler kvHandler = new KeyValueHandler(conf,
        datanodeId, containerSet, volumeSet, metrics,
        c -> icrReceived.incrementAndGet(), new ContainerChecksumTreeManager(conf));

    Exception exception = assertThrows(
        StorageContainerException.class,
        () -> kvHandler.deleteContainer(container, false));
    assertThat(containerSet.getContainerMapCopy())
        .containsKey(testContainerID);
    assertThat(exception.getMessage())
        .contains("Non-force deletion of non-empty container is not allowed.");
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testDeleteContainer(ContainerTestVersionInfo versionInfo)
      throws IOException {
    initSchemaAndVersionInfo(versionInfo);
    assumeTrue(isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V3));
    long testContainerID = getTestContainerID();
    Container<KeyValueContainerData> container = addContainer(containerSet,
        testContainerID);
    BlockID blockID = addBlockToContainer(container);
    container.close();
    assertThat(containerSet.getContainerMapCopy())
        .containsKey(testContainerID);
    KeyValueContainerData containerData = container.getContainerData();

    // Block data and metadata tables should have data.
    assertContainerInSchema3DB(containerData, blockID);

    KeyValueContainerUtil.removeContainerDB(
        container.getContainerData(),
        container.getContainerData().getVolume().getConf());
    container.delete();
    containerSet.removeContainer(testContainerID);
    assertThat(containerSet.getContainerMapCopy())
        .doesNotContainKey(testContainerID);

    // Block data and metadata tables should be cleared.
    assertContainerNotInSchema3DB(containerData, blockID);
  }

  private void assertContainerInSchema3DB(KeyValueContainerData containerData,
      BlockID testBlock) throws IOException {
    // This test is only valid for schema v3. Earlier schemas will have their
    // own RocksDB that is deleted with the container.
    if (!containerData.hasSchema(OzoneConsts.SCHEMA_V3)) {
      return;
    }
    try (DBHandle dbHandle = BlockUtils.getDB(containerData, conf)) {
      DatanodeStoreSchemaThreeImpl store = (DatanodeStoreSchemaThreeImpl)
          dbHandle.getStore();
      Table<String, BlockData> blockTable = store.getBlockDataTable();
      Table<String, Long> metadataTable = store.getMetadataTable();

      // Block data and metadata tables should have data.
      assertNotNull(blockTable
          .getIfExist(containerData.getBlockKey(testBlock.getLocalID())));
      assertNotNull(metadataTable
          .getIfExist(containerData.getBlockCountKey()));
    }
  }

  private void assertContainerNotInSchema3DB(
      KeyValueContainerData containerData, BlockID testBlock)
      throws IOException {
    // This test is only valid for schema v3. Earlier schemas will have their
    // own RocksDB that is deleted with the container.
    if (!containerData.hasSchema(OzoneConsts.SCHEMA_V3)) {
      return;
    }
    try (DBHandle dbHandle = BlockUtils.getDB(containerData, conf)) {
      DatanodeStoreSchemaThreeImpl store = (DatanodeStoreSchemaThreeImpl)
          dbHandle.getStore();
      Table<String, BlockData> blockTable = store.getBlockDataTable();
      Table<String, Long> metadataTable = store.getMetadataTable();

      // Block data and metadata tables should have data.
      assertNull(blockTable
          .getIfExist(containerData.getBlockKey(testBlock.getLocalID())));
      assertNull(metadataTable
          .getIfExist(containerData.getBlockCountKey()));
    }
  }

  private BlockID addBlockToContainer(
      Container<KeyValueContainerData> container) throws IOException {
    BlockID blockID = ContainerTestHelper.getTestBlockID(
            container.getContainerData().getContainerID());
    ChunkInfo info = writeChunkHelper(blockID);
    BlockData blockData = new BlockData(blockID);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    blockData.setChunks(chunkList);
    blockManager.putBlock(container, blockData);

    return blockID;
  }

  /**
   * If SchemaV3 is enabled, HddsVolume has already been
   * formatted and initialized in
   * setupPaths#createDbInstancesForTestIfNeeded.
   *
   * @throws Exception
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testDeleteContainerWithRenaming(
      ContainerTestVersionInfo versionInfo) throws Exception {
    initSchemaAndVersionInfo(versionInfo);
    // Set up container 1.
    long testContainerID1 = getTestContainerID();
    Container<KeyValueContainerData> container1 =
        addContainer(containerSet, testContainerID1);
    BlockID container1Block = addBlockToContainer(container1);
    container1.close();
    KeyValueContainerData container1Data = container1.getContainerData();
    assertContainerInSchema3DB(container1Data, container1Block);

    // Set up container 2.
    long testContainerID2 = getTestContainerID();
    Container<KeyValueContainerData> container2 =
        addContainer(containerSet, testContainerID2);
    BlockID container2Block = addBlockToContainer(container2);
    container2.close();
    KeyValueContainerData container2Data = container2.getContainerData();
    assertContainerInSchema3DB(container2Data, container2Block);

    assertThat(containerSet.getContainerMapCopy())
        .containsKey(testContainerID1);
    assertThat(containerSet.getContainerMapCopy())
        .containsKey(testContainerID2);

    // Since this test only uses one volume, both containers will reside in
    // the same volume.
    HddsVolume hddsVolume = container1Data.getVolume();
    // Volume setup should have created the tmp directory for container
    // deletion.
    File volumeTmpDir = hddsVolume.getTmpDir();
    assertTrue(volumeTmpDir.exists(), String.format("Volume level " +
        "tmp dir %s not created.", volumeTmpDir));
    File deletedContainerDir = hddsVolume.getDeletedContainerDir();
    assertTrue(deletedContainerDir.exists(),
        String.format("Volume level container deleted directory" +
            " %s not created.", deletedContainerDir));

    // Move containers to delete directory. RocksDB should not yet be updated.
    KeyValueContainerUtil.moveToDeletedContainerDir(container1Data, hddsVolume);
    assertContainerInSchema3DB(container1Data, container1Block);
    KeyValueContainerUtil.moveToDeletedContainerDir(container2Data, hddsVolume);
    assertContainerInSchema3DB(container2Data, container2Block);

    // Both containers should be present in the deleted directory.
    File[] deleteDirFilesArray = deletedContainerDir.listFiles();
    assertNotNull(deleteDirFilesArray);
    Set<File> deleteDirFiles = Arrays.stream(deleteDirFilesArray)
        .collect(Collectors.toSet());
    assertEquals(2, deleteDirFiles.size());

    File container1Dir = KeyValueContainerUtil.getTmpDirectoryPath(
        container1Data, hddsVolume).toFile();
    assertThat(deleteDirFiles).contains(container1Dir);
    File container2Dir = KeyValueContainerUtil.getTmpDirectoryPath(
        container2Data, hddsVolume).toFile();
    assertThat(deleteDirFiles).contains(container2Dir);

    // Delete container1 from the disk. Container2 should remain in the
    // deleted containers directory.
    KeyValueContainerUtil.removeContainerDB(
        container1.getContainerData(),
        container1.getContainerData().getVolume().getConf());
    container1.delete();
    assertContainerNotInSchema3DB(container1Data, container1Block);
    assertContainerInSchema3DB(container2Data, container2Block);

    // Check the delete directory again. Only container 2 should remain.
    deleteDirFilesArray = deletedContainerDir.listFiles();
    assertNotNull(deleteDirFilesArray);
    assertEquals(1, deleteDirFilesArray.length);
    assertEquals(deleteDirFilesArray[0], container2Dir);

    // Delete container2 from the disk.
    KeyValueContainerUtil.removeContainerDB(
        container2.getContainerData(),
        container2.getContainerData().getVolume().getConf());
    container2.delete();
    assertContainerNotInSchema3DB(container1Data, container1Block);
    assertContainerNotInSchema3DB(container2Data, container2Block);

    // Remove containers from containerSet
    containerSet.removeContainer(testContainerID1);
    containerSet.removeContainer(testContainerID2);
    assertThat(containerSet.getContainerMapCopy())
        .doesNotContainKey(testContainerID1);
    assertThat(containerSet.getContainerMapCopy())
        .doesNotContainKey(testContainerID2);

    // Deleted containers directory should now be empty.
    deleteDirFilesArray = deletedContainerDir.listFiles();
    assertNotNull(deleteDirFilesArray);
    assertEquals(0, deleteDirFilesArray.length);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testGetContainerReports(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initSchemaAndVersionInfo(versionInfo);
    final int count = 10;
    List<Long> containerIDs = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      long testContainerID = getTestContainerID();
      Container container = addContainer(containerSet, testContainerID);

      // Close a bunch of containers.
      if (i % 3 == 0) {
        container.close();
      }
      containerIDs.add(testContainerID);
    }

    // ContainerSet#getContainerReport currently returns all containers (open
    // and closed) reports.
    List<StorageContainerDatanodeProtocolProtos.ContainerReplicaProto> reports =
        containerSet.getContainerReport().getReportsList();
    assertEquals(10, reports.size());
    for (StorageContainerDatanodeProtocolProtos.ContainerReplicaProto report :
        reports) {
      long actualContainerID = report.getContainerID();
      assertTrue(containerIDs.remove(actualContainerID));
    }
    assertThat(containerIDs).isEmpty();
  }

  /**
   * This test creates 50 containers and reads them back 5 containers at a time
   * and verifies that we did get back all containers.
   *
   * @throws IOException
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testListContainer(ContainerTestVersionInfo versionInfo)
      throws IOException {
    initSchemaAndVersionInfo(versionInfo);
    final int count = 10;
    final int step = 5;

    Map<Long, ContainerData> testMap = new HashMap<>();
    for (int x = 0; x < count; x++) {
      long testContainerID = getTestContainerID();
      Container container = addContainer(containerSet, testContainerID);
      testMap.put(testContainerID, container.getContainerData());
    }

    int counter = 0;
    long prevKey = 0;
    List<ContainerData> results = new LinkedList<>();
    while (counter < count) {
      containerSet.listContainer(prevKey, step, results);
      for (ContainerData result : results) {
        testMap.remove(result.getContainerID());
      }
      counter += step;
      long nextKey = results.get(results.size() - 1).getContainerID();

      //Assert that container is returning results in a sorted fashion.
      assertThat(prevKey).isLessThan(nextKey);
      prevKey = nextKey + 1;
      results.clear();
    }
    // Assert that we listed all the keys that we had put into
    // container.
    assertThat(testMap).isEmpty();
  }

  private ChunkInfo writeChunkHelper(BlockID blockID) throws IOException {
    final int datalen = 1024;
    long commitBytesBefore = 0;
    long commitBytesAfter = 0;
    long commitDecrement = 0;
    long testContainerID = blockID.getContainerID();
    Container container = containerSet.getContainer(testContainerID);
    if (container == null) {
      container = addContainer(containerSet, testContainerID);
    }
    ChunkInfo info = getChunk(
        blockID.getLocalID(), 0, 0, datalen);
    ChunkBuffer data = getData(datalen);
    setDataChecksum(info, data);
    commitBytesBefore = container.getContainerData()
        .getVolume().getCommittedBytes();
    chunkManager.writeChunk(container, blockID, info, data,
        DispatcherContext.getHandleWriteChunk());
    commitBytesAfter = container.getContainerData()
        .getVolume().getCommittedBytes();
    commitDecrement = commitBytesBefore - commitBytesAfter;
    // did we decrement commit bytes by the amount of data we wrote?
    assertEquals(commitDecrement, info.getLen());
    return info;

  }

  /**
   * Writes a single chunk.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testWriteChunk(ContainerTestVersionInfo versionInfo)
      throws IOException {
    initSchemaAndVersionInfo(versionInfo);
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(getTestContainerID());
    writeChunkHelper(blockID);
  }

  /**
   * Tests that committed space is correctly decremented when a write fits entirely within the available space under
   * max container size. It should be decremented by the number of bytes written.
   *
   * Before write: Container size is less than max size
   * After write: Container size is still less than max size
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testIncrWriteBytesDecrementsCommittedWhenWriteFits(ContainerTestVersionInfo versionInfo)
      throws IOException {
    initSchemaAndVersionInfo(versionInfo);
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(getTestContainerID());
    long testContainerID = blockID.getContainerID();
    Container container = containerSet.getContainer(testContainerID);
    if (container == null) {
      container = addContainer(containerSet, testContainerID);
    }

    long commitBytesBefore = container.getContainerData().getVolume().getCommittedBytes();
    long writeBytes = 256 * OzoneConsts.MB;
    container.getContainerData().updateWriteStats(writeBytes, false);
    long commitBytesAfter = container.getContainerData().getVolume().getCommittedBytes();
    long commitDecrement = commitBytesBefore - commitBytesAfter;
    // did we decrement commit bytes by the amount of data we wrote?
    assertEquals(writeBytes, commitDecrement);
  }

  /**
   * Tests the scenario where a write operation causes the container usage to exceed max container size.
   * The committed space should only be decremented by the amount of the write that fit within the max limit.
   *
   * Before Write: Container size is within max size
   * After Write: Container size exceeds max size
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testIncrWriteBytesDecrementsCommittedCorrectlyWhenWriteExceedsMax(ContainerTestVersionInfo versionInfo)
      throws IOException {
    initSchemaAndVersionInfo(versionInfo);
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(getTestContainerID());
    long testContainerID = blockID.getContainerID();
    Container container = containerSet.getContainer(testContainerID);
    if (container == null) {
      container = addContainer(containerSet, testContainerID);
    }

    // fill the container close to the max size first
    long writeBytes = container.getContainerData().getMaxSize() - 100;
    container.getContainerData().updateWriteStats(writeBytes, false);

    // the next write will make the container size exceed max size
    long commitBytesBefore = container.getContainerData().getVolume().getCommittedBytes();
    writeBytes = 256 * OzoneConsts.MB;
    container.getContainerData().updateWriteStats(writeBytes, false);
    long commitBytesAfter = container.getContainerData().getVolume().getCommittedBytes();
    long commitDecrement = commitBytesBefore - commitBytesAfter;
    // did we decrement commit bytes by the amount that was remaining, ie, 100 bytes?
    assertEquals(100, commitDecrement);
  }

  /**
   * Tests that committed space is not decremented if the container was already
   * full (or overfull) before the write operation.
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testIncrWriteBytesDoesNotDecrementCommittedWhenContainerFull(ContainerTestVersionInfo versionInfo)
      throws IOException {
    initSchemaAndVersionInfo(versionInfo);
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(getTestContainerID());
    long testContainerID = blockID.getContainerID();
    Container container = containerSet.getContainer(testContainerID);
    if (container == null) {
      container = addContainer(containerSet, testContainerID);
    }

    // fill the container completely first
    long writeBytes = container.getContainerData().getMaxSize();
    container.getContainerData().updateWriteStats(writeBytes, false);

    // the next write will make the container size exceed max size
    long commitBytesBefore = container.getContainerData().getVolume().getCommittedBytes();
    writeBytes = 256 * OzoneConsts.MB;
    container.getContainerData().updateWriteStats(writeBytes, false);
    long commitBytesAfter = container.getContainerData().getVolume().getCommittedBytes();
    long commitDecrement = commitBytesBefore - commitBytesAfter;
    // decrement should be 0, as the container was already full before the write
    assertEquals(0, commitDecrement);
  }

  /**
   * In this test, the write fills the container exactly to max size. Committed space should be decremented by the
   * amount of bytes written.
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testIncrWriteBytesDecrementsCommittedCorrectlyWhenWrittenToBrim(ContainerTestVersionInfo versionInfo)
      throws IOException {
    initSchemaAndVersionInfo(versionInfo);
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(getTestContainerID());
    long testContainerID = blockID.getContainerID();
    Container container = containerSet.getContainer(testContainerID);
    if (container == null) {
      container = addContainer(containerSet, testContainerID);
    }

    // fill the container completely first
    long writeBytes = container.getContainerData().getMaxSize() - 256 * OzoneConsts.MB;
    container.getContainerData().updateWriteStats(writeBytes, false);

    // the next write will make the container size exactly equal to max size
    long commitBytesBefore = container.getContainerData().getVolume().getCommittedBytes();
    writeBytes = 256 * OzoneConsts.MB;
    container.getContainerData().updateWriteStats(writeBytes, false);
    long commitBytesAfter = container.getContainerData().getVolume().getCommittedBytes();
    long commitDecrement = commitBytesBefore - commitBytesAfter;
    // decrement should be writeBytes, as the write does not exceed max size
    assertEquals(writeBytes, commitDecrement);
  }

  /**
   * Commited space should not change when committedSpace is set to false.
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testIncrWriteBytesDoesNotChangeCommittedSpaceWhenItsDisabled(ContainerTestVersionInfo versionInfo)
      throws IOException {
    initSchemaAndVersionInfo(versionInfo);
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(getTestContainerID());
    long testContainerID = blockID.getContainerID();
    Container container = containerSet.getContainer(testContainerID);
    if (container == null) {
      container = addContainer(containerSet, testContainerID);
    }

    // For setup, we need to set committedSpace to false first
    container.getContainerData().setCommittedSpace(false);
    long commitBytesBefore = container.getContainerData().getVolume().getCommittedBytes();
    long writeBytes = 256 * OzoneConsts.MB;
    container.getContainerData().updateWriteStats(writeBytes, false);
    long commitBytesAfter = container.getContainerData().getVolume().getCommittedBytes();
    long commitDecrement = commitBytesBefore - commitBytesAfter;
    // decrement should be 0, as commit space is disabled for this container
    assertEquals(0, commitDecrement);
  }

  /**
     * Writes many chunks of the same block into different chunk files and
     * verifies that we have that data in many files.
     *
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
  @ContainerTestVersionInfo.ContainerTest
  public void testWritReadManyChunks(ContainerTestVersionInfo versionInfo)
      throws IOException {
    initSchemaAndVersionInfo(versionInfo);
    final int datalen = 1024;
    final int chunkCount = 1024;

    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);

    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    List<ChunkInfo> chunks = new ArrayList<>(chunkCount);
    BlockData blockData = new BlockData(blockID);
    for (int x = 0; x < chunkCount; x++) {
      ChunkInfo info = getChunk(blockID.getLocalID(), x, x * datalen, datalen);
      ChunkBuffer data = getData(datalen);
      setDataChecksum(info, data);
      chunkManager.writeChunk(container, blockID, info, data,
          DispatcherContext.getHandleWriteChunk());
      chunks.add(info);
      blockData.addChunk(info.getProtoBufMessage());
    }
    blockManager.putBlock(container, blockData);

    KeyValueContainerData cNewData =
        (KeyValueContainerData) container.getContainerData();
    assertNotNull(cNewData);

    // Read chunk via file system and verify.
    Checksum checksum = new Checksum(ChecksumType.CRC32, 1024 * 1024);

    // Read chunk via ReadChunk call.
    for (int x = 0; x < chunkCount; x++) {
      ChunkInfo info = chunks.get(x);
      final ChunkBufferToByteString data = chunkManager.readChunk(container, blockID, info,
          DispatcherContext.getHandleReadChunk());
      ChecksumData checksumData = checksum.computeChecksum(Collections.singletonList(data.toByteString()));
      assertEquals(info.getChecksumData(), checksumData);
    }
  }

  /**
   * Writes a single chunk and tries to overwrite that chunk without over write
   * flag then re-tries with overwrite flag.
   *
   * @throws IOException
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testOverWrite(ContainerTestVersionInfo versionInfo)
      throws IOException {
    initSchemaAndVersionInfo(versionInfo);
    final int datalen = 1024;

    long testContainerID = getTestContainerID();
    KeyValueContainer container = addContainer(containerSet, testContainerID);

    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    ChunkInfo info = getChunk(
        blockID.getLocalID(), 0, 0, datalen);
    ChunkBuffer data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(container, blockID, info, data,
        DispatcherContext.getHandleWriteChunk());
    data.rewind();
    chunkManager.writeChunk(container, blockID, info, data,
        DispatcherContext.getHandleWriteChunk());
    data.rewind();
    // With the overwrite flag it should work now.
    info.addMetadata(OzoneConsts.CHUNK_OVERWRITE, "true");
    chunkManager.writeChunk(container, blockID, info, data,
        DispatcherContext.getHandleWriteChunk());
    final ContainerData.Statistics statistics = container.getContainerData().getStatistics();
    statistics.assertWrite(datalen * 3, 3);
    statistics.assertBlock(datalen, 0, 0);
  }

  /**
   * Writes a chunk and deletes it, re-reads to make sure it is gone.
   *
   * @throws IOException
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testDeleteChunk(ContainerTestVersionInfo versionInfo)
      throws IOException {
    initSchemaAndVersionInfo(versionInfo);
    final int datalen = 1024;
    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);

    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    ChunkInfo info = getChunk(
        blockID.getLocalID(), 1, 0, datalen);
    ChunkBuffer data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(container, blockID, info, data,
        DispatcherContext.getHandleWriteChunk());
    chunkManager.deleteChunk(container, blockID, info);
    assertThrows(StorageContainerException.class,
        () -> chunkManager.readChunk(
            container, blockID, info, DispatcherContext.getHandleReadChunk()));
  }

  /**
   * Tests a put block and read block.
   *
   * @throws IOException
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testPutBlock(ContainerTestVersionInfo versionInfo)
      throws IOException {
    initSchemaAndVersionInfo(versionInfo);
    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);

    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    ChunkInfo info = writeChunkHelper(blockID);
    BlockData blockData = new BlockData(blockID);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    blockData.setChunks(chunkList);
    blockManager.putBlock(container, blockData);
    BlockData readBlockData = blockManager.
        getBlock(container, blockData.getBlockID());
    ChunkInfo readChunk =
        ChunkInfo.getFromProtoBuf(readBlockData.getChunks().get(0));
    assertEquals(info.getChecksumData(), readChunk.getChecksumData());
  }

  /**
   * Tests a put block and read block with invalid bcsId.
   *
   * @throws IOException
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testPutBlockWithInvalidBCSId(ContainerTestVersionInfo versionInfo)
      throws IOException {
    initSchemaAndVersionInfo(versionInfo);
    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);

    BlockID blockID1 = ContainerTestHelper.getTestBlockID(testContainerID);
    ChunkInfo info = writeChunkHelper(blockID1);
    BlockData blockData = new BlockData(blockID1);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    blockData.setChunks(chunkList);
    blockData.setBlockCommitSequenceId(3);
    blockManager.putBlock(container, blockData);
    chunkList.clear();

    // write a 2nd block
    BlockID blockID2 = ContainerTestHelper.getTestBlockID(testContainerID);
    info = writeChunkHelper(blockID2);
    blockData = new BlockData(blockID2);
    chunkList.add(info.getProtoBufMessage());
    blockData.setChunks(chunkList);
    blockData.setBlockCommitSequenceId(4);
    blockManager.putBlock(container, blockData);
    BlockData readBlockData;
    StorageContainerException sce = assertThrows(StorageContainerException.class, () -> {
      blockID1.setBlockCommitSequenceId(5);
      // read with bcsId higher than container bcsId
      blockManager.
          getBlock(container, blockID1);
    });
    assertSame(UNKNOWN_BCSID, sce.getResult());

    sce = assertThrows(StorageContainerException.class, () -> {
      blockID1.setBlockCommitSequenceId(4);
      // read with bcsId lower than container bcsId but greater than committed
      // bcsId.
      blockManager.
          getBlock(container, blockID1);
    });
    assertSame(BCSID_MISMATCH, sce.getResult());

    readBlockData = blockManager.
        getBlock(container, blockData.getBlockID());
    ChunkInfo readChunk =
        ChunkInfo.getFromProtoBuf(readBlockData.getChunks().get(0));
    assertEquals(info.getChecksumData(), readChunk.getChecksumData());
  }

  /**
   * Tests a put block and read block.
   *
   * @throws IOException
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testPutBlockWithLotsOfChunks(ContainerTestVersionInfo versionInfo)
      throws IOException {
    initSchemaAndVersionInfo(versionInfo);
    final int chunkCount = 2;
    final int datalen = 1024;
    long totalSize = 0L;
    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);
    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    List<ChunkInfo> chunkList = new LinkedList<>();
    for (int x = 0; x < chunkCount; x++) {
      ChunkInfo info = new ChunkInfo(String.format("%d.data",
          blockID.getLocalID()), x * datalen, datalen);
      ChunkBuffer data = getData(datalen);
      setDataChecksum(info, data);
      chunkManager.writeChunk(container, blockID, info, data,
          DispatcherContext.getHandleWriteChunk());
      totalSize += datalen;
      chunkList.add(info);
    }

    final ContainerData.Statistics statistics = container.getContainerData().getStatistics();
    statistics.assertRead(0, 0);
    statistics.assertWrite(chunkCount * datalen, chunkCount);
    statistics.assertBlock(totalSize, 0, 0);

    BlockData blockData = new BlockData(blockID);
    List<ContainerProtos.ChunkInfo> chunkProtoList = new LinkedList<>();
    for (ChunkInfo i : chunkList) {
      chunkProtoList.add(i.getProtoBufMessage());
    }
    blockData.setChunks(chunkProtoList);
    blockManager.putBlock(container, blockData);
    BlockData readBlockData = blockManager.
        getBlock(container, blockData.getBlockID());
    ChunkInfo lastChunk = chunkList.get(chunkList.size() - 1);
    ChunkInfo readChunk =
        ChunkInfo.getFromProtoBuf(readBlockData.getChunks().get(readBlockData
            .getChunks().size() - 1));
    assertEquals(
        lastChunk.getChecksumData(), readChunk.getChecksumData());
  }

  /**
   * Tries to update an existing and non-existing container. Verifies container
   * map and persistent data both updated.
   *
   * @throws IOException
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testUpdateContainer(ContainerTestVersionInfo versionInfo)
      throws IOException {
    initSchemaAndVersionInfo(versionInfo);
    long testContainerID = ContainerTestHelper.getTestContainerID();
    KeyValueContainer container =
        (KeyValueContainer) addContainer(containerSet, testContainerID);

    File orgContainerFile = container.getContainerFile();
    assertTrue(orgContainerFile.exists());

    Map<String, String> newMetadata = Maps.newHashMap();
    newMetadata.put("VOLUME", "shire_new");
    newMetadata.put("owner", "bilbo_new");

    container.update(newMetadata, false);

    assertEquals(1, containerSet.getContainerMapCopy().size());
    assertThat(containerSet.getContainerMapCopy())
        .containsKey(testContainerID);

    // Verify in-memory map
    KeyValueContainerData actualNewData = (KeyValueContainerData)
        containerSet.getContainer(testContainerID).getContainerData();
    assertEquals("shire_new",
        actualNewData.getMetadata().get("VOLUME"));
    assertEquals("bilbo_new",
        actualNewData.getMetadata().get("owner"));

    // Verify container data on disk
    File containerBaseDir = new File(actualNewData.getMetadataPath())
        .getParentFile();
    File newContainerFile = ContainerUtils.getContainerFile(containerBaseDir);
    assertTrue(newContainerFile.exists(),
        "Container file should exist.");
    assertEquals(
        orgContainerFile.getAbsolutePath(), newContainerFile.getAbsolutePath(),
        "Container file should be in same location.");

    ContainerData actualContainerData = ContainerDataYaml.readContainerFile(
        newContainerFile);
    assertEquals("shire_new",
        actualContainerData.getMetadata().get("VOLUME"));
    assertEquals("bilbo_new",
        actualContainerData.getMetadata().get("owner"));


    // Test force update flag.
    // Close the container and then try to update without force update flag.
    container.close();
    StorageContainerException exception = assertThrows(StorageContainerException.class,
        () -> container.update(newMetadata, false));
    assertThat(exception).hasMessageContaining(container.getContainerData().toString());


    // Update with force flag, it should be success.
    newMetadata.put("VOLUME", "shire_new_1");
    newMetadata.put("owner", "bilbo_new_1");
    container.update(newMetadata, true);

    // Verify in-memory map
    actualNewData = (KeyValueContainerData)
        containerSet.getContainer(testContainerID).getContainerData();
    assertEquals("shire_new_1",
        actualNewData.getMetadata().get("VOLUME"));
    assertEquals("bilbo_new_1",
        actualNewData.getMetadata().get("owner"));

  }

  private BlockData writeBlockHelper(BlockID blockID, int i)
      throws IOException {
    ChunkInfo info = writeChunkHelper(blockID);
    BlockData blockData = new BlockData(blockID);
    blockData.setBlockCommitSequenceId(i);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    blockData.setChunks(chunkList);
    return blockData;
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testListBlock(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initSchemaAndVersionInfo(versionInfo);
    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);
    List<BlockID> expectedBlocks = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      BlockID blockID = new BlockID(testContainerID, i);
      expectedBlocks.add(blockID);
      BlockData kd = writeBlockHelper(blockID, i);
      blockManager.putBlock(container, kd);
    }

    // List all blocks
    List<BlockData> result = blockManager.listBlock(
        container, 0, 100);
    assertEquals(10, result.size());

    int index = 0;
    for (int i = index; i < result.size(); i++) {
      BlockData data = result.get(i);
      assertEquals(testContainerID, data.getContainerID());
      assertEquals(expectedBlocks.get(i).getLocalID(),
          data.getLocalID());
      index++;
    }

    // List block with startBlock filter
    long k6 = expectedBlocks.get(6).getLocalID();
    result = blockManager.listBlock(container, k6, 100);

    assertEquals(4, result.size());
    for (int i = 6; i < 10; i++) {
      assertEquals(expectedBlocks.get(i).getLocalID(),
          result.get(i - 6).getLocalID());
    }

    // Count must be >0
    Exception exception = assertThrows(IllegalArgumentException.class,
        () -> blockManager.listBlock(container, 0, -1));
    assertThat(exception.getMessage())
        .contains("Count must be a positive number.");
  }
}
