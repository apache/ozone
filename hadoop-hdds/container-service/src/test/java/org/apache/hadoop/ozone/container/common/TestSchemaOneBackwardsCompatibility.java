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

package org.apache.hadoop.ozone.container.common;

import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.SchemaOneDeletedBlocksTable;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.yaml.snakeyaml.Yaml;

/**
 * Tests processing of containers written with DB schema version 1,
 * which stores all its data in the default RocksDB column family.
 * Newer schema version will use a different column family layout, but they
 * should still be able to read, delete data, and update metadata for schema
 * version 1 containers.
 * <p>
 * The functionality executed by these tests assumes that all containers will
 * have to be closed before an upgrade, meaning that containers written with
 * schema version 1 will only ever be encountered in their closed state.
 * <p>
 * For an example of a RocksDB instance written with schema version 1, see
 * {@link TestDB}, which is used by these tests to load a pre created schema
 * version 1 RocksDB instance from test resources.
 */
public class TestSchemaOneBackwardsCompatibility {
  private OzoneConfiguration conf;

  private File metadataDir;
  private File containerFile;

  @TempDir
  private Path tempFolder;

  private void initSchemaVersion(String schemaVersion) {
    this.conf = new OzoneConfiguration();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
  }

  private static Iterable<Object[]> schemaVersion() {
    return Arrays.asList(new Object[][]{
        {OzoneConsts.SCHEMA_V2},
        {OzoneConsts.SCHEMA_V3}
    });
  }

  private void setup(String schemaVersion) throws Exception {
    initSchemaVersion(schemaVersion);
    TestDB testDB = new TestDB();

    // Copy data to the temporary folder so it can be safely modified.
    File tempMetadataDir =
        Files.createDirectories(
            tempFolder.resolve(OzoneConsts.CONTAINER_META_PATH +
                "/" + TestDB.CONTAINER_ID)).toFile();
    FileUtils.copyDirectoryToDirectory(testDB.getDBDirectory(),
        tempMetadataDir);
    FileUtils.copyFileToDirectory(testDB.getContainerFile(), tempMetadataDir);

    metadataDir = tempMetadataDir;
    containerFile = new File(metadataDir, TestDB.CONTAINER_FILE_NAME);

    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
        metadataDir.getAbsolutePath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        metadataDir.getAbsolutePath());
  }

  @AfterEach
  public void cleanup() {
    BlockUtils.shutdownCache(conf);
  }

  /**
   * Because all tables in schema version one map back to the default table,
   * directly iterating any of the table instances should be forbidden.
   * Otherwise, the iterators for each table would read the entire default
   * table, return all database contents, and yield unexpected results.
   *
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("schemaVersion")
  public void testDirectTableIterationDisabled(String schemaVersion)
      throws Exception {
    setup(schemaVersion);
    try (DBHandle refCountedDB = BlockUtils.getDB(newKvData(), conf)) {
      DatanodeStore store = refCountedDB.getStore();

      assertTableIteratorUnsupported(store.getMetadataTable());
      assertTableIteratorUnsupported(store.getBlockDataTable());
      assertTableIteratorUnsupported(store.getDeletedBlocksTable());
    }
  }

  private void assertTableIteratorUnsupported(Table<?, ?> table) {
    assertThrows(UnsupportedOperationException.class, table::iterator);
  }

  /**
   * Counts the number of deleted, pending delete, and regular blocks in the
   * database, and checks that they match the expected values.
   * Also makes sure that internal prefixes used to manage data in the schema
   * one deleted blocks table are removed from keys in iterator results.
   *
   * @throws IOException
   */
  @ParameterizedTest
  @MethodSource("schemaVersion")
  public void testBlockIteration(String schemaVersion) throws Exception {
    setup(schemaVersion);
    KeyValueContainerData cData = newKvData();
    try (DBHandle refCountedDB = BlockUtils.getDB(cData, conf)) {
      assertEquals(TestDB.NUM_DELETED_BLOCKS,
          countDeletedBlocks(refCountedDB, cData));

      assertEquals(TestDB.NUM_PENDING_DELETION_BLOCKS,
          countDeletingBlocks(refCountedDB, cData));

      assertEquals(TestDB.KEY_COUNT - TestDB.NUM_PENDING_DELETION_BLOCKS,
          countUnprefixedBlocks(refCountedDB, cData));

      // Test that deleted block keys do not have a visible prefix when
      // iterating.
      final String prefix = SchemaOneDeletedBlocksTable.DELETED_KEY_PREFIX;
      Table<String, ChunkInfoList> deletedBlocksTable =
              refCountedDB.getStore().getDeletedBlocksTable();

      // Test rangeKVs.
      List<Table.KeyValue<String, ChunkInfoList>> deletedBlocks =
              deletedBlocksTable.getRangeKVs(cData.startKeyEmpty(), 100,
                  cData.containerPrefix());

      for (Table.KeyValue<String, ChunkInfoList> kv: deletedBlocks) {
        assertThat(kv.getKey()).doesNotContain(prefix);
      }

      // Test sequentialRangeKVs.
      deletedBlocks = deletedBlocksTable.getRangeKVs(cData.startKeyEmpty(),
          100, cData.containerPrefix());

      for (Table.KeyValue<String, ChunkInfoList> kv: deletedBlocks) {
        assertThat(kv.getKey()).doesNotContain(prefix);
      }
    }

  }

  /**
   * Tests reading of a container that was written in schema version 1, when
   * the container has metadata keys present.
   * The {@link KeyValueContainerUtil} will read these values to fill in a
   * {@link KeyValueContainerData} object.
   *
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("schemaVersion")
  public void testReadWithMetadata(String schemaVersion) throws Exception {
    setup(schemaVersion);
    checkContainerData(newKvData());
  }

  /**
   * Tests reading of a container that was written in schema version 1, when
   * the container has no metadata keys present.
   * The {@link KeyValueContainerUtil} will scan the blocks in the database
   * to fill these metadata values into the database and into a
   * {@link KeyValueContainerData} object.
   *
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("schemaVersion")
  public void testReadWithoutMetadata(String schemaVersion) throws Exception {
    // Delete metadata keys from our copy of the DB.
    // This simulates them not being there to start with.
    setup(schemaVersion);
    KeyValueContainerData cData = newKvData();
    try (DBHandle db = BlockUtils.getDB(cData, conf)) {
      Table<String, Long> metadataTable = db.getStore().getMetadataTable();

      metadataTable.delete(cData.getBlockCountKey());
      assertNull(metadataTable.get(cData.getBlockCountKey()));

      metadataTable.delete(cData.getBytesUsedKey());
      assertNull(metadataTable.get(cData.getBytesUsedKey()));

      metadataTable.delete(cData.getPendingDeleteBlockCountKey());
      assertNull(metadataTable.get(cData.getPendingDeleteBlockCountKey()));
    }

    // Create a new container data object, and fill in its metadata by
    // counting blocks from the database, since the metadata keys in the
    // database are now gone.
    checkContainerData(newKvData());
  }

  /**
   * Tests reading blocks marked for deletion from a container written in
   * schema version 1. Because the block deleting service both reads for
   * deleted blocks and deletes them, this test will modify its copy of the
   * database.
   */
  @ParameterizedTest
  @MethodSource("schemaVersion")
  public void testDelete(String schemaVersion) throws Exception {
    setup(schemaVersion);
    final long numBlocksToDelete = TestDB.NUM_PENDING_DELETION_BLOCKS;
    String datanodeUuid = UUID.randomUUID().toString();
    ContainerSet containerSet = makeContainerSet();
    VolumeSet volumeSet = new MutableVolumeSet(datanodeUuid, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    KeyValueHandler keyValueHandler =
        ContainerTestUtils.getKeyValueHandler(conf, datanodeUuid, containerSet, volumeSet);
    long initialTotalSpace = newKvData().getBytesUsed();
    long blockSpace = initialTotalSpace / TestDB.KEY_COUNT;

    runBlockDeletingService(keyValueHandler);

    GenericTestUtils.waitFor(() -> {
      try {
        return (newKvData().getBytesUsed() != initialTotalSpace);
      } catch (IOException ex) {
      }
      return false;
    }, 100, 3000);

    long currentTotalSpace = newKvData().getBytesUsed();
    long numberOfBlocksDeleted =
        (initialTotalSpace - currentTotalSpace) / blockSpace;

    // Expected values after blocks with #deleting# prefix in original DB are
    // deleted.
    final long expectedDeletingBlocks =
            TestDB.NUM_PENDING_DELETION_BLOCKS - numBlocksToDelete;
    final long expectedDeletedBlocks =
            TestDB.NUM_DELETED_BLOCKS + numBlocksToDelete;
    final long expectedRegularBlocks =
            TestDB.KEY_COUNT - numBlocksToDelete;

    KeyValueContainerData cData = newKvData();
    try (DBHandle refCountedDB = BlockUtils.getDB(cData, conf)) {
      // Test results via block iteration.

      assertEquals(expectedDeletingBlocks,
              countDeletingBlocks(refCountedDB, cData));
      assertEquals(expectedDeletedBlocks,
          TestDB.NUM_DELETED_BLOCKS + numberOfBlocksDeleted);
      assertEquals(expectedRegularBlocks,
              countUnprefixedBlocks(refCountedDB, cData));

      // Test table metadata.
      // Because the KeyValueHandler used for the block deleting service is
      // mocked, the bytes used will not be updated by a ChunkManager in the
      // KeyValueHandler. Therefore, this value is not checked.
      Table<String, Long> metadataTable =
              refCountedDB.getStore().getMetadataTable();
      assertEquals(expectedRegularBlocks + expectedDeletingBlocks,
              (long)metadataTable.get(cData.getBlockCountKey()));
    }
  }

  /**
   * Tests reading the chunk info saved from a block that was deleted from a
   * database in schema version one. Blocks deleted from schema version one
   * before the upgrade will have the block ID saved as their value. Trying
   * to retrieve this value as a {@link ChunkInfoList} should fail. Blocks
   * deleted from schema version one after the upgrade should have their
   * {@link ChunkInfoList} saved as the corresponding value in the deleted
   * blocks table. Reading these values should succeed.
   *
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("schemaVersion")
  public void testReadDeletedBlockChunkInfo(String schemaVersion)
      throws Exception {
    setup(schemaVersion);
    String datanodeUuid = UUID.randomUUID().toString();
    ContainerSet containerSet = makeContainerSet();
    VolumeSet volumeSet = new MutableVolumeSet(datanodeUuid, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    KeyValueHandler keyValueHandler =
        ContainerTestUtils.getKeyValueHandler(conf, datanodeUuid, containerSet, volumeSet);
    KeyValueContainerData cData = newKvData();
    try (DBHandle refCountedDB = BlockUtils.getDB(cData, conf)) {
      // Read blocks that were already deleted before the upgrade.
      List<Table.KeyValue<String, ChunkInfoList>> deletedBlocks =
              refCountedDB.getStore().getDeletedBlocksTable()
                  .getRangeKVs(cData.startKeyEmpty(), 100,
                      cData.containerPrefix());

      Set<String> preUpgradeBlocks = new HashSet<>();

      for (Table.KeyValue<String, ChunkInfoList> chunkListKV: deletedBlocks) {
        preUpgradeBlocks.add(chunkListKV.getKey());
        assertNull(chunkListKV.getValue());
      }

      assertEquals(TestDB.NUM_DELETED_BLOCKS, preUpgradeBlocks.size());

      long initialTotalSpace = newKvData().getBytesUsed();
      long blockSpace = initialTotalSpace / TestDB.KEY_COUNT;

      runBlockDeletingService(keyValueHandler);

      GenericTestUtils.waitFor(() -> {
        try {
          return (newKvData().getBytesUsed() != initialTotalSpace);
        } catch (IOException ex) {
        }
        return false;
      }, 100, 3000);

      long currentTotalSpace = newKvData().getBytesUsed();

      // After the block deleting service runs, get the number of
      // deleted blocks.
      long numberOfBlocksDeleted =
          (initialTotalSpace - currentTotalSpace) / blockSpace;

      // The blocks that were originally marked for deletion should now be
      // deleted.
      assertEquals(TestDB.NUM_PENDING_DELETION_BLOCKS,
          numberOfBlocksDeleted);
    }
  }

  @ParameterizedTest
  @MethodSource("schemaVersion")
  public void testReadBlockData(String schemaVersion) throws Exception {
    setup(schemaVersion);
    KeyValueContainerData cData = newKvData();
    try (DBHandle refCountedDB = BlockUtils.getDB(cData, conf)) {
      Table<String, BlockData> blockDataTable =
          refCountedDB.getStore().getBlockDataTable();

      // Test encoding keys and decoding database values.
      for (String blockID : TestDB.BLOCK_IDS) {
        String blockKey = cData.getBlockKey(Long.parseLong(blockID));
        BlockData blockData = blockDataTable.get(blockKey);
        assertEquals(Long.toString(blockData.getLocalID()), blockID);
      }

      // Test decoding keys from the database.
      List<Table.KeyValue<String, BlockData>> blockKeyValues =
          blockDataTable.getRangeKVs(cData.startKeyEmpty(), 100,
              cData.containerPrefix(), cData.getUnprefixedKeyFilter());

      List<String> decodedKeys = new ArrayList<>();

      for (Table.KeyValue<String, BlockData> blockDataKV :
          blockKeyValues) {
        decodedKeys.add(blockDataKV.getKey());
      }

      assertEquals(TestDB.BLOCK_IDS, decodedKeys);

      // Test reading blocks with block iterator.
      try (BlockIterator<BlockData> iter =
               refCountedDB.getStore().getBlockIterator(TestDB.CONTAINER_ID)) {

        List<String> iteratorBlockIDs = new ArrayList<>();

        while (iter.hasNext()) {
          long localID = iter.nextBlock().getBlockID().getLocalID();
          iteratorBlockIDs.add(Long.toString(localID));
        }

        assertEquals(TestDB.BLOCK_IDS, iteratorBlockIDs);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("schemaVersion")
  public void testReadDeletingBlockData(String schemaVersion) throws Exception {
    setup(schemaVersion);
    KeyValueContainerData cData = newKvData();
    try (DBHandle refCountedDB = BlockUtils.getDB(cData, conf)) {
      Table<String, BlockData> blockDataTable =
          refCountedDB.getStore().getBlockDataTable();

      for (String blockID : TestDB.DELETING_BLOCK_IDS) {
        String blockKey = cData.getDeletingBlockKey(
            Long.parseLong(blockID));
        BlockData blockData = blockDataTable.get(blockKey);
        assertEquals(Long.toString(blockData.getLocalID()), blockID);
      }

      // Test decoding keys from the database.
      List<Table.KeyValue<String, BlockData>> blockKeyValues =
          blockDataTable.getRangeKVs(cData.startKeyEmpty(), 100,
              cData.containerPrefix(), cData.getDeletingBlockKeyFilter());

      List<String> decodedKeys = new ArrayList<>();

      for (Table.KeyValue<String, BlockData> blockDataKV:
          blockKeyValues) {
        decodedKeys.add(blockDataKV.getKey());
      }

      // Apply the deleting prefix to the saved block IDs so we can compare
      // them to the retrieved keys.
      List<String> expectedKeys = TestDB.DELETING_BLOCK_IDS.stream()
          .map(key -> cData.getDeletingBlockKey(Long.parseLong(key)))
          .collect(Collectors.toList());

      assertEquals(expectedKeys, decodedKeys);

      // Test reading deleting blocks with block iterator.
      KeyPrefixFilter filter = cData.getDeletingBlockKeyFilter();

      try (BlockIterator<BlockData> iter =
              refCountedDB.getStore().getBlockIterator(TestDB.CONTAINER_ID,
                  filter)) {

        List<String> iteratorBlockIDs = new ArrayList<>();

        while (iter.hasNext()) {
          long localID = iter.nextBlock().getBlockID().getLocalID();
          iteratorBlockIDs.add(Long.toString(localID));
        }

        assertEquals(TestDB.DELETING_BLOCK_IDS, iteratorBlockIDs);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("schemaVersion")
  public void testReadMetadata(String schemaVersion) throws Exception {
    setup(schemaVersion);
    KeyValueContainerData cData = newKvData();
    try (DBHandle refCountedDB = BlockUtils.getDB(cData, conf)) {
      Table<String, Long> metadataTable =
          refCountedDB.getStore().getMetadataTable();

      assertEquals(TestDB.KEY_COUNT,
          metadataTable.get(cData.getBlockCountKey()).longValue());
      assertEquals(TestDB.BYTES_USED,
          metadataTable.get(cData.getBytesUsedKey()).longValue());
      assertEquals(TestDB.NUM_PENDING_DELETION_BLOCKS,
          metadataTable.get(cData.getPendingDeleteBlockCountKey())
              .longValue());
    }
  }

  @ParameterizedTest
  @MethodSource("schemaVersion")
  public void testReadDeletedBlocks(String schemaVersion) throws Exception {
    setup(schemaVersion);
    KeyValueContainerData cData = newKvData();
    try (DBHandle refCountedDB = BlockUtils.getDB(cData, conf)) {
      Table<String, ChunkInfoList> deletedBlocksTable =
          refCountedDB.getStore().getDeletedBlocksTable();

      for (String blockID : TestDB.DELETED_BLOCK_IDS) {
        // Since chunk info for deleted blocks was not stored in schema
        // version 1, there is no value to retrieve here.
        assertTrue(deletedBlocksTable.isExist(blockID));
      }

      // Test decoding keys from the database.
      List<Table.KeyValue<String, ChunkInfoList>> chunkInfoKeyValues =
          deletedBlocksTable.getRangeKVs(cData.startKeyEmpty(), 100,
              cData.containerPrefix());

      List<String> decodedKeys = new ArrayList<>();

      for (Table.KeyValue<String, ChunkInfoList> kv:
          chunkInfoKeyValues) {
        decodedKeys.add(kv.getKey());
      }

      assertEquals(TestDB.DELETED_BLOCK_IDS, decodedKeys);
    }
  }

  private void runBlockDeletingService(KeyValueHandler keyValueHandler)
      throws Exception {
    OzoneContainer container = makeMockOzoneContainer(keyValueHandler);

    BlockDeletingServiceTestImpl service =
        new BlockDeletingServiceTestImpl(container, 1000, conf);
    service.start();
    GenericTestUtils.waitFor(service::isStarted, 100, 3000);
    service.runDeletingTasks();
    GenericTestUtils
        .waitFor(() -> service.getTimesOfProcessed() == 1, 100, 3000);

  }

  private ContainerSet makeContainerSet() throws Exception {
    ContainerSet containerSet = newContainerSet();
    KeyValueContainer container = new KeyValueContainer(newKvData(), conf);
    containerSet.addContainer(container);

    return containerSet;
  }

  private OzoneContainer makeMockOzoneContainer(KeyValueHandler keyValueHandler)
      throws Exception {
    ContainerSet containerSet = makeContainerSet();

    OzoneContainer ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
    when(ozoneContainer.getWriteChannel()).thenReturn(null);
    ContainerDispatcher dispatcher = mock(ContainerDispatcher.class);
    when(ozoneContainer.getDispatcher()).thenReturn(dispatcher);
    when(dispatcher.getHandler(any())).thenReturn(keyValueHandler);

    return ozoneContainer;
  }

  /**
   * @return A {@link KeyValueContainerData} object that is read from
   * {@link TestSchemaOneBackwardsCompatibility#containerFile} constructed to
   * point to the instance of {@link TestDB} being used.
   * @throws IOException
   */
  private KeyValueContainerData newKvData() throws IOException {
    KeyValueContainerData kvData = (KeyValueContainerData)
        ContainerDataYaml.readContainerFile(containerFile);

    // Because the test DB is set up in a temp folder, we cannot know any
    // absolute paths to container components until run time.
    // For this reason, any path fields are omitted from the container file,
    // and added here.
    kvData.setMetadataPath(metadataDir.getAbsolutePath());
    kvData.setChunksPath(metadataDir.getAbsolutePath());

    // Changing the paths above affects the checksum, so it was also removed
    // from the container file and calculated at run time.
    Yaml yaml = ContainerDataYaml.getYamlForContainerType(
            kvData.getContainerType(),
        kvData.getReplicaIndex() > 0);
    kvData.computeAndSetContainerFileChecksum(yaml);

    KeyValueContainerUtil.parseKVContainerData(kvData, conf);

    return kvData;
  }

  /**
   * @param kvData The container data that will be tested to see if it has
   * metadata values matching those in the database under test.
   */
  private void checkContainerData(KeyValueContainerData kvData) {
    assertTrue(kvData.isClosed());
    assertEquals(TestDB.SCHEMA_VERSION, kvData.getSchemaVersion());
    assertEquals(TestDB.KEY_COUNT, kvData.getBlockCount());
    assertEquals(TestDB.BYTES_USED, kvData.getBytesUsed());
    assertEquals(TestDB.NUM_PENDING_DELETION_BLOCKS,
            kvData.getNumPendingDeletionBlocks());
  }

  private int countDeletedBlocks(DBHandle refCountedDB,
      KeyValueContainerData cData)
          throws IOException {
    return refCountedDB.getStore().getDeletedBlocksTable()
            .getRangeKVs(cData.startKeyEmpty(), 100,
                cData.containerPrefix(),
                cData.getUnprefixedKeyFilter()).size();
  }

  private int countDeletingBlocks(DBHandle refCountedDB,
      KeyValueContainerData cData)
          throws IOException {
    return refCountedDB.getStore().getBlockDataTable()
            .getRangeKVs(cData.startKeyEmpty(), 100,
                cData.containerPrefix(),
                cData.getDeletingBlockKeyFilter()).size();
  }

  private int countUnprefixedBlocks(DBHandle refCountedDB,
      KeyValueContainerData cData)
          throws IOException {
    return refCountedDB.getStore().getBlockDataTable()
            .getRangeKVs(cData.startKeyEmpty(), 100,
                cData.containerPrefix(),
                cData.getUnprefixedKeyFilter()).size();
  }

  /**
   * Holds information about the database used for testing by this class.
   * This database was generated by an old version of the code that wrote
   * data in schema version 1. The values are arbitrary. We only care that
   * it has keys representing metadata, deleted blocks, pending delete blocks,
   * and regular blocks.
   * <p>
   * The contents of the database are listed below. All values are present in
   * the default column family. String values are surrounded by double
   * quotes, protobuf objects are surrounded by angle brackets, and longs are
   * written literally.
   *
   * "#BLOCKCOUNT" : 4
   * "#BYTESUSED" : 400
   * "#PENDINGDELETEBLOCKCOUNT" : 2
   * "#deleted#1596029079371" : 1596029079371
   * "#deleted#1596029079374" : 1596029079374
   * "#deleting#1596029079378" : <block_data>
   * "#deleting#1596029079380" : <block_data>
   * 1596029079382 : <block_data>
   * 1596029079385 : <block_data>
   */
  private static class TestDB {
    // Non configurable properties of database.
    public static final long CONTAINER_ID = 123;
    public static final String CONTAINER_FILE_NAME =
        CONTAINER_ID + OzoneConsts.CONTAINER_EXTENSION;
    public static final String DB_NAME =
        CONTAINER_ID + OzoneConsts.DN_CONTAINER_DB;

    public static final String SCHEMA_VERSION = OzoneConsts.SCHEMA_V1;
    public static final long KEY_COUNT = 4;
    public static final long BYTES_USED = 400;
    public static final long NUM_PENDING_DELETION_BLOCKS = 2;
    public static final long NUM_DELETED_BLOCKS = 2;

    // All keys are stored as strings, since the codecs reading to the
    // database should handle conversion from long to string for keys before
    // data is returned to the caller.
    public static final List<String> DELETED_BLOCK_IDS = Arrays.asList(
        "1596029079371",
        "1596029079374");
    public static final List<String> DELETING_BLOCK_IDS = Arrays.asList(
        "1596029079378",
        "1596029079380");
    public static final List<String> BLOCK_IDS = Arrays.asList(
        "1596029079382",
        "1596029079385");

    private final ClassLoader loader;

    TestDB() {
      loader = getClass().getClassLoader();
    }

    public File getContainerFile() {
      return load(CONTAINER_FILE_NAME);
    }

    public File getDBDirectory() {
      return load(DB_NAME);
    }

    private File load(String name) {
      File file = null;
      URL url = loader.getResource(name);

      if (url != null) {
        file = new File(url.getFile());
      }

      return file;
    }
  }
}
