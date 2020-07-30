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

package org.apache.hadoop.ozone.container.common;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.testutils.BlockDeletingServiceTestImpl;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests reading of containers written with DB schema version 1,
 * which stores all its data in the default RocksDB column family.
 * Newer schema version will use a different column family layout, but they
 * should still be able to read from schema version 1.
 */
public class TestSchemaOneBackwardsCompatibility {
  private OzoneConfiguration conf;

  private File metadataDir;
  private File dbFile;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    TestDB testDB = new TestDB();

    // Copy data to the temporary folder so it can be safely modified.
    File tempMetadataDir =
            tempFolder.newFolder(Long.toString(TestDB.CONTAINER_ID),
                    OzoneConsts.CONTAINER_META_PATH);

    FileUtils.copyDirectoryToDirectory(testDB.getDBDirectory(),
            tempMetadataDir);
    FileUtils.copyFileToDirectory(testDB.getContainerFile(), tempMetadataDir);

    metadataDir = tempMetadataDir;
    File[] potentialDBFiles = metadataDir.listFiles((dir, name) ->
            name.equals(TestDB.DB_NAME));

    if (potentialDBFiles == null || potentialDBFiles.length != 1) {
      throw new IOException("Failed load file named " + TestDB.DB_NAME + " " +
              "from the " + "metadata directory " +
              metadataDir.getAbsolutePath());
    }

    dbFile = potentialDBFiles[0];

    // Fix incorrect values.
//    RocksDB rocksDB = RocksDB.open(testDB.getDBDirectory().getAbsolutePath());
//    rocksDB.put(StringUtils.string2Bytes(OzoneConsts.CONTAINER_BYTES_USED),
//            Longs.toByteArray(200));
//    rocksDB.put(StringUtils.string2Bytes(
//            OzoneConsts.PENDING_DELETE_BLOCK_COUNT),
//            Longs.toByteArray(2));
//    rocksDB.close();
//    System.out.println('f');
  }

  /**
   * Counts the number of #deleted#, #deleting#, and unprefixed blocks in the
   * database, and checks that they match the expected values.
   * @throws IOException
   */
  @Test
  public void testBlockIteration() throws IOException {
    KeyValueContainerData kvData = newKvData();
    KeyValueContainerUtil.parseKVContainerData(kvData, conf);

    try(ReferenceCountedDB refCountedDB = BlockUtils.getDB(kvData, conf)) {
      assertEquals(TestDB.NUM_DELETED_BLOCKS, countDeletedBlocks(refCountedDB));

      assertEquals(TestDB.NUM_PENDING_DELETION_BLOCKS,
              countDeletingBlocks(refCountedDB));

      assertEquals(TestDB.KEY_COUNT,
              countUnprefixedBlocks(refCountedDB));
    }
  }

  /**
   * Tests reading of a container that was written in schema version 1, when
   * the container has metadata keys present.
   * The {@link KeyValueContainerUtil} will read these values to fill in a
   * {@link KeyValueContainerData} object.
   * @throws Exception
   */
  @Test
  public void testReadWithMetadata() throws Exception {
    KeyValueContainerData kvData = newKvData();
    KeyValueContainerUtil.parseKVContainerData(kvData, conf);
    checkContainerData(kvData);
  }

  /**
   * Tests reading of a container that was written in schema version 1, when
   * the container has no metadata keys present.
   * The {@link KeyValueContainerUtil} will scan the blocks in the database
   * to fill these metadata values into the database and into a
   * {@link KeyValueContainerData} object.
   * @throws Exception
   */
  @Test
  public void testReadWithoutMetadata() throws Exception {
    // Init the kvData enough values so we can get the database to modify for
    // testing and then read.
    KeyValueContainerData kvData = newKvData();
    KeyValueContainerUtil.parseKVContainerData(kvData, conf);

    // Delete metadata keys from our copy of the DB.
    // This simulates them not being there to start with.
    try (ReferenceCountedDB db = BlockUtils.getDB(kvData, conf)) {
      Table<String, Long> metadataTable = db.getStore().getMetadataTable();

      metadataTable.delete(OzoneConsts.BLOCK_COUNT);
      assertNull(metadataTable.get(OzoneConsts.BLOCK_COUNT));

      metadataTable.delete(OzoneConsts.CONTAINER_BYTES_USED);
      assertNull(metadataTable.get(OzoneConsts.CONTAINER_BYTES_USED));

      metadataTable.delete(OzoneConsts.PENDING_DELETE_BLOCK_COUNT);
      assertNull(metadataTable.get(OzoneConsts.PENDING_DELETE_BLOCK_COUNT));
    }

    // Create a new container data object, and fill in its metadata by
    // counting blocks from the database, since the metadata keys in the
    // database are now gone.
    kvData = newKvData();
    KeyValueContainerUtil.parseKVContainerData(kvData, conf);
    checkContainerData(kvData);
  }

  /**
   * Tests reading blocks marked for deletion from a container written in
   * schema version 1. Because the block deleting service both reads for
   * deleted blocks and deletes them, this test will modify its copy of the
   * database.
   */
  @Test
  public void testDelete() throws Exception {
    final int numBlocksToDelete = 2;

    runBlockDeletingService();

    try(ReferenceCountedDB refCountedDB = BlockUtils.getDB(newKvData(), conf)) {
      // Blocks marked with #deleting# prefix should be deleted.
      assertEquals(TestDB.NUM_PENDING_DELETION_BLOCKS - numBlocksToDelete,
              countDeletingBlocks(refCountedDB));
      assertEquals(TestDB.NUM_DELETED_BLOCKS + numBlocksToDelete,
              countDeletedBlocks(refCountedDB));

      // Regular blocks should remain unchanged.
      assertEquals(TestDB.KEY_COUNT, countUnprefixedBlocks(refCountedDB));

      // Since metadata is being stored in the same table, make sure it is not
      // altered as well.
      Table<String, Long> metadataTable =
              refCountedDB.getStore().getMetadataTable();
      assertEquals(TestDB.BYTES_USED,
              (long)metadataTable.get(OzoneConsts.CONTAINER_BYTES_USED));
      // TODO : Fix key count in KeyValueContainerUtil
      //  .initializeUsedBytesAndBlockCount() to include #deleting# blocks,
      //  and update the value stored in the DB as well. This will make this
      //  test pass.
//      assertEquals(TestDB.KEY_COUNT,
//              (long)metadataTable.get(OzoneConsts.BLOCK_COUNT));
    }
  }

  private void runBlockDeletingService() throws Exception {
    conf.setInt(OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL, 10);
    conf.setInt(OzoneConfigKeys.OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER, 2);
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
            metadataDir.getAbsolutePath());

    OzoneContainer container = makeMockOzoneContainer();

    BlockDeletingServiceTestImpl service =
            new BlockDeletingServiceTestImpl(container, 1000, conf);
    service.start();
    GenericTestUtils.waitFor(service::isStarted, 100, 3000);
    service.runDeletingTasks();
    GenericTestUtils.waitFor(() -> service.getTimesOfProcessed() == 1,
            100, 3000);
  }

  private ContainerSet makeContainerSet() throws Exception {
    KeyValueContainerData kvData = newKvData();
    KeyValueContainerUtil.parseKVContainerData(kvData, conf);

    ContainerSet containerSet = new ContainerSet();
    KeyValueContainer container = new KeyValueContainer(kvData, conf);
    containerSet.addContainer(container);

    return containerSet;
  }

  private OzoneContainer makeMockOzoneContainer() throws Exception {
    ContainerSet containerSet = makeContainerSet();

    OzoneContainer ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
    when(ozoneContainer.getWriteChannel()).thenReturn(null);
    ContainerDispatcher dispatcher = mock(ContainerDispatcher.class);
    when(ozoneContainer.getDispatcher()).thenReturn(dispatcher);
    KeyValueHandler handler = mock(KeyValueHandler.class);
    when(dispatcher.getHandler(any())).thenReturn(handler);

    return ozoneContainer;
  }

  /**
   * @return A {@link KeyValueContainerData} object that has only its
   * metadata path, db file, and checksum set to match the database under test.
   * @throws IOException
   */
  private KeyValueContainerData newKvData() throws IOException {
    ChunkLayOutVersion clVersion =
            ChunkLayOutVersion.getChunkLayOutVersion(1);
    String pipelineID = UUID.randomUUID().toString();
    String nodeID = UUID.randomUUID().toString();

    KeyValueContainerData kvData = new KeyValueContainerData(
            TestDB.CONTAINER_ID, clVersion,
            ContainerTestHelper.CONTAINER_MAX_SIZE, pipelineID, nodeID);
    kvData.setMetadataPath(metadataDir.getAbsolutePath());
    kvData.setDbFile(dbFile);

    kvData.closeContainer();

    // This must be set to some directory for the block deleting service to run.
    kvData.setChunksPath(metadataDir.getAbsolutePath());

    Yaml yaml = ContainerDataYaml.getYamlForContainerType(
            kvData.getContainerType());
    kvData.computeAndSetChecksum(yaml);

    return kvData;
  }

  /**
   * @param kvData The container data that will be tested to see if it has
   * metadata values matching those in the database under test.
   */
  private void checkContainerData(KeyValueContainerData kvData) {
    assertEquals(TestDB.SCHEMA_VERSION, kvData.getSchemaVersion());
    assertEquals(TestDB.KEY_COUNT, kvData.getKeyCount());
    assertEquals(TestDB.BYTES_USED, kvData.getBytesUsed());
    assertEquals(TestDB.NUM_PENDING_DELETION_BLOCKS,
            kvData.getNumPendingDeletionBlocks());
  }

  private int countDeletedBlocks(ReferenceCountedDB refCountedDB)
          throws IOException {
    return refCountedDB.getStore().getDeletedBlocksTable()
            .getRangeKVs(null, 100).size();
  }

  private int countDeletingBlocks(ReferenceCountedDB refCountedDB)
          throws IOException {
    return refCountedDB.getStore().getBlockDataTable()
            .getRangeKVs(null, 100,
                    MetadataKeyFilters.getDeletingKeyFilter()).size();
  }

  private int countUnprefixedBlocks(ReferenceCountedDB refCountedDB)
          throws IOException {
    return refCountedDB.getStore().getBlockDataTable()
            .getRangeKVs(null, 100,
                    MetadataKeyFilters.getNormalKeyFilter()).size();
  }

  /**
   * Holds information about the database used for testing by this class.
   * This database was generated by an old version of the code that wrote
   * data in schema version 1. The values are arbitrary. We only care that
   * it has keys representing metadata, deleted blocks, deleting blocks, and
   * regular blocks.
   *
   * The contents of the database are:
   *
   * #BLOCKCOUNT : 2
   * #BYTESUSED : 200
   * #PENDINGDELETEBLOCKCOUNT : 2
   * #deleted#1596029079371 : 1596029079371
   * #deleted#1596029079374 : 1596029079374
   * #deleting#1596029079378 : <block_data>
   * #deleting#1596029079380 : <block_data>
   * 1596029079382 : <block_data>
   * 1596029079385 : <block_data>
   */
  private static class TestDB {
    // Non configurable properties of the files.
    public static final long CONTAINER_ID = 123;
    public static final String CONTAINER_FILE_NAME =
            CONTAINER_ID + ".container";
    public static final String DB_NAME = CONTAINER_ID + "-dn-container.db";

    public static final String SCHEMA_VERSION = OzoneConsts.SCHEMA_V1;
    public static final long KEY_COUNT = 2;
    public static final long BYTES_USED = 200;
    public static final long NUM_PENDING_DELETION_BLOCKS = 2;
    public static final long NUM_DELETED_BLOCKS = 2;

    private final ClassLoader loader;

    TestDB() {
      loader = getClass().getClassLoader();
    }

    private File getContainerFile() {
      return load(CONTAINER_FILE_NAME);
    }

    private File getDBDirectory() {
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
