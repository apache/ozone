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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.secure;
import static org.apache.hadoop.ozone.OzoneConsts.BLOCK_COUNT;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_BYTES_USED;
import static org.apache.hadoop.ozone.OzoneConsts.PENDING_DELETE_BLOCK_COUNT;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.COMMIT_STAGE;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.WRITE_STAGE;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.impl.BlockManagerImpl;
import org.apache.hadoop.ozone.container.keyvalue.impl.FilePerBlockStrategy;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaTwoImpl;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests processing of containers written with DB schema version 2,
 * which stores all its data in a one-db-per-container layout.
 * Schema version 3 will use a one-db-per-disk layout, but it
 * should still be able to read, delete data, and update metadata for schema
 * version 2 containers.
 * We have a switch "hdds.datanode.container.schema.v3.enabled", so we could
 * create a test container with it off and turn it on later to
 * test the container operations above.
 * <p>
 * The functionality executed by these tests assumes that all containers will
 * have to be closed before an upgrade, meaning that containers written with
 * schema version 2 will only ever be encountered in their closed state.
 * <p>
 */
public class TestSchemaTwoBackwardsCompatibility {

  private OzoneConfiguration conf;
  private String clusterID;
  private String datanodeUuid;
  @TempDir
  private File testRoot;
  private MutableVolumeSet volumeSet;
  private BlockManager blockManager;
  private ChunkManager chunkManager;
  private ContainerSet containerSet;
  private OzoneContainer ozoneContainer;

  private static final int BLOCKS_PER_CONTAINER = 6;
  private static final int CHUNKS_PER_BLOCK = 2;
  private static final int DELETE_TXNS_PER_CONTAINER = 2;
  private static final int BLOCKS_PER_TXN = 2;
  private static final int CHUNK_LENGTH = 1024;
  private static final byte[] SAMPLE_DATA =
      secure().nextAlphanumeric(1024).getBytes(UTF_8);

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();

    clusterID = UUID.randomUUID().toString();
    datanodeUuid = UUID.randomUUID().toString();

    // turn off schemaV3 first
    ContainerTestUtils.disableSchemaV3(conf);
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, testRoot.getAbsolutePath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testRoot.getAbsolutePath());

    volumeSet = new MutableVolumeSet(datanodeUuid, clusterID, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);

    blockManager = new BlockManagerImpl(conf);
    chunkManager = new FilePerBlockStrategy(true, blockManager);

    containerSet = newContainerSet();
    KeyValueHandler keyValueHandler =
        ContainerTestUtils.getKeyValueHandler(conf, datanodeUuid, containerSet, volumeSet);
    ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
    when(ozoneContainer.getWriteChannel()).thenReturn(null);
    ContainerDispatcher dispatcher = mock(ContainerDispatcher.class);
    when(ozoneContainer.getDispatcher()).thenReturn(dispatcher);
    when(dispatcher.getHandler(any())).thenReturn(keyValueHandler);
  }

  @AfterEach
  public void cleanup() {
    BlockUtils.shutdownCache(conf);
  }

  @Test
  public void testDBFile() throws IOException {
    // create a container of schema v2
    KeyValueContainer container = createTestContainer();
    assertEquals(container.getContainerData().getSchemaVersion(),
        OzoneConsts.SCHEMA_V2);

    // db file should be under the container path
    String containerPath = container.getContainerData().getDbFile()
        .getParentFile().getParentFile().getName();
    assertEquals(containerPath,
        Long.toString(container.getContainerData().getContainerID()));
  }

  @Test
  public void testBlockIteration() throws IOException {
    // create a container of schema v2
    KeyValueContainer container = createTestContainer();
    assertEquals(container.getContainerData().getSchemaVersion(),
        OzoneConsts.SCHEMA_V2);

    // turn on schema v3 first, then do operations
    ContainerTestUtils.enableSchemaV3(conf);

    try (DBHandle db = BlockUtils.getDB(container.getContainerData(), conf)) {
      long containerID = container.getContainerData().getContainerID();
      int blockCount = 0;
      try (BlockIterator<BlockData> iter = db.getStore()
          .getBlockIterator(containerID)) {
        while (iter.hasNext()) {
          BlockData blockData = iter.nextBlock();
          int chunkCount = 0;
          for (ContainerProtos.ChunkInfo chunkInfo : blockData.getChunks()) {
            assertEquals(chunkInfo.getLen(), CHUNK_LENGTH);
            chunkCount++;
          }
          assertEquals(chunkCount, CHUNKS_PER_BLOCK);
          blockCount++;
        }
      }
      assertEquals(blockCount, BLOCKS_PER_CONTAINER);
    }
  }

  @Test
  public void testReadMetadata() throws IOException {
    // create a container of schema v2
    KeyValueContainer container = createTestContainer();
    assertEquals(container.getContainerData().getSchemaVersion(),
        OzoneConsts.SCHEMA_V2);
    KeyValueContainerData cData = container.getContainerData();
    assertEquals(cData.getBlockCount(), BLOCKS_PER_CONTAINER);
    assertEquals(cData.getNumPendingDeletionBlocks(),
        DELETE_TXNS_PER_CONTAINER * BLOCKS_PER_TXN);
    assertEquals(cData.getBytesUsed(),
        CHUNK_LENGTH * CHUNKS_PER_BLOCK * BLOCKS_PER_CONTAINER);

    // turn on schema v3 first, then do operations
    ContainerTestUtils.enableSchemaV3(conf);

    try (DBHandle db = BlockUtils.getDB(cData, conf)) {
      Table<String, Long> metadatatable = db.getStore().getMetadataTable();
      assertEquals((long)metadatatable.get(BLOCK_COUNT),
          BLOCKS_PER_CONTAINER);
      assertEquals((long)metadatatable.get(PENDING_DELETE_BLOCK_COUNT),
          DELETE_TXNS_PER_CONTAINER * BLOCKS_PER_TXN);
      assertEquals((long)metadatatable.get(CONTAINER_BYTES_USED),
          CHUNK_LENGTH * CHUNKS_PER_BLOCK * BLOCKS_PER_CONTAINER);
    }
  }

  @Test
  public void testDeleteViaTransation() throws IOException, TimeoutException,
      InterruptedException {
    // create a container of schema v2
    KeyValueContainer container = createTestContainer();
    assertEquals(container.getContainerData().getSchemaVersion(),
        OzoneConsts.SCHEMA_V2);
    // close it
    container.close();
    containerSet.addContainer(container);
    KeyValueContainerData cData = container.getContainerData();

    // turn on schema v3 first, then do operations
    ContainerTestUtils.enableSchemaV3(conf);

    // start block deleting service
    long initialTotalSpace = cData.getBytesUsed();
    BlockDeletingServiceTestImpl service =
        new BlockDeletingServiceTestImpl(ozoneContainer, 1000, conf);
    service.start();
    GenericTestUtils.waitFor(service::isStarted, 100, 3000);
    service.runDeletingTasks();
    GenericTestUtils.waitFor(() -> service.getTimesOfProcessed() == 1,
        100, 3000);
    GenericTestUtils.waitFor(() -> cData.getBytesUsed() != initialTotalSpace,
        100, 3000);

    // check in-memory metadata after deletion
    long blockSize = CHUNK_LENGTH * CHUNKS_PER_BLOCK;
    long expectedKeyCount = BLOCKS_PER_CONTAINER -
        DELETE_TXNS_PER_CONTAINER * BLOCKS_PER_TXN;
    long expectedBytesUsed = blockSize * expectedKeyCount;
    assertEquals(cData.getBlockCount(), expectedKeyCount);
    assertEquals(cData.getNumPendingDeletionBlocks(), 0);
    assertEquals(cData.getBytesUsed(), expectedBytesUsed);

    // check db metadata after deletion
    try (DBHandle db = BlockUtils.getDB(cData, conf)) {
      Table<String, Long> metadatatable = db.getStore().getMetadataTable();
      assertEquals((long)metadatatable.get(BLOCK_COUNT), expectedKeyCount);
      assertEquals((long)metadatatable.get(PENDING_DELETE_BLOCK_COUNT), 0);
      assertEquals((long)metadatatable.get(CONTAINER_BYTES_USED),
          expectedBytesUsed);
    }
  }

  private KeyValueContainer createTestContainer() throws IOException {
    long containerID = ContainerTestHelper.getTestContainerID();
    KeyValueContainerData cData = new KeyValueContainerData(containerID,
        ContainerLayoutVersion.FILE_PER_BLOCK,
        ContainerTestHelper.CONTAINER_MAX_SIZE,
        UUID.randomUUID().toString(), datanodeUuid);
    cData.setSchemaVersion(OzoneConsts.SCHEMA_V2);
    KeyValueContainer container = new KeyValueContainer(cData, conf);
    container.create(volumeSet, new RoundRobinVolumeChoosingPolicy(),
        clusterID);

    // populate with some blocks
    // metadata will be updated here, too
    for (long localID = 0; localID < BLOCKS_PER_CONTAINER; localID++) {
      BlockData blockData = createTestBlockData(localID, container);
      blockManager.putBlock(container, blockData);
    }

    // populate with some delete txns
    for (long txnID = 0; txnID < DELETE_TXNS_PER_CONTAINER; txnID++) {
      long startBlockID = txnID * DELETE_TXNS_PER_CONTAINER;
      List<Long> blocks = Arrays.asList(startBlockID, startBlockID + 1);
      DeletedBlocksTransaction txn =
          createTestDeleteTxn(txnID, blocks, containerID);
      try (DBHandle db = BlockUtils.getDB(cData, conf)) {
        try (BatchOperation batch = db.getStore().getBatchHandler()
            .initBatchOperation()) {
          DatanodeStoreSchemaTwoImpl dnStoreTwoImpl =
              (DatanodeStoreSchemaTwoImpl) db.getStore();
          dnStoreTwoImpl.getDeleteTransactionTable()
              .putWithBatch(batch, txnID, txn);

          // update delete related metadata
          db.getStore().getMetadataTable().putWithBatch(batch,
              cData.getLatestDeleteTxnKey(), txn.getTxID());
          db.getStore().getMetadataTable().putWithBatch(batch,
              cData.getPendingDeleteBlockCountKey(),
              cData.getNumPendingDeletionBlocks() + BLOCKS_PER_TXN);
          db.getStore().getBatchHandler().commitBatchOperation(batch);

          cData.updateDeleteTransactionId(txn.getTxID());
          cData.incrPendingDeletionBlocks(BLOCKS_PER_TXN, 256);
        }
      }
    }
    return container;
  }

  private BlockData createTestBlockData(long localID, Container container)
      throws StorageContainerException {
    long containerID = container.getContainerData().getContainerID();
    BlockID blockID = new BlockID(containerID, localID);
    BlockData blockData = new BlockData(blockID);

    // populate with some chunks
    for (int chunkIndex = 0; chunkIndex < CHUNKS_PER_BLOCK; chunkIndex++) {
      ChunkInfo chunk = createTestChunkData(chunkIndex, blockID, container);
      blockData.addChunk(chunk.getProtoBufMessage());
    }

    return blockData;
  }

  private ChunkInfo createTestChunkData(long chunkIndex,
      BlockID blockID, Container container) throws StorageContainerException {
    String chunkName = blockID.getLocalID() + "_chunk_" + (chunkIndex + 1);
    ChunkBuffer chunkData = ChunkBuffer.wrap(ByteBuffer.wrap(SAMPLE_DATA));
    ChunkInfo chunkInfo = new ChunkInfo(chunkName,
        chunkIndex * CHUNK_LENGTH, CHUNK_LENGTH);
    chunkManager
        .writeChunk(container, blockID, chunkInfo, chunkData, WRITE_STAGE);
    chunkManager
        .writeChunk(container, blockID, chunkInfo, chunkData, COMMIT_STAGE);
    return chunkInfo;
  }

  private DeletedBlocksTransaction createTestDeleteTxn(long txnID,
      List<Long> blocks, long containerID) {
    return DeletedBlocksTransaction.newBuilder().setTxID(txnID)
        .setContainerID(containerID).addAllLocalID(blocks).setCount(0).build();
  }
}
