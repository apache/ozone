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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V1;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V2;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V3;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.readChecksumFile;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.COMMIT_STAGE;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.WRITE_STAGE;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createDbInstancesForTestIfNeeded;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion.FILE_PER_BLOCK;
import static org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil.isSameSchemaVersion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.BlockDeletingServiceMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.BlockDeletingService;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.TopNOrderedContainerDeletionChoosingPolicy;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.impl.FilePerBlockStrategy;
import org.apache.hadoop.ozone.container.keyvalue.impl.FilePerChunkStrategy;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.keyvalue.statemachine.background.BlockDeletingTask;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaTwoImpl;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests to test block deleting service.
 */
public class TestBlockDeletingService {

  private static final Logger LOG = LoggerFactory.getLogger(TestBlockDeletingService.class);

  @TempDir
  private File testRoot;
  private String scmId;
  private String datanodeUuid;
  private final OzoneConfiguration conf = new OzoneConfiguration();

  private ContainerLayoutVersion layout;
  private String schemaVersion;
  private int blockLimitPerInterval;
  private MutableVolumeSet volumeSet;
  private static final int BLOCK_CHUNK_SIZE = 100;

  @BeforeEach
  public void init() throws IOException {
    CodecBuffer.enableLeakDetection();
    scmId = UUID.randomUUID().toString();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, testRoot.getAbsolutePath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testRoot.getAbsolutePath());
    datanodeUuid = UUID.randomUUID().toString();
    volumeSet = new MutableVolumeSet(datanodeUuid, scmId, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    createDbInstancesForTestIfNeeded(volumeSet, scmId, scmId, conf);
  }

  @AfterEach
  public void cleanup() throws IOException {
    BlockUtils.shutdownCache(conf);
    CodecBuffer.assertNoLeaks();
  }

  /**
   * A helper method to create some blocks and put them under deletion
   * state for testing. This method directly updates container.db and
   * creates some fake chunk files for testing.
   */
  private void createToDeleteBlocks(ContainerSet containerSet,
      int numOfContainers, int numOfBlocksPerContainer,
      int numOfChunksPerBlock) throws IOException {
    for (int i = 0; i < numOfContainers; i++) {
      createToDeleteBlocks(containerSet, numOfBlocksPerContainer,
          numOfChunksPerBlock);
    }
  }

  private KeyValueContainerData createToDeleteBlocks(ContainerSet containerSet,
      int numOfBlocksPerContainer, int numOfChunksPerBlock) throws IOException {
    ChunkManager chunkManager;
    if (layout == FILE_PER_BLOCK) {
      chunkManager = new FilePerBlockStrategy(true, null);
    } else {
      chunkManager = new FilePerChunkStrategy(true, null);
    }
    byte[] arr = new byte[1048576];
    ThreadLocalRandom.current().nextBytes(arr);
    ChunkBuffer buffer = ChunkBuffer.wrap(ByteBuffer.wrap(arr));
    int txnID = 0;
    long containerID = ContainerTestHelper.getTestContainerID();
    KeyValueContainerData data =
        new KeyValueContainerData(containerID, layout,
            ContainerTestHelper.CONTAINER_MAX_SIZE,
            UUID.randomUUID().toString(), datanodeUuid);
    data.closeContainer();
    data.setSchemaVersion(schemaVersion);
    KeyValueContainer container = new KeyValueContainer(data, conf);
    container.create(volumeSet,
        new RoundRobinVolumeChoosingPolicy(), scmId);
    containerSet.addContainer(container);
    data = (KeyValueContainerData) containerSet.getContainer(
        containerID).getContainerData();
    data.setSchemaVersion(schemaVersion);
    if (isSameSchemaVersion(schemaVersion, SCHEMA_V1)) {
      createPendingDeleteBlocksSchema1(numOfBlocksPerContainer, data,
          containerID, numOfChunksPerBlock, buffer, chunkManager, container);
    } else if (isSameSchemaVersion(schemaVersion, SCHEMA_V2)
        || isSameSchemaVersion(schemaVersion, SCHEMA_V3)) {
      createPendingDeleteBlocksViaTxn(numOfBlocksPerContainer, txnID,
          containerID, numOfChunksPerBlock, buffer, chunkManager,
          container, data);
    } else {
      throw new UnsupportedOperationException(
          "Only schema version 1,2,3 are supported.");
    }

    return data;
  }

  @SuppressWarnings("checkstyle:parameternumber")
  private void createPendingDeleteBlocksSchema1(int numOfBlocksPerContainer,
      KeyValueContainerData data, long containerID, int numOfChunksPerBlock,
      ChunkBuffer buffer, ChunkManager chunkManager,
      KeyValueContainer container) {
    BlockID blockID = null;
    try (DBHandle metadata = BlockUtils.getDB(data, conf)) {
      for (int j = 0; j < numOfBlocksPerContainer; j++) {
        blockID = ContainerTestHelper.getTestBlockID(containerID);
        String deleteStateName = data.getDeletingBlockKey(
            blockID.getLocalID());
        BlockData kd = new BlockData(blockID);
        List<ContainerProtos.ChunkInfo> chunks = Lists.newArrayList();
        putChunksInBlock(numOfChunksPerBlock, j, chunks, buffer, chunkManager,
            container, blockID);
        kd.setChunks(chunks);
        metadata.getStore().getBlockDataTable().put(deleteStateName, kd);
        container.getContainerData().incrPendingDeletionBlocks(1, BLOCK_CHUNK_SIZE);
      }
      updateMetaData(data, container, numOfBlocksPerContainer,
          numOfChunksPerBlock);
    } catch (IOException exception) {
      LOG.info("Exception " + exception);
      LOG.warn("Failed to put block: " + blockID + " in BlockDataTable.");
    }
  }

  @SuppressWarnings("checkstyle:parameternumber")
  private void createPendingDeleteBlocksViaTxn(int numOfBlocksPerContainer,
      int txnID, long containerID, int numOfChunksPerBlock, ChunkBuffer buffer,
      ChunkManager chunkManager, KeyValueContainer container,
      KeyValueContainerData data) {
    List<Long> containerBlocks = new ArrayList<>();
    for (int i = 0; i < numOfBlocksPerContainer; i++) {
      txnID = txnID + 1;
      BlockID blockID = ContainerTestHelper.getTestBlockID(containerID);
      BlockData kd = new BlockData(blockID);
      List<ContainerProtos.ChunkInfo> chunks = Lists.newArrayList();
      putChunksInBlock(numOfChunksPerBlock, i, chunks, buffer, chunkManager,
          container, blockID);
      kd.setChunks(chunks);
      try (DBHandle metadata = BlockUtils.getDB(data, conf)) {
        String blockKey = data.getBlockKey(blockID.getLocalID());
        metadata.getStore().getBlockDataTable().put(blockKey, kd);
      } catch (IOException exception) {
        LOG.info("Exception = " + exception);
        LOG.warn("Failed to put block: " + blockID.getLocalID()
            + " in BlockDataTable.");
      }
      container.getContainerData().incrPendingDeletionBlocks(1, BLOCK_CHUNK_SIZE);

      // Below we are creating one transaction per block just for
      // testing purpose

      containerBlocks.add(blockID.getLocalID());
      createTxn(data, containerBlocks, txnID, containerID);
      containerBlocks.clear();
    }
    updateMetaData(data, container, numOfBlocksPerContainer,
        numOfChunksPerBlock);
  }

  private void createTxn(KeyValueContainerData data, List<Long> containerBlocks,
      int txnID, long containerID) {
    try (DBHandle metadata = BlockUtils.getDB(data, conf)) {
      StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction dtx =
          StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction
              .newBuilder().setTxID(txnID).setContainerID(containerID)
              .addAllLocalID(containerBlocks)
              .setTotalBlockSize(containerBlocks.size() * BLOCK_CHUNK_SIZE)
              .setCount(0).build();
      try (BatchOperation batch = metadata.getStore().getBatchHandler()
          .initBatchOperation()) {
        DatanodeStore ds = metadata.getStore();

        if (isSameSchemaVersion(schemaVersion, SCHEMA_V3)) {
          DatanodeStoreSchemaThreeImpl dnStoreThreeImpl =
              (DatanodeStoreSchemaThreeImpl) ds;
          dnStoreThreeImpl.getDeleteTransactionTable()
              .putWithBatch(batch, data.getDeleteTxnKey(txnID), dtx);
        } else {
          DatanodeStoreSchemaTwoImpl dnStoreTwoImpl =
              (DatanodeStoreSchemaTwoImpl) ds;
          dnStoreTwoImpl.getDeleteTransactionTable()
              .putWithBatch(batch, (long) txnID, dtx);
        }
        metadata.getStore().getBatchHandler().commitBatchOperation(batch);
      }
    } catch (IOException exception) {
      LOG.warn("Transaction creation was not successful for txnID: " + txnID
          + " consisting of " + containerBlocks.size() + " blocks.");
    }
  }

  private void putChunksInBlock(int numOfChunksPerBlock, int i,
      List<ContainerProtos.ChunkInfo> chunks, ChunkBuffer buffer,
      ChunkManager chunkManager, KeyValueContainer container, BlockID blockID) {
    long chunkLength = BLOCK_CHUNK_SIZE;
    try {
      for (int k = 0; k < numOfChunksPerBlock; k++) {
        // This real chunkName should be localID_chunk_chunkIndex, here is for
        // explicit debug and the necessity of HDDS-7446 to detect the orphan
        // chunks through the chunk file name
        final String chunkName = String.format("%d_chunk_%d_block_%d",
            blockID.getContainerBlockID().getLocalID(), k, i);
        final long offset = k * chunkLength;
        ContainerProtos.ChunkInfo info =
            ContainerProtos.ChunkInfo.newBuilder().setChunkName(chunkName)
                .setLen(chunkLength).setOffset(offset)
                .setChecksumData(Checksum.getNoChecksumDataProto()).build();
        chunks.add(info);
        ChunkInfo chunkInfo = new ChunkInfo(chunkName, offset, chunkLength);
        ChunkBuffer chunkData = buffer.duplicate(0, (int) chunkLength);
        chunkManager
            .writeChunk(container, blockID, chunkInfo, chunkData, WRITE_STAGE);
        chunkManager
            .writeChunk(container, blockID, chunkInfo, chunkData, COMMIT_STAGE);
      }
    } catch (IOException ex) {
      LOG.warn("Putting chunks in blocks was not successful for BlockID: "
          + blockID);
    }
  }

  private void updateMetaData(KeyValueContainerData data,
      KeyValueContainer container, int numOfBlocksPerContainer,
      int numOfChunksPerBlock) {
    long chunkLength = BLOCK_CHUNK_SIZE;
    try (DBHandle metadata = BlockUtils.getDB(data, conf)) {
      container.getContainerData().getStatistics().setBlockCountForTesting(numOfBlocksPerContainer);
      // Set block count, bytes used and pending delete block count.
      metadata.getStore().getMetadataTable()
          .put(data.getBlockCountKey(), (long) numOfBlocksPerContainer);
      metadata.getStore().getMetadataTable()
          .put(data.getBytesUsedKey(),
              chunkLength * numOfChunksPerBlock * numOfBlocksPerContainer);
      metadata.getStore().getMetadataTable()
          .put(data.getPendingDeleteBlockCountKey(),
              (long) numOfBlocksPerContainer);
      metadata.getStore().getMetadataTable()
          .put(data.getPendingDeleteBlockBytesKey(),
              (long) numOfBlocksPerContainer * BLOCK_CHUNK_SIZE);
    } catch (IOException exception) {
      LOG.warn("Meta Data update was not successful for container: "
          + container);
    }
  }

  /**
   *  Run service runDeletingTasks and wait for it's been processed.
   */
  private void deleteAndWait(BlockDeletingServiceTestImpl service,
      int timesOfProcessed) throws TimeoutException, InterruptedException {
    service.runDeletingTasks();
    GenericTestUtils.waitFor(()
        -> service.getTimesOfProcessed() == timesOfProcessed, 100, 3000);
  }

  /**
   * Get under deletion blocks count from DB,
   * note this info is parsed from container.db.
   */
  private int getUnderDeletionBlocksCount(DBHandle meta,
      KeyValueContainerData data) throws IOException {
    if (data.hasSchema(SCHEMA_V1)) {
      return meta.getStore().getBlockDataTable()
          .getRangeKVs(null, 100, data.containerPrefix(),
              data.getDeletingBlockKeyFilter())
          .size();
    } else if (data.hasSchema(SCHEMA_V2)) {
      int pendingBlocks = 0;
      DatanodeStore ds = meta.getStore();
      DatanodeStoreSchemaTwoImpl dnStoreTwoImpl =
          (DatanodeStoreSchemaTwoImpl) ds;
      try (Table.KeyValueIterator<Long, DeletedBlocksTransaction>
              iter = dnStoreTwoImpl.getDeleteTransactionTable().iterator()) {
        while (iter.hasNext()) {
          StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction
              delTx = iter.next().getValue();
          pendingBlocks += delTx.getLocalIDList().size();
        }
      }
      return pendingBlocks;
    } else if (data.hasSchema(SCHEMA_V3)) {
      int pendingBlocks = 0;
      DatanodeStore ds = meta.getStore();
      DatanodeStoreSchemaThreeImpl dnStoreThreeImpl =
          (DatanodeStoreSchemaThreeImpl) ds;
      try (Table.KeyValueIterator<String, DeletedBlocksTransaction>
              iter = dnStoreThreeImpl.getDeleteTransactionTable()
              .iterator(data.containerPrefix())) {
        while (iter.hasNext()) {
          StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction
              delTx = iter.next().getValue();
          pendingBlocks += delTx.getLocalIDList().size();
        }
      }
      return pendingBlocks;
    } else {
      throw new UnsupportedOperationException(
          "Only schema version 1,2,3 are supported.");
    }
  }


  /**
   * In some cases, the pending delete blocks metadata will become larger
   * than the actual number of pending delete blocks in the database. If
   * there are no delete transactions in the DB, this metadata counter should
   * be reset to zero.
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testPendingDeleteBlockReset(ContainerTestVersionInfo versionInfo)
      throws Exception {
    setLayoutAndSchemaForTest(versionInfo);
    // This test is not relevant for schema V1.
    if (isSameSchemaVersion(schemaVersion, SCHEMA_V1)) {
      return;
    }

    final int blockDeleteLimit = 2;
    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    dnConf.setBlockDeletionLimit(blockDeleteLimit);
    this.blockLimitPerInterval = dnConf.getBlockDeletionLimit();
    conf.setFromObject(dnConf);
    ContainerSet containerSet = newContainerSet();

    // Create one container with no actual pending delete blocks, but an
    // incorrect metadata value indicating it has enough pending deletes to
    // use up the whole block deleting limit.
    KeyValueContainerData incorrectData =
        createToDeleteBlocks(containerSet,
        0, 1);
    try (DBHandle db = BlockUtils.getDB(incorrectData, conf)) {
      // Check pre-create state.
      assertEquals(0, getUnderDeletionBlocksCount(db,
          incorrectData));
      assertEquals(0, db.getStore().getMetadataTable()
          .get(incorrectData.getPendingDeleteBlockCountKey()).longValue());
      assertEquals(0, db.getStore().getMetadataTable()
          .get(incorrectData.getPendingDeleteBlockBytesKey()).longValue());
      assertEquals(0,
          incorrectData.getNumPendingDeletionBlocks());
      assertEquals(0,
          incorrectData.getBlockPendingDeletionBytes());

      // Alter the pending delete value in memory and the DB.
      incorrectData.incrPendingDeletionBlocks(blockDeleteLimit, 512);
      db.getStore().getMetadataTable().put(
          incorrectData.getPendingDeleteBlockCountKey(),
          (long)blockDeleteLimit);
    }

    // Create one container with fewer pending delete blocks than the first.
    int correctNumBlocksToDelete = blockDeleteLimit - 1;
    KeyValueContainerData correctData = createToDeleteBlocks(containerSet,
        correctNumBlocksToDelete, 1);
    // Check its metadata was set up correctly.
    assertEquals(correctNumBlocksToDelete,
        correctData.getNumPendingDeletionBlocks());
    assertEquals(correctNumBlocksToDelete * BLOCK_CHUNK_SIZE,
        correctData.getBlockPendingDeletionBytes());
    try (DBHandle db = BlockUtils.getDB(correctData, conf)) {
      assertEquals(correctNumBlocksToDelete,
          getUnderDeletionBlocksCount(db, correctData));
      assertEquals(correctNumBlocksToDelete,
          db.getStore().getMetadataTable()
              .get(correctData.getPendingDeleteBlockCountKey()).longValue());
      assertEquals(correctNumBlocksToDelete * BLOCK_CHUNK_SIZE,
          db.getStore().getMetadataTable()
              .get(correctData.getPendingDeleteBlockBytesKey()).longValue());
    }


    // Create the deleting service instance with very large interval between
    // runs so we can trigger it manually.
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        ContainerTestUtils.getKeyValueHandler(conf, datanodeUuid, containerSet, volumeSet, metrics);
    OzoneContainer ozoneContainer =
        mockDependencies(containerSet, keyValueHandler);
    BlockDeletingService svc = new BlockDeletingService(ozoneContainer,
        1_000_000, 1_000_000, TimeUnit.SECONDS, 1, conf, new ContainerChecksumTreeManager(conf));

    // On the first run, the container with incorrect metadata should consume
    // the block deletion limit, and the correct container with fewer pending
    // delete blocks will not be processed.
    svc.runPeriodicalTaskNow();

    // Pending delete block count in the incorrect container should be fixed
    // and reset to 0.
    assertEquals(0, incorrectData.getNumPendingDeletionBlocks());
    assertEquals(0, incorrectData.getBlockPendingDeletionBytes());
    try (DBHandle db = BlockUtils.getDB(incorrectData, conf)) {
      assertEquals(0, getUnderDeletionBlocksCount(db,
          incorrectData));
      assertEquals(0, db.getStore().getMetadataTable()
          .get(incorrectData.getPendingDeleteBlockCountKey()).longValue());
    }
    // Correct container should not have been processed.
    assertEquals(correctNumBlocksToDelete,
        correctData.getNumPendingDeletionBlocks());
    assertEquals(correctNumBlocksToDelete * BLOCK_CHUNK_SIZE,
        correctData.getBlockPendingDeletionBytes());
    try (DBHandle db = BlockUtils.getDB(correctData, conf)) {
      assertEquals(correctNumBlocksToDelete,
          getUnderDeletionBlocksCount(db, correctData));
      assertEquals(correctNumBlocksToDelete,
          db.getStore().getMetadataTable()
              .get(correctData.getPendingDeleteBlockCountKey()).longValue());
      assertEquals(correctNumBlocksToDelete * BLOCK_CHUNK_SIZE,
          db.getStore().getMetadataTable()
              .get(correctData.getPendingDeleteBlockBytesKey()).longValue());
    }

    // On the second run, the correct container should be picked up, because
    // it now has the most pending delete blocks.
    svc.runPeriodicalTaskNow();

    // The incorrect container should remain in the same state after being
    // fixed.
    assertEquals(0, incorrectData.getNumPendingDeletionBlocks());
    assertEquals(0, incorrectData.getBlockPendingDeletionBytes());
    try (DBHandle db = BlockUtils.getDB(incorrectData, conf)) {
      assertEquals(0, getUnderDeletionBlocksCount(db,
          incorrectData));
      assertEquals(0, db.getStore().getMetadataTable()
          .get(incorrectData.getPendingDeleteBlockCountKey()).longValue());
      assertEquals(0, db.getStore().getMetadataTable()
          .get(incorrectData.getPendingDeleteBlockBytesKey()).longValue());
    }
    // The correct container should have been processed this run and had its
    // blocks deleted.
    assertEquals(0, correctData.getNumPendingDeletionBlocks());
    assertEquals(0, correctData.getBlockPendingDeletionBytes());
    try (DBHandle db = BlockUtils.getDB(correctData, conf)) {
      assertEquals(0, getUnderDeletionBlocksCount(db,
          correctData));
      assertEquals(0, db.getStore().getMetadataTable()
          .get(correctData.getPendingDeleteBlockCountKey()).longValue());
      assertEquals(0, db.getStore().getMetadataTable()
          .get(correctData.getPendingDeleteBlockBytesKey()).longValue());
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testBlockDeletion(ContainerTestVersionInfo versionInfo)
      throws Exception {
    setLayoutAndSchemaForTest(versionInfo);
    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    dnConf.setBlockDeletionLimit(2);
    this.blockLimitPerInterval = dnConf.getBlockDeletionLimit();
    conf.setFromObject(dnConf);
    ContainerSet containerSet = newContainerSet();
    createToDeleteBlocks(containerSet, 1, 3, 1);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        ContainerTestUtils.getKeyValueHandler(conf, datanodeUuid, containerSet, volumeSet, metrics);
    BlockDeletingServiceTestImpl svc =
        getBlockDeletingService(containerSet, conf, keyValueHandler);
    svc.start();
    BlockDeletingServiceMetrics deletingServiceMetrics = svc.getMetrics();
    GenericTestUtils.waitFor(svc::isStarted, 100, 3000);

    // Ensure 1 container was created
    List<ContainerData> containerData = Lists.newArrayList();
    containerSet.listContainer(0L, 1, containerData);
    assertEquals(1, containerData.size());
    KeyValueContainerData data = (KeyValueContainerData) containerData.get(0);
    KeyPrefixFilter filter = isSameSchemaVersion(schemaVersion, SCHEMA_V1) ?
        data.getDeletingBlockKeyFilter() : data.getUnprefixedKeyFilter();

    try (DBHandle meta = BlockUtils.getDB(data, conf)) {
      Map<Long, Container<?>> containerMap = containerSet.getContainerMapCopy();
      assertBlockDataTableRecordCount(3, meta, filter, data.getContainerID());
      // NOTE: this test assumes that all the container is KetValueContainer and
      // have DeleteTransactionId in KetValueContainerData. If other
      // types is going to be added, this test should be checked.
      long transactionId = ((KeyValueContainerData) containerMap
          .get(containerData.get(0).getContainerID()).getContainerData())
          .getDeleteTransactionId();
      long containerSpace = containerData.get(0).getBytesUsed();
      // Number of deleted blocks in container should be equal to 0 before
      // block delete

      long deleteSuccessCount =
          deletingServiceMetrics.getSuccessCount();
      long totalBlockChosenCount =
          deletingServiceMetrics.getTotalBlockChosenCount();
      long totalContainerChosenCount =
          deletingServiceMetrics.getTotalContainerChosenCount();
      assertEquals(0, transactionId);

      // Ensure there are 3 blocks under deletion and 0 deleted blocks
      assertEquals(3, getUnderDeletionBlocksCount(meta, data));
      assertEquals(3, meta.getStore().getMetadataTable()
          .get(data.getPendingDeleteBlockCountKey()).longValue());
      assertEquals(3 * BLOCK_CHUNK_SIZE, meta.getStore().getMetadataTable()
          .get(data.getPendingDeleteBlockBytesKey()).longValue());

      // Container contains 3 blocks. So, space used by the container
      // should be greater than zero.
      assertThat(containerSpace).isGreaterThan(0);

      // An interval will delete 1 * 2 blocks
      deleteAndWait(svc, 1);

      // Make sure that deletions for each container were recorded in the checksum tree file.
      containerData.forEach(c -> assertDeletionsInChecksumFile(c, 2));

      GenericTestUtils.waitFor(() ->
          containerData.get(0).getBytesUsed() == containerSpace /
              3, 100, 3000);
      // After first interval 2 blocks will be deleted. Hence, current space
      // used by the container should be less than the space used by the
      // container initially(before running deletion services).
      assertThat(containerData.get(0).getBytesUsed()).isLessThan(containerSpace);
      assertEquals(2,
          deletingServiceMetrics.getSuccessCount()
              - deleteSuccessCount);
      assertEquals(2,
          deletingServiceMetrics.getTotalBlockChosenCount()
              - totalBlockChosenCount);
      assertEquals(1,
          deletingServiceMetrics.getTotalContainerChosenCount()
              - totalContainerChosenCount);
      // The value of the getTotalPendingBlockCount Metrics is obtained
      // before the deletion is processing
      // So the Pending Block count will be 3
      assertEquals(3,
          deletingServiceMetrics.getTotalPendingBlockCount());

      assertEquals(3 * BLOCK_CHUNK_SIZE,
          deletingServiceMetrics.getTotalPendingBlockBytes());

      deleteAndWait(svc, 2);

      containerData.forEach(c -> assertDeletionsInChecksumFile(c, 3));

      // After deletion of all 3 blocks, space used by the containers
      // should be zero.
      GenericTestUtils.waitFor(() ->
              containerData.get(0).getBytesUsed() == 0, 100, 3000);

      // Check finally DB counters.
      // Not checking bytes used, as handler is a mock call.
      assertEquals(0, meta.getStore().getMetadataTable()
          .get(data.getPendingDeleteBlockCountKey()).longValue());
      assertEquals(0,
          meta.getStore().getMetadataTable().get(data.getBlockCountKey())
              .longValue());
      assertEquals(3,
          deletingServiceMetrics.getSuccessCount()
              - deleteSuccessCount);
      assertEquals(3,
          deletingServiceMetrics.getTotalBlockChosenCount()
              - totalBlockChosenCount);
      assertEquals(2,
          deletingServiceMetrics.getTotalContainerChosenCount()
              - totalContainerChosenCount);

      // check if blockData get deleted
      assertBlockDataTableRecordCount(0, meta, filter, data.getContainerID());
      // The value of the getTotalPendingBlockCount Metrics is obtained
      // before the deletion is processing
      // So the Pending Block count will be 1
      assertEquals(1,
          deletingServiceMetrics.getTotalPendingBlockCount());
      assertEquals(BLOCK_CHUNK_SIZE,
          deletingServiceMetrics.getTotalPendingBlockBytes());
    }
    svc.shutdown();
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testBlockDeletionMetricsUpdatedProperlyAfterEachExecution(ContainerTestVersionInfo versionInfo)
      throws Exception {
    setLayoutAndSchemaForTest(versionInfo);
    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    dnConf.setBlockDeletionLimit(1);
    this.blockLimitPerInterval = dnConf.getBlockDeletionLimit();
    conf.setFromObject(dnConf);
    ContainerSet containerSet = newContainerSet();

    // Create transactions including duplicates
    createToDeleteBlocks(containerSet, 1, 3, 1);

    ContainerMetrics metrics = ContainerMetrics.create(conf);
    BlockDeletingServiceMetrics blockDeletingServiceMetrics = BlockDeletingServiceMetrics.create();
    KeyValueHandler keyValueHandler =
        ContainerTestUtils.getKeyValueHandler(conf, datanodeUuid, containerSet, volumeSet, metrics);
    BlockDeletingServiceTestImpl svc =
        getBlockDeletingService(containerSet, conf, keyValueHandler);
    svc.start();
    GenericTestUtils.waitFor(svc::isStarted, 100, 3000);

    // Ensure 1 container was created
    List<ContainerData> containerData = Lists.newArrayList();
    containerSet.listContainer(0L, 1, containerData);
    assertEquals(1, containerData.size());
    KeyValueContainerData data = (KeyValueContainerData) containerData.get(0);

    try (DBHandle meta = BlockUtils.getDB(data, conf)) {
      //Execute fist delete to update metrics
      deleteAndWait(svc, 1);

      assertEquals(3, blockDeletingServiceMetrics.getTotalPendingBlockCount());
      assertEquals(3 * BLOCK_CHUNK_SIZE, blockDeletingServiceMetrics.getTotalPendingBlockBytes());

      //Execute the second delete to check whether metrics values decreased
      deleteAndWait(svc, 2);

      assertEquals(2, blockDeletingServiceMetrics.getTotalPendingBlockCount());
      assertEquals(2 * BLOCK_CHUNK_SIZE, blockDeletingServiceMetrics.getTotalPendingBlockBytes());

      //Execute the third delete to check whether metrics values decreased
      deleteAndWait(svc, 3);

      assertEquals(1, blockDeletingServiceMetrics.getTotalPendingBlockCount());
      assertEquals(1 * BLOCK_CHUNK_SIZE, blockDeletingServiceMetrics.getTotalPendingBlockBytes());

    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Test failed with exception: " + ex.getMessage());
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testWithUnrecordedBlocks(ContainerTestVersionInfo versionInfo)
      throws Exception {
    setLayoutAndSchemaForTest(versionInfo);
    // Skip schemaV1, when markBlocksForDeletionSchemaV1, the unrecorded blocks
    // from received TNXs will be deleted, not in BlockDeletingService
    Assumptions.assumeFalse(isSameSchemaVersion(schemaVersion, SCHEMA_V1));

    int numOfContainers = 2;
    int numOfChunksPerBlock = 1;
    int numOfBlocksPerContainer = 3;
    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    dnConf.setBlockDeletionLimit(2);
    this.blockLimitPerInterval = dnConf.getBlockDeletionLimit();
    conf.setFromObject(dnConf);
    ContainerSet containerSet = newContainerSet();

    createToDeleteBlocks(containerSet, numOfContainers, numOfBlocksPerContainer,
        numOfChunksPerBlock);

    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        ContainerTestUtils.getKeyValueHandler(conf, datanodeUuid, containerSet, volumeSet, metrics);
    BlockDeletingServiceTestImpl svc =
        getBlockDeletingService(containerSet, conf, keyValueHandler);
    svc.start();
    GenericTestUtils.waitFor(svc::isStarted, 100, 3000);

    // Ensure 2 container was created
    List<ContainerData> containerData = Lists.newArrayList();
    containerSet.listContainer(0L, 2, containerData);
    assertEquals(2, containerData.size());
    KeyValueContainerData ctr1 = (KeyValueContainerData) containerData.get(0);
    KeyValueContainerData ctr2 = (KeyValueContainerData) containerData.get(1);
    KeyPrefixFilter filter = isSameSchemaVersion(schemaVersion, SCHEMA_V1) ?
        ctr1.getDeletingBlockKeyFilter() : ctr1.getUnprefixedKeyFilter();

    // Have two unrecorded blocks onDisk and another two not to simulate the
    // possible cases
    int numUnrecordedBlocks = 4;
    int numExistingOnDiskUnrecordedBlocks = 2;
    List<Long> unrecordedBlockIds = new ArrayList<>();
    Set<File> unrecordedChunks = new HashSet<>();

    try (DBHandle meta = BlockUtils.getDB(ctr1, conf)) {
      // create unrecorded blocks in a new txn and update metadata,
      // service shall first choose the top pendingDeletion container
      // if using the TopNOrderedContainerDeletionChoosingPolicy
      File chunkDir = ContainerUtils.getChunkDir(ctr1);
      for (int i = 0; i < numUnrecordedBlocks; i++) {
        long localId = System.nanoTime() + i;
        unrecordedBlockIds.add(localId);
        String chunkName;
        for (int indexOfChunk = 0; indexOfChunk < numOfChunksPerBlock;
             indexOfChunk++) {
          if (layout == FILE_PER_BLOCK) {
            chunkName = localId + ".block";
          } else {
            chunkName = localId + "_chunk_" + indexOfChunk;
          }
          File chunkFile = new File(chunkDir, chunkName);
          unrecordedChunks.add(chunkFile);
        }
      }
      // create unreferenced onDisk chunks
      Iterator<File> iter = unrecordedChunks.iterator();
      for (int m = 0; m < numExistingOnDiskUnrecordedBlocks; m++) {
        File chunk = iter.next();
        createRandomContentFile(chunk.getName(), chunkDir, 100);
        assertTrue(chunk.exists());
      }

      createTxn(ctr1, unrecordedBlockIds, 100, ctr1.getContainerID());
      ctr1.updateDeleteTransactionId(100);
      ctr1.incrPendingDeletionBlocks(numUnrecordedBlocks, BLOCK_CHUNK_SIZE);
      updateMetaData(ctr1, (KeyValueContainer) containerSet.getContainer(
          ctr1.getContainerID()), 3, 1);
      // Ensure there are 3 + 4 = 7 blocks under deletion
      assertEquals(7, getUnderDeletionBlocksCount(meta, ctr1));
    }

    assertBlockDataTableRecordCount(3, ctr1, filter);
    assertBlockDataTableRecordCount(3, ctr2, filter);
    assertEquals(3, ctr2.getNumPendingDeletionBlocks());

    // Totally 2 container * 3 blocks + 4 unrecorded block = 10 blocks
    // So we shall experience 5 rounds to delete all blocks
    // Unrecorded blocks should not affect the actual NumPendingDeletionBlocks
    deleteAndWait(svc, 1);
    deleteAndWait(svc, 2);
    deleteAndWait(svc, 3);
    deleteAndWait(svc, 4);
    deleteAndWait(svc, 5);
    GenericTestUtils.waitFor(() -> ctr2.getNumPendingDeletionBlocks() == 0,
        200, 2000);

    // To make sure the container stat calculation is right
    assertEquals(0, ctr1.getBlockCount());
    assertEquals(0, ctr1.getBytesUsed());
    assertEquals(0, ctr2.getBlockCount());
    assertEquals(0, ctr2.getBytesUsed());

    // check if blockData get deleted
    assertBlockDataTableRecordCount(0, ctr1, filter);
    assertBlockDataTableRecordCount(0, ctr2, filter);

    // check if all the unreferenced chunks get deleted
    for (File f : unrecordedChunks) {
      assertFalse(f.exists());
    }

    svc.shutdown();
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testShutdownService(ContainerTestVersionInfo versionInfo)
      throws Exception {
    setLayoutAndSchemaForTest(versionInfo);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 500,
        TimeUnit.MILLISECONDS);

    ContainerSet containerSet = newContainerSet();
    // Create 1 container with 100 blocks
    createToDeleteBlocks(containerSet, 1, 100, 1);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        ContainerTestUtils.getKeyValueHandler(conf, datanodeUuid, containerSet, volumeSet, metrics);
    BlockDeletingServiceTestImpl service =
        getBlockDeletingService(containerSet, conf, keyValueHandler);
    service.start();
    GenericTestUtils.waitFor(service::isStarted, 100, 3000);

    // Run some deleting tasks and verify there are threads running
    service.runDeletingTasks();
    GenericTestUtils.waitFor(() -> service.getThreadCount() > 0, 100, 1000);

    // Shutdown service and verify all threads are stopped
    service.shutdown();
    GenericTestUtils.waitFor(() -> service.getThreadCount() == 0, 100, 1000);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testBlockDeletionTimeout(ContainerTestVersionInfo versionInfo)
      throws Exception {
    setLayoutAndSchemaForTest(versionInfo);
    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    dnConf.setBlockDeletionLimit(3);
    blockLimitPerInterval = dnConf.getBlockDeletionLimit();
    conf.setFromObject(dnConf);

    ContainerSet containerSet = newContainerSet();
    createToDeleteBlocks(containerSet, 1, 3, 1);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        ContainerTestUtils.getKeyValueHandler(conf, datanodeUuid, containerSet, volumeSet, metrics);
    // set timeout value as 1ns to trigger timeout behavior
    long timeout  = 1;
    OzoneContainer ozoneContainer =
        mockDependencies(containerSet, keyValueHandler);
    BlockDeletingService svc = new BlockDeletingService(ozoneContainer,
        TimeUnit.MILLISECONDS.toNanos(1000), timeout, TimeUnit.NANOSECONDS,
        10, conf, new ContainerChecksumTreeManager(conf));
    svc.start();

    LogCapturer log = LogCapturer.captureLogs(BackgroundService.class);
    GenericTestUtils.waitFor(() -> {
      if (log.getOutput().contains("Background task execution took")) {
        log.stopCapturing();
        return true;
      }

      return false;
    }, 100, 1000);

    log.stopCapturing();
    svc.shutdown();

    // test for normal case that doesn't have timeout limitation

    createToDeleteBlocks(containerSet, 1, 3, 1);
    timeout  = 0;
    svc = new BlockDeletingService(ozoneContainer,
        TimeUnit.MILLISECONDS.toNanos(1000), timeout, TimeUnit.MILLISECONDS,
        10, conf, "", mock(ContainerChecksumTreeManager.class), mock(ReconfigurationHandler.class));
    svc.start();

    // get container meta data
    KeyValueContainer container =
        (KeyValueContainer) containerSet.iterator().next();
    KeyValueContainerData data = container.getContainerData();
    try (DBHandle meta = BlockUtils.getDB(data, conf)) {
      LogCapturer newLog = LogCapturer.captureLogs(BackgroundService.class);
      GenericTestUtils.waitFor(() -> {
        try {
          return getUnderDeletionBlocksCount(meta, data) == 0;
        } catch (IOException ignored) {
        }
        return false;
      }, 100, 1000);
      newLog.stopCapturing();

      // The block deleting successfully and shouldn't catch timed
      // out warning log.
      assertThat(newLog.getOutput())
          .doesNotContain("Background task executes timed out, retrying in next interval");
    }
    svc.shutdown();
  }

  private BlockDeletingServiceTestImpl getBlockDeletingService(
      ContainerSet containerSet, ConfigurationSource config,
      KeyValueHandler keyValueHandler) {
    OzoneContainer ozoneContainer =
        mockDependencies(containerSet, keyValueHandler);
    return new BlockDeletingServiceTestImpl(ozoneContainer, 1000, config);
  }

  private OzoneContainer mockDependencies(ContainerSet containerSet,
      KeyValueHandler keyValueHandler) {
    OzoneContainer ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
    when(ozoneContainer.getWriteChannel()).thenReturn(null);
    ContainerDispatcher dispatcher = mock(ContainerDispatcher.class);
    when(ozoneContainer.getDispatcher()).thenReturn(dispatcher);
    when(dispatcher.getHandler(any())).thenReturn(keyValueHandler);
    return ozoneContainer;
  }

  @Unhealthy
  @ContainerTestVersionInfo.ContainerTest
  public void testContainerThrottle(ContainerTestVersionInfo versionInfo)
      throws Exception {
    setLayoutAndSchemaForTest(versionInfo);
    // Properties :
    //  - Number of containers : 2
    //  - Number of blocks per container : 1
    //  - Number of chunks per block : 10
    //  - Container limit per interval : 1
    //  - Block limit per container : 1
    //
    // Each time only 1 container can be processed, so each time
    // 1 block from 1 container can be deleted.
    // Process 1 container per interval
    conf.set(
        ScmConfigKeys.OZONE_SCM_KEY_VALUE_CONTAINER_DELETION_CHOOSING_POLICY,
        TopNOrderedContainerDeletionChoosingPolicy.class.getName());
    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    dnConf.setBlockDeletionLimit(1);
    this.blockLimitPerInterval = dnConf.getBlockDeletionLimit();
    conf.setFromObject(dnConf);
    ContainerSet containerSet = newContainerSet();

    int containerCount = 2;
    int chunksPerBlock = 10;
    int blocksPerContainer = 1;
    createToDeleteBlocks(containerSet, containerCount, blocksPerContainer,
        chunksPerBlock);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        ContainerTestUtils.getKeyValueHandler(conf, datanodeUuid, containerSet, volumeSet, metrics);
    BlockDeletingServiceTestImpl service =
        getBlockDeletingService(containerSet, conf, keyValueHandler);
    service.start();
    List<ContainerData> containerData = Lists.newArrayList();
    containerSet.listContainer(0L, containerCount, containerData);
    try {
      GenericTestUtils.waitFor(service::isStarted, 100, 3000);

      // Deleting one of the two containers and its single block.
      // Hence, space used by the container of whose block has been
      // deleted should be zero.
      deleteAndWait(service, 1);

      GenericTestUtils.waitFor(() ->
              (containerData.get(0).getBytesUsed() == 0 ||
                      containerData.get(1).getBytesUsed() == 0),
              100, 3000);

      assertFalse((containerData.get(0).getBytesUsed() == 0) && (
          containerData.get(1).getBytesUsed() == 0));

      // Deleting the second container. Hence, space used by both the
      // containers should be zero.
      deleteAndWait(service, 2);

      GenericTestUtils.waitFor(() ->
              (containerData.get(0).getBytesUsed() ==
                  0 && containerData.get(1).getBytesUsed() == 0),
          100, 3000);
    } finally {
      service.shutdown();
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testContainerMaxLockHoldingTime(
      ContainerTestVersionInfo versionInfo) throws Exception {
    setLayoutAndSchemaForTest(versionInfo);
    LogCapturer log = LogCapturer.captureLogs(BlockDeletingTask.class);
    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);

    // Ensure that the lock holding timeout occurs every time a deletion
    // transaction is executed by setting BlockDeletingMaxLockHoldingTime to -1.
    dnConf.setBlockDeletingMaxLockHoldingTime(Duration.ofMillis(-1));
    dnConf.setBlockDeletionLimit(3);
    conf.setFromObject(dnConf);
    ContainerSet containerSet = newContainerSet();

    int containerCount = 1;
    int chunksPerBlock = 10;
    int blocksPerContainer = 3;
    createToDeleteBlocks(containerSet, containerCount, blocksPerContainer,
        chunksPerBlock);
    KeyValueHandler keyValueHandler =
        ContainerTestUtils.getKeyValueHandler(conf, datanodeUuid, containerSet, volumeSet);
    BlockDeletingServiceTestImpl service =
        getBlockDeletingService(containerSet, conf, keyValueHandler);
    service.start();
    List<ContainerData> containerData = Lists.newArrayList();
    containerSet.listContainer(0L, containerCount, containerData);
    try {
      GenericTestUtils.waitFor(service::isStarted, 100, 3000);
      deleteAndWait(service, 1);
      GenericTestUtils.waitFor(() ->
              (containerData.get(0).getBytesUsed() == 0),
          100, 3000);
      if (schemaVersion != null && (
          schemaVersion.equals(SCHEMA_V2) || schemaVersion.equals(SCHEMA_V3))) {

        // Since MaxLockHoldingTime is -1, every "deletion transaction" triggers
        // a timeout except the last one, where a "deletion transaction"
        // will be created for each Block, so it will start
        // blocksPerContainer - 1 timeout.
        assertEquals(blocksPerContainer - 1,
            StringUtils.countMatches(log.getOutput(), "Max lock hold time"));
      }
    } finally {
      service.shutdown();
    }
  }

  public long currentBlockSpace(List<ContainerData> containerData,
      int totalContainers) {
    long totalSpaceUsed = 0;
    for (int i = 0; i < totalContainers; i++) {
      totalSpaceUsed += containerData.get(i).getBytesUsed();
    }
    return totalSpaceUsed;
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testBlockThrottle(ContainerTestVersionInfo versionInfo)
      throws Exception {
    setLayoutAndSchemaForTest(versionInfo);
    // Properties :
    //  - Number of containers : 5
    //  - Number of blocks per container : 3
    //  - Number of chunks per block : 1
    //  - Container limit per interval : 10
    //  - Block limit per container : 2
    //
    // Each time containers can be all scanned, but only 10 blocks
    // can be actually deleted. So it requires 2 waves
    // to cleanup all the 15 blocks.
    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    dnConf.setBlockDeletionLimit(10);
    this.blockLimitPerInterval = dnConf.getBlockDeletionLimit();
    conf.setFromObject(dnConf);
    ContainerSet containerSet = newContainerSet();
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        ContainerTestUtils.getKeyValueHandler(conf, datanodeUuid, containerSet, volumeSet, metrics);
    int containerCount = 5;
    int blocksPerContainer = 3;
    createToDeleteBlocks(containerSet, containerCount,
        blocksPerContainer, 1);

    BlockDeletingServiceTestImpl service =
        getBlockDeletingService(containerSet, conf, keyValueHandler);
    service.start();
    List<ContainerData> containerData = Lists.newArrayList();
    containerSet.listContainer(0L, containerCount, containerData);
    long blockSpace = containerData.get(0).getBytesUsed() / blocksPerContainer;
    long totalContainerSpace =
        containerCount * containerData.get(0).getBytesUsed();
    try {
      GenericTestUtils.waitFor(service::isStarted, 100, 3000);
      // Total blocks = 3 * 5 = 15
      // blockLimitPerInterval = 10
      // each interval will at most runDeletingTasks = 10 blocks
      // but as per of deletion policy (random/topNorder), it will fetch all 3
      // blocks from first 3 containers and 1 block from last container.
      // C1 - 3 BLOCKS, C2 - 3 BLOCKS, C3 - 3 BLOCKS, C4 - 1 BLOCK

      // Deleted space of 10 blocks should be equal to (initial total space
      // of container - current total space of container).
      deleteAndWait(service, 1);

      GenericTestUtils.waitFor(() ->
              blockLimitPerInterval * blockSpace ==
                      (totalContainerSpace -
                              currentBlockSpace(containerData, containerCount)),
              100, 3000);

      // There is only 5 blocks left to runDeletingTasks

      // (Deleted space of previous 10 blocks + these left 5 blocks) should
      // be equal to (initial total space of container
      // - current total space of container(it will be zero as all blocks
      // in all the containers are deleted)).
      deleteAndWait(service, 2);

      long totalContainerBlocks = blocksPerContainer * containerCount;
      GenericTestUtils.waitFor(() ->
              totalContainerBlocks * blockSpace ==
                      (totalContainerSpace -
                              currentBlockSpace(containerData, containerCount)),
              100, 3000);

    } finally {
      service.shutdown();
    }
  }

  /**
   * The container checksum tree file is updated with the blocks that have been deleted after the on disk block files
   * are removed from disk, but before the transaction is removed from the DB. If there is a failure partway through,
   * the checksum tree file should still get updated when the transaction is retried, even if the block file is not
   * present.
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testChecksumFileUpdatedWhenDeleteRetried(ContainerTestVersionInfo versionInfo) throws Exception {
    final int numBlocks = 4;
    setLayoutAndSchemaForTest(versionInfo);
    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    dnConf.setBlockDeletionLimit(4);
    this.blockLimitPerInterval = dnConf.getBlockDeletionLimit();
    conf.setFromObject(dnConf);
    ContainerSet containerSet = newContainerSet();
    KeyValueContainerData contData = createToDeleteBlocks(containerSet, numBlocks, 4);
    KeyValueHandler keyValueHandler =
        ContainerTestUtils.getKeyValueHandler(conf, datanodeUuid, containerSet, volumeSet);
    BlockDeletingServiceTestImpl svc =
        getBlockDeletingService(containerSet, conf, keyValueHandler);
    svc.start();
    GenericTestUtils.waitFor(svc::isStarted, 100, 3000);

    // Remove all the block files from the disk, as if they were deleted previously but the system failed before
    // doing any metadata updates or removing the transaction of to-delete block IDs from the DB.
    File blockDataDir = new File(contData.getChunksPath());
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(blockDataDir.toPath())) {
      for (Path entry : stream) {
        assertTrue(entry.toFile().delete());
      }
    }

    String[] blockFilesRemaining = blockDataDir.list();
    assertNotNull(blockFilesRemaining);
    assertEquals(0, blockFilesRemaining.length);

    deleteAndWait(svc, 1);

    assertDeletionsInChecksumFile(contData, numBlocks);
  }

  /**
   *  Check blockData record count of certain container (DBHandle not provided).
   *
   * @param expectedCount expected records in the table
   * @param containerData KeyValueContainerData
   * @param filter KeyPrefixFilter
   * @throws IOException
   */
  private void assertBlockDataTableRecordCount(int expectedCount,
      KeyValueContainerData containerData, KeyPrefixFilter filter)
      throws IOException {
    try (DBHandle handle = BlockUtils.getDB(containerData, conf)) {
      long containerID = containerData.getContainerID();
      assertBlockDataTableRecordCount(expectedCount, handle, filter,
          containerID);
    }
  }

  /**
   *  Check blockData record count of certain container (DBHandle provided).
   *
   * @param expectedCount expected records in the table
   * @param handle DB handle
   * @param filter KeyPrefixFilter
   * @param containerID the container ID to filter results
   * @throws IOException
   */
  private void assertBlockDataTableRecordCount(int expectedCount,
      DBHandle handle, KeyPrefixFilter filter, long containerID)
      throws IOException {
    long count = 0L;
    try (BlockIterator<BlockData> iterator = handle.getStore().
        getBlockIterator(containerID, filter)) {
      iterator.seekToFirst();
      while (iterator.hasNext()) {
        iterator.nextBlock();
        count += 1;
      }
    }
    assertEquals(expectedCount, count, "Excepted: " + expectedCount
        + ", but actual: " + count + " in the blockData table of container: "
        + containerID + ".");
  }

  /**
   * Create the certain sized file in the appointed directory.
   *
   * @param fileName the string of file to be created
   * @param dir      the directory where file to be created
   * @param sizeInBytes the bytes size of the file
   * @throws IOException
   */
  private void createRandomContentFile(String fileName, File dir,
      long sizeInBytes) throws IOException {
    File file = new File(dir, fileName);
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file,
        "rw")) {
      randomAccessFile.setLength(sizeInBytes);
    }
  }

  private void setLayoutAndSchemaForTest(ContainerTestVersionInfo versionInfo) {
    this.layout = versionInfo.getLayout();
    this.schemaVersion = versionInfo.getSchemaVersion();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
  }

  private void assertDeletionsInChecksumFile(ContainerData data, int expectedNumBlocks) {
    ContainerProtos.ContainerChecksumInfo checksumInfo = null;
    try {
      checksumInfo = readChecksumFile(data);
    } catch (IOException ex) {
      fail("Failed to read container checksum tree file: " + ex.getMessage());
    }
    assertNotNull(checksumInfo);

    long numDeletedBlocks = checksumInfo.getContainerMerkleTree().getBlockMerkleTreeList().stream()
        .filter(ContainerProtos.BlockMerkleTree::getDeleted)
        .count();
    assertEquals(expectedNumBlocks, numDeletedBlocks);
  }
}
