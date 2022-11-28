/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common;


import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.BlockDeletingServiceMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.TopNOrderedContainerDeletionChoosingPolicy;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
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
import org.apache.hadoop.ozone.container.keyvalue.statemachine.background.BlockDeletingService;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaTwoImpl;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.testutils.BlockDeletingServiceTestImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V1;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V2;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V3;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createDbInstancesForTestIfNeeded;
import static org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion.FILE_PER_BLOCK;
import static org.apache.hadoop.ozone.container.common.states.endpoint.VersionEndpointTask.LOG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests to test block deleting service.
 */
@RunWith(Parameterized.class)
public class TestBlockDeletingService {

  private File testRoot;
  private String scmId;
  private String clusterID;
  private String datanodeUuid;
  private OzoneConfiguration conf;

  private final ContainerLayoutVersion layout;
  private final String schemaVersion;
  private int blockLimitPerInterval;
  private MutableVolumeSet volumeSet;

  public TestBlockDeletingService(ContainerTestVersionInfo versionInfo) {
    this.layout = versionInfo.getLayout();
    this.schemaVersion = versionInfo.getSchemaVersion();
    conf = new OzoneConfiguration();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return ContainerTestVersionInfo.versionParameters();
  }

  @Before
  public void init() throws IOException {
    testRoot = GenericTestUtils
        .getTestDir(TestBlockDeletingService.class.getSimpleName());
    if (testRoot.exists()) {
      FileUtils.cleanDirectory(testRoot);
    }
    scmId = UUID.randomUUID().toString();
    clusterID = UUID.randomUUID().toString();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, testRoot.getAbsolutePath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testRoot.getAbsolutePath());
    datanodeUuid = UUID.randomUUID().toString();
    volumeSet = new MutableVolumeSet(datanodeUuid, scmId, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    createDbInstancesForTestIfNeeded(volumeSet, scmId, scmId, conf);
  }

  @After
  public void cleanup() throws IOException {
    BlockUtils.shutdownCache(conf);
    FileUtils.deleteDirectory(testRoot);
  }

  private static final DispatcherContext WRITE_STAGE =
      new DispatcherContext.Builder()
          .setStage(DispatcherContext.WriteChunkStage.WRITE_DATA).build();

  private static final DispatcherContext COMMIT_STAGE =
      new DispatcherContext.Builder()
          .setStage(DispatcherContext.WriteChunkStage.COMMIT_DATA).build();

  /**
   * A helper method to create some blocks and put them under deletion
   * state for testing. This method directly updates container.db and
   * creates some fake chunk files for testing.
   */
  private void createToDeleteBlocks(ContainerSet containerSet,
      int numOfContainers,
      int numOfBlocksPerContainer,
      int numOfChunksPerBlock) throws IOException {
    ChunkManager chunkManager;
    if (layout == FILE_PER_BLOCK) {
      chunkManager = new FilePerBlockStrategy(true, null, null);
    } else {
      chunkManager = new FilePerChunkStrategy(true, null, null);
    }
    byte[] arr = randomAlphanumeric(1048576).getBytes(UTF_8);
    ChunkBuffer buffer = ChunkBuffer.wrap(ByteBuffer.wrap(arr));
    int txnID = 0;
    for (int x = 0; x < numOfContainers; x++) {
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
      if (schemaVersion.equals(SCHEMA_V1)) {
        createPendingDeleteBlocksSchema1(numOfBlocksPerContainer, data,
            containerID, numOfChunksPerBlock, buffer, chunkManager, container);
      } else if (schemaVersion.equals(SCHEMA_V2)
          || schemaVersion.equals(SCHEMA_V3)) {
        createPendingDeleteBlocksViaTxn(numOfBlocksPerContainer, txnID,
            containerID, numOfChunksPerBlock, buffer, chunkManager,
            container, data);
      } else {
        throw new UnsupportedOperationException(
            "Only schema version 1,2,3 are supported.");
      }
    }
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
        String deleteStateName = data.deletingBlockKey(blockID.getLocalID());
        BlockData kd = new BlockData(blockID);
        List<ContainerProtos.ChunkInfo> chunks = Lists.newArrayList();
        putChunksInBlock(numOfChunksPerBlock, j, chunks, buffer, chunkManager,
            container, blockID);
        kd.setChunks(chunks);
        metadata.getStore().getBlockDataTable().put(deleteStateName, kd);
        container.getContainerData().incrPendingDeletionBlocks(1);
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
        String blockKey = data.blockKey(blockID.getLocalID());
        metadata.getStore().getBlockDataTable().put(blockKey, kd);
      } catch (IOException exception) {
        LOG.info("Exception = " + exception);
        LOG.warn("Failed to put block: " + blockID.getLocalID()
            + " in BlockDataTable.");
      }
      container.getContainerData().incrPendingDeletionBlocks(1);

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
              .addAllLocalID(containerBlocks).setCount(0).build();
      try (BatchOperation batch = metadata.getStore().getBatchHandler()
          .initBatchOperation()) {
        DatanodeStore ds = metadata.getStore();

        if (schemaVersion.equals(SCHEMA_V3)) {
          DatanodeStoreSchemaThreeImpl dnStoreThreeImpl =
              (DatanodeStoreSchemaThreeImpl) ds;
          dnStoreThreeImpl.getDeleteTransactionTable()
              .putWithBatch(batch, data.deleteTxnKey(txnID), dtx);
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
    long chunkLength = 100;
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
    long chunkLength = 100;
    try (DBHandle metadata = BlockUtils.getDB(data, conf)) {
      container.getContainerData().setBlockCount(numOfBlocksPerContainer);
      // Set block count, bytes used and pending delete block count.
      metadata.getStore().getMetadataTable()
          .put(data.blockCountKey(), (long) numOfBlocksPerContainer);
      metadata.getStore().getMetadataTable()
          .put(data.bytesUsedKey(),
              chunkLength * numOfChunksPerBlock * numOfBlocksPerContainer);
      metadata.getStore().getMetadataTable()
          .put(data.pendingDeleteBlockCountKey(),
              (long) numOfBlocksPerContainer);
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
    if (data.getSchemaVersion().equals(SCHEMA_V1)) {
      return meta.getStore().getBlockDataTable()
          .getRangeKVs(null, 100, data.containerPrefix(),
              data.getDeletingBlockKeyFilter())
          .size();
    } else if (data.getSchemaVersion().equals(SCHEMA_V2)) {
      int pendingBlocks = 0;
      DatanodeStore ds = meta.getStore();
      DatanodeStoreSchemaTwoImpl dnStoreTwoImpl =
          (DatanodeStoreSchemaTwoImpl) ds;
      try (
          TableIterator<Long, ? extends Table.KeyValue<Long,
              StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction>>
              iter = dnStoreTwoImpl.getDeleteTransactionTable().iterator()) {
        while (iter.hasNext()) {
          StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction
              delTx = iter.next().getValue();
          pendingBlocks += delTx.getLocalIDList().size();
        }
      }
      return pendingBlocks;
    } else if (data.getSchemaVersion().equals(SCHEMA_V3)) {
      int pendingBlocks = 0;
      DatanodeStore ds = meta.getStore();
      DatanodeStoreSchemaThreeImpl dnStoreThreeImpl =
          (DatanodeStoreSchemaThreeImpl) ds;
      try (
          TableIterator<String, ? extends Table.KeyValue<String,
              StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction>>
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


  @Test
  public void testBlockDeletion() throws Exception {
    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    dnConf.setBlockDeletionLimit(2);
    this.blockLimitPerInterval = dnConf.getBlockDeletionLimit();
    conf.setFromObject(dnConf);
    ContainerSet containerSet = new ContainerSet(1000);
    createToDeleteBlocks(containerSet, 1, 3, 1);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        new KeyValueHandler(conf, datanodeUuid, containerSet, volumeSet,
            metrics, c -> {
        });
    BlockDeletingServiceTestImpl svc =
        getBlockDeletingService(containerSet, conf, keyValueHandler);
    svc.start();
    BlockDeletingServiceMetrics deletingServiceMetrics = svc.getMetrics();
    GenericTestUtils.waitFor(svc::isStarted, 100, 3000);

    // Ensure 1 container was created
    List<ContainerData> containerData = Lists.newArrayList();
    containerSet.listContainer(0L, 1, containerData);
    Assert.assertEquals(1, containerData.size());
    KeyValueContainerData data = (KeyValueContainerData) containerData.get(0);
    KeyPrefixFilter filter = Objects.equals(schemaVersion, SCHEMA_V1) ?
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
      Assert.assertEquals(0, transactionId);

      // Ensure there are 3 blocks under deletion and 0 deleted blocks
      Assert.assertEquals(3, getUnderDeletionBlocksCount(meta, data));
      Assert.assertEquals(3, meta.getStore().getMetadataTable()
          .get(data.pendingDeleteBlockCountKey()).longValue());

      // Container contains 3 blocks. So, space used by the container
      // should be greater than zero.
      Assert.assertTrue(containerSpace > 0);

      // An interval will delete 1 * 2 blocks
      deleteAndWait(svc, 1);

      GenericTestUtils.waitFor(() ->
              containerData.get(0).getBytesUsed() == containerSpace /
                      3, 100, 3000);
      // After first interval 2 blocks will be deleted. Hence, current space
      // used by the container should be less than the space used by the
      // container initially(before running deletion services).
      Assert.assertTrue(containerData.get(0).getBytesUsed() < containerSpace);
      Assert.assertEquals(2,
          deletingServiceMetrics.getSuccessCount()
              - deleteSuccessCount);

      deleteAndWait(svc, 2);

      // After deletion of all 3 blocks, space used by the containers
      // should be zero.
      GenericTestUtils.waitFor(() ->
              containerData.get(0).getBytesUsed() == 0, 100, 3000);

      // Check finally DB counters.
      // Not checking bytes used, as handler is a mock call.
      Assert.assertEquals(0, meta.getStore().getMetadataTable()
          .get(data.pendingDeleteBlockCountKey()).longValue());
      Assert.assertEquals(0,
          meta.getStore().getMetadataTable().get(data.blockCountKey())
              .longValue());
      Assert.assertEquals(3,
          deletingServiceMetrics.getSuccessCount()
              - deleteSuccessCount);

      // check if blockData get deleted
      assertBlockDataTableRecordCount(0, meta, filter, data.getContainerID());
    }

    svc.shutdown();
  }

  @Test
  public void testWithUnrecordedBlocks() throws Exception {
    // Skip schemaV1, when markBlocksForDeletionSchemaV1, the unrecorded blocks
    // from received TNXs will be deleted, not in BlockDeletingService
    Assume.assumeFalse(Objects.equals(schemaVersion, SCHEMA_V1));

    int numOfContainers = 2;
    int numOfChunksPerBlock = 1;
    int numOfBlocksPerContainer = 3;
    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    dnConf.setBlockDeletionLimit(2);
    this.blockLimitPerInterval = dnConf.getBlockDeletionLimit();
    conf.setFromObject(dnConf);
    ContainerSet containerSet = new ContainerSet(1000);

    createToDeleteBlocks(containerSet, numOfContainers, numOfBlocksPerContainer,
        numOfChunksPerBlock);

    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        new KeyValueHandler(conf, datanodeUuid, containerSet, volumeSet,
            metrics, c -> {
        });
    BlockDeletingServiceTestImpl svc =
        getBlockDeletingService(containerSet, conf, keyValueHandler);
    svc.start();
    GenericTestUtils.waitFor(svc::isStarted, 100, 3000);

    // Ensure 2 container was created
    List<ContainerData> containerData = Lists.newArrayList();
    containerSet.listContainer(0L, 2, containerData);
    Assert.assertEquals(2, containerData.size());
    KeyValueContainerData ctr1 = (KeyValueContainerData) containerData.get(0);
    KeyValueContainerData ctr2 = (KeyValueContainerData) containerData.get(1);
    KeyPrefixFilter filter = Objects.equals(schemaVersion, SCHEMA_V1) ?
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
        Assert.assertTrue(chunk.exists());
      }

      createTxn(ctr1, unrecordedBlockIds, 100, ctr1.getContainerID());
      ctr1.updateDeleteTransactionId(100);
      ctr1.incrPendingDeletionBlocks(numUnrecordedBlocks);
      updateMetaData(ctr1, (KeyValueContainer) containerSet.getContainer(
          ctr1.getContainerID()), 3, 1);
      // Ensure there are 3 + 4 = 7 blocks under deletion
      Assert.assertEquals(7, getUnderDeletionBlocksCount(meta, ctr1));
    }

    assertBlockDataTableRecordCount(3, ctr1, filter);
    assertBlockDataTableRecordCount(3, ctr2, filter);
    Assert.assertEquals(3, ctr2.getNumPendingDeletionBlocks());

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
    Assert.assertEquals(0, ctr1.getBlockCount());
    Assert.assertEquals(0, ctr1.getBytesUsed());
    Assert.assertEquals(0, ctr2.getBlockCount());
    Assert.assertEquals(0, ctr2.getBytesUsed());

    // check if blockData get deleted
    assertBlockDataTableRecordCount(0, ctr1, filter);
    assertBlockDataTableRecordCount(0, ctr2, filter);

    // check if all the unreferenced chunks get deleted
    for (File f: unrecordedChunks) {
      Assert.assertFalse(f.exists());
    }

    svc.shutdown();
  }

  @Test
  @SuppressWarnings("java:S2699") // waitFor => assertion with timeout
  public void testShutdownService() throws Exception {
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 500,
        TimeUnit.MILLISECONDS);
    conf.setInt(OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL, 10);
    conf.setInt(OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER, 10);

    ContainerSet containerSet = new ContainerSet(1000);
    // Create 1 container with 100 blocks
    createToDeleteBlocks(containerSet, 1, 100, 1);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        new KeyValueHandler(conf, datanodeUuid, containerSet, volumeSet,
            metrics, c -> {
        });
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

  @Test
  public void testBlockDeletionTimeout() throws Exception {
    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    dnConf.setBlockDeletionLimit(3);
    blockLimitPerInterval = dnConf.getBlockDeletionLimit();
    conf.setFromObject(dnConf);

    ContainerSet containerSet = new ContainerSet(1000);
    createToDeleteBlocks(containerSet, 1, 3, 1);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        new KeyValueHandler(conf, datanodeUuid, containerSet, volumeSet,
            metrics, c -> {
        });
    // set timeout value as 1ns to trigger timeout behavior
    long timeout  = 1;
    OzoneContainer ozoneContainer =
        mockDependencies(containerSet, keyValueHandler);
    BlockDeletingService svc = new BlockDeletingService(ozoneContainer,
        TimeUnit.MILLISECONDS.toNanos(1000), timeout, TimeUnit.NANOSECONDS,
        10, conf);
    svc.start();

    LogCapturer log = LogCapturer.captureLogs(BackgroundService.LOG);
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
        10, conf);
    svc.start();

    // get container meta data
    KeyValueContainer container =
        (KeyValueContainer) containerSet.getContainerIterator().next();
    KeyValueContainerData data = container.getContainerData();
    try (DBHandle meta = BlockUtils.getDB(data, conf)) {
      LogCapturer newLog = LogCapturer.captureLogs(BackgroundService.LOG);
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
      Assert.assertFalse(newLog.getOutput().contains(
          "Background task executes timed out, retrying in next interval"));
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

  @Test(timeout = 30000)
  @org.junit.Ignore
  public void testContainerThrottle() throws Exception {
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
    ContainerSet containerSet = new ContainerSet(1000);

    int containerCount = 2;
    int chunksPerBlock = 10;
    int blocksPerContainer = 1;
    createToDeleteBlocks(containerSet, containerCount, blocksPerContainer,
        chunksPerBlock);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        new KeyValueHandler(conf, datanodeUuid, containerSet, volumeSet,
            metrics, c -> {
        });
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

      Assert.assertFalse((containerData.get(0).getBytesUsed() == 0) && (
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

  public long currentBlockSpace(List<ContainerData> containerData,
      int totalContainers) {
    long totalSpaceUsed = 0;
    for (int i = 0; i < totalContainers; i++) {
      totalSpaceUsed += containerData.get(i).getBytesUsed();
    }
    return totalSpaceUsed;
  }

  @Test(timeout = 30000)
  public void testBlockThrottle() throws Exception {
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
    ContainerSet containerSet = new ContainerSet(1000);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        new KeyValueHandler(conf, datanodeUuid, containerSet, volumeSet,
            metrics, c -> {
        });
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
    BlockIterator<BlockData> iterator = handle.getStore().
        getBlockIterator(containerID, filter);
    iterator.seekToFirst();
    while (iterator.hasNext()) {
      iterator.nextBlock();
      count += 1;
    }
    Assert.assertEquals("Excepted: " + expectedCount
        + ", but actual: " + count + " in the blockData table of container: "
        + containerID + ".", expectedCount, count);
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
}
