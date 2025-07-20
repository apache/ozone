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

package org.apache.hadoop.hdds.upgrade;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.scm.block.DeletedBlockLogStateManagerImpl.EMPTY_SUMMARY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogStateManagerImpl;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBuffer;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationCheckpoint;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizationContext;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.common.DeletedBlock;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizationExecutor;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests upgrade finalization failure scenarios and corner cases specific to SCM data distribution feature.
 */
public class TestScmDataDistributionFinalization {
  private static final String CLIENT_ID = UUID.randomUUID().toString();
  private static final Logger LOG =
      LoggerFactory.getLogger(TestScmDataDistributionFinalization.class);

  private StorageContainerLocationProtocol scmClient;
  private MiniOzoneHAClusterImpl cluster;
  private OzoneClient ozoneClient;
  private ObjectStore store;
  private static final int NUM_DATANODES = 3;
  private static final int NUM_SCMS = 3;
  private Future<?> finalizationFuture;
  private final String volumeName = UUID.randomUUID().toString();
  private final String bucketName = UUID.randomUUID().toString();
  private OzoneBucket bucket;
  private static final long BLOCK_SIZE = 1024 * 1024; // 1 MB
  private static final long BLOCKS_PER_TX = 5; // 1 MB

  public void init(OzoneConfiguration conf,
      UpgradeFinalizationExecutor<SCMUpgradeFinalizationContext> executor,
      int numInactiveSCMs, boolean doFinalize) throws Exception {

    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setUpgradeFinalizationExecutor(executor);

    conf.setInt(SCMStorageConfig.TESTING_INIT_LAYOUT_VERSION_KEY, HDDSLayoutFeature.HBASE_SUPPORT.layoutVersion());
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);

    conf.set(ScmConfigKeys.OZONE_SCM_HA_RATIS_SERVER_RPC_FIRST_ELECTION_TIMEOUT, "5s");

    MiniOzoneHAClusterImpl.Builder clusterBuilder = MiniOzoneCluster.newHABuilder(conf);
    clusterBuilder.setNumOfStorageContainerManagers(NUM_SCMS)
        .setNumOfActiveSCMs(NUM_SCMS - numInactiveSCMs)
        .setSCMServiceId("scmservice")
        .setOMServiceId("omServiceId")
        .setNumOfOzoneManagers(1)
        .setSCMConfigurator(configurator)
        .setNumDatanodes(NUM_DATANODES)
        .setDatanodeFactory(UniformDatanodesFactory.newBuilder()
            .setLayoutVersion(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion())
            .build());
    this.cluster = clusterBuilder.build();

    scmClient = cluster.getStorageContainerLocationClient();
    cluster.waitForClusterToBeReady();
    assertEquals(HDDSLayoutFeature.HBASE_SUPPORT.layoutVersion(), scmClient.getScmInfo().getMetaDataLayoutVersion());
    ozoneClient = OzoneClientFactory.getRpcClient(conf);
    store = ozoneClient.getObjectStore();

    // Create Volume and Bucket
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    volume.createBucket(bucketName, builder.build());
    bucket = volume.getBucket(bucketName);

    // Launch finalization from the client. In the current implementation,
    // this call will block until finalization completes. If the test
    // involves restarts or leader changes the client may be disconnected,
    // but finalization should still proceed.
    if (doFinalize) {
      finalizationFuture = Executors.newSingleThreadExecutor().submit(
          () -> {
            try {
              scmClient.finalizeScmUpgrade(CLIENT_ID);
            } catch (IOException ex) {
              LOG.info("finalization client failed. This may be expected if the" +
                  " test injected failures.", ex);
            }
          });
    }
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test for an empty cluster.
   */
  @Test
  public void testFinalizationEmptyClusterDataDistribution() throws Exception {
    init(new OzoneConfiguration(), null, 0, true);

    finalizationFuture.get();
    TestHddsUpgradeUtils.waitForFinalizationFromClient(scmClient, CLIENT_ID);
    // Make sure old leader has caught up and all SCMs have finalized.
    waitForScmsToFinalize(cluster.getStorageContainerManagersList());
    assertEquals(HDDSLayoutFeature.DATA_DISTRIBUTION.layoutVersion(),
        scmClient.getScmInfo().getMetaDataLayoutVersion());

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        cluster.getStorageContainerManagersList(), 0, NUM_DATANODES);
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes(), 0, CLOSED);
    for (StorageContainerManager scm: cluster.getStorageContainerManagersList()) {
      DeletedBlockLogImpl deletedBlockLog = (DeletedBlockLogImpl) scm.getScmBlockManager().getDeletedBlockLog();
      SCMHAInvocationHandler handler =
          (SCMHAInvocationHandler) Proxy.getInvocationHandler(deletedBlockLog.getDeletedBlockLogStateManager());
      DeletedBlockLogStateManagerImpl deletedBlockLogStateManager =
          (DeletedBlockLogStateManagerImpl) handler.getLocalHandler();
      HddsProtos.DeletedBlocksTransactionSummary summary = deletedBlockLogStateManager.getTransactionSummary();
      assertEquals(EMPTY_SUMMARY, summary);
    }

    long lastTxId = findLastTx();
    StorageContainerManager activeSCM = cluster.getActiveSCM();
    assertEquals(-1, lastTxId, "Last transaction ID should be -1");

    // generate old format deletion tx, summary should keep empty, total DB tx 4
    int txCount = 4;
    activeSCM.getScmBlockManager().getDeletedBlockLog().addTransactions(generateDeletedBlocks(txCount, false));
    flushDBTransactionBuffer(activeSCM);
    ArrayList<Long> txIdList = getRowsInTable(activeSCM.getScmMetadataStore().getDeletedBlocksTXTable());
    assertEquals(txCount, txIdList.size());
    DeletedBlockLogImpl deletedBlockLog = (DeletedBlockLogImpl) activeSCM.getScmBlockManager().getDeletedBlockLog();
    SCMHAInvocationHandler handler =
        (SCMHAInvocationHandler) Proxy.getInvocationHandler(deletedBlockLog.getDeletedBlockLogStateManager());
    DeletedBlockLogStateManagerImpl deletedBlockLogStateManager =
        (DeletedBlockLogStateManagerImpl) handler.getLocalHandler();
    HddsProtos.DeletedBlocksTransactionSummary summary = deletedBlockLogStateManager.getTransactionSummary();
    assertEquals(EMPTY_SUMMARY, summary);
    deletedBlockLogStateManager.removeTransactionsFromDB(txIdList);

    // generate new deletion tx, summary should be updated, total DB tx 4
    lastTxId = findLastTx();
    activeSCM.getScmBlockManager().getDeletedBlockLog().addTransactions(generateDeletedBlocks(txCount, true));
    flushDBTransactionBuffer(activeSCM);
    ArrayList<Long> txWithSizeList = getRowsInTable(activeSCM.getScmMetadataStore().getDeletedBlocksTXTable());
    assertEquals(txCount, txWithSizeList.size());

    summary = deletedBlockLogStateManager.getTransactionSummary();
    assertEquals(lastTxId + 1, summary.getFirstTxID());
    assertEquals(txCount, summary.getTotalTransactionCount());
    assertEquals(txCount * BLOCKS_PER_TX, summary.getTotalBlockCount());
    assertEquals(txCount * BLOCKS_PER_TX * BLOCK_SIZE, summary.getTotalBlockSize());
    assertEquals(txCount * BLOCKS_PER_TX * BLOCK_SIZE * 3, summary.getTotalBlockReplicatedSize());

    // delete first half of txs and verify summary, total DB tx 2
    txIdList = txWithSizeList.stream().limit(txCount / 2).collect(Collectors.toCollection(ArrayList::new));
    assertEquals(txCount / 2, txIdList.size());
    deletedBlockLogStateManager.removeTransactionsFromDB(txIdList);
    flushDBTransactionBuffer(activeSCM);
    txWithSizeList = getRowsInTable(activeSCM.getScmMetadataStore().getDeletedBlocksTXTable());
    assertEquals(txCount / 2, txWithSizeList.size());
    summary = deletedBlockLogStateManager.getTransactionSummary();
    assertEquals(lastTxId + 1, summary.getFirstTxID());
    assertEquals(txCount / 2, summary.getTotalTransactionCount());
    assertEquals(txCount * BLOCKS_PER_TX / 2, summary.getTotalBlockCount());
    assertEquals(txCount * BLOCKS_PER_TX * BLOCK_SIZE / 2, summary.getTotalBlockSize());
    assertEquals(txCount * BLOCKS_PER_TX * BLOCK_SIZE * 3 / 2, summary.getTotalBlockReplicatedSize());

    // generate old format deletion tx, summary should keep the same, total DB tx 6
    activeSCM.getScmBlockManager().getDeletedBlockLog().addTransactions(generateDeletedBlocks(txCount, false));
    flushDBTransactionBuffer(activeSCM);
    txIdList = getRowsInTable(activeSCM.getScmMetadataStore().getDeletedBlocksTXTable());
    assertEquals(txCount + txCount / 2, txIdList.size());
    txIdList.removeAll(txWithSizeList);
    ArrayList<Long> txWithoutSizeList = txIdList;

    summary = deletedBlockLogStateManager.getTransactionSummary();
    assertEquals(lastTxId + 1, summary.getFirstTxID());
    assertEquals(txCount / 2, summary.getTotalTransactionCount());
    assertEquals(txCount * BLOCKS_PER_TX / 2, summary.getTotalBlockCount());
    assertEquals(txCount * BLOCKS_PER_TX * BLOCK_SIZE / 2, summary.getTotalBlockSize());
    assertEquals(txCount * BLOCKS_PER_TX * BLOCK_SIZE * 3 / 2, summary.getTotalBlockReplicatedSize());

    // delete old format deletion tx, summary should keep the same
    deletedBlockLogStateManager.removeTransactionsFromDB(txWithoutSizeList);
    flushDBTransactionBuffer(activeSCM);
    summary = deletedBlockLogStateManager.getTransactionSummary();
    assertEquals(lastTxId + 1, summary.getFirstTxID());
    assertEquals(txCount / 2, summary.getTotalTransactionCount());
    assertEquals(txCount * BLOCKS_PER_TX / 2, summary.getTotalBlockCount());
    assertEquals(txCount * BLOCKS_PER_TX * BLOCK_SIZE / 2, summary.getTotalBlockSize());
    assertEquals(txCount * BLOCKS_PER_TX * BLOCK_SIZE * 3 / 2, summary.getTotalBlockReplicatedSize());

    // delete remaining txs, summary should become nearly empty
    deletedBlockLogStateManager.removeTransactionsFromDB(txWithSizeList);
    flushDBTransactionBuffer(activeSCM);
    summary = deletedBlockLogStateManager.getTransactionSummary();
    assertEquals(lastTxId + 1, summary.getFirstTxID());
    assertEquals(0, summary.getTotalTransactionCount());
    assertEquals(0, summary.getTotalBlockCount());
    assertEquals(0, summary.getTotalBlockSize());
    assertEquals(0, summary.getTotalBlockReplicatedSize());

    // delete remaining txs twice, summary should keep the same
    deletedBlockLogStateManager.removeTransactionsFromDB(txWithSizeList);
    flushDBTransactionBuffer(activeSCM);
    summary = deletedBlockLogStateManager.getTransactionSummary();
    assertEquals(lastTxId + 1, summary.getFirstTxID());
    assertEquals(0, summary.getTotalTransactionCount());
    assertEquals(0, summary.getTotalBlockCount());
    assertEquals(0, summary.getTotalBlockSize());
    assertEquals(0, summary.getTotalBlockReplicatedSize());
  }

  /**
   * Test for none empty cluster.
   */
  @Test
  public void testFinalizationNonEmptyClusterDataDistribution() throws Exception {
    init(new OzoneConfiguration(), null, 0, false);
    // stop SCMBlockDeletingService
    for (StorageContainerManager scm: cluster.getStorageContainerManagersList()) {
      scm.getScmBlockManager().getSCMBlockDeletingService().stop();
    }

    // write some tx
    int txCount = 2;
    StorageContainerManager activeSCM = cluster.getActiveSCM();
    activeSCM.getScmBlockManager().getDeletedBlockLog().addTransactions(generateDeletedBlocks(txCount, false));
    ((SCMHADBTransactionBuffer)activeSCM.getScmHAManager().getDBTransactionBuffer()).flush();

    finalizationFuture = Executors.newSingleThreadExecutor().submit(
        () -> {
          try {
            scmClient.finalizeScmUpgrade(CLIENT_ID);
          } catch (IOException ex) {
            LOG.info("finalization client failed. This may be expected if the" +
                " test injected failures.", ex);
          }
        });
    finalizationFuture.get();
    TestHddsUpgradeUtils.waitForFinalizationFromClient(scmClient, CLIENT_ID);
    // Make sure old leader has caught up and all SCMs have finalized.
    waitForScmsToFinalize(cluster.getStorageContainerManagersList());
    assertEquals(HDDSLayoutFeature.DATA_DISTRIBUTION.layoutVersion(),
        scmClient.getScmInfo().getMetaDataLayoutVersion());

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        cluster.getStorageContainerManagersList(), 0, NUM_DATANODES);
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes(), 0, CLOSED);

    for (StorageContainerManager scm: cluster.getStorageContainerManagersList()) {
      DeletedBlockLogImpl deletedBlockLog = (DeletedBlockLogImpl) scm.getScmBlockManager().getDeletedBlockLog();
      SCMHAInvocationHandler handler =
          (SCMHAInvocationHandler) Proxy.getInvocationHandler(deletedBlockLog.getDeletedBlockLogStateManager());
      DeletedBlockLogStateManagerImpl deletedBlockLogStateManager =
          (DeletedBlockLogStateManagerImpl) handler.getLocalHandler();
      HddsProtos.DeletedBlocksTransactionSummary summary = deletedBlockLogStateManager.getTransactionSummary();
      assertEquals(EMPTY_SUMMARY, summary);
    }

    long lastTxId = findLastTx();
    assertNotEquals(-1, lastTxId, "Last transaction ID should not be -1");

    final String keyName = "key" + System.nanoTime();
    // Create the key
    String value = "sample value";
    TestDataUtil.createKey(bucket, keyName, ReplicationConfig.fromTypeAndFactor(RATIS, THREE), value.getBytes(UTF_8));
    // restart OM to get new scmInfo
    cluster.restartOzoneManager();
    cluster.waitForLeaderOM();
    // delete the key
    bucket.deleteKey(keyName);

    DeletedBlockLogImpl deletedBlockLog = (DeletedBlockLogImpl) activeSCM.getScmBlockManager().getDeletedBlockLog();
    SCMHAInvocationHandler handler =
        (SCMHAInvocationHandler) Proxy.getInvocationHandler(deletedBlockLog.getDeletedBlockLogStateManager());
    DeletedBlockLogStateManagerImpl deletedBlockLogStateManager =
        (DeletedBlockLogStateManagerImpl) handler.getLocalHandler();

    GenericTestUtils.waitFor(
        () -> !EMPTY_SUMMARY.equals(deletedBlockLogStateManager.getTransactionSummary()), 100, 5000);
    HddsProtos.DeletedBlocksTransactionSummary summary = deletedBlockLogStateManager.getTransactionSummary();
    assertEquals(lastTxId + 1, summary.getFirstTxID());
    assertEquals(1, summary.getTotalTransactionCount());
    assertEquals(1, summary.getTotalBlockCount());
    assertEquals(value.getBytes(UTF_8).length, summary.getTotalBlockSize());
    assertEquals(value.getBytes(UTF_8).length * 3, summary.getTotalBlockReplicatedSize());
  }

  private Map<Long, List<DeletedBlock>> generateDeletedBlocks(int dataSize, boolean withSize) {
    Map<Long, List<DeletedBlock>> blockMap = new HashMap<>();
    int continerIDBase = RandomUtils.secure().randomInt(0, 100);
    int localIDBase = RandomUtils.secure().randomInt(0, 1000);
    for (int i = 0; i < dataSize; i++) {
      long containerID = continerIDBase + i;
      List<DeletedBlock> blocks = new ArrayList<>();
      for (int j = 0; j < BLOCKS_PER_TX; j++)  {
        long localID = localIDBase + j;
        if (withSize) {
          blocks.add(new DeletedBlock(new BlockID(containerID, localID), BLOCK_SIZE, BLOCK_SIZE * 3));
        } else {
          blocks.add(new DeletedBlock(new BlockID(containerID, localID), -1, -1));
        }
      }
      blockMap.put(containerID, blocks);
    }
    return blockMap;
  }

  private long findLastTx() throws RocksDatabaseException, CodecException {
    StorageContainerManager activeSCM = cluster.getActiveSCM();
    long lastTxId = -1;
    try (Table.KeyValueIterator<Long, DeletedBlocksTransaction> iter =
             activeSCM.getScmMetadataStore().getDeletedBlocksTXTable().iterator()) {
      while (iter.hasNext()) {
        Table.KeyValue<Long, DeletedBlocksTransaction> entry = iter.next();
        if (lastTxId < entry.getKey()) {
          lastTxId = entry.getKey();
        }
      }
    }
    return lastTxId;
  }

  private void waitForScmsToFinalize(Collection<StorageContainerManager> scms)
      throws Exception {
    for (StorageContainerManager scm: scms) {
      waitForScmToFinalize(scm);
    }
  }

  private void waitForScmToFinalize(StorageContainerManager scm)
      throws Exception {
    GenericTestUtils.waitFor(() -> !scm.isInSafeMode(), 500, 5000);
    GenericTestUtils.waitFor(() -> {
      FinalizationCheckpoint checkpoint =
          scm.getScmContext().getFinalizationCheckpoint();
      LOG.info("Waiting for SCM {} (leader? {}) to finalize. Current " +
          "finalization checkpoint is {}",
          scm.getSCMNodeId(), scm.checkLeader(), checkpoint);
      return checkpoint.hasCrossed(
          FinalizationCheckpoint.FINALIZATION_COMPLETE);
    }, 2_000, 60_000);
  }

  private void flushDBTransactionBuffer(StorageContainerManager scm) throws IOException {
    DBTransactionBuffer dbTxBuffer = scm.getScmHAManager().getDBTransactionBuffer();
    if (dbTxBuffer instanceof SCMHADBTransactionBuffer) {
      SCMHADBTransactionBuffer buffer = (SCMHADBTransactionBuffer) dbTxBuffer;
      buffer.flush();
    }
  }

  private ArrayList<Long> getRowsInTable(Table<Long, DeletedBlocksTransaction> table)
      throws IOException {
    ArrayList<Long> txIdList = new ArrayList<>();
    if (table != null) {
      try (Table.KeyValueIterator<Long, DeletedBlocksTransaction> keyValueTableIterator = table.iterator()) {
        while (keyValueTableIterator.hasNext()) {
          txIdList.add(keyValueTableIterator.next().getKey());
        }
      }
    }
    return txIdList;
  }
}
