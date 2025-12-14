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
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.EMPTY_SUMMARY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.common.BlockGroup.SIZE_NOT_AVAILABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBuffer;
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
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.common.DeletedBlock;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizationExecutor;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
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
  private static final int NUM_DATANODES = 3;
  private static final int NUM_SCMS = 3;
  private Future<?> finalizationFuture;
  private final String volumeName = UUID.randomUUID().toString();
  private final String bucketName = UUID.randomUUID().toString();
  private OzoneBucket bucket;
  private static final long BLOCK_SIZE = 1024 * 1024; // 1 MB
  private static final long BLOCKS_PER_TX = 5; // 1 MB

  public void init(OzoneConfiguration conf,
      UpgradeFinalizationExecutor<SCMUpgradeFinalizationContext> executor, boolean doFinalize) throws Exception {

    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setUpgradeFinalizationExecutor(executor);

    conf.setInt(SCMStorageConfig.TESTING_INIT_LAYOUT_VERSION_KEY, HDDSLayoutFeature.HBASE_SUPPORT.layoutVersion());
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    scmConfig.setBlockDeletionInterval(Duration.ofMillis(100));
    conf.setFromObject(scmConfig);
    conf.set(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "0s");

    DatanodeConfiguration dnConf =
        conf.getObject(DatanodeConfiguration.class);
    dnConf.setBlockDeletionInterval(Duration.ofMillis(100));
    conf.setFromObject(dnConf);

    MiniOzoneHAClusterImpl.Builder clusterBuilder = MiniOzoneCluster.newHABuilder(conf);
    clusterBuilder.setNumOfStorageContainerManagers(NUM_SCMS)
        .setNumOfActiveSCMs(NUM_SCMS)
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
    assertEquals(HDDSLayoutFeature.HBASE_SUPPORT.layoutVersion(),
        cluster.getStorageContainerManager().getLayoutVersionManager().getMetadataLayoutVersion());

    // Create Volume and Bucket
    try (OzoneClient ozoneClient = OzoneClientFactory.getRpcClient(conf)) {
      ObjectStore store = ozoneClient.getObjectStore();
      store.createVolume(volumeName);
      OzoneVolume volume = store.getVolume(volumeName);
      BucketArgs.Builder builder = BucketArgs.newBuilder();
      volume.createBucket(bucketName, builder.build());
      bucket = volume.getBucket(bucketName);
    }

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
  @Flaky("HDDS-14050")
  public void testFinalizationEmptyClusterDataDistribution() throws Exception {
    init(new OzoneConfiguration(), null, true);
    assertEquals(EMPTY_SUMMARY, cluster.getStorageContainerLocationClient().getDeletedBlockSummary());

    finalizationFuture.get();
    TestHddsUpgradeUtils.waitForFinalizationFromClient(scmClient, CLIENT_ID);
    // Make sure old leader has caught up and all SCMs have finalized.
    waitForScmsToFinalize(cluster.getStorageContainerManagersList());
    assertEquals(HDDSLayoutFeature.STORAGE_SPACE_DISTRIBUTION.layoutVersion(),
        cluster.getStorageContainerManager().getLayoutVersionManager().getMetadataLayoutVersion());

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        cluster.getStorageContainerManagersList(), 0, NUM_DATANODES);
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes(), 0, CLOSED);
    assertNotNull(cluster.getStorageContainerLocationClient().getDeletedBlockSummary());

    for (StorageContainerManager scm: cluster.getStorageContainerManagersList()) {
      DeletedBlockLogImpl deletedBlockLog = (DeletedBlockLogImpl) scm.getScmBlockManager().getDeletedBlockLog();
      SCMDeletedBlockTransactionStatusManager statusManager =
          deletedBlockLog.getSCMDeletedBlockTransactionStatusManager();
      HddsProtos.DeletedBlocksTransactionSummary summary = statusManager.getTransactionSummary();
      assertEquals(EMPTY_SUMMARY, summary);
    }

    long lastTxId = findLastTx();
    StorageContainerManager activeSCM = cluster.getActiveSCM();
    assertEquals(-1, lastTxId, "Last transaction ID should be -1");

    // generate old format deletion tx, summary should keep empty, total DB tx 4
    int txCount = 4;
    DeletedBlockLogImpl deletedBlockLog = (DeletedBlockLogImpl) activeSCM.getScmBlockManager().getDeletedBlockLog();
    deletedBlockLog.addTransactions(generateDeletedBlocks(txCount, false));
    flushDBTransactionBuffer(activeSCM);
    ArrayList<Long> txIdList = getRowsInTable(activeSCM.getScmMetadataStore().getDeletedBlocksTXTable());
    assertEquals(txCount, txIdList.size());

    SCMDeletedBlockTransactionStatusManager statusManager =
        deletedBlockLog.getSCMDeletedBlockTransactionStatusManager();
    HddsProtos.DeletedBlocksTransactionSummary summary = statusManager.getTransactionSummary();
    assertEquals(EMPTY_SUMMARY, summary);
    statusManager.removeTransactions(txIdList);

    // generate 4 new format deletion tx
    Map<Long, List<DeletedBlock>> txList = generateDeletedBlocks(txCount, true);
    deletedBlockLog.addTransactions(txList);
    flushDBTransactionBuffer(activeSCM);

    ArrayList<Long> txWithSizeList = getRowsInTable(activeSCM.getScmMetadataStore().getDeletedBlocksTXTable());
    assertEquals(txCount, txWithSizeList.size());
    summary = statusManager.getTransactionSummary();
    assertEquals(txCount, summary.getTotalTransactionCount());
    assertEquals(txCount * BLOCKS_PER_TX, summary.getTotalBlockCount());
    assertEquals(txCount * BLOCKS_PER_TX * BLOCK_SIZE, summary.getTotalBlockSize());
    assertEquals(txCount * BLOCKS_PER_TX * BLOCK_SIZE * 3, summary.getTotalBlockReplicatedSize());

    // wait for all transactions deleted by SCMBlockDeletingService
    GenericTestUtils.waitFor(() -> {
      try {
        flushDBTransactionBuffer(activeSCM);
        return getRowsInTable(activeSCM.getScmMetadataStore().getDeletedBlocksTXTable()).isEmpty();
      } catch (IOException e) {
        fail("Failed to get keys from DeletedBlocksTXTable", e);
        return false;
      }
    }, 100, 5000);

    // generate old format deletion tx, summary should keep the same
    deletedBlockLog.addTransactions(generateDeletedBlocks(txCount, false));
    flushDBTransactionBuffer(activeSCM);
    ArrayList<Long> txWithoutSizeList = getRowsInTable(activeSCM.getScmMetadataStore().getDeletedBlocksTXTable());
    assertEquals(txCount, txWithoutSizeList.size());
    summary = statusManager.getTransactionSummary();
    assertEquals(EMPTY_SUMMARY, summary);

    // delete old format deletion tx, summary should keep the same
    statusManager.removeTransactions(txWithoutSizeList);
    flushDBTransactionBuffer(activeSCM);
    summary = statusManager.getTransactionSummary();
    assertEquals(EMPTY_SUMMARY, summary);

    // delete already deleted new format txs again, summary should become nearly empty
    statusManager.removeTransactions(txWithSizeList);
    flushDBTransactionBuffer(activeSCM);
    summary = statusManager.getTransactionSummary();
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
    init(new OzoneConfiguration(), null, false);
    // stop SCMBlockDeletingService
    for (StorageContainerManager scm: cluster.getStorageContainerManagersList()) {
      scm.getScmBlockManager().getSCMBlockDeletingService().stop();
    }

    // write some tx
    int txCount = 2;
    StorageContainerManager activeSCM = cluster.getActiveSCM();
    activeSCM.getScmBlockManager().getDeletedBlockLog().addTransactions(generateDeletedBlocks(txCount, false));
    flushDBTransactionBuffer(activeSCM);
    assertEquals(EMPTY_SUMMARY, cluster.getStorageContainerLocationClient().getDeletedBlockSummary());

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
    assertEquals(HDDSLayoutFeature.STORAGE_SPACE_DISTRIBUTION.layoutVersion(),
        cluster.getStorageContainerManager().getLayoutVersionManager().getMetadataLayoutVersion());

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        cluster.getStorageContainerManagersList(), 0, NUM_DATANODES);
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes(), 0, CLOSED);
    assertNotNull(cluster.getStorageContainerLocationClient().getDeletedBlockSummary());

    for (StorageContainerManager scm: cluster.getStorageContainerManagersList()) {
      DeletedBlockLogImpl deletedBlockLog = (DeletedBlockLogImpl) scm.getScmBlockManager().getDeletedBlockLog();
      SCMDeletedBlockTransactionStatusManager statusManager =
          deletedBlockLog.getSCMDeletedBlockTransactionStatusManager();
      HddsProtos.DeletedBlocksTransactionSummary summary = statusManager.getTransactionSummary();
      assertEquals(EMPTY_SUMMARY, summary);
    }

    long lastTxId = findLastTx();
    assertNotEquals(-1, lastTxId, "Last transaction ID should not be -1");

    final String keyName = "key" + System.nanoTime();
    // Create the key
    String value = "sample value";
    TestDataUtil.createKey(bucket, keyName, ReplicationConfig.fromTypeAndFactor(RATIS, THREE), value.getBytes(UTF_8));
    // update scmInfo in OM
    OzoneKeyDetails keyDetails = bucket.getKey(keyName);
    // delete the key
    bucket.deleteKey(keyName);

    DeletedBlockLogImpl deletedBlockLog = (DeletedBlockLogImpl) activeSCM.getScmBlockManager().getDeletedBlockLog();
    SCMDeletedBlockTransactionStatusManager statusManager =
        deletedBlockLog.getSCMDeletedBlockTransactionStatusManager();
    GenericTestUtils.waitFor(
        () -> !EMPTY_SUMMARY.equals(statusManager.getTransactionSummary()), 100, 5000);
    HddsProtos.DeletedBlocksTransactionSummary summary = statusManager.getTransactionSummary();
    assertEquals(1, summary.getTotalTransactionCount());
    assertEquals(1, summary.getTotalBlockCount());
    assertEquals(value.getBytes(UTF_8).length, summary.getTotalBlockSize());
    assertEquals(value.getBytes(UTF_8).length * 3, summary.getTotalBlockReplicatedSize());

    // force close the container so that block can be deleted
    activeSCM.getClientProtocolServer().closeContainer(
        keyDetails.getOzoneKeyLocations().get(0).getContainerID());
    // wait for container to be closed
    GenericTestUtils.waitFor(() -> {
      try {
        return activeSCM.getClientProtocolServer().getContainer(
            keyDetails.getOzoneKeyLocations().get(0).getContainerID())
            .getState() == HddsProtos.LifeCycleState.CLOSED;
      } catch (IOException e) {
        fail("Error while checking container state", e);
        return false;
      }
    }, 100, 5000);

    // flush buffer and start SCMBlockDeletingService
    for (StorageContainerManager scm: cluster.getStorageContainerManagersList()) {
      flushDBTransactionBuffer(scm);
      scm.getScmBlockManager().getSCMBlockDeletingService().start();
    }

    // wait for block deletion transactions to be confirmed by DN
    GenericTestUtils.waitFor(
        () -> statusManager.getTransactionSummary().getTotalTransactionCount() == 0, 100, 30000);
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
          blocks.add(new DeletedBlock(new BlockID(containerID, localID), SIZE_NOT_AVAILABLE, SIZE_NOT_AVAILABLE));
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
