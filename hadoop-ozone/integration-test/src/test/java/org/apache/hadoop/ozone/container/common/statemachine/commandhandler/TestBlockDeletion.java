/*
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
package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.block.SCMBlockDeletingService;
import org.apache.hadoop.hdds.scm.block.ScmBlockDeletingServiceMetrics;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.replication.LegacyReplicationManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds
    .HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone
    .OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;

/**
 * Tests for Block deletion.
 */
public class TestBlockDeletion {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestBlockDeletion.class);

  private OzoneConfiguration conf = null;
  private ObjectStore store;
  private MiniOzoneCluster cluster = null;
  private StorageContainerManager scm = null;
  private OzoneManager om = null;
  private OzoneManagerProtocol writeClient;
  private Set<Long> containerIdsWithDeletedBlocks;
  private long maxTransactionId = 0;
  private ScmBlockDeletingServiceMetrics metrics;

  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    GenericTestUtils.setLogLevel(DeletedBlockLogImpl.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(SCMBlockDeletingService.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(LegacyReplicationManager.LOG, Level.DEBUG);

    conf.set("ozone.replication.allowed-configs", "^(RATIS/THREE)|(EC/2-1)$");
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    DatanodeConfiguration datanodeConfiguration = conf.getObject(
            DatanodeConfiguration.class);
    datanodeConfiguration.setBlockDeletionInterval(Duration.ofMillis(100));
    conf.setFromObject(datanodeConfiguration);
    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    scmConfig.setBlockDeletionInterval(Duration.ofMillis(100));
    conf.setFromObject(scmConfig);

    conf.setTimeDuration(RatisHelper.HDDS_DATANODE_RATIS_PREFIX_KEY
        + ".client.request.write.timeout", 30, TimeUnit.SECONDS);
    conf.setTimeDuration(RatisHelper.HDDS_DATANODE_RATIS_PREFIX_KEY
        + ".client.request.watch.timeout", 30, TimeUnit.SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
        3, TimeUnit.SECONDS);
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE,
        false);
    conf.setInt(OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 100);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setQuietMode(false);
    conf.setTimeDuration("hdds.scm.replication.event.timeout", 100,
        TimeUnit.MILLISECONDS);
    conf.setInt("hdds.datanode.block.delete.threads.max", 5);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setHbInterval(200)
        .build();
    cluster.waitForClusterToBeReady();
    store = OzoneClientFactory.getRpcClient(conf).getObjectStore();
    om = cluster.getOzoneManager();
    writeClient = cluster.getRpcClient().getObjectStore()
        .getClientProxy().getOzoneManagerClient();
    scm = cluster.getStorageContainerManager();
    containerIdsWithDeletedBlocks = new HashSet<>();
    metrics = scm.getScmBlockManager().getSCMBlockDeletingService()
        .getMetrics();
  }

  @AfterEach
  public void cleanup() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static Stream<ReplicationConfig> replicationConfigs() {
    return Stream.of(
        ReplicationConfig.fromTypeAndFactor(
            ReplicationType.RATIS, ReplicationFactor.THREE),
        new ECReplicationConfig("rs-2-1-256k"));
  }

  @ParameterizedTest
  @MethodSource("replicationConfigs")
  public void testBlockDeletion(ReplicationConfig repConfig) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = RandomStringUtils.random(1024 * 1024);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String keyName = UUID.randomUUID().toString();

    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(UTF_8).length, repConfig, new HashMap<>());
    for (int i = 0; i < 10; i++) {
      out.write(value.getBytes(UTF_8));
    }
    out.close();

    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName).setKeyName(keyName).setDataSize(0)
        .setReplicationConfig(repConfig)
        .build();
    List<OmKeyLocationInfoGroup> omKeyLocationInfoGroupList =
        om.lookupKey(keyArgs).getKeyLocationVersions();

    // verify key blocks were created in DN.
    GenericTestUtils.waitFor(() -> {
      try {
        verifyBlocksCreated(omKeyLocationInfoGroupList);
        return true;
      } catch (Throwable t) {
        LOG.warn("Verify blocks creation failed", t);
        return false;
      }
    }, 1000, 10000);
    // No containers with deleted blocks
    Assertions.assertTrue(containerIdsWithDeletedBlocks.isEmpty());
    // Delete transactionIds for the containers should be 0.
    // NOTE: this test assumes that all the container is KetValueContainer. If
    // other container types is going to be added, this test should be checked.
    matchContainerTransactionIds();

    Assertions.assertEquals(0L,
        metrics.getNumBlockDeletionTransactionCreated());
    writeClient.deleteKey(keyArgs);
    Thread.sleep(5000);
    // The blocks should not be deleted in the DN as the container is open
    Throwable e = Assertions.assertThrows(AssertionError.class,
        () -> verifyBlocksDeleted(omKeyLocationInfoGroupList));
    Assertions.assertTrue(
        e.getMessage().startsWith("expected: <null> but was:"));

    Assertions.assertEquals(0L, metrics.getNumBlockDeletionTransactionSent());
    // close the containers which hold the blocks for the key
    OzoneTestUtils.closeAllContainers(scm.getEventQueue(), scm);
    Thread.sleep(2000);
    // make sure the containers are closed on the dn
    omKeyLocationInfoGroupList.forEach((group) -> {
      List<OmKeyLocationInfo> locationInfo = group.getLocationList();
      locationInfo.forEach(
          (info) -> cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
              .getContainer().getContainerSet()
              .getContainer(info.getContainerID()).getContainerData()
              .setState(ContainerProtos.ContainerDataProto.State.CLOSED));
    });

    // The blocks should be deleted in the DN.
    GenericTestUtils.waitFor(() -> {
      try {
        verifyBlocksDeleted(omKeyLocationInfoGroupList);
        return true;
      } catch (Throwable t) {
        LOG.warn("Verify blocks deletion failed", t);
        return false;
      }
    }, 2000, 30000);

    // Few containers with deleted blocks
    Assertions.assertFalse(containerIdsWithDeletedBlocks.isEmpty());
    // Containers in the DN and SCM should have same delete transactionIds
    matchContainerTransactionIds();
    // Containers in the DN and SCM should have same delete transactionIds
    // after DN restart. The assertion is just to verify that the state of
    // containerInfos in dn and scm is consistent after dn restart.
    cluster.restartHddsDatanode(0, true);
    matchContainerTransactionIds();

    // Verify transactions committed
    GenericTestUtils.waitFor(() -> {
      try {
        verifyTransactionsCommitted();
        return true;
      } catch (Throwable t) {
        LOG.warn("Container closing failed", t);
        return false;
      }
    }, 500, 10000);
    Assertions.assertEquals(metrics.getNumBlockDeletionTransactionCreated(),
        metrics.getNumBlockDeletionTransactionCompleted());
    Assertions.assertTrue(metrics.getNumBlockDeletionCommandSent() >=
        metrics.getNumBlockDeletionCommandSuccess() +
            metrics.getBNumBlockDeletionCommandFailure());
    Assertions.assertTrue(metrics.getNumBlockDeletionTransactionSent() >=
        metrics.getNumBlockDeletionTransactionFailure() +
            metrics.getNumBlockDeletionTransactionSuccess());
    LOG.info(metrics.toString());
  }

  @Test
  public void testContainerStatisticsAfterDelete() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = RandomStringUtils.random(1024 * 1024);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String keyName = UUID.randomUUID().toString();
    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(UTF_8).length, ReplicationType.RATIS,
        ReplicationFactor.THREE, new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();

    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName).setKeyName(keyName).setDataSize(0)
        .setReplicationConfig(
            RatisReplicationConfig
                .getInstance(HddsProtos.ReplicationFactor.THREE))
        .build();
    List<OmKeyLocationInfoGroup> omKeyLocationInfoGroupList =
        om.lookupKey(keyArgs).getKeyLocationVersions();
    Thread.sleep(5000);
    List<ContainerInfo> containerInfos =
        scm.getContainerManager().getContainers();
    final int valueSize = value.getBytes(UTF_8).length;
    final int keyCount = 1;
    containerInfos.stream().forEach(container -> {
      Assertions.assertEquals(valueSize, container.getUsedBytes());
      Assertions.assertEquals(keyCount, container.getNumberOfKeys());
    });

    OzoneTestUtils.closeAllContainers(scm.getEventQueue(), scm);
    // Wait for container to close
    Thread.sleep(2000);
    // make sure the containers are closed on the dn
    omKeyLocationInfoGroupList.forEach((group) -> {
      List<OmKeyLocationInfo> locationInfo = group.getLocationList();
      locationInfo.forEach(
          (info) -> cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
              .getContainer().getContainerSet()
              .getContainer(info.getContainerID()).getContainerData()
              .setState(ContainerProtos.ContainerDataProto.State.CLOSED));
    });

    writeClient.deleteKey(keyArgs);
    // Wait for blocks to be deleted and container reports to be processed
    Thread.sleep(5000);
    containerInfos = scm.getContainerManager().getContainers();
    containerInfos.stream().forEach(container -> {
      Assertions.assertEquals(0, container.getUsedBytes());
      Assertions.assertEquals(0, container.getNumberOfKeys());
    });
    // Verify that pending block delete num are as expected with resent cmds
    cluster.getHddsDatanodes().forEach(dn -> {
      Map<Long, Container<?>> containerMap = dn.getDatanodeStateMachine()
          .getContainer().getContainerSet().getContainerMap();
      containerMap.values().forEach(container -> {
        KeyValueContainerData containerData =
            (KeyValueContainerData)container.getContainerData();
        Assertions.assertEquals(0, containerData.getNumPendingDeletionBlocks());
      });
    });

    cluster.shutdownHddsDatanode(0);
    scm.getReplicationManager().processAll();
    ((EventQueue)scm.getEventQueue()).processAll(1000);
    containerInfos = scm.getContainerManager().getContainers();
    containerInfos.stream().forEach(container ->
        Assertions.assertEquals(HddsProtos.LifeCycleState.DELETING,
            container.getState()));
    LogCapturer logCapturer =
        LogCapturer.captureLogs(LegacyReplicationManager.LOG);
    logCapturer.clearOutput();

    Thread.sleep(2000);
    scm.getReplicationManager().processAll();
    ((EventQueue)scm.getEventQueue()).processAll(1000);
    GenericTestUtils.waitFor(() -> logCapturer.getOutput()
        .contains("Resend delete Container"), 500, 5000);
    cluster.restartHddsDatanode(0, true);
    Thread.sleep(2000);

    scm.getReplicationManager().processAll();
    ((EventQueue)scm.getEventQueue()).processAll(1000);
    GenericTestUtils.waitFor(() -> {
      List<ContainerInfo> infos = scm.getContainerManager().getContainers();
      try {
        infos.stream().forEach(container -> {
          Assertions.assertEquals(HddsProtos.LifeCycleState.DELETED,
              container.getState());
          try {
            Assertions.assertEquals(HddsProtos.LifeCycleState.DELETED,
                scm.getScmMetadataStore().getContainerTable()
                    .get(container.containerID()).getState());
          } catch (IOException e) {
            Assertions.fail(
                "Container from SCM DB should be marked as DELETED");
          }
        });
      } catch (Throwable e) {
        return false;
      }
      return true;
    }, 500, 5000);
    LOG.info(metrics.toString());
  }

  private void verifyTransactionsCommitted() throws IOException {
    scm.getScmBlockManager().getDeletedBlockLog();
    for (long txnID = 1; txnID <= maxTransactionId; txnID++) {
      Assertions.assertNull(
          scm.getScmMetadataStore().getDeletedBlocksTXTable().get(txnID));
    }
  }

  private void matchContainerTransactionIds() throws IOException {
    for (HddsDatanodeService datanode : cluster.getHddsDatanodes()) {
      ContainerSet dnContainerSet =
          datanode.getDatanodeStateMachine().getContainer().getContainerSet();
      List<ContainerData> containerDataList = new ArrayList<>();
      dnContainerSet.listContainer(0, 10000, containerDataList);
      for (ContainerData containerData : containerDataList) {
        long containerId = containerData.getContainerID();
        if (containerIdsWithDeletedBlocks.contains(containerId)) {
          Assertions.assertTrue(
              scm.getContainerInfo(containerId).getDeleteTransactionId() > 0);
          maxTransactionId = max(maxTransactionId,
              scm.getContainerInfo(containerId).getDeleteTransactionId());
        } else {
          Assertions.assertEquals(
              scm.getContainerInfo(containerId).getDeleteTransactionId(), 0);
        }
        Assertions.assertEquals(
            ((KeyValueContainerData) dnContainerSet.getContainer(containerId)
                .getContainerData()).getDeleteTransactionId(),
            scm.getContainerInfo(containerId).getDeleteTransactionId());
      }
    }
  }

  private void verifyBlocksCreated(
      List<OmKeyLocationInfoGroup> omKeyLocationInfoGroups) throws Exception {
    for (HddsDatanodeService datanode : cluster.getHddsDatanodes()) {
      ContainerSet dnContainerSet =
          datanode.getDatanodeStateMachine().getContainer().getContainerSet();
      OzoneTestUtils.performOperationOnKeyContainers((blockID) -> {
        KeyValueContainerData cData = (KeyValueContainerData) dnContainerSet
            .getContainer(blockID.getContainerID()).getContainerData();
        try (DBHandle db = BlockUtils.getDB(cData, conf)) {
          Assertions.assertNotNull(db.getStore().getBlockDataTable()
              .get(cData.blockKey(blockID.getLocalID())));
        }
      }, omKeyLocationInfoGroups);
    }
  }

  private void verifyBlocksDeleted(
      List<OmKeyLocationInfoGroup> omKeyLocationInfoGroups) throws Exception {
    for (HddsDatanodeService datanode : cluster.getHddsDatanodes()) {
      ContainerSet dnContainerSet =
          datanode.getDatanodeStateMachine().getContainer().getContainerSet();
      OzoneTestUtils.performOperationOnKeyContainers((blockID) -> {
        KeyValueContainerData cData = (KeyValueContainerData) dnContainerSet
            .getContainer(blockID.getContainerID()).getContainerData();
        try (DBHandle db = BlockUtils.getDB(cData, conf)) {
          Table<String, BlockData> blockDataTable =
              db.getStore().getBlockDataTable();

          String blockKey = cData.blockKey(blockID.getLocalID());

          BlockData blockData = blockDataTable.get(blockKey);
          Assertions.assertNull(blockData);

          String deletingKey = cData.deletingBlockKey(blockID.getLocalID());
          Assertions.assertNull(blockDataTable.get(deletingKey));
        }
        containerIdsWithDeletedBlocks.add(blockID.getContainerID());
      }, omKeyLocationInfoGroups);
    }
  }

  @Test
  public void testBlockDeleteCommandParallelProcess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = RandomStringUtils.random(64 * 1024);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    int keyCount = 10;
    List<String> keys = new ArrayList<>();
    for (int j = 0; j < keyCount; j++) {
      String keyName = UUID.randomUUID().toString();
      OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes(UTF_8).length, ReplicationType.RATIS,
          ReplicationFactor.THREE, new HashMap<>());
      out.write(value.getBytes(UTF_8));
      out.close();
      keys.add(keyName);
    }

    // close the containers which hold the blocks for the key
    OzoneTestUtils.closeAllContainers(scm.getEventQueue(), scm);
    Thread.sleep(2000);

    for (int j = 0; j < keyCount; j++) {
      OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
          .setBucketName(bucketName).setKeyName(keys.get(j)).setDataSize(0)
          .setReplicationConfig(
              RatisReplicationConfig
                  .getInstance(HddsProtos.ReplicationFactor.THREE))
          .build();
      writeClient.deleteKey(keyArgs);
    }

    // Wait for block delete command sent from OM
    GenericTestUtils.waitFor(() -> {
      try {
        if (scm.getScmBlockManager().getDeletedBlockLog()
            .getNumOfValidTransactions() > 0) {
          return true;
        }
      } catch (IOException e) {
      }
      return false;
    }, 100, 5000);

    long start = System.currentTimeMillis();
    // Wait for all blocks been deleted.
    GenericTestUtils.waitFor(() -> {
      try {
        if (scm.getScmBlockManager().getDeletedBlockLog()
            .getNumOfValidTransactions() == 0) {
          return true;
        }
      } catch (IOException e) {
      }
      return false;
    }, 100, 30000);
    long end = System.currentTimeMillis();
    System.out.println("Block deletion costs " + (end - start) + "ms");
  }
}
