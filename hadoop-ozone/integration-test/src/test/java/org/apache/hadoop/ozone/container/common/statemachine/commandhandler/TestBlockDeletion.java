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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
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
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
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
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
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
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
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
  private Set<Long> containerIdsWithDeletedBlocks;
  private long maxTransactionId = 0;
  private File baseDir;

  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    GenericTestUtils.setLogLevel(DeletedBlockLogImpl.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(SCMBlockDeletingService.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(ReplicationManager.LOG, Level.DEBUG);

    String path =
        GenericTestUtils.getTempPath(TestBlockDeletion.class.getSimpleName());
    baseDir = new File(path);
    baseDir.mkdirs();

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
    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
        3, TimeUnit.SECONDS);
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE,
        false);
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setQuietMode(false);
    conf.setTimeDuration("hdds.scm.replication.event.timeout", 100,
        TimeUnit.MILLISECONDS);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setHbInterval(200)
        .build();
    cluster.waitForClusterToBeReady();
    store = OzoneClientFactory.getRpcClient(conf).getObjectStore();
    om = cluster.getOzoneManager();
    scm = cluster.getStorageContainerManager();
    containerIdsWithDeletedBlocks = new HashSet<>();
  }

  @After
  public void cleanup() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    FileUtils.deleteDirectory(baseDir);
  }

  @Test
  public void testBlockDeletion() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = RandomStringUtils.random(10000000);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String keyName = UUID.randomUUID().toString();

    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(UTF_8).length, ReplicationType.RATIS,
        ReplicationFactor.THREE, new HashMap<>());
    for (int i = 0; i < 10; i++) {
      out.write(value.getBytes(UTF_8));
    }
    out.close();

    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName).setKeyName(keyName).setDataSize(0)
        .setReplicationConfig(
            new RatisReplicationConfig(HddsProtos.ReplicationFactor.THREE))
        .setRefreshPipeline(true)
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
    Assert.assertTrue(containerIdsWithDeletedBlocks.isEmpty());
    // Delete transactionIds for the containers should be 0.
    // NOTE: this test assumes that all the container is KetValueContainer. If
    // other container types is going to be added, this test should be checked.
    matchContainerTransactionIds();
    om.deleteKey(keyArgs);
    Thread.sleep(5000);
    // The blocks should not be deleted in the DN as the container is open
    try {
      verifyBlocksDeleted(omKeyLocationInfoGroupList);
      Assert.fail("Blocks should not have been deleted");
    } catch (Throwable e) {
      Assert.assertTrue(e.getMessage().contains("expected null, but was"));
      Assert.assertEquals(e.getClass(), AssertionError.class);
    }

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
    Assert.assertTrue(!containerIdsWithDeletedBlocks.isEmpty());
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
  }

  @Test
  public void testContainerStatisticsAfterDelete() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = RandomStringUtils.random(1000000);
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
            new RatisReplicationConfig(HddsProtos.ReplicationFactor.THREE))
        .setRefreshPipeline(true)
        .build();
    List<OmKeyLocationInfoGroup> omKeyLocationInfoGroupList =
        om.lookupKey(keyArgs).getKeyLocationVersions();
    Thread.sleep(5000);
    List<ContainerInfo> containerInfos =
        scm.getContainerManager().getContainers();
    final int valueSize = value.getBytes(UTF_8).length;
    final int keyCount = 1;
    containerInfos.stream().forEach(container -> {
      Assert.assertEquals(valueSize, container.getUsedBytes());
      Assert.assertEquals(keyCount, container.getNumberOfKeys());
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

    om.deleteKey(keyArgs);
    // Wait for blocks to be deleted and container reports to be processed
    Thread.sleep(5000);
    containerInfos = scm.getContainerManager().getContainers();
    containerInfos.stream().forEach(container -> {
      Assert.assertEquals(0, container.getUsedBytes());
      Assert.assertEquals(0, container.getNumberOfKeys());
    });
    // Verify that pending block delete num are as expected with resent cmds
    cluster.getHddsDatanodes().forEach(dn -> {
      Map<Long, Container<?>> containerMap = dn.getDatanodeStateMachine()
          .getContainer().getContainerSet().getContainerMap();
      containerMap.values().forEach(container -> {
        KeyValueContainerData containerData =
            (KeyValueContainerData)container.getContainerData();
        Assert.assertEquals(0, containerData.getNumPendingDeletionBlocks());
      });
    });

    cluster.shutdownHddsDatanode(0);
    scm.getReplicationManager().processContainersNow();
    // Wait for container state change to DELETING
    Thread.sleep(100);
    containerInfos = scm.getContainerManager().getContainers();
    containerInfos.stream().forEach(container ->
        Assert.assertEquals(HddsProtos.LifeCycleState.DELETING,
            container.getState()));
    LogCapturer logCapturer =
        LogCapturer.captureLogs(ReplicationManager.LOG);
    logCapturer.clearOutput();

    scm.getReplicationManager().processContainersNow();
    Thread.sleep(100);
    // Wait for delete replica command resend
    GenericTestUtils.waitFor(() -> logCapturer.getOutput()
        .contains("Resend delete Container"), 500, 5000);
    cluster.restartHddsDatanode(0, true);
    Thread.sleep(100);

    scm.getReplicationManager().processContainersNow();
    // Wait for container state change to DELETED
    Thread.sleep(100);
    containerInfos = scm.getContainerManager().getContainers();
    containerInfos.stream().forEach(container -> {
      Assert.assertEquals(HddsProtos.LifeCycleState.DELETED,
          container.getState());
      try {
        Assert.assertTrue(scm.getScmMetadataStore().getContainerTable()
            .get(container.containerID()).getState() ==
            HddsProtos.LifeCycleState.DELETED);
      } catch (IOException e) {
        Assert.fail("Container from SCM DB should be marked as DELETED");
      }
    });
  }

  private void verifyTransactionsCommitted() throws IOException {
    scm.getScmBlockManager().getDeletedBlockLog();
    for (long txnID = 1; txnID <= maxTransactionId; txnID++) {
      Assert.assertNull(
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
          Assert.assertTrue(
              scm.getContainerInfo(containerId).getDeleteTransactionId() > 0);
          maxTransactionId = max(maxTransactionId,
              scm.getContainerInfo(containerId).getDeleteTransactionId());
        } else {
          Assert.assertEquals(
              scm.getContainerInfo(containerId).getDeleteTransactionId(), 0);
        }
        Assert.assertEquals(
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
        try (ReferenceCountedDB db = BlockUtils.getDB(
            (KeyValueContainerData) dnContainerSet
                .getContainer(blockID.getContainerID()).getContainerData(),
            conf)) {
          Assert.assertNotNull(db.getStore().getBlockDataTable()
              .get(Long.toString(blockID.getLocalID())));
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
        try (ReferenceCountedDB db = BlockUtils.getDB(
            (KeyValueContainerData) dnContainerSet
                .getContainer(blockID.getContainerID()).getContainerData(),
            conf)) {
          Table<String, BlockData> blockDataTable =
              db.getStore().getBlockDataTable();

          String blockIDString = Long.toString(blockID.getLocalID());

          BlockData blockData = blockDataTable.get(blockIDString);
          Assert.assertNull(blockData);

          String deletingKey = OzoneConsts.DELETING_KEY_PREFIX + blockIDString;
          Assert.assertNull(blockDataTable.get(deletingKey));
        }
        containerIdsWithDeletedBlocks.add(blockID.getContainerID());
      }, omKeyLocationInfoGroups);
    }
  }
}
