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

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.utils.IOUtils;
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
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.container.replication.LegacyReplicationManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.TestHelper;
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
import org.apache.ozone.test.tag.Flaky;
import org.junit.Assert;
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
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EXPIRED_CONTAINER_REPLICA_OP_SCRUB_INTERVAL;
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
  private OzoneClient client;

  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    GenericTestUtils.setLogLevel(DeletedBlockLogImpl.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(SCMBlockDeletingService.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(LegacyReplicationManager.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(ReplicationManager.LOG, Level.DEBUG);

    conf.set("ozone.replication.allowed-configs",
        "^(RATIS/THREE)|(EC/2-1-256k)$");
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
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 50,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
        3, TimeUnit.SECONDS);
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE,
        false);
    conf.setTimeDuration(OZONE_SCM_EXPIRED_CONTAINER_REPLICA_OP_SCRUB_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    conf.setInt(OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 100);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setQuietMode(false);
    conf.setTimeDuration("hdds.scm.replication.event.timeout", 2,
        TimeUnit.SECONDS);
    conf.setTimeDuration("hdds.scm.replication.event.timeout.datanode.offset",
        0,
        TimeUnit.MILLISECONDS);
    conf.setInt("hdds.datanode.block.delete.threads.max", 5);
    ReplicationManager.ReplicationManagerConfiguration replicationConf = conf
        .getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    replicationConf.setInterval(Duration.ofSeconds(300));
    conf.setFromObject(replicationConf);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setHbInterval(50)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    store = client.getObjectStore();
    om = cluster.getOzoneManager();
    writeClient = store
        .getClientProxy().getOzoneManagerClient();
    scm = cluster.getStorageContainerManager();
    containerIdsWithDeletedBlocks = new HashSet<>();
    metrics = scm.getScmBlockManager().getSCMBlockDeletingService()
        .getMetrics();
  }

  @AfterEach
  public void cleanup() throws IOException {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  @Flaky("HDDS-8353")
  public void testContainerStatisticsAfterDelete() throws Exception {
    ReplicationManager replicationManager = scm.getReplicationManager();
    boolean legacyEnabled = replicationManager.getConfig().isLegacyEnabled();

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
    /*omKeyLocationInfoGroupList.forEach((group) -> {
      List<OmKeyLocationInfo> locationInfo = group.getLocationList();
      locationInfo.forEach(
          (info) -> {
            cluster.getHddsDatanodes().forEach(dn -> {
              Container cn =
                  dn.getDatanodeStateMachine().getContainer().getContainerSet()
                      .getContainer(info.getContainerID());
              if (cn != null) {
                cn.getContainerData()
                    .setState(ContainerProtos.ContainerDataProto.State.CLOSED);
              }
            });
          });
    });*/

    writeClient.deleteKey(keyArgs);
    // Wait for blocks to be deleted and container reports to be processed
    GenericTestUtils.waitFor(() -> {
      scm.getContainerManager().getContainers().forEach(containerInfo -> {
        try {
          Set<ContainerReplica> containerReplicas =
              scm.getContainerManager().getContainerReplicas(
                  ContainerID.valueOf(containerInfo.getContainerID()));
          containerReplicas.forEach(containerReplica -> {
            LOG.info("containerId --- {} , containerKeys --- {}, " +
                    "containerReplica.getKeyCount()--- {} ," +
                    "containerReplica.getBytesUsed() -- {}, " +
                    "containerReplica.getDatanodeDetails()-- {}",
                containerInfo.getContainerID(), containerInfo.getNumberOfKeys(),
                containerReplica.getKeyCount(), containerReplica.getBytesUsed(),
                containerReplica.getDatanodeDetails());
          });
        } catch (ContainerNotFoundException e) {
          throw new RuntimeException(e);
        }
      });
      return
            scm.getContainerManager().getContainers().stream()
                .allMatch(c -> c.getUsedBytes() == 0 &&
                    c.getNumberOfKeys() == 0);
            }, 500, 5000);
    Thread.sleep(5000);
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
    replicationManager.processAll();
    ((EventQueue)scm.getEventQueue()).processAll(1000);
    containerInfos = scm.getContainerManager().getContainers();
    containerInfos.stream().forEach(container ->
        Assertions.assertEquals(HddsProtos.LifeCycleState.DELETING,
            container.getState()));
    LogCapturer logCapturer = LogCapturer.captureLogs(
        legacyEnabled ? LegacyReplicationManager.LOG  : ReplicationManager.LOG);
    logCapturer.clearOutput();

    Thread.sleep(5000);
    replicationManager.processAll();
    ((EventQueue) scm.getEventQueue()).processAll(1000);
    String expectedOutput = legacyEnabled
        ? "Resend delete Container"
        : "Sending delete command for container";
    GenericTestUtils.waitFor(() -> logCapturer.getOutput()
        .contains(expectedOutput), 500, 5000);

    cluster.restartHddsDatanode(0, true);
    Thread.sleep(2000);

    GenericTestUtils.waitFor(() -> {
      replicationManager.processAll();
      ((EventQueue)scm.getEventQueue()).processAll(1000);
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
        LOG.info(e.getMessage());
        return false;
      }
      return true;
    }, 500, 15000);
    LOG.info(metrics.toString());
  }

}
