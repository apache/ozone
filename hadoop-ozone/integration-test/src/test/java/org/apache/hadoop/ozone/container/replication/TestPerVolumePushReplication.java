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

package org.apache.hadoop.ozone.container.replication;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.node.TestNodeUtil.getDNHostAndPort;
import static org.apache.hadoop.hdds.scm.node.TestNodeUtil.waitForDnToReachHealthState;
import static org.apache.hadoop.hdds.scm.node.TestNodeUtil.waitForDnToReachOpState;
import static org.apache.hadoop.hdds.scm.pipeline.MockPipeline.createPipeline;
import static org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls.createContainer;
import static org.apache.hadoop.ozone.container.TestHelper.waitForContainerClose;
import static org.apache.hadoop.ozone.container.TestHelper.waitForReplicaCount;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.dn.DatanodeTestUtils;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for per-volume push replication thread pools (HDDS-15412).
 */
class TestPerVolumePushReplication {

  private static final AtomicLong CONTAINER_ID = new AtomicLong(1_000_000L);
  private static final int DATA_VOLUMES = 2;
  private static final String VOLUME = "vol1";
  private static final String BUCKET = "bucket1";
  private static final RatisReplicationConfig RATIS_THREE =
      RatisReplicationConfig.getInstance(THREE);
  private static final ECReplicationConfig EC_REP = new ECReplicationConfig(3, 2);

  @Test
  void testPushAndScmReplicationWithPerVolumeEnabled() throws Exception {
    OzoneConfiguration conf = createPerVolumeConfig(true, 1);
    try (MiniOzoneCluster cluster = newCluster(conf, 3);
        XceiverClientFactory clientFactory = new XceiverClientManager(conf);
        OzoneClient client = cluster.newClient()) {
      cluster.waitForClusterToBeReady();

      DatanodeDetails source = cluster.getHddsDatanodes().get(0).getDatanodeDetails();
      DatanodeDetails target = selectOtherNode(cluster, source);
      long containerId = createClosedContainer(clientFactory, source, 0L);

      ReplicateContainerCommand cmd =
          ReplicateContainerCommand.toTarget(containerId, target);
      queuePushAndWait(cluster, cmd, source, ReplicationSupervisor::getReplicationSuccessCount);
      assertNotNull(getContainer(cluster, target, containerId));

      HddsDatanodeService sourceDn = cluster.getHddsDatanode(source);
      assertVolumePools(sourceDn, DATA_VOLUMES, 1);

      OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client, VOLUME, BUCKET);
      TestDataUtil.createKey(bucket, "key1", RATIS_THREE, "data".getBytes(UTF_8));
      OzoneKeyDetails keyDetails = bucket.getKey("key1");
      long scmContainerId = keyDetails.getOzoneKeyLocations().get(0).getContainerID();
      waitForContainerClose(cluster, scmContainerId);

      ContainerManager containerManager = cluster.getStorageContainerManager().getContainerManager();
      Set<ContainerReplica> replicas =
          containerManager.getContainerReplicas(ContainerID.valueOf(scmContainerId));
      DatanodeDetails replicaDn = replicas.iterator().next().getDatanodeDetails();
      cluster.shutdownHddsDatanode(replicaDn);
      waitForReplicaCount(scmContainerId, 3, cluster);
    }
  }

  @Test
  void testHealthyVolumeReplicationAfterVolumeFailure() throws Exception {
    OzoneConfiguration conf = createPerVolumeConfig(true, 1);
    configureVolumeFailureDetection(conf);
    try (MiniOzoneCluster cluster = newCluster(conf, 3);
        XceiverClientFactory clientFactory = new XceiverClientManager(conf)) {
      cluster.waitForClusterToBeReady();

      HddsDatanodeService sourceDn = cluster.getHddsDatanodes().get(0);
      DatanodeDetails source = sourceDn.getDatanodeDetails();
      DatanodeDetails target = selectOtherNode(cluster, source);
      MutableVolumeSet volSet = sourceDn.getDatanodeStateMachine().getContainer().getVolumeSet();
      HddsVolume vol0 = (HddsVolume) volSet.getVolumesList().get(0);
      HddsVolume vol1 = (HddsVolume) volSet.getVolumesList().get(1);

      long containerOnVol0 = findOrCreateContainerOnVolume(
          cluster, clientFactory, source, vol0, 0L);
      long containerOnVol1 = findOrCreateContainerOnVolume(
          cluster, clientFactory, source, vol1, 0L);

      assertVolumePools(sourceDn, DATA_VOLUMES, 1);

      triggerAndWaitForVolumeFailure(volSet, vol0);

      VolumeReplicationThreadPools pools =
          sourceDn.getDatanodeStateMachine().getSupervisor().getVolumeReplicationThreadPools();
      assertNotNull(pools);
      assertFalse(pools.hasPool(vol0.getStorageDir().getPath()));
      assertTrue(pools.hasPool(vol1.getStorageDir().getPath()));

      ReplicateContainerCommand cmd =
          ReplicateContainerCommand.toTarget(containerOnVol1, target);
      queuePushAndWait(cluster, cmd, source, ReplicationSupervisor::getReplicationSuccessCount);
      assertNotNull(getContainer(cluster, target, containerOnVol1));
      assertEquals(1, volSet.getFailedVolumesList().size());

      // Container on the failed volume must not replicate successfully.
      ReplicateContainerCommand failedVolCmd =
          ReplicateContainerCommand.toTarget(containerOnVol0, target);
      long previousFailures = sourceDn.getDatanodeStateMachine().getSupervisor()
          .getReplicationFailureCount();
      queuePushAndWait(cluster, failedVolCmd, source,
          ReplicationSupervisor::getReplicationFailureCount);
      assertEquals(previousFailures + 1,
          sourceDn.getDatanodeStateMachine().getSupervisor().getReplicationFailureCount());

      DatanodeTestUtils.restoreBadVolume(vol0);
    }
  }

  @Test
  void testDecommissionWithPerVolumePools() throws Exception {
    OzoneConfiguration conf = createDecommissionConfig(true, 1);
    try (MiniOzoneCluster cluster = newCluster(conf, 7);
        OzoneClient client = cluster.newClient();
        ContainerOperationClient scmClient = new ContainerOperationClient(conf)) {
      cluster.waitForClusterToBeReady();
      StorageContainerManager scm = cluster.getStorageContainerManager();
      NodeManager nm = scm.getScmNodeManager();
      ContainerManager cm = scm.getContainerManager();
      PipelineManager pm = scm.getPipelineManager();

      OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client, VOLUME, BUCKET);
      generateData(bucket, 20, "key", RATIS_THREE);
      generateData(bucket, 20, "ecKey", EC_REP);

      ContainerInfo ratisContainer = waitForAndReturnContainer(cm, RATIS_THREE, 3);
      ContainerInfo ecContainer = waitForAndReturnContainer(cm, EC_REP, 5);
      Pipeline ratisPipeline = pm.getPipeline(ratisContainer.getPipelineID());
      Pipeline ecPipeline = pm.getPipeline(ecContainer.getPipelineID());

      DatanodeID dnId = ratisPipeline.getNodes().stream()
          .filter(node -> ecPipeline.getNodes().contains(node))
          .findFirst()
          .orElseThrow(() -> new AssertionError("no intersecting datanode found"))
          .getID();
      DatanodeDetails toDecommission = nm.getNode(dnId);

      scmClient.decommissionNodes(singletonList(getDNHostAndPort(toDecommission)), false);
      waitForDnToReachOpState(nm, toDecommission, DECOMMISSIONED);

      waitForContainerReplicas(cm, ratisContainer, 4);
      waitForContainerReplicas(cm, ecContainer, 6);

      cluster.shutdownHddsDatanode(toDecommission);
      waitForDnToReachHealthState(nm, toDecommission, DEAD);

      waitForContainerReplicas(cm, ratisContainer, 3);
      waitForContainerReplicas(cm, ecContainer, 5);

      TestDataUtil.createKey(bucket, "sanityKey", RATIS_THREE,
          "still healthy".getBytes(StandardCharsets.UTF_8));
    }
  }

  private static void assertVolumePools(HddsDatanodeService dn,
      int expectedVolumeCount, int expectedPoolSize) {
    VolumeReplicationThreadPools pools =
        dn.getDatanodeStateMachine().getSupervisor().getVolumeReplicationThreadPools();
    assertNotNull(pools);
    List<? extends StorageVolume> volumes =
        dn.getDatanodeStateMachine().getContainer().getVolumeSet().getVolumesList();
    assertEquals(expectedVolumeCount, volumes.size());
    for (StorageVolume volume : volumes) {
      String volumeRoot = volume.getStorageDir().getPath();
      assertTrue(pools.hasPool(volumeRoot), "missing pool for " + volumeRoot);
      assertEquals(expectedPoolSize, pools.getPoolSize(volumeRoot));
    }
  }

  private static void queuePushAndWait(MiniOzoneCluster cluster,
      ReplicateContainerCommand cmd, DatanodeDetails source,
      ToLongFunction<ReplicationSupervisor> counter)
      throws IOException, InterruptedException, TimeoutException {
    DatanodeStateMachine stateMachine = cluster.getHddsDatanode(source).getDatanodeStateMachine();
    ReplicationSupervisor supervisor = stateMachine.getSupervisor();
    long previousCount = counter.applyAsLong(supervisor);
    StateContext context = stateMachine.getContext();
    context.getTermOfLeaderSCM().ifPresent(cmd::setTerm);
    context.addCommand(cmd);
    waitFor(() -> counter.applyAsLong(supervisor) == previousCount + 1, 100, 30000);
  }

  private static DatanodeDetails selectOtherNode(MiniOzoneCluster cluster,
      DatanodeDetails source) throws IOException {
    int sourceIndex = cluster.getHddsDatanodeIndex(source);
    int targetIndex = IntStream.range(0, cluster.getHddsDatanodes().size())
        .filter(index -> index != sourceIndex)
        .findAny()
        .orElseThrow(() -> new AssertionError("no target datanode found"));
    return cluster.getHddsDatanodes().get(targetIndex).getDatanodeDetails();
  }

  private static long findOrCreateContainerOnVolume(MiniOzoneCluster cluster,
      XceiverClientFactory clientFactory, DatanodeDetails dn, HddsVolume targetVolume,
      long dataSize) throws Exception {
    for (int attempt = 0; attempt < 30; attempt++) {
      long containerId = createClosedContainer(clientFactory, dn, dataSize);
      Container<?> container = getContainer(cluster, dn, containerId);
      if (targetVolume.equals(container.getContainerData().getVolume())) {
        return containerId;
      }
    }
    throw new AssertionError("Could not place container on volume " + targetVolume);
  }

  private static long createClosedContainer(XceiverClientFactory clientFactory,
      DatanodeDetails dn, long dataSize) throws Exception {
    long containerId = CONTAINER_ID.incrementAndGet();
    try (XceiverClientSpi client = clientFactory.acquireClient(createPipeline(singleton(dn)))) {
      if (dataSize <= 0) {
        createContainer(client, containerId, null, CLOSED, 0);
        return containerId;
      }
      createContainer(client, containerId, null);
      int chunkSize = 1024 * 1024;
      long totalBytesWritten = 0;
      while (totalBytesWritten < dataSize) {
        BlockID blockId = ContainerTestHelper.getTestBlockID(containerId);
        long remainingBytes = dataSize - totalBytesWritten;
        int currentChunkSize = (int) Math.min(chunkSize, remainingBytes);
        ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
            ContainerTestHelper.getWriteChunkRequest(
                createPipeline(singleton(dn)), blockId, currentChunkSize);
        client.sendCommand(writeChunkRequest);
        ContainerProtos.ContainerCommandRequestProto putBlockRequest =
            ContainerTestHelper.getPutBlockRequest(writeChunkRequest);
        client.sendCommand(putBlockRequest);
        totalBytesWritten += currentChunkSize;
      }
      ContainerProtos.CloseContainerRequestProto closeRequest =
          ContainerProtos.CloseContainerRequestProto.newBuilder().build();
      ContainerProtos.ContainerCommandRequestProto closeContainerRequest =
          ContainerProtos.ContainerCommandRequestProto.newBuilder()
              .setCmdType(ContainerProtos.Type.CloseContainer)
              .setContainerID(containerId)
              .setCloseContainer(closeRequest)
              .setDatanodeUuid(dn.getUuidString())
              .build();
      client.sendCommand(closeContainerRequest);
      return containerId;
    }
  }

  private static Container<?> getContainer(MiniOzoneCluster cluster,
      DatanodeDetails datanode, long containerId) throws IOException {
    HddsDatanodeService dnService = cluster.getHddsDatanode(datanode);
    Container<?> container = dnService.getDatanodeStateMachine().getContainer()
        .getContainerSet().getContainer(containerId);
    if (container == null) {
      throw new AssertionError("Container " + containerId + " not found on " + datanode);
    }
    return container;
  }

  private static MiniOzoneCluster newCluster(OzoneConfiguration conf, int numDatanodes)
      throws IOException {
    UniformDatanodesFactory uniformFactory = UniformDatanodesFactory.newBuilder()
        .setNumDataVolumes(DATA_VOLUMES)
        .build();
    ReplicationServer.ReplicationConfig clusterReplicationConfig =
        conf.getObject(ReplicationServer.ReplicationConfig.class);
    return MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numDatanodes)
        .setDatanodeFactory(baseConf -> {
          OzoneConfiguration dnConf = uniformFactory.apply(baseConf);
          ReplicationServer.ReplicationConfig dnReplicationConfig =
              dnConf.getObject(ReplicationServer.ReplicationConfig.class);
          dnReplicationConfig.setPerVolumeEnabled(
              clusterReplicationConfig.isPerVolumeEnabled());
          dnReplicationConfig.setPerVolumeStreamsLimit(
              clusterReplicationConfig.getPerVolumeStreamsLimit());
          dnConf.setFromObject(dnReplicationConfig);
          return dnConf;
        })
        .build();
  }

  private static OzoneConfiguration createPerVolumeConfig(boolean perVolumeEnabled,
      int streamsLimit) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, TimeUnit.SECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE, 5, StorageUnit.MB);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 1, StorageUnit.MB);

    ReplicationManagerConfiguration repConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    repConf.setInterval(Duration.ofSeconds(1));
    repConf.setUnderReplicatedInterval(Duration.ofSeconds(1));
    conf.setFromObject(repConf);

    ReplicationServer.ReplicationConfig replicationConfig =
        conf.getObject(ReplicationServer.ReplicationConfig.class);
    replicationConfig.setPerVolumeEnabled(perVolumeEnabled);
    replicationConfig.setPerVolumeStreamsLimit(streamsLimit);
    conf.setFromObject(replicationConfig);
    return conf;
  }

  private static OzoneConfiguration createDecommissionConfig(boolean perVolumeEnabled,
      int streamsLimit) {
    OzoneConfiguration conf = createPerVolumeConfig(perVolumeEnabled, streamsLimit);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setTimeDuration(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, 0, TimeUnit.SECONDS);
    conf.set(OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION, "2s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL, "2s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, "5s");
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setTimeDuration(
        ScmConfigKeys.OZONE_SCM_EXPIRED_CONTAINER_REPLICA_OP_SCRUB_INTERVAL, 1, TimeUnit.SECONDS);

    ReplicationManagerConfiguration repConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    repConf.setOverReplicatedInterval(Duration.ofSeconds(1));
    conf.setFromObject(repConf);
    return conf;
  }

  private static void configureVolumeFailureDetection(OzoneConfiguration conf) {
    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    dnConf.setFailedDataVolumesTolerated(1);
    dnConf.setDiskCheckMinGap(Duration.ofSeconds(0));
    dnConf.setPeriodicDiskCheckIntervalMinutes(1);
    conf.setFromObject(dnConf);
  }

  private static void triggerAndWaitForVolumeFailure(MutableVolumeSet volSet,
      StorageVolume volume) throws Exception {
    DatanodeTestUtils.simulateBadVolume(volume);
    volSet.checkVolumeAsync(volume);
    GenericTestUtils.waitFor(
        () -> volSet.getFailedVolumesList().size() == 1,
        100, 60000);
  }

  private static void generateData(OzoneBucket bucket, int keyCount, String keyPrefix,
      ReplicationConfig replicationConfig) throws IOException {
    for (int i = 0; i < keyCount; i++) {
      TestDataUtil.createKey(bucket, keyPrefix + i, replicationConfig,
          "this is the content".getBytes(StandardCharsets.UTF_8));
    }
  }

  private static ContainerInfo waitForAndReturnContainer(ContainerManager cm,
      ReplicationConfig repConfig, int expectedReplicas) throws Exception {
    List<ContainerInfo> containers = cm.getContainers();
    ContainerInfo container = null;
    for (ContainerInfo candidate : containers) {
      if (candidate.getReplicationConfig().equals(repConfig)) {
        container = candidate;
        break;
      }
    }
    assertNotNull(container, "no container for replication config " + repConfig);
    waitForContainerReplicas(cm, container, expectedReplicas);
    return container;
  }

  private static void waitForContainerReplicas(ContainerManager cm,
      ContainerInfo container, int count) throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(
        () -> getContainerReplicas(cm, container).size() == count,
        200, 60000);
  }

  private static Set<ContainerReplica> getContainerReplicas(ContainerManager cm,
      ContainerInfo container) {
    try {
      return cm.getContainerReplicas(container.containerID());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
