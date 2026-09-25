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
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.node.NodeTestUtil.getDNHostAndPort;
import static org.apache.hadoop.hdds.scm.node.NodeTestUtil.waitForDnToReachHealthState;
import static org.apache.hadoop.hdds.scm.node.NodeTestUtil.waitForDnToReachOpState;
import static org.apache.hadoop.hdds.scm.pipeline.MockPipeline.createPipeline;
import static org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls.createContainer;
import static org.apache.hadoop.ozone.container.OzoneTestHelper.waitForContainerClose;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
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
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Integration tests for per-volume push replication thread pools (HDDS-15412).
 *
 * <p>The tests share one cluster and run in {@link Order} sequence, so each one restores whatever it broke
 * (stopped datanode, failed volume) before returning. {@link #selectHealthyDatanode} additionally filters out
 * stopped datanodes so a leaked shutdown cannot silently hand a later test a dead node.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TestPerVolumePushReplication {

  private static final AtomicLong CONTAINER_ID = new AtomicLong(1_000_000L);
  private static final int DATA_VOLUMES = 2;
  private static final int DATANODE_COUNT = 7;
  private static final int PER_VOLUME_STREAMS = 1;
  private static final String VOLUME = "vol1";
  private static final String BUCKET = "bucket1";
  private static final RatisReplicationConfig RATIS_THREE = RatisReplicationConfig.getInstance(THREE);
  private static final ECReplicationConfig EC_REP = new ECReplicationConfig(3, 2);

  private MiniOzoneCluster cluster;
  private XceiverClientFactory clientFactory;
  private OzoneClient client;
  private OzoneBucket bucket;

  @BeforeAll
  void setUp() throws Exception {
    OzoneConfiguration conf = createConfig();
    cluster = newCluster(conf, DATANODE_COUNT);
    cluster.waitForClusterToBeReady();
    clientFactory = new XceiverClientManager(conf);
    // Keep the client open for the lifetime of the class: the bucket handle below delegates to it, and all
    // three tests write keys through that handle.
    client = cluster.newClient();
    bucket = DataTestUtil.createVolumeAndBucket(client, VOLUME, BUCKET);
  }

  @AfterAll
  void tearDown() {
    IOUtils.closeQuietly(client, clientFactory, cluster);
  }

  @Order(1)
  @Test
  void testPushAndScmReplicationWithPerVolumeEnabled() throws Exception {
    HddsDatanodeService sourceDn = selectHealthyDatanode(0);
    DatanodeDetails source = sourceDn.getDatanodeDetails();
    DatanodeDetails target = selectOtherHealthyNode(source);
    long containerId = createClosedContainer(clientFactory, source);

    assertVolumePools(sourceDn, DATA_VOLUMES, PER_VOLUME_STREAMS);

    // Assert the push was dispatched to the pool of the volume holding the container, not the global pool.
    // Without this the test would also pass with hdds.datanode.replication.per.volume.enabled=false.
    HddsVolume containerVolume = getContainer(cluster, source, containerId).getContainerData().getVolume();
    ThreadPoolExecutor volumePool = volumePoolOf(sourceDn, containerVolume);
    long completedBefore = volumePool.getCompletedTaskCount();

    ReplicateContainerCommand cmd = ReplicateContainerCommand.toTarget(containerId, target);
    queuePushAndWaitForContainer(cluster, cmd, source, target, containerId);
    GenericTestUtils.waitFor(() -> volumePool.getCompletedTaskCount() > completedBefore, 100, 30000);

    DataTestUtil.createKey(bucket, "pushKey1", RATIS_THREE, "data".getBytes(UTF_8));
    OzoneKeyDetails keyDetails = bucket.getKey("pushKey1");
    long scmContainerId = keyDetails.getOzoneKeyLocations().get(0).getContainerID();
    waitForContainerClose(cluster, scmContainerId);

    // SCM learns about replicas asynchronously via ICR, so settle on the full replica set before picking one
    // to stop; otherwise the iterator below can be empty.
    StorageContainerManager scm = cluster.getStorageContainerManager();
    ContainerManager containerManager = scm.getContainerManager();
    ContainerID scmContainer = ContainerID.valueOf(scmContainerId);
    waitForReplicas(containerManager, scmContainer, 3);
    Set<ContainerReplica> replicas = containerManager.getContainerReplicas(scmContainer);
    DatanodeDetails replicaDn = replicas.iterator().next().getDatanodeDetails();

    cluster.shutdownHddsDatanode(replicaDn);
    try {
      // SCM drops a dead node's replicas only once it is declared DEAD, so waiting for a count of 3 right after
      // the shutdown would be satisfied by the stale replica on its very first poll. Require 3 replicas none of
      // which sit on the stopped node, which only re-replication can produce.
      waitForDnToReachHealthState(scm.getScmNodeManager(), replicaDn, DEAD);
      GenericTestUtils.waitFor(() -> {
        Set<ContainerReplica> current = replicasOf(containerManager, scmContainer);
        return current.size() == 3
            && current.stream().noneMatch(r -> r.getDatanodeDetails().equals(replicaDn));
      }, 500, 60000);
    } finally {
      // Restore the cluster so the later tests see all DATANODE_COUNT nodes.
      cluster.restartHddsDatanode(replicaDn, true);
    }
  }

  @Order(2)
  @Test
  void testHealthyVolumeReplicationAfterVolumeFailure() throws Exception {
    HddsDatanodeService sourceDn = selectHealthyDatanode(1);
    DatanodeDetails source = sourceDn.getDatanodeDetails();
    DatanodeDetails target = selectOtherHealthyNode(source);
    MutableVolumeSet volSet = sourceDn.getDatanodeStateMachine().getContainer().getVolumeSet();
    HddsVolume vol0 = (HddsVolume) volSet.getVolumesList().get(0);
    HddsVolume vol1 = (HddsVolume) volSet.getVolumesList().get(1);

    long containerOnVol0 = findOrCreateContainerOnVolume(cluster, clientFactory, source, vol0);
    long containerOnVol1 = findOrCreateContainerOnVolume(cluster, clientFactory, source, vol1);

    assertVolumePools(sourceDn, DATA_VOLUMES, PER_VOLUME_STREAMS);

    try {
      triggerAndWaitForVolumeFailure(volSet, vol0);
      waitForVolumePoolState(sourceDn, vol0, vol1);

      // The healthy volume keeps its own pool and keeps serving pushes from it.
      ThreadPoolExecutor healthyPool = volumePoolOf(sourceDn, vol1);
      long completedBefore = healthyPool.getCompletedTaskCount();
      ReplicateContainerCommand cmd = ReplicateContainerCommand.toTarget(containerOnVol1, target);
      queuePushAndWaitForContainer(cluster, cmd, source, target, containerOnVol1);
      GenericTestUtils.waitFor(() -> healthyPool.getCompletedTaskCount() > completedBefore, 100, 30000);
      assertThat(volSet.getFailedVolumesList()).hasSize(1);

      // Task routes via global pool fallback (HDDS-15327); replication fails on bad volume.
      ReplicateContainerCommand failedVolCmd = ReplicateContainerCommand.toTarget(containerOnVol0, target);
      ReplicationSupervisor supervisor = sourceDn.getDatanodeStateMachine().getSupervisor();
      // Scope the counter to push replication: the cluster-wide counter also moves for unrelated
      // SCM-driven work on this datanode and would satisfy the wait on its own.
      long previousFailures = supervisor.getReplicationFailureCount(ReplicationTask.METRIC_NAME);
      queuePushAndWaitForFailure(cluster, failedVolCmd, source, supervisor, previousFailures);
      assertThat(supervisor.getReplicationFailureCount(ReplicationTask.METRIC_NAME))
          .isGreaterThanOrEqualTo(previousFailures + 1);
      assertThat(hasContainer(cluster, target, containerOnVol0)).isFalse();
    } finally {
      // Must run even if an assertion above fails: the volume dir is left read-only otherwise, which breaks
      // the next test and stops the cluster from cleaning up its base dir.
      DatanodeTestUtils.restoreBadVolume(vol0);
    }
  }

  @Order(3)
  @Test
  void testDecommissionWithPerVolumePools() throws Exception {
    try (ContainerOperationClient scmClient = new ContainerOperationClient(cluster.getConf())) {
      StorageContainerManager scm = cluster.getStorageContainerManager();
      NodeManager nm = scm.getScmNodeManager();
      ContainerManager cm = scm.getContainerManager();
      PipelineManager pm = scm.getPipelineManager();

      generateData(bucket, 20, "decomKey", RATIS_THREE);
      generateData(bucket, 20, "decomEcKey", EC_REP);

      ContainerInfo ratisContainer = waitForKeyContainer(bucket, cm, "decomKey0", 3);
      ContainerInfo ecContainer = waitForKeyContainer(bucket, cm, "decomEcKey0", 5);
      Pipeline ratisPipeline = pm.getPipeline(ratisContainer.getPipelineID());
      Pipeline ecPipeline = pm.getPipeline(ecContainer.getPipelineID());

      DatanodeID dnId = ratisPipeline.getNodes().stream()
          .filter(node -> ecPipeline.getNodes().contains(node))
          .findFirst()
          .orElseThrow(() -> new AssertionError("no intersecting datanode found"))
          .getID();
      DatanodeDetails toDecommission = nm.getNode(dnId);
      HddsDatanodeService decommissioningDn = cluster.getHddsDatanode(toDecommission);

      scmClient.decommissionNodes(singletonList(getDNHostAndPort(toDecommission)), false);
      waitForDnToReachOpState(nm, toDecommission, DECOMMISSIONED);

      // Per-volume behaviour that the existing decommission coverage cannot catch: SCM's operational state
      // change reaches the datanode as a SetNodeOperationalStateCommand, which drives
      // ReplicationSupervisor.nodeStateUpdated -> resize, scaling every per-volume pool by the
      // out-of-service factor. Only an integration test exercises that whole path.
      ReplicationSupervisor supervisor = decommissioningDn.getDatanodeStateMachine().getSupervisor();
      int scaledStreams = supervisor.getReplicationConfig().scaleOutOfServiceLimit(PER_VOLUME_STREAMS);
      assertThat(scaledStreams).isGreaterThan(PER_VOLUME_STREAMS);
      waitForVolumePoolSize(decommissioningDn, scaledStreams);

      waitForContainerReplicas(cm, ratisContainer, 4);
      waitForContainerReplicas(cm, ecContainer, 6);

      cluster.shutdownHddsDatanode(toDecommission);
      waitForDnToReachHealthState(nm, toDecommission, DEAD);

      waitForContainerReplicas(cm, ratisContainer, 3);
      waitForContainerReplicas(cm, ecContainer, 5);

      DataTestUtil.createKey(bucket, "sanityKey", RATIS_THREE, "still healthy".getBytes(UTF_8));
    }
  }

  private HddsDatanodeService selectHealthyDatanode(int indexAmongHealthy) {
    List<HddsDatanodeService> healthy = cluster.getHddsDatanodes().stream()
        .filter(this::isHealthyDatanode)
        .collect(Collectors.toList());
    if (indexAmongHealthy >= healthy.size()) {
      throw new AssertionError("not enough healthy datanodes: requested index "
          + indexAmongHealthy + ", found " + healthy.size());
    }
    return healthy.get(indexAmongHealthy);
  }

  private DatanodeDetails selectOtherHealthyNode(DatanodeDetails source) {
    return cluster.getHddsDatanodes().stream()
        .filter(this::isHealthyDatanode)
        .map(HddsDatanodeService::getDatanodeDetails)
        .filter(dn -> !dn.equals(source))
        .findAny()
        .orElseThrow(() -> new AssertionError("no target datanode found"));
  }

  private boolean isHealthyDatanode(HddsDatanodeService datanode) {
    // A stopped datanode keeps its DatanodeStateMachine and stays IN_SERVICE in getHddsDatanodes(), so the
    // op-state and volume checks below cannot tell it from a live one. Without this guard a leaked shutdown
    // in an earlier test would hand this one a dead node and the push waits would simply time out.
    if (datanode.isStopped()) {
      return false;
    }
    if (datanode.getDatanodeDetails().getPersistedOpState() != IN_SERVICE) {
      return false;
    }
    MutableVolumeSet volumeSet = datanode.getDatanodeStateMachine().getContainer().getVolumeSet();
    return volumeSet.getFailedVolumesList().isEmpty() && volumeSet.getVolumesList().size() == DATA_VOLUMES;
  }

  private static VolumeReplicationThreadPools volumePoolsOf(HddsDatanodeService dn) {
    return dn.getDatanodeStateMachine().getSupervisor().getVolumeReplicationThreadPools();
  }

  /**
   * The executor backing {@code volume}'s pool, so a test can prove a push ran on it via
   * {@link ThreadPoolExecutor#getCompletedTaskCount()}.
   */
  private static ThreadPoolExecutor volumePoolOf(HddsDatanodeService dn, HddsVolume volume) {
    String volumeRoot = volume.getStorageDir().getPath();
    VolumeReplicationThreadPools pools = volumePoolsOf(dn);
    assertThat(pools).isNotNull();
    assertThat(pools.hasPool(volumeRoot)).as("pool for %s", volumeRoot).isTrue();
    return (ThreadPoolExecutor) pools.getExecutor(volumeRoot);
  }

  private static void assertVolumePools(HddsDatanodeService dn, int expectedVolumeCount, int expectedPoolSize) {
    VolumeReplicationThreadPools pools = volumePoolsOf(dn);
    assertThat(pools).isNotNull();
    List<? extends StorageVolume> volumes = dn.getDatanodeStateMachine().getContainer().getVolumeSet().getVolumesList();
    assertThat(volumes).hasSize(expectedVolumeCount);
    for (StorageVolume volume : volumes) {
      String volumeRoot = volume.getStorageDir().getPath();
      assertThat(pools.hasPool(volumeRoot)).as("pool for %s", volumeRoot).isTrue();
      assertThat(pools.getPoolSize(volumeRoot)).as("pool size for %s", volumeRoot).isEqualTo(expectedPoolSize);
    }
  }

  private static void waitForVolumePoolSize(HddsDatanodeService dn, int expectedPoolSize)
      throws TimeoutException, InterruptedException {
    List<? extends StorageVolume> volumes = dn.getDatanodeStateMachine().getContainer().getVolumeSet().getVolumesList();
    GenericTestUtils.waitFor(() -> {
      VolumeReplicationThreadPools pools = volumePoolsOf(dn);
      return pools != null && volumes.stream()
          .allMatch(v -> pools.getPoolSize(v.getStorageDir().getPath()) == expectedPoolSize);
    }, 200, 30000);
    assertVolumePools(dn, DATA_VOLUMES, expectedPoolSize);
  }

  private static void waitForVolumePoolState(HddsDatanodeService sourceDn, HddsVolume failedVolume,
      HddsVolume healthyVolume) throws TimeoutException, InterruptedException {
    String failedPath = failedVolume.getStorageDir().getPath();
    String healthyPath = healthyVolume.getStorageDir().getPath();
    GenericTestUtils.waitFor(() -> {
      VolumeReplicationThreadPools pools = volumePoolsOf(sourceDn);
      return pools != null && !pools.hasPool(failedPath) && pools.hasPool(healthyPath);
    }, 100, 60000);
    VolumeReplicationThreadPools pools = volumePoolsOf(sourceDn);
    assertThat(pools).isNotNull();
    assertThat(pools.hasPool(failedPath)).as("pool for failed volume %s", failedPath).isFalse();
    assertThat(pools.hasPool(healthyPath)).as("pool for healthy volume %s", healthyPath).isTrue();
  }

  private static void queuePushAndWaitForContainer(MiniOzoneCluster cluster, ReplicateContainerCommand cmd,
      DatanodeDetails source, DatanodeDetails target, long containerId)
      throws IOException, InterruptedException, TimeoutException {
    queueReplicationCommand(cluster, cmd, source);
    GenericTestUtils.waitFor(() -> hasContainer(cluster, target, containerId), 100, 30000);
  }

  private static void queuePushAndWaitForFailure(MiniOzoneCluster cluster, ReplicateContainerCommand cmd,
      DatanodeDetails source, ReplicationSupervisor supervisor, long previousFailureCount)
      throws IOException, InterruptedException, TimeoutException {
    queueReplicationCommand(cluster, cmd, source);
    GenericTestUtils.waitFor(
        () -> supervisor.getReplicationFailureCount(ReplicationTask.METRIC_NAME) >= previousFailureCount + 1,
        100, 30000);
  }

  private static void queueReplicationCommand(MiniOzoneCluster cluster,
      ReplicateContainerCommand cmd, DatanodeDetails source) throws IOException {
    DatanodeStateMachine stateMachine = cluster.getHddsDatanode(source).getDatanodeStateMachine();
    StateContext context = stateMachine.getContext();
    context.getTermOfLeaderSCM().ifPresent(cmd::setTerm);
    context.addCommand(cmd);
  }

  private static boolean hasContainer(MiniOzoneCluster cluster,
      DatanodeDetails datanode, long containerId) {
    try {
      return cluster.getHddsDatanode(datanode).getDatanodeStateMachine().getContainer()
          .getContainerSet().getContainer(containerId) != null;
    } catch (IOException e) {
      return false;
    }
  }

  private static long findOrCreateContainerOnVolume(MiniOzoneCluster cluster,
      XceiverClientFactory clientFactory, DatanodeDetails dn, HddsVolume targetVolume) throws Exception {
    for (int attempt = 0; attempt < 30; attempt++) {
      long containerId = createClosedContainer(clientFactory, dn);
      Container<?> container = getContainer(cluster, dn, containerId);
      if (targetVolume.equals(container.getContainerData().getVolume())) {
        return containerId;
      }
    }
    throw new AssertionError("Could not place container on volume " + targetVolume);
  }

  private static long createClosedContainer(XceiverClientFactory clientFactory, DatanodeDetails dn)
      throws Exception {
    long containerId = CONTAINER_ID.incrementAndGet();
    try (XceiverClientSpi client = clientFactory.acquireClient(createPipeline(singleton(dn)))) {
      createContainer(client, containerId, null, CLOSED, 0);
    }
    return containerId;
  }

  private static Container<?> getContainer(MiniOzoneCluster cluster, DatanodeDetails datanode, long containerId)
      throws IOException {
    HddsDatanodeService dnService = cluster.getHddsDatanode(datanode);
    Container<?> container = dnService.getDatanodeStateMachine().getContainer()
        .getContainerSet().getContainer(containerId);
    if (container == null) {
      throw new AssertionError("Container " + containerId + " not found on " + datanode);
    }
    return container;
  }

  private static MiniOzoneCluster newCluster(OzoneConfiguration conf, int numDatanodes) throws IOException {
    UniformDatanodesFactory uniformFactory = UniformDatanodesFactory.newBuilder()
        .setNumDataVolumes(DATA_VOLUMES)
        .build();
    return MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numDatanodes)
        .setDatanodeFactory(baseConf -> {
          OzoneConfiguration dnConf = uniformFactory.apply(baseConf);
          // UniformDatanodesFactory assigns the replication port with
          // setFromObject(new ReplicationConfig().setPort(..)), which rewrites every field of that config
          // back to its default and so drops the per-volume settings from the cluster conf. Re-apply them.
          dnConf.setBoolean(ReplicationServer.ReplicationConfig.PER_VOLUME_ENABLED_KEY, true);
          dnConf.setInt(ReplicationServer.ReplicationConfig.PER_VOLUME_STREAMS_LIMIT_KEY, PER_VOLUME_STREAMS);
          return dnConf;
        })
        .build();
  }

  private static OzoneConfiguration createConfig() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, TimeUnit.SECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setTimeDuration(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, 0, TimeUnit.SECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_EXPIRED_CONTAINER_REPLICA_OP_SCRUB_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE, 5, StorageUnit.MB);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 1, StorageUnit.MB);
    conf.set(OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION, "2s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL, "2s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, "5s");
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);

    ReplicationManagerConfiguration repConf = conf.getObject(ReplicationManagerConfiguration.class);
    repConf.setInterval(Duration.ofSeconds(1));
    repConf.setUnderReplicatedInterval(Duration.ofSeconds(1));
    repConf.setOverReplicatedInterval(Duration.ofSeconds(1));
    conf.setFromObject(repConf);

    ReplicationServer.ReplicationConfig replicationConfig =
        conf.getObject(ReplicationServer.ReplicationConfig.class);
    replicationConfig.setPerVolumeEnabled(true);
    replicationConfig.setPerVolumeStreamsLimit(PER_VOLUME_STREAMS);
    conf.setFromObject(replicationConfig);

    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    dnConf.setFailedDataVolumesTolerated(1);
    dnConf.setDiskCheckMinGap(Duration.ofSeconds(0));
    dnConf.setPeriodicDiskCheckIntervalMinutes(1);
    conf.setFromObject(dnConf);
    return conf;
  }

  private static void triggerAndWaitForVolumeFailure(MutableVolumeSet volSet, StorageVolume volume)
      throws Exception {
    DatanodeTestUtils.simulateBadVolume(volume);
    volSet.checkVolumeAsync(volume);
    DatanodeTestUtils.waitForHandleFailedVolume(volSet, 1);
  }

  private static void generateData(OzoneBucket bucket, int keyCount, String keyPrefix,
      ReplicationConfig replicationConfig) throws IOException {
    for (int i = 0; i < keyCount; i++) {
      DataTestUtil.createKey(bucket, keyPrefix + i, replicationConfig, "this is the content".getBytes(UTF_8));
    }
  }

  private static ContainerInfo waitForKeyContainer(OzoneBucket bucket,
      ContainerManager cm, String keyName, int expectedReplicas) throws Exception {
    OzoneKeyDetails keyDetails = bucket.getKey(keyName);
    long containerId = keyDetails.getOzoneKeyLocations().get(0).getContainerID();
    ContainerInfo container = cm.getContainer(ContainerID.valueOf(containerId));
    waitForContainerReplicas(cm, container, expectedReplicas);
    return container;
  }

  private static void waitForContainerReplicas(ContainerManager cm, ContainerInfo container, int count)
      throws TimeoutException, InterruptedException {
    waitForReplicas(cm, container.containerID(), count);
  }

  /**
   * Waits until SCM reports exactly {@code count} replicas. Kept at 30s per wait so that the eight waits in
   * {@link #testDecommissionWithPerVolumePools} stay inside the 5m default JUnit timeout the root pom sets,
   * and a genuine stall is reported by the predicate rather than by an anonymous test timeout.
   */
  private static void waitForReplicas(ContainerManager cm, ContainerID containerId, int count)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> replicasOf(cm, containerId).size() == count, 200, 30000);
  }

  /** SCM's replica set, treating a transient lookup failure as "not ready yet". */
  private static Set<ContainerReplica> replicasOf(ContainerManager cm, ContainerID containerId) {
    try {
      return cm.getContainerReplicas(containerId);
    } catch (Exception e) {
      return emptySet();
    }
  }

}
