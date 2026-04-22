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

package org.apache.hadoop.hdds.scm.node;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLog;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.MockRatisPipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test DeadNodeHandler.
 */
public class TestDeadNodeHandler {

  private StorageContainerManager scm;
  private SCMNodeManager nodeManager;
  private ContainerManager containerManager;
  private PipelineManagerImpl pipelineManager;
  private DeadNodeHandler deadNodeHandler;
  private HealthyReadOnlyNodeHandler healthyReadOnlyNodeHandler;
  private EventPublisher publisher;
  @TempDir
  private File storageDir;
  private DeletedBlockLog deletedBlockLog;

  @BeforeEach
  public void setup() throws IOException, AuthenticationException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        0, TimeUnit.SECONDS);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 2);
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        10, StorageUnit.MB);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, storageDir.getPath());
    EventQueue eventQueue = new EventQueue();
    scm = HddsTestUtils.getScm(conf);
    nodeManager = (SCMNodeManager) scm.getScmNodeManager();
    SCMContext scmContext = new SCMContext.Builder()
                                .setSafeModeStatus(SafeModeStatus.PRE_CHECKS_PASSED)
                                .setLeader(true)
                                .setSCM(scm).build();
    pipelineManager =
        (PipelineManagerImpl)scm.getPipelineManager();
    pipelineManager.setScmContext(scmContext);
    scm.getReplicationManager().setScmContext(scmContext);
    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), conf);
    pipelineManager.setPipelineProvider(RATIS,
        mockRatisProvider);
    containerManager = scm.getContainerManager();
    deletedBlockLog = mock(DeletedBlockLog.class);
    deadNodeHandler = new DeadNodeHandler(nodeManager,
        mock(PipelineManager.class), containerManager, deletedBlockLog);
    healthyReadOnlyNodeHandler =
        new HealthyReadOnlyNodeHandler(nodeManager,
            pipelineManager);
    eventQueue.addHandler(SCMEvents.DEAD_NODE, deadNodeHandler);
    publisher = mock(EventPublisher.class);
  }

  @AfterEach
  public void teardown() {
    scm.stop();
    scm.join();
  }

  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  public void testOnMessage(@TempDir File tempDir) throws Exception {
    //GIVEN
    DatanodeDetails datanode1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails datanode2 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails datanode3 = MockDatanodeDetails.randomDatanodeDetails();

    String storagePath = tempDir.getPath()
        .concat("/data-" + datanode1.getID());
    String metaStoragePath = tempDir.getPath()
        .concat("/metadata-" + datanode1.getID());

    StorageReportProto storageOne = HddsTestUtils.createStorageReport(
        datanode1.getID(), storagePath, 100 * OzoneConsts.TB,
        10 * OzoneConsts.TB, 90 * OzoneConsts.TB, null);
    MetadataStorageReportProto metaStorageOne =
        HddsTestUtils.createMetadataStorageReport(metaStoragePath,
            100 * OzoneConsts.GB, 10 * OzoneConsts.GB,
            90 * OzoneConsts.GB, null);

    // Exit safemode, as otherwise the safemode precheck will prevent pipelines
    // from getting created. Due to how this test is wired up, safemode will
    // not exit when the DNs are registered directly with the node manager.
    scm.exitSafeMode();
    // Standalone pipeline now excludes the nodes which are already used,
    // is the a proper behavior. Adding 9 datanodes for now to make the
    // test case happy.

    nodeManager.register(datanode1,
        HddsTestUtils.createNodeReport(Arrays.asList(storageOne),
            Arrays.asList(metaStorageOne)), null);
    nodeManager.register(datanode2,
        HddsTestUtils.createNodeReport(Arrays.asList(storageOne),
            Arrays.asList(metaStorageOne)), null);
    nodeManager.register(datanode3,
        HddsTestUtils.createNodeReport(Arrays.asList(storageOne),
            Arrays.asList(metaStorageOne)), null);

    nodeManager.register(MockDatanodeDetails.randomDatanodeDetails(),
        HddsTestUtils.createNodeReport(Arrays.asList(storageOne),
            Arrays.asList(metaStorageOne)), null);
    nodeManager.register(MockDatanodeDetails.randomDatanodeDetails(),
        HddsTestUtils.createNodeReport(Arrays.asList(storageOne),
            Arrays.asList(metaStorageOne)), null);
    nodeManager.register(MockDatanodeDetails.randomDatanodeDetails(),
        HddsTestUtils.createNodeReport(Arrays.asList(storageOne),
            Arrays.asList(metaStorageOne)), null);

    nodeManager.register(MockDatanodeDetails.randomDatanodeDetails(),
        HddsTestUtils.createNodeReport(Arrays.asList(storageOne),
            Arrays.asList(metaStorageOne)), null);
    nodeManager.register(MockDatanodeDetails.randomDatanodeDetails(),
        HddsTestUtils.createNodeReport(Arrays.asList(storageOne),
            Arrays.asList(metaStorageOne)), null);
    nodeManager.register(MockDatanodeDetails.randomDatanodeDetails(),
        HddsTestUtils.createNodeReport(Arrays.asList(storageOne),
            Arrays.asList(metaStorageOne)), null);

    LambdaTestUtils.await(120000, 1000,
        () -> pipelineManager.getPipelines(RatisReplicationConfig
                .getInstance(THREE))
            .size() > 3);
    HddsTestUtils.openAllRatisPipelines(pipelineManager);

    ContainerInfo container1 =
        HddsTestUtils.allocateContainer(containerManager);
    ContainerInfo container2 =
        HddsTestUtils.allocateContainer(containerManager);
    ContainerInfo container3 =
        HddsTestUtils.allocateContainer(containerManager);
    ContainerInfo container4 =
        HddsTestUtils.allocateContainer(containerManager);

    registerContainers(datanode1, container1, container2, container4);
    registerContainers(datanode2, container1, container2);
    registerContainers(datanode3, container3);

    registerReplicas(containerManager, container1, datanode1, datanode2);
    registerReplicas(containerManager, container2, datanode1, datanode2);
    registerReplicas(containerManager, container3, datanode3);
    registerReplicas(containerManager, container4, datanode1);

    HddsTestUtils.closeContainer(containerManager, container1.containerID());
    HddsTestUtils.closeContainer(containerManager, container2.containerID());
    HddsTestUtils.quasiCloseContainer(containerManager,
        container3.containerID());

    // First set the node to IN_MAINTENANCE and ensure the container replicas
    // are not removed on the dead event
    datanode1 = nodeManager.getNode(datanode1.getID());
    assertTrue(
        nodeManager.getClusterNetworkTopologyMap().contains(datanode1));
    nodeManager.setNodeOperationalState(datanode1,
        HddsProtos.NodeOperationalState.IN_MAINTENANCE);
    setNodeHealthState(datanode1, HddsProtos.NodeState.DEAD);
    deadNodeHandler.onMessage(datanode1, publisher);
    // make sure the node is removed from
    // ClusterNetworkTopology when it is considered as dead
    assertFalse(
        nodeManager.getClusterNetworkTopologyMap().contains(datanode1));

    verify(publisher, times(0)).fireEvent(SCMEvents.REPLICATION_MANAGER_NOTIFY, datanode1);

    clearInvocations(publisher);

    verify(deletedBlockLog, times(0))
        .onDatanodeDead(datanode1.getID());

    Set<ContainerReplica> container1Replicas = containerManager
        .getContainerReplicas(ContainerID.valueOf(container1.getContainerID()));
    assertEquals(2, container1Replicas.size());

    Set<ContainerReplica> container2Replicas = containerManager
        .getContainerReplicas(ContainerID.valueOf(container2.getContainerID()));
    assertEquals(2, container2Replicas.size());

    Set<ContainerReplica> container3Replicas = containerManager
            .getContainerReplicas(
                ContainerID.valueOf(container3.getContainerID()));
    assertEquals(1, container3Replicas.size());

    // Now set the node to anything other than IN_MAINTENANCE and the relevant
    // replicas should be removed
    DeleteBlocksCommand cmd = new DeleteBlocksCommand(Collections.emptyList());
    nodeManager.addDatanodeCommand(datanode1.getID(), cmd);
    nodeManager.setNodeOperationalState(datanode1,
        HddsProtos.NodeOperationalState.IN_SERVICE);
    setNodeHealthState(datanode1, HddsProtos.NodeState.DEAD);
    deadNodeHandler.onMessage(datanode1, publisher);
    //datanode1 has been removed from ClusterNetworkTopology, another
    //deadNodeHandler.onMessage call will not change this
    assertFalse(
        nodeManager.getClusterNetworkTopologyMap().contains(datanode1));
    assertEquals(0, nodeManager.getCommandQueueCount(datanode1.getID(), cmd.getType()));

    verify(publisher).fireEvent(SCMEvents.REPLICATION_MANAGER_NOTIFY, datanode1);
    verify(deletedBlockLog).onDatanodeDead(datanode1.getID());

    container1Replicas = containerManager
        .getContainerReplicas(ContainerID.valueOf(container1.getContainerID()));
    assertEquals(1, container1Replicas.size());
    assertEquals(datanode2, container1Replicas.iterator().next().getDatanodeDetails());

    container2Replicas = containerManager
        .getContainerReplicas(ContainerID.valueOf(container2.getContainerID()));
    assertEquals(1, container2Replicas.size());
    assertEquals(datanode2, container2Replicas.iterator().next().getDatanodeDetails());

    container3Replicas = containerManager
        .getContainerReplicas(ContainerID.valueOf(container3.getContainerID()));
    assertEquals(1, container3Replicas.size());
    assertEquals(datanode3, container3Replicas.iterator().next().getDatanodeDetails());

    //datanode will be added back to ClusterNetworkTopology if it resurrects
    healthyReadOnlyNodeHandler.onMessage(datanode1, publisher);
    assertTrue(
        nodeManager.getClusterNetworkTopologyMap().contains(datanode1));

  }

  /**
   * Verifies that DeadNodeHandler skips topology removal when the node has
   * already been resurrected (state changed from DEAD to HEALTHY_READONLY).
   */
  @Test
  public void testDeadNodeHandlerSkipsRemovalWhenNodeResurrected(
      @TempDir File tempDir) throws Exception {
    DatanodeDetails datanode = MockDatanodeDetails.randomDatanodeDetails();
    String storagePath = tempDir.getPath()
        .concat("/data-" + datanode.getID());
    String metaStoragePath = tempDir.getPath()
        .concat("/metadata-" + datanode.getID());
    StorageReportProto storageReport = HddsTestUtils.createStorageReport(
        datanode.getID(), storagePath, 100 * OzoneConsts.TB,
        10 * OzoneConsts.TB, 90 * OzoneConsts.TB, null);
    MetadataStorageReportProto metaStorageReport =
        HddsTestUtils.createMetadataStorageReport(metaStoragePath,
            100 * OzoneConsts.GB, 10 * OzoneConsts.GB,
            90 * OzoneConsts.GB, null);
    nodeManager.register(datanode,
        HddsTestUtils.createNodeReport(Arrays.asList(storageReport),
            Arrays.asList(metaStorageReport)), null);
    datanode = nodeManager.getNode(datanode.getID());

    assertTrue(
        nodeManager.getClusterNetworkTopologyMap().contains(datanode));

    // Simulate: DEAD_NODE event was fired, but before DeadNodeHandler
    // processes it, the node heartbeated and was resurrected to
    // HEALTHY_READONLY. The handler should see the current state and
    // skip removal.
    setNodeHealthState(datanode, HddsProtos.NodeState.HEALTHY_READONLY);
    deadNodeHandler.onMessage(datanode, publisher);

    assertTrue(
        nodeManager.getClusterNetworkTopologyMap().contains(datanode),
        "Node should remain in topology when it has been resurrected");
  }

  /**
   * Verifies that HealthyReadOnlyNodeHandler re-adds a node to topology
   * even if it was removed by a concurrent DeadNodeHandler.
   */
  @Test
  public void testHealthyReadOnlyHandlerAddsRemovedNode(
      @TempDir File tempDir) throws Exception {
    DatanodeDetails datanode = MockDatanodeDetails.randomDatanodeDetails();
    String storagePath = tempDir.getPath()
        .concat("/data-" + datanode.getID());
    String metaStoragePath = tempDir.getPath()
        .concat("/metadata-" + datanode.getID());
    StorageReportProto storageReport = HddsTestUtils.createStorageReport(
        datanode.getID(), storagePath, 100 * OzoneConsts.TB,
        10 * OzoneConsts.TB, 90 * OzoneConsts.TB, null);
    MetadataStorageReportProto metaStorageReport =
        HddsTestUtils.createMetadataStorageReport(metaStoragePath,
            100 * OzoneConsts.GB, 10 * OzoneConsts.GB,
            90 * OzoneConsts.GB, null);
    nodeManager.register(datanode,
        HddsTestUtils.createNodeReport(Arrays.asList(storageReport),
            Arrays.asList(metaStorageReport)), null);
    datanode = nodeManager.getNode(datanode.getID());

    // Manually remove node from topology to simulate DeadNodeHandler
    // having run first.
    nodeManager.getClusterNetworkTopologyMap().remove(datanode);
    assertFalse(
        nodeManager.getClusterNetworkTopologyMap().contains(datanode));

    // HealthyReadOnlyNodeHandler should add it back unconditionally.
    healthyReadOnlyNodeHandler.onMessage(datanode, publisher);
    assertTrue(
        nodeManager.getClusterNetworkTopologyMap().contains(datanode),
        "Node should be re-added to topology by HealthyReadOnlyNodeHandler");
  }

  /**
   * Reproduces the race condition between DeadNodeHandler and
   * HealthyReadOnlyNodeHandler where interleaved execution could leave
   * a resurrected node missing from the network topology.
   *
   * The interleaving being tested:
   * 1. DeadNodeHandler starts processing (slow: closing containers, etc.)
   * 2. Node is resurrected (DEAD -> HEALTHY_READONLY) via heartbeat
   * 3. HealthyReadOnlyNodeHandler runs, sees node in topology, does not add
   * 4. DeadNodeHandler finishes and removes node from topology
   *
   * With the fix: step 4 checks current state and skips removal.
   */
  @Test
  public void testDeadNodeAndHealthyReadOnlyRaceCondition(
      @TempDir File tempDir) throws Exception {
    DatanodeDetails datanode = MockDatanodeDetails.randomDatanodeDetails();
    String storagePath = tempDir.getPath()
        .concat("/data-" + datanode.getID());
    String metaStoragePath = tempDir.getPath()
        .concat("/metadata-" + datanode.getID());
    StorageReportProto storageReport = HddsTestUtils.createStorageReport(
        datanode.getID(), storagePath, 100 * OzoneConsts.TB,
        10 * OzoneConsts.TB, 90 * OzoneConsts.TB, null);
    MetadataStorageReportProto metaStorageReport =
        HddsTestUtils.createMetadataStorageReport(metaStoragePath,
            100 * OzoneConsts.GB, 10 * OzoneConsts.GB,
            90 * OzoneConsts.GB, null);
    nodeManager.register(datanode,
        HddsTestUtils.createNodeReport(Arrays.asList(storageReport),
            Arrays.asList(metaStorageReport)), null);
    datanode = nodeManager.getNode(datanode.getID());

    assertTrue(
        nodeManager.getClusterNetworkTopologyMap().contains(datanode));

    // Block DeadNodeHandler just before the topology removal by making
    // deletedBlockLog.onDatanodeDead() pause. This call happens right
    // before the topology check in the handler.
    CountDownLatch deadHandlerBlocked = new CountDownLatch(1);
    CountDownLatch proceedWithRemoval = new CountDownLatch(1);

    DeletedBlockLog blockingDeletedBlockLog = mock(DeletedBlockLog.class);
    doAnswer(invocation -> {
      deadHandlerBlocked.countDown();
      proceedWithRemoval.await();
      return null;
    }).when(blockingDeletedBlockLog).onDatanodeDead(any());

    setNodeHealthState(datanode, HddsProtos.NodeState.DEAD);

    DeadNodeHandler blockingDeadHandler = new DeadNodeHandler(nodeManager,
        mock(PipelineManager.class), containerManager,
        blockingDeletedBlockLog);

    DatanodeDetails finalDatanode = datanode;
    AtomicReference<Exception> threadException = new AtomicReference<>();
    Thread deadHandlerThread = new Thread(() -> {
      try {
        blockingDeadHandler.onMessage(finalDatanode, publisher);
      } catch (Exception e) {
        threadException.set(e);
      }
    });
    deadHandlerThread.start();

    // Wait for DeadNodeHandler to be blocked mid-execution.
    assertTrue(deadHandlerBlocked.await(10, TimeUnit.SECONDS),
        "DeadNodeHandler should have started processing");

    // Simulate resurrection: node transitions to HEALTHY_READONLY.
    setNodeHealthState(datanode, HddsProtos.NodeState.HEALTHY_READONLY);

    // HealthyReadOnlyNodeHandler runs while DeadNodeHandler is blocked.
    healthyReadOnlyNodeHandler.onMessage(datanode, publisher);

    // Release the DeadNodeHandler to finish.
    proceedWithRemoval.countDown();
    deadHandlerThread.join(10_000);

    assertNull(threadException.get(),
        "DeadNodeHandler should not throw");

    // With the fix, the node should remain in topology because
    // DeadNodeHandler sees the node is no longer DEAD and skips removal.
    assertTrue(
        nodeManager.getClusterNetworkTopologyMap().contains(datanode),
        "Resurrected node must remain in topology after race between "
            + "DeadNodeHandler and HealthyReadOnlyNodeHandler");
  }

  private void setNodeHealthState(DatanodeDetails datanode,
      HddsProtos.NodeState healthState) throws NodeNotFoundException {
    DatanodeInfo dnInfo = nodeManager.getNodeStateManager()
        .getNode(datanode);
    NodeStatus current = dnInfo.getNodeStatus();
    dnInfo.setNodeStatus(NodeStatus.valueOf(
        current.getOperationalState(), healthState));
  }

  private void registerReplicas(ContainerManager contManager,
                   ContainerInfo container, DatanodeDetails... datanodes)
      throws ContainerNotFoundException {
    for (DatanodeDetails datanode : datanodes) {
      contManager.updateContainerReplica(
          ContainerID.valueOf(container.getContainerID()),
          ContainerReplica.newBuilder()
              .setContainerState(ContainerReplicaProto.State.OPEN)
              .setContainerID(container.containerID())
              .setDatanodeDetails(datanode).build());
    }
  }

  /**
   * Update containers available on the datanode.
   */
  private void registerContainers(DatanodeDetails datanode,
      ContainerInfo... containers)
      throws NodeNotFoundException {
    ScmNodeTestUtil.setContainers(nodeManager,
        datanode,
            Arrays.stream(containers)
                .map(ContainerInfo::containerID)
                .collect(Collectors.toSet()));
  }
}
