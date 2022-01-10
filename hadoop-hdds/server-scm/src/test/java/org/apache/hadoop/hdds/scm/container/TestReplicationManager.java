/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container;

import com.google.common.primitives.Longs;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.container.ReplicationManager
    .ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.container.ReplicationManager.MoveResult;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.MockSCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.metadata.SCMDBTransactionBufferImpl;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.TestClock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.createDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.STALE;
import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.scm.TestUtils.getContainer;
import static org.apache.hadoop.hdds.scm.TestUtils.getReplicas;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.mockito.Mockito.when;

/**
 * Test cases to verify the functionality of ReplicationManager.
 */
public class TestReplicationManager {

  private ReplicationManager replicationManager;
  private ContainerStateManager containerStateManager;
  private PlacementPolicy containerPlacementPolicy;
  private EventQueue eventQueue;
  private DatanodeCommandHandler datanodeCommandHandler;
  private SimpleMockNodeManager nodeManager;
  private ContainerManager containerManager;
  private GenericTestUtils.LogCapturer scmLogs;
  private SCMServiceManager serviceManager;
  private TestClock clock;
  private File testDir;
  private DBStore dbStore;
  private PipelineManager pipelineManager;
  private SCMHAManager scmhaManager;

  @Before
  public void setup()
      throws IOException, InterruptedException,
      NodeNotFoundException, InvalidStateTransitionException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        0, TimeUnit.SECONDS);

    scmLogs = GenericTestUtils.LogCapturer.captureLogs(ReplicationManager.LOG);
    containerManager = Mockito.mock(ContainerManager.class);
    nodeManager = new SimpleMockNodeManager();
    eventQueue = new EventQueue();
    scmhaManager = MockSCMHAManager.getInstance(true);
    testDir = GenericTestUtils.getTestDir(
        TestContainerManagerImpl.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(
        conf, new SCMDBDefinition());
    pipelineManager = Mockito.mock(PipelineManager.class);
    when(pipelineManager.containsPipeline(Mockito.any(PipelineID.class)))
        .thenReturn(true);
    containerStateManager = ContainerStateManagerImpl.newBuilder()
        .setConfiguration(conf)
        .setPipelineManager(pipelineManager)
        .setRatisServer(scmhaManager.getRatisServer())
        .setContainerStore(SCMDBDefinition.CONTAINERS.getTable(dbStore))
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();
    serviceManager = new SCMServiceManager();

    datanodeCommandHandler = new DatanodeCommandHandler();
    eventQueue.addHandler(SCMEvents.DATANODE_COMMAND, datanodeCommandHandler);

    Mockito.when(containerManager.getContainers())
        .thenAnswer(invocation -> {
          Set<ContainerID> ids = containerStateManager.getContainerIDs();
          List<ContainerInfo> containers = new ArrayList<>();
          for (ContainerID id : ids) {
            containers.add(containerStateManager.getContainer(
                id.getProtobuf()));
          }
          return containers;
        });

    Mockito.when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainer(((ContainerID)invocation
                .getArguments()[0]).getProtobuf()));

    Mockito.when(containerManager.getContainerReplicas(
        Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainerReplicas(((ContainerID)invocation
                .getArguments()[0]).getProtobuf()));

    containerPlacementPolicy = Mockito.mock(PlacementPolicy.class);

    Mockito.when(containerPlacementPolicy.chooseDatanodes(
        Mockito.any(), Mockito.any(), Mockito.anyInt(),
            Mockito.anyLong(), Mockito.anyLong()))
        .thenAnswer(invocation -> {
          int count = (int) invocation.getArguments()[2];
          return IntStream.range(0, count)
              .mapToObj(i -> randomDatanodeDetails())
              .collect(Collectors.toList());
        });

    Mockito.when(containerPlacementPolicy.validateContainerPlacement(
        Mockito.any(),
        Mockito.anyInt()
        )).thenAnswer(invocation ->
        new ContainerPlacementStatusDefault(2, 2, 3));
    clock = new TestClock(Instant.now(), ZoneId.of("UTC"));
    createReplicationManager(new ReplicationManagerConfiguration());
  }

  private void createReplicationManager(ReplicationManagerConfiguration rmConf)
      throws InterruptedException, IOException {
    OzoneConfiguration config = new OzoneConfiguration();
    testDir = GenericTestUtils
      .getTestDir(TestContainerManagerImpl.class.getSimpleName());
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        testDir.getAbsolutePath());
    config.setTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        0, TimeUnit.SECONDS);
    config.setFromObject(rmConf);

    SCMHAManager scmHAManager = MockSCMHAManager
        .getInstance(true, new SCMDBTransactionBufferImpl());
    dbStore = DBStoreBuilder.createDBStore(
      config, new SCMDBDefinition());

    replicationManager = new ReplicationManager(
        config,
        containerManager,
        containerPlacementPolicy,
        eventQueue,
        SCMContext.emptyContext(),
        serviceManager,
        nodeManager,
        clock,
        scmHAManager,
        SCMDBDefinition.MOVE.getTable(dbStore));

    serviceManager.notifyStatusChanged();
    scmLogs.clearOutput();
    Thread.sleep(100L);
  }

  @After
  public void tearDown() throws Exception {
    containerStateManager.close();
    if (dbStore != null) {
      dbStore.close();
    }

    FileUtil.fullyDelete(testDir);
  }

  /**
   * Checks if restarting of replication manager works.
   */
  @Test
  public void testReplicationManagerRestart() throws InterruptedException {
    Assert.assertTrue(replicationManager.isRunning());
    replicationManager.stop();
    // Stop is a non-blocking call, it might take sometime for the
    // ReplicationManager to shutdown
    Thread.sleep(500);
    Assert.assertFalse(replicationManager.isRunning());
    replicationManager.start();
    Assert.assertTrue(replicationManager.isRunning());
  }

  /**
   * Open containers are not handled by ReplicationManager.
   * This test-case makes sure that ReplicationManages doesn't take
   * any action on OPEN containers.
   */
  @Test
  public void testOpenContainer() throws IOException {
    final ContainerInfo container = getContainer(LifeCycleState.OPEN);
    containerStateManager.addContainer(container.getProtobuf());
    replicationManager.processAll();
    eventQueue.processAll(1000);
    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.OPEN));
    Assert.assertEquals(0, datanodeCommandHandler.getInvocation());
  }

  /**
   * If the container is in CLOSING state we resend close container command
   * to all the datanodes.
   */
  @Test
  public void testClosingContainer() throws IOException {
    final ContainerInfo container = getContainer(LifeCycleState.CLOSING);
    final ContainerID id = container.containerID();

    containerStateManager.addContainer(container.getProtobuf());

    // Two replicas in CLOSING state
    final Set<ContainerReplica> replicas = getReplicas(id, State.CLOSING,
        randomDatanodeDetails(),
        randomDatanodeDetails());

    // One replica in OPEN state
    final DatanodeDetails datanode = randomDatanodeDetails();
    replicas.addAll(getReplicas(id, State.OPEN, datanode));

    for (ContainerReplica replica : replicas) {
      containerStateManager.updateContainerReplica(id.getProtobuf(), replica);
    }

    final int currentCloseCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentCloseCommandCount + 3, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand));

    // Update the OPEN to CLOSING
    for (ContainerReplica replica : getReplicas(id, State.CLOSING, datanode)) {
      containerStateManager.updateContainerReplica(id.getProtobuf(), replica);
    }

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentCloseCommandCount + 6, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand));
    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.CLOSING));
  }


  /**
   * The container is QUASI_CLOSED but two of the replica is still in
   * open state. ReplicationManager should resend close command to those
   * datanodes.
   */
  @Test
  public void testQuasiClosedContainerWithTwoOpenReplica() throws IOException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.OPEN, 1000L, originNodeId, randomDatanodeDetails());
    final DatanodeDetails datanodeDetails = randomDatanodeDetails();
    final ContainerReplica replicaThree = getReplicas(
        id, State.OPEN, 1000L, datanodeDetails.getUuid(), datanodeDetails);

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaOne);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaTwo);
    containerStateManager.updateContainerReplica(
        id.getProtobuf(), replicaThree);

    final int currentCloseCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand);
    // Two of the replicas are in OPEN state
    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentCloseCommandCount + 2, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand));
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.closeContainerCommand,
        replicaTwo.getDatanodeDetails()));
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.closeContainerCommand,
        replicaThree.getDatanodeDetails()));
    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
  }

  /**
   * When the container is in QUASI_CLOSED state and all the replicas are
   * also in QUASI_CLOSED state and doesn't have a quorum to force close
   * the container, ReplicationManager will not do anything.
   */
  @Test
  public void testHealthyQuasiClosedContainer() throws IOException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaOne);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaTwo);
    containerStateManager.updateContainerReplica(
        id.getProtobuf(), replicaThree);

    // All the QUASI_CLOSED replicas have same originNodeId, so the
    // container will not be closed. ReplicationManager should take no action.
    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(0, datanodeCommandHandler.getInvocation());
    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
  }

  /**
   * When a container is QUASI_CLOSED and we don't have quorum to force close
   * the container, the container should have all the replicas in QUASI_CLOSED
   * state, else ReplicationManager will take action.
   *
   * In this test case we make one of the replica unhealthy, replication manager
   * will send delete container command to the datanode which has the unhealthy
   * replica.
   */
  @Test
  public void testQuasiClosedContainerWithUnhealthyReplica()
      throws IOException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    container.setUsedBytes(100);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaOne);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaTwo);
    containerStateManager.updateContainerReplica(
        id.getProtobuf(), replicaThree);

    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);
    final int currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);

    // All the QUASI_CLOSED replicas have same originNodeId, so the
    // container will not be closed. ReplicationManager should take no action.
    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(0, datanodeCommandHandler.getInvocation());

    // Make the first replica unhealthy
    final ContainerReplica unhealthyReplica = getReplicas(
        id, State.UNHEALTHY, 1000L, originNodeId,
        replicaOne.getDatanodeDetails());
    containerStateManager.updateContainerReplica(
        id.getProtobuf(), unhealthyReplica);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentDeleteCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.deleteContainerCommand,
        replicaOne.getDatanodeDetails()));
    Assert.assertEquals(currentDeleteCommandCount + 1,
        replicationManager.getMetrics().getNumDeletionCmdsSent());

    // Now we will delete the unhealthy replica from in-memory.
    containerStateManager.removeContainerReplica(id.getProtobuf(), replicaOne);

    final long currentBytesToReplicate = replicationManager.getMetrics()
        .getNumReplicationBytesTotal();

    // The container is under replicated as unhealthy replica is removed
    replicationManager.processAll();
    eventQueue.processAll(1000);

    // We should get replicate command
    Assert.assertEquals(currentReplicateCommandCount + 1,
        datanodeCommandHandler.getInvocationCount(
            SCMCommandProto.Type.replicateContainerCommand));
    Assert.assertEquals(currentReplicateCommandCount + 1,
        replicationManager.getMetrics().getNumReplicationCmdsSent());
    Assert.assertEquals(currentBytesToReplicate + 100L,
        replicationManager.getMetrics().getNumReplicationBytesTotal());
    Assert.assertEquals(1, replicationManager.getInflightReplication().size());
    Assert.assertEquals(1, replicationManager.getMetrics()
        .getInflightReplication());

    // We should have one under replicated and one quasi_closed_stuck
    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));

    // Now we add the missing replica back
    DatanodeDetails targetDn = replicationManager.getInflightReplication()
        .get(id).get(0).getDatanode();
    final ContainerReplica replicatedReplicaOne = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, targetDn);
    containerStateManager.updateContainerReplica(
        id.getProtobuf(), replicatedReplicaOne);

    final long currentReplicationCommandCompleted = replicationManager
        .getMetrics().getNumReplicationCmdsCompleted();
    final long currentBytesCompleted = replicationManager.getMetrics()
        .getNumReplicationBytesCompleted();

    replicationManager.processAll();
    eventQueue.processAll(1000);

    Assert.assertEquals(0, replicationManager.getInflightReplication().size());
    Assert.assertEquals(0, replicationManager.getMetrics()
        .getInflightReplication());
    Assert.assertEquals(currentReplicationCommandCompleted + 1,
        replicationManager.getMetrics().getNumReplicationCmdsCompleted());
    Assert.assertEquals(currentBytesCompleted + 100L,
        replicationManager.getMetrics().getNumReplicationBytesCompleted());

    report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
    Assert.assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
  }

  /**
   * When a QUASI_CLOSED container is over replicated, ReplicationManager
   * deletes the excess replicas.
   */
  @Test
  public void testOverReplicatedQuasiClosedContainer() throws IOException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    container.setUsedBytes(101);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaFour = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaOne);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaTwo);
    containerStateManager.updateContainerReplica(
        id.getProtobuf(), replicaThree);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaFour);

    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentDeleteCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    Assert.assertEquals(currentDeleteCommandCount + 1,
        replicationManager.getMetrics().getNumDeletionCmdsSent());
    Assert.assertEquals(1, replicationManager.getInflightDeletion().size());
    Assert.assertEquals(1, replicationManager.getMetrics()
        .getInflightDeletion());

    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));

    // Now we remove the replica according to inflight
    DatanodeDetails targetDn = replicationManager.getInflightDeletion()
        .get(id).get(0).getDatanode();
    if (targetDn.equals(replicaOne.getDatanodeDetails())) {
      containerStateManager.removeContainerReplica(
          id.getProtobuf(), replicaOne);
    } else if (targetDn.equals(replicaTwo.getDatanodeDetails())) {
      containerStateManager.removeContainerReplica(
          id.getProtobuf(), replicaTwo);
    } else if (targetDn.equals(replicaThree.getDatanodeDetails())) {
      containerStateManager.removeContainerReplica(
          id.getProtobuf(), replicaThree);
    } else if (targetDn.equals(replicaFour.getDatanodeDetails())) {
      containerStateManager.removeContainerReplica(
          id.getProtobuf(), replicaFour);
    }

    final long currentDeleteCommandCompleted = replicationManager.getMetrics()
        .getNumDeletionCmdsCompleted();
    final long deleteBytesCompleted =
        replicationManager.getMetrics().getNumDeletionBytesCompleted();

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(0, replicationManager.getInflightDeletion().size());
    Assert.assertEquals(0, replicationManager.getMetrics()
        .getInflightDeletion());
    Assert.assertEquals(currentDeleteCommandCompleted + 1,
        replicationManager.getMetrics().getNumDeletionCmdsCompleted());
    Assert.assertEquals(deleteBytesCompleted + 101,
        replicationManager.getMetrics().getNumDeletionBytesCompleted());

    report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
    Assert.assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  /**
   * When a QUASI_CLOSED container is over replicated, ReplicationManager
   * deletes the excess replicas. While choosing the replica for deletion
   * ReplicationManager should prioritize unhealthy replica over QUASI_CLOSED
   * replica.
   */
  @Test
  public void testOverReplicatedQuasiClosedContainerWithUnhealthyReplica()
      throws IOException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.UNHEALTHY, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaFour = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaOne);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaTwo);
    containerStateManager.updateContainerReplica(
        id.getProtobuf(), replicaThree);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaFour);

    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentDeleteCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.deleteContainerCommand,
        replicaOne.getDatanodeDetails()));
    Assert.assertEquals(currentDeleteCommandCount + 1,
        replicationManager.getMetrics().getNumDeletionCmdsSent());
    Assert.assertEquals(1, replicationManager.getInflightDeletion().size());
    Assert.assertEquals(1, replicationManager.getMetrics()
        .getInflightDeletion());

    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));

    final long currentDeleteCommandCompleted = replicationManager.getMetrics()
        .getNumDeletionCmdsCompleted();
    // Now we remove the replica to simulate deletion complete
    containerStateManager.removeContainerReplica(id.getProtobuf(), replicaOne);

    replicationManager.processAll();
    eventQueue.processAll(1000);

    Assert.assertEquals(currentDeleteCommandCompleted + 1,
        replicationManager.getMetrics().getNumDeletionCmdsCompleted());
    Assert.assertEquals(0, replicationManager.getInflightDeletion().size());
    Assert.assertEquals(0, replicationManager.getMetrics()
        .getInflightDeletion());

    report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
    Assert.assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  /**
   * ReplicationManager should replicate an QUASI_CLOSED replica if it is
   * under replicated.
   */
  @Test
  public void testUnderReplicatedQuasiClosedContainer() throws IOException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    container.setUsedBytes(100);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaOne);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaTwo);

    final int currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);
    final long currentBytesToReplicate = replicationManager.getMetrics()
        .getNumReplicationBytesTotal();

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentReplicateCommandCount + 1,
        datanodeCommandHandler.getInvocationCount(
            SCMCommandProto.Type.replicateContainerCommand));
    Assert.assertEquals(currentReplicateCommandCount + 1,
        replicationManager.getMetrics().getNumReplicationCmdsSent());
    Assert.assertEquals(currentBytesToReplicate + 100,
        replicationManager.getMetrics().getNumReplicationBytesTotal());
    Assert.assertEquals(1, replicationManager.getInflightReplication().size());
    Assert.assertEquals(1, replicationManager.getMetrics()
        .getInflightReplication());

    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));

    final long currentReplicateCommandCompleted = replicationManager
        .getMetrics().getNumReplicationCmdsCompleted();
    final long currentReplicateBytesCompleted = replicationManager
        .getMetrics().getNumReplicationBytesCompleted();

    // Now we add the replicated new replica
    DatanodeDetails targetDn = replicationManager.getInflightReplication()
        .get(id).get(0).getDatanode();
    final ContainerReplica replicatedReplicaThree = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, targetDn);
    containerStateManager.updateContainerReplica(
        id.getProtobuf(), replicatedReplicaThree);

    replicationManager.processAll();
    eventQueue.processAll(1000);

    Assert.assertEquals(currentReplicateCommandCompleted + 1,
        replicationManager.getMetrics().getNumReplicationCmdsCompleted());
    Assert.assertEquals(currentReplicateBytesCompleted + 100,
        replicationManager.getMetrics().getNumReplicationBytesCompleted());
    Assert.assertEquals(0, replicationManager.getInflightReplication().size());
    Assert.assertEquals(0, replicationManager.getMetrics()
        .getInflightReplication());

    report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
    Assert.assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
  }

  /**
   * When a QUASI_CLOSED container is under replicated, ReplicationManager
   * should re-replicate it. If there are any unhealthy replica, it has to
   * be deleted.
   *
   * In this test case, the container is QUASI_CLOSED and is under replicated
   * and also has an unhealthy replica.
   *
   * In the first iteration of ReplicationManager, it should re-replicate
   * the container so that it has enough replicas.
   *
   * In the second iteration, ReplicationManager should delete the unhealthy
   * replica.
   *
   * In the third iteration, ReplicationManager will re-replicate as the
   * container has again become under replicated after the unhealthy
   * replica has been deleted.
   *
   */
  @Test
  public void testUnderReplicatedQuasiClosedContainerWithUnhealthyReplica()
      throws IOException, InterruptedException,
      TimeoutException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    container.setUsedBytes(99);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.UNHEALTHY, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaOne);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaTwo);

    final int currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);
    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);
    final long currentBytesToDelete = replicationManager.getMetrics()
        .getNumDeletionBytesTotal();

    replicationManager.processAll();
    GenericTestUtils.waitFor(
        () -> (currentReplicateCommandCount + 1) == datanodeCommandHandler
            .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand),
        50, 5000);

    Optional<CommandForDatanode> replicateCommand = datanodeCommandHandler
        .getReceivedCommands().stream()
        .filter(c -> c.getCommand().getType()
            .equals(SCMCommandProto.Type.replicateContainerCommand))
        .findFirst();

    Assert.assertTrue(replicateCommand.isPresent());

    DatanodeDetails newNode = createDatanodeDetails(
        replicateCommand.get().getDatanodeId());
    ContainerReplica newReplica = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, newNode);
    containerStateManager.updateContainerReplica(id.getProtobuf(), newReplica);

    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.UNHEALTHY));

    /*
     * We have report the replica to SCM, in the next ReplicationManager
     * iteration it should delete the unhealthy replica.
     */

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentDeleteCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    // ReplicaTwo should be deleted, that is the unhealthy one
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.deleteContainerCommand,
        replicaTwo.getDatanodeDetails()));
    Assert.assertEquals(currentDeleteCommandCount + 1,
        replicationManager.getMetrics().getNumDeletionCmdsSent());
    Assert.assertEquals(currentBytesToDelete + 99,
        replicationManager.getMetrics().getNumDeletionBytesTotal());
    Assert.assertEquals(1, replicationManager.getInflightDeletion().size());
    Assert.assertEquals(1, replicationManager.getMetrics()
        .getInflightDeletion());

    containerStateManager.removeContainerReplica(id.getProtobuf(), replicaTwo);

    final long currentDeleteCommandCompleted = replicationManager.getMetrics()
        .getNumDeletionCmdsCompleted();

    report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
    Assert.assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNHEALTHY));
    /*
     * We have now removed unhealthy replica, next iteration of
     * ReplicationManager should re-replicate the container as it
     * is under replicated now
     */

    replicationManager.processAll();
    eventQueue.processAll(1000);

    Assert.assertEquals(0, replicationManager.getInflightDeletion().size());
    Assert.assertEquals(0, replicationManager.getMetrics()
        .getInflightDeletion());
    Assert.assertEquals(currentDeleteCommandCompleted + 1,
        replicationManager.getMetrics().getNumDeletionCmdsCompleted());

    Assert.assertEquals(currentReplicateCommandCount + 2,
        datanodeCommandHandler.getInvocationCount(
            SCMCommandProto.Type.replicateContainerCommand));
    Assert.assertEquals(currentReplicateCommandCount + 2,
        replicationManager.getMetrics().getNumReplicationCmdsSent());
    Assert.assertEquals(1, replicationManager.getInflightReplication().size());
    Assert.assertEquals(1, replicationManager.getMetrics()
        .getInflightReplication());

    report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.UNHEALTHY));
  }


  /**
   * When a container is QUASI_CLOSED and it has >50% of its replica
   * in QUASI_CLOSED state with unique origin node id,
   * ReplicationManager should force close the replica(s) with
   * highest BCSID.
   */
  @Test
  public void testQuasiClosedToClosed() throws IOException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final Set<ContainerReplica> replicas = getReplicas(id, State.QUASI_CLOSED,
        randomDatanodeDetails(),
        randomDatanodeDetails(),
        randomDatanodeDetails());
    containerStateManager.addContainer(container.getProtobuf());
    for (ContainerReplica replica : replicas) {
      containerStateManager.updateContainerReplica(id.getProtobuf(), replica);
    }

    final int currentCloseCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);

    // All the replicas have same BCSID, so all of them will be closed.
    Assert.assertEquals(currentCloseCommandCount + 3, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand));

    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
    Assert.assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
  }


  /**
   * ReplicationManager should not take any action if the container is
   * CLOSED and healthy.
   */
  @Test
  public void testHealthyClosedContainer() throws IOException {
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    final ContainerID id = container.containerID();
    final Set<ContainerReplica> replicas = getReplicas(id, State.CLOSED,
        randomDatanodeDetails(),
        randomDatanodeDetails(),
        randomDatanodeDetails());

    containerStateManager.addContainer(container.getProtobuf());
    for (ContainerReplica replica : replicas) {
      containerStateManager.updateContainerReplica(id.getProtobuf(), replica);
    }

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(0, datanodeCommandHandler.getInvocation());

    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.CLOSED));
    for (ReplicationManagerReport.HealthState s :
        ReplicationManagerReport.HealthState.values()) {
      Assert.assertEquals(0, report.getStat(s));
    }
  }

  /**
   * ReplicationManager should close the unhealthy OPEN container.
   */
  @Test
  public void testUnhealthyOpenContainer() throws IOException {
    final ContainerInfo container = getContainer(LifeCycleState.OPEN);
    final ContainerID id = container.containerID();
    final Set<ContainerReplica> replicas = getReplicas(id, State.OPEN,
        randomDatanodeDetails(),
        randomDatanodeDetails());
    replicas.addAll(getReplicas(id, State.UNHEALTHY, randomDatanodeDetails()));

    containerStateManager.addContainer(container.getProtobuf());
    for (ContainerReplica replica : replicas) {
      containerStateManager.updateContainerReplica(id.getProtobuf(), replica);
    }

    final CloseContainerEventHandler closeContainerHandler =
        Mockito.mock(CloseContainerEventHandler.class);
    eventQueue.addHandler(SCMEvents.CLOSE_CONTAINER, closeContainerHandler);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Mockito.verify(closeContainerHandler, Mockito.times(1))
        .onMessage(id, eventQueue);

    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.OPEN));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.OPEN_UNHEALTHY));
  }

  /**
   * ReplicationManager should skip send close command to unhealthy replica.
   */
  @Test
  public void testCloseUnhealthyReplica() throws IOException {
    final ContainerInfo container = getContainer(LifeCycleState.CLOSING);
    final ContainerID id = container.containerID();
    final Set<ContainerReplica> replicas = getReplicas(id, State.UNHEALTHY,
        randomDatanodeDetails());
    replicas.addAll(getReplicas(id, State.OPEN, randomDatanodeDetails()));
    replicas.addAll(getReplicas(id, State.OPEN, randomDatanodeDetails()));

    containerStateManager.addContainer(container.getProtobuf());
    for (ContainerReplica replica : replicas) {
      containerStateManager.updateContainerReplica(id.getProtobuf(), replica);
    }

    replicationManager.processAll();
    // Wait for EventQueue to call the event handler
    eventQueue.processAll(1000);
    Assert.assertEquals(2, datanodeCommandHandler.getInvocation());
  }

  @Test
  public void testGeneratedConfig() {
    ReplicationManagerConfiguration rmc =
        OzoneConfiguration.newInstanceOf(ReplicationManagerConfiguration.class);

    //default is not included in ozone-site.xml but generated from annotation
    //to the ozone-site-generated.xml which should be loaded by the
    // OzoneConfiguration.
    Assert.assertEquals(1800000, rmc.getEventTimeout());

  }

  @Test
  public void additionalReplicaScheduledWhenMisReplicated() throws IOException {
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    container.setUsedBytes(100);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaOne);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaTwo);
    containerStateManager.updateContainerReplica(
        id.getProtobuf(), replicaThree);

    // Ensure a mis-replicated status is returned for any containers in this
    // test where there are 3 replicas. When there are 2 or 4 replicas
    // the status returned will be healthy.
    Mockito.when(containerPlacementPolicy.validateContainerPlacement(
        Mockito.argThat(list -> list.size() == 3),
        Mockito.anyInt()
    )).thenAnswer(invocation ->  {
      return new ContainerPlacementStatusDefault(1, 2, 3);
    });

    int currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);
    final long currentBytesToReplicate = replicationManager.getMetrics()
        .getNumReplicationBytesTotal();

    replicationManager.processAll();
    eventQueue.processAll(1000);
    // At this stage, due to the mocked calls to validateContainerPlacement
    // the policy will not be satisfied, and replication will be triggered.

    Assert.assertEquals(currentReplicateCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand));
    Assert.assertEquals(currentReplicateCommandCount + 1,
        replicationManager.getMetrics().getNumReplicationCmdsSent());
    Assert.assertEquals(currentBytesToReplicate + 100,
        replicationManager.getMetrics().getNumReplicationBytesTotal());
    Assert.assertEquals(1, replicationManager.getInflightReplication().size());
    Assert.assertEquals(1, replicationManager.getMetrics()
        .getInflightReplication());

    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.CLOSED));
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.MIS_REPLICATED));

    // Now make it so that all containers seem mis-replicated no matter how
    // many replicas. This will test replicas are not scheduled if the new
    // replica does not fix the mis-replication.
    Mockito.when(containerPlacementPolicy.validateContainerPlacement(
        Mockito.anyList(),
        Mockito.anyInt()
    )).thenAnswer(invocation ->  {
      return new ContainerPlacementStatusDefault(1, 2, 3);
    });

    currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    // At this stage, due to the mocked calls to validateContainerPlacement
    // the mis-replicated racks will not have improved, so expect to see nothing
    // scheduled.
    Assert.assertEquals(currentReplicateCommandCount, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand));
    Assert.assertEquals(currentReplicateCommandCount,
        replicationManager.getMetrics().getNumReplicationCmdsSent());
    Assert.assertEquals(1, replicationManager.getInflightReplication().size());
    Assert.assertEquals(1, replicationManager.getMetrics()
        .getInflightReplication());
  }

  @Test
  public void overReplicatedButRemovingMakesMisReplicated() throws IOException {
    // In this test, the excess replica should not be removed.
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaFour = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaFive = getReplicas(
        id, State.UNHEALTHY, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaOne);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaTwo);
    containerStateManager.updateContainerReplica(
        id.getProtobuf(), replicaThree);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaFour);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaFive);

    // Ensure a mis-replicated status is returned for any containers in this
    // test where there are exactly 3 replicas checked.
    Mockito.when(containerPlacementPolicy.validateContainerPlacement(
        Mockito.argThat(list -> list.size() == 3),
        Mockito.anyInt()
    )).thenAnswer(
        invocation -> new ContainerPlacementStatusDefault(1, 2, 3));

    int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    // The unhealthy replica should be removed, but not the other replica
    // as each time we test with 3 replicas, Mockito ensures it returns
    // mis-replicated
    Assert.assertEquals(currentDeleteCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    Assert.assertEquals(currentDeleteCommandCount + 1,
        replicationManager.getMetrics().getNumDeletionCmdsSent());

    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.deleteContainerCommand,
        replicaFive.getDatanodeDetails()));
    Assert.assertEquals(1, replicationManager.getInflightDeletion().size());
    Assert.assertEquals(1, replicationManager.getMetrics()
        .getInflightDeletion());
    assertOverReplicatedCount(1);
  }

  @Test
  public void testOverReplicatedAndPolicySatisfied() throws IOException {
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaFour = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaOne);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaTwo);
    containerStateManager.updateContainerReplica(
        id.getProtobuf(), replicaThree);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaFour);

    Mockito.when(containerPlacementPolicy.validateContainerPlacement(
        Mockito.argThat(list -> list.size() == 3),
        Mockito.anyInt()
    )).thenAnswer(
        invocation -> new ContainerPlacementStatusDefault(2, 2, 3));

    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentDeleteCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    Assert.assertEquals(currentDeleteCommandCount + 1,
        replicationManager.getMetrics().getNumDeletionCmdsSent());
    Assert.assertEquals(1, replicationManager.getInflightDeletion().size());
    Assert.assertEquals(1, replicationManager.getMetrics()
        .getInflightDeletion());

    assertOverReplicatedCount(1);
  }

  @Test
  public void testOverReplicatedAndPolicyUnSatisfiedAndDeleted() throws
      IOException {
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaFour = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaFive = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaOne);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaTwo);
    containerStateManager.updateContainerReplica(
        id.getProtobuf(), replicaThree);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaFour);
    containerStateManager.updateContainerReplica(id.getProtobuf(), replicaFive);

    Mockito.when(containerPlacementPolicy.validateContainerPlacement(
        Mockito.argThat(list -> list != null && list.size() <= 4),
        Mockito.anyInt()
    )).thenAnswer(
        invocation -> new ContainerPlacementStatusDefault(1, 2, 3));

    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentDeleteCommandCount + 2, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    Assert.assertEquals(currentDeleteCommandCount + 2,
        replicationManager.getMetrics().getNumDeletionCmdsSent());
    Assert.assertEquals(1, replicationManager.getInflightDeletion().size());
    Assert.assertEquals(1, replicationManager.getMetrics()
        .getInflightDeletion());
  }

  /**
   * ReplicationManager should replicate an additional replica if there are
   * decommissioned replicas.
   */
  @Test
  public void testUnderReplicatedDueToDecommission() throws IOException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONING, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONING, HEALTHY), CLOSED);
    assertReplicaScheduled(2);
    assertUnderReplicatedCount(1);
  }

  /**
   * ReplicationManager should replicate an additional replica when all copies
   * are decommissioning.
   */
  @Test
  public void testUnderReplicatedDueToAllDecommission() throws IOException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONING, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONING, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONING, HEALTHY), CLOSED);
    assertReplicaScheduled(3);
    assertUnderReplicatedCount(1);
  }

  /**
   * ReplicationManager should not take any action when the container is
   * correctly replicated with decommissioned replicas still present.
   */
  @Test
  public void testCorrectlyReplicatedWithDecommission() throws IOException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONING, HEALTHY), CLOSED);
    assertReplicaScheduled(0);
    assertUnderReplicatedCount(0);
  }

  /**
   * ReplicationManager should replicate an additional replica when min rep
   * is not met for maintenance.
   */
  @Test
  public void testUnderReplicatedDueToMaintenance() throws IOException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    assertReplicaScheduled(1);
    assertUnderReplicatedCount(1);
  }

  /**
   * ReplicationManager should not replicate an additional replica when if
   * min replica for maintenance is 1 and another replica is available.
   */
  @Test
  public void testNotUnderReplicatedDueToMaintenanceMinRepOne()
      throws Exception {
    replicationManager.stop();
    ReplicationManagerConfiguration newConf =
        new ReplicationManagerConfiguration();
    newConf.setMaintenanceReplicaMinimum(1);
    dbStore.close();
    createReplicationManager(newConf);
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    assertReplicaScheduled(0);
    assertUnderReplicatedCount(0);
  }

  /**
   * ReplicationManager should replicate an additional replica when all copies
   * are going off line and min rep is 1.
   */
  @Test
  public void testUnderReplicatedDueToMaintenanceMinRepOne()
      throws Exception {
    replicationManager.stop();
    ReplicationManagerConfiguration newConf =
        new ReplicationManagerConfiguration();
    newConf.setMaintenanceReplicaMinimum(1);
    dbStore.close();
    createReplicationManager(newConf);
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    assertReplicaScheduled(1);
    assertUnderReplicatedCount(1);
  }

  /**
   * ReplicationManager should replicate additional replica when all copies
   * are going into maintenance.
   */
  @Test
  public void testUnderReplicatedDueToAllMaintenance() throws IOException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    assertReplicaScheduled(2);
    assertUnderReplicatedCount(1);
  }

  /**
   * ReplicationManager should not replicate additional replica sufficient
   * replica are available.
   */
  @Test
  public void testCorrectlyReplicatedWithMaintenance() throws IOException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    assertReplicaScheduled(0);
    assertUnderReplicatedCount(0);
  }

  /**
   * ReplicationManager should replicate additional replica when all copies
   * are decommissioning or maintenance.
   */
  @Test
  public void testUnderReplicatedWithDecommissionAndMaintenance()
      throws IOException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONED, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONED, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    assertReplicaScheduled(2);
    assertUnderReplicatedCount(1);
  }

  /**
   * ReplicationManager should replicate zero replica when all copies
   * are missing.
   */
  @Test
  public void testContainerWithMissingReplicas()
      throws IOException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    assertReplicaScheduled(0);
    assertUnderReplicatedCount(1);
    assertMissingCount(1);
  }
  /**
   * When a CLOSED container is over replicated, ReplicationManager
   * deletes the excess replicas. While choosing the replica for deletion
   * ReplicationManager should not attempt to remove a DECOMMISSION or
   * MAINTENANCE replica.
   */
  @Test
  public void testOverReplicatedClosedContainerWithDecomAndMaint()
      throws IOException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, NodeStatus.inServiceHealthy(), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONED, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, NodeStatus.inServiceHealthy(), CLOSED);
    addReplica(container, NodeStatus.inServiceHealthy(), CLOSED);
    addReplica(container, NodeStatus.inServiceHealthy(), CLOSED);
    addReplica(container, NodeStatus.inServiceHealthy(), CLOSED);

    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentDeleteCommandCount + 2, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    Assert.assertEquals(currentDeleteCommandCount + 2,
        replicationManager.getMetrics().getNumDeletionCmdsSent());
    Assert.assertEquals(1, replicationManager.getInflightDeletion().size());
    Assert.assertEquals(1, replicationManager.getMetrics()
        .getInflightDeletion());
    // Get the DECOM and Maint replica and ensure none of them are scheduled
    // for removal
    Set<ContainerReplica> decom =
        containerStateManager.getContainerReplicas(
            container.containerID().getProtobuf())
        .stream()
        .filter(r -> r.getDatanodeDetails().getPersistedOpState() != IN_SERVICE)
        .collect(Collectors.toSet());
    for (ContainerReplica r : decom) {
      Assert.assertFalse(datanodeCommandHandler.received(
          SCMCommandProto.Type.deleteContainerCommand,
          r.getDatanodeDetails()));
    }
    assertOverReplicatedCount(1);
  }

  /**
   * Replication Manager should not attempt to replicate from an unhealthy
   * (stale or dead) node. To test this, setup a scenario where a replia needs
   * to be created, but mark all nodes stale. That way, no new replica will be
   * scheduled.
   */
  @Test
  public void testUnderReplicatedNotHealthySource() throws IOException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, NodeStatus.inServiceStale(), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONED, STALE), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONED, STALE), CLOSED);
    // There should be replica scheduled, but as all nodes are stale, nothing
    // gets scheduled.
    assertReplicaScheduled(0);
    assertUnderReplicatedCount(1);
  }

  /**
   * if all the prerequisites are satisfied, move should work as expected.
   */
  @Test
  public void testMove() throws IOException, NodeNotFoundException,
      InterruptedException, ExecutionException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    ContainerID id = container.containerID();
    ContainerReplica dn1 = addReplica(container,
        new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container,
        new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container,
        new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    DatanodeDetails dn3 = addNode(new NodeStatus(IN_SERVICE, HEALTHY));
    CompletableFuture<MoveResult> cf =
        replicationManager.move(id,
            new MoveDataNodePair(dn1.getDatanodeDetails(), dn3));
    Assert.assertTrue(scmLogs.getOutput().contains(
        "receive a move request about container"));
    Thread.sleep(100L);
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.replicateContainerCommand, dn3));
    Assert.assertEquals(1, datanodeCommandHandler.getInvocationCount(
        SCMCommandProto.Type.replicateContainerCommand));

    //replicate container to dn3
    addReplicaToDn(container, dn3, CLOSED);
    replicationManager.processAll();
    eventQueue.processAll(1000);

    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.deleteContainerCommand, dn1.getDatanodeDetails()));
    Assert.assertEquals(1, datanodeCommandHandler.getInvocationCount(
        SCMCommandProto.Type.deleteContainerCommand));
    containerStateManager.removeContainerReplica(id.getProtobuf(), dn1);

    replicationManager.processAll();
    eventQueue.processAll(1000);

    Assert.assertTrue(cf.isDone() && cf.get() == MoveResult.COMPLETED);
  }

  /**
   * if crash happened and restarted, move option should work as expected.
   */
  @Test
  public void testMoveCrashAndRestart() throws IOException,
      NodeNotFoundException, InterruptedException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    ContainerID id = container.containerID();
    ContainerReplica dn1 = addReplica(container,
        new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container,
        new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container,
        new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    DatanodeDetails dn3 = addNode(new NodeStatus(IN_SERVICE, HEALTHY));
    replicationManager.move(id,
        new MoveDataNodePair(dn1.getDatanodeDetails(), dn3));
    Assert.assertTrue(scmLogs.getOutput().contains(
        "receive a move request about container"));
    Thread.sleep(100L);
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.replicateContainerCommand, dn3));
    Assert.assertEquals(1, datanodeCommandHandler.getInvocationCount(
        SCMCommandProto.Type.replicateContainerCommand));

    //crash happens, restart scm.
    //clear current inflight actions and reload inflightMove from DBStore.
    resetReplicationManager();
    replicationManager.getMoveScheduler()
        .reinitialize(SCMDBDefinition.MOVE.getTable(dbStore));
    Assert.assertTrue(replicationManager.getMoveScheduler()
        .getInflightMove().containsKey(id));
    MoveDataNodePair kv = replicationManager.getMoveScheduler()
        .getInflightMove().get(id);
    Assert.assertEquals(kv.getSrc(), dn1.getDatanodeDetails());
    Assert.assertEquals(kv.getTgt(), dn3);
    serviceManager.notifyStatusChanged();

    Thread.sleep(100L);
    // now, the container is not over-replicated,
    // so no deleteContainerCommand will be sent
    Assert.assertFalse(datanodeCommandHandler.received(
        SCMCommandProto.Type.deleteContainerCommand, dn1.getDatanodeDetails()));
    //replica does not exist in target datanode, so a replicateContainerCommand
    //will be sent again at notifyStatusChanged#onLeaderReadyAndOutOfSafeMode
    Assert.assertEquals(2, datanodeCommandHandler.getInvocationCount(
        SCMCommandProto.Type.replicateContainerCommand));


    //replicate container to dn3, now, over-replicated
    addReplicaToDn(container, dn3, CLOSED);
    replicationManager.processAll();
    eventQueue.processAll(1000);

    //deleteContainerCommand is sent, but the src replica is not deleted now
    Assert.assertEquals(1, datanodeCommandHandler.getInvocationCount(
        SCMCommandProto.Type.deleteContainerCommand));

    //crash happens, restart scm.
    //clear current inflight actions and reload inflightMove from DBStore.
    resetReplicationManager();
    replicationManager.getMoveScheduler()
        .reinitialize(SCMDBDefinition.MOVE.getTable(dbStore));
    Assert.assertTrue(replicationManager.getMoveScheduler()
        .getInflightMove().containsKey(id));
    kv = replicationManager.getMoveScheduler()
        .getInflightMove().get(id);
    Assert.assertEquals(kv.getSrc(), dn1.getDatanodeDetails());
    Assert.assertEquals(kv.getTgt(), dn3);
    serviceManager.notifyStatusChanged();

    //after restart and the container is over-replicated now,
    //deleteContainerCommand will be sent again
    Assert.assertEquals(2, datanodeCommandHandler.getInvocationCount(
        SCMCommandProto.Type.deleteContainerCommand));
    containerStateManager.removeContainerReplica(id.getProtobuf(), dn1);

    //replica in src datanode is deleted now
    containerStateManager.removeContainerReplica(id.getProtobuf(), dn1);
    replicationManager.processAll();
    eventQueue.processAll(1000);

    //since the move is complete,so after scm crash and restart
    //inflightMove should not contain the container again
    resetReplicationManager();
    replicationManager.getMoveScheduler()
        .reinitialize(SCMDBDefinition.MOVE.getTable(dbStore));
    Assert.assertFalse(replicationManager.getMoveScheduler()
        .getInflightMove().containsKey(id));

    //completeableFuture is not stored in DB, so after scm crash and
    //restart ,completeableFuture is missing
  }

  /**
   * make sure RM does not delete replica if placement policy is not satisfied.
   */
  @Test
  public void testMoveNotDeleteSrcIfPolicyNotSatisfied()
      throws IOException, NodeNotFoundException,
      InterruptedException, ExecutionException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    ContainerID id = container.containerID();
    ContainerReplica dn1 = addReplica(container,
        new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    ContainerReplica dn2 = addReplica(container,
        new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container,
        new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    DatanodeDetails dn4 = addNode(new NodeStatus(IN_SERVICE, HEALTHY));
    CompletableFuture<MoveResult> cf =
        replicationManager.move(id,
            new MoveDataNodePair(dn1.getDatanodeDetails(), dn4));
    Assert.assertTrue(scmLogs.getOutput().contains(
        "receive a move request about container"));
    Thread.sleep(100L);
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.replicateContainerCommand, dn4));
    Assert.assertEquals(1, datanodeCommandHandler.getInvocationCount(
        SCMCommandProto.Type.replicateContainerCommand));

    //replicate container to dn4
    addReplicaToDn(container, dn4, CLOSED);
    //now, replication succeeds, but replica in dn2 lost,
    //and there are only tree replicas totally, so rm should
    //not delete the replica on dn1
    containerStateManager.removeContainerReplica(id.getProtobuf(), dn2);
    replicationManager.processAll();
    eventQueue.processAll(1000);

    Assert.assertFalse(datanodeCommandHandler.received(
        SCMCommandProto.Type.deleteContainerCommand, dn1.getDatanodeDetails()));

    Assert.assertTrue(cf.isDone() && cf.get() == MoveResult.DELETE_FAIL_POLICY);
  }


  /**
   * test src and target datanode become unhealthy when moving.
   */
  @Test
  public void testDnBecameUnhealthyWhenMoving() throws IOException,
      NodeNotFoundException, InterruptedException, ExecutionException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    ContainerID id = container.containerID();
    ContainerReplica dn1 = addReplica(container,
        new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container,
        new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container,
        new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    DatanodeDetails dn3 = addNode(new NodeStatus(IN_SERVICE, HEALTHY));
    CompletableFuture<MoveResult> cf =
        replicationManager.move(id,
            new MoveDataNodePair(dn1.getDatanodeDetails(), dn3));
    Assert.assertTrue(scmLogs.getOutput().contains(
        "receive a move request about container"));

    nodeManager.setNodeStatus(dn3, new NodeStatus(IN_SERVICE, STALE));
    replicationManager.processAll();
    eventQueue.processAll(1000);

    Assert.assertTrue(cf.isDone() && cf.get() ==
        MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);

    nodeManager.setNodeStatus(dn3, new NodeStatus(IN_SERVICE, HEALTHY));
    cf = replicationManager.move(id,
        new MoveDataNodePair(dn1.getDatanodeDetails(), dn3));
    addReplicaToDn(container, dn3, CLOSED);
    replicationManager.processAll();
    eventQueue.processAll(1000);
    nodeManager.setNodeStatus(dn1.getDatanodeDetails(),
        new NodeStatus(IN_SERVICE, STALE));
    replicationManager.processAll();
    eventQueue.processAll(1000);

    Assert.assertTrue(cf.isDone() && cf.get() ==
        MoveResult.DELETION_FAIL_NODE_UNHEALTHY);
  }

  /**
   * before Replication Manager generates a completablefuture for a move option,
   * some Prerequisites should be satisfied.
   */
  @Test
  public void testMovePrerequisites() throws IOException, NodeNotFoundException,
      InterruptedException, ExecutionException,
      InvalidStateTransitionException {
    //all conditions is met
    final ContainerInfo container = createContainer(LifeCycleState.OPEN);
    ContainerID id = container.containerID();
    ContainerReplica dn1 = addReplica(container,
        new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    ContainerReplica dn2 = addReplica(container,
        new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    DatanodeDetails dn3 = addNode(new NodeStatus(IN_SERVICE, HEALTHY));
    ContainerReplica dn4 = addReplica(container,
        new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    
    CompletableFuture<MoveResult> cf;
    //the above move is executed successfully, so there may be some item in
    //inflightReplication or inflightDeletion. here we stop replication manager
    //to clear these states, which may impact the tests below.
    //we don't need a running replicationManamger now
    replicationManager.stop();
    Thread.sleep(100L);
    cf = replicationManager.move(id,
        new MoveDataNodePair(dn1.getDatanodeDetails(), dn3));
    Assert.assertTrue(cf.isDone() && cf.get() ==
        MoveResult.FAIL_NOT_RUNNING);
    replicationManager.start();
    Thread.sleep(100L);

    //container in not in OPEN state
    cf = replicationManager.move(id,
        new MoveDataNodePair(dn1.getDatanodeDetails(), dn3));
    Assert.assertTrue(cf.isDone() && cf.get() ==
        MoveResult.REPLICATION_FAIL_CONTAINER_NOT_CLOSED);
    //open -> closing
    containerStateManager.updateContainerState(id.getProtobuf(),
        LifeCycleEvent.FINALIZE);
    cf = replicationManager.move(id,
        new MoveDataNodePair(dn1.getDatanodeDetails(), dn3));
    Assert.assertTrue(cf.isDone() && cf.get() ==
        MoveResult.REPLICATION_FAIL_CONTAINER_NOT_CLOSED);
    //closing -> quasi_closed
    containerStateManager.updateContainerState(id.getProtobuf(),
        LifeCycleEvent.QUASI_CLOSE);
    cf = replicationManager.move(id,
        new MoveDataNodePair(dn1.getDatanodeDetails(), dn3));
    Assert.assertTrue(cf.isDone() && cf.get() ==
        MoveResult.REPLICATION_FAIL_CONTAINER_NOT_CLOSED);

    //quasi_closed -> closed
    containerStateManager.updateContainerState(id.getProtobuf(),
        LifeCycleEvent.FORCE_CLOSE);
    Assert.assertTrue(LifeCycleState.CLOSED ==
        containerStateManager.getContainer(id.getProtobuf()).getState());

    //Node is not in healthy state
    for (HddsProtos.NodeState state : HddsProtos.NodeState.values()) {
      if (state != HEALTHY) {
        nodeManager.setNodeStatus(dn3,
            new NodeStatus(IN_SERVICE, state));
        cf = replicationManager.move(id,
            new MoveDataNodePair(dn1.getDatanodeDetails(), dn3));
        Assert.assertTrue(cf.isDone() && cf.get() ==
            MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);
        cf = replicationManager.move(id,
            new MoveDataNodePair(dn3, dn1.getDatanodeDetails()));
        Assert.assertTrue(cf.isDone() && cf.get() ==
            MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);
      }
    }
    nodeManager.setNodeStatus(dn3, new NodeStatus(IN_SERVICE, HEALTHY));

    //Node is not in IN_SERVICE state
    for (HddsProtos.NodeOperationalState state :
        HddsProtos.NodeOperationalState.values()) {
      if (state != IN_SERVICE) {
        nodeManager.setNodeStatus(dn3,
            new NodeStatus(state, HEALTHY));
        cf = replicationManager.move(id,
            new MoveDataNodePair(dn1.getDatanodeDetails(), dn3));
        Assert.assertTrue(cf.isDone() && cf.get() ==
            MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE);
        cf = replicationManager.move(id,
            new MoveDataNodePair(dn3, dn1.getDatanodeDetails()));
        Assert.assertTrue(cf.isDone() && cf.get() ==
            MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE);
      }
    }
    nodeManager.setNodeStatus(dn3, new NodeStatus(IN_SERVICE, HEALTHY));

    //container exists in target datanode
    cf = replicationManager.move(id,
        new MoveDataNodePair(dn1.getDatanodeDetails(),
        dn2.getDatanodeDetails()));
    Assert.assertTrue(cf.isDone() && cf.get() ==
        MoveResult.REPLICATION_FAIL_EXIST_IN_TARGET);

    //container does not exist in source datanode
    cf = replicationManager.move(id, new MoveDataNodePair(dn3, dn3));
    Assert.assertTrue(cf.isDone() && cf.get() ==
        MoveResult.REPLICATION_FAIL_NOT_EXIST_IN_SOURCE);

    //make container over relplicated to test the
    // case that container is in inflightDeletion
    ContainerReplica dn5 = addReplica(container,
        new NodeStatus(IN_SERVICE, HEALTHY), State.CLOSED);
    replicationManager.processAll();
    //waiting for inflightDeletion generation
    eventQueue.processAll(1000);
    cf = replicationManager.move(id,
        new MoveDataNodePair(dn1.getDatanodeDetails(), dn3));
    Assert.assertTrue(cf.isDone() && cf.get() ==
        MoveResult.REPLICATION_FAIL_INFLIGHT_DELETION);
    resetReplicationManager();

    //make the replica num be 2 to test the case
    //that container is in inflightReplication
    containerStateManager.removeContainerReplica(id.getProtobuf(), dn5);
    containerStateManager.removeContainerReplica(id.getProtobuf(), dn4);
    //replication manager should generate inflightReplication
    replicationManager.processAll();
    //waiting for inflightReplication generation
    eventQueue.processAll(1000);
    cf = replicationManager.move(id,
        new MoveDataNodePair(dn1.getDatanodeDetails(), dn3));
    Assert.assertTrue(cf.isDone() && cf.get() ==
        MoveResult.REPLICATION_FAIL_INFLIGHT_REPLICATION);
  }

  @Test
  public void testReplicateCommandTimeout() throws IOException {
    long timeout = new ReplicationManagerConfiguration().getEventTimeout();

    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    assertReplicaScheduled(1);

    // Already a pending replica, so nothing scheduled
    assertReplicaScheduled(0);

    // Advance the clock past the timeout, and there should be a replica
    // scheduled
    clock.fastForward(timeout + 1000);
    assertReplicaScheduled(1);
    Assert.assertEquals(1, replicationManager.getMetrics()
        .getNumReplicationCmdsTimeout());
  }

  @Test
  public void testDeleteCommandTimeout() throws
      IOException, InterruptedException {
    long timeout = new ReplicationManagerConfiguration().getEventTimeout();

    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    assertDeleteScheduled(1);

    // Already a pending replica, so nothing scheduled
    assertReplicaScheduled(0);

    // Advance the clock past the timeout, and there should be a replica
    // scheduled
    clock.fastForward(timeout + 1000);
    assertDeleteScheduled(1);
    Assert.assertEquals(1, replicationManager.getMetrics()
        .getNumDeletionCmdsTimeout());
  }

  private ContainerInfo createContainer(LifeCycleState containerState)
      throws IOException {
    final ContainerInfo container = getContainer(containerState);
    container.setUsedBytes(1234);
    containerStateManager.addContainer(container.getProtobuf());
    return container;
  }

  private DatanodeDetails addNode(NodeStatus nodeStatus) {
    DatanodeDetails dn = randomDatanodeDetails();
    dn.setPersistedOpState(nodeStatus.getOperationalState());
    dn.setPersistedOpStateExpiryEpochSec(
        nodeStatus.getOpStateExpiryEpochSeconds());
    nodeManager.register(dn, nodeStatus);
    return dn;
  }

  private void resetReplicationManager() throws InterruptedException {
    replicationManager.stop();
    Thread.sleep(100L);
    replicationManager.start();
    Thread.sleep(100L);
  }

  private ContainerReplica addReplica(ContainerInfo container,
      NodeStatus nodeStatus, State replicaState)
      throws ContainerNotFoundException {
    DatanodeDetails dn = addNode(nodeStatus);
    return addReplicaToDn(container, dn, replicaState);
  }

  private ContainerReplica addReplicaToDn(ContainerInfo container,
                               DatanodeDetails dn, State replicaState)
      throws ContainerNotFoundException {
    // Using the same originID for all replica in the container set. If each
    // replica has a unique originID, it causes problems in ReplicationManager
    // when processing over-replicated containers.
    final UUID originNodeId =
        UUID.nameUUIDFromBytes(Longs.toByteArray(container.getContainerID()));
    final ContainerReplica replica = getReplicas(
        container.containerID(), replicaState, 1000L, originNodeId, dn);
    containerStateManager
        .updateContainerReplica(container.containerID().getProtobuf(), replica);
    return replica;
  }

  private void assertReplicaScheduled(int delta) {
    final int currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentReplicateCommandCount + delta,
        datanodeCommandHandler.getInvocationCount(
            SCMCommandProto.Type.replicateContainerCommand));
    Assert.assertEquals(currentReplicateCommandCount + delta,
        replicationManager.getMetrics().getNumReplicationCmdsSent());
  }

  private void assertDeleteScheduled(int delta) throws InterruptedException {
    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentDeleteCommandCount + delta,
        datanodeCommandHandler.getInvocationCount(
            SCMCommandProto.Type.deleteContainerCommand));
    Assert.assertEquals(currentDeleteCommandCount + delta,
        replicationManager.getMetrics().getNumDeletionCmdsSent());
  }

  private void assertUnderReplicatedCount(int count) {
    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(count, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
  }

  private void assertMissingCount(int count) {
    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(count, report.getStat(
        ReplicationManagerReport.HealthState.MISSING));
  }

  private void assertOverReplicatedCount(int count) {
    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(count, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  @After
  public void teardown() throws Exception {
    containerStateManager.close();
    replicationManager.stop();
    if (dbStore != null) {
      dbStore.close();
    }
    FileUtils.deleteDirectory(testDir);
  }

  private static class DatanodeCommandHandler implements
      EventHandler<CommandForDatanode> {

    private AtomicInteger invocation = new AtomicInteger(0);
    private Map<SCMCommandProto.Type, AtomicInteger> commandInvocation =
        new HashMap<>();
    private List<CommandForDatanode> commands = new ArrayList<>();

    @Override
    public void onMessage(final CommandForDatanode command,
                          final EventPublisher publisher) {
      final SCMCommandProto.Type type = command.getCommand().getType();
      commandInvocation.computeIfAbsent(type, k -> new AtomicInteger(0));
      commandInvocation.get(type).incrementAndGet();
      invocation.incrementAndGet();
      commands.add(command);
    }

    private int getInvocation() {
      return invocation.get();
    }

    private int getInvocationCount(SCMCommandProto.Type type) {
      return commandInvocation.containsKey(type) ?
          commandInvocation.get(type).get() : 0;
    }

    private List<CommandForDatanode> getReceivedCommands() {
      return commands;
    }

    /**
     * Returns true if the command handler has received the given
     * command type for the provided datanode.
     *
     * @param type Command Type
     * @param datanode DatanodeDetails
     * @return True if command was received, false otherwise
     */
    private boolean received(final SCMCommandProto.Type type,
                             final DatanodeDetails datanode) {
      return commands.stream().anyMatch(dc ->
          dc.getCommand().getType().equals(type) &&
              dc.getDatanodeId().equals(datanode.getUuid()));
    }
  }
}
