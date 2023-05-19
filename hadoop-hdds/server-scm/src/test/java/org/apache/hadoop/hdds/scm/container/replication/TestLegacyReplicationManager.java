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

package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.primitives.Longs;
import org.apache.commons.io.FileUtils;
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
import org.apache.hadoop.hdds.scm.container.CloseContainerEventHandler;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.container.ContainerStateManagerImpl;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.SimpleMockNodeManager;
import org.apache.hadoop.hdds.scm.container.TestContainerManagerImpl;
import org.apache.hadoop.hdds.scm.container.balancer.MoveManager;
import org.apache.hadoop.hdds.scm.container.replication.LegacyReplicationManager.LegacyReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
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
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.QUASI_CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.UNHEALTHY;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.CONTAINER_NUM_KEYS_DEFAULT;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.CONTAINER_USED_BYTES_DEFAULT;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getContainer;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getReplicas;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.mockito.Mockito.when;

/**
 * Test cases to verify the functionality of ReplicationManager.
 */
public class TestLegacyReplicationManager {

  private ReplicationManager replicationManager;
  private ContainerStateManager containerStateManager;
  private PlacementPolicy ratisContainerPlacementPolicy;
  private PlacementPolicy ecContainerPlacementPolicy;
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
  private ContainerReplicaPendingOps containerReplicaPendingOps;

  int getInflightCount(InflightType type) {
    return replicationManager.getLegacyReplicationManager()
        .getInflightCount(type);
  }

  @BeforeEach
  public void setup()
      throws IOException, InterruptedException,
      NodeNotFoundException, InvalidStateTransitionException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        0, TimeUnit.SECONDS);

    scmLogs = GenericTestUtils.LogCapturer.
        captureLogs(LegacyReplicationManager.LOG);
    containerManager = Mockito.mock(ContainerManager.class);
    nodeManager = new SimpleMockNodeManager();
    eventQueue = new EventQueue();
    scmhaManager = SCMHAManagerStub.getInstance(true);
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
        .setContainerReplicaPendingOps(new ContainerReplicaPendingOps(
            conf, Clock.system(ZoneId.systemDefault())))
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
                id));
          }
          return containers;
        });

    Mockito.when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainer(((ContainerID)invocation
                .getArguments()[0])));

    Mockito.when(containerManager.getContainerReplicas(
        Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainerReplicas(((ContainerID)invocation
                .getArguments()[0])));

    ratisContainerPlacementPolicy = Mockito.mock(PlacementPolicy.class);
    ecContainerPlacementPolicy = Mockito.mock(PlacementPolicy.class);

    Mockito.when(ratisContainerPlacementPolicy.chooseDatanodes(
        Mockito.any(), Mockito.any(), Mockito.anyInt(),
            Mockito.anyLong(), Mockito.anyLong()))
        .thenAnswer(invocation -> {
          int count = (int) invocation.getArguments()[2];
          return IntStream.range(0, count)
              .mapToObj(i -> randomDatanodeDetails())
              .collect(Collectors.toList());
        });

    Mockito.when(ratisContainerPlacementPolicy.validateContainerPlacement(
        Mockito.any(),
        Mockito.anyInt()
        )).thenAnswer(invocation ->
        new ContainerPlacementStatusDefault(2, 2, 3));
    clock = new TestClock(Instant.now(), ZoneId.of("UTC"));
    containerReplicaPendingOps = new ContainerReplicaPendingOps(conf, clock);
    createReplicationManager(newRMConfig());
  }

  void createReplicationManager(int replicationLimit, int deletionLimit)
      throws Exception {
    replicationManager.stop();
    dbStore.close();
    final LegacyReplicationManagerConfiguration conf
        = new LegacyReplicationManagerConfiguration();
    conf.setContainerInflightReplicationLimit(replicationLimit);
    conf.setContainerInflightDeletionLimit(deletionLimit);
    createReplicationManager(conf);
  }

  void createReplicationManager(
      LegacyReplicationManagerConfiguration conf)
      throws Exception {
    createReplicationManager(newRMConfig(), conf);
  }

  private void createReplicationManager(ReplicationManagerConfiguration rmConf)
      throws InterruptedException, IOException {
    createReplicationManager(rmConf, null);
  }

  void createReplicationManager(ReplicationManagerConfiguration rmConf,
      LegacyReplicationManagerConfiguration lrmConf)
      throws InterruptedException, IOException {
    OzoneConfiguration config = new OzoneConfiguration();
    testDir = GenericTestUtils
      .getTestDir(TestContainerManagerImpl.class.getSimpleName());
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        testDir.getAbsolutePath());
    config.setTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        0, TimeUnit.SECONDS);
    Optional.ofNullable(rmConf).ifPresent(config::setFromObject);
    Optional.ofNullable(lrmConf).ifPresent(config::setFromObject);

    SCMHAManager scmHAManager = SCMHAManagerStub
        .getInstance(true, new SCMDBTransactionBufferImpl());
    dbStore = DBStoreBuilder.createDBStore(
      config, new SCMDBDefinition());

    LegacyReplicationManager legacyRM = new LegacyReplicationManager(
        config, containerManager, ratisContainerPlacementPolicy, eventQueue,
        SCMContext.emptyContext(), nodeManager, scmHAManager, clock,
        SCMDBDefinition.MOVE.getTable(dbStore));

    replicationManager = new ReplicationManager(
        config,
        containerManager,
        ratisContainerPlacementPolicy,
        ecContainerPlacementPolicy,
        eventQueue,
        SCMContext.emptyContext(),
        nodeManager,
        clock,
        legacyRM,
        containerReplicaPendingOps);

    serviceManager.register(replicationManager);
    serviceManager.notifyStatusChanged();
    scmLogs.clearOutput();
    Thread.sleep(100L);
  }

  @AfterEach
  public void teardown() throws Exception {
    containerStateManager.close();
    replicationManager.stop();
    if (dbStore != null) {
      dbStore.close();
    }
    FileUtils.deleteDirectory(testDir);
  }

  @Nested
  class Misc {
    /**
     * Checks if restarting of replication manager works.
     */
    @Test
    public void testReplicationManagerRestart() throws InterruptedException {
      Assertions.assertTrue(replicationManager.isRunning());
      replicationManager.stop();
      // Stop is a non-blocking call, it might take sometime for the
      // ReplicationManager to shutdown
      Thread.sleep(500);
      Assertions.assertFalse(replicationManager.isRunning());
      replicationManager.start();
      Assertions.assertTrue(replicationManager.isRunning());
    }
  }

  /**
   * Tests replication manager with healthy open and closed containers. No
   * quasi closed or unhealthy containers are involved.
   */
  @Nested
  class StableReplicas {
    /**
     * Open containers are not handled by ReplicationManager.
     * This test-case makes sure that ReplicationManages doesn't take
     * any action on OPEN containers.
     */
    @Test
    public void testOpenContainer() throws IOException, TimeoutException {
      final ContainerInfo container = getContainer(LifeCycleState.OPEN);
      containerStateManager.addContainer(container.getProtobuf());
      replicationManager.processAll();
      eventQueue.processAll(1000);
      ReplicationManagerReport report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.OPEN));
      Assertions.assertEquals(0, datanodeCommandHandler.getInvocation());
    }

    /**
     * 1 open replica
     * 2 closing replicas
     * Expectation: Close command is sent to the open replicas.
     */
    @Test
    public void testClosingContainer() throws IOException, TimeoutException {
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
        containerStateManager.updateContainerReplica(id, replica);
      }

      final int currentCloseCommandCount = datanodeCommandHandler
              .getInvocationCount(SCMCommandProto.Type.closeContainerCommand);

      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(currentCloseCommandCount + 3,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.closeContainerCommand));

      // Update the OPEN to CLOSING
      for (ContainerReplica replica: getReplicas(id, State.CLOSING, datanode)) {
        containerStateManager.updateContainerReplica(id, replica);
      }

      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(currentCloseCommandCount + 6,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.closeContainerCommand));
      ReplicationManagerReport report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.CLOSING));
    }

    /**
     * Create closing container with 1 replica.
     * Expectation: Missing containers 0.
     * Remove the only replica.
     * Expectation: Missing containers 1.
     */
    @Test
    public void testClosingMissingContainer()
            throws IOException, TimeoutException {
      final ContainerInfo container = getContainer(LifeCycleState.CLOSING);
      final ContainerID id = container.containerID();

      containerStateManager.addContainer(container.getProtobuf());

      // One replica in OPEN state
      final Set<ContainerReplica> replicas = getReplicas(id, State.OPEN,
              randomDatanodeDetails());

      for (ContainerReplica replica : replicas) {
        containerStateManager.updateContainerReplica(id, replica);
      }

      final int currentCloseCommandCount = datanodeCommandHandler
              .getInvocationCount(SCMCommandProto.Type.closeContainerCommand);

      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(currentCloseCommandCount + 1,
              datanodeCommandHandler.getInvocationCount(
                      SCMCommandProto.Type.closeContainerCommand));

      ReplicationManagerReport report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.CLOSING));
      Assertions.assertEquals(0, report.getStat(
              ReplicationManagerReport.HealthState.MISSING));

      for (ContainerReplica replica : replicas) {
        containerStateManager.removeContainerReplica(id, replica);
      }

      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(currentCloseCommandCount + 1,
              datanodeCommandHandler.getInvocationCount(
                      SCMCommandProto.Type.closeContainerCommand));

      report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.CLOSING));
      Assertions.assertEquals(1, report.getStat(
              ReplicationManagerReport.HealthState.MISSING));
      Assertions.assertEquals(1, report.getStat(
              ReplicationManagerReport.HealthState.UNDER_REPLICATED));
      Assertions.assertEquals(1, report.getStat(
              ReplicationManagerReport.HealthState.MIS_REPLICATED));
    }

    @Test
    public void testReplicateCommandTimeout()
            throws IOException, TimeoutException {
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
      Assertions.assertEquals(1, replicationManager.getMetrics()
              .getNumReplicationCmdsTimeout());
    }

    @Test
    public void testDeleteCommandTimeout()
            throws IOException, TimeoutException {
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
      Assertions.assertEquals(1, replicationManager.getMetrics()
              .getNumDeletionCmdsTimeout());
    }

    /**
     * A closed empty container with all the replicas also closed and empty
     * should be deleted.
     * A container/ replica should be deemed empty when it has 0 keyCount even
     * if the usedBytes is not 0 (usedBytes should not be used to determine if
     * the container or replica is empty).
     */
    @Test
    public void testDeleteEmptyContainer() throws Exception {
      runTestDeleteEmptyContainer(3);
    }

    Void runTestDeleteEmptyContainer(int expectedDelete) throws Exception {
      // Create container with usedBytes = 1000 and keyCount = 0
      final ContainerInfo container = createContainer(
          LifeCycleState.CLOSED, 1000, 0);
      addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
      addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
      // Create a replica with usedBytes != 0 and keyCount = 0
      addReplica(container,
          new NodeStatus(IN_SERVICE, HEALTHY), CLOSED, 100, 0);

      assertDeleteScheduled(expectedDelete);
      return null;
    }

    @Test
    public void testDeletionLimit() throws Exception {
      runTestLimit(0, 2, 0, 1,
              () -> runTestDeleteEmptyContainer(2));
    }

    /**
     * A closed empty container with a non-empty replica should not be deleted.
     */
    @Test
    public void testDeleteEmptyContainerNonEmptyReplica() throws Exception {
      final ContainerInfo container = createContainer(LifeCycleState.CLOSED, 0,
              0);
      addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
      addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
      // Create the 3rd replica with non-zero key count and used bytes
      addReplica(container,
          new NodeStatus(IN_SERVICE, HEALTHY), CLOSED, 100, 1);
      assertDeleteScheduled(0);
    }

    /**
     * ReplicationManager should replicate zero replica when all copies
     * are missing.
     */
    @Test
    public void testContainerWithMissingReplicas()
            throws IOException, TimeoutException {
      createContainer(LifeCycleState.CLOSED);
      assertReplicaScheduled(0);
      assertUnderReplicatedCount(1);
      assertMissingCount(1);
    }

    /**
     * 3 healthy closed replicas.
     * Expectation: No action.
     *
     * ReplicationManager should not take any action if the container is
     * CLOSED and healthy.
     */
    @Test
    public void testHealthyClosedContainer()
            throws IOException, TimeoutException {
      final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
      final ContainerID id = container.containerID();
      final Set<ContainerReplica> replicas = getReplicas(id, State.CLOSED,
              randomDatanodeDetails(),
              randomDatanodeDetails(),
              randomDatanodeDetails());

      containerStateManager.addContainer(container.getProtobuf());
      for (ContainerReplica replica : replicas) {
        containerStateManager.updateContainerReplica(id, replica);
      }

      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(0, datanodeCommandHandler.getInvocation());

      ReplicationManagerReport report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.CLOSED));
      for (ReplicationManagerReport.HealthState s :
              ReplicationManagerReport.HealthState.values()) {
        Assertions.assertEquals(0, report.getStat(s));
      }
    }
  }

  /**
   * Tests replication manager with unhealthy and quasi-closed container
   * replicas.
   */
  @Nested
  class UnstableReplicas {
    /**
     * 2 open replicas
     * 1 quasi-closed replica
     * Expectation: close command is sent to the open replicas.
     */
    @Test
    public void testQuasiClosedContainerWithTwoOpenReplica()
        throws IOException, TimeoutException {
      final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
      final ContainerID id = container.containerID();
      final UUID originNodeId = UUID.randomUUID();
      final ContainerReplica replicaOne = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
      final ContainerReplica replicaTwo = getReplicas(
          id, State.OPEN, 1000L, originNodeId, randomDatanodeDetails());
      final DatanodeDetails datanodeDetails = randomDatanodeDetails();
      final ContainerReplica replicaThree = getReplicas(
          id, State.OPEN, 1000L, datanodeDetails.getUuid(), datanodeDetails);

      containerStateManager.addContainer(container.getProtobuf());
      containerStateManager.updateContainerReplica(id, replicaOne);
      containerStateManager.updateContainerReplica(id, replicaTwo);
      containerStateManager.updateContainerReplica(
          id, replicaThree);

      // First iteration

      final int currentCloseCommandCount = datanodeCommandHandler
          .getInvocationCount(SCMCommandProto.Type.closeContainerCommand);
      // Two of the replicas are in OPEN state
      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(currentCloseCommandCount + 2,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.closeContainerCommand));
      Assertions.assertTrue(datanodeCommandHandler.received(
          SCMCommandProto.Type.closeContainerCommand,
          replicaTwo.getDatanodeDetails()));
      Assertions.assertTrue(datanodeCommandHandler.received(
          SCMCommandProto.Type.closeContainerCommand,
          replicaThree.getDatanodeDetails()));
      ReplicationManagerReport report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
    }

    /**
     * 3 quasi closed replicas with the same origin node ID.
     * Expectation: No action taken.
     *
     * When the container is in QUASI_CLOSED state and all the replicas are
     * also in QUASI_CLOSED state and doesn't have a quorum to force close
     * the container, ReplicationManager will not do anything.
     */
    @Test
    public void testHealthyQuasiClosedContainer()
        throws IOException, TimeoutException {
      final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
      final ContainerID id = container.containerID();
      final UUID originNodeId = UUID.randomUUID();
      final ContainerReplica replicaOne = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
      final ContainerReplica replicaTwo = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
      final ContainerReplica replicaThree = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

      containerStateManager.addContainer(container.getProtobuf());
      containerStateManager.updateContainerReplica(id, replicaOne);
      containerStateManager.updateContainerReplica(id, replicaTwo);
      containerStateManager.updateContainerReplica(
          id, replicaThree);

      // All the QUASI_CLOSED replicas have same originNodeId, so the
      // container will not be closed. ReplicationManager should take no action.
      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(0, datanodeCommandHandler.getInvocation());
      ReplicationManagerReport report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
    }

    /**
     * 2 quasi-closed replicas.
     * 1 unhealthy replica.
     * All replicas have same origin node ID.
     * Expectation:
     *    Round 1: Quasi closed replica is replicated.
     *    Round 2: Unhealthy replica is deleted.
     *
     * When a container is QUASI_CLOSED and we don't have quorum to force close
     * the container, the container should have all the replicas in QUASI_CLOSED
     * state, else ReplicationManager will take action.
     *
     * In this test case we make one of the replica unhealthy, replication
     * manager will send delete container command to the datanode which has the
     * unhealthy replica.
     */
    @Test
    public void testQuasiClosedContainerWithUnhealthyReplica()
        throws IOException, TimeoutException {
      final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
      container.setUsedBytes(100);
      final ContainerID id = container.containerID();
      final UUID originNodeId = UUID.randomUUID();
      final ContainerReplica replicaOne = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
      final ContainerReplica replicaTwo = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
      final ContainerReplica replicaThree = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

      containerStateManager.addContainer(container.getProtobuf());
      containerStateManager.updateContainerReplica(id, replicaOne);
      containerStateManager.updateContainerReplica(id, replicaTwo);
      containerStateManager.updateContainerReplica(id, replicaThree);

      int currentReplicateCommandCount = datanodeCommandHandler
          .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);

      // All the QUASI_CLOSED replicas have same originNodeId, so the
      // container will not be closed. ReplicationManager should take no action.
      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(0, datanodeCommandHandler.getInvocation());

      // Make the first replica unhealthy
      final ContainerReplica unhealthyReplica = getReplicas(
          id, UNHEALTHY, 1000L, originNodeId,
          replicaOne.getDatanodeDetails());
      containerStateManager.updateContainerReplica(
          id, unhealthyReplica);

      long currentBytesToReplicate = replicationManager.getMetrics()
          .getNumReplicationBytesTotal();
      replicationManager.processAll();
      eventQueue.processAll(1000);
      // Under replication handler should first re-replicate one of the quasi
      // closed containers.
      // The unhealthy container should not have been deleted in the first pass.
      assertDeleteScheduled(0);
      currentReplicateCommandCount += 1;
      currentBytesToReplicate += 100L;
      Assertions.assertEquals(currentReplicateCommandCount,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.replicateContainerCommand));
      Assertions.assertEquals(currentReplicateCommandCount,
          replicationManager.getMetrics().getNumReplicationCmdsSent());
      Assertions.assertEquals(currentBytesToReplicate,
          replicationManager.getMetrics().getNumReplicationBytesTotal());
      Assertions.assertEquals(1, getInflightCount(InflightType.REPLICATION));
      Assertions.assertEquals(1, replicationManager.getMetrics()
          .getInflightReplication());

      // The quasi closed container cannot be closed, but it should have been
      // restored to full replication on the previous run.
      // The unhealthy replica should remain until the next iteration.
      ReplicationManagerReport report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
      Assertions.assertEquals(0, report.getStat(
          ReplicationManagerReport.HealthState.UNDER_REPLICATED));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.UNHEALTHY));

      // Create the replica so replication manager sees it on the next run.
      List<CommandForDatanode> replicateCommands = datanodeCommandHandler
          .getReceivedCommands().stream()
          .filter(c -> c.getCommand().getType()
              .equals(SCMCommandProto.Type.replicateContainerCommand))
          .collect(Collectors.toList());
      for (CommandForDatanode replicateCommand: replicateCommands) {
        DatanodeDetails newNode = createDatanodeDetails(
            replicateCommand.getDatanodeId());
        ContainerReplica newReplica = getReplicas(
            id, QUASI_CLOSED, 1000L, originNodeId, newNode);
        containerStateManager.updateContainerReplica(id, newReplica);
      }

      // On the next run, the unhealthy container should be scheduled for
      // deletion, since the quasi closed container is now sufficiently
      // replicated.
      // This method runs an iteration of replication manager.
      assertDeleteScheduled(1);
      assertExactDeleteTargets(unhealthyReplica.getDatanodeDetails());
      // Replication should have finished on the previous iteration, leaving
      // these numbers unchanged.
      Assertions.assertEquals(currentReplicateCommandCount,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.replicateContainerCommand));
      Assertions.assertEquals(currentReplicateCommandCount,
          replicationManager.getMetrics().getNumReplicationCmdsSent());
      Assertions.assertEquals(currentBytesToReplicate,
          replicationManager.getMetrics().getNumReplicationBytesTotal());
      Assertions.assertEquals(0, getInflightCount(InflightType.REPLICATION));
      Assertions.assertEquals(0, replicationManager.getMetrics()
          .getInflightReplication());

      // Now we will delete the unhealthy replica.
      containerStateManager.removeContainerReplica(id, unhealthyReplica);

      // There should be no work left on the following runs.
      replicationManager.processAll();
      eventQueue.processAll(1000);
      // The two commands shown are the previous delete and replicate commands.
      Assertions.assertEquals(2, datanodeCommandHandler.getInvocation());
    }


    /**
     * Container is quasi closed.
     * 3 quasi-closed replicas with the same origin node ID.
     * 1 unhealthy replica with unique origin node ID.
     *
     * Expectation:
     * No action taken. 3 healthy replicas are present. The unhealthy replica
     * should not be deleted since it has a unique origin node ID. The
     * container cannot be closed because there are not enough healthy unique
     * origin node IDs.
     */
    @Test
    public void testQuasiClosedContainerWithUniqueUnhealthyReplica()
        throws IOException, TimeoutException {
      final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
      container.setUsedBytes(100);
      final ContainerID id = container.containerID();
      final UUID originNodeId = UUID.randomUUID();
      final ContainerReplica replicaOne = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
      final ContainerReplica replicaTwo = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
      final ContainerReplica replicaThree = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
      final ContainerReplica replica4 = getReplicas(
          id, UNHEALTHY, 1000L, randomDatanodeDetails().getUuid(),
          randomDatanodeDetails());

      containerStateManager.addContainer(container.getProtobuf());
      containerStateManager.updateContainerReplica(id, replicaOne);
      containerStateManager.updateContainerReplica(id, replicaTwo);
      containerStateManager.updateContainerReplica(id, replicaThree);
      containerStateManager.updateContainerReplica(id, replica4);

      replicationManager.processAll();
      eventQueue.processAll(1000);

      Assertions.assertEquals(0,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.replicateContainerCommand));

      Assertions.assertEquals(0,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.deleteContainerCommand));
    }

    /**
     * Container is closed.
     * 2 quasi-closed replicas.
     * 1 unhealthy replica.
     * All replicas have unique origin node IDs.
     * Quasi closed replicas BCS IDs match closed container's BCS ID.
     *
     * Expectation:
     * Iteration 1: Quasi closed replicas are closed since their BCS IDs
     * match the closed container state.
     * Iteration 2: The now closed replicas are replicated.
     * Iteration 3: The unhealthy replica is deleted.
     */
    @Test
    public void testCloseableContainerWithUniqueUnhealthyReplica()
        throws Exception {
      final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
      ContainerReplica quasi1 = addReplicaToDn(container,
          randomDatanodeDetails(), QUASI_CLOSED, container.getSequenceId());
      ContainerReplica quasi2 = addReplicaToDn(container,
          randomDatanodeDetails(), QUASI_CLOSED, container.getSequenceId());
      ContainerReplica unhealthyReplica = addReplicaToDn(container,
          randomDatanodeDetails(),
          UNHEALTHY,
          900L);

      // First RM iteration.
      // The quasi containers should be closed since their BCSIDs match the
      // closed container's state.
      assertDeleteScheduled(0);
      // All the containers are unhealthy, so it will not be counted as under
      // replicated.
      assertUnderReplicatedCount(0);
      Assertions.assertEquals(2,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.closeContainerCommand));

      // Update RM with the results of the close commands.
      ContainerReplica closedRep1 = getReplicas(
          container.containerID(), CLOSED,
          container.getSequenceId(), quasi1.getDatanodeDetails())
          .stream().findFirst().get();
      ContainerReplica closedRep2 = getReplicas(
          container.containerID(), CLOSED,
          container.getSequenceId(), quasi2.getDatanodeDetails())
          .stream().findFirst().get();

      containerStateManager.updateContainerReplica(container.containerID(),
          closedRep1);
      containerStateManager.updateContainerReplica(container.containerID(),
          closedRep2);

      // Second RM iteration
      // Now that we have healthy replicas, they should be replicated.
      assertDeleteScheduled(0);
      assertUnderReplicatedCount(1);
      Assertions.assertEquals(1,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.replicateContainerCommand));


      // Process the replicate command and report the replica back to SCM.
      List<CommandForDatanode> replicateCommands = datanodeCommandHandler
          .getReceivedCommands().stream()
          .filter(c -> c.getCommand().getType()
              .equals(SCMCommandProto.Type.replicateContainerCommand))
          .collect(Collectors.toList());

      // Report the new replica to SCM.
      for (CommandForDatanode replicateCommand: replicateCommands) {
        DatanodeDetails newNode = createDatanodeDetails(
            replicateCommand.getDatanodeId());
        ContainerReplica newReplica = getReplicas(
            container.containerID(), CLOSED,
            container.getSequenceId(), newNode.getUuid(), newNode);
        containerStateManager.updateContainerReplica(container.containerID(),
            newReplica);
      }

      // Third RM iteration
      // The unhealthy replica can be deleted since we have 3 healthy copies
      // of a closed container.
      assertDeleteScheduled(1);
      assertUnderReplicatedCount(0);
      Assertions.assertEquals(1,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.deleteContainerCommand));
      assertExactDeleteTargets(unhealthyReplica.getDatanodeDetails());
    }


    /**
     * $numReplicas unhealthy replicas.
     * Expectation: The remaining replicas are scheduled.
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testUnderReplicatedWithOnlyUnhealthyReplicas(int numReplicas)
        throws Exception {
      final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
      for (int i = 0; i < numReplicas; i++) {
        addReplica(container, NodeStatus.inServiceHealthy(), UNHEALTHY);
      }
      int numReplicasNeeded = HddsProtos.ReplicationFactor.THREE_VALUE -
          numReplicas;
      assertReplicaScheduled(numReplicasNeeded);
      assertUnderReplicatedCount(1);
    }

    /**
     * 1 unhealthy replica.
     * 4 closed replicas.
     * Expectation:
     * Iteration 1: The unhealthy replica should be deleted.
     * Iteration 2: One of the closed replicas should be deleted.
     */
    @Test
    public void testOverReplicatedClosedAndUnhealthy() throws Exception {
      final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
      ContainerReplica unhealthy = addReplica(container,
          NodeStatus.inServiceHealthy(), UNHEALTHY);
      addReplica(container, NodeStatus.inServiceHealthy(), CLOSED);
      addReplica(container, NodeStatus.inServiceHealthy(), CLOSED);
      addReplica(container, NodeStatus.inServiceHealthy(), CLOSED);
      addReplica(container, NodeStatus.inServiceHealthy(), CLOSED);

      // This method does one run of replication manager.
      assertReplicaScheduled(0);
      assertUnderReplicatedCount(0);
      boolean unhealthyDeleted = false;
      boolean closedDeleted = false;
      UUID closedDeletedUUID = null;

      for (CommandForDatanode<?> command :
          datanodeCommandHandler.getReceivedCommands()) {
        if (command.getCommand().getType() ==
            SCMCommandProto.Type.deleteContainerCommand) {
          if (command.getDatanodeId() ==
              unhealthy.getDatanodeDetails().getUuid()) {
            unhealthyDeleted = true;
          } else {
            closedDeleted = true;
            closedDeletedUUID = command.getDatanodeId();
          }
        }
      }

      Assertions.assertFalse(unhealthyDeleted);
      Assertions.assertTrue(closedDeleted);

      // Do a second run.
      assertReplicaScheduled(0);
      assertUnderReplicatedCount(0);
      unhealthyDeleted = false;
      closedDeleted = false;
      for (CommandForDatanode<?> command :
          datanodeCommandHandler.getReceivedCommands()) {
        if (command.getCommand().getType() ==
            SCMCommandProto.Type.deleteContainerCommand) {
          if (command.getDatanodeId() ==
              unhealthy.getDatanodeDetails().getUuid()) {
            unhealthyDeleted = true;
          } else {
            closedDeleted = true;
            // The delete command should have been left over from the last run.
            Assertions.assertEquals(closedDeletedUUID, command.getDatanodeId());
          }
        }
      }

      Assertions.assertTrue(unhealthyDeleted);
      Assertions.assertTrue(closedDeleted);
    }

    /**
     * 4 unhealthy replicas.
     * Expectation: One unhealthy replica should be deleted.
     */
    @Test
    public void testOverReplicatedUnhealthy() throws Exception {
      final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
      Set<UUID> unhealthyContainerDNIDs = new HashSet<>();

      final int numReplicas = 4;
      for (int i = 0; i < numReplicas; i++) {
        ContainerReplica replica = addReplica(container,
            NodeStatus.inServiceHealthy(), UNHEALTHY);
        unhealthyContainerDNIDs.add(replica.getDatanodeDetails().getUuid());
      }

      // No replications should be scheduled.
      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(0,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.replicateContainerCommand));
      assertUnderReplicatedCount(0);

      // One replica should be deleted.
      Assertions.assertEquals(1,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.deleteContainerCommand));
      Assertions.assertTrue(
          datanodeCommandHandler.getReceivedCommands().stream()
              .anyMatch(c -> c.getCommand().getType() ==
                  SCMCommandProto.Type.deleteContainerCommand &&
                  unhealthyContainerDNIDs.contains(c.getDatanodeId())));
    }

    /**
     * 4 quasi-closed replicas.
     * All have same origin node ID.
     * Expectation: One of the replicas is deleted.
     */
    @Test
    public void testOverReplicatedQuasiClosedContainer()
        throws IOException, TimeoutException {
      final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
      container.setUsedBytes(101);
      final ContainerID id = container.containerID();
      final UUID originNodeId = UUID.randomUUID();
      final ContainerReplica replicaOne = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
      final ContainerReplica replicaTwo = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
      final ContainerReplica replicaThree = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
      final ContainerReplica replicaFour = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

      containerStateManager.addContainer(container.getProtobuf());
      containerStateManager.updateContainerReplica(id, replicaOne);
      containerStateManager.updateContainerReplica(id, replicaTwo);
      containerStateManager.updateContainerReplica(
          id, replicaThree);
      containerStateManager.updateContainerReplica(id, replicaFour);

      final int currentDeleteCommandCount = datanodeCommandHandler
          .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(currentDeleteCommandCount + 1,
          datanodeCommandHandler
              .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
      Assertions.assertEquals(currentDeleteCommandCount + 1,
          replicationManager.getMetrics().getNumDeletionCmdsSent());
      Assertions.assertEquals(1, getInflightCount(InflightType.DELETION));
      Assertions.assertEquals(1, replicationManager.getMetrics()
          .getInflightDeletion());

      ReplicationManagerReport report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.OVER_REPLICATED));

      // Now we remove the replica according to inflight
      DatanodeDetails targetDn =
          replicationManager.getLegacyReplicationManager()
              .getFirstDatanode(InflightType.DELETION, id);
      if (targetDn.equals(replicaOne.getDatanodeDetails())) {
        containerStateManager.removeContainerReplica(
            id, replicaOne);
      } else if (targetDn.equals(replicaTwo.getDatanodeDetails())) {
        containerStateManager.removeContainerReplica(
            id, replicaTwo);
      } else if (targetDn.equals(replicaThree.getDatanodeDetails())) {
        containerStateManager.removeContainerReplica(
            id, replicaThree);
      } else if (targetDn.equals(replicaFour.getDatanodeDetails())) {
        containerStateManager.removeContainerReplica(
            id, replicaFour);
      }

      final long currentDeleteCommandCompleted = replicationManager.getMetrics()
          .getNumDeletionCmdsCompleted();
      final long deleteBytesCompleted =
          replicationManager.getMetrics().getNumDeletionBytesCompleted();

      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(0, getInflightCount(InflightType.DELETION));
      Assertions.assertEquals(0, replicationManager.getMetrics()
          .getInflightDeletion());
      Assertions.assertEquals(currentDeleteCommandCompleted + 1,
          replicationManager.getMetrics().getNumDeletionCmdsCompleted());
      Assertions.assertEquals(deleteBytesCompleted + 101,
          replicationManager.getMetrics().getNumDeletionBytesCompleted());

      report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
      Assertions.assertEquals(0, report.getStat(
          ReplicationManagerReport.HealthState.OVER_REPLICATED));
    }

    /**
     * 2 open replicas.
     * 1 unhealthy replica.
     * Expectation: Container is closed.
     *
     * ReplicationManager should close the unhealthy OPEN container.
     */
    @Test
    public void testUnhealthyOpenContainer()
        throws IOException, TimeoutException {
      final ContainerInfo container = getContainer(LifeCycleState.OPEN);
      final ContainerID id = container.containerID();
      final Set<ContainerReplica> replicas = getReplicas(id, State.OPEN,
          randomDatanodeDetails(),
          randomDatanodeDetails());
      replicas.addAll(getReplicas(id, UNHEALTHY, randomDatanodeDetails()));

      containerStateManager.addContainer(container.getProtobuf());
      for (ContainerReplica replica : replicas) {
        containerStateManager.updateContainerReplica(id, replica);
      }

      final CloseContainerEventHandler closeContainerHandler =
          Mockito.mock(CloseContainerEventHandler.class);
      eventQueue.addHandler(SCMEvents.CLOSE_CONTAINER, closeContainerHandler);

      replicationManager.processAll();
      eventQueue.processAll(1000);
      Mockito.verify(closeContainerHandler, Mockito.times(1))
          .onMessage(id, eventQueue);

      ReplicationManagerReport report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.OPEN));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.OPEN_UNHEALTHY));
    }

    /**
     * 1 unhealthy replica.
     * 2 open replicas.
     * Expectation: Close command should be sent to open replicas only.
     *
     * ReplicationManager should skip send close command to unhealthy replica.
     */
    @Test
    public void testCloseUnhealthyReplica()
        throws IOException, TimeoutException {
      final ContainerInfo container = getContainer(LifeCycleState.CLOSING);
      final ContainerID id = container.containerID();
      final Set<ContainerReplica> replicas = getReplicas(id, UNHEALTHY,
          randomDatanodeDetails());
      replicas.addAll(getReplicas(id, State.OPEN, randomDatanodeDetails()));
      replicas.addAll(getReplicas(id, State.OPEN, randomDatanodeDetails()));

      containerStateManager.addContainer(container.getProtobuf());
      for (ContainerReplica replica : replicas) {
        containerStateManager.updateContainerReplica(id, replica);
      }

      replicationManager.processAll();
      // Wait for EventQueue to call the event handler
      eventQueue.processAll(1000);
      Assertions.assertEquals(2,
          datanodeCommandHandler.getInvocation());
    }

    /**
     * 1 unhealthy replica.
     * 3 quasi closed replicas.
     * All have same origin node ID.
     * Expectation: Unhealthy replica is deleted.
     *
     * When a QUASI_CLOSED container is over replicated, ReplicationManager
     * deletes the excess replicas. While choosing the replica for deletion
     * ReplicationManager should prioritize deleting the unhealthy replica over
     * QUASI_CLOSED replica if the unhealthy replica does not have a unique
     * origin node ID.
     */
    @Test
    public void testQuasiClosedContainerWithExtraUnhealthyReplica()
        throws IOException, TimeoutException {
      final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
      final ContainerID id = container.containerID();
      final UUID originNodeId = UUID.randomUUID();
      final ContainerReplica unhealthyReplica = getReplicas(
          id, UNHEALTHY, 1000L, originNodeId, randomDatanodeDetails());
      final ContainerReplica replicaTwo = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
      final ContainerReplica replicaThree = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
      final ContainerReplica replicaFour = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

      containerStateManager.addContainer(container.getProtobuf());
      containerStateManager.updateContainerReplica(id, unhealthyReplica);
      containerStateManager.updateContainerReplica(id, replicaTwo);
      containerStateManager.updateContainerReplica(id, replicaThree);
      containerStateManager.updateContainerReplica(id, replicaFour);

      assertDeleteScheduled(1);
      Assertions.assertTrue(
          datanodeCommandHandler.getReceivedCommands().stream()
              .anyMatch(c -> c.getCommand().getType() ==
                  SCMCommandProto.Type.deleteContainerCommand &&
                  c.getDatanodeId().equals(
                      unhealthyReplica.getDatanodeDetails().getUuid())));

      ReplicationManagerReport report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
      // Container should have been considered over replicated including the
      // unhealthy replica.
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.OVER_REPLICATED));

      final long currentDeleteCommandCompleted = replicationManager.getMetrics()
          .getNumDeletionCmdsCompleted();
      // Now we remove the replica to simulate deletion complete
      containerStateManager.removeContainerReplica(id, unhealthyReplica);

      // On the next run, the over replicated status should be reconciled and
      // the delete completed.
      replicationManager.processAll();
      eventQueue.processAll(1000);

      Assertions.assertEquals(currentDeleteCommandCompleted + 1,
          replicationManager.getMetrics().getNumDeletionCmdsCompleted());
      Assertions.assertEquals(0, getInflightCount(InflightType.DELETION));
      Assertions.assertEquals(0, replicationManager.getMetrics()
          .getInflightDeletion());

      report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
      Assertions.assertEquals(0, report.getStat(
          ReplicationManagerReport.HealthState.OVER_REPLICATED));
    }

    /**
     * 2 quasi-closed replicas.
     * Expectation: Replicate one of the replicas.
     */
    @Test
    public void testUnderReplicatedQuasiClosedContainer()
        throws IOException, TimeoutException {
      final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
      container.setUsedBytes(100);
      final ContainerID id = container.containerID();
      final UUID originNodeId = UUID.randomUUID();
      final ContainerReplica replicaOne = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
      final ContainerReplica replicaTwo = getReplicas(
          id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

      containerStateManager.addContainer(container.getProtobuf());
      containerStateManager.updateContainerReplica(id, replicaOne);
      containerStateManager.updateContainerReplica(id, replicaTwo);

      final int currentReplicateCommandCount = datanodeCommandHandler
          .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);
      final long currentBytesToReplicate = replicationManager.getMetrics()
          .getNumReplicationBytesTotal();

      // On the first iteration, one of the quasi closed replicas should be
      // replicated.
      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(currentReplicateCommandCount + 1,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.replicateContainerCommand));
      Assertions.assertEquals(currentReplicateCommandCount + 1,
          replicationManager.getMetrics().getNumReplicationCmdsSent());
      Assertions.assertEquals(currentBytesToReplicate + 100,
          replicationManager.getMetrics().getNumReplicationBytesTotal());
      Assertions.assertEquals(1, getInflightCount(InflightType.REPLICATION));
      Assertions.assertEquals(1, replicationManager.getMetrics()
          .getInflightReplication());

      ReplicationManagerReport report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.UNDER_REPLICATED));

      final long currentReplicateCommandCompleted = replicationManager
          .getMetrics().getNumReplicationCmdsCompleted();
      final long currentReplicateBytesCompleted = replicationManager
          .getMetrics().getNumReplicationBytesCompleted();

      // Now we add the replicated new replica
      DatanodeDetails targetDn =
          replicationManager.getLegacyReplicationManager()
          .getFirstDatanode(InflightType.REPLICATION, id);
      final ContainerReplica replicatedReplicaThree = getReplicas(
          id, State.QUASI_CLOSED, 1000L, originNodeId, targetDn);
      containerStateManager.updateContainerReplica(
          id, replicatedReplicaThree);

      // On the next run, no additional replications should be scheduled.
      replicationManager.processAll();
      eventQueue.processAll(1000);

      Assertions.assertEquals(currentReplicateCommandCompleted + 1,
          replicationManager.getMetrics().getNumReplicationCmdsCompleted());
      Assertions.assertEquals(currentReplicateBytesCompleted + 100,
          replicationManager.getMetrics().getNumReplicationBytesCompleted());
      Assertions.assertEquals(0, getInflightCount(InflightType.REPLICATION));
      Assertions.assertEquals(0, replicationManager.getMetrics()
          .getInflightReplication());

      report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
      Assertions.assertEquals(0, report.getStat(
          ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    }

    /**
     * 1 quasi-closed replica.
     * 1 unhealthy replica.
     * All have same origin node ID.
     * Expectation:
     *
     * In the first iteration of ReplicationManager, it should re-replicate
     * the quasi closed replicas so that there are 3 of them.
     *
     * In the second iteration, ReplicationManager should delete the unhealthy
     * replica since its origin node ID is not unique.
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
          id, QUASI_CLOSED, 1000L, originNodeId,
          randomDatanodeDetails());
      final ContainerReplica unhealthyReplica = getReplicas(
          id, UNHEALTHY, 1000L, originNodeId,
          randomDatanodeDetails());

      containerStateManager.addContainer(container.getProtobuf());
      containerStateManager.updateContainerReplica(id, replicaOne);
      containerStateManager.updateContainerReplica(id, unhealthyReplica);

      final int currentReplicateCommandCount = datanodeCommandHandler
          .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);
      final int currentDeleteCommandCount = datanodeCommandHandler
          .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);
      final long currentBytesToDelete = replicationManager.getMetrics()
          .getNumDeletionBytesTotal();

      // Run first iteraiton

      replicationManager.processAll();
      GenericTestUtils.waitFor(
          () -> (currentReplicateCommandCount + 2) == datanodeCommandHandler
              .getInvocationCount(
                  SCMCommandProto.Type.replicateContainerCommand),
          50, 5000);

      ReplicationManagerReport report = replicationManager.getContainerReport();
      Assertions.assertEquals(1,
          report.getStat(LifeCycleState.QUASI_CLOSED));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.UNDER_REPLICATED));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.UNHEALTHY));

      List<CommandForDatanode> replicateCommands = datanodeCommandHandler
          .getReceivedCommands().stream()
          .filter(c -> c.getCommand().getType()
              .equals(SCMCommandProto.Type.replicateContainerCommand))
          .collect(Collectors.toList());

      Assertions.assertEquals(2, replicateCommands.size());

      // Report the two new replicas to SCM.
      for (CommandForDatanode replicateCommand: replicateCommands) {
        DatanodeDetails newNode = createDatanodeDetails(
            replicateCommand.getDatanodeId());
        ContainerReplica newReplica = getReplicas(
            id, QUASI_CLOSED, 1000L, originNodeId, newNode);
        containerStateManager.updateContainerReplica(id, newReplica);
      }

      // Run second iteration.
      // Now that the quasi closed replica is sufficiently replicated, SCM
      // should delete the unhealthy replica on the next iteration.

      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(currentDeleteCommandCount + 1,
          datanodeCommandHandler
              .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
      Assertions.assertTrue(datanodeCommandHandler.received(
          SCMCommandProto.Type.deleteContainerCommand,
          unhealthyReplica.getDatanodeDetails()));
      Assertions.assertEquals(currentDeleteCommandCount + 1,
          replicationManager.getMetrics().getNumDeletionCmdsSent());
      Assertions.assertEquals(currentBytesToDelete + 99,
          replicationManager.getMetrics().getNumDeletionBytesTotal());
      Assertions.assertEquals(1,
          getInflightCount(InflightType.DELETION));
      Assertions.assertEquals(1, replicationManager.getMetrics()
          .getInflightDeletion());

      containerStateManager.removeContainerReplica(id, unhealthyReplica);

      report = replicationManager.getContainerReport();
      Assertions.assertEquals(1,
          report.getStat(LifeCycleState.QUASI_CLOSED));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
      Assertions.assertEquals(0, report.getStat(
          ReplicationManagerReport.HealthState.UNDER_REPLICATED));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.UNHEALTHY));
    }

    /**
     * If a QUASI_CLOSED container and a QUASI_CLOSED replica have the same
     * sequence ID, it's not guaranteed that this is the latest sequence ID.
     * Such a replica should not be closed.
     */
    @Test
    public void testQuasiClosedContainerAndReplicaWithSameSequenceID()
        throws IOException, TimeoutException {
      /*
      Create an under replicated QUASI_CLOSED container with 2 QUASI_CLOSED
      replicas. All have the container's sequence ID.
       */
      final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
      container.setUsedBytes(100);
      final ContainerID id = container.containerID();
      final UUID originNodeId = UUID.randomUUID();
      final ContainerReplica replicaOne = getReplicas(
          id, QUASI_CLOSED, container.getSequenceId(), originNodeId,
          randomDatanodeDetails());
      final ContainerReplica replicaTwo = getReplicas(
          id, QUASI_CLOSED, container.getSequenceId(), originNodeId,
          randomDatanodeDetails());

      containerStateManager.addContainer(container.getProtobuf());
      containerStateManager.updateContainerReplica(id, replicaOne);
      containerStateManager.updateContainerReplica(id, replicaTwo);

      final int currentCloseCommandCount = datanodeCommandHandler
          .getInvocationCount(SCMCommandProto.Type.closeContainerCommand);
      final int currentReplicateCommandCount = datanodeCommandHandler
          .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);
      final long currentBytesToReplicate = replicationManager.getMetrics()
          .getNumReplicationBytesTotal();

      /*
      One of the quasi closed replicas should be replicated and no close
      commands should be sent.
       */
      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(currentCloseCommandCount,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.closeContainerCommand));

      // One replication command should be sent
      Assertions.assertEquals(currentReplicateCommandCount + 1,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.replicateContainerCommand));
      Assertions.assertEquals(currentReplicateCommandCount + 1,
          replicationManager.getMetrics().getNumReplicationCmdsSent());
      Assertions.assertEquals(
          currentBytesToReplicate + container.getUsedBytes(),
          replicationManager.getMetrics().getNumReplicationBytesTotal());
      Assertions.assertEquals(1, getInflightCount(InflightType.REPLICATION));
      Assertions.assertEquals(1, replicationManager.getMetrics()
          .getInflightReplication());

      ReplicationManagerReport report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
      Assertions.assertEquals(1, report.getStat(
          ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    }

    /**
     * 3 quasi-closed replicas.
     * All unique origin IDs.
     * Expectation: Container is closed.
     *
     * When a container is QUASI_CLOSED and it has >50% of its replica
     * in QUASI_CLOSED state with unique origin node id,
     * ReplicationManager should force close the replica(s) with
     * highest BCSID.
     */
    @Test
    public void testQuasiClosedToClosed() throws IOException, TimeoutException {
      final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
      final ContainerID id = container.containerID();
      final Set<ContainerReplica> replicas = getReplicas(id, QUASI_CLOSED,
          randomDatanodeDetails(),
          randomDatanodeDetails(),
          randomDatanodeDetails());
      containerStateManager.addContainer(container.getProtobuf());
      for (ContainerReplica replica : replicas) {
        containerStateManager.updateContainerReplica(id, replica);
      }

      final int currentCloseCommandCount = datanodeCommandHandler
          .getInvocationCount(SCMCommandProto.Type.closeContainerCommand);

      replicationManager.processAll();
      eventQueue.processAll(1000);

      // All the replicas have same BCSID, so all of them will be closed.
      Assertions.assertEquals(currentCloseCommandCount + 3,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.closeContainerCommand));

      ReplicationManagerReport report = replicationManager.getContainerReport();
      Assertions.assertEquals(1,
          report.getStat(LifeCycleState.QUASI_CLOSED));
      Assertions.assertEquals(0, report.getStat(
          ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK));
    }
  }

  /**
   * Tests replication manager handling of decommissioning and maintainence
   * mode datanodes.
   */
  @Nested
  class DecomAndMaintenance {
    /**
     * ReplicationManager should replicate an additional replica if there are
     * decommissioned replicas.
     */
    @Test
    public void testUnderReplicatedDueToOutOfService()
        throws IOException, TimeoutException {
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
    public void testUnderReplicatedDueToAllDecommission()
        throws IOException, TimeoutException {
      runTestUnderReplicatedDueToAllDecommission(3);
    }

    Void runTestUnderReplicatedDueToAllDecommission(int expectedReplication)
        throws IOException, TimeoutException {
      final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
      addReplica(container, new NodeStatus(DECOMMISSIONING, HEALTHY), CLOSED);
      addReplica(container, new NodeStatus(DECOMMISSIONING, HEALTHY), CLOSED);
      addReplica(container, new NodeStatus(DECOMMISSIONING, HEALTHY), CLOSED);
      assertReplicaScheduled(expectedReplication);
      assertUnderReplicatedCount(1);
      return null;
    }

    @Test
    public void testReplicationLimit() throws Exception {
      runTestLimit(1, 0, 2, 0,
          () -> runTestUnderReplicatedDueToAllDecommission(1));
    }

    /**
     * ReplicationManager should not take any action when the container is
     * correctly replicated with decommissioned replicas still present.
     */
    @Test
    public void testCorrectlyReplicatedWithDecommission()
        throws IOException, TimeoutException {
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
    public void testUnderReplicatedDueToMaintenance()
        throws IOException, TimeoutException {
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
      ReplicationManagerConfiguration newConf = newRMConfig();
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
      ReplicationManagerConfiguration newConf = newRMConfig();
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
    public void testUnderReplicatedDueToAllMaintenance()
        throws IOException, TimeoutException {
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
    public void testCorrectlyReplicatedWithMaintenance()
        throws IOException, TimeoutException {
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
        throws IOException, TimeoutException {
      final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
      addReplica(container, new NodeStatus(DECOMMISSIONED, HEALTHY), CLOSED);
      addReplica(container, new NodeStatus(DECOMMISSIONED, HEALTHY), CLOSED);
      addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
      addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
      assertReplicaScheduled(2);
      assertUnderReplicatedCount(1);
    }

    /**
     * When a CLOSED container is over replicated, ReplicationManager
     * deletes the excess replicas. While choosing the replica for deletion
     * ReplicationManager should not attempt to remove a DECOMMISSION or
     * MAINTENANCE replica.
     */
    @Test
    public void testOverReplicatedClosedContainerWithDecomAndMaint()
        throws IOException, TimeoutException {
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
      Assertions.assertEquals(currentDeleteCommandCount + 2,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.deleteContainerCommand));
      Assertions.assertEquals(currentDeleteCommandCount + 2,
          replicationManager.getMetrics().getNumDeletionCmdsSent());
      Assertions.assertEquals(1, getInflightCount(InflightType.DELETION));
      Assertions.assertEquals(1, replicationManager.getMetrics()
          .getInflightDeletion());
      // Get the DECOM and Maint replica and ensure none of them are scheduled
      // for removal
      Set<ContainerReplica> decom =
          containerStateManager.getContainerReplicas(
                  container.containerID())
              .stream()
              .filter(r ->
                  r.getDatanodeDetails().getPersistedOpState() != IN_SERVICE)
              .collect(Collectors.toSet());
      for (ContainerReplica r : decom) {
        Assertions.assertFalse(datanodeCommandHandler.received(
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
    public void testUnderReplicatedNotHealthySource()
        throws IOException, TimeoutException {
      final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
      addReplica(container, NodeStatus.inServiceStale(), CLOSED);
      addReplica(container, new NodeStatus(DECOMMISSIONED, STALE), CLOSED);
      addReplica(container, new NodeStatus(DECOMMISSIONED, STALE), CLOSED);
      // There should be replica scheduled, but as all nodes are stale, nothing
      // gets scheduled.
      assertReplicaScheduled(0);
      assertUnderReplicatedCount(1);
    }
  }

  private static ReplicationManagerConfiguration newRMConfig() {
    ReplicationManagerConfiguration conf =
        new ReplicationManagerConfiguration();
    conf.setEnableLegacy(true);
    return conf;
  }

  /**
   * Tests replication manager move command.
   */
  @Nested
  class Move {
    /**
     * if all the prerequisites are satisfied, move should work as expected.
     */
    @Test
    public void testMove() throws IOException, NodeNotFoundException,
            InterruptedException, ExecutionException, TimeoutException {
      final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
      ContainerID id = container.containerID();
      ContainerReplica dn1 = addReplica(container,
              new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
      addReplica(container,
              new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
      addReplica(container,
              new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
      DatanodeDetails dn3 = addNode(new NodeStatus(IN_SERVICE, HEALTHY));
      CompletableFuture<MoveManager.MoveResult> cf =
              replicationManager.move(id, dn1.getDatanodeDetails(), dn3);
      Assertions.assertTrue(scmLogs.getOutput().contains(
              "receive a move request about container"));
      Thread.sleep(100L);
      Assertions.assertTrue(datanodeCommandHandler.received(
              SCMCommandProto.Type.replicateContainerCommand, dn3));
      Assertions.assertEquals(1, datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.replicateContainerCommand));

      //replicate container to dn3
      addReplicaToDn(container, dn3, CLOSED);
      replicationManager.processAll();
      eventQueue.processAll(1000);

      Assertions.assertTrue(datanodeCommandHandler.received(
          SCMCommandProto.Type.deleteContainerCommand,
          dn1.getDatanodeDetails()));
      Assertions.assertEquals(1, datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.deleteContainerCommand));
      containerStateManager.removeContainerReplica(id, dn1);

      replicationManager.processAll();
      eventQueue.processAll(1000);

      Assertions.assertTrue(
          cf.isDone() && cf.get() == MoveManager.MoveResult.COMPLETED);
    }

    /**
     * if crash happened and restarted, move option should work as expected.
     */
    @Test
    public void testMoveCrashAndRestart() throws IOException,
            NodeNotFoundException, InterruptedException, TimeoutException {
      final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
      ContainerID id = container.containerID();
      ContainerReplica dn1 = addReplica(container,
              new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
      addReplica(container,
              new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
      addReplica(container,
              new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
      DatanodeDetails dn3 = addNode(new NodeStatus(IN_SERVICE, HEALTHY));
      replicationManager.move(id, dn1.getDatanodeDetails(), dn3);
      Assertions.assertTrue(scmLogs.getOutput().contains(
              "receive a move request about container"));
      Thread.sleep(100L);
      Assertions.assertTrue(datanodeCommandHandler.received(
              SCMCommandProto.Type.replicateContainerCommand, dn3));
      Assertions.assertEquals(1, datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.replicateContainerCommand));

      //crash happens, restart scm.
      //clear current inflight actions and reload inflightMove from DBStore.
      resetReplicationManager();
      replicationManager.getMoveScheduler()
              .reinitialize(SCMDBDefinition.MOVE.getTable(dbStore));
      Assertions.assertTrue(replicationManager.getMoveScheduler()
              .getInflightMove().containsKey(id));
      MoveDataNodePair kv = replicationManager.getMoveScheduler()
              .getInflightMove().get(id);
      Assertions.assertEquals(kv.getSrc(), dn1.getDatanodeDetails());
      Assertions.assertEquals(kv.getTgt(), dn3);
      serviceManager.notifyStatusChanged();

      Thread.sleep(100L);
      // now, the container is not over-replicated,
      // so no deleteContainerCommand will be sent
      Assertions.assertFalse(datanodeCommandHandler.received(
          SCMCommandProto.Type.deleteContainerCommand,
          dn1.getDatanodeDetails()));
      //replica does not exist in target datanode, so a
      // replicateContainerCommand will be sent again at
      // notifyStatusChanged#onLeaderReadyAndOutOfSafeMode
      Assertions.assertEquals(2, datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.replicateContainerCommand));


      //replicate container to dn3, now, over-replicated
      addReplicaToDn(container, dn3, CLOSED);
      replicationManager.processAll();
      eventQueue.processAll(1000);

      //deleteContainerCommand is sent, but the src replica is not deleted now
      Assertions.assertEquals(1, datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.deleteContainerCommand));

      //crash happens, restart scm.
      //clear current inflight actions and reload inflightMove from DBStore.
      resetReplicationManager();
      replicationManager.getMoveScheduler()
              .reinitialize(SCMDBDefinition.MOVE.getTable(dbStore));
      Assertions.assertTrue(replicationManager.getMoveScheduler()
              .getInflightMove().containsKey(id));
      kv = replicationManager.getMoveScheduler()
              .getInflightMove().get(id);
      Assertions.assertEquals(kv.getSrc(), dn1.getDatanodeDetails());
      Assertions.assertEquals(kv.getTgt(), dn3);
      serviceManager.notifyStatusChanged();

      //after restart and the container is over-replicated now,
      //deleteContainerCommand will be sent again
      Assertions.assertEquals(2, datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.deleteContainerCommand));
      containerStateManager.removeContainerReplica(id, dn1);

      //replica in src datanode is deleted now
      containerStateManager.removeContainerReplica(id, dn1);
      replicationManager.processAll();
      eventQueue.processAll(1000);

      //since the move is complete,so after scm crash and restart
      //inflightMove should not contain the container again
      resetReplicationManager();
      replicationManager.getMoveScheduler()
              .reinitialize(SCMDBDefinition.MOVE.getTable(dbStore));
      Assertions.assertFalse(replicationManager.getMoveScheduler()
              .getInflightMove().containsKey(id));

      //completeableFuture is not stored in DB, so after scm crash and
      //restart ,completeableFuture is missing
    }

    /**
     * make sure RM does not delete replica if placement policy is not
     * satisfied.
     */
    @Test
    public void testMoveNotDeleteSrcIfPolicyNotSatisfied()
            throws IOException, NodeNotFoundException,
            InterruptedException, ExecutionException, TimeoutException {
      final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
      ContainerID id = container.containerID();
      ContainerReplica dn1 = addReplica(container,
              new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
      ContainerReplica dn2 = addReplica(container,
              new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
      addReplica(container,
              new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
      DatanodeDetails dn4 = addNode(new NodeStatus(IN_SERVICE, HEALTHY));
      CompletableFuture<MoveManager.MoveResult> cf =
              replicationManager.move(id, dn1.getDatanodeDetails(), dn4);
      Assertions.assertTrue(scmLogs.getOutput().contains(
              "receive a move request about container"));
      Thread.sleep(100L);
      Assertions.assertTrue(datanodeCommandHandler.received(
              SCMCommandProto.Type.replicateContainerCommand, dn4));
      Assertions.assertEquals(1, datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.replicateContainerCommand));

      //replicate container to dn4
      addReplicaToDn(container, dn4, CLOSED);
      //now, replication succeeds, but replica in dn2 lost,
      //and there are only tree replicas totally, so rm should
      //not delete the replica on dn1
      containerStateManager.removeContainerReplica(id, dn2);
      replicationManager.processAll();
      eventQueue.processAll(1000);

      Assertions.assertFalse(
          datanodeCommandHandler.received(
              SCMCommandProto.Type.deleteContainerCommand,
              dn1.getDatanodeDetails()));

      Assertions.assertTrue(cf.isDone() &&
              cf.get() == MoveManager.MoveResult.DELETE_FAIL_POLICY);
    }


    /**
     * test src and target datanode become unhealthy when moving.
     */
    @Test
    public void testDnBecameUnhealthyWhenMoving() throws IOException,
            NodeNotFoundException, InterruptedException, ExecutionException,
            TimeoutException {
      final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
      ContainerID id = container.containerID();
      ContainerReplica dn1 = addReplica(container,
              new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
      addReplica(container,
              new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
      addReplica(container,
              new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
      DatanodeDetails dn3 = addNode(new NodeStatus(IN_SERVICE, HEALTHY));
      CompletableFuture<MoveManager.MoveResult> cf =
              replicationManager.move(id, dn1.getDatanodeDetails(), dn3);
      Assertions.assertTrue(scmLogs.getOutput().contains(
              "receive a move request about container"));

      nodeManager.setNodeStatus(dn3, new NodeStatus(IN_SERVICE, STALE));
      replicationManager.processAll();
      eventQueue.processAll(1000);

      Assertions.assertTrue(cf.isDone() && cf.get() ==
              MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);

      nodeManager.setNodeStatus(dn3, new NodeStatus(IN_SERVICE, HEALTHY));
      cf = replicationManager.move(id, dn1.getDatanodeDetails(), dn3);
      addReplicaToDn(container, dn3, CLOSED);
      replicationManager.processAll();
      eventQueue.processAll(1000);
      nodeManager.setNodeStatus(dn1.getDatanodeDetails(),
              new NodeStatus(IN_SERVICE, STALE));
      replicationManager.processAll();
      eventQueue.processAll(1000);

      Assertions.assertTrue(cf.isDone() && cf.get() ==
              MoveManager.MoveResult.DELETION_FAIL_NODE_UNHEALTHY);
    }

    /**
     * before Replication Manager generates a completablefuture for a move
     * option, some Prerequisites should be satisfied.
     */
    @Test
    public void testMovePrerequisites() throws IOException,
        NodeNotFoundException, InterruptedException, ExecutionException,
        InvalidStateTransitionException, TimeoutException {
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

      CompletableFuture<MoveManager.MoveResult> cf;
      //the above move is executed successfully, so there may be some item in
      //inflightReplication or inflightDeletion. here we stop replication
      // manager to clear these states, which may impact the tests below.
      //we don't need a running replicationManamger now
      replicationManager.stop();
      Thread.sleep(100L);
      cf = replicationManager.move(id, dn1.getDatanodeDetails(), dn3);
      Assertions.assertTrue(cf.isDone() && cf.get() ==
              MoveManager.MoveResult.FAIL_UNEXPECTED_ERROR);
      replicationManager.start();
      Thread.sleep(100L);

      //container in not in OPEN state
      cf = replicationManager.move(id, dn1.getDatanodeDetails(), dn3);
      Assertions.assertTrue(cf.isDone() && cf.get() ==
              MoveManager.MoveResult.REPLICATION_FAIL_CONTAINER_NOT_CLOSED);
      //open -> closing
      containerStateManager.updateContainerState(id.getProtobuf(),
              LifeCycleEvent.FINALIZE);
      cf = replicationManager.move(id, dn1.getDatanodeDetails(), dn3);
      Assertions.assertTrue(cf.isDone() && cf.get() ==
              MoveManager.MoveResult.REPLICATION_FAIL_CONTAINER_NOT_CLOSED);
      //closing -> quasi_closed
      containerStateManager.updateContainerState(id.getProtobuf(),
              LifeCycleEvent.QUASI_CLOSE);
      cf = replicationManager.move(id, dn1.getDatanodeDetails(), dn3);
      Assertions.assertTrue(cf.isDone() && cf.get() ==
              MoveManager.MoveResult.REPLICATION_FAIL_CONTAINER_NOT_CLOSED);

      //quasi_closed -> closed
      containerStateManager.updateContainerState(id.getProtobuf(),
              LifeCycleEvent.FORCE_CLOSE);
      Assertions.assertSame(LifeCycleState.CLOSED,
              containerStateManager.getContainer(id).getState());

      //Node is not in healthy state
      for (HddsProtos.NodeState state : HddsProtos.NodeState.values()) {
        if (state != HEALTHY) {
          nodeManager.setNodeStatus(dn3,
                  new NodeStatus(IN_SERVICE, state));
          cf = replicationManager.move(id, dn1.getDatanodeDetails(), dn3);
          Assertions.assertTrue(cf.isDone() && cf.get() ==
                  MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);
          cf = replicationManager.move(id, dn3, dn1.getDatanodeDetails());
          Assertions.assertTrue(cf.isDone() && cf.get() ==
                  MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);
        }
      }
      nodeManager.setNodeStatus(dn3, new NodeStatus(IN_SERVICE, HEALTHY));

      //Node is not in IN_SERVICE state
      for (HddsProtos.NodeOperationalState state :
              HddsProtos.NodeOperationalState.values()) {
        if (state != IN_SERVICE) {
          nodeManager.setNodeStatus(dn3,
                  new NodeStatus(state, HEALTHY));
          cf = replicationManager.move(id, dn1.getDatanodeDetails(), dn3);
          Assertions.assertTrue(cf.isDone() && cf.get() ==
                  MoveManager.MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE);
          cf = replicationManager.move(id, dn3, dn1.getDatanodeDetails());
          Assertions.assertTrue(cf.isDone() && cf.get() ==
                  MoveManager.MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE);
        }
      }
      nodeManager.setNodeStatus(dn3, new NodeStatus(IN_SERVICE, HEALTHY));

      //container exists in target datanode
      cf = replicationManager.move(id, dn1.getDatanodeDetails(),
              dn2.getDatanodeDetails());
      Assertions.assertTrue(cf.isDone() && cf.get() ==
              MoveManager.MoveResult.REPLICATION_FAIL_EXIST_IN_TARGET);

      //container does not exist in source datanode
      cf = replicationManager.move(id, dn3, dn3);
      Assertions.assertTrue(cf.isDone() && cf.get() ==
              MoveManager.MoveResult.REPLICATION_FAIL_NOT_EXIST_IN_SOURCE);

      //make container over relplicated to test the
      // case that container is in inflightDeletion
      ContainerReplica dn5 = addReplica(container,
              new NodeStatus(IN_SERVICE, HEALTHY), State.CLOSED);
      replicationManager.processAll();
      //waiting for inflightDeletion generation
      eventQueue.processAll(1000);
      cf = replicationManager.move(id, dn1.getDatanodeDetails(), dn3);
      Assertions.assertTrue(cf.isDone() && cf.get() ==
              MoveManager.MoveResult.REPLICATION_FAIL_INFLIGHT_DELETION);
      resetReplicationManager();

      //make the replica num be 2 to test the case
      //that container is in inflightReplication
      containerStateManager.removeContainerReplica(id, dn5);
      containerStateManager.removeContainerReplica(id, dn4);
      //replication manager should generate inflightReplication
      replicationManager.processAll();
      //waiting for inflightReplication generation
      eventQueue.processAll(1000);
      cf = replicationManager.move(id, dn1.getDatanodeDetails(), dn3);
      Assertions.assertTrue(cf.isDone() && cf.get() ==
              MoveManager.MoveResult.REPLICATION_FAIL_INFLIGHT_REPLICATION);
    }
  }

  /**
   * Tests mis-replicated containers with rack topology information.
   */
  @Nested
  class MisReplicated {

    @Test
    public void additionalReplicaScheduledWhenMisReplicated()
            throws IOException, TimeoutException {
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
      containerStateManager.updateContainerReplica(id, replicaOne);
      containerStateManager.updateContainerReplica(id, replicaTwo);
      containerStateManager.updateContainerReplica(
              id, replicaThree);

      // Ensure a mis-replicated status is returned for any containers in this
      // test where there are 3 replicas. When there are 2 or 4 replicas
      // the status returned will be healthy.
      Mockito.when(ratisContainerPlacementPolicy.validateContainerPlacement(
              Mockito.argThat(list -> list.size() == 3),
              Mockito.anyInt()
      )).thenAnswer(invocation -> {
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

      Assertions.assertEquals(currentReplicateCommandCount + 1,
              datanodeCommandHandler.getInvocationCount(
                      SCMCommandProto.Type.replicateContainerCommand));
      Assertions.assertEquals(currentReplicateCommandCount + 1,
              replicationManager.getMetrics().getNumReplicationCmdsSent());
      Assertions.assertEquals(currentBytesToReplicate + 100,
              replicationManager.getMetrics().getNumReplicationBytesTotal());
      Assertions.assertEquals(1, getInflightCount(InflightType.REPLICATION));
      Assertions.assertEquals(1, replicationManager.getMetrics()
              .getInflightReplication());

      ReplicationManagerReport report = replicationManager.getContainerReport();
      Assertions.assertEquals(1, report.getStat(LifeCycleState.CLOSED));
      Assertions.assertEquals(1, report.getStat(
              ReplicationManagerReport.HealthState.MIS_REPLICATED));

      // Now make it so that all containers seem mis-replicated no matter how
      // many replicas. This will test replicas are not scheduled if the new
      // replica does not fix the mis-replication.
      Mockito.when(ratisContainerPlacementPolicy.validateContainerPlacement(
              Mockito.anyList(),
              Mockito.anyInt()
      )).thenAnswer(invocation -> {
        return new ContainerPlacementStatusDefault(1, 2, 3);
      });

      currentReplicateCommandCount = datanodeCommandHandler.getInvocationCount(
          SCMCommandProto.Type.replicateContainerCommand);

      replicationManager.processAll();
      eventQueue.processAll(1000);
      // At this stage, due to the mocked calls to validateContainerPlacement
      // the mis-replicated racks will not have improved, so expect to see
      // nothing scheduled.
      Assertions.assertEquals(currentReplicateCommandCount,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.replicateContainerCommand));
      Assertions.assertEquals(currentReplicateCommandCount,
              replicationManager.getMetrics().getNumReplicationCmdsSent());
      Assertions.assertEquals(1, getInflightCount(InflightType.REPLICATION));
      Assertions.assertEquals(1, replicationManager.getMetrics()
              .getInflightReplication());
    }

    @Test
    public void overReplicatedButRemovingMakesMisReplicated()
            throws IOException, TimeoutException {
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
              id, UNHEALTHY, 1000L, originNodeId, randomDatanodeDetails());

      containerStateManager.addContainer(container.getProtobuf());
      containerStateManager.updateContainerReplica(id, replicaOne);
      containerStateManager.updateContainerReplica(id, replicaTwo);
      containerStateManager.updateContainerReplica(
              id, replicaThree);
      containerStateManager.updateContainerReplica(id, replicaFour);
      containerStateManager.updateContainerReplica(id, replicaFive);

      // Ensure a mis-replicated status is returned for any containers in this
      // test where there are exactly 3 replicas checked.
      Mockito.when(ratisContainerPlacementPolicy.validateContainerPlacement(
              Mockito.argThat(list -> list.size() == 3),
              Mockito.anyInt()
      )).thenAnswer(
              invocation -> new ContainerPlacementStatusDefault(1, 2, 3));

      int currentDeleteCommandCount = datanodeCommandHandler
              .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

      replicationManager.processAll();
      eventQueue.processAll(1000);
      // TODO the new (non-legacy) RM needs a separate handler for
      //  topology status to make progress in this case by:
      //  1. Deleting the closed replica to restore proper replica count.
      //  2. Deleting the unhealthy replica since there are adequate healthy
      //  replicas.
      //  3. Fixing topology issues left by the previous cleanup tasks.
      // Current legacy RM implementation will take no action in this case
      // because deletion would compromise topology.
      Assertions.assertEquals(currentDeleteCommandCount,
              datanodeCommandHandler.getInvocationCount(
                  SCMCommandProto.Type.deleteContainerCommand));
      Assertions.assertEquals(currentDeleteCommandCount,
              replicationManager.getMetrics().getNumDeletionCmdsSent());

      Assertions.assertFalse(datanodeCommandHandler.received(
              SCMCommandProto.Type.deleteContainerCommand,
              replicaFive.getDatanodeDetails()));
      Assertions.assertEquals(0, getInflightCount(InflightType.DELETION));
      Assertions.assertEquals(0, replicationManager.getMetrics()
              .getInflightDeletion());
      assertOverReplicatedCount(1);
    }

    @Test
    public void testOverReplicatedAndPolicySatisfied()
            throws IOException, TimeoutException {
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
      containerStateManager.updateContainerReplica(id, replicaOne);
      containerStateManager.updateContainerReplica(id, replicaTwo);
      containerStateManager.updateContainerReplica(
              id, replicaThree);
      containerStateManager.updateContainerReplica(id, replicaFour);

      Mockito.when(ratisContainerPlacementPolicy.validateContainerPlacement(
              Mockito.argThat(list -> list.size() == 3),
              Mockito.anyInt()
      )).thenAnswer(
              invocation -> new ContainerPlacementStatusDefault(2, 2, 3));

      final int currentDeleteCommandCount = datanodeCommandHandler
              .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(currentDeleteCommandCount + 1,
              datanodeCommandHandler.getInvocationCount(
                  SCMCommandProto.Type.deleteContainerCommand));
      Assertions.assertEquals(currentDeleteCommandCount + 1,
              replicationManager.getMetrics().getNumDeletionCmdsSent());
      Assertions.assertEquals(1, getInflightCount(InflightType.DELETION));
      Assertions.assertEquals(1, replicationManager.getMetrics()
              .getInflightDeletion());

      assertOverReplicatedCount(1);
    }

    @Test
    public void testOverReplicatedAndPolicyUnSatisfiedAndDeleted()
            throws IOException, TimeoutException {
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
              id, QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

      containerStateManager.addContainer(container.getProtobuf());
      containerStateManager.updateContainerReplica(id, replicaOne);
      containerStateManager.updateContainerReplica(id, replicaTwo);
      containerStateManager.updateContainerReplica(
              id, replicaThree);
      containerStateManager.updateContainerReplica(id, replicaFour);
      containerStateManager.updateContainerReplica(id, replicaFive);

      Mockito.when(ratisContainerPlacementPolicy.validateContainerPlacement(
              Mockito.argThat(list -> list != null && list.size() <= 4),
              Mockito.anyInt()
      )).thenAnswer(
              invocation -> new ContainerPlacementStatusDefault(1, 2, 3));

      int currentDeleteCommandCount = datanodeCommandHandler
              .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

      // On the first run, RM will delete one of the extra closed replicas.
      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(currentDeleteCommandCount + 1,
              datanodeCommandHandler.getInvocationCount(
                  SCMCommandProto.Type.deleteContainerCommand));
      Assertions.assertEquals(currentDeleteCommandCount + 1,
              replicationManager.getMetrics().getNumDeletionCmdsSent());
      Assertions.assertEquals(1, getInflightCount(InflightType.DELETION));
      Assertions.assertEquals(1, replicationManager.getMetrics()
              .getInflightDeletion());

      assertAnyDeleteTargets(
          replicaOne.getDatanodeDetails(),
          replicaTwo.getDatanodeDetails(),
          replicaThree.getDatanodeDetails(),
          replicaFour.getDatanodeDetails()
      );

      currentDeleteCommandCount = datanodeCommandHandler
          .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

      // One the second run, the container is now properly replicated when
      // counting in flight deletes. This allows the quasi closed container to
      // be deleted by the unhealthy container handler.
      replicationManager.processAll();
      eventQueue.processAll(1000);
      Assertions.assertEquals(currentDeleteCommandCount + 1,
          datanodeCommandHandler.getInvocationCount(
              SCMCommandProto.Type.deleteContainerCommand));
      Assertions.assertEquals(currentDeleteCommandCount + 1,
          replicationManager.getMetrics().getNumDeletionCmdsSent());
      Assertions.assertEquals(1, getInflightCount(InflightType.DELETION));
      Assertions.assertEquals(1, replicationManager.getMetrics()
          .getInflightDeletion());

      assertDeleteTargetsContain(replicaFive.getDatanodeDetails());
    }
  }

  void runTestLimit(int replicationLimit, int deletionLimit,
      int expectedReplicationSkipped, int expectedDeletionSkipped,
      Callable<Void> testcase) throws Exception {
    createReplicationManager(replicationLimit, deletionLimit);

    final ReplicationManagerMetrics metrics = replicationManager.getMetrics();
    final long replicationSkipped = metrics.getInflightReplicationSkipped();
    final long deletionSkipped = metrics.getInflightDeletionSkipped();

    testcase.call();

    Assertions.assertEquals(replicationSkipped + expectedReplicationSkipped,
        metrics.getInflightReplicationSkipped());
    Assertions.assertEquals(deletionSkipped + expectedDeletionSkipped,
        metrics.getInflightDeletionSkipped());

    //reset limits for other tests.
    createReplicationManager(0, 0);
  }

  /**
   * Checks that the set of datanodes given delete commands exactly matches
   * targetDNs.
   */
  private void assertExactDeleteTargets(DatanodeDetails... targetDNs) {
    List<CommandForDatanode> deleteCommands = datanodeCommandHandler
        .getReceivedCommands().stream()
        .filter(c -> c.getCommand().getType() ==
            SCMCommandProto.Type.deleteContainerCommand)
        .collect(Collectors.toList());

    Assertions.assertEquals(targetDNs.length, deleteCommands.size());

    Set<UUID> targetDNIDs = Arrays.stream(targetDNs)
        .map(DatanodeDetails::getUuid)
        .collect(Collectors.toSet());
    Set<UUID> chosenDNIDs = deleteCommands.stream()
        .map(CommandForDatanode::getDatanodeId)
        .collect(Collectors.toSet());

    Assertions.assertEquals(targetDNIDs, chosenDNIDs);
  }

  /**
   * Checks if the set of nodes with deletions scheduled were taken from the
   * provided set of DNs.
   */
  private void assertAnyDeleteTargets(DatanodeDetails... validDeleteDNs) {
    List<CommandForDatanode> deleteCommands = datanodeCommandHandler
        .getReceivedCommands().stream()
        .filter(c -> c.getCommand().getType() ==
            SCMCommandProto.Type.deleteContainerCommand)
        .collect(Collectors.toList());

    Set<UUID> deleteCandidateIDs = Arrays.stream(validDeleteDNs)
        .map(DatanodeDetails::getUuid)
        .collect(Collectors.toSet());
    Set<UUID> chosenDNIDs = deleteCommands.stream()
        .map(CommandForDatanode::getDatanodeId)
        .collect(Collectors.toSet());

    Assertions.assertTrue(deleteCandidateIDs.containsAll(chosenDNIDs));
  }

  /**
   * Checks if the set of nodes with deletions scheduled contains all of the
   * provided DNs.
   */
  private void assertDeleteTargetsContain(DatanodeDetails... deleteDN) {
    List<CommandForDatanode> deleteCommands = datanodeCommandHandler
        .getReceivedCommands().stream()
        .filter(c -> c.getCommand().getType() ==
            SCMCommandProto.Type.deleteContainerCommand)
        .collect(Collectors.toList());

    Set<UUID> deleteDNIDs = Arrays.stream(deleteDN)
        .map(DatanodeDetails::getUuid)
        .collect(Collectors.toSet());
    Set<UUID> chosenDNIDs = deleteCommands.stream()
        .map(CommandForDatanode::getDatanodeId)
        .collect(Collectors.toSet());

    Assertions.assertTrue(chosenDNIDs.containsAll(deleteDNIDs));
  }

  private ContainerInfo createContainer(LifeCycleState containerState)
      throws IOException, TimeoutException {
    return createContainer(containerState, CONTAINER_USED_BYTES_DEFAULT,
        CONTAINER_NUM_KEYS_DEFAULT);
  }

  private ContainerInfo createContainer(LifeCycleState containerState,
      long usedBytes, long numKeys) throws IOException, TimeoutException {
    final ContainerInfo container = getContainer(containerState);
    container.setUsedBytes(usedBytes);
    container.setNumberOfKeys(numKeys);
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

  private ContainerReplica addReplica(ContainerInfo container,
      NodeStatus nodeStatus, State replicaState, long usedBytes, long numOfKeys)
      throws ContainerNotFoundException {
    DatanodeDetails dn = addNode(nodeStatus);
    return addReplicaToDn(container, dn, replicaState, usedBytes, numOfKeys);
  }

  private ContainerReplica addReplicaToDn(ContainerInfo container,
                               DatanodeDetails dn, State replicaState)
      throws ContainerNotFoundException {
    return addReplicaToDn(container, dn, replicaState, 1000L);
  }

  private ContainerReplica addReplicaToDn(ContainerInfo container,
      DatanodeDetails dn, State replicaState, long bcsId)
      throws ContainerNotFoundException {
    // Using the same originID for all replica in the container set. If each
    // replica has a unique originID, it causes problems in ReplicationManager
    // when processing over-replicated containers.
    final UUID originNodeId =
        UUID.nameUUIDFromBytes(Longs.toByteArray(container.getContainerID()));
    final ContainerReplica replica = getReplicas(container.containerID(),
        replicaState, container.getUsedBytes(), container.getNumberOfKeys(),
        bcsId, originNodeId, dn);
    containerStateManager
        .updateContainerReplica(container.containerID(), replica);
    return replica;
  }

  private ContainerReplica addReplicaToDn(ContainerInfo container,
      DatanodeDetails dn, State replicaState, long usedBytes, long numOfKeys)
      throws ContainerNotFoundException {
    // Using the same originID for all replica in the container set. If each
    // replica has a unique originID, it causes problems in ReplicationManager
    // when processing over-replicated containers.
    final UUID originNodeId =
        UUID.nameUUIDFromBytes(Longs.toByteArray(container.getContainerID()));
    final ContainerReplica replica = getReplicas(container.containerID(),
        replicaState, usedBytes, numOfKeys, 1000L, originNodeId, dn);
    containerStateManager
        .updateContainerReplica(container.containerID(), replica);
    return replica;
  }

  private void assertReplicaScheduled(int delta) {
    final int currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assertions.assertEquals(currentReplicateCommandCount + delta,
        datanodeCommandHandler.getInvocationCount(
            SCMCommandProto.Type.replicateContainerCommand));
    Assertions.assertEquals(currentReplicateCommandCount + delta,
        replicationManager.getMetrics().getNumReplicationCmdsSent());
  }

  private void assertDeleteScheduled(int delta) {
    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assertions.assertEquals(currentDeleteCommandCount + delta,
        datanodeCommandHandler.getInvocationCount(
            SCMCommandProto.Type.deleteContainerCommand));
    Assertions.assertEquals(currentDeleteCommandCount + delta,
        replicationManager.getMetrics().getNumDeletionCmdsSent());
  }

  private void assertUnderReplicatedCount(int count) {
    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assertions.assertEquals(count, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
  }

  private void assertMissingCount(int count) {
    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assertions.assertEquals(count, report.getStat(
        ReplicationManagerReport.HealthState.MISSING));
  }

  private void assertOverReplicatedCount(int count) {
    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assertions.assertEquals(count, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
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
