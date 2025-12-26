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

package org.apache.hadoop.hdds.scm.container.replication;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority.LOW;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.ADD;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerInfo;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerReplica;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createReplicas;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createReplicasWithSameOrigin;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.getNoNodesTestPlacementPolicy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.protobuf.ProtoUtils;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.token.ContainerTokenGenerator;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.TestClock;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/**
 * Tests for the ReplicationManager.
 */
public class TestReplicationManager {

  private OzoneConfiguration configuration;
  private ReplicationManager replicationManager;
  private ReplicationManager.ReplicationManagerConfiguration rmConf;
  private ContainerManager containerManager;
  private PlacementPolicy ratisPlacementPolicy;
  private PlacementPolicy ecPlacementPolicy;
  private EventPublisher eventPublisher;
  private SCMContext scmContext;
  private NodeManager nodeManager;
  private TestClock clock;
  private ContainerReplicaPendingOps containerReplicaPendingOps;

  private Map<ContainerID, Set<ContainerReplica>> containerReplicaMap;
  private Set<ContainerInfo> containerInfoSet;
  private ReplicationConfig repConfig;
  private ReplicationManagerReport repReport;
  private ReplicationQueue repQueue;
  private Set<Pair<DatanodeID, SCMCommand<?>>> commandsSent;

  @BeforeEach
  public void setup() throws IOException {
    configuration = new OzoneConfiguration();
    configuration.set(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "0s");
    rmConf = configuration.getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    containerManager = mock(ContainerManager.class);
    ratisPlacementPolicy = mock(PlacementPolicy.class);
    when(ratisPlacementPolicy.validateContainerPlacement(anyList(),
        anyInt())).thenReturn(new ContainerPlacementStatusDefault(2, 2, 3));
    ecPlacementPolicy = mock(PlacementPolicy.class);
    when(ecPlacementPolicy.validateContainerPlacement(
        anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(2, 2, 3));

    scmContext = mock(SCMContext.class);

    nodeManager = mock(NodeManager.class);
    commandsSent = new HashSet<>();
    eventPublisher = mock(EventPublisher.class);
    doAnswer(invocation -> {
      commandsSent.add(Pair.of(invocation.getArgument(0),
          invocation.getArgument(1)));
      return null;
    }).when(nodeManager).addDatanodeCommand(any(), any());

    clock = new TestClock(Instant.now(), ZoneId.systemDefault());
    containerReplicaPendingOps =
        new ContainerReplicaPendingOps(clock, null);

    when(containerManager
        .getContainerReplicas(any(ContainerID.class))).thenAnswer(
          invocation -> {
            ContainerID cid = invocation.getArgument(0);
            return containerReplicaMap.get(cid);
          });

    when(containerManager.getContainers()).thenAnswer(
        invocation -> new ArrayList<>(containerInfoSet));
    replicationManager = createReplicationManager();
    containerReplicaMap = new HashMap<>();
    containerInfoSet = new HashSet<>();
    repConfig = new ECReplicationConfig(3, 2);
    repReport = new ReplicationManagerReport(rmConf.getContainerSampleLimit());
    repQueue = new ReplicationQueue();

    // Ensure that RM will run when asked.
    when(scmContext.isLeaderReady()).thenReturn(true);
    when(scmContext.isInSafeMode()).thenReturn(false);

    PipelineManager pipelineManager = mock(PipelineManager.class);
    when(pipelineManager.getPipeline(any()))
        .thenReturn(HddsTestUtils.getRandomPipeline());

    StorageContainerManager scm = mock(StorageContainerManager.class);
    when(scm.getPipelineManager()).thenReturn(pipelineManager);
    when(scm.getContainerTokenGenerator()).thenReturn(ContainerTokenGenerator.DISABLED);

    when(scmContext.getScm()).thenReturn(scm);
  }

  @AfterEach
  void cleanup() {
    if (replicationManager.getMetrics() != null) {
      replicationManager.getMetrics().unRegister();
    }
  }

  private ReplicationManager createReplicationManager() throws IOException {
    return new ReplicationManager(
        rmConf,
        configuration,
        containerManager,
        ratisPlacementPolicy,
        ecPlacementPolicy,
        eventPublisher,
        scmContext,
        nodeManager,
        clock,
        containerReplicaPendingOps) {
      @Override
      protected void startSubServices() {
        // do not start any threads for processing
      }
    };
  }

  private void enableProcessAll() {
    SCMServiceManager serviceManager = new SCMServiceManager();
    serviceManager.register(replicationManager);
    serviceManager.notifyStatusChanged();
  }

  @Test
  public void testPendingOpsClearedWhenStarting() {
    containerReplicaPendingOps.scheduleAddReplica(ContainerID.valueOf(1),
        MockDatanodeDetails.randomDatanodeDetails(), 1, null, Integer.MAX_VALUE, 5L, clock.millis());
    containerReplicaPendingOps.scheduleDeleteReplica(ContainerID.valueOf(2),
        MockDatanodeDetails.randomDatanodeDetails(), 1, null, Integer.MAX_VALUE);
    assertEquals(1, containerReplicaPendingOps
        .getPendingOpCount(ContainerReplicaOp.PendingOpType.ADD));
    assertEquals(1, containerReplicaPendingOps
        .getPendingOpCount(ContainerReplicaOp.PendingOpType.DELETE));

    // Registers against serviceManager and notifies the status has changed.
    enableProcessAll();

    // Pending ops should be cleared.
    assertEquals(0, containerReplicaPendingOps
        .getPendingOpCount(ContainerReplicaOp.PendingOpType.ADD));
    assertEquals(0, containerReplicaPendingOps
        .getPendingOpCount(ContainerReplicaOp.PendingOpType.DELETE));
  }

  @Test
  public void testOpenContainerSkipped() throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.OPEN);
    // It is under replicated, but as its still open it is seen as healthy.
    addReplicas(container, ContainerReplicaProto.State.OPEN, 1, 2, 3, 4);
    replicationManager.processContainer(
        container, repQueue, repReport);
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testUnhealthyOpenContainerClosed()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.OPEN);
    // Container is open but replicas are closed, so it is open but unhealthy.
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4);
    replicationManager.processContainer(
        container, repQueue, repReport);
    verify(eventPublisher, times(1))
        .fireEvent(SCMEvents.CLOSE_CONTAINER, container.containerID());
    assertEquals(1, repReport.getStat(
        ContainerHealthState.OPEN_UNHEALTHY));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void misMatchedReplicasOfRatisContainerShouldBeClosed()
      throws ContainerNotFoundException {
    RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    ContainerInfo container = createContainerInfo(ratisRepConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    // Container is closed but replicas are open
    Set<ContainerReplica> replicas =
        addReplicas(container, ContainerReplicaProto.State.CLOSING, 0, 0, 0);
    ContainerReplica closedReplica =
        createContainerReplica(container.containerID(), 0, IN_SERVICE,
            ContainerReplicaProto.State.CLOSED);
    replicas.add(closedReplica);

    replicationManager.processContainer(container, repQueue, repReport);

    verify(nodeManager, times(3)).addDatanodeCommand(any(), any());
    assertEquals(1, repReport.getStat(
        ContainerHealthState.OVER_REPLICATED));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    /*
    Though over replicated, this container should not be added to over
    replicated queue until all replicas are closed.
     */
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testUnderReplicatedQuasiClosedContainerWithUnhealthyReplica()
      throws ContainerNotFoundException {
    RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    ContainerInfo container = createContainerInfo(ratisRepConfig, 1,
        HddsProtos.LifeCycleState.QUASI_CLOSED);
    // Container is closed but replicas are open
    Set<ContainerReplica> replicas =
        addReplicas(container, ContainerReplicaProto.State.CLOSING, 0);
    ContainerReplica quasiClosedReplica =
        createContainerReplica(container.containerID(), 0, IN_SERVICE,
            ContainerReplicaProto.State.QUASI_CLOSED);
    ContainerReplica unhealthyReplica =
        createContainerReplica(container.containerID(), 0, IN_SERVICE,
            ContainerReplicaProto.State.UNHEALTHY);
    replicas.add(quasiClosedReplica);
    replicas.add(unhealthyReplica);

    replicationManager.processContainer(container, repQueue, repReport);

    verify(nodeManager, times(1)).addDatanodeCommand(any(), any());
    // Quasi-closed container with unhealthy replica is handled differently
    // Check for appropriate state based on handler behavior
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @ParameterizedTest
  @EnumSource(value = HddsProtos.LifeCycleState.class,
      names = {"CLOSED", "QUASI_CLOSED"})
  public void testUnderReplicatedClosedContainerWithOnlyUnhealthyReplicas(
      HddsProtos.LifeCycleState state)
      throws ContainerNotFoundException {
    RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    ContainerInfo container = createContainerInfo(ratisRepConfig, 1,
        state);
    // Container is closed but replicas are UNHEALTHY
    Set<ContainerReplica> replicas =
        addReplicas(container, ContainerReplicaProto.State.UNHEALTHY, 0, 0);
    ContainerReplica unhealthyOnDecommissioning = createContainerReplica(
        container.containerID(), 0, DECOMMISSIONING,
        ContainerReplicaProto.State.UNHEALTHY);
    replicas.add(unhealthyOnDecommissioning);

    replicationManager.processContainer(container, repQueue, repReport);
    // Container with only unhealthy replicas is UNHEALTHY_UNDER_REPLICATED
    assertEquals(1, repReport.getStat(
        ContainerHealthState.UNHEALTHY_UNDER_REPLICATED));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  /**
   * Situation: QUASI_CLOSED container with 3 QUASI_CLOSED and 1 UNHEALTHY
   * replica. They all have the same origin node id. It's expected that the
   * UNHEALTHY replica (and not a QUASI_CLOSED one) is deleted.
   */
  @Test
  public void testQuasiClosedContainerWithExcessUnhealthyReplica()
      throws IOException, NodeNotFoundException {
    when(nodeManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenReturn(NodeStatus.inServiceHealthy());
    RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    ContainerInfo container = createContainerInfo(ratisRepConfig, 1,
        HddsProtos.LifeCycleState.QUASI_CLOSED);
    Set<ContainerReplica> replicas =
        createReplicasWithSameOrigin(container.containerID(),
            ContainerReplicaProto.State.QUASI_CLOSED, 0, 0, 0);
    final DatanodeID origin = replicas.iterator().next().getOriginDatanodeId();
    ContainerReplica unhealthy =
        createContainerReplica(container.containerID(), 0, IN_SERVICE,
            ContainerReplicaProto.State.UNHEALTHY, 1, 123,
            MockDatanodeDetails.randomDatanodeDetails(), origin);
    replicas.add(unhealthy);
    storeContainerAndReplicas(container, replicas);

    replicationManager.processContainer(container, repQueue, repReport);
    assertEquals(0, repReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(1, repReport.getStat(
        ContainerHealthState.OVER_REPLICATED));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(1, repQueue.overReplicatedQueueSize());

    RatisOverReplicationHandler handler = new RatisOverReplicationHandler(
        ratisPlacementPolicy, replicationManager);

    handler.processAndSendCommands(replicas, Collections.emptyList(),
            repQueue.dequeueOverReplicatedContainer(), 2);
    assertTrue(commandsSent.iterator().hasNext());
    assertEquals(unhealthy.getDatanodeDetails().getID(),
        commandsSent.iterator().next().getKey());
    assertEquals(SCMCommandProto.Type.deleteContainerCommand,
        commandsSent.iterator().next().getValue().getType());

  }

  @Test
  public void testClosedContainerWithOverReplicatedAllUnhealthy()
      throws ContainerNotFoundException {
    RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    ContainerInfo container = createContainerInfo(ratisRepConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas =
        createReplicas(container.containerID(),
            ContainerReplicaProto.State.UNHEALTHY, 0, 0, 0, 0);
    storeContainerAndReplicas(container, replicas);

    replicationManager.processContainer(container, repQueue, repReport);
    // Container with all unhealthy replicas is UNHEALTHY_OVER_REPLICATED
    assertEquals(1, repReport.getStat(
        ContainerHealthState.UNHEALTHY_OVER_REPLICATED));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(1, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testClosedContainerWithExcessUnhealthy()
      throws ContainerNotFoundException {
    RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    ContainerInfo container = createContainerInfo(ratisRepConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas =
        createReplicas(container.containerID(),
            ContainerReplicaProto.State.CLOSED, 0, 0, 0);
    ContainerReplica unhealthyReplica =
        createContainerReplica(container.containerID(), 0, IN_SERVICE,
            ContainerReplicaProto.State.UNHEALTHY);
    replicas.add(unhealthyReplica);
    storeContainerAndReplicas(container, replicas);

    replicationManager.processContainer(container, repQueue, repReport);
    assertEquals(0, repReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(1, repReport.getStat(
        ContainerHealthState.OVER_REPLICATED));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(1, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testQuasiClosedContainerWithUnhealthyReplicaOnUniqueOrigin()
      throws IOException {
    RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    ContainerInfo container = createContainerInfo(ratisRepConfig, 1,
        HddsProtos.LifeCycleState.QUASI_CLOSED);
    Set<ContainerReplica> replicas =
        createReplicasWithSameOrigin(container.containerID(),
            ContainerReplicaProto.State.QUASI_CLOSED, 0, 0, 0);
    ContainerReplica unhealthy =
        createContainerReplica(container.containerID(), 0, IN_SERVICE,
            ContainerReplicaProto.State.UNHEALTHY);
    replicas.add(unhealthy);
    storeContainerAndReplicas(container, replicas);

    replicationManager.processContainer(container, repQueue, repReport);
    // Quasi-closed stuck with unhealthy replica on unique origin
    // Container is stuck (same origin) so QuasiClosedStuckReplicationCheck handles it first
    // Sets QUASI_CLOSED_STUCK_UNDER_REPLICATED (not UNHEALTHY_UNDER_REPLICATED)
    assertEquals(1, repReport.getStat(
        ContainerHealthState.QUASI_CLOSED_STUCK_UNDER_REPLICATED));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  /**
   * When there is Quasi Closed Replica with incorrect sequence id
   * for a Closed container, it's treated as unhealthy and deleted.
   * The deletion should happen only after we re-replicate a healthy
   * replica.
   */
  @Test
  public void testClosedContainerWithQuasiClosedReplicaWithWrongSequence()
      throws IOException, NodeNotFoundException {
    final RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    final long sequenceId = 101;

    final ContainerInfo container = createContainerInfo(ratisRepConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    final ContainerReplica replicaOne = createContainerReplica(
        ContainerID.valueOf(1), 0, IN_SERVICE,
        ContainerReplicaProto.State.CLOSED, sequenceId);
    final ContainerReplica replicaTwo = createContainerReplica(
        ContainerID.valueOf(1), 0, IN_SERVICE,
        ContainerReplicaProto.State.CLOSED, sequenceId);

    final ContainerReplica quasiCloseReplica = createContainerReplica(
        ContainerID.valueOf(1), 0, IN_SERVICE,
        ContainerReplicaProto.State.QUASI_CLOSED, sequenceId - 5);

    Set<ContainerReplica> replicas = new HashSet<>();
    replicas.add(replicaOne);
    replicas.add(replicaTwo);
    replicas.add(quasiCloseReplica);
    storeContainerAndReplicas(container, replicas);

    replicationManager.processContainer(container, repQueue, repReport);
    assertEquals(1, repReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(0, repReport.getStat(
        ContainerHealthState.OVER_REPLICATED));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());

    // Add the third CLOSED replica
    replicas.add(createContainerReplica(
        ContainerID.valueOf(1), 0, IN_SERVICE,
        ContainerReplicaProto.State.CLOSED, sequenceId));
    storeContainerAndReplicas(container, replicas);

    replicationManager.processContainer(container, repQueue, repReport);
    assertEquals(1, repReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(1, repReport.getStat(
        ContainerHealthState.OVER_REPLICATED));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(1, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testHealthyContainer() throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4, 5);

    replicationManager.processContainer(
        container, repQueue, repReport);
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testHealthyContainerStatus() throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4, 5);

    boolean result = replicationManager.checkContainerStatus(
        container, repReport);
    assertFalse(result);
  }

  @Test
  public void testUnderReplicatedContainer() throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4);

    replicationManager.processContainer(
        container, repQueue, repReport);
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, repReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedContainerStatus()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4);

    boolean result = replicationManager.checkContainerStatus(
        container, repReport);
    assertEquals(1, repReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertTrue(result);
  }

  /**
   * {@link
   * ReplicationManager#getContainerReplicationHealth(ContainerInfo, Set)}
   * should return under replicated result for an under replicated container.
   */
  @Test
  public void testGetContainerReplicationHealthForUnderReplicatedContainer() {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas =
        addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4);

    ContainerHealthResult result =
        replicationManager.getContainerReplicationHealth(container, replicas);
    assertEquals(ContainerHealthResult.HealthState.UNDER_REPLICATED,
        result.getHealthState());

    // Test the same for a RATIS container
    RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    container = createContainerInfo(ratisRepConfig, 1L,
        HddsProtos.LifeCycleState.CLOSED);
    replicas = addReplicas(container, ContainerReplicaProto.State.CLOSED, 0,
        0);

    result =
        replicationManager.getContainerReplicationHealth(container, replicas);
    assertEquals(ContainerHealthResult.HealthState.UNDER_REPLICATED,
        result.getHealthState());
  }

  @Test
  public void testUnderReplicatedContainerFixedByPending()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4);
    containerReplicaPendingOps.scheduleAddReplica(container.containerID(),
        MockDatanodeDetails.randomDatanodeDetails(), 5, null,
        clock.millis() + 10000, 5L, clock.millis());

    replicationManager.processContainer(
        container, repQueue, repReport);
    // As the pending replication fixes the under replication, nothing is added
    // to the under replication list.
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    // As the container is still under replicated, as the pending have not
    // completed yet, the container is still marked as under-replicated in the
    // report.
    assertEquals(1, repReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedAndUnrecoverable()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2);

    replicationManager.processContainer(
        container, repQueue, repReport);
    // If it is unrecoverable, there is no point in putting it into the under
    // replication list. It will be checked again on the next RM run.
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, repReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(1, repReport.getStat(
        ContainerHealthState.MISSING));
  }

  @Test
  public void testUnrecoverableAndEmpty()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);

    ContainerReplica replica  = createContainerReplica(container.containerID(),
        1, IN_SERVICE, ContainerReplicaProto.State.CLOSED,
        0, 0, MockDatanodeDetails.randomDatanodeDetails(), DatanodeID.randomID());

    storeContainerAndReplicas(container, Collections.singleton(replica));

    replicationManager.processContainer(container, repQueue, repReport);
    // If it is unrecoverable, there is no point in putting it into the under
    // replication list. It will be checked again on the next RM run.
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, repReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(0, repReport.getStat(
        ContainerHealthState.MISSING));
    // As it is marked empty in the report, it must have gone through the
    // empty container handler, indicating is was handled as empty.
    assertEquals(1, repReport.getStat(
        ContainerHealthState.EMPTY));
  }


  /**
   * A closed EC container with 3 closed and 2 unhealthy replicas is under
   * replicated. RM should add it to under replicated queue.
   */
  @Test
  public void testUnderReplicatedClosedContainerWithUnhealthyReplicas()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas = addReplicas(container,
        ContainerReplicaProto.State.CLOSED, 1, 2, 3);
    ContainerReplica unhealthyReplica1 =
        createContainerReplica(container.containerID(), 4,
            IN_SERVICE, ContainerReplicaProto.State.UNHEALTHY);
    ContainerReplica unhealthyReplica2 =
        createContainerReplica(container.containerID(), 5,
            IN_SERVICE, ContainerReplicaProto.State.UNHEALTHY);
    replicas.add(unhealthyReplica1);
    replicas.add(unhealthyReplica2);

    replicationManager.processContainer(
        container, repQueue, repReport);

    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, repReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  /**
   * A closed EC container with 2 closed and 3 unhealthy replicas is
   * unrecoverable. It should not be queued to under replicated queue but
   * should be recorded as missing (currently, we're calling an unrecoverable
   * EC container missing).
   */
  @Test
  public void testUnrecoverableClosedContainerWithUnhealthyReplicas()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas = addReplicas(container,
        ContainerReplicaProto.State.UNHEALTHY, 3, 4, 5);
    ContainerReplica closedReplica1 =
        createContainerReplica(container.containerID(), 1,
            IN_SERVICE, ContainerReplicaProto.State.CLOSED);
    ContainerReplica closedReplica2 =
        createContainerReplica(container.containerID(), 2,
            IN_SERVICE, ContainerReplicaProto.State.CLOSED);
    replicas.add(closedReplica1);
    replicas.add(closedReplica2);

    replicationManager.processContainer(
        container, repQueue, repReport);

    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, repReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(0, repReport.getStat(
        ContainerHealthState.MISSING));
    assertEquals(1, repReport.getStat(
        ContainerHealthState.UNHEALTHY));
  }

  /**
   * A closed EC container with all healthy replicas and 1 extra unhealthy
   * replica. It should be logged as over replicated, but not added to the over
   * replication queue, as the unhealthy replica will be removed by the handler
   * directly.
   */
  @Test
  public void testPerfectlyReplicatedWithUnhealthyReplica()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas = addReplicas(container,
        ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4, 5);
    ContainerReplica unhealthyReplica =
        createContainerReplica(container.containerID(), 1,
            IN_SERVICE, ContainerReplicaProto.State.UNHEALTHY);
    replicas.add(unhealthyReplica);

    replicationManager.processContainer(
        container, repQueue, repReport);

    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    // Perfectly replicated with extra unhealthy replica
    // Handler may not process this depending on replication config
    assertEquals(0, repReport.getStat(
        ContainerHealthState.MISSING));
  }

  @Test
  public void
      testUnderReplicatedClosedContainerWithUnHealthyAndClosingReplicas()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas = addReplicas(container,
        ContainerReplicaProto.State.CLOSED, 1, 2, 3);
    ContainerReplica unhealthyReplica1 =
        createContainerReplica(container.containerID(), 4,
            IN_SERVICE, ContainerReplicaProto.State.UNHEALTHY);
    ContainerReplica unhealthyReplica2 =
        createContainerReplica(container.containerID(), 5,
            IN_SERVICE, ContainerReplicaProto.State.CLOSING);
    replicas.add(unhealthyReplica1);
    replicas.add(unhealthyReplica2);

    replicationManager.processContainer(
        container, repQueue, repReport);

    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, repReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderAndOverReplicated()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 5, 5);

    replicationManager.processContainer(
        container, repQueue, repReport);
    // If it is both under and over replicated, we set it to the most important
    // state, which is under-replicated. When that is fixed, over replication
    // will be handled.
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, repReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(0, repReport.getStat(
        ContainerHealthState.OVER_REPLICATED));
  }

  /**
   * Situation: CLOSED EC container with 3 CLOSED replicas and 2 UNHEALTHY
   * replicas. This is under replication. Mocked such that the placement
   * policy throws an exception saying no target datanodes found.
   * <p>
   * Tests that EC under replication handling tries to delete an UNHEALTHY
   * replica if no target datanodes are found. It should delete only one
   * UNHEALTHY replica so that the replica's host DN becomes available as a
   * target for reconstruction/replication of a healthy replica.
   */
  @Test
  public void testUnderReplicationBlockedByUnhealthyReplicas()
      throws IOException, NodeNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas =
        addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3);
    ContainerReplica unhealthyReplica1 =
        createContainerReplica(container.containerID(), 1, IN_SERVICE,
            ContainerReplicaProto.State.UNHEALTHY);
    ContainerReplica unhealthyReplica4 =
        createContainerReplica(container.containerID(), 4, IN_SERVICE,
            ContainerReplicaProto.State.UNHEALTHY);
    replicas.add(unhealthyReplica4);
    replicas.add(unhealthyReplica1);

    // assert that this container is seen as under replicated
    replicationManager.processContainer(container, repQueue, repReport);
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, repReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(0, repReport.getStat(
        ContainerHealthState.OVER_REPLICATED));

    // now, pass this container to ec under replication handling
    when(nodeManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenReturn(NodeStatus.inServiceHealthy());
    ECUnderReplicationHandler handler = new ECUnderReplicationHandler(
        getNoNodesTestPlacementPolicy(nodeManager, configuration),
        configuration, replicationManager);

    // an exception should be thrown so that this container is queued again
    assertThrows(SCMException.class,
        () -> handler.processAndSendCommands(replicas,
            containerReplicaPendingOps.getPendingOps(container.containerID()),
            repQueue.dequeueUnderReplicatedContainer(), 1));
    // a delete command should also have been sent for UNHEALTHY replica of
    // index 1
    assertEquals(1, commandsSent.size());
    Pair<DatanodeID, SCMCommand<?>> command = commandsSent.iterator().next();
    assertEquals(SCMCommandProto.Type.deleteContainerCommand,
        command.getValue().getType());
    DeleteContainerCommand deleteCommand =
        (DeleteContainerCommand) command.getValue();
    assertEquals(unhealthyReplica1.getDatanodeDetails().getID(),
        command.getKey());
    assertEquals(container.containerID(),
        ContainerID.valueOf(deleteCommand.getContainerID()));
    assertEquals(unhealthyReplica1.getReplicaIndex(),
        deleteCommand.getReplicaIndex());
  }

  @Test
  public void testOverReplicated() throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED,
        1, 2, 3, 4, 5, 5);
    replicationManager.processContainer(
        container, repQueue, repReport);
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(1, repQueue.overReplicatedQueueSize());
    assertEquals(1, repReport.getStat(
        ContainerHealthState.OVER_REPLICATED));
  }

  @Test
  public void testOverReplicatedFixByPending()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED,
        1, 2, 3, 4, 5, 5);
    containerReplicaPendingOps.scheduleDeleteReplica(container.containerID(),
        MockDatanodeDetails.randomDatanodeDetails(), 5, null,
        clock.millis() + 10000);
    replicationManager.processContainer(
        container, repQueue, repReport);
    assertEquals(0, repQueue.underReplicatedQueueSize());
    // If the pending replication fixes the over-replication, nothing is added
    // to the over replication list.
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, repReport.getStat(
        ContainerHealthState.OVER_REPLICATED));
  }

  @Test
  public void testMisReplicatedECContainer() throws IOException {
    when(ecPlacementPolicy.validateContainerPlacement(anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(4, 5, 5));

    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4, 5);

    replicationManager.processContainer(container, repQueue, repReport);
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, repReport.getStat(
        ContainerHealthState.MIS_REPLICATED));
    assertEquals(0, repReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(0, repReport.getStat(
        ContainerHealthState.OVER_REPLICATED));
  }

  /**
   * Consider an EC container with 5 closed and 1 unhealthy replica.
   * Assume this container is also mis replicated because it's on
   * insufficient racks. For such EC containers, RM should first delete the
   * unhealthy replica and then solve mis replication.
   */
  @Test
  public void testMisReplicatedECContainerWithUnhealthyReplica()
      throws ContainerNotFoundException {
    when(ecPlacementPolicy.validateContainerPlacement(anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(5, 5, 5, 1,
            Lists.newArrayList(2, 1, 1, 1, 1)));

    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas =
        addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4,
            5);
    ContainerReplica unhealthyReplica =
        createContainerReplica(container.containerID(), 1, IN_SERVICE,
            ContainerReplicaProto.State.UNHEALTHY);
    replicas.add(unhealthyReplica);
    storeContainerAndReplicas(container, replicas);

    replicationManager.processContainer(container, repQueue, repReport);
    assertEquals(0, repQueue.underReplicatedQueueSize());
    /*
     Does not get queued as over replicated because a delete command is sent
     directly for the unhealthy replica.
     */
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, repReport.getStat(
        ContainerHealthState.MIS_REPLICATED));
    // EC container with unhealthy replica is handled by ClosedWithUnhealthyReplicasHandler
    // which sets UNHEALTHY_OVER_REPLICATED
    assertEquals(1, repReport.getStat(
        ContainerHealthState.UNHEALTHY_OVER_REPLICATED));
    List<ContainerReplicaOp> ops =
        containerReplicaPendingOps.getPendingOps(container.containerID());
    verify(nodeManager).addDatanodeCommand(any(), any());
    assertEquals(1, ops.size());
    assertEquals(ContainerReplicaOp.PendingOpType.DELETE,
        ops.get(0).getOpType());
    assertEquals(unhealthyReplica.getDatanodeDetails(),
        ops.get(0).getTarget());
    assertEquals(1, ops.get(0).getReplicaIndex());
    assertEquals(1, replicationManager.getMetrics()
        .getEcDeletionCmdsSentTotal());
    assertEquals(0, replicationManager.getMetrics()
        .getDeletionCmdsSentTotal());

    /*
    Now, remove the unhealthy replica. This leaves 5 replicas on 4 racks,
    which is mis replication. RM should queue this container as mis
    replicated now.
     */
    replicas.remove(unhealthyReplica);
    containerReplicaPendingOps.completeDeleteReplica(container.containerID(),
        unhealthyReplica.getDatanodeDetails(), 1);
    when(ecPlacementPolicy.validateContainerPlacement(anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(4, 5, 5, 1,
            Lists.newArrayList(2, 1, 1, 1)));

    repReport = new ReplicationManagerReport(rmConf.getContainerSampleLimit());
    replicationManager.processContainer(container, repQueue, repReport);
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, repReport.getStat(
        ContainerHealthState.MIS_REPLICATED));
    assertEquals(0, repReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(0, repReport.getStat(
        ContainerHealthState.OVER_REPLICATED));
  }

  @Test
  public void testUnderReplicationQueuePopulated() {
    // Make it always return mis-replicated. Only a perfectly replicated
    // container should make it the mis-replicated state as under / over
    // replicated take precedence.
    when(ecPlacementPolicy.validateContainerPlacement(anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(1, 2, 3));

    ContainerInfo decomContainer = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(decomContainer, ContainerReplicaProto.State.CLOSED,
        Pair.of(DECOMMISSIONING, 1),
        Pair.of(DECOMMISSIONING, 2), Pair.of(DECOMMISSIONING, 3),
        Pair.of(DECOMMISSIONING, 4), Pair.of(DECOMMISSIONING, 5));

    ContainerInfo underRep1 = createContainerInfo(repConfig, 2,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(underRep1, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4);
    ContainerInfo underRep0 = createContainerInfo(repConfig, 3,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(underRep0, ContainerReplicaProto.State.CLOSED, 1, 2, 3);

    ContainerInfo misRep = createContainerInfo(repConfig, 4,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(misRep, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4, 5);

    enableProcessAll();
    replicationManager.processAll();

    ReplicationQueue queue = replicationManager.getQueue();

    // Get the first message off the queue - it should be underRep0.
    ContainerHealthResult.UnderReplicatedHealthResult res
        = queue.dequeueUnderReplicatedContainer();
    assertEquals(underRep0, res.getContainerInfo());

    // Now requeue it
    queue.enqueue(res);

    // Now get the next message. It should be underRep1, as it has remaining
    // redundancy 1 + zero retries. UnderRep0 will have remaining redundancy 0
    // and 1 retry. They will have the same weighted redundancy so lesser
    // retries should come first
    res = queue.dequeueUnderReplicatedContainer();
    assertEquals(underRep1, res.getContainerInfo());

    // Next message is underRep0. It starts with a weighted redundancy of 0 + 1
    // retry. The other message on the queue is a decommission only with a
    // weighted redundancy of 5 + 0. So lets dequeue and requeue the message 4
    // times. Then the weighted redundancy will be equal and the decommission
    // one will be next due to having less retries.
    for (int i = 0; i < 4; i++) {
      res = queue.dequeueUnderReplicatedContainer();
      assertEquals(underRep0, res.getContainerInfo());
      queue.enqueue(res);
    }
    res = queue.dequeueUnderReplicatedContainer();
    assertEquals(decomContainer, res.getContainerInfo());

    res = queue.dequeueUnderReplicatedContainer();
    assertEquals(underRep0, res.getContainerInfo());

    // Next is the mis-rep container, which has a remaining redundancy of 6.
    res = queue.dequeueUnderReplicatedContainer();
    assertEquals(misRep, res.getContainerInfo());

    res = queue.dequeueUnderReplicatedContainer();
    assertNull(res);
  }

  @Test
  public void testSendDatanodeDeleteCommand() throws NotLeaderException {
    ECReplicationConfig ecRepConfig = new ECReplicationConfig(3, 2);
    ContainerInfo containerInfo =
        ReplicationTestUtil.createContainerInfo(ecRepConfig, 1,
        HddsProtos.LifeCycleState.CLOSED, 10, 20);
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();

    DeleteContainerCommand deleteContainerCommand = new DeleteContainerCommand(
        containerInfo.getContainerID());
    deleteContainerCommand.setReplicaIndex(1);

    replicationManager.sendDatanodeCommand(deleteContainerCommand,
        containerInfo, target);

    List<ContainerReplicaOp> ops = containerReplicaPendingOps.getPendingOps(
        containerInfo.containerID());
    verify(nodeManager).addDatanodeCommand(any(), any());
    assertEquals(1, ops.size());
    assertEquals(ContainerReplicaOp.PendingOpType.DELETE,
        ops.get(0).getOpType());
    assertEquals(target, ops.get(0).getTarget());
    assertEquals(1, ops.get(0).getReplicaIndex());
    assertEquals(1, replicationManager.getMetrics()
            .getEcDeletionCmdsSentTotal());
    assertEquals(0, replicationManager.getMetrics()
        .getDeletionCmdsSentTotal());

    // Repeat with Ratis container, as different metrics should be incremented
    clearInvocations(nodeManager);
    RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    containerInfo = ReplicationTestUtil.createContainerInfo(ratisRepConfig, 2,
            HddsProtos.LifeCycleState.CLOSED, 10, 20);

    deleteContainerCommand = new DeleteContainerCommand(
        containerInfo.getContainerID());
    replicationManager.sendDatanodeCommand(deleteContainerCommand,
        containerInfo, target);

    ops = containerReplicaPendingOps.getPendingOps(containerInfo.containerID());
    verify(nodeManager).addDatanodeCommand(any(), any());
    assertEquals(1, ops.size());
    assertEquals(ContainerReplicaOp.PendingOpType.DELETE,
        ops.get(0).getOpType());
    assertEquals(target, ops.get(0).getTarget());
    assertEquals(0, ops.get(0).getReplicaIndex());
    assertEquals(1, replicationManager.getMetrics()
        .getEcDeletionCmdsSentTotal());
    assertEquals(1, replicationManager.getMetrics()
        .getDeletionCmdsSentTotal());
    assertEquals(20, replicationManager.getMetrics()
        .getDeletionBytesTotal());
  }

  @Test
  public void testSendDatanodeReconstructCommand() throws NotLeaderException {
    ECReplicationConfig ecRepConfig = new ECReplicationConfig(3, 2);
    ContainerInfo containerInfo =
        ReplicationTestUtil.createContainerInfo(ecRepConfig, 1,
            HddsProtos.LifeCycleState.CLOSED, 10, 20);

    List<ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex>
        sourceNodes = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      sourceNodes.add(
          new ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex(
              MockDatanodeDetails.randomDatanodeDetails(), i));
    }
    List<DatanodeDetails> targetNodes = new ArrayList<>();
    DatanodeDetails target4 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails target5 = MockDatanodeDetails.randomDatanodeDetails();
    targetNodes.add(target4);
    targetNodes.add(target5);
    byte[] missingIndexes = {4, 5};

    ReconstructECContainersCommand command = new ReconstructECContainersCommand(
        containerInfo.getContainerID(), sourceNodes, targetNodes,
        ProtoUtils.unsafeByteString(missingIndexes), ecRepConfig);

    replicationManager.sendDatanodeCommand(command, containerInfo, target4);

    List<ContainerReplicaOp> ops = containerReplicaPendingOps.getPendingOps(
        containerInfo.containerID());
    verify(nodeManager).addDatanodeCommand(any(), any());
    assertEquals(2, ops.size());
    Set<DatanodeDetails> cmdTargets = new HashSet<>();
    Set<Integer> cmdIndexes = new HashSet<>();
    for (ContainerReplicaOp op : ops) {
      assertEquals(ADD, op.getOpType());
      cmdTargets.add(op.getTarget());
      cmdIndexes.add(op.getReplicaIndex());
    }
    assertEquals(2, cmdTargets.size());
    for (DatanodeDetails dn : targetNodes) {
      assertThat(cmdTargets).contains(dn);
    }

    assertEquals(2, cmdIndexes.size());
    for (int i : missingIndexes) {
      assertThat(cmdIndexes).contains(i);
    }
    assertEquals(1, replicationManager.getMetrics()
        .getEcReconstructionCmdsSentTotal());
  }

  @Test
  public void testSendDatanodeReplicateCommand() throws NotLeaderException {
    ECReplicationConfig ecRepConfig = new ECReplicationConfig(3, 2);
    ContainerInfo containerInfo =
        ReplicationTestUtil.createContainerInfo(ecRepConfig, 1,
            HddsProtos.LifeCycleState.CLOSED, 10, 20);
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();

    List<DatanodeDetails> sources = new ArrayList<>();
    sources.add(MockDatanodeDetails.randomDatanodeDetails());
    sources.add(MockDatanodeDetails.randomDatanodeDetails());


    ReplicateContainerCommand command = ReplicateContainerCommand.fromSources(
        containerInfo.getContainerID(), sources);
    command.setReplicaIndex(1);

    replicationManager.sendDatanodeCommand(command, containerInfo, target);

    // Ensure that the command deadline is set to current time
    // + evenTime * factor
    long expectedDeadline = clock.millis() + rmConf.getEventTimeout() -
            rmConf.getDatanodeTimeoutOffset();
    assertEquals(expectedDeadline, command.getDeadline());

    List<ContainerReplicaOp> ops = containerReplicaPendingOps.getPendingOps(
        containerInfo.containerID());
    verify(nodeManager).addDatanodeCommand(any(), any());
    assertEquals(1, ops.size());
    assertEquals(ContainerReplicaOp.PendingOpType.ADD,
        ops.get(0).getOpType());
    assertEquals(target, ops.get(0).getTarget());
    assertEquals(1, ops.get(0).getReplicaIndex());
    assertEquals(1, replicationManager.getMetrics()
        .getEcReplicationCmdsSentTotal());
    assertEquals(0, replicationManager.getMetrics()
        .getReplicationCmdsSentTotal());

    // Repeat with Ratis container, as different metrics should be incremented
    clearInvocations(nodeManager);
    RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    containerInfo = ReplicationTestUtil.createContainerInfo(ratisRepConfig, 2,
        HddsProtos.LifeCycleState.CLOSED, 10, 20);

    command = ReplicateContainerCommand.fromSources(
        containerInfo.getContainerID(), sources);
    replicationManager.sendDatanodeCommand(command, containerInfo, target);

    ops = containerReplicaPendingOps.getPendingOps(containerInfo.containerID());
    verify(nodeManager).addDatanodeCommand(any(), any());
    assertEquals(1, ops.size());
    assertEquals(ContainerReplicaOp.PendingOpType.ADD,
        ops.get(0).getOpType());
    assertEquals(target, ops.get(0).getTarget());
    assertEquals(0, ops.get(0).getReplicaIndex());
    assertEquals(1, replicationManager.getMetrics()
        .getEcReplicationCmdsSentTotal());
    assertEquals(1, replicationManager.getMetrics()
        .getReplicationCmdsSentTotal());
  }

  /**
   * Tests that a ReplicateContainerCommand that is sent from source to
   * target has the correct deadline and that ContainerReplicaOp for
   * replica ADD is created correctly.
   */
  @Test
  public void testReplicateContainerCommandToTarget()
      throws NotLeaderException {
    // create a closed EC container
    ECReplicationConfig ecRepConfig = new ECReplicationConfig(3, 2);
    ContainerInfo containerInfo =
        ReplicationTestUtil.createContainerInfo(ecRepConfig, 1,
            HddsProtos.LifeCycleState.CLOSED, 10, 20);

    // command will be pushed from source to target
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails source = MockDatanodeDetails.randomDatanodeDetails();
    ReplicateContainerCommand command = ReplicateContainerCommand.toTarget(
        containerInfo.getContainerID(), target);
    command.setReplicaIndex(1);
    replicationManager.sendDatanodeCommand(command, containerInfo, source);

    // check the command's deadline
    long expectedDeadline = clock.millis() + rmConf.getEventTimeout() -
        rmConf.getDatanodeTimeoutOffset();
    assertEquals(expectedDeadline, command.getDeadline());

    List<ContainerReplicaOp> ops = containerReplicaPendingOps.getPendingOps(
        containerInfo.containerID());
    verify(nodeManager).addDatanodeCommand(any(), any());
    assertEquals(1, ops.size());
    assertEquals(ContainerReplicaOp.PendingOpType.ADD,
        ops.get(0).getOpType());
    assertEquals(target, ops.get(0).getTarget());
    assertEquals(1, ops.get(0).getReplicaIndex());
    assertEquals(1, replicationManager.getMetrics()
        .getEcReplicationCmdsSentTotal());
    assertEquals(0, replicationManager.getMetrics()
        .getReplicationCmdsSentTotal());
  }

  @Test
  public void testSendLowPriorityReplicateContainerCommand()
      throws NotLeaderException {
    ContainerInfo containerInfo =
        ReplicationTestUtil.createContainerInfo(repConfig, 1,
            HddsProtos.LifeCycleState.CLOSED, 10, 20);
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails src = MockDatanodeDetails.randomDatanodeDetails();

    long scmDeadline = clock.millis() + rmConf.getEventTimeout();
    long datanodeDeadline = scmDeadline - rmConf.getDatanodeTimeoutOffset();

    replicationManager.sendLowPriorityReplicateContainerCommand(containerInfo,
        0, src, target, scmDeadline);

    ArgumentCaptor<SCMCommand<?>> command =
        ArgumentCaptor.forClass(SCMCommand.class);
    ArgumentCaptor<DatanodeID> targetUUID = ArgumentCaptor.forClass(DatanodeID.class);
    verify(nodeManager).addDatanodeCommand(targetUUID.capture(), command.capture());

    ReplicateContainerCommand sentCommand =
        (ReplicateContainerCommand)command.getValue();
    assertEquals(datanodeDeadline, sentCommand.getDeadline());
    assertEquals(LOW, sentCommand.getPriority());
    assertEquals(src.getID(), targetUUID.getValue());
    assertEquals(target, sentCommand.getTargetDatanode());
  }

  @Test
  public void testSendThrottledReplicateContainerCommand()
      throws CommandTargetOverloadedException, NodeNotFoundException,
      NotLeaderException {
    Map<DatanodeDetails, Integer> sourceNodes = new HashMap<>();
    DatanodeDetails cmdTarget = MockDatanodeDetails.randomDatanodeDetails();
    sourceNodes.put(cmdTarget, 0);
    for (int i = 1; i < 3; i++) {
      sourceNodes.put(MockDatanodeDetails.randomDatanodeDetails(), i * 5);
    }
    mockReplicationCommandCounts(sourceNodes::get, any -> 0);

    testReplicationCommand(cmdTarget, sourceNodes.keySet(), 0,
        MockDatanodeDetails.randomDatanodeDetails());
    assertEquals(0, replicationManager.getMetrics()
        .getReplicateContainerCmdsDeferredTotal());
  }

  @Test
  public void sendsReplicateCommandToMaintenanceNode()
      throws CommandTargetOverloadedException, NodeNotFoundException,
      NotLeaderException {
    // All nodes are over the limit, but one of them is entering maintenance
    int limit = replicationManager.getConfig().getDatanodeReplicationLimit();
    Map<DatanodeDetails, Integer> sourceNodes = new HashMap<>();

    DatanodeDetails cmdTarget = MockDatanodeDetails.randomDatanodeDetails();
    cmdTarget.setPersistedOpState(ENTERING_MAINTENANCE);
    sourceNodes.put(cmdTarget, limit + 2);

    for (int i = 1; i < 3; i++) {
      sourceNodes.put(MockDatanodeDetails.randomDatanodeDetails(), limit + 1);
    }

    mockReplicationCommandCounts(sourceNodes::get, any -> 0);

    testReplicationCommand(cmdTarget, sourceNodes.keySet(), 0,
        MockDatanodeDetails.randomDatanodeDetails());
  }

  private void testReplicationCommand(
      DatanodeDetails expectedTarget, Set<DatanodeDetails> sourceNodes,
      int replicaIndex, DatanodeDetails destination)
      throws CommandTargetOverloadedException, NotLeaderException {
    ContainerInfo container = ReplicationTestUtil.createContainerInfo(
        repConfig, 1, HddsProtos.LifeCycleState.CLOSED, 10, 20);

    replicationManager.sendThrottledReplicationCommand(
        container, new ArrayList<>(sourceNodes), destination, replicaIndex);

    assertEquals(1, commandsSent.size());
    Pair<DatanodeID, SCMCommand<?>> cmdWithTarget = commandsSent.iterator().next();
    assertEquals(expectedTarget.getID(), cmdWithTarget.getLeft());
    assertEquals(ReplicateContainerCommand.class,
        cmdWithTarget.getRight().getClass());
    ReplicateContainerCommand cmd =
        (ReplicateContainerCommand) cmdWithTarget.getRight();
    assertEquals(destination, cmd.getTargetDatanode());
    assertEquals(replicaIndex, cmd.getReplicaIndex());
  }

  @Test
  public void testSendThrottledReplicateContainerCommandThrowsWhenNoSources()
      throws NodeNotFoundException {
    // Reconstruction commands also count toward the limit, so set things up
    // so that the nodes are at the limit caused by 1 reconstruction command
    // and the remaining replication commands
    int limit = replicationManager.getConfig().getDatanodeReplicationLimit();
    int reconstructionWeight = replicationManager.getConfig()
        .getReconstructionCommandWeight();
    int reconstructionCount = 1;
    int replicationCount = limit - reconstructionCount * reconstructionWeight;
    List<DatanodeDetails> sourceNodes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      sourceNodes.add(MockDatanodeDetails.randomDatanodeDetails());
    }

    mockReplicationCommandCounts(any -> replicationCount,
        any -> reconstructionCount);

    DatanodeDetails destination = MockDatanodeDetails.randomDatanodeDetails();
    ContainerInfo container = ReplicationTestUtil.createContainerInfo(
        repConfig, 1, HddsProtos.LifeCycleState.CLOSED, 10, 20);

    long overLoadedCount = replicationManager.getMetrics()
        .getReplicateContainerCmdsDeferredTotal();
    assertThrows(CommandTargetOverloadedException.class,
        () -> replicationManager.sendThrottledReplicationCommand(
            container, sourceNodes, destination, 0));
    assertEquals(overLoadedCount + 1, replicationManager.getMetrics()
        .getReplicateContainerCmdsDeferredTotal());
  }

  @Test
  public void testSendThrottledReconstructionCommand()
      throws CommandTargetOverloadedException, NodeNotFoundException,
      NotLeaderException {
    Map<DatanodeDetails, Integer> targetNodes = new HashMap<>();
    DatanodeDetails cmdTarget = MockDatanodeDetails.randomDatanodeDetails();
    targetNodes.put(cmdTarget, 0);
    targetNodes.put(MockDatanodeDetails.randomDatanodeDetails(), 5);

    mockReplicationCommandCounts(targetNodes::get, any -> 0);

    ContainerInfo container = ReplicationTestUtil.createContainerInfo(
        repConfig, 1, HddsProtos.LifeCycleState.CLOSED, 10, 20);

    ReconstructECContainersCommand command = createReconstructionCommand(
        container, targetNodes.keySet().toArray(new DatanodeDetails[0]));

    replicationManager.sendThrottledReconstructionCommand(container, command);

    assertEquals(1, commandsSent.size());
    Pair<DatanodeID, SCMCommand<?>> cmd = commandsSent.iterator().next();
    assertEquals(cmdTarget.getID(), cmd.getLeft());
    assertEquals(0, replicationManager.getMetrics()
        .getEcReconstructionCmdsDeferredTotal());
  }

  @Test
  public void testSendThrottledReconstructionCommandThrowsWhenNoTargets()
      throws NodeNotFoundException {
    int limit = replicationManager.getConfig().getDatanodeReplicationLimit();
    int reconstructionWeight = replicationManager.getConfig()
        .getReconstructionCommandWeight();

    // We want to test that Replication commands also count toward the limit,
    // and also that the weight is applied the the reconstruction count.
    // Using the values below will set the targets at their limit.
    int reconstructionCount = 2;
    int replicationCount = limit - reconstructionCount * reconstructionWeight;

    mockReplicationCommandCounts(any -> replicationCount,
        any -> reconstructionCount);

    ContainerInfo container = ReplicationTestUtil.createContainerInfo(
        repConfig, 1, HddsProtos.LifeCycleState.CLOSED, 10, 20);
    ReconstructECContainersCommand command = createReconstructionCommand(
        container, MockDatanodeDetails.randomDatanodeDetails(),
        MockDatanodeDetails.randomDatanodeDetails());
    long overLoadedCount = replicationManager.getMetrics()
        .getEcReconstructionCmdsDeferredTotal();
    assertThrows(CommandTargetOverloadedException.class,
        () -> replicationManager.sendThrottledReconstructionCommand(
            container, command));
    assertEquals(overLoadedCount + 1, replicationManager.getMetrics()
        .getEcReconstructionCmdsDeferredTotal());
  }

  private ReconstructECContainersCommand createReconstructionCommand(
      ContainerInfo containerInfo, DatanodeDetails... targets) {
    List<ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex> sources
        = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      sources.add(
          new ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex(
              MockDatanodeDetails.randomDatanodeDetails(), i));
    }
    byte[] missingIndexes = new byte[]{4, 5};
    return new ReconstructECContainersCommand(
        containerInfo.getContainerID(), sources,
        Arrays.asList(targets), ProtoUtils.unsafeByteString(missingIndexes),
        (ECReplicationConfig) repConfig);
  }

  @Test
  public void testCreateThrottledDeleteContainerCommand()
      throws CommandTargetOverloadedException, NodeNotFoundException,
      NotLeaderException {
    when(nodeManager.getTotalDatanodeCommandCount(any(),
            eq(SCMCommandProto.Type.deleteContainerCommand)))
        .thenAnswer(invocation -> 0);

    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    ContainerInfo container = ReplicationTestUtil.createContainerInfo(
        repConfig, 1, HddsProtos.LifeCycleState.CLOSED, 10, 20);
    replicationManager.sendThrottledDeleteCommand(container, 1, target, true);
    assertEquals(commandsSent.size(), 1);
    assertEquals(0, replicationManager.getMetrics()
        .getDeleteContainerCmdsDeferredTotal());
  }

  @Test
  public void testCreateThrottledDeleteContainerCommandThrowsWhenNoSources()
      throws NodeNotFoundException {
    int limit = replicationManager.getConfig().getDatanodeDeleteLimit();

    when(nodeManager.getTotalDatanodeCommandCount(any(),
            eq(SCMCommandProto.Type.deleteContainerCommand)))
        .thenAnswer(invocation -> limit + 1);

    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    ContainerInfo container = ReplicationTestUtil.createContainerInfo(
        repConfig, 1, HddsProtos.LifeCycleState.CLOSED, 10, 20);
    long overLoadedCount = replicationManager.getMetrics()
        .getDeleteContainerCmdsDeferredTotal();
    assertThrows(CommandTargetOverloadedException.class,
        () -> replicationManager.sendThrottledDeleteCommand(
            container, 1, target, true));
    assertEquals(overLoadedCount + 1, replicationManager.getMetrics()
        .getDeleteContainerCmdsDeferredTotal());
  }

  @Test
  public void testExcludedNodes() throws NodeNotFoundException,
      NotLeaderException, CommandTargetOverloadedException {
    int repLimit = replicationManager.getConfig().getDatanodeReplicationLimit();
    int reconstructionWeight = replicationManager.getConfig()
        .getReconstructionCommandWeight();
    ContainerInfo container = ReplicationTestUtil.createContainerInfo(
        repConfig, 1, HddsProtos.LifeCycleState.CLOSED, 10, 20);
    Map<DatanodeDetails, Integer> commandCounts = new HashMap<>();
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails dn2 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails dn3 = MockDatanodeDetails.randomDatanodeDetails();

    commandCounts.put(dn1, repLimit - 1);
    commandCounts.put(dn2, repLimit - reconstructionWeight);
    commandCounts.put(dn3, repLimit);

    mockReplicationCommandCounts(commandCounts::get, any -> 0);

    replicationManager.sendThrottledReplicationCommand(container,
        new ArrayList<>(commandCounts.keySet()),
        MockDatanodeDetails.randomDatanodeDetails(), 1);

    Set<DatanodeDetails> excluded = replicationManager.getExcludedNodes();
    assertEquals(1, excluded.size());
    // dn 3 was at the limit already, so should be added when filtering the
    // nodes
    assertThat(excluded).contains(dn3);

    // Trigger an update for dn3, but it should stay in the excluded list as its
    // count is still at the limit.
    replicationManager.datanodeCommandCountUpdated(dn3);
    assertEquals(replicationManager.getExcludedNodes().size(), 1);

    // Starting maintenance on dn3 increases its limits, so it should no longer
    // be excluded
    dn3.setPersistedOpState(ENTERING_MAINTENANCE);
    replicationManager.datanodeCommandCountUpdated(dn3);
    assertEquals(0, replicationManager.getExcludedNodes().size());

    // now sent a reconstruction command. It should be sent to dn2, which is
    // at the lowest count, but this command should push it to the limit and
    // cause it to be excluded.
    ReconstructECContainersCommand command = createReconstructionCommand(
        container, dn1, dn2);
    replicationManager.sendThrottledReconstructionCommand(container, command);
    excluded = replicationManager.getExcludedNodes();
    assertEquals(1, excluded.size());
    // dn 2 reached the limit from the reconstruction command
    assertThat(excluded).contains(dn2);

    // Update received for DN2, it should be cleared from the excluded list.
    replicationManager.datanodeCommandCountUpdated(dn2);
    excluded = replicationManager.getExcludedNodes();
    assertEquals(0, excluded.size());

    // Finally, update received for DN1 - it is not excluded and should not
    // be added or cause any problems by not being there
    replicationManager.datanodeCommandCountUpdated(dn1);
    assertEquals(0, excluded.size());
  }

  @Test
  public void testInflightReplicationLimit() throws IOException {
    int healthyNodes = 10;
    ReplicationManager.ReplicationManagerConfiguration config =
        new ReplicationManager.ReplicationManagerConfiguration();
    when(nodeManager.getNodeCount(isNull(), eq(HddsProtos.NodeState.HEALTHY)))
        .thenReturn(healthyNodes);

    rmConf.setInflightReplicationLimitFactor(0.0);
    ReplicationManager rm = createReplicationManager();
    assertEquals(0, rm.getReplicationInFlightLimit());

    rmConf.setInflightReplicationLimitFactor(1);
    rm = createReplicationManager();
    assertEquals(
        healthyNodes * config.getDatanodeReplicationLimit(),
        rm.getReplicationInFlightLimit());

    rmConf.setInflightReplicationLimitFactor(0.75);
    rm = createReplicationManager();
    assertEquals(
        (int) Math.ceil(healthyNodes
            * config.getDatanodeReplicationLimit() * 0.75),
        rm.getReplicationInFlightLimit());
  }

  @Test
  public void testPendingOpExpiry() throws ContainerNotFoundException {
    when(containerManager.getContainer(any()))
        .thenReturn(ReplicationTestUtil.createContainerInfo(repConfig, 1,
            HddsProtos.LifeCycleState.CLOSED, 10, 20));
    // This is just some arbitrary epoch time in the past
    long commandDeadline = 1000;
    SCMCommand<?> command = new DeleteContainerCommand(1L, true);

    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails dn2 = MockDatanodeDetails.randomDatanodeDetails();

    ContainerReplicaOp addOp = new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.ADD,
        dn1,
        1,
        null,
        Long.MAX_VALUE,
        0);
    ContainerReplicaOp delOp = new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.DELETE, dn2, 1, command, commandDeadline, 0);

    replicationManager.opCompleted(addOp, ContainerID.valueOf(1L), false);
    replicationManager.opCompleted(delOp, ContainerID.valueOf(1L), false);
    // No commands should be sent for either of the above ops.
    assertEquals(0, commandsSent.size());

    replicationManager.opCompleted(delOp, ContainerID.valueOf(1L), true);
    assertEquals(1, commandsSent.size());
    Pair<DatanodeID, SCMCommand<?>> sentCommand = commandsSent.iterator().next();
    // The target should be DN2 and the deadline should have been updated from the value set in commandDeadline above
    assertEquals(dn2.getID(), sentCommand.getLeft());
    assertNotEquals(commandDeadline, sentCommand.getRight().getDeadline());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testNotifyNodeStateChangeWakesUpThread(boolean queueIsEmpty) 
      throws IOException, InterruptedException, ReflectiveOperationException, TimeoutException {

    AtomicBoolean processAllCalled = new AtomicBoolean(false);
    ReplicationQueue queue = mock(ReplicationQueue.class);
    when(queue.isEmpty()).thenReturn(queueIsEmpty);
    final ReplicationManager customRM = new ReplicationManager(
        configuration.getObject(ReplicationManager.ReplicationManagerConfiguration.class),
        configuration,
        containerManager,
        ratisPlacementPolicy,
        ecPlacementPolicy,
        eventPublisher,
        scmContext,
        nodeManager,
        clock,
        containerReplicaPendingOps) {
          @Override
          public ReplicationQueue getQueue() {
            return queue;
          }

          @Override
          public synchronized void processAll() {
            processAllCalled.set(true);
          }
        };

    customRM.notifyStatusChanged();
    customRM.start();

    // wait for the thread become TIMED_WAITING
    GenericTestUtils.waitFor(
        () -> customRM.isThreadWaiting(),
        100,
        1000);

    // The processAll method will be called when the ReplicationManager's run
    // method is executed by the replicationMonitor thread.
    assertTrue(processAllCalled.get());
    processAllCalled.set(false);

    assertThat(customRM.notifyNodeStateChange()).isEqualTo(queueIsEmpty);

    GenericTestUtils.waitFor(
        () -> customRM.isThreadWaiting(),
        100,
        1000);

    // If the queue is empty, the processAll method should have been called
    assertEquals(processAllCalled.get(), queueIsEmpty);

    customRM.stop();
  }

  @Test
  public void testReconfigureContainerSampleLimit() {
    // Create 120 under replicated containers
    int totalContainers = 120;
    for (int i = 0; i < totalContainers; i++) {
      ContainerInfo container = createContainerInfo(
          RatisReplicationConfig.getInstance(THREE), i,
          HddsProtos.LifeCycleState.CLOSED);
      containerInfoSet.add(container);

      Set<ContainerReplica> replicas = new HashSet<>();
      replicas.add(createContainerReplica(container.containerID(), 0,
          IN_SERVICE, ContainerReplicaProto.State.CLOSED));
      replicas.add(createContainerReplica(container.containerID(), 0,
          IN_SERVICE, ContainerReplicaProto.State.CLOSED));
      containerReplicaMap.put(container.containerID(), replicas);
    }

    enableProcessAll();

    // First report with default sample limit 100
    replicationManager.processAll();

    ReplicationManagerReport report1 = replicationManager.getContainerReport();
    assertEquals(totalContainers, report1.getStat(
            ContainerHealthState.UNDER_REPLICATED));

    List<ContainerID> sample1 = report1.getSample(
        ContainerHealthState.UNDER_REPLICATED);
    assertEquals(100, report1.getSampleLimit(),
        "First report should have sample limit of 100");
    assertEquals(100, sample1.size(),
        "First report should have 100 samples with initial config");

    // Reconfigure to new limit
    int newLimit = 50;
    rmConf.setContainerSampleLimit(newLimit);

    assertEquals(newLimit, rmConf.getContainerSampleLimit(),
        "Config should be updated to new limit");

    replicationManager.processAll();

    ReplicationManagerReport report2 = replicationManager.getContainerReport();
    assertEquals(totalContainers, report2.getStat(
            ContainerHealthState.UNDER_REPLICATED));

    List<ContainerID> sample2 = report2.getSample(
        ContainerHealthState.UNDER_REPLICATED);
    assertEquals(newLimit, report2.getSampleLimit(),
        "Second report should have sample limit of 50");
    assertEquals(newLimit, sample2.size(),
        "Second report should have 50 samples after reconfiguration");
  }

  @SafeVarargs
  private final Set<ContainerReplica>  addReplicas(ContainerInfo container,
      ContainerReplicaProto.State replicaState,
      Pair<HddsProtos.NodeOperationalState, Integer>... nodes) {
    final Set<ContainerReplica> replicas =
        createReplicas(container.containerID(), replicaState, nodes);
    storeContainerAndReplicas(container, replicas);
    return replicas;
  }

  private Set<ContainerReplica> addReplicas(ContainerInfo container,
      ContainerReplicaProto.State replicaState, int... indexes) {
    final Set<ContainerReplica> replicas =
        createReplicas(container.containerID(), replicaState, indexes);
    storeContainerAndReplicas(container, replicas);
    return replicas;
  }

  private void storeContainerAndReplicas(ContainerInfo container,
      Set<ContainerReplica> replicas) {
    containerReplicaMap.put(container.containerID(), replicas);
    containerInfoSet.add(container);
  }

  private void mockReplicationCommandCounts(
      Function<DatanodeDetails, Integer> replicateCount,
      Function<DatanodeDetails, Integer> reconstructCount
  ) throws NodeNotFoundException {
    when(nodeManager.getTotalDatanodeCommandCounts(any(),
            eq(SCMCommandProto.Type.replicateContainerCommand),
            eq(SCMCommandProto.Type.reconstructECContainersCommand)))
        .thenAnswer(invocation -> {
          Map<SCMCommandProto.Type, Integer> counts = new HashMap<>();
          DatanodeDetails dn = invocation.getArgument(0);
          counts.put(SCMCommandProto.Type.replicateContainerCommand,
              replicateCount.apply(dn));
          counts.put(SCMCommandProto.Type.reconstructECContainersCommand,
              reconstructCount.apply(dn));
          return counts;
        });
  }

}
