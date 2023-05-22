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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ozone.test.TestClock;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

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
import java.util.UUID;
import java.util.function.Function;

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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;

/**
 * Tests for the ReplicationManager.
 */
public class TestReplicationManager {

  private OzoneConfiguration configuration;
  private ReplicationManager replicationManager;
  private LegacyReplicationManager legacyReplicationManager;
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
  private Set<Pair<UUID, SCMCommand<?>>> commandsSent;

  @Before
  public void setup() throws IOException {
    configuration = new OzoneConfiguration();
    configuration.set(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "0s");
    containerManager = Mockito.mock(ContainerManager.class);
    ratisPlacementPolicy = Mockito.mock(PlacementPolicy.class);
    Mockito.when(ratisPlacementPolicy.validateContainerPlacement(anyList(),
        anyInt())).thenReturn(new ContainerPlacementStatusDefault(2, 2, 3));
    ecPlacementPolicy = Mockito.mock(PlacementPolicy.class);
    Mockito.when(ecPlacementPolicy.validateContainerPlacement(
        anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(2, 2, 3));

    scmContext = Mockito.mock(SCMContext.class);

    nodeManager = Mockito.mock(NodeManager.class);
    commandsSent = new HashSet<>();
    eventPublisher = Mockito.mock(EventPublisher.class);
    Mockito.doAnswer(invocation -> {
      commandsSent.add(Pair.of(invocation.getArgument(0),
          invocation.getArgument(1)));
      return null;
    }).when(nodeManager).addDatanodeCommand(any(), any());

    legacyReplicationManager = Mockito.mock(LegacyReplicationManager.class);
    clock = new TestClock(Instant.now(), ZoneId.systemDefault());
    containerReplicaPendingOps =
        new ContainerReplicaPendingOps(configuration, clock);

    Mockito.when(containerManager
        .getContainerReplicas(Mockito.any(ContainerID.class))).thenAnswer(
          invocation -> {
            ContainerID cid = invocation.getArgument(0);
            return containerReplicaMap.get(cid);
          });

    Mockito.when(containerManager.getContainers()).thenAnswer(
        invocation -> new ArrayList<>(containerInfoSet));
    replicationManager = createReplicationManager();
    containerReplicaMap = new HashMap<>();
    containerInfoSet = new HashSet<>();
    repConfig = new ECReplicationConfig(3, 2);
    repReport = new ReplicationManagerReport();
    repQueue = new ReplicationQueue();

    // Ensure that RM will run when asked.
    Mockito.when(scmContext.isLeaderReady()).thenReturn(true);
    Mockito.when(scmContext.isInSafeMode()).thenReturn(false);
  }

  private ReplicationManager createReplicationManager() throws IOException {
    return new ReplicationManager(
        configuration,
        containerManager,
        ratisPlacementPolicy,
        ecPlacementPolicy,
        eventPublisher,
        scmContext,
        nodeManager,
        clock,
        legacyReplicationManager,
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
        MockDatanodeDetails.randomDatanodeDetails(), 1, Integer.MAX_VALUE);
    containerReplicaPendingOps.scheduleDeleteReplica(ContainerID.valueOf(2),
        MockDatanodeDetails.randomDatanodeDetails(), 1, Integer.MAX_VALUE);
    Assert.assertEquals(1, containerReplicaPendingOps
        .getPendingOpCount(ContainerReplicaOp.PendingOpType.ADD));
    Assert.assertEquals(1, containerReplicaPendingOps
        .getPendingOpCount(ContainerReplicaOp.PendingOpType.DELETE));

    // Registers against serviceManager and notifies the status has changed.
    enableProcessAll();

    // Pending ops should be cleared.
    Assert.assertEquals(0, containerReplicaPendingOps
        .getPendingOpCount(ContainerReplicaOp.PendingOpType.ADD));
    Assert.assertEquals(0, containerReplicaPendingOps
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
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
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
    Mockito.verify(eventPublisher, Mockito.times(1))
        .fireEvent(SCMEvents.CLOSE_CONTAINER, container.containerID());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.OPEN_UNHEALTHY));
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
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

    Mockito.verify(nodeManager, Mockito.times(3))
        .addDatanodeCommand(any(), any());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    /*
    Though over replicated, this container should not be added to over
    replicated queue until all replicas are closed.
     */
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
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

    Mockito.verify(nodeManager, Mockito.times(1))
        .addDatanodeCommand(any(), any());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(1, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testUnderReplicatedClosedContainerWithOnlyUnhealthyReplicas()
      throws ContainerNotFoundException {
    RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    ContainerInfo container = createContainerInfo(ratisRepConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    // Container is closed but replicas are UNHEALTHY
    Set<ContainerReplica> replicas =
        addReplicas(container, ContainerReplicaProto.State.UNHEALTHY, 0, 0);
    ContainerReplica unhealthyOnDecommissioning = createContainerReplica(
        container.containerID(), 0, DECOMMISSIONING,
        ContainerReplicaProto.State.UNHEALTHY);
    replicas.add(unhealthyOnDecommissioning);

    replicationManager.processContainer(container, repQueue, repReport);
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.UNHEALTHY));
    Assert.assertEquals(1, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  /**
   * Situation: QUASI_CLOSED container with 3 QUASI_CLOSED and 1 UNHEALTHY
   * replica. They all have the same origin node id. It's expected that the
   * UNHEALTHY replica (and not a QUASI_CLOSED one) is deleted.
   */
  @Test
  public void testQuasiClosedContainerWithExcessUnhealthyReplica()
      throws IOException, NodeNotFoundException {
    RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    ContainerInfo container = createContainerInfo(ratisRepConfig, 1,
        HddsProtos.LifeCycleState.QUASI_CLOSED);
    Set<ContainerReplica> replicas =
        createReplicasWithSameOrigin(container.containerID(),
            ContainerReplicaProto.State.QUASI_CLOSED, 0, 0, 0);
    UUID origin = replicas.iterator().next().getOriginDatanodeId();
    ContainerReplica unhealthy =
        createContainerReplica(container.containerID(), 0, IN_SERVICE,
            ContainerReplicaProto.State.UNHEALTHY, 1, 123,
            MockDatanodeDetails.randomDatanodeDetails(), origin);
    replicas.add(unhealthy);
    storeContainerAndReplicas(container, replicas);

    replicationManager.processContainer(container, repQueue, repReport);
    Assert.assertEquals(0, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(1, repQueue.overReplicatedQueueSize());

    RatisOverReplicationHandler handler = new RatisOverReplicationHandler(
        ratisPlacementPolicy, replicationManager);

    Mockito.when(nodeManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenReturn(NodeStatus.inServiceHealthy());
    handler.processAndSendCommands(replicas, Collections.emptyList(),
            repQueue.dequeueOverReplicatedContainer(), 2);
    Assert.assertTrue(commandsSent.iterator().hasNext());
    Assert.assertEquals(unhealthy.getDatanodeDetails().getUuid(),
        commandsSent.iterator().next().getKey());
    Assert.assertEquals(SCMCommandProto.Type.deleteContainerCommand,
        commandsSent.iterator().next().getValue().getType());

  }

  @Test
  public void testQuasiClosedContainerWithUnhealthyReplicaOnUniqueOrigin()
      throws IOException, NodeNotFoundException {
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
    Assert.assertEquals(0, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(1, repQueue.overReplicatedQueueSize());

    RatisOverReplicationHandler handler = new RatisOverReplicationHandler(
        ratisPlacementPolicy, replicationManager);

    Mockito.when(nodeManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenReturn(NodeStatus.inServiceHealthy());
    handler.processAndSendCommands(replicas, Collections.emptyList(),
        repQueue.dequeueOverReplicatedContainer(), 2);
    Assert.assertTrue(commandsSent.iterator().hasNext());

    // unhealthy replica can't be deleted because it has a unique origin DN
    Assert.assertNotEquals(unhealthy.getDatanodeDetails().getUuid(),
        commandsSent.iterator().next().getKey());
    Assert.assertEquals(SCMCommandProto.Type.deleteContainerCommand,
        commandsSent.iterator().next().getValue().getType());
  }

  @Test
  public void testHealthyContainer() throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4, 5);

    replicationManager.processContainer(
        container, repQueue, repReport);
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testUnderReplicatedContainer() throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4);

    replicationManager.processContainer(
        container, repQueue, repReport);
    Assert.assertEquals(1, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
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
    Assert.assertEquals(ContainerHealthResult.HealthState.UNDER_REPLICATED,
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
    Assert.assertEquals(ContainerHealthResult.HealthState.UNDER_REPLICATED,
        result.getHealthState());
  }

  @Test
  public void testUnderReplicatedContainerFixedByPending()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4);
    containerReplicaPendingOps.scheduleAddReplica(container.containerID(),
        MockDatanodeDetails.randomDatanodeDetails(), 5,
        clock.millis() + 10000);

    replicationManager.processContainer(
        container, repQueue, repReport);
    // As the pending replication fixes the under replication, nothing is added
    // to the under replication list.
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    // As the container is still under replicated, as the pending have not
    // completed yet, the container is still marked as under-replicated in the
    // report.
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
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
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(0, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.MISSING));
  }

  @Test
  public void testUnrecoverableAndEmpty()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);

    ContainerReplica replica  = createContainerReplica(container.containerID(),
        1, IN_SERVICE, ContainerReplicaProto.State.CLOSED,
        0, 0, MockDatanodeDetails.randomDatanodeDetails(), UUID.randomUUID());

    storeContainerAndReplicas(container, Collections.singleton(replica));

    replicationManager.processContainer(container, repQueue, repReport);
    // If it is unrecoverable, there is no point in putting it into the under
    // replication list. It will be checked again on the next RM run.
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(0, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(0, repReport.getStat(
        ReplicationManagerReport.HealthState.MISSING));
    // As it is marked empty in the report, it must have gone through the
    // empty container handler, indicating is was handled as empty.
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.EMPTY));
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

    Assert.assertEquals(1, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
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

    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(0, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.MISSING));
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

    Assert.assertEquals(1, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
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
    Assert.assertEquals(1, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(0, repReport.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  @Test
  public void testOverReplicated() throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED,
        1, 2, 3, 4, 5, 5);
    replicationManager.processContainer(
        container, repQueue, repReport);
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(1, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  @Test
  public void testOverReplicatedFixByPending()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED,
        1, 2, 3, 4, 5, 5);
    containerReplicaPendingOps.scheduleDeleteReplica(container.containerID(),
        MockDatanodeDetails.randomDatanodeDetails(), 5,
        clock.millis() + 10000);
    replicationManager.processContainer(
        container, repQueue, repReport);
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    // If the pending replication fixes the over-replication, nothing is added
    // to the over replication list.
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  @Test
  public void testUnderReplicationQueuePopulated() {
    // Make it always return mis-replicated. Only a perfectly replicated
    // container should make it the mis-replicated state as under / over
    // replicated take precedence.
    Mockito.when(ecPlacementPolicy.validateContainerPlacement(
            anyList(), anyInt()))
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
    Assert.assertEquals(underRep0, res.getContainerInfo());

    // Now requeue it
    queue.enqueue(res);

    // Now get the next message. It should be underRep1, as it has remaining
    // redundancy 1 + zero retries. UnderRep0 will have remaining redundancy 0
    // and 1 retry. They will have the same weighted redundancy so lesser
    // retries should come first
    res = queue.dequeueUnderReplicatedContainer();
    Assert.assertEquals(underRep1, res.getContainerInfo());

    // Next message is underRep0. It starts with a weighted redundancy of 0 + 1
    // retry. The other message on the queue is a decommission only with a
    // weighted redundancy of 5 + 0. So lets dequeue and requeue the message 4
    // times. Then the weighted redundancy will be equal and the decommission
    // one will be next due to having less retries.
    for (int i = 0; i < 4; i++) {
      res = queue.dequeueUnderReplicatedContainer();
      Assert.assertEquals(underRep0, res.getContainerInfo());
      queue.enqueue(res);
    }
    res = queue.dequeueUnderReplicatedContainer();
    Assert.assertEquals(decomContainer, res.getContainerInfo());

    res = queue.dequeueUnderReplicatedContainer();
    Assert.assertEquals(underRep0, res.getContainerInfo());

    // Next is the mis-rep container, which has a remaining redundancy of 6.
    res = queue.dequeueUnderReplicatedContainer();
    Assert.assertEquals(misRep, res.getContainerInfo());

    res = queue.dequeueUnderReplicatedContainer();
    Assert.assertNull(res);
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
    Mockito.verify(nodeManager).addDatanodeCommand(any(), any());
    Assertions.assertEquals(1, ops.size());
    Assertions.assertEquals(ContainerReplicaOp.PendingOpType.DELETE,
        ops.get(0).getOpType());
    Assertions.assertEquals(target, ops.get(0).getTarget());
    Assertions.assertEquals(1, ops.get(0).getReplicaIndex());
    Assertions.assertEquals(1, replicationManager.getMetrics()
            .getEcDeletionCmdsSentTotal());
    Assertions.assertEquals(0, replicationManager.getMetrics()
        .getNumDeletionCmdsSent());

    // Repeat with Ratis container, as different metrics should be incremented
    Mockito.clearInvocations(nodeManager);
    RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    containerInfo = ReplicationTestUtil.createContainerInfo(ratisRepConfig, 2,
            HddsProtos.LifeCycleState.CLOSED, 10, 20);

    deleteContainerCommand = new DeleteContainerCommand(
        containerInfo.getContainerID());
    replicationManager.sendDatanodeCommand(deleteContainerCommand,
        containerInfo, target);

    ops = containerReplicaPendingOps.getPendingOps(containerInfo.containerID());
    Mockito.verify(nodeManager).addDatanodeCommand(any(), any());
    Assertions.assertEquals(1, ops.size());
    Assertions.assertEquals(ContainerReplicaOp.PendingOpType.DELETE,
        ops.get(0).getOpType());
    Assertions.assertEquals(target, ops.get(0).getTarget());
    Assertions.assertEquals(0, ops.get(0).getReplicaIndex());
    Assertions.assertEquals(1, replicationManager.getMetrics()
        .getEcDeletionCmdsSentTotal());
    Assertions.assertEquals(1, replicationManager.getMetrics()
        .getNumDeletionCmdsSent());
    Assertions.assertEquals(20, replicationManager.getMetrics()
        .getNumDeletionBytesTotal());
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
        missingIndexes, ecRepConfig);

    replicationManager.sendDatanodeCommand(command, containerInfo, target4);

    List<ContainerReplicaOp> ops = containerReplicaPendingOps.getPendingOps(
        containerInfo.containerID());
    Mockito.verify(nodeManager).addDatanodeCommand(any(), any());
    Assertions.assertEquals(2, ops.size());
    Set<DatanodeDetails> cmdTargets = new HashSet<>();
    Set<Integer> cmdIndexes = new HashSet<>();
    for (ContainerReplicaOp op : ops) {
      Assertions.assertEquals(ADD, op.getOpType());
      cmdTargets.add(op.getTarget());
      cmdIndexes.add(op.getReplicaIndex());
    }
    Assertions.assertEquals(2, cmdTargets.size());
    for (DatanodeDetails dn : targetNodes) {
      Assertions.assertTrue(cmdTargets.contains(dn));
    }

    Assertions.assertEquals(2, cmdIndexes.size());
    for (int i : missingIndexes) {
      Assertions.assertTrue(cmdIndexes.contains(i));
    }
    Assertions.assertEquals(1, replicationManager.getMetrics()
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
    ReplicationManager.ReplicationManagerConfiguration rmConf = configuration
        .getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    long expectedDeadline = clock.millis() + rmConf.getEventTimeout() -
            rmConf.getDatanodeTimeoutOffset();
    Assert.assertEquals(expectedDeadline, command.getDeadline());

    List<ContainerReplicaOp> ops = containerReplicaPendingOps.getPendingOps(
        containerInfo.containerID());
    Mockito.verify(nodeManager).addDatanodeCommand(any(), any());
    Assertions.assertEquals(1, ops.size());
    Assertions.assertEquals(ContainerReplicaOp.PendingOpType.ADD,
        ops.get(0).getOpType());
    Assertions.assertEquals(target, ops.get(0).getTarget());
    Assertions.assertEquals(1, ops.get(0).getReplicaIndex());
    Assertions.assertEquals(1, replicationManager.getMetrics()
        .getEcReplicationCmdsSentTotal());
    Assertions.assertEquals(0, replicationManager.getMetrics()
        .getNumReplicationCmdsSent());

    // Repeat with Ratis container, as different metrics should be incremented
    Mockito.clearInvocations(nodeManager);
    RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    containerInfo = ReplicationTestUtil.createContainerInfo(ratisRepConfig, 2,
        HddsProtos.LifeCycleState.CLOSED, 10, 20);

    command = ReplicateContainerCommand.fromSources(
        containerInfo.getContainerID(), sources);
    replicationManager.sendDatanodeCommand(command, containerInfo, target);

    ops = containerReplicaPendingOps.getPendingOps(containerInfo.containerID());
    Mockito.verify(nodeManager).addDatanodeCommand(any(), any());
    Assertions.assertEquals(1, ops.size());
    Assertions.assertEquals(ContainerReplicaOp.PendingOpType.ADD,
        ops.get(0).getOpType());
    Assertions.assertEquals(target, ops.get(0).getTarget());
    Assertions.assertEquals(0, ops.get(0).getReplicaIndex());
    Assertions.assertEquals(1, replicationManager.getMetrics()
        .getEcReplicationCmdsSentTotal());
    Assertions.assertEquals(1, replicationManager.getMetrics()
        .getNumReplicationCmdsSent());
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
    ReplicationManager.ReplicationManagerConfiguration rmConf = configuration
        .getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    long expectedDeadline = clock.millis() + rmConf.getEventTimeout() -
        rmConf.getDatanodeTimeoutOffset();
    Assert.assertEquals(expectedDeadline, command.getDeadline());

    List<ContainerReplicaOp> ops = containerReplicaPendingOps.getPendingOps(
        containerInfo.containerID());
    Mockito.verify(nodeManager).addDatanodeCommand(any(), any());
    Assertions.assertEquals(1, ops.size());
    Assertions.assertEquals(ContainerReplicaOp.PendingOpType.ADD,
        ops.get(0).getOpType());
    Assertions.assertEquals(target, ops.get(0).getTarget());
    Assertions.assertEquals(1, ops.get(0).getReplicaIndex());
    Assertions.assertEquals(1, replicationManager.getMetrics()
        .getEcReplicationCmdsSentTotal());
    Assertions.assertEquals(0, replicationManager.getMetrics()
        .getNumReplicationCmdsSent());
  }

  @Test
  public void testSendLowPriorityReplicateContainerCommand()
      throws NotLeaderException {
    ContainerInfo containerInfo =
        ReplicationTestUtil.createContainerInfo(repConfig, 1,
            HddsProtos.LifeCycleState.CLOSED, 10, 20);
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails src = MockDatanodeDetails.randomDatanodeDetails();

    ReplicationManager.ReplicationManagerConfiguration rmConf = configuration
        .getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    long scmDeadline = clock.millis() + rmConf.getEventTimeout();
    long datanodeDeadline = scmDeadline - rmConf.getDatanodeTimeoutOffset();

    replicationManager.sendLowPriorityReplicateContainerCommand(containerInfo,
        0, src, target, scmDeadline);

    ArgumentCaptor<SCMCommand> command =
        ArgumentCaptor.forClass(SCMCommand.class);
    ArgumentCaptor<UUID> targetUUID =
        ArgumentCaptor.forClass(UUID.class);
    Mockito.verify(nodeManager).addDatanodeCommand(targetUUID.capture(),
        command.capture());

    ReplicateContainerCommand sentCommand =
        (ReplicateContainerCommand)command.getValue();
    Assertions.assertEquals(datanodeDeadline, sentCommand.getDeadline());
    Assertions.assertEquals(LOW, sentCommand.getPriority());
    Assertions.assertEquals(src.getUuid(), targetUUID.getValue());
    Assertions.assertEquals(target, sentCommand.getTargetDatanode());
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

    Assertions.assertEquals(1, commandsSent.size());
    Pair<UUID, SCMCommand<?>> cmdWithTarget = commandsSent.iterator().next();
    Assertions.assertEquals(expectedTarget.getUuid(), cmdWithTarget.getLeft());
    Assertions.assertEquals(ReplicateContainerCommand.class,
        cmdWithTarget.getRight().getClass());
    ReplicateContainerCommand cmd =
        (ReplicateContainerCommand) cmdWithTarget.getRight();
    Assertions.assertEquals(destination, cmd.getTargetDatanode());
    Assertions.assertEquals(replicaIndex, cmd.getReplicaIndex());
  }

  @Test(expected = CommandTargetOverloadedException.class)
  public void testSendThrottledReplicateContainerCommandThrowsWhenNoSources()
      throws CommandTargetOverloadedException, NodeNotFoundException,
      NotLeaderException {
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
    replicationManager.sendThrottledReplicationCommand(
            container, sourceNodes, destination, 0);
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

    Assertions.assertEquals(1, commandsSent.size());
    Pair<UUID, SCMCommand<?>> cmd = commandsSent.iterator().next();
    Assertions.assertEquals(cmdTarget.getUuid(), cmd.getLeft());
  }

  @Test(expected = CommandTargetOverloadedException.class)
  public void testSendThrottledReconstructionCommandThrowsWhenNoTargets()
      throws CommandTargetOverloadedException, NodeNotFoundException,
      NotLeaderException {
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
    replicationManager.sendThrottledReconstructionCommand(container, command);
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
        new ArrayList<>(Arrays.asList(targets)), missingIndexes,
        (ECReplicationConfig) repConfig);
  }

  @Test
  public void testCreateThrottledDeleteContainerCommand()
      throws CommandTargetOverloadedException, NodeNotFoundException,
      NotLeaderException {
    Mockito.when(nodeManager.getTotalDatanodeCommandCount(any(),
            eq(SCMCommandProto.Type.deleteContainerCommand)))
        .thenAnswer(invocation -> 0);

    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    ContainerInfo container = ReplicationTestUtil.createContainerInfo(
        repConfig, 1, HddsProtos.LifeCycleState.CLOSED, 10, 20);
    replicationManager.sendThrottledDeleteCommand(container, 1, target, true);
    Assert.assertEquals(commandsSent.size(), 1);
  }

  @Test(expected = CommandTargetOverloadedException.class)
  public void testCreateThrottledDeleteContainerCommandThrowsWhenNoSources()
      throws CommandTargetOverloadedException, NodeNotFoundException,
      NotLeaderException {
    int limit = replicationManager.getConfig().getDatanodeDeleteLimit();

    Mockito.when(nodeManager.getTotalDatanodeCommandCount(any(),
            eq(SCMCommandProto.Type.deleteContainerCommand)))
        .thenAnswer(invocation -> limit + 1);

    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    ContainerInfo container = ReplicationTestUtil.createContainerInfo(
        repConfig, 1, HddsProtos.LifeCycleState.CLOSED, 10, 20);
    replicationManager.sendThrottledDeleteCommand(container, 1, target, true);
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
    Assert.assertEquals(1, excluded.size());
    // dn 3 was at the limit already, so should be added when filtering the
    // nodes
    Assert.assertTrue(excluded.contains(dn3));

    // Trigger an update for dn3, but it should stay in the excluded list as its
    // count is still at the limit.
    replicationManager.datanodeCommandCountUpdated(dn3);
    Assert.assertEquals(replicationManager.getExcludedNodes().size(), 1);

    // Starting maintenance on dn3 increases its limits, so it should no longer
    // be excluded
    dn3.setPersistedOpState(ENTERING_MAINTENANCE);
    replicationManager.datanodeCommandCountUpdated(dn3);
    Assert.assertEquals(0, replicationManager.getExcludedNodes().size());

    // now sent a reconstruction command. It should be sent to dn2, which is
    // at the lowest count, but this command should push it to the limit and
    // cause it to be excluded.
    ReconstructECContainersCommand command = createReconstructionCommand(
        container, dn1, dn2);
    replicationManager.sendThrottledReconstructionCommand(container, command);
    excluded = replicationManager.getExcludedNodes();
    Assert.assertEquals(1, excluded.size());
    // dn 2 reached the limit from the reconstruction command
    Assert.assertTrue(excluded.contains(dn2));

    // Update received for DN2, it should be cleared from the excluded list.
    replicationManager.datanodeCommandCountUpdated(dn2);
    excluded = replicationManager.getExcludedNodes();
    Assert.assertEquals(0, excluded.size());

    // Finally, update received for DN1 - it is not excluded and should not
    // be added or cause any problems by not being there
    replicationManager.datanodeCommandCountUpdated(dn1);
    Assert.assertEquals(0, excluded.size());
  }

  @Test
  public void testInflightReplicationLimit() throws IOException {
    int healthyNodes = 10;
    ReplicationManager.ReplicationManagerConfiguration config =
        new ReplicationManager.ReplicationManagerConfiguration();
    Mockito.when(nodeManager.getNodeCount(
        Mockito.isNull(), eq(HddsProtos.NodeState.HEALTHY)))
        .thenReturn(healthyNodes);

    config.setInflightReplicationLimitFactor(0.0);
    configuration.setFromObject(config);
    ReplicationManager rm = createReplicationManager();
    Assertions.assertEquals(0, rm.getReplicationInFlightLimit());

    config.setInflightReplicationLimitFactor(1);
    configuration.setFromObject(config);
    rm = createReplicationManager();
    Assertions.assertEquals(
        healthyNodes * config.getDatanodeReplicationLimit(),
        rm.getReplicationInFlightLimit());

    config.setInflightReplicationLimitFactor(0.75);
    configuration.setFromObject(config);
    rm = createReplicationManager();
    Assertions.assertEquals(
        (int) Math.ceil(healthyNodes
            * config.getDatanodeReplicationLimit() * 0.75),
        rm.getReplicationInFlightLimit());
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
    Mockito.when(nodeManager.getTotalDatanodeCommandCounts(any(),
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
