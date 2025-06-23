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

package org.apache.hadoop.hdds.scm.container.reconciliation;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.DATANODE_COMMAND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReconcileContainerCommandProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.reconciliation.ReconciliationEligibilityHandler.EligibilityResult;
import org.apache.hadoop.hdds.scm.container.reconciliation.ReconciliationEligibilityHandler.Result;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

/**
 * Tests that the ReconcileContainerEventHandler properly accepts and rejects reconciliation events based on
 * container state, and dispatches commands to datanodes accordingly.
 */
public class TestReconcileContainerEventHandler {
  private ContainerManager containerManager;
  private EventPublisher eventPublisher;
  private ReconcileContainerEventHandler eventHandler;
  private SCMContext scmContext;

  private static final ContainerID CONTAINER_ID = ContainerID.valueOf(123L);
  private static final long LEADER_TERM = 3L;

  private static final ReplicationConfig RATIS_THREE_REP = RatisReplicationConfig.getInstance(THREE);
  private static final ReplicationConfig RATIS_ONE_REP = RatisReplicationConfig.getInstance(ONE);
  private static final ReplicationConfig EC_REP = new ECReplicationConfig(3, 2);

  private ArgumentCaptor<CommandForDatanode<ReconcileContainerCommandProto>> commandCaptor;

  @BeforeEach
  public void setup() throws Exception {
    commandCaptor = ArgumentCaptor.forClass(CommandForDatanode.class);
    containerManager = mock(ContainerManager.class);
    scmContext = mock(SCMContext.class);
    when(scmContext.isLeader()).thenReturn(true);
    when(scmContext.getTermOfLeader()).thenReturn(LEADER_TERM);
    eventPublisher = mock(EventPublisher.class);
    eventHandler = new ReconcileContainerEventHandler(containerManager, scmContext);
  }

  /**
   * EC containers are not yet supported for reconciliation.
   */
  @Test
  public void testReconcileECContainer() throws Exception {
    addContainer(EC_REP, LifeCycleState.CLOSED);
    addReplicasToContainer(5);

    EligibilityResult result =
        ReconciliationEligibilityHandler.isEligibleForReconciliation(CONTAINER_ID, containerManager);
    assertFalse(result.isOk());
    assertEquals(Result.INELIGIBLE_REPLICATION_TYPE, result.getResult());

    eventHandler.onMessage(CONTAINER_ID, eventPublisher);
    verify(eventPublisher, never()).fireEvent(eq(DATANODE_COMMAND), any());
  }

  /**
   * Ratis 1 containers are not currently supported for reconciliation.
   */
  @Test
  public void testReconcileRatisOneContainer() throws Exception {
    addContainer(RATIS_ONE_REP, LifeCycleState.CLOSED);
    addReplicasToContainer(1);

    EligibilityResult result =
        ReconciliationEligibilityHandler.isEligibleForReconciliation(CONTAINER_ID, containerManager);
    assertFalse(result.isOk());
    assertEquals(Result.NOT_ENOUGH_REQUIRED_NODES, result.getResult());

    eventHandler.onMessage(CONTAINER_ID, eventPublisher);
    verify(eventPublisher, never()).fireEvent(eq(DATANODE_COMMAND), any());
  }

  @Test
  public void testReconcileWhenNotLeader() throws Exception {
    addContainer(RATIS_THREE_REP, LifeCycleState.CLOSED);
    addReplicasToContainer(3);
    when(scmContext.isLeader()).thenReturn(false);

    // Container is eligible for reconciliation, but the request will not go through because this SCM is not the leader.
    EligibilityResult result =
        ReconciliationEligibilityHandler.isEligibleForReconciliation(CONTAINER_ID, containerManager);
    assertTrue(result.isOk());
    assertEquals(Result.OK, result.getResult());

    eventHandler.onMessage(CONTAINER_ID, eventPublisher);
    verify(eventPublisher, never()).fireEvent(eq(DATANODE_COMMAND), any());
  }

  @Test
  public void testReconcileNonexistentContainer() throws Exception {
    // The step of adding the container to the mocked ContainerManager is intentionally skipped to simulate a
    // nonexistent container.
    // No exceptions should be thrown out of this test method when this happens. If they are, they will be propagated
    // and the test will fail.
    when(containerManager.getContainer(any())).thenThrow(new ContainerNotFoundException());

    EligibilityResult result =
        ReconciliationEligibilityHandler.isEligibleForReconciliation(CONTAINER_ID, containerManager);
    assertFalse(result.isOk());
    assertEquals(Result.CONTAINER_NOT_FOUND, result.getResult());

    eventHandler.onMessage(CONTAINER_ID, eventPublisher);
    verify(eventPublisher, never()).fireEvent(eq(DATANODE_COMMAND), any());
  }

  @Test
  public void testReconcileMissingContainer() throws Exception {
    addContainer(RATIS_THREE_REP, LifeCycleState.CLOSED);
    assertTrue(containerManager.getContainerReplicas(CONTAINER_ID).isEmpty(),
        "Expected no replicas for this container");

    EligibilityResult result =
        ReconciliationEligibilityHandler.isEligibleForReconciliation(CONTAINER_ID, containerManager);
    assertFalse(result.isOk());
    assertEquals(Result.NO_REPLICAS_FOUND, result.getResult());

    eventHandler.onMessage(CONTAINER_ID, eventPublisher);
    verify(eventPublisher, never()).fireEvent(eq(DATANODE_COMMAND), any());
  }

  @ParameterizedTest
  @EnumSource(LifeCycleState.class)
  public void testReconcileWithContainerStates(LifeCycleState state) throws Exception {
    addContainer(RATIS_THREE_REP, state);
    addReplicasToContainer(3);
    EligibilityResult result =
        ReconciliationEligibilityHandler.isEligibleForReconciliation(CONTAINER_ID, containerManager);
    eventHandler.onMessage(CONTAINER_ID, eventPublisher);
    switch (state) {
    case OPEN:
    case CLOSING:
    case DELETING:
    case DELETED:
    case RECOVERING:
      assertFalse(result.isOk());
      assertEquals(Result.INELIGIBLE_CONTAINER_STATE, result.getResult());
      verify(eventPublisher, never()).fireEvent(eq(DATANODE_COMMAND), commandCaptor.capture());
      break;
    default:
      assertTrue(result.isOk());
      assertEquals(Result.OK, result.getResult());
      verify(eventPublisher, times(3)).fireEvent(eq(DATANODE_COMMAND), commandCaptor.capture());
      break;
    }
  }

  // TODO HDDS-10714 will change which datanodes are eligible to participate in reconciliation.
  @Test
  public void testReconcileSentToAllPeers() throws Exception {
    addContainer(RATIS_THREE_REP, LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas = addReplicasToContainer(3);
    Set<DatanodeID> allNodeIDs = replicas.stream()
        .map(r -> r.getDatanodeDetails().getID())
        .collect(Collectors.toSet());

    EligibilityResult result =
        ReconciliationEligibilityHandler.isEligibleForReconciliation(CONTAINER_ID, containerManager);
    assertTrue(result.isOk());
    assertEquals(Result.OK, result.getResult());

    eventHandler.onMessage(CONTAINER_ID, eventPublisher);
    assertEquals(3, replicas.size());
    assertEquals(allNodeIDs.size(), replicas.size());
    verify(eventPublisher, times(replicas.size())).fireEvent(eq(DATANODE_COMMAND), commandCaptor.capture());

    // Check each reconcile command sent for correctness.
    Set<DatanodeID> nodesReceivingCommands = new HashSet<>();
    for (CommandForDatanode<ReconcileContainerCommandProto> dnCommand: commandCaptor.getAllValues()) {
      SCMCommand<ReconcileContainerCommandProto> reconcileCommand = dnCommand.getCommand();
      ReconcileContainerCommandProto reconcileProto = reconcileCommand.getProto();
      // All commands should use the latest term of SCM so the datanode does not drop them.
      assertEquals(LEADER_TERM, reconcileCommand.getTerm());
      // All commands should have the same container ID.
      assertEquals(CONTAINER_ID, ContainerID.valueOf(reconcileProto.getContainerID()));
      // Container ID is also used as the command's identifier.
      assertEquals(CONTAINER_ID, ContainerID.valueOf(reconcileCommand.getId()));

      // Every node should receive exactly one reconcile command.
      DatanodeID targetNodeID = dnCommand.getDatanodeId();
      assertTrue(nodesReceivingCommands.add(targetNodeID), "Duplicate reconcile command sent to datanode.");
      // All commands should have correctly constructed peer lists that exclude the node receiving the command.
      Set<DatanodeID> expectedPeerIDs = allNodeIDs.stream()
          .filter(id -> !id.equals(targetNodeID))
          .collect(Collectors.toSet());
      Set<DatanodeID> actualPeerIDs = reconcileProto.getPeersList().stream()
              .map(dn -> DatanodeID.fromProto(dn.getId()))
              .collect(Collectors.toSet());
      assertEquals(replicas.size() - 1, actualPeerIDs.size());
      assertEquals(expectedPeerIDs, actualPeerIDs);
    }

    assertEquals(allNodeIDs, nodesReceivingCommands);
  }

  @ParameterizedTest
  @EnumSource(State.class)
  public void testReconcileFailsWithIneligibleReplicas(State replicaState) throws Exception {
    // Overall container state is eligible for reconciliation, but some replicas may not be.
    // This means the container will not be considered eligible.
    addContainer(RATIS_THREE_REP, LifeCycleState.CLOSED);
    // Only one replica is in a different state.
    addReplicasToContainer(replicaState, State.CLOSED, State.CLOSED);

    EligibilityResult result =
        ReconciliationEligibilityHandler.isEligibleForReconciliation(CONTAINER_ID, containerManager);

    eventHandler.onMessage(CONTAINER_ID, eventPublisher);
    switch (replicaState) {
    case OPEN:
    case INVALID:
    case DELETED:
    case CLOSING:
      assertFalse(result.isOk());
      assertEquals(Result.INELIGIBLE_REPLICA_STATES, result.getResult());
      verify(eventPublisher, never()).fireEvent(eq(DATANODE_COMMAND), commandCaptor.capture());
      break;
    default:
      assertTrue(result.isOk());
      assertEquals(Result.OK, result.getResult());
      verify(eventPublisher, times(3)).fireEvent(eq(DATANODE_COMMAND), commandCaptor.capture());
      break;
    }
  }

  private ContainerInfo addContainer(ReplicationConfig repConfig, LifeCycleState state) throws Exception {
    ContainerInfo container = new ContainerInfo.Builder()
        .setContainerID(CONTAINER_ID.getId())
        .setReplicationConfig(repConfig)
        .setState(state)
        .build();
    when(containerManager.getContainer(CONTAINER_ID)).thenReturn(container);
    return container;
  }

  private Set<ContainerReplica> addReplicasToContainer(int count) throws Exception {
    State[] replicaStates = new State[count];
    Arrays.fill(replicaStates, State.CLOSED);
    return addReplicasToContainer(replicaStates);
  }

  private Set<ContainerReplica> addReplicasToContainer(State... replicaStates) throws Exception {
    // Add one container replica for each replica state specified.
    // If no states are specified, replica list will be empty.
    Set<ContainerReplica> replicas = new HashSet<>();
    try (MockNodeManager nodeManager = new MockNodeManager(true, replicaStates.length)) {
      List<DatanodeDetails> nodes = nodeManager.getAllNodes();
      for (int i = 0; i < replicaStates.length; i++) {
        replicas.addAll(HddsTestUtils.getReplicas(CONTAINER_ID, replicaStates[i], nodes.get(i)));
      }
    }
    when(containerManager.getContainerReplicas(CONTAINER_ID)).thenReturn(replicas);

    return replicas;
  }
}
