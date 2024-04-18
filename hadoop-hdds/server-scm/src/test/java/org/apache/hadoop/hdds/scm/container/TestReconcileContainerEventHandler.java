package org.apache.hadoop.hdds.scm.container;

import org.apache.commons.lang3.stream.Streams;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReconcileContainerCommandProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.DATANODE_COMMAND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

  @Captor
  private ArgumentCaptor<CommandForDatanode<ReconcileContainerCommandProto>> commandCaptor;

  @BeforeEach
  public void setup() throws Exception {
    // TODO for command captor?
    MockitoAnnotations.initMocks(this);
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
    addContainer(CONTAINER_ID, EC_REP, LifeCycleState.CLOSED);
    addReplicasToContainer(CONTAINER_ID, 5);
    eventHandler.onMessage(CONTAINER_ID, eventPublisher);
    verify(eventPublisher, never()).fireEvent(eq(DATANODE_COMMAND), any());
  }

  /**
   * Ratis 1 containers are not currently supported for reconciliation.
   */
  @Test
  public void testReconcileRatisOneContainer() throws Exception {
    addContainer(CONTAINER_ID, RATIS_ONE_REP, LifeCycleState.CLOSED);
    addReplicasToContainer(CONTAINER_ID, 1);
    eventHandler.onMessage(CONTAINER_ID, eventPublisher);
    verify(eventPublisher, never()).fireEvent(eq(DATANODE_COMMAND), any());
  }

  @Test
  public void testReconcileWhenNotLeader() throws Exception {
    addContainer(CONTAINER_ID, RATIS_THREE_REP, LifeCycleState.CLOSED);
    addReplicasToContainer(CONTAINER_ID, 3);
    when(scmContext.isLeader()).thenReturn(false);
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
    eventHandler.onMessage(CONTAINER_ID, eventPublisher);
    verify(eventPublisher, never()).fireEvent(eq(DATANODE_COMMAND), any());
  }

  @Test
  public void testReconcileMissingContainer() throws Exception {
    addContainer(CONTAINER_ID, RATIS_THREE_REP, LifeCycleState.CLOSED);
    assertTrue(containerManager.getContainerReplicas(CONTAINER_ID).isEmpty(),
        "Expected no replicas for this container");
    eventHandler.onMessage(CONTAINER_ID, eventPublisher);
    verify(eventPublisher, never()).fireEvent(eq(DATANODE_COMMAND), any());
  }

  @ParameterizedTest
  @EnumSource(LifeCycleState.class)
  public void testReconcileWithContainerStates(LifeCycleState state) throws Exception {
    addContainer(CONTAINER_ID, RATIS_THREE_REP, state);
    addReplicasToContainer(CONTAINER_ID, 3);
    eventHandler.onMessage(CONTAINER_ID, eventPublisher);
    switch (state) {
    case OPEN:
    case CLOSING:
    case DELETING:
    case DELETED:
    case RECOVERING:
      verify(eventPublisher, never()).fireEvent(eq(DATANODE_COMMAND), commandCaptor.capture());
      break;
    default:
      verify(eventPublisher, times(3)).fireEvent(eq(DATANODE_COMMAND), commandCaptor.capture());
      break;
    }


    if (state == LifeCycleState.OPEN || state == LifeCycleState.CLOSING) {
      verify(eventPublisher, never()).fireEvent(eq(DATANODE_COMMAND), any());
    } else {
      verify(eventPublisher, any()).fireEvent(eq(DATANODE_COMMAND), any());
    }
  }

  @Test
  public void testReconcileSentToAllPeers() throws Exception {
    addContainer(CONTAINER_ID, RATIS_THREE_REP, LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas = addReplicasToContainer(CONTAINER_ID, 3);
    Set<UUID> allNodeIDs = replicas.stream()
        .map(r -> r.getDatanodeDetails().getUuid())
        .collect(Collectors.toSet());

    eventHandler.onMessage(CONTAINER_ID, eventPublisher);
    assertEquals(3, replicas.size());
    assertEquals(allNodeIDs.size(), replicas.size());
    verify(eventPublisher, times(replicas.size())).fireEvent(eq(DATANODE_COMMAND), commandCaptor.capture());

    // Check each reconcile command sent for correctness.
    Set<UUID> nodesReceivingCommands = new HashSet<>();
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
      UUID targetNodeID = dnCommand.getDatanodeId();
      assertTrue(nodesReceivingCommands.add(targetNodeID), "Duplicate reconcile command sent to datanode.");
      // All commands should have correctly constructed peer lists that exclude the node receiving the command.
      Set<UUID> expectedPeerIDs = allNodeIDs.stream()
          .filter(id -> id != targetNodeID)
          .collect(Collectors.toSet());
      Set<UUID> actualPeerIDs = reconcileProto.getPeersList().stream()
              .map(dn -> UUID.fromString(dn.getUuid()))
              .collect(Collectors.toSet());
      assertEquals(replicas.size() - 1, actualPeerIDs.size());
      assertEquals(expectedPeerIDs, actualPeerIDs);
    }

    assertEquals(allNodeIDs, nodesReceivingCommands);
  }

  @ParameterizedTest
  @EnumSource(State.class)
  public void testReconcileFailsWithOpenReplicas(State replicaState) throws Exception {
    // Overall container state is eligible for reconciliation, but some replicas may not be.
    // This means the container will not be considered eligible.
    addContainer(CONTAINER_ID, RATIS_THREE_REP, LifeCycleState.CLOSED);
    // Only one replica is in a different state.
    addReplicasToContainer(CONTAINER_ID, replicaState, State.CLOSED, State.CLOSED);
    eventHandler.onMessage(CONTAINER_ID, eventPublisher);
    switch (replicaState) {
    case OPEN:
    case INVALID:
    case DELETED:
    case CLOSING:
      verify(eventPublisher, never()).fireEvent(eq(DATANODE_COMMAND), commandCaptor.capture());
      break;
    default:
      verify(eventPublisher, times(3)).fireEvent(eq(DATANODE_COMMAND), commandCaptor.capture());
      break;
    }
  }

  private ContainerInfo addContainer(ContainerID id, ReplicationConfig repConfig, LifeCycleState state) throws Exception {
    ContainerInfo container = new ContainerInfo.Builder()
        .setContainerID(id.getId())
//        .setOwner("Ozone")
//        .setPipelineID(pipelineID)
        .setReplicationConfig(repConfig)
        .setState(state)
        .build();
    when(containerManager.getContainer(id)).thenReturn(container);
    return container;
  }

  private Set<ContainerReplica> addReplicasToContainer(ContainerID id, int count) throws Exception {
    State[] replicaStates = new State[count];
    Arrays.fill(replicaStates, State.CLOSED);
    return addReplicasToContainer(id, replicaStates);
  }

  private Set<ContainerReplica> addReplicasToContainer(ContainerID id, State... replicaStates) throws Exception {
    // Add one container replica for each replica state specified.
    // If no states are specified, replica list will be empty.
    Set<ContainerReplica> replicas = new HashSet<>();
    try (MockNodeManager nodeManager = new MockNodeManager(true, replicaStates.length)) {
      List<DatanodeDetails> nodes = nodeManager.getAllNodes();
      for (int i = 0; i < replicaStates.length; i++) {
        replicas.addAll(HddsTestUtils.getReplicas(id, replicaStates[i], nodes.get(i)));
      }
    }
    when(containerManager.getContainerReplicas(id)).thenReturn(replicas);

    return replicas;
  }
}
