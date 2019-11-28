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
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.*;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.reset;

/**
 * Tests to ensure the DatanodeAdminMonitor is working correctly. This class
 * uses mocks or basic implementations of the key classes outside of the
 * datanodeAdminMonitor to allow it to be tested in isolation.
 */
public class TestDatanodeAdminMonitor {

  private SimpleMockNodeManager nodeManager;
  private OzoneConfiguration conf;
  private DatanodeAdminMonitor monitor;
  private DatanodeAdminHandler startAdminHandler;
  private ReplicationManager repManager;

  @Before
  public void setup() throws IOException, AuthenticationException {
    conf = new OzoneConfiguration();

    EventQueue eventQueue = new EventQueue();
    startAdminHandler = new DatanodeAdminHandler();
    eventQueue.addHandler(SCMEvents.START_ADMIN_ON_NODE, startAdminHandler);

    nodeManager = new SimpleMockNodeManager();

    repManager = Mockito.mock(ReplicationManager.class);

    monitor = new DatanodeAdminMonitor(conf);
    monitor.setEventQueue(eventQueue);
    monitor.setNodeManager(nodeManager);
    monitor.setReplicationManager(repManager);
  }

  @After
  public void teardown() {
  }

  @Test
  public void testNodeCanBeQueuedAndCancelled() {
    DatanodeDetails dn = TestUtils.randomDatanodeDetails();
    monitor.startMonitoring(dn, 0);
    assertEquals(1, monitor.getPendingCount());

    monitor.stopMonitoring(dn);
    assertEquals(0, monitor.getPendingCount());
    assertEquals(1, monitor.getCancelledCount());

    monitor.startMonitoring(dn, 0);
    assertEquals(1, monitor.getPendingCount());
    assertEquals(0, monitor.getCancelledCount());
  }

  /**
   * In this test we ensure there are some pipelines for the node being
   * decommissioned, but there are no containers. Therefore the workflow
   * must wait until the piplines have closed before completing the decommission
   * flow.
   */
  @Test
  public void testClosePipelinesEventFiredWhenAdminStarted() {
    DatanodeDetails dn1 = TestUtils.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));
    // Ensure the node has some pipelines
    nodeManager.setPipelines(dn1, 2);
    // Add the node to the monitor
    monitor.startMonitoring(dn1, 0);
    monitor.run();
    // Ensure a StartAdmin event was fired
    assertEquals(1, startAdminHandler.getInvocation());
    // Ensure a node is now tracked for decommission
    assertEquals(1, monitor.getTrackedNodeCount());
    // Ensure the node at the CLOSE_PIPELINES state
    assertEquals(DatanodeAdminMonitor.States.CLOSE_PIPELINES,
        getFirstTrackedNode().getCurrentState());
    // Run the monitor again, and it should remain in CLOSE_PIPELINES state
    monitor.run();
    assertEquals(DatanodeAdminMonitor.States.CLOSE_PIPELINES,
        getFirstTrackedNode().getCurrentState());
    // Clear the pipelines and the node should transition to the next state
    nodeManager.setPipelines(dn1, 0);
    monitor.run();
    assertEquals(0, monitor.getTrackedNodeCount());
    NodeStatus newStatus = nodeManager.getNodeStatus(dn1);
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONED,
        newStatus.getOperationalState());
  }

  /**
   * In this test, there are no open pipelines and no containers on the node.
   * Therefore, we expect the decommission flow to finish on the first run
   * on the monitor, leaving zero nodes tracked and the node in DECOMMISSIONED
   * state.
   */
  @Test
  public void testDecommissionNodeTransitionsToCompleteWhenNoContainers() {
    DatanodeDetails dn1 = TestUtils.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));

    // Add the node to the monitor. By default we have zero pipelines and
    // zero containers in the test setup, so the node should immediately
    // transition to COMPLETED state
    monitor.startMonitoring(dn1, 0);
    monitor.run();
    assertEquals(0, monitor.getTrackedNodeCount());
    NodeStatus newStatus = nodeManager.getNodeStatus(dn1);
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONED,
        newStatus.getOperationalState());
  }

  @Test
  public void testDecommissionNodeWaitsForContainersToReplicate()
      throws NodeNotFoundException, ContainerNotFoundException {
    DatanodeDetails dn1 = TestUtils.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));

    nodeManager.setContainers(dn1, generateContainers(3));
    // Mock Replication Manager to return ContainerReplicaCount's which
    // always have a DECOMMISSIONED replica.
    mockGetContainerReplicaCount(
        HddsProtos.LifeCycleState.CLOSED,
        ContainerReplicaProto.State.DECOMMISSIONED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED);

    // Run the monitor for the first time and the node will transition to
    // REPLICATE_CONTAINERS as there are no pipelines to close.
    monitor.startMonitoring(dn1, 0);
    monitor.run();
    DatanodeAdminNodeDetails node = getFirstTrackedNode();
    assertEquals(1, monitor.getTrackedNodeCount());
    assertEquals(DatanodeAdminMonitor.States.REPLICATE_CONTAINERS,
        node.getCurrentState());

    // Running the monitor again causes it to remain at replicate_containers
    // as nothing has changed.
    monitor.run();
    assertEquals(DatanodeAdminMonitor.States.REPLICATE_CONTAINERS,
        node.getCurrentState());
    assertEquals(0, node.getSufficientlyReplicatedContainers());
    assertEquals(0, node.getUnHealthyContainers());
    assertEquals(3, node.getUnderReplicatedContainers());

    // Now change the replicationManager mock to return 3 CLOSED replicas
    // and the node should complete the REPLICATE_CONTAINERS step, moving to
    // complete which will end the decommission workflow
    mockGetContainerReplicaCount(
        HddsProtos.LifeCycleState.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED);

    monitor.run();

    assertEquals(0, monitor.getTrackedNodeCount());
    assertEquals(3, node.getSufficientlyReplicatedContainers());
    assertEquals(0, node.getUnHealthyContainers());
    assertEquals(0, node.getUnderReplicatedContainers());
    NodeStatus newStatus = nodeManager.getNodeStatus(dn1);
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONED,
        newStatus.getOperationalState());
  }

  @Test
  public void testDecommissionAbortedWhenNodeInUnexpectedState()
      throws NodeNotFoundException, ContainerNotFoundException {
    DatanodeDetails dn1 = TestUtils.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));

    nodeManager.setContainers(dn1, generateContainers(3));
    mockGetContainerReplicaCount(
        HddsProtos.LifeCycleState.CLOSED,
        ContainerReplicaProto.State.DECOMMISSIONED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED);

    // Add the node to the monitor, it should have 3 under-replicated containers
    // after the first run
    monitor.startMonitoring(dn1, 0);
    monitor.run();
    assertEquals(1, monitor.getTrackedNodeCount());
    DatanodeAdminNodeDetails node = getFirstTrackedNode();
    assertEquals(DatanodeAdminMonitor.States.REPLICATE_CONTAINERS,
        node.getCurrentState());
    assertEquals(3, node.getUnderReplicatedContainers());

    // Set the node to dead, and then the workflow should get aborted, setting
    // the node state back to IN_SERVICE on the next run.
    nodeManager.setNodeStatus(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.IN_SERVICE,
            HddsProtos.NodeState.HEALTHY));
    monitor.run();
    assertEquals(0, monitor.getTrackedNodeCount());
    NodeStatus newStatus = nodeManager.getNodeStatus(dn1);
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        newStatus.getOperationalState());
  }

  @Test
  public void testDecommissionAbortedWhenNodeGoesDead()
      throws NodeNotFoundException, ContainerNotFoundException {
    DatanodeDetails dn1 = TestUtils.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));

    nodeManager.setContainers(dn1, generateContainers(3));
    mockGetContainerReplicaCount(
        HddsProtos.LifeCycleState.CLOSED,
        ContainerReplicaProto.State.DECOMMISSIONED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED);

    // Add the node to the monitor, it should have 3 under-replicated containers
    // after the first run
    monitor.startMonitoring(dn1, 0);
    monitor.run();
    assertEquals(1, monitor.getTrackedNodeCount());
    DatanodeAdminNodeDetails node = getFirstTrackedNode();
    assertEquals(DatanodeAdminMonitor.States.REPLICATE_CONTAINERS,
        node.getCurrentState());
    assertEquals(3, node.getUnderReplicatedContainers());

    // Set the node to dead, and then the workflow should get aborted, setting
    // the node state back to IN_SERVICE.
    nodeManager.setNodeStatus(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.DECOMMISSIONING,
            HddsProtos.NodeState.DEAD));
    monitor.run();
    assertEquals(0, monitor.getTrackedNodeCount());
    NodeStatus newStatus = nodeManager.getNodeStatus(dn1);
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        newStatus.getOperationalState());
  }

  @Test
  public void testMaintenanceWaitsForMaintenanceToComplete() {
    DatanodeDetails dn1 = TestUtils.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
            HddsProtos.NodeState.HEALTHY));

    // Add the node to the monitor, it should transiting to
    // AWAIT_MAINTENANCE_END as there are no under-replicated containers.
    // The operational state should also goto IN_MAINTENANCE
    monitor.startMonitoring(dn1, 1);
    monitor.run();
    assertEquals(1, monitor.getTrackedNodeCount());
    DatanodeAdminNodeDetails node = getFirstTrackedNode();
    assertEquals(DatanodeAdminMonitor.States.AWAIT_MAINTENANCE_END,
        node.getCurrentState());
    assertEquals(0, node.getUnderReplicatedContainers());
    assertTrue(nodeManager.getNodeStatus(dn1).isInMaintenance());

    // Running the monitor again causes the node to remain in maintenance
    monitor.run();
    assertEquals(1, monitor.getTrackedNodeCount());
    assertEquals(DatanodeAdminMonitor.States.AWAIT_MAINTENANCE_END,
        node.getCurrentState());
    assertTrue(nodeManager.getNodeStatus(dn1).isInMaintenance());

    // Set the maintenance end time to a time in the past and then the node
    // should complete the workflow and transition to IN_SERVICE
    node.setMaintenanceEnd(-1);
    monitor.run();
    assertEquals(0, monitor.getTrackedNodeCount());
    NodeStatus newStatus = nodeManager.getNodeStatus(dn1);
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        newStatus.getOperationalState());
  }

  @Test
  public void testMaintenanceEndsClosingPipelines() {
    DatanodeDetails dn1 = TestUtils.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
            HddsProtos.NodeState.HEALTHY));
    // Ensure the node has some pipelines
    nodeManager.setPipelines(dn1, 2);
    // Add the node to the monitor
    monitor.startMonitoring(dn1, 1);
    monitor.run();
    DatanodeAdminNodeDetails node = getFirstTrackedNode();
    assertEquals(1, monitor.getTrackedNodeCount());
    assertEquals(DatanodeAdminMonitor.States.CLOSE_PIPELINES,
        node.getCurrentState());

    // Set the maintenance end time to the past and the node should complete
    // the workflow and return to IN_SERVICE
    node.setMaintenanceEnd(-1);
    monitor.run();
    assertEquals(0, monitor.getTrackedNodeCount());
    NodeStatus newStatus = nodeManager.getNodeStatus(dn1);
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        newStatus.getOperationalState());
  }

  @Test
  public void testMaintenanceEndsWhileReplicatingContainers()
      throws ContainerNotFoundException, NodeNotFoundException {
    DatanodeDetails dn1 = TestUtils.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
            HddsProtos.NodeState.HEALTHY));

    nodeManager.setContainers(dn1, generateContainers(3));
    mockGetContainerReplicaCount(
        HddsProtos.LifeCycleState.CLOSED,
        ContainerReplicaProto.State.MAINTENANCE,
        ContainerReplicaProto.State.MAINTENANCE,
        ContainerReplicaProto.State.MAINTENANCE);

    // Add the node to the monitor, it should transiting to
    // REPLICATE_CONTAINERS as the containers are under-replicated for
    // maintenance.
    monitor.startMonitoring(dn1, 1);
    monitor.run();
    assertEquals(1, monitor.getTrackedNodeCount());
    DatanodeAdminNodeDetails node = getFirstTrackedNode();
    assertEquals(DatanodeAdminMonitor.States.REPLICATE_CONTAINERS,
        node.getCurrentState());
    assertEquals(3, node.getUnderReplicatedContainers());

    node.setMaintenanceEnd(-1);
    monitor.run();
    assertEquals(0, monitor.getTrackedNodeCount());
    NodeStatus newStatus = nodeManager.getNodeStatus(dn1);
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        newStatus.getOperationalState());
  }

  @Test
  public void testDeadMaintenanceNodeDoesNotAbortWorkflow() {
    DatanodeDetails dn1 = TestUtils.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
            HddsProtos.NodeState.HEALTHY));

    // Add the node to the monitor, it should transiting to
    // AWAIT_MAINTENANCE_END as there are no under-replicated containers.
    monitor.startMonitoring(dn1, 1);
    monitor.run();
    assertEquals(1, monitor.getTrackedNodeCount());
    DatanodeAdminNodeDetails node = getFirstTrackedNode();
    assertEquals(DatanodeAdminMonitor.States.AWAIT_MAINTENANCE_END,
        node.getCurrentState());
    assertEquals(0, node.getUnderReplicatedContainers());

    // Set the node dead and ensure the workflow does not end
    NodeStatus status = nodeManager.getNodeStatus(dn1);
    nodeManager.setNodeStatus(dn1, new NodeStatus(
        status.getOperationalState(), HddsProtos.NodeState.DEAD));

    // Running the monitor again causes the node to remain in maintenance
    monitor.run();
    assertEquals(1, monitor.getTrackedNodeCount());
    assertEquals(DatanodeAdminMonitor.States.AWAIT_MAINTENANCE_END,
        node.getCurrentState());
  }

  /**
   * Generate a set of ContainerID, starting from an ID of zero up to the given
   * count minus 1.
   * @param count The number of ContainerID objects to generate.
   * @return A Set of ContainerID objects.
   */
  private Set<ContainerID> generateContainers(int count) {
    Set<ContainerID> containers = new HashSet<>();
    for (int i=0; i<count; i++) {
      containers.add(new ContainerID(i));
    }
    return containers;
  }

  /**
   * Create a ContainerReplicaCount object, including a container with the
   * requested ContainerID and state, along with a set of replicas of the given
   * states.
   * @param containerID The ID of the container to create an included
   * @param containerState The state of the container
   * @param states Create a replica for each of the given states.
   * @return A ContainerReplicaCount containing the generated container and
   *         replica set
   */
  private ContainerReplicaCount generateReplicaCount(ContainerID containerID,
      HddsProtos.LifeCycleState containerState,
      ContainerReplicaProto.State... states) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (ContainerReplicaProto.State s : states) {
      replicas.add(generateReplica(containerID, s));
    }
    ContainerInfo container = new ContainerInfo.Builder()
        .setContainerID(containerID.getId())
        .setState(containerState)
        .build();

    return new ContainerReplicaCount(container, replicas, 0, 0, 3, 2);
  }

  /**
   * Generate a new ContainerReplica with the given containerID and State.
   * @param containerID The ID the replica is associated with
   * @param state The state of the generated replica.
   * @return A containerReplica with the given ID and state
   */
  private ContainerReplica generateReplica(ContainerID containerID,
      ContainerReplicaProto.State state) {
    return ContainerReplica.newBuilder()
        .setContainerState(state)
        .setContainerID(containerID)
        .setSequenceId(1)
        .setDatanodeDetails(TestUtils.randomDatanodeDetails())
        .build();
  }

  /**
   * Helper method to get the first node from the set of trackedNodes within
   * the monitor.
   * @return DatanodeAdminNodeDetails for the first tracked node found.
   */
  private DatanodeAdminNodeDetails getFirstTrackedNode() {
    return
        monitor.getTrackedNodes().toArray(new DatanodeAdminNodeDetails[0])[0];
  }

  /**
   * The only interaction the DatanodeAdminMonitor has with the
   * ReplicationManager, is to request a ContainerReplicaCount object for each
   * container on nodes being deocmmissioned or moved to maintenance. This
   * method mocks that interface to return a ContainerReplicaCount with a
   * container in the given containerState and a set of replias in the given
   * replicaStates.
   * @param containerState
   * @param replicaStates
   * @throws ContainerNotFoundException
   */
  private void mockGetContainerReplicaCount(
      HddsProtos.LifeCycleState containerState,
      ContainerReplicaProto.State... replicaStates)
      throws ContainerNotFoundException {
    reset(repManager);
    Mockito.when(repManager.getContainerReplicaCount(
        Mockito.any(ContainerID.class)))
        .thenAnswer(invocation ->
            generateReplicaCount((ContainerID)invocation.getArguments()[0],
                containerState, replicaStates));
  }

  /**
   * This simple internal class is used to track and handle any DatanodeAdmin
   * events fired by the DatanodeAdminMonitor during tests.
   */
  private class DatanodeAdminHandler implements
      EventHandler<DatanodeDetails> {

    private AtomicInteger invocation = new AtomicInteger(0);

    @Override
    public void onMessage(final DatanodeDetails dn,
                          final EventPublisher publisher) {
      invocation.incrementAndGet();
    }

    public int getInvocation() {
      return invocation.get();
    }
  }
}