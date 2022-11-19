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
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.SimpleMockNodeManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;

/**
 * Tests to ensure the DatanodeAdminMonitor is working correctly. This class
 * uses mocks or basic implementations of the key classes outside of the
 * datanodeAdminMonitor to allow it to be tested in isolation.
 */
public class TestDatanodeAdminMonitor {

  private SimpleMockNodeManager nodeManager;
  private OzoneConfiguration conf;
  private DatanodeAdminMonitorImpl monitor;
  private DatanodeAdminMonitorTestUtil
          .DatanodeAdminHandler startAdminHandler;
  private ReplicationManager repManager;
  private EventQueue eventQueue;

  @BeforeEach
  public void setup() throws IOException, AuthenticationException {
    conf = new OzoneConfiguration();

    eventQueue = new EventQueue();
    startAdminHandler = new DatanodeAdminMonitorTestUtil
            .DatanodeAdminHandler();
    eventQueue.addHandler(SCMEvents.START_ADMIN_ON_NODE, startAdminHandler);

    nodeManager = new SimpleMockNodeManager();

    repManager = Mockito.mock(ReplicationManager.class);

    monitor =
        new DatanodeAdminMonitorImpl(conf, eventQueue, nodeManager, repManager);
    monitor.setMetrics(NodeDecommissionMetrics.create());
  }

  @Test
  public void testNodeCanBeQueuedAndCancelled() {
    DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
    monitor.startMonitoring(dn);
    assertEquals(1, monitor.getPendingCount());

    monitor.stopMonitoring(dn);
    assertEquals(0, monitor.getPendingCount());
    assertEquals(1, monitor.getCancelledCount());

    monitor.startMonitoring(dn);
    assertEquals(1, monitor.getPendingCount());
    assertEquals(0, monitor.getCancelledCount());
  }

  /**
   * In this test we ensure there are some pipelines for the node being
   * decommissioned, but there are no containers. Therefore the workflow
   * must wait until the pipelines have closed before completing the flow.
   */
  @Test
  public void testClosePipelinesEventFiredWhenAdminStarted()
      throws NodeNotFoundException {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));
    // Ensure the node has some pipelines
    nodeManager.setPipelines(dn1, 2);
    // Add the node to the monitor
    monitor.startMonitoring(dn1);
    monitor.run();
    // Ensure a StartAdmin event was fired
    eventQueue.processAll(20000);
    assertEquals(1, startAdminHandler.getInvocation());
    // Ensure a node is now tracked for decommission
    assertEquals(1, monitor.getTrackedNodeCount());
    // Ensure the node remains decommissioning
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dn1).getOperationalState());
    // Run the monitor again, and it should remain decommissioning
    monitor.run();
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dn1).getOperationalState());

    // Clear the pipelines and the node should transition to DECOMMISSIONED
    nodeManager.setPipelines(dn1, 0);
    monitor.run();
    assertEquals(0, monitor.getTrackedNodeCount());
    assertEquals(DECOMMISSIONED,
        nodeManager.getNodeStatus(dn1).getOperationalState());
  }

  /**
   * In this test, there are no open pipelines and no containers on the node.
   * Therefore, we expect the decommission flow to finish on the first run
   * on the monitor, leaving zero nodes tracked and the node in DECOMMISSIONED
   * state.
   */
  @Test
  public void testDecommissionNodeTransitionsToCompleteWhenNoContainers()
      throws NodeNotFoundException {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));

    // Add the node to the monitor. By default we have zero pipelines and
    // zero containers in the test setup, so the node should immediately
    // transition to COMPLETED state
    monitor.startMonitoring(dn1);
    monitor.run();
    assertEquals(0, monitor.getTrackedNodeCount());
    NodeStatus newStatus = nodeManager.getNodeStatus(dn1);
    assertEquals(DECOMMISSIONED,
        newStatus.getOperationalState());
  }

  @Test
  public void testDecommissionNodeWaitsForContainersToReplicate()
      throws NodeNotFoundException, ContainerNotFoundException {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));

    nodeManager.setContainers(dn1, generateContainers(3));
    // Mock Replication Manager to return ContainerReplicaCount's which
    // always have a DECOMMISSIONED replica.
    DatanodeAdminMonitorTestUtil
            .mockGetContainerReplicaCount(
                    repManager,
                    HddsProtos.LifeCycleState.CLOSED,
                    DECOMMISSIONED,
                    IN_SERVICE,
                    IN_SERVICE);

    // Run the monitor for the first time and the node will transition to
    // REPLICATE_CONTAINERS as there are no pipelines to close.
    monitor.startMonitoring(dn1);
    monitor.run();
    DatanodeDetails node = getFirstTrackedNode();
    assertEquals(1, monitor.getTrackedNodeCount());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dn1).getOperationalState());

    // Running the monitor again causes it to remain DECOMMISSIONING
    // as nothing has changed.
    monitor.run();
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dn1).getOperationalState());

    // Now change the replicationManager mock to return 3 CLOSED replicas
    // and the node should complete the REPLICATE_CONTAINERS step, moving to
    // complete which will end the decommission workflow
    DatanodeAdminMonitorTestUtil
            .mockGetContainerReplicaCount(
                    repManager,
                    HddsProtos.LifeCycleState.CLOSED,
                    IN_SERVICE,
                    IN_SERVICE,
                    IN_SERVICE);

    monitor.run();

    assertEquals(0, monitor.getTrackedNodeCount());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONED,
        nodeManager.getNodeStatus(dn1).getOperationalState());
  }

  @Test
  public void testDecommissionAbortedWhenNodeInUnexpectedState()
      throws NodeNotFoundException, ContainerNotFoundException {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));

    nodeManager.setContainers(dn1, generateContainers(3));
    DatanodeAdminMonitorTestUtil
            .mockGetContainerReplicaCount(
                    repManager,
                    HddsProtos.LifeCycleState.CLOSED,
                    DECOMMISSIONED,
                    IN_SERVICE,
                    IN_SERVICE);

    // Add the node to the monitor, it should have 3 under-replicated containers
    // after the first run
    monitor.startMonitoring(dn1);
    monitor.run();
    assertEquals(1, monitor.getTrackedNodeCount());
    DatanodeDetails node = getFirstTrackedNode();
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dn1).getOperationalState());

    // Set the node to dead, and then the workflow should get aborted, setting
    // the node state back to IN_SERVICE on the next run.
    nodeManager.setNodeStatus(dn1,
        new NodeStatus(IN_SERVICE,
            HddsProtos.NodeState.HEALTHY));
    monitor.run();
    assertEquals(0, monitor.getTrackedNodeCount());
    assertEquals(IN_SERVICE,
        nodeManager.getNodeStatus(dn1).getOperationalState());
  }

  @Test
  public void testDecommissionAbortedWhenNodeGoesDead()
      throws NodeNotFoundException, ContainerNotFoundException {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));

    nodeManager.setContainers(dn1, generateContainers(3));
    DatanodeAdminMonitorTestUtil
            .mockGetContainerReplicaCount(
                    repManager,
                    HddsProtos.LifeCycleState.CLOSED,
                    DECOMMISSIONED,
                    IN_SERVICE,
                    IN_SERVICE);

    // Add the node to the monitor, it should have 3 under-replicated containers
    // after the first run
    monitor.startMonitoring(dn1);
    monitor.run();
    assertEquals(1, monitor.getTrackedNodeCount());
    DatanodeDetails node = getFirstTrackedNode();
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dn1).getOperationalState());

    // Set the node to dead, and then the workflow should get aborted, setting
    // the node state back to IN_SERVICE.
    nodeManager.setNodeStatus(dn1,
        new NodeStatus(HddsProtos.NodeOperationalState.DECOMMISSIONING,
            HddsProtos.NodeState.DEAD));
    monitor.run();
    assertEquals(0, monitor.getTrackedNodeCount());
    assertEquals(IN_SERVICE,
        nodeManager.getNodeStatus(dn1).getOperationalState());
  }

  @Test
  public void testMaintenanceWaitsForMaintenanceToComplete()
      throws NodeNotFoundException {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(ENTERING_MAINTENANCE,
            HddsProtos.NodeState.HEALTHY));

    // Add the node to the monitor, it should transiting to
    // IN_MAINTENANCE as there are no containers to replicate.
    monitor.startMonitoring(dn1);
    monitor.run();
    assertEquals(1, monitor.getTrackedNodeCount());
    DatanodeDetails node = getFirstTrackedNode();
    assertTrue(nodeManager.getNodeStatus(dn1).isInMaintenance());

    // Running the monitor again causes the node to remain in maintenance
    monitor.run();
    assertEquals(1, monitor.getTrackedNodeCount());
    assertTrue(nodeManager.getNodeStatus(dn1).isInMaintenance());

    // Set the maintenance end time to a time in the past and then the node
    // should complete the workflow and transition to IN_SERVICE
    nodeManager.setNodeOperationalState(node,
        HddsProtos.NodeOperationalState.IN_MAINTENANCE, -1);
    monitor.run();
    assertEquals(0, monitor.getTrackedNodeCount());
    assertEquals(IN_SERVICE,
        nodeManager.getNodeStatus(dn1).getOperationalState());
  }

  @Test
  public void testMaintenanceEndsClosingPipelines()
      throws NodeNotFoundException {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(ENTERING_MAINTENANCE,
            HddsProtos.NodeState.HEALTHY));
    // Ensure the node has some pipelines
    nodeManager.setPipelines(dn1, 2);
    // Add the node to the monitor
    monitor.startMonitoring(dn1);
    monitor.run();
    DatanodeDetails node = getFirstTrackedNode();
    assertEquals(1, monitor.getTrackedNodeCount());
    assertTrue(nodeManager.getNodeStatus(dn1).isEnteringMaintenance());

    // Set the maintenance end time to the past and the node should complete
    // the workflow and return to IN_SERVICE
    nodeManager.setNodeOperationalState(node,
        HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE, -1);
    monitor.run();
    assertEquals(0, monitor.getTrackedNodeCount());
    assertEquals(IN_SERVICE,
        nodeManager.getNodeStatus(dn1).getOperationalState());
  }

  @Test
  public void testMaintenanceEndsWhileReplicatingContainers()
      throws ContainerNotFoundException, NodeNotFoundException {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(ENTERING_MAINTENANCE,
            HddsProtos.NodeState.HEALTHY));

    nodeManager.setContainers(dn1, generateContainers(3));
    DatanodeAdminMonitorTestUtil
            .mockGetContainerReplicaCount(
                    repManager,
                    HddsProtos.LifeCycleState.CLOSED,
                    IN_MAINTENANCE,
                    ENTERING_MAINTENANCE,
                    IN_MAINTENANCE);

    // Add the node to the monitor, it should transiting to
    // REPLICATE_CONTAINERS as the containers are under-replicated for
    // maintenance.
    monitor.startMonitoring(dn1);
    monitor.run();
    assertEquals(1, monitor.getTrackedNodeCount());
    DatanodeDetails node = getFirstTrackedNode();
    assertTrue(nodeManager.getNodeStatus(dn1).isEnteringMaintenance());

    nodeManager.setNodeOperationalState(node,
        HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE, -1);
    monitor.run();
    assertEquals(0, monitor.getTrackedNodeCount());
    assertEquals(IN_SERVICE,
        nodeManager.getNodeStatus(dn1).getOperationalState());
  }

  @Test
  public void testDeadMaintenanceNodeDoesNotAbortWorkflow()
      throws NodeNotFoundException {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(ENTERING_MAINTENANCE,
            HddsProtos.NodeState.HEALTHY));

    // Add the node to the monitor, it should transiting to
    // AWAIT_MAINTENANCE_END as there are no under-replicated containers.
    monitor.startMonitoring(dn1);
    monitor.run();
    assertEquals(1, monitor.getTrackedNodeCount());
    DatanodeDetails node = getFirstTrackedNode();
    assertTrue(nodeManager.getNodeStatus(dn1).isInMaintenance());

    // Set the node dead and ensure the workflow does not end
    NodeStatus status = nodeManager.getNodeStatus(dn1);
    nodeManager.setNodeStatus(dn1, new NodeStatus(
        status.getOperationalState(), HddsProtos.NodeState.DEAD));

    // Running the monitor again causes the node to remain in maintenance
    monitor.run();
    assertEquals(1, monitor.getTrackedNodeCount());
    assertTrue(nodeManager.getNodeStatus(dn1).isInMaintenance());
  }

  @Test
  public void testCancelledNodesMovedToInService()
      throws NodeNotFoundException {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
        new NodeStatus(ENTERING_MAINTENANCE,
            HddsProtos.NodeState.HEALTHY));

    // Add the node to the monitor, it should transiting to
    // AWAIT_MAINTENANCE_END as there are no under-replicated containers.
    monitor.startMonitoring(dn1);
    monitor.run();
    assertEquals(1, monitor.getTrackedNodeCount());
    DatanodeDetails node = getFirstTrackedNode();
    assertTrue(nodeManager.getNodeStatus(dn1).isInMaintenance());

    // Now cancel the node and run the monitor, the node should be IN_SERVICE
    monitor.stopMonitoring(dn1);
    monitor.run();
    assertEquals(0, monitor.getTrackedNodeCount());
    assertEquals(IN_SERVICE,
        nodeManager.getNodeStatus(dn1).getOperationalState());
  }

  /**
   * Generate a set of ContainerID, starting from an ID of zero up to the given
   * count minus 1.
   * @param count The number of ContainerID objects to generate.
   * @return A Set of ContainerID objects.
   */
  private Set<ContainerID> generateContainers(int count) {
    Set<ContainerID> containers = new HashSet<>();
    for (int i = 0; i < count; i++) {
      containers.add(ContainerID.valueOf(i));
    }
    return containers;
  }

  /**
   * Helper method to get the first node from the set of trackedNodes within
   * the monitor.
   * @return DatanodeAdminNodeDetails for the first tracked node found.
   */
  private DatanodeDetails getFirstTrackedNode() {
    return
        monitor.getTrackedNodes().toArray(new DatanodeDetails[0])[0];
  }
}