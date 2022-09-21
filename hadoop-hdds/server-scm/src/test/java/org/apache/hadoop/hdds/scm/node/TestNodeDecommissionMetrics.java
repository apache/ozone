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
import org.apache.hadoop.hdds.scm.container.SimpleMockNodeManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;

/**
 * Tests for the NodeDecommissionMetrics class.
 */
public class TestNodeDecommissionMetrics {
  private NodeDecommissionMetrics metrics;
  private SimpleMockNodeManager nodeManager;
  private OzoneConfiguration conf;
  private DatanodeAdminMonitorImpl monitor;
  private DatanodeAdminMonitorTestUtil
          .DatanodeAdminHandler startAdminHandler;
  private ReplicationManager repManager;
  private EventQueue eventQueue;


  @BeforeEach
  public void setup() {
    conf = new OzoneConfiguration();
    eventQueue = new EventQueue();
    startAdminHandler = new DatanodeAdminMonitorTestUtil
            .DatanodeAdminHandler();
    eventQueue.addHandler(SCMEvents.START_ADMIN_ON_NODE, startAdminHandler);
    nodeManager = new SimpleMockNodeManager();
    repManager = Mockito.mock(ReplicationManager.class);
    monitor =
            new DatanodeAdminMonitorImpl(
                    conf, eventQueue, nodeManager, repManager);
    metrics = NodeDecommissionMetrics.create();
    monitor.setMetrics(metrics);
  }

  @AfterEach
  public void after() {
    metrics.unRegister();
  }

  @Test
  public void testDecommMonitorCollectTrackedNodes() {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
            new NodeStatus(ENTERING_MAINTENANCE,
                    HddsProtos.NodeState.HEALTHY));
    monitor.startMonitoring(dn1);
    monitor.run();
    Assertions.assertEquals(1,
            metrics.getTotalTrackedDecommissioningMaintenanceNodes());
  }

  @Test
  public void testDecommMonitorCollectRecommissionNodes() {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
            new NodeStatus(DECOMMISSIONING,
                    HddsProtos.NodeState.HEALTHY));
    monitor.startMonitoring(dn1);
    monitor.run();
    // recommission
    monitor.stopMonitoring(dn1);
    monitor.run();
    Assertions.assertEquals(0,
            metrics.getTotalTrackedDecommissioningMaintenanceNodes());
    Assertions.assertEquals(1,
            metrics.getTotalTrackedRecommissionNodes());
  }

  @Test
  public void testDecommMonitorCollectPipelinesWaitingClosed() {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
            new NodeStatus(HddsProtos.NodeOperationalState.DECOMMISSIONING,
                    HddsProtos.NodeState.HEALTHY));
    // Ensure the node has some pipelines
    nodeManager.setPipelines(dn1, 2);
    // Add the node to the monitor
    monitor.startMonitoring(dn1);
    monitor.run();
    Assertions.assertEquals(2,
            metrics.getTotalTrackedPipelinesWaitingToClose());
  }

  @Test
  public void testDecommMonitorCollectUnderReplicated()
          throws ContainerNotFoundException, NodeNotFoundException {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
            new NodeStatus(HddsProtos.NodeOperationalState.DECOMMISSIONING,
                    HddsProtos.NodeState.HEALTHY));

    Set<ContainerID> containers = new HashSet<>();
    containers.add(ContainerID.valueOf(1));

    nodeManager.setContainers(dn1, containers);
    DatanodeAdminMonitorTestUtil
            .mockGetContainerReplicaCount(
                    repManager,
                    HddsProtos.LifeCycleState.CLOSED,
                    DECOMMISSIONED,
                    IN_SERVICE,
                    IN_SERVICE);

    // Add the node to the monitor, it should have 1 under-replicated containers
    // after the first run
    monitor.startMonitoring(dn1);
    monitor.run();
    Assertions.assertEquals(1,
            metrics.getTotalTrackedContainersUnderReplicated());
  }

  @Test
  public void testDecommMonitorCollectSufficientlyReplicated()
          throws ContainerNotFoundException, NodeNotFoundException {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
            new NodeStatus(DECOMMISSIONING,
                    HddsProtos.NodeState.HEALTHY));
    Set<ContainerID> containers = new HashSet<>();
    containers.add(ContainerID.valueOf(1));

    nodeManager.setContainers(dn1, containers);
    DatanodeAdminMonitorTestUtil
            .mockGetContainerReplicaCount(
                    repManager,
                    HddsProtos.LifeCycleState.CLOSED,
                    IN_SERVICE,
                    IN_SERVICE,
                    IN_SERVICE);
    monitor.startMonitoring(dn1);

    monitor.run();
    Assertions.assertEquals(1,
            metrics.getTotalTrackedContainersSufficientlyReplicated());

  }

  @Test
  public void testDecommMonitorCollectUnhealthyContainers()
          throws ContainerNotFoundException, NodeNotFoundException {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
            new NodeStatus(DECOMMISSIONING,
                    HddsProtos.NodeState.HEALTHY));
    Set<ContainerID> containers = new HashSet<>();
    containers.add(ContainerID.valueOf(1));

    nodeManager.setContainers(dn1, containers);
    DatanodeAdminMonitorTestUtil
            .mockGetContainerReplicaCount(
                    repManager,
                    HddsProtos.LifeCycleState.CLOSED,
                    DECOMMISSIONED,
                    IN_SERVICE,
                    IN_SERVICE);
    monitor.startMonitoring(dn1);

    monitor.run();
    Assertions.assertEquals(1,
            metrics.getTotalTrackedContainersUnhealthy());

  }

  @Test
  public void testDecommMonitorCollectionMultipleDnContainers()
          throws ContainerNotFoundException, NodeNotFoundException {
    // test metric aggregation over several datanodes
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails dn2 = MockDatanodeDetails.randomDatanodeDetails();

    nodeManager.register(dn1,
            new NodeStatus(DECOMMISSIONING,
                    HddsProtos.NodeState.HEALTHY));
    nodeManager.register(dn2,
            new NodeStatus(DECOMMISSIONING,
                    HddsProtos.NodeState.HEALTHY));

    Set<ContainerID> containersDn1 = new HashSet<>();
    containersDn1.add(ContainerID.valueOf(1));
    containersDn1.add(ContainerID.valueOf(2));

    nodeManager.setContainers(dn1, containersDn1);

    Set<ContainerID> containersDn2 = new HashSet<>();
    containersDn2.add(ContainerID.valueOf(3));

    nodeManager.setContainers(dn2, containersDn2);
    DatanodeAdminMonitorTestUtil
            .mockGetContainerReplicaCount(
                    repManager,
                    HddsProtos.LifeCycleState.CLOSED,
                    DECOMMISSIONED,
                    IN_SERVICE,
                    IN_SERVICE);

    monitor.startMonitoring(dn1);
    monitor.startMonitoring(dn2);

    monitor.run();
    Assertions.assertEquals(3,
            metrics.getTotalTrackedContainersUnderReplicated());

  }

  @Test
  public void testDecommMonitorCollectionMultipleDnPipelines() {
    // test metric aggregation over several datanodes
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails dn2 = MockDatanodeDetails.randomDatanodeDetails();

    nodeManager.register(dn1,
            new NodeStatus(DECOMMISSIONING,
                    HddsProtos.NodeState.HEALTHY));
    nodeManager.register(dn2,
            new NodeStatus(DECOMMISSIONING,
                    HddsProtos.NodeState.HEALTHY));

    nodeManager.setPipelines(dn1, 2);
    nodeManager.setPipelines(dn2, 1);

    monitor.startMonitoring(dn1);
    monitor.startMonitoring(dn2);

    monitor.run();
    Assertions.assertEquals(3,
            metrics.getTotalTrackedPipelinesWaitingToClose());
  }
}
