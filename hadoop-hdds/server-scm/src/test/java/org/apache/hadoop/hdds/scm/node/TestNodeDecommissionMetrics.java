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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.HashSet;
import java.util.Set;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the NodeDecommissionMetrics class.
 */
public class TestNodeDecommissionMetrics {
  private NodeDecommissionMetrics metrics;
  private SimpleMockNodeManager nodeManager;
  private DatanodeAdminMonitorImpl monitor;
  private ReplicationManager repManager;
  private EventQueue eventQueue;

  @BeforeEach
  public void setup() {
    OzoneConfiguration conf = new OzoneConfiguration();
    eventQueue = new EventQueue();
    DatanodeAdminMonitorTestUtil.DatanodeAdminHandler startAdminHandler = new DatanodeAdminMonitorTestUtil
                                                                                  .DatanodeAdminHandler();
    eventQueue.addHandler(SCMEvents.START_ADMIN_ON_NODE, startAdminHandler);
    nodeManager = new SimpleMockNodeManager();
    repManager = mock(ReplicationManager.class);
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

  /**
   * Test for collecting metric for nodes tracked in decommissioning
   * and maintenance workflow.  Dn in entering maintenance mode.
   */
  @Test
  public void testDecommMonitorCollectTrackedNodes() {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
        NodeStatus.valueOf(ENTERING_MAINTENANCE,
            HddsProtos.NodeState.HEALTHY));
    monitor.startMonitoring(dn1);
    monitor.run();
    assertEquals(1, metrics.getDecommissioningMaintenanceNodesTotal());
  }

  /**
   * Test for collecting metric for nodes tracked in workflow that are
   * in recommission workflow.  Dn decommissioned, and recommissioned.
   */
  @Test
  public void testDecommMonitorCollectRecommissionNodes() {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn1,
        NodeStatus.valueOf(DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));
    monitor.startMonitoring(dn1);
    monitor.run();
    // Recommission by putting node back in service.
    // Stop monitor and run.
    monitor.stopMonitoring(dn1);
    monitor.run();
    assertEquals(0, metrics.getDecommissioningMaintenanceNodesTotal());
    assertEquals(1, metrics.getRecommissionNodesTotal());
  }

  /**
   * Test for collecting metric for pipelines waiting to be closed when
   * datanode enters decommissioning workflow.
   */
  @Test
  public void testDecommMonitorCollectPipelinesWaitingClosed() {
    DatanodeDetails dn1 = MockDatanodeDetails.createDatanodeDetails(
        "datanode_host1",
        "/r1/ng1");
    nodeManager.register(dn1,
        NodeStatus.valueOf(HddsProtos.NodeOperationalState.DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));
    // Ensure the node has some pipelines
    nodeManager.setPipelines(dn1, 2);
    // Add the node to the monitor
    monitor.startMonitoring(dn1);
    monitor.run();
    // Ensure a StartAdmin event was fired
    eventQueue.processAll(20000);
    assertEquals(2, metrics.getPipelinesWaitingToCloseTotal());

    // should have host specific metric collected
    // for datanode_host1
    assertEquals(2, metrics.getPipelinesWaitingToCloseByHost("datanode_host1"));
    // Clear the pipelines and the metric collected for
    // datanode_host1 should clear
    nodeManager.setPipelines(dn1, 0);
    monitor.run();
    eventQueue.processAll(20000);
    assertEquals(0, metrics.getPipelinesWaitingToCloseByHost("datanode_host1"));
  }

  /**
   * Test for collecting metric for under replicated containers
   * from nodes in decommissioning and maintenance workflow.
   */
  @Test
  public void testDecommMonitorCollectUnderReplicated()
          throws ContainerNotFoundException, NodeNotFoundException {
    DatanodeDetails dn1 = MockDatanodeDetails.createDatanodeDetails(
        "datanode_host1",
        "/r1/ng1");
    nodeManager.register(dn1,
        NodeStatus.valueOf(HddsProtos.NodeOperationalState.DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));

    Set<ContainerID> containers = new HashSet<>();
    containers.add(ContainerID.valueOf(1));

    // create container with 3 replicas, 2 replicas in-service
    // 1 decommissioned; will result in an under-replicated
    // container
    nodeManager.setContainers(dn1, containers);
    DatanodeAdminMonitorTestUtil
        .mockGetContainerReplicaCount(repManager,
            true,
            HddsProtos.LifeCycleState.CLOSED,
            DECOMMISSIONED,
            IN_SERVICE,
            IN_SERVICE);

    // Add the node to the monitor, it should have 1 under-replicated
    // container after the first run
    monitor.startMonitoring(dn1);
    monitor.run();
    assertEquals(1, metrics.getContainersUnderReplicatedTotal());

    // should have host specific metric collected
    // for datanode_host1
    assertEquals(1, metrics.getUnderReplicatedByHost("datanode_host1"));
  }

  /**
   * Test for collecting metric for sufficiently replicated containers
   * from nodes in decommissioning and maintenance workflow.
   */
  @Test
  public void testDecommMonitorCollectSufficientlyReplicated()
          throws ContainerNotFoundException, NodeNotFoundException {
    DatanodeDetails dn1 = MockDatanodeDetails.createDatanodeDetails(
        "datanode_host1",
        "/r1/ng1");
    nodeManager.register(dn1,
        NodeStatus.valueOf(DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));
    Set<ContainerID> containers = new HashSet<>();
    containers.add(ContainerID.valueOf(1));

    // create container with 3 replicas,
    // all in-service
    nodeManager.setContainers(dn1, containers);
    DatanodeAdminMonitorTestUtil
        .mockGetContainerReplicaCount(repManager,
            false,
            HddsProtos.LifeCycleState.CLOSED,
            IN_SERVICE,
            IN_SERVICE,
            IN_SERVICE);
    monitor.startMonitoring(dn1);

    monitor.run();
    // expect dn in decommissioning workflow with container
    // sufficiently replicated
    assertEquals(1, metrics.getContainersSufficientlyReplicatedTotal());

    // should have host specific metric collected
    // for datanode_host1
    assertEquals(1, metrics.getSufficientlyReplicatedByHost("datanode_host1"));
  }

  /**
   * Test for collecting metric for unclosed containers
   * from nodes in decommissioning and maintenance workflow.
   */
  @Test
  public void testDecommMonitorCollectUnclosedContainers()
      throws ContainerNotFoundException, NodeNotFoundException {
    DatanodeDetails dn1 = MockDatanodeDetails.createDatanodeDetails(
        "datanode_host1",
        "/r1/ng1");
    nodeManager.register(dn1,
        NodeStatus.valueOf(DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));
    Set<ContainerID> containers = new HashSet<>();
    containers.add(ContainerID.valueOf(1));

    // set OPEN container with 1 replica CLOSED replica state,
    // in-service node, generates monitored  unclosed container replica
    nodeManager.setContainers(dn1, containers);
    DatanodeAdminMonitorTestUtil
        .mockGetContainerReplicaCount(repManager,
            true,
            HddsProtos.LifeCycleState.OPEN,
            IN_SERVICE);
    monitor.startMonitoring(dn1);

    monitor.run();
    assertEquals(1, metrics.getContainersUnClosedTotal());

    // should have host specific metric collected
    // for datanode_host1
    assertEquals(1, metrics.getUnClosedContainersByHost("datanode_host1"));
  }

  /**
   * Test for collecting aggregated metric for replicated state -
   * total number of under-replicated containers over multiple
   * datanodes in the decommissioning and maintenance workflow.
   */
  @Test
  public void testDecommMonitorCollectionMultipleDnContainers()
      throws ContainerNotFoundException, NodeNotFoundException {
    // test metric aggregation over several datanodes
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails dn2 = MockDatanodeDetails.randomDatanodeDetails();

    nodeManager.register(dn1,
        NodeStatus.valueOf(DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));
    nodeManager.register(dn2,
        NodeStatus.valueOf(DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));

    Set<ContainerID> containersDn1 = new HashSet<>();
    containersDn1.add(ContainerID.valueOf(1));
    containersDn1.add(ContainerID.valueOf(2));

    nodeManager.setContainers(dn1, containersDn1);

    Set<ContainerID> containersDn2 = new HashSet<>();
    containersDn2.add(ContainerID.valueOf(3));

    nodeManager.setContainers(dn2, containersDn2);
    DatanodeAdminMonitorTestUtil
        .mockGetContainerReplicaCount(repManager,
            true,
            HddsProtos.LifeCycleState.CLOSED,
            DECOMMISSIONED,
            IN_SERVICE,
            IN_SERVICE);

    monitor.startMonitoring(dn1);
    monitor.startMonitoring(dn2);

    monitor.run();
    assertEquals(3, metrics.getContainersUnderReplicatedTotal());
  }

  /**
   * Test for collecting aggregated metric for total number
   * of pipelines waiting to close - over multiple
   * datanodes in the decommissioning and maintenance workflow.
   */
  @Test
  public void testDecommMonitorCollectionMultipleDnPipelines() {
    // test metric aggregation over several datanodes
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails dn2 = MockDatanodeDetails.randomDatanodeDetails();

    nodeManager.register(dn1,
        NodeStatus.valueOf(DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));
    nodeManager.register(dn2,
        NodeStatus.valueOf(DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));

    nodeManager.setPipelines(dn1, 2);
    nodeManager.setPipelines(dn2, 1);

    monitor.startMonitoring(dn1);
    monitor.startMonitoring(dn2);

    monitor.run();
    assertEquals(3, metrics.getPipelinesWaitingToCloseTotal());
  }

  @Test
  public void testDecommMonitorStartTimeForHost() {
    // test metric aggregation over several datanodes
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();

    nodeManager.register(dn1,
        NodeStatus.valueOf(DECOMMISSIONING,
            HddsProtos.NodeState.HEALTHY));

    nodeManager.setPipelines(dn1, 2);

    long before = System.currentTimeMillis();
    monitor.startMonitoring(dn1);
    long after = System.currentTimeMillis();

    monitor.run();
    long startTime = monitor.getSingleTrackedNode(dn1.getIpAddress())
        .getStartTime();
    assertThat(before).isLessThanOrEqualTo(startTime);
    assertThat(after).isGreaterThanOrEqualTo(startTime);
  }
}
