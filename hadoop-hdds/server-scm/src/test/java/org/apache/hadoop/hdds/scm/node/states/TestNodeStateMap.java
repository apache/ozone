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

package org.apache.hadoop.hdds.scm.node.states;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Class to test the NodeStateMap class, which is an internal class used by
 * NodeStateManager.
 */
public class TestNodeStateMap {
  private static final NodeStatus DECOMMISSIONING_HEALTHY_999 = NodeStatus.valueOf(
      NodeOperationalState.DECOMMISSIONING, NodeState.HEALTHY, 999);

  private final DatanodeDetails dn = generateDatanode();
  private NodeStateMap map;

  void addNode(NodeStatus status) throws NodeAlreadyExistsException {
    addNode(dn, status);
  }

  void addNode(DatanodeDetails datanode, NodeStatus status) throws NodeAlreadyExistsException {
    map.addNode(new DatanodeInfo(datanode, status, null));
  }

  @BeforeEach
  public void setUp() {
    map = new NodeStateMap();
  }

  @AfterEach
  public void tearDown() {
  }

  @Test
  public void testNodeCanBeAddedAndRetrieved()
      throws NodeAlreadyExistsException, NodeNotFoundException {
    NodeStatus status = NodeStatus.inServiceHealthy();
    addNode(status);
    assertEquals(dn, map.getNodeInfo(dn.getID()));
    assertEquals(status, map.getNodeStatus(dn.getID()));
  }

  private void runTestUpdateHealth(NodeStatus original, NodeState newHealth) throws Exception {
    addNode(original);
    final NodeStatus returned = map.updateNodeHealthState(dn.getID(), newHealth);

    final NodeStatus expected = NodeStatus.valueOf(
        original.getOperationalState(), newHealth, original.getOpStateExpiryEpochSeconds());
    assertEquals(expected, returned);
    assertEquals(returned, map.getNodeStatus(dn.getID()));
  }

  @Test
  public void testUpdateHealthyToStale() throws Exception {
    runTestUpdateHealth(NodeStatus.inServiceHealthy(), NodeState.STALE);
  }

  @Test
  public void testUpdateDecommissioningHealthyToStale() throws Exception {
    runTestUpdateHealth(DECOMMISSIONING_HEALTHY_999, NodeState.STALE);
  }

  @Test
  public void testNodeOperationalStateCanBeUpdated()
      throws NodeAlreadyExistsException, NodeNotFoundException {
    NodeStatus status = NodeStatus.inServiceHealthy();
    addNode(status);

    NodeStatus expectedStatus = DECOMMISSIONING_HEALTHY_999;
    NodeStatus returnedStatus = map.updateNodeOperationalState(
        dn.getID(), expectedStatus.getOperationalState(), expectedStatus.getOpStateExpiryEpochSeconds());
    assertEquals(expectedStatus, returnedStatus);
    assertEquals(returnedStatus, map.getNodeStatus(dn.getID()));
  }

  @Test
  public void testGetNodeNonZeroExpiry() throws Exception {
    runTestGetNode(123);
  }

  @Test
  public void testGetNodeMethodsReturnCorrectCountsAndStates()
      throws NodeAlreadyExistsException {
    runTestGetNode(0);
  }

  private void runTestGetNode(long opExpiryEpochSeconds)
      throws NodeAlreadyExistsException {
    // Add one node for all possible states
    int nodeCount = 0;
    for (NodeOperationalState op : NodeOperationalState.values()) {
      for (NodeState health : NodeState.values()) {
        addRandomNodeWithState(op, health, opExpiryEpochSeconds);
        nodeCount++;
      }
    }
    final NodeStatus requestedState = NodeStatus.valueOf(
        NodeOperationalState.IN_SERVICE, NodeState.STALE, opExpiryEpochSeconds);
    final List<DatanodeDetails> nodes = map.getDatanodeDetails(requestedState);
    assertEquals(1, nodes.size());
    assertEquals(1, map.getNodeCount(requestedState));

    List<DatanodeInfo> nodes2 = map.getDatanodeInfos(
        requestedState.getOperationalState(), requestedState.getHealth());
    assertEquals(1, nodes2.size());

    assertEquals(nodeCount, map.getTotalNodeCount());
    assertEquals(nodeCount, map.getNodeCount());
    assertEquals(nodeCount, map.getAllDatanodeInfos().size());

    // Checks for the getNodeCount(opstate, health) method
    assertEquals(nodeCount, map.getNodeCount(null, null));
    assertEquals(1,
        map.getNodeCount(NodeOperationalState.DECOMMISSIONING,
            NodeState.STALE));
    assertEquals(5, map.getNodeCount(null, NodeState.HEALTHY));
    assertEquals(4,
        map.getNodeCount(NodeOperationalState.DECOMMISSIONING, null));
  }

  /**
   * Test if container list is iterable even if it's modified from other thread.
   */
  @Test
  public void testConcurrency() throws Exception {
    final DatanodeDetails datanodeDetails =
        MockDatanodeDetails.randomDatanodeDetails();

    addNode(datanodeDetails, NodeStatus.inServiceHealthy());

    DatanodeID id = datanodeDetails.getID();

    map.addContainer(id, ContainerID.valueOf(1L));
    map.addContainer(id, ContainerID.valueOf(2L));
    map.addContainer(id, ContainerID.valueOf(3L));

    CountDownLatch elementRemoved = new CountDownLatch(1);
    CountDownLatch loopStarted = new CountDownLatch(1);

    final AtomicReference<Exception> failure = new AtomicReference<>();
    new Thread(() -> {
      try {
        loopStarted.await();
        map.removeContainer(id, ContainerID.valueOf(1L));
        elementRemoved.countDown();
      } catch (Exception e) {
        failure.set(e);
      }

    }).start();

    boolean first = true;
    for (ContainerID key : map.getContainers(id)) {
      if (first) {
        loopStarted.countDown();
        elementRemoved.await();
      }
      first = false;
      System.out.println(key);
    }
    assertNull(failure.get());
  }

  private void addRandomNodeWithState(
      NodeOperationalState opState, NodeState health,
      long opExpiryEpochSeconds) throws NodeAlreadyExistsException {
    DatanodeDetails random = generateDatanode();
    NodeStatus status = NodeStatus.valueOf(opState, health, opExpiryEpochSeconds);
    addNode(random, status);
  }

  private DatanodeDetails generateDatanode() {
    return DatanodeDetails.newBuilder().setUuid(UUID.randomUUID()).build();
  }

}
