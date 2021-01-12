/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.node.states;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.node.NodeStatus;

import static junit.framework.TestCase.assertEquals;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Class to test the NodeStateMap class, which is an internal class used by
 * NodeStateManager.
 */

public class TestNodeStateMap {

  private NodeStateMap map;

  @Before
  public void setUp() {
    map = new NodeStateMap();
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testNodeCanBeAddedAndRetrieved()
      throws NodeAlreadyExistsException, NodeNotFoundException {
    DatanodeDetails dn = generateDatanode();
    NodeStatus status = NodeStatus.inServiceHealthy();
    map.addNode(dn, status);
    assertEquals(dn, map.getNodeInfo(dn.getUuid()));
    assertEquals(status, map.getNodeStatus(dn.getUuid()));
  }

  @Test
  public void testNodeHealthStateCanBeUpdated()
      throws NodeAlreadyExistsException, NodeNotFoundException {
    DatanodeDetails dn = generateDatanode();
    NodeStatus status = NodeStatus.inServiceHealthy();
    map.addNode(dn, status);

    NodeStatus expectedStatus = NodeStatus.inServiceStale();
    NodeStatus returnedStatus =
        map.updateNodeHealthState(dn.getUuid(), expectedStatus.getHealth());
    assertEquals(expectedStatus, returnedStatus);
    assertEquals(returnedStatus, map.getNodeStatus(dn.getUuid()));
  }

  @Test
  public void testNodeOperationalStateCanBeUpdated()
      throws NodeAlreadyExistsException, NodeNotFoundException {
    DatanodeDetails dn = generateDatanode();
    NodeStatus status = NodeStatus.inServiceHealthy();
    map.addNode(dn, status);

    NodeStatus expectedStatus = new NodeStatus(
        NodeOperationalState.DECOMMISSIONING,
        NodeState.HEALTHY, 999);
    NodeStatus returnedStatus = map.updateNodeOperationalState(
        dn.getUuid(), expectedStatus.getOperationalState(), 999);
    assertEquals(expectedStatus, returnedStatus);
    assertEquals(returnedStatus, map.getNodeStatus(dn.getUuid()));
    assertEquals(999, returnedStatus.getOpStateExpiryEpochSeconds());
  }

  @Test
  public void testGetNodeMethodsReturnCorrectCountsAndStates()
      throws NodeAlreadyExistsException {
    // Add one node for all possible states
    int nodeCount = 0;
    for(NodeOperationalState op : NodeOperationalState.values()) {
      for(NodeState health : NodeState.values()) {
        addRandomNodeWithState(op, health);
        nodeCount++;
      }
    }
    NodeStatus requestedState = NodeStatus.inServiceStale();
    List<UUID> nodes = map.getNodes(requestedState);
    assertEquals(1, nodes.size());
    assertEquals(1, map.getNodeCount(requestedState));
    assertEquals(nodeCount, map.getTotalNodeCount());
    assertEquals(nodeCount, map.getAllNodes().size());
    assertEquals(nodeCount, map.getAllDatanodeInfos().size());

    // Checks for the getNodeCount(opstate, health) method
    assertEquals(nodeCount, map.getNodeCount(null, null));
    assertEquals(1,
        map.getNodeCount(NodeOperationalState.DECOMMISSIONING,
            NodeState.STALE));
    assertEquals(5, map.getNodeCount(null, NodeState.HEALTHY));
    assertEquals(3,
        map.getNodeCount(NodeOperationalState.DECOMMISSIONING, null));
  }

  /**
   * Test if container list is iterable even if it's modified from other thread.
   */
  @Test
  public void testConcurrency() throws Exception {
    NodeStateMap nodeStateMap = new NodeStateMap();

    final DatanodeDetails datanodeDetails =
        MockDatanodeDetails.randomDatanodeDetails();

    nodeStateMap.addNode(datanodeDetails, NodeStatus.inServiceHealthy());

    UUID dnUuid = datanodeDetails.getUuid();

    nodeStateMap.addContainer(dnUuid, new ContainerID(1L));
    nodeStateMap.addContainer(dnUuid, new ContainerID(2L));
    nodeStateMap.addContainer(dnUuid, new ContainerID(3L));

    CountDownLatch elementRemoved = new CountDownLatch(1);
    CountDownLatch loopStarted = new CountDownLatch(1);

    new Thread(() -> {
      try {
        loopStarted.await();
        nodeStateMap.removeContainer(dnUuid, new ContainerID(1L));
        elementRemoved.countDown();
      } catch (Exception e) {
        e.printStackTrace();
      }

    }).start();

    boolean first = true;
    for (ContainerID key : nodeStateMap.getContainers(dnUuid)) {
      if (first) {
        loopStarted.countDown();
        elementRemoved.await();
      }
      first = false;
      System.out.println(key);
    }
  }

  private void addNodeWithState(
      DatanodeDetails dn,
      NodeOperationalState opState, NodeState health
  )
      throws NodeAlreadyExistsException {
    NodeStatus status = new NodeStatus(opState, health);
    map.addNode(dn, status);
  }

  private void addRandomNodeWithState(
      NodeOperationalState opState, NodeState health
  )
      throws NodeAlreadyExistsException {
    DatanodeDetails dn = generateDatanode();
    addNodeWithState(dn, opState, health);
  }

  private DatanodeDetails generateDatanode() {
    return DatanodeDetails.newBuilder().setUuid(UUID.randomUUID()).build();
  }

}
