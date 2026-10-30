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

package org.apache.hadoop.ozone.om.jobworker.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.ozone.om.jobworker.states.JobworkerNodeAlreadyExistsException;
import org.apache.hadoop.ozone.om.jobworker.states.JobworkerNodeNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the JobworkerNodeStateManager class.
 */
public class TestJobworkerNodeStateManager {

  private JobworkerNodeStateManager stateManager;

  @BeforeEach
  public void setUp() {
    stateManager = new JobworkerNodeStateManager();
  }

  @AfterEach
  public void tearDown() throws IOException {
    stateManager.close();
  }

  @Test
  public void testAddAndRetrieveNode() throws JobworkerNodeAlreadyExistsException,
      JobworkerNodeNotFoundException {
    // Create a jobworker node, then add and retrieve it
    JobworkerDetails jobworker = MockJobworkerDetails.randomJobworkerDetails();
    stateManager.addNode(jobworker);
    JobworkerInfo retrievedInfo = stateManager.getNodeInfo(jobworker.getUuid());

    assertEquals(jobworker.getUuid(), retrievedInfo.getUuid());

    // Verify the newly added node is in IN_SERVICE and HEALTHY state
    JobworkerNodeStatus expectedStatus = JobworkerNodeStatus.inServiceHealthy();
    assertEquals(expectedStatus, retrievedInfo.getNodeStatus());
  }

  @Test
  public void testAddDuplicateNode() throws JobworkerNodeAlreadyExistsException {
    JobworkerDetails jobworker = MockJobworkerDetails.randomJobworkerDetails();
    stateManager.addNode(jobworker);

    // Try to add the same node again
    assertThrows(JobworkerNodeAlreadyExistsException.class,
        () -> stateManager.addNode(jobworker));
  }

  @Test
  public void testRemoveNode() throws JobworkerNodeAlreadyExistsException,
      JobworkerNodeNotFoundException {
    JobworkerDetails jobworker = MockJobworkerDetails.randomJobworkerDetails();
    stateManager.addNode(jobworker);

    // Verify node exists
    assertNotNull(stateManager.getNodeInfo(jobworker.getUuid()));
    assertEquals(1, stateManager.getTotalNodeCount());

    // Remove the node
    stateManager.removeNode(jobworker.getUuid());

    // Verify node is gone
    assertEquals(0, stateManager.getTotalNodeCount());
    assertThrows(JobworkerNodeNotFoundException.class,
        () -> stateManager.getNodeInfo(jobworker.getUuid()));
  }

  @Test
  public void testRemoveNonExistentNode() {
    UUID nonExistentUuid = UUID.randomUUID();

    // Try to remove a node that doesn't exist
    assertThrows(JobworkerNodeNotFoundException.class,
        () -> stateManager.removeNode(nonExistentUuid));
  }

  @Test
  public void testUpdateLastHeartbeatTime() throws JobworkerNodeAlreadyExistsException,
      JobworkerNodeNotFoundException {
    JobworkerDetails jobworker = MockJobworkerDetails.randomJobworkerDetails();
    stateManager.addNode(jobworker);

    JobworkerInfo info = stateManager.getNodeInfo(jobworker.getUuid());
    long initialTime = info.getLastHeartbeatTime();

    // Sleep a bit to ensure time difference
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Update heartbeat time
    stateManager.updateLastHeartbeatTime(jobworker);

    // Verify heartbeat time was updated
    long updatedTime = stateManager.getNodeInfo(jobworker.getUuid()).getLastHeartbeatTime();
    assertTrue(updatedTime > initialTime,
        "Heartbeat time should have been updated to a later time");
  }

  @Test
  public void testGetAllJobworkerInfos() throws JobworkerNodeAlreadyExistsException {
    // Add multiple nodes
    JobworkerDetails jobworker1 = MockJobworkerDetails.randomJobworkerDetails();
    JobworkerDetails jobworker2 = MockJobworkerDetails.randomJobworkerDetails();
    JobworkerDetails jobworker3 = MockJobworkerDetails.randomJobworkerDetails();

    stateManager.addNode(jobworker1);
    stateManager.addNode(jobworker2);
    stateManager.addNode(jobworker3);

    // Get all nodes
    List<JobworkerInfo> allNodes = stateManager.getAllJobworkerInfos();

    assertEquals(3, allNodes.size());
    assertEquals(3, stateManager.getTotalNodeCount());

    // Verify all nodes are present
    assertTrue(allNodes.stream()
        .anyMatch(info -> info.getUuid().equals(jobworker1.getUuid())));
    assertTrue(allNodes.stream()
        .anyMatch(info -> info.getUuid().equals(jobworker2.getUuid())));
    assertTrue(allNodes.stream()
        .anyMatch(info -> info.getUuid().equals(jobworker3.getUuid())));
  }

  @Test
  public void testGetNodeCountByHealthState() throws JobworkerNodeAlreadyExistsException,
      JobworkerNodeNotFoundException {
    // Add nodes in different states
    JobworkerDetails healthyNode = MockJobworkerDetails.randomJobworkerDetails();
    stateManager.addNode(healthyNode);

    JobworkerDetails staleNode = MockJobworkerDetails.randomJobworkerDetails();
    stateManager.addNode(staleNode);

    // Make stale node actually stale by setting its status
    JobworkerInfo staleInfo = stateManager.getNodeInfo(staleNode.getUuid());
    staleInfo.setNodeStatus(new JobworkerNodeStatus(
        NodeOperationalState.IN_SERVICE, NodeState.STALE));

    // Count nodes by state
    assertEquals(1, stateManager.getNodeCount(NodeState.HEALTHY));
    assertEquals(1, stateManager.getNodeCount(NodeState.STALE));
    assertEquals(0, stateManager.getNodeCount(NodeState.DEAD));
  }

}
