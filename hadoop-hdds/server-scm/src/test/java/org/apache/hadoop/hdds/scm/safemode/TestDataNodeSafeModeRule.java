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

package org.apache.hadoop.hdds.scm.safemode;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * This class tests DataNodeSafeModeRule.
 */
public class TestDataNodeSafeModeRule {

  @TempDir
  private Path tempDir;
  private DataNodeSafeModeRule rule;
  private EventQueue eventQueue;
  // private SCMServiceManager serviceManager; // Removed
  // private SCMContext scmContext; // Removed
  private NodeManager nodeManager;

  private void setup(int requiredDns) throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        tempDir.toString());
    ozoneConfiguration.setInt(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE, requiredDns);

    // List<ContainerInfo> containers = // Removed
    //     new ArrayList<>(HddsTestUtils.getContainerInfo(1)); // Removed
    nodeManager = mock(NodeManager.class);
    // ContainerManager containerManager = mock(ContainerManager.class); // Removed
    // when(containerManager.getContainers()).thenReturn(containers); // Removed
    eventQueue = new EventQueue();
    // serviceManager = new SCMServiceManager(); // Removed
    // scmContext = SCMContext.emptyContext(); // Removed

    // SCMSafeModeManager scmSafeModeManager = new SCMSafeModeManager(ozoneConfiguration, // Original line
    //     nodeManager, null, containerManager, serviceManager, eventQueue, scmContext); // Original line
    // scmSafeModeManager.start(); // Original line

    // Mock SCMSafeModeManager as it's a dependency for SafeModeExitRule (parent of DataNodeSafeModeRule)
    // for the scmInSafeMode() method.
    SCMSafeModeManager mockScmSafeModeManager = mock(SCMSafeModeManager.class);
    when(mockScmSafeModeManager.getLogger()).thenReturn(LoggerFactory.getLogger(SCMSafeModeManager.class)); // if getLogger() is called

    // rule = SafeModeRuleFactory.getInstance().getSafeModeRule(DataNodeSafeModeRule.class); // Original line
    rule = new DataNodeSafeModeRule(eventQueue, ozoneConfiguration, nodeManager, mockScmSafeModeManager);
    assertNotNull(rule);

    // rule.setValidateBasedOnReportProcessing(true); // Removed
  }

  @Test
  public void testDataNodeSafeModeRuleWithNoNodes() throws Exception {
    int requiredDns = 1;
    setup(requiredDns);

    // Initial state: no healthy nodes
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy())).thenReturn(new ArrayList<>());
    assertFalse(rule.validate(), "Should not validate when no nodes are healthy.");

    // Simulate one node becoming healthy
    List<DatanodeDetails> healthyNodes = new ArrayList<>();
    healthyNodes.add(MockDatanodeDetails.randomDatanodeDetails());
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy())).thenReturn(healthyNodes);
    assertTrue(rule.validate(), "Should validate when enough nodes are healthy.");

    // Verify getNodes was called (getStatusText also calls it)
    // Exact number of times depends on how many times validate() and getStatusText() are called by other parts if any.
    // For this specific test structure, validate() is called twice.
    // If SCMSafeModeManager was fully active, getStatusText might be called too.
    // Let's assume at least 2 calls for the two rule.validate() calls.
    verify(nodeManager, times(2)).getNodes(NodeStatus.inServiceHealthy());
  }

  @Test
  public void testDataNodeSafeModeRuleWithMultipleNodes() throws Exception {
    int requiredDns = 3;
    setup(requiredDns);

    List<DatanodeDetails> healthyNodes = new ArrayList<>();

    // Initial state: no healthy nodes
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy())).thenReturn(healthyNodes);
    assertFalse(rule.validate(), "Should not validate with 0 healthy nodes.");

    // Simulate 1 node becoming healthy
    healthyNodes.add(MockDatanodeDetails.randomDatanodeDetails());
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy())).thenReturn(new ArrayList<>(healthyNodes)); // Return a copy
    assertFalse(rule.validate(), "Should not validate with 1 healthy node, 3 required.");

    // Simulate 2 nodes becoming healthy
    healthyNodes.add(MockDatanodeDetails.randomDatanodeDetails());
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy())).thenReturn(new ArrayList<>(healthyNodes)); // Return a copy
    assertFalse(rule.validate(), "Should not validate with 2 healthy nodes, 3 required.");

    // Simulate 3 nodes becoming healthy
    healthyNodes.add(MockDatanodeDetails.randomDatanodeDetails());
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy())).thenReturn(new ArrayList<>(healthyNodes)); // Return a copy
    assertTrue(rule.validate(), "Should validate with 3 healthy nodes.");
    
    // Called 4 times (once for each validate call)
    verify(nodeManager, times(4)).getNodes(NodeStatus.inServiceHealthy());
  }

  @Test
  public void testDataNodeSafeModeRuleWithNodeManager() throws Exception {
    int requiredDns = 2;
    setup(requiredDns);

    // Initial state: no healthy nodes
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy())).thenReturn(new ArrayList<>());
    assertFalse(rule.validate());

    // Simulate enough nodes becoming healthy
    List<DatanodeDetails> healthyNodesList = new ArrayList<>();
    for (int i = 0; i < requiredDns; i++) {
      healthyNodesList.add(MockDatanodeDetails.randomDatanodeDetails());
    }
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy())).thenReturn(healthyNodesList);
    assertTrue(rule.validate());

    // verify it was called for each validate()
    verify(nodeManager, times(2)).getNodes(NodeStatus.inServiceHealthy());
  }
}
