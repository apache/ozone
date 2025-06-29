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
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.ozone.test.GenericTestUtils;
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
  private NodeManager nodeManager;
  private SCMSafeModeManager mockSafeModeManager;

  private void setup(int requiredDns) throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        tempDir.toString());
    ozoneConfiguration.setInt(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE, requiredDns);

    nodeManager = mock(NodeManager.class);
    eventQueue = new EventQueue();

    mockSafeModeManager = mock(SCMSafeModeManager.class);

    rule = new DataNodeSafeModeRule(eventQueue, ozoneConfiguration, nodeManager, mockSafeModeManager);
    assertNotNull(rule);

    rule.setValidateBasedOnReportProcessing(true);
  }

  @Test
  public void testDataNodeSafeModeRuleWithNoNodes() throws Exception {
    int requiredDns = 1;
    setup(requiredDns);
    when(mockSafeModeManager.getInSafeMode()).thenReturn(true);

    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(
            LoggerFactory.getLogger(SCMSafeModeManager.class));

    assertFalse(rule.validate());

    DatanodeDetails dd = MockDatanodeDetails.randomDatanodeDetails();
    NodeRegistrationContainerReport nodeReg = 
        new NodeRegistrationContainerReport(dd, null);
    
    eventQueue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT, nodeReg);

    GenericTestUtils.waitFor(() -> logCapturer.getOutput().contains(
        "SCM in safe mode. 1 DataNodes registered, 1 required."), 1000, 5000);

    assertTrue(rule.validate());
  }

  @Test
  public void testDataNodeSafeModeRuleWithMultipleNodes() throws Exception {
    int requiredDns = 3;
    setup(requiredDns);
    when(mockSafeModeManager.getInSafeMode()).thenReturn(true);

    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(
            LoggerFactory.getLogger(SCMSafeModeManager.class));

    assertFalse(rule.validate());

    for (int i = 0; i < 2; i++) {
      DatanodeDetails dd = MockDatanodeDetails.randomDatanodeDetails();
      NodeRegistrationContainerReport nodeReg = 
          new NodeRegistrationContainerReport(dd, null);
      
      eventQueue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT, nodeReg);
    }

    GenericTestUtils.waitFor(() -> logCapturer.getOutput().contains(
        "SCM in safe mode. 2 DataNodes registered, 3 required."), 1000, 5000);

    assertFalse(rule.validate());

    DatanodeDetails dd = MockDatanodeDetails.randomDatanodeDetails();
    NodeRegistrationContainerReport nodeReg = 
        new NodeRegistrationContainerReport(dd, null);
    
    eventQueue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT, nodeReg);

    GenericTestUtils.waitFor(() -> logCapturer.getOutput().contains(
        "SCM in safe mode. 3 DataNodes registered, 3 required."), 1000, 5000);

    assertTrue(rule.validate());
  }

  @Test
  public void testDataNodeSafeModeRuleWithNodeManager() throws Exception {
    int requiredDns = 2;
    setup(requiredDns);
    when(mockSafeModeManager.getInSafeMode()).thenReturn(true);
    
    rule.setValidateBasedOnReportProcessing(false);

    when(nodeManager.getNodes(NodeStatus.inServiceHealthy())).thenReturn(new ArrayList<>());

    assertFalse(rule.validate());

    List<DatanodeDetails> healthyNodes = new ArrayList<>();
    for (int i = 0; i < requiredDns; i++) {
      DatanodeDetails dd = MockDatanodeDetails.randomDatanodeDetails();
      healthyNodes.add(dd);
    }
    
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenReturn(healthyNodes);

    assertTrue(rule.validate());
    
    verify(nodeManager, times(2)).getNodes(NodeStatus.inServiceHealthy());
  }
}
