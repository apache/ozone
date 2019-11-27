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

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.MockRatisPipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static junit.framework.TestCase.assertEquals;

/**
 * Tests to ensure the DatanodeAdminMonitor is working correctly.
 */
public class TestDatanodeAdminMonitor {

  private StorageContainerManager scm;
  private NodeManager nodeManager;
  private ContainerManager containerManager;
  private SCMPipelineManager pipelineManager;
  private OzoneConfiguration conf;
  private DatanodeAdminMonitor monitor;
  private DatanodeDetails datanode1;
  private DatanodeDetails datanode2;
  private DatanodeDetails datanode3;

  @Before
  public void setup() throws IOException, AuthenticationException {
    // This creates a mocked cluster of 6 nodes, where there are mock pipelines
    // etc. Borrows heavily from TestDeadNodeHandler.
    conf = new OzoneConfiguration();
    String storageDir = GenericTestUtils.getTempPath(
        TestDeadNodeHandler.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, storageDir);

    scm = HddsTestUtils.getScm(conf);
    nodeManager = scm.getScmNodeManager();
    pipelineManager = (SCMPipelineManager)scm.getPipelineManager();
    containerManager = scm.getContainerManager();

    monitor = new DatanodeAdminMonitor(conf);
    monitor.setEventQueue(scm.getEventQueue());
    monitor.setNodeManager(nodeManager);
    monitor.setPipelineManager(pipelineManager);

    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), conf);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);

    datanode1 = TestUtils.randomDatanodeDetails();
    datanode2 = TestUtils.randomDatanodeDetails();
    datanode3 = TestUtils.randomDatanodeDetails();

    String storagePath = GenericTestUtils.getRandomizedTempPath()
        .concat("/" + datanode1.getUuidString());

    StorageContainerDatanodeProtocolProtos.StorageReportProto
        storageOne = TestUtils.createStorageReport(
        datanode1.getUuid(), storagePath, 100, 10, 90, null);

    nodeManager.register(datanode1,
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(datanode2,
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(datanode3,
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(TestUtils.randomDatanodeDetails(),
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(TestUtils.randomDatanodeDetails(),
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(TestUtils.randomDatanodeDetails(),
        TestUtils.createNodeReport(storageOne), null);
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


  @Test
  @Ignore // HDDS-2631
  public void testMonitoredNodeHasPipelinesClosed()
      throws NodeNotFoundException, TimeoutException, InterruptedException {

    GenericTestUtils.waitFor(() -> nodeManager
        .getPipelines(datanode1).size() == 2, 100, 20000);

    nodeManager.setNodeOperationalState(datanode1,
        HddsProtos.NodeOperationalState.DECOMMISSIONING);
    monitor.startMonitoring(datanode1, 0);
    monitor.run();
    // Ensure the node moves from pending to tracked
    assertEquals(0, monitor.getPendingCount());
    assertEquals(1, monitor.getTrackedNodeCount());

    // Ensure the pipelines are closed, as this is the first step in the admin
    // workflow
    GenericTestUtils.waitFor(() -> nodeManager
        .getPipelines(datanode1).size() == 0, 100, 20000);

    // Run the run loop again and ensure the tracked node is moved to the next
    // state
    monitor.run();
    for (DatanodeAdminNodeDetails node : monitor.getTrackedNodes()) {
      assertEquals(
          DatanodeAdminMonitor.States.GET_CONTAINERS, node.getCurrentState());
    }
    // Finally, cancel decommission and see the node is removed from tracking
    monitor.stopMonitoring(datanode1);
    monitor.run();
    assertEquals(0, monitor.getTrackedNodeCount());
  }

}