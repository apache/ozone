/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.upgrade;

import static java.lang.Thread.sleep;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.OPEN;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.QUASI_CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY_READONLY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.STARTING_FINALIZATION;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.StatusAndMessages;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test SCM and DataNode Upgrade sequence.
 */
public class TestHDDSUpgrade {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = new Timeout(300000);
  private static final Logger LOG =
      LoggerFactory.getLogger(TestHDDSUpgrade.class);
  private static final int NUM_DATA_NODES = 3;

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerManager scm;
  private static ContainerManager scmContainerManager;
  private static PipelineManager scmPipelineManager;
  private static Pipeline ratisPipeline1;
  private static final int CONTAINERS_CREATED_FOR_TESTING = 1;
  private static HDDSLayoutVersionManager scmVersionManager;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1000,
            TimeUnit.MILLISECONDS);
    int numOfNodes = NUM_DATA_NODES;
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numOfNodes)
        // allow only one FACTOR THREE pipeline.
        .setTotalPipelineNumLimit(numOfNodes + 1)
        .setHbInterval(1000)
        .setHbProcessorInterval(1000)
        .build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    scmContainerManager = scm.getContainerManager();
    scmPipelineManager = scm.getPipelineManager();
    scmVersionManager = scm.getLayoutVersionManager();

    // we will create CONTAINERS_CREATED_FOR_TESTING number of containers.
    XceiverClientManager xceiverClientManager = new XceiverClientManager(conf);
    ContainerInfo ci1 = scmContainerManager.allocateContainer(
        RATIS, THREE, "Owner1");
    ratisPipeline1 = scmPipelineManager.getPipeline(ci1.getPipelineID());
    scmPipelineManager.openPipeline(ratisPipeline1.getId());
    XceiverClientSpi client1 =
        xceiverClientManager.acquireClient(ratisPipeline1);
    ContainerProtocolCalls.createContainer(client1,
        ci1.getContainerID(), null);
    // At this stage, there should be 1 pipeline one with 1 open container
    // each.
    xceiverClientManager.releaseClient(client1, false);
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void testPreUpgradeConditionsSCM() {
    Assert.assertEquals(0, scmVersionManager.getMetadataLayoutVersion());
    for (ContainerInfo ci : scmContainerManager.getContainers()) {
      Assert.assertEquals(ci.getState(), HddsProtos.LifeCycleState.OPEN);
    }
  }

  private void testPostUpgradeConditionsSCM() {
    Assert.assertEquals(scmVersionManager.getSoftwareLayoutVersion(),
        scmVersionManager.getMetadataLayoutVersion());
    Assert.assertTrue(scmVersionManager.getMetadataLayoutVersion() >= 1);
    int countContainers = 0;
    for (ContainerInfo ci : scmContainerManager.getContainers()) {
      HddsProtos.LifeCycleState ciState = ci.getState();
      Assert.assertTrue((ciState == HddsProtos.LifeCycleState.CLOSED) ||
          (ciState == HddsProtos.LifeCycleState.CLOSING) ||
          (ciState == HddsProtos.LifeCycleState.QUASI_CLOSED));
      countContainers++;
    }
    Assert.assertEquals(CONTAINERS_CREATED_FOR_TESTING, countContainers);
  }

  private void testPreUpgradeConditionsDataNodes() {
    for (HddsDatanodeService dataNode : cluster.getHddsDatanodes()) {
      DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
      HDDSLayoutVersionManager dnVersionManager =
          dsm.getDataNodeVersionManager();
      Assert.assertEquals(0, dnVersionManager.getMetadataLayoutVersion());

    }

    int countContainers = 0;
    for (HddsDatanodeService dataNode : cluster.getHddsDatanodes()) {
      DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
      // Also verify that all the existing containers are open.
      for (Iterator<Container<?>> it =
           dsm.getContainer().getController().getContainers(); it.hasNext();) {
        Container container = it.next();
        Assert.assertTrue(container.getContainerState() == OPEN);
        countContainers++;
      }
    }
    Assert.assertTrue(countContainers >= 1);
  }


  private void testPostUpgradeConditionsDataNodes() {
    try {
      GenericTestUtils.waitFor(() -> {
        for (HddsDatanodeService dataNode : cluster.getHddsDatanodes()) {
          DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
          HDDSLayoutVersionManager dnVersionManager =
              dsm.getDataNodeVersionManager();
          try {
            if (dsm.queryUpgradeStatus().status() != FINALIZATION_DONE) {
              return false;
            }
          } catch (IOException e) {
            e.printStackTrace();
            return false;
          }
        }
        return true;
      }, 1000, 20000);
    } catch (TimeoutException | InterruptedException e) {
      Assert.fail("Timeout waiting for Upgrade to complete on Data Nodes.");
    }

    int countContainers = 0;
    for (HddsDatanodeService dataNode : cluster.getHddsDatanodes()) {
      DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
      HDDSLayoutVersionManager dnVersionManager =
          dsm.getDataNodeVersionManager();
      Assert.assertEquals(dnVersionManager.getSoftwareLayoutVersion(),
          dnVersionManager.getMetadataLayoutVersion());
      Assert.assertTrue(dnVersionManager.getMetadataLayoutVersion() >= 1);

      // Also verify that all the existing containers are closed.
      for (Iterator<Container<?>> it =
           dsm.getContainer().getController().getContainers(); it.hasNext();) {
        Container container = it.next();
        Assert.assertTrue(container.getContainerState() == CLOSED ||
            container.getContainerState() == QUASI_CLOSED);
        countContainers++;
      }
    }
    Assert.assertTrue(countContainers >= 1);
  }

  private void testPostUpgradePipelineCreation() throws IOException {
    ratisPipeline1 = scmPipelineManager.createPipeline(RATIS, THREE);
    scmPipelineManager.openPipeline(ratisPipeline1.getId());
    Assert.assertEquals(0,
        scmPipelineManager.getNumberOfContainers(ratisPipeline1.getId()));
    PipelineID pid = scmContainerManager.allocateContainer(RATIS, THREE,
        "Owner1").getPipelineID();
    Assert.assertEquals(1, scmPipelineManager.getNumberOfContainers(pid));
    Assert.assertEquals(pid, ratisPipeline1.getId());
  }

  private void testDataNodesStateOnSCM(NodeState state) {
    int countNodes = 0;
    for (DatanodeDetails dn : scm.getScmNodeManager().getAllNodes()){
      try {
        Assert.assertEquals(state,
            scm.getScmNodeManager().getNodeStatus(dn).getHealth());
      } catch (NodeNotFoundException e) {
        e.printStackTrace();
        Assert.fail("Node not found");
      }
      ++countNodes;
    }
    Assert.assertEquals(NUM_DATA_NODES, countNodes);
  }

  @Test
  public void testLayoutUpgrade() throws IOException, InterruptedException {
    // Test the Pre-Upgrade conditions on SCM as well as DataNodes.
    testPreUpgradeConditionsSCM();
    testPreUpgradeConditionsDataNodes();

    // Trigger Finalization on the SCM
    StatusAndMessages status = scm.finalizeUpgrade("xyz");
    Assert.assertEquals(STARTING_FINALIZATION, status.status());

    // Wait for the Finalization to complete on the SCM.
    while (status.status() != FINALIZATION_DONE) {
      status = scm.queryUpgradeFinalizationProgress("xyz", false);
    }

    // Verify Post-Upgrade conditions on the SCM.
    testPostUpgradeConditionsSCM();

    // All datanodes on the SCM should have moved to HEALTHY-READONLY state.
    testDataNodesStateOnSCM(HEALTHY_READONLY);

    // Verify the SCM has driven all the DataNodes through Layout Upgrade.
    sleep(5000);
    testPostUpgradeConditionsDataNodes();

    // Allow some time for heartbeat exchanges.
    sleep(5000);

    // All datanodes on the SCM should have moved to HEALTHY-READ-WRITE state.
    testDataNodesStateOnSCM(HEALTHY);

    // Verify that new pipeline can be created with upgraded datanodes.
    testPostUpgradePipelineCreation();
  }
}
