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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.QUASI_CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.OPEN;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.STARTING_FINALIZATION;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.StatusAndMessages;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private StorageContainerManager scm;
  private ContainerManagerV2 scmContainerManager;
  private PipelineManager scmPipelineManager;
  private Pipeline ratisPipeline1;
  private final int numContainersCreated = 1;
  private HDDSLayoutVersionManager scmVersionManager;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1000,
            TimeUnit.MILLISECONDS);

    conf.set(OZONE_DATANODE_PIPELINE_LIMIT, "1");
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(NUM_DATA_NODES)
        // allow only one FACTOR THREE pipeline.
        .setTotalPipelineNumLimit(NUM_DATA_NODES + 1)
        .setHbInterval(1000)
        .setHbProcessorInterval(1000)
        .setScmLayoutVersion(INITIAL_VERSION.layoutVersion())
        .setDnLayoutVersion(INITIAL_VERSION.layoutVersion())
        .build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    scmContainerManager = scm.getContainerManager();
    scmPipelineManager = scm.getPipelineManager();
    scmVersionManager = scm.getLayoutVersionManager();

  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void testPreUpgradeConditionsSCM() {
    Assert.assertEquals(INITIAL_VERSION.layoutVersion(),
        scmVersionManager.getMetadataLayoutVersion());
    for (ContainerInfo ci : scmContainerManager.getContainers()) {
      Assert.assertEquals(HddsProtos.LifeCycleState.OPEN, ci.getState());
    }
  }

  private void testPostUpgradeConditionsSCM() {
    Assert.assertEquals(scmVersionManager.getSoftwareLayoutVersion(),
        scmVersionManager.getMetadataLayoutVersion());
    Assert.assertTrue(scmVersionManager.getMetadataLayoutVersion() >= 1);

    // SCM should not return from finalization until there is at least one
    // pipeline to use.
    int pipelineCount = scmPipelineManager.getPipelines(RATIS, THREE, OPEN)
        .size();
    Assert.assertTrue(pipelineCount >= 1);

    // SCM will not return from finalization until there is at least one
    // RATIS 3 pipeline. For this to exist, all three of our datanodes must
    // be in the HEALTHY state.
    testDataNodesStateOnSCM(HEALTHY);

    int countContainers = 0;
    for (ContainerInfo ci : scmContainerManager.getContainers()) {
      HddsProtos.LifeCycleState ciState = ci.getState();
      Assert.assertTrue((ciState == HddsProtos.LifeCycleState.CLOSED) ||
          (ciState == HddsProtos.LifeCycleState.CLOSING) ||
          (ciState == HddsProtos.LifeCycleState.QUASI_CLOSED));
      countContainers++;
    }
    Assert.assertEquals(numContainersCreated, countContainers);
  }

  private void testPreUpgradeConditionsDataNodes() {
    for (HddsDatanodeService dataNode : cluster.getHddsDatanodes()) {
      DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
      HDDSLayoutVersionManager dnVersionManager =
          dsm.getLayoutVersionManager();
      Assert.assertEquals(0, dnVersionManager.getMetadataLayoutVersion());

    }

    int countContainers = 0;
    for (HddsDatanodeService dataNode : cluster.getHddsDatanodes()) {
      DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
      // Also verify that all the existing containers are open.
      for (Iterator<Container<?>> it =
           dsm.getContainer().getController().getContainers(); it.hasNext();) {
        Container container = it.next();
        Assert.assertTrue(container.getContainerState() ==
            ContainerProtos.ContainerDataProto.State.OPEN);
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
          try {
            if (dsm.queryUpgradeStatus().status() != FINALIZATION_DONE) {
              return false;
            }
          } catch (IOException e) {
            return false;
          }
        }
        return true;
      }, 2000, 20000);
    } catch (TimeoutException | InterruptedException e) {
      Assert.fail("Timeout waiting for Upgrade to complete on Data Nodes.");
    }

    int countContainers = 0;
    for (HddsDatanodeService dataNode : cluster.getHddsDatanodes()) {
      DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
      HDDSLayoutVersionManager dnVersionManager =
          dsm.getLayoutVersionManager();
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

  private void waitForPipelineCreated() throws Exception {
    LambdaTestUtils.await(10000, 2000, () -> {
      List<Pipeline> pipelines =
          scmPipelineManager.getPipelines(RATIS, THREE, OPEN);
      return pipelines.size() == 1;
    });
  }

  @Test
  public void testFinalizationFromInitialVersionToLatestVersion()
      throws Exception {

    waitForPipelineCreated();

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
    xceiverClientManager.releaseClient(client1, false);

    // Test the Pre-Upgrade conditions on SCM as well as DataNodes.
    testPreUpgradeConditionsSCM();
    testPreUpgradeConditionsDataNodes();

    Set<PipelineID> preUpgradeOpenPipelines =
        scmPipelineManager.getPipelines(RATIS, THREE, OPEN)
            .stream()
            .map(Pipeline::getId)
            .collect(Collectors.toSet());

    // Trigger Finalization on the SCM
    StatusAndMessages status = scm.finalizeUpgrade("xyz");
    Assert.assertEquals(STARTING_FINALIZATION, status.status());

    // Wait for the Finalization to complete on the SCM.
    while (status.status() != FINALIZATION_DONE) {
      status = scm.queryUpgradeFinalizationProgress("xyz", false);
    }

    Set<PipelineID> postUpgradeOpenPipelines =
        scmPipelineManager.getPipelines(RATIS, THREE, OPEN)
            .stream()
            .map(Pipeline::getId)
            .collect(Collectors.toSet());

    // No pipelines from before the upgrade should still be open after the
    // upgrade.
    long numPreUpgradeOpenPipelines = preUpgradeOpenPipelines
        .stream()
        .filter(postUpgradeOpenPipelines::contains)
        .count();
    Assert.assertEquals(0, numPreUpgradeOpenPipelines);

    // Verify Post-Upgrade conditions on the SCM.
    testPostUpgradeConditionsSCM();

    // Verify the SCM has driven all the DataNodes through Layout Upgrade.
    testPostUpgradeConditionsDataNodes();

    // Test that we can use a pipeline after upgrade.
    // Will fail with exception if there are no pipelines.
    ObjectStore store = cluster.getClient().getObjectStore();
    store.createVolume("vol1");
    store.getVolume("vol1").createBucket("buc1");
    store.getVolume("vol1").getBucket("buc1").createKey("key1", 100,
        ReplicationType.RATIS, ReplicationFactor.THREE, new HashMap<>());
  }
}
