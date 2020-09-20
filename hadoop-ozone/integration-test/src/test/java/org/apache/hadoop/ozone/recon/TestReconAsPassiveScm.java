/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.CLOSE_CONTAINER;
import static org.apache.hadoop.ozone.container.ozoneimpl.TestOzoneContainer.runTestOzoneContainerViaDataNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Recon's passive SCM integration tests.
 */
public class TestReconAsPassiveScm {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = new Timeout(300000);

  private MiniOzoneCluster cluster = null;
  private OzoneConfiguration conf;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(HDDS_CONTAINER_REPORT_INTERVAL, "5s");
    conf.set(HDDS_PIPELINE_REPORT_INTERVAL, "5s");
    cluster =  MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3)
        .includeRecon(true).build();
    cluster.waitForClusterToBeReady();
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDatanodeRegistrationAndReports() throws Exception {
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
        cluster.getReconServer().getReconStorageContainerManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    PipelineManager reconPipelineManager = reconScm.getPipelineManager();
    PipelineManager scmPipelineManager = scm.getPipelineManager();

    LambdaTestUtils.await(60000, 5000,
        () -> (reconPipelineManager.getPipelines().size() >= 4));

    // Verify if Recon has all the pipelines from SCM.
    scmPipelineManager.getPipelines().forEach(p -> {
      try {
        assertNotNull(reconPipelineManager.getPipeline(p.getId()));
      } catch (PipelineNotFoundException e) {
        Assert.fail();
      }
    });

    // Verify we can never create a pipeline in Recon.
    LambdaTestUtils.intercept(UnsupportedOperationException.class,
        "Trying to create pipeline in Recon, which is prohibited!",
        () -> reconPipelineManager.createPipeline(RATIS, ONE));

    ContainerManager scmContainerManager = scm.getContainerManager();
    assertTrue(scmContainerManager.getContainerIDs().isEmpty());

    // Verify if all the 3 nodes are registered with Recon.
    NodeManager reconNodeManager = reconScm.getScmNodeManager();
    NodeManager scmNodeManager = scm.getScmNodeManager();
    assertEquals(scmNodeManager.getAllNodes().size(),
        reconNodeManager.getAllNodes().size());

    // Create container
    ContainerManager reconContainerManager = reconScm.getContainerManager();
    ContainerInfo containerInfo =
        scmContainerManager.allocateContainer(RATIS, ONE, "test");
    long containerID = containerInfo.getContainerID();
    Pipeline pipeline =
        scmPipelineManager.getPipeline(containerInfo.getPipelineID());
    XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf);
    runTestOzoneContainerViaDataNode(containerID, client);

    // Verify Recon picked up the new container that was created.
    assertEquals(scmContainerManager.getContainerIDs(),
        reconContainerManager.getContainerIDs());

    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(ReconNodeManager.LOG);
    reconScm.getEventQueue().fireEvent(CLOSE_CONTAINER,
        containerInfo.containerID());
    GenericTestUtils.waitFor(() -> logCapturer.getOutput()
            .contains("Ignoring unsupported command closeContainerCommand"),
        1000, 20000);
  }

  @Test
  public void testReconRestart() throws Exception {
    final OzoneStorageContainerManager reconScm =
            cluster.getReconServer().getReconStorageContainerManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();

    // Stop Recon
    ContainerManager scmContainerManager = scm.getContainerManager();
    assertTrue(scmContainerManager.getContainerIDs().isEmpty());
    ContainerManager reconContainerManager = reconScm.getContainerManager();
    assertTrue(reconContainerManager.getContainerIDs().isEmpty());

    LambdaTestUtils.await(60000, 5000,
        () -> (reconScm.getScmNodeManager().getAllNodes().size() == 3));

    cluster.stopRecon();

    // Create container in SCM.
    ContainerInfo containerInfo =
        scmContainerManager.allocateContainer(RATIS, ONE, "test");
    long containerID = containerInfo.getContainerID();
    PipelineManager scmPipelineManager = scm.getPipelineManager();
    Pipeline pipeline =
        scmPipelineManager.getPipeline(containerInfo.getPipelineID());
    XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf);
    runTestOzoneContainerViaDataNode(containerID, client);
    assertFalse(scmContainerManager.getContainerIDs().isEmpty());

    // Close a pipeline
    Optional<Pipeline> pipelineToClose = scmPipelineManager
        .getPipelines(RATIS, ONE)
        .stream()
        .filter(p -> !p.getId().equals(containerInfo.getPipelineID()))
        .findFirst();
    assertTrue(pipelineToClose.isPresent());
    scmPipelineManager.finalizeAndDestroyPipeline(pipelineToClose.get(), false);

    // Start Recon
    cluster.startRecon();

    // Verify if Recon has all the nodes on restart (even if heartbeats are
    // not yet received).
    NodeManager reconNodeManager = reconScm.getScmNodeManager();
    NodeManager scmNodeManager = scm.getScmNodeManager();
    assertEquals(scmNodeManager.getAllNodes().size(),
        reconNodeManager.getAllNodes().size());

    // Verify Recon picks up new container, close pipeline SCM actions.
    OzoneStorageContainerManager newReconScm =
        cluster.getReconServer().getReconStorageContainerManager();
    PipelineManager reconPipelineManager = newReconScm.getPipelineManager();
    assertFalse(
        reconPipelineManager.containsPipeline(pipelineToClose.get().getId()));

    LambdaTestUtils.await(90000, 5000,
        () -> (newReconScm.getContainerManager()
            .exists(ContainerID.valueof(containerID))));
  }
}
