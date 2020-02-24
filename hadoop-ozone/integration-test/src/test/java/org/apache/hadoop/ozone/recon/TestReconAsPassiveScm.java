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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Recon's passive SCM integration tests.
 */
public class TestReconAsPassiveScm {

  private MiniOzoneCluster cluster = null;
  private OzoneConfiguration conf;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    int reconDnPort = NetUtils.getFreeSocketPort();
    conf.set(HDDS_CONTAINER_REPORT_INTERVAL, "10s");
    conf.set(HDDS_PIPELINE_REPORT_INTERVAL, "10s");
    cluster =  MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3)
        .setReconDatanodePort(reconDnPort).build();
    cluster.waitForClusterToBeReady();
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 120000)
  public void testDatanodeRegistrationAndReports() throws Exception {
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
        cluster.getReconServer().getReconStorageContainerManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    PipelineManager reconPipelineManager = reconScm.getPipelineManager();
    PipelineManager scmPipelineManager = scm.getPipelineManager();
    while (scmPipelineManager.getPipelines().size() < 4 ||
        scmPipelineManager.getPipelines().size() >
            reconPipelineManager.getPipelines().size()) {
      Thread.sleep(5000);
    }

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

    // Verify if Recon registered all the nodes.
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
  }

  @Test(timeout = 120000)
  public void testReconRestart() throws Exception {
    OzoneStorageContainerManager reconScm =
            cluster.getReconServer().getReconStorageContainerManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();

    // Stop Recon
    ContainerManager scmContainerManager = scm.getContainerManager();
    assertTrue(scmContainerManager.getContainerIDs().isEmpty());
    ContainerManager reconContainerManager = reconScm.getContainerManager();
    assertTrue(reconContainerManager.getContainerIDs().isEmpty());

    cluster.getReconServer().stop();
    cluster.getReconServer().join();

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
    // Using restart since there is no start API in MiniOzoneCluster.
    cluster.restartReconServer();

    // Verify if Recon has all the nodes on restart (even if heartbeats are
    // not yet received).
    NodeManager reconNodeManager = reconScm.getScmNodeManager();
    NodeManager scmNodeManager = scm.getScmNodeManager();
    assertEquals(scmNodeManager.getAllNodes().size(),
        reconNodeManager.getAllNodes().size());

    // Verify Recon picks up new container, close pipeline SCM actions.
    reconScm = cluster.getReconServer().getReconStorageContainerManager();
    PipelineManager reconPipelineManager = reconScm.getPipelineManager();
    assertFalse(
        reconPipelineManager.containsPipeline(pipelineToClose.get().getId()));

    long startTime = System.currentTimeMillis();
    long endTime = startTime + 60000L;
    boolean containerPresentInRecon =
        reconScm.getContainerManager().exists(ContainerID.valueof(containerID));
    while (endTime > System.currentTimeMillis() && !containerPresentInRecon) {
      containerPresentInRecon = reconScm.getContainerManager()
          .exists(ContainerID.valueof(containerID));
      Thread.sleep(5000);
    }
    assertTrue(containerPresentInRecon);
  }
}
