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

package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.CLOSE_CONTAINER;
import static org.apache.hadoop.ozone.container.ozoneimpl.TestOzoneContainer.runTestOzoneContainerViaDataNode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Optional;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
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
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

/**
 * Recon's passive SCM integration tests.
 */
public class TestReconAsPassiveScm {
  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private ReconService recon;

  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(HDDS_CONTAINER_REPORT_INTERVAL, "5s");
    conf.set(HDDS_PIPELINE_REPORT_INTERVAL, "5s");
    recon = new ReconService(conf);
    cluster =  MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .addService(recon)
        .build();
    cluster.waitForClusterToBeReady();
    GenericTestUtils.setLogLevel(ReconNodeManager.class, Level.DEBUG);
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDatanodeRegistrationAndReports() throws Exception {
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
        recon.getReconServer().getReconStorageContainerManager();
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
        fail();
      }
    });

    // Verify we can never create a pipeline in Recon.
    UnsupportedOperationException exception = assertThrows(
        UnsupportedOperationException.class,
        () -> reconPipelineManager
            .createPipeline(RatisReplicationConfig.getInstance(ONE)));
    assertTrue(exception.getMessage()
        .contains("Trying to create pipeline in Recon, which is prohibited!"));

    ContainerManager scmContainerManager = scm.getContainerManager();
    assertTrue(scmContainerManager.getContainers().isEmpty());

    // Verify if all the 3 nodes are registered with Recon.
    NodeManager reconNodeManager = reconScm.getScmNodeManager();
    NodeManager scmNodeManager = scm.getScmNodeManager();
    assertEquals(scmNodeManager.getAllNodes().size(),
        reconNodeManager.getAllNodes().size());

    // Create container
    ContainerManager reconContainerManager = reconScm.getContainerManager();
    ContainerInfo containerInfo =
        scmContainerManager
            .allocateContainer(RatisReplicationConfig.getInstance(ONE), "test");
    long containerID = containerInfo.getContainerID();
    Pipeline pipeline =
        scmPipelineManager.getPipeline(containerInfo.getPipelineID());
    XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf);
    runTestOzoneContainerViaDataNode(containerID, client);

    // Verify Recon picked up the new container that was created.
    assertEquals(scmContainerManager.getContainers(),
        reconContainerManager.getContainers());

    LogCapturer logCapturer = LogCapturer.captureLogs(ReconNodeManager.class);
    GenericTestUtils.setLogLevel(ReconNodeManager.class, Level.DEBUG);
    reconScm.getEventQueue().fireEvent(CLOSE_CONTAINER,
        containerInfo.containerID());
    GenericTestUtils.waitFor(() -> logCapturer.getOutput()
            .contains("Ignoring unsupported command closeContainerCommand"),
        1000, 20000);
  }

  @Test
  public void testReconRestart() throws Exception {
    final OzoneStorageContainerManager reconScm =
            recon.getReconServer().getReconStorageContainerManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();

    // Stop Recon
    ContainerManager scmContainerManager = scm.getContainerManager();
    assertTrue(scmContainerManager.getContainers().isEmpty());
    ContainerManager reconContainerManager = reconScm.getContainerManager();
    assertTrue(reconContainerManager.getContainers().isEmpty());

    LambdaTestUtils.await(60000, 5000,
        () -> (reconScm.getScmNodeManager().getAllNodes().size() == 3));

    recon.stop();

    // Create container in SCM.
    ContainerInfo containerInfo =
        scmContainerManager
            .allocateContainer(RatisReplicationConfig.getInstance(ONE), "test");
    long containerID = containerInfo.getContainerID();
    PipelineManager scmPipelineManager = scm.getPipelineManager();
    Pipeline pipeline =
        scmPipelineManager.getPipeline(containerInfo.getPipelineID());
    XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf);
    runTestOzoneContainerViaDataNode(containerID, client);
    assertFalse(scmContainerManager.getContainers().isEmpty());

    // Close a pipeline
    Optional<Pipeline> pipelineToClose = scmPipelineManager
        .getPipelines(RatisReplicationConfig.getInstance(ONE))
        .stream()
        .filter(p -> !p.getId().equals(containerInfo.getPipelineID()))
        .findFirst();
    assertTrue(pipelineToClose.isPresent());
    scmPipelineManager.closePipeline(pipelineToClose.get().getId());
    scmPipelineManager.deletePipeline(pipelineToClose.get().getId());

    // Start Recon
    recon.start(cluster.getConf());

    // Verify if Recon has all the nodes on restart (even if heartbeats are
    // not yet received).
    NodeManager reconNodeManager = reconScm.getScmNodeManager();
    NodeManager scmNodeManager = scm.getScmNodeManager();
    assertEquals(scmNodeManager.getAllNodes().size(),
        reconNodeManager.getAllNodes().size());

    // Verify Recon picks up new container, close pipeline SCM actions.
    OzoneStorageContainerManager newReconScm =
        recon.getReconServer().getReconStorageContainerManager();
    PipelineManager reconPipelineManager = newReconScm.getPipelineManager();
    assertFalse(
        reconPipelineManager.containsPipeline(pipelineToClose.get().getId()));

    LambdaTestUtils.await(90000, 5000,
        () -> (newReconScm.getContainerManager()
            .containerExist(ContainerID.valueOf(containerID))));
  }
}
