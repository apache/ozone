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

import java.time.Duration;
import java.util.List;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.hadoop.test.LambdaTestUtils;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.pojos.UnhealthyContainers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Integration Tests for Recon's tasks.
 */
public class TestReconTasks {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private MiniOzoneCluster cluster = null;
  private OzoneConfiguration conf;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(HDDS_CONTAINER_REPORT_INTERVAL, "5s");
    conf.set(HDDS_PIPELINE_REPORT_INTERVAL, "5s");

    ReconTaskConfig taskConfig = conf.getObject(ReconTaskConfig.class);
    taskConfig.setMissingContainerTaskInterval(Duration.ofSeconds(15));
    conf.setFromObject(taskConfig);

    conf.set("ozone.scm.stale.node.interval", "10s");
    conf.set("ozone.scm.dead.node.interval", "20s");
    cluster =  MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1)
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
  public void testMissingContainerDownNode() throws Exception {
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            cluster.getReconServer().getReconStorageContainerManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    PipelineManager reconPipelineManager = reconScm.getPipelineManager();
    PipelineManager scmPipelineManager = scm.getPipelineManager();

    // Make sure Recon's pipeline state is initialized.
    LambdaTestUtils.await(60000, 5000,
        () -> (reconPipelineManager.getPipelines().size() >= 1));

    ContainerManager scmContainerManager = scm.getContainerManager();
    ReconContainerManager reconContainerManager =
        (ReconContainerManager) reconScm.getContainerManager();
    ContainerInfo containerInfo =
        scmContainerManager.allocateContainer(RATIS, ONE, "test");
    long containerID = containerInfo.getContainerID();
    Pipeline pipeline =
        scmPipelineManager.getPipeline(containerInfo.getPipelineID());
    XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf);
    runTestOzoneContainerViaDataNode(containerID, client);

    // Make sure Recon got the container report with new container.
    assertEquals(scmContainerManager.getContainerIDs(),
        reconContainerManager.getContainerIDs());

    // Bring down the Datanode that had the container replica.
    cluster.shutdownHddsDatanode(pipeline.getFirstNode());

    LambdaTestUtils.await(120000, 10000, () -> {
      List<UnhealthyContainers> allMissingContainers =
          reconContainerManager.getContainerSchemaManager()
              .getUnhealthyContainers(
                  ContainerSchemaDefinition.UnHealthyContainerStates.MISSING,
                  0, 1000);
      return (allMissingContainers.size() == 1);
    });

    // Restart the Datanode to make sure we remove the missing container.
    cluster.restartHddsDatanode(pipeline.getFirstNode(), true);
    LambdaTestUtils.await(120000, 10000, () -> {
      List<UnhealthyContainers> allMissingContainers =
          reconContainerManager.getContainerSchemaManager()
              .getUnhealthyContainers(
                  ContainerSchemaDefinition.UnHealthyContainerStates.MISSING,
                  0, 1000);
      return (allMissingContainers.isEmpty());
    });
  }
}
