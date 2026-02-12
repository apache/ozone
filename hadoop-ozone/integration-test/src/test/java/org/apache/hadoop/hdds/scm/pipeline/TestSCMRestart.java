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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test SCM restart and recovery wrt pipelines.
 */
public class TestSCMRestart {

  private static MiniOzoneCluster cluster;
  private static Pipeline ratisPipeline1;
  private static Pipeline ratisPipeline2;
  private static ContainerManager newContainerManager;
  private static PipelineManager pipelineManager;

  @BeforeAll
  public static void init() throws Exception {
    final int numOfNodes = 4;
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1000,
            TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 1000, TimeUnit.MILLISECONDS);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    // allow only one FACTOR THREE pipeline.
    conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT, numOfNodes + 1);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numOfNodes)
        .build();
    cluster.waitForClusterToBeReady();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    ContainerManager containerManager = scm.getContainerManager();
    pipelineManager = scm.getPipelineManager();
    ratisPipeline1 = pipelineManager.getPipeline(
        containerManager.allocateContainer(
            RatisReplicationConfig.getInstance(
                ReplicationFactor.THREE), "Owner1").getPipelineID());
    pipelineManager.openPipeline(ratisPipeline1.getId());
    ratisPipeline2 = pipelineManager.getPipeline(
        containerManager.allocateContainer(
            RatisReplicationConfig.getInstance(
                ReplicationFactor.ONE), "Owner2").getPipelineID());
    pipelineManager.openPipeline(ratisPipeline2.getId());
    // At this stage, there should be 2 pipeline one with 1 open container
    // each. Try restarting the SCM and then discover that pipeline are in
    // correct state.
    cluster.restartStorageContainerManager(true);
    newContainerManager = cluster.getStorageContainerManager()
        .getContainerManager();
    pipelineManager = cluster.getStorageContainerManager().getPipelineManager();
  }

  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testPipelineWithScmRestart()
      throws IOException, TimeoutException {
    // After restart make sure that the pipeline are still present
    Pipeline ratisPipeline1AfterRestart =
        pipelineManager.getPipeline(ratisPipeline1.getId());
    Pipeline ratisPipeline2AfterRestart =
        pipelineManager.getPipeline(ratisPipeline2.getId());
    assertNotSame(ratisPipeline1AfterRestart, ratisPipeline1);
    assertNotSame(ratisPipeline2AfterRestart, ratisPipeline2);
    assertEquals(ratisPipeline1AfterRestart, ratisPipeline1);
    assertEquals(ratisPipeline2AfterRestart, ratisPipeline2);

    // Try creating a new container, it should be from the same pipeline
    // as was before restart
    ContainerInfo containerInfo = newContainerManager
        .allocateContainer(RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), "Owner1");
    assertEquals(ratisPipeline1.getId(), containerInfo.getPipelineID());
  }
}
