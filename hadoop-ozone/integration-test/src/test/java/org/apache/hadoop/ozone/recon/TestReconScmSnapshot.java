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
package org.apache.hadoop.ozone.recon;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Recon SCM Snapshot Download implementation.
 */
@Timeout(100)
public class TestReconScmSnapshot {
  private OzoneConfiguration conf;
  private MiniOzoneCluster ozoneCluster = null;

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(
        ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_ENABLED, true);
    conf.setInt(ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_THRESHOLD, 0);
    ozoneCluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(4)
        .includeRecon(true)
        .build();
    ozoneCluster.waitForClusterToBeReady();
  }

  @Test
  public void testScmSnapshot() throws Exception {
    testSnapshot(ozoneCluster);
  }

  public static void testSnapshot(MiniOzoneCluster cluster) throws Exception {
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(LoggerFactory.getLogger(
        ReconStorageContainerManagerFacade.class));

    List<ContainerInfo> reconContainers = cluster.getReconServer()
        .getReconStorageContainerManager().getContainerManager()
        .getContainers();
    assertEquals(0, reconContainers.size());

    ReconNodeManager nodeManager;
    nodeManager = (ReconNodeManager) cluster.getReconServer()
        .getReconStorageContainerManager().getScmNodeManager();
    long keyCountBefore = nodeManager.getNodeDBKeyCount();

    //Stopping Recon to add Containers in SCM
    cluster.stopRecon();

    ContainerManager containerManager;
    containerManager = cluster.getStorageContainerManager()
        .getContainerManager();

    for (int i = 0; i < 10; i++) {
      containerManager.allocateContainer(RatisReplicationConfig.getInstance(
          HddsProtos.ReplicationFactor.ONE), "testOwner");
    }

    cluster.startRecon();

    //ContainerCount after Recon DB is updated with SCM DB
    containerManager = cluster.getStorageContainerManager()
        .getContainerManager();
    ContainerManager reconContainerManager = cluster.getReconServer()
        .getReconStorageContainerManager().getContainerManager();
    assertTrue(logCapturer.getOutput()
        .contains("Recon Container Count: " + reconContainers.size() +
        ", SCM Container Count: " + containerManager.getContainers().size()));
    assertEquals(containerManager.getContainers().size(),
        reconContainerManager.getContainers().size());

    //PipelineCount after Recon DB is updated with SCM DB
    PipelineManager scmPipelineManager = cluster.getStorageContainerManager()
        .getPipelineManager();
    PipelineManager reconPipelineManager = cluster.getReconServer()
        .getReconStorageContainerManager().getPipelineManager();
    assertEquals(scmPipelineManager.getPipelines().size(),
        reconPipelineManager.getPipelines().size());

    //NodeCount after Recon DB updated with SCM DB
    nodeManager = (ReconNodeManager) cluster.getReconServer()
        .getReconStorageContainerManager().getScmNodeManager();
    long keyCountAfter = nodeManager.getNodeDBKeyCount();
    assertEquals(keyCountAfter, keyCountBefore);
  }

  @AfterEach
  public void shutdown() throws Exception {
    if (ozoneCluster != null) {
      ozoneCluster.shutdown();
    }
  }
}
