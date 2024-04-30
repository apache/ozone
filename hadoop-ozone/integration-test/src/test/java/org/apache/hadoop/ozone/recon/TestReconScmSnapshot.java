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
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator.CONTAINER_ID;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CLIENT_FAILOVER_MAX_RETRY_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CLIENT_FAILOVER_MAX_RETRY_KEY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CLIENT_MAX_RETRY_TIMEOUT_KEY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CLIENT_RPC_TIME_OUT_KEY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_TASK_INITIAL_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_TASK_INTERVAL_DELAY;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test Recon SCM Snapshot Download implementation.
 */
@Timeout(300)
public class TestReconScmSnapshot {
  private OzoneConfiguration conf;
  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(
        ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_ENABLED, true);
    conf.setInt(ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_THRESHOLD, 0);
    conf.setTimeDuration(OZONE_RECON_SCM_SNAPSHOT_TASK_INITIAL_DELAY, 1, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_RECON_SCM_SNAPSHOT_TASK_INTERVAL_DELAY, 3, TimeUnit.SECONDS);

    conf.setTimeDuration(OZONE_RECON_SCM_CLIENT_RPC_TIME_OUT_KEY, 5, TimeUnit.MINUTES);
    conf.setTimeDuration(OZONE_RECON_SCM_CLIENT_MAX_RETRY_TIMEOUT_KEY, 6, TimeUnit.SECONDS);
    conf.setInt(OZONE_RECON_SCM_CLIENT_FAILOVER_MAX_RETRY_KEY, OZONE_RECON_SCM_CLIENT_FAILOVER_MAX_RETRY_DEFAULT);

    ozoneCluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(4)
        .includeRecon(true)
        .build();
    ozoneCluster.waitForClusterToBeReady();
  }

  private MiniOzoneCluster ozoneCluster = null;

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

    // wait for SCM DB sync scheduler thread to run
    ContainerManager finalContainerManager = containerManager;
    GenericTestUtils.waitFor(() -> logCapturer.getOutput()
        .contains("Recon Container Count: " + reconContainerManager.getContainers().size() +
            ", SCM Container Count: " + finalContainerManager.getContainers().size()), 1000, 60000);
    assertEquals(containerManager.getContainers().size(), reconContainerManager.getContainers().size());

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

  @Test
  public void testFullSCMDbSync() throws Exception {
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(LoggerFactory.getLogger(
            ReconStorageContainerManagerFacade.class));
    List<ContainerInfo> reconContainers = ozoneCluster.getReconServer()
        .getReconStorageContainerManager().getContainerManager()
        .getContainers();
    assertEquals(0, reconContainers.size());

    ReconNodeManager nodeManager;
    nodeManager = (ReconNodeManager) ozoneCluster.getReconServer()
        .getReconStorageContainerManager().getScmNodeManager();
    long keyCountBefore = nodeManager.getNodeDBKeyCount();

    //Stopping Recon to add Containers in SCM
    ozoneCluster.stopRecon();
    ContainerManager containerManager;
    containerManager = ozoneCluster.getStorageContainerManager()
        .getContainerManager();

    for (int i = 0; i < 10; i++) {
      containerManager.allocateContainer(RatisReplicationConfig.getInstance(
          HddsProtos.ReplicationFactor.ONE), "testOwner");
    }

    ozoneCluster.startRecon();
    //ContainerCount after Recon DB is updated with SCM DB
    containerManager = ozoneCluster.getStorageContainerManager()
        .getContainerManager();
    ReconContainerMetadataManager reconContainerMetadataManager =
        ozoneCluster.getReconServer().getReconContainerMetadataManager();

    ContainerManager reconContainerManager = ozoneCluster.getReconServer()
        .getReconStorageContainerManager().getContainerManager();

    // wait for SCM DB sync scheduler thread to run
    GenericTestUtils.waitFor(() -> logCapturer.getOutput()
        .contains("Obtaining full snapshot from SCM"), 1000, 60000);
    assertEquals(containerManager.getContainers().size(), reconContainerManager.getContainers().size());
    assertEquals(containerManager.getContainers().size(),
        reconContainerMetadataManager.getCountForContainers(HddsProtos.LifeCycleState.OPEN));

    //PipelineCount after Recon DB is updated with SCM DB
    PipelineManager scmPipelineManager = ozoneCluster.getStorageContainerManager()
        .getPipelineManager();
    PipelineManager reconPipelineManager = ozoneCluster.getReconServer()
        .getReconStorageContainerManager().getPipelineManager();
    assertEquals(scmPipelineManager.getPipelines().size(),
        reconPipelineManager.getPipelines().size());

    //NodeCount after Recon DB updated with SCM DB
    nodeManager = (ReconNodeManager) ozoneCluster.getReconServer()
        .getReconStorageContainerManager().getScmNodeManager();
    long keyCountAfter = nodeManager.getNodeDBKeyCount();
    assertEquals(keyCountAfter, keyCountBefore);
  }

  @Test
  public void testIncrementalSCMDbSync() throws Exception {
    OzoneStorageContainerManager reconStorageContainerManager =
        ozoneCluster.getReconServer().getReconStorageContainerManager();
    ContainerManager reconContainerManager = reconStorageContainerManager.getContainerManager();
    List<ContainerInfo> reconContainers = reconContainerManager.getContainers();
    assertEquals(0, reconContainers.size());

    ContainerManager containerManager = ozoneCluster.getStorageContainerManager()
        .getContainerManager();

    // Allocating 2 containers in SCM.
    for (int i = 0; i < 2; i++) {
      containerManager.allocateContainer(RatisReplicationConfig.getInstance(
          HddsProtos.ReplicationFactor.ONE), "testOwner");
    }
    GenericTestUtils.waitFor(() -> {
      return containerManager.getContainers().size() == reconContainerManager.getContainers().size();
    }, 1000, 10000);

    PipelineManager pipelineManager = ozoneCluster.getStorageContainerManager().getPipelineManager();
    assertEquals(pipelineManager.getPipelines().size(),
        reconStorageContainerManager.getPipelineManager().getPipelines().size());

    assertEquals(pipelineManager.getPipelines().get(0).getNodes().size(),
        reconStorageContainerManager.getPipelineManager().getPipelines().get(0).getNodes().size());
    assertEquals(pipelineManager.getPipelines().get(0).getPipelineState().name(),
        reconStorageContainerManager.getPipelineManager().getPipelines().get(0).getPipelineState().name());

    NodeManager scmNodeManager = ozoneCluster.getStorageContainerManager().getScmNodeManager();
    assertEquals(scmNodeManager.getAllNodes().size(),
        reconStorageContainerManager.getScmNodeManager().getAllNodes().size());

    assertEquals(scmNodeManager.getNodes(NodeStatus.inServiceHealthy()).size(),
        reconStorageContainerManager.getScmNodeManager().getNodes(NodeStatus.inServiceHealthy()).size());

    DatanodeDetails datanodeDetails = ozoneCluster.getHddsDatanodes().get(0).getDatanodeDetails();
    HddsProtos.NodeOperationalState scmNodePersistedOpState =
        scmNodeManager.getNodeByUuid(datanodeDetails.getUuid()).getPersistedOpState();
    HddsProtos.NodeOperationalState reconNodePersistedOpState =
        reconStorageContainerManager.getScmNodeManager().getNodeByUuid(datanodeDetails.getUuid()).getPersistedOpState();
    assertEquals(scmNodePersistedOpState.name(), reconNodePersistedOpState.name());

    // Assertions related to sequence Id generator, Sequence Id generator is used for generating
    // container Id, so since we allocated 2 containers and scm and recon metadata is in sync, so
    // next sequence id should be total containers count at recon plus 1 for nextId.
    assertEquals(ozoneCluster.getStorageContainerManager().getSequenceIdGen().getNextId(CONTAINER_ID),
        reconStorageContainerManager.getContainerManager().getContainers().size() + 1);
  }

  @AfterEach
  public void shutdown() throws Exception {
    if (ozoneCluster != null) {
      ozoneCluster.shutdown();
    }
  }
}
