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
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
    conf.set("ozone.scm.stale.node.interval", "6s");
    conf.set("ozone.scm.dead.node.interval", "8s");
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

  @Test
  @Flaky("HDDS-11645")
  public void testExplicitRemovalOfNode() throws Exception {
    ReconNodeManager nodeManager = (ReconNodeManager) ozoneCluster.getReconServer()
        .getReconStorageContainerManager().getScmNodeManager();
    long nodeDBCountBefore = nodeManager.getNodeDBKeyCount();
    List<DatanodeDetails> allNodes = nodeManager.getAllNodes();
    assertEquals(nodeDBCountBefore, allNodes.size());

    DatanodeDetails datanodeDetails = allNodes.get(3);
    ozoneCluster.shutdownHddsDatanode(datanodeDetails);

    GenericTestUtils.waitFor(() -> {
      try {
        return nodeManager.getNodeStatus(datanodeDetails).isDead();
      } catch (NodeNotFoundException e) {
        fail("getNodeStatus() Failed for " + datanodeDetails.getUuid(), e);
        throw new RuntimeException(e);
      }
    }, 2000, 10000);

    // Even after one node is DEAD, node manager is still keep tracking the DEAD node.
    long nodeDBCountAfter = nodeManager.getNodeDBKeyCount();
    assertEquals(nodeDBCountAfter, 4);

    final NodeStatus nStatus = nodeManager.getNodeStatus(datanodeDetails);

    final HddsProtos.NodeOperationalState backupOpState =
        datanodeDetails.getPersistedOpState();
    final long backupOpStateExpiry =
        datanodeDetails.getPersistedOpStateExpiryEpochSec();
    assertEquals(backupOpState, nStatus.getOperationalState());
    assertEquals(backupOpStateExpiry, nStatus.getOpStateExpiryEpochSeconds());

    // Now removing the DEAD node from both node DB and node manager memory.
    nodeManager.removeNode(datanodeDetails);

    // Now check the count of datanodes node DB has and node manager is tracking in memory
    nodeDBCountAfter = nodeManager.getNodeDBKeyCount();
    assertEquals(nodeDBCountAfter, 3);

    allNodes = nodeManager.getAllNodes();
    assertEquals(nodeDBCountAfter, allNodes.size());
  }

  @AfterEach
  public void shutdown() throws Exception {
    if (ozoneCluster != null) {
      ozoneCluster.shutdown();
    }
  }
}
