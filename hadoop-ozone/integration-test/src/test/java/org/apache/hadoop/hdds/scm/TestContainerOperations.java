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

package org.apache.hadoop.hdds.scm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.REPLICATION;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * This class tests container operations (TODO currently only supports create)
 * from cblock clients.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestContainerOperations implements NonHATests.TestCase {

  private static final int CONTAINER_LIST_LIMIT = 1;

  private ScmClient storageClient;
  private XceiverClientManager xceiverClientManager;

  @BeforeAll
  void setup() throws Exception {
    OzoneConfiguration clientConf = new OzoneConfiguration(cluster().getConf());
    clientConf.setInt(ScmConfigKeys.OZONE_SCM_CONTAINER_LIST_MAX_COUNT, CONTAINER_LIST_LIMIT);
    storageClient = new ContainerOperationClient(clientConf);
    xceiverClientManager = new XceiverClientManager(cluster().getConf());
  }

  @AfterAll
  void cleanup() {
    IOUtils.closeQuietly(xceiverClientManager);
  }

  @Test
  void testContainerStateMachineIdempotency() throws Exception {
    ContainerWithPipeline container = storageClient.createContainer(HddsProtos
        .ReplicationType.RATIS, HddsProtos.ReplicationFactor
        .ONE, OzoneConsts.OZONE);
    long containerID = container.getContainerInfo().getContainerID();
    Pipeline pipeline = container.getPipeline();
    XceiverClientSpi client = xceiverClientManager.acquireClient(pipeline);

    // call create Container again
    BlockID blockID = ContainerTestHelper.getTestBlockID(containerID);
    byte[] data =
        RandomStringUtils.secure().next(RandomUtils.secure().randomInt(0, 1024)).getBytes(UTF_8);
    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        ContainerTestHelper
            .getWriteChunkRequest(container.getPipeline(), blockID,
                data.length);
    client.sendCommand(writeChunkRequest);

    //Make the write chunk request again without requesting for overWrite
    client.sendCommand(writeChunkRequest);
    // Now, explicitly make a putKey request for the block.
    ContainerProtos.ContainerCommandRequestProto putKeyRequest =
        ContainerTestHelper
            .getPutBlockRequest(pipeline, writeChunkRequest.getWriteChunk());
    client.sendCommand(putKeyRequest);
    // send the putBlock again
    client.sendCommand(putKeyRequest);

    // close container call
    ContainerProtocolCalls.closeContainer(client, containerID, null);
    ContainerProtocolCalls.closeContainer(client, containerID, null);

    xceiverClientManager.releaseClient(client, false);
  }

  /**
   * A simple test to create a container with {@link ContainerOperationClient}.
   */
  @Test
  public void testCreate() throws Exception {
    ContainerWithPipeline container = storageClient.createContainer(HddsProtos
        .ReplicationType.STAND_ALONE, HddsProtos.ReplicationFactor
        .ONE, OzoneConsts.OZONE);
    assertEquals(container.getContainerInfo().getContainerID(), storageClient
        .getContainer(container.getContainerInfo().getContainerID())
        .getContainerID());
  }

  /**
   * Test to try to list number of containers over the max number Ozone allows.
   */
  @Test
  public void testListContainerExceedMaxAllowedCountOperations() throws Exception {
    // create n+1 containers
    for (int i = 0; i < CONTAINER_LIST_LIMIT + 1; i++) {
      storageClient.createContainer(HddsProtos
          .ReplicationType.STAND_ALONE, HddsProtos.ReplicationFactor
          .ONE, OzoneConsts.OZONE);
    }

    int count = storageClient.listContainer(0, CONTAINER_LIST_LIMIT + 1)
        .getContainerInfoList()
        .size();
    assertEquals(CONTAINER_LIST_LIMIT, count);
  }

  /**
   * A simple test to get Pipeline with {@link ContainerOperationClient}.
   */
  @Test
  public void testGetPipeline() throws Exception {
    Exception e =
        assertThrows(Exception.class, () -> storageClient.getPipeline(PipelineID.randomId().getProtobuf()));
    assertInstanceOf(PipelineNotFoundException.class, SCMHAUtils.unwrapException(e));
    assertThat(storageClient.listPipelines()).isNotEmpty();
  }

  @Test
  public void testDatanodeUsageInfoCompatibility() throws IOException {
    DatanodeDetails dn = cluster().getStorageContainerManager()
        .getScmNodeManager()
        .getAllNodes()
        .get(0);
    dn.setCurrentVersion(0);

    List<HddsProtos.DatanodeUsageInfoProto> usageInfoList =
        storageClient.getDatanodeUsageInfo(
            dn.getIpAddress(), dn.getUuidString());

    for (HddsProtos.DatanodeUsageInfoProto info : usageInfoList) {
      assertTrue(info.getNode().getPortsList().stream()
          .anyMatch(port -> REPLICATION.name().equals(port.getName())));
    }

    usageInfoList =
        storageClient.getDatanodeUsageInfo(true, 3);

    for (HddsProtos.DatanodeUsageInfoProto info : usageInfoList) {
      assertTrue(info.getNode().getPortsList().stream()
          .anyMatch(port -> REPLICATION.name().equals(port.getName())));
    }
  }

  @Test
  public void testDatanodeUsageInfoContainerCount() throws Exception {
    NodeManager nodeManager = cluster().getStorageContainerManager().getScmNodeManager();
    List<? extends DatanodeDetails> dnList = nodeManager.getAllNodes();

    for (DatanodeDetails dn : dnList) {
      List<HddsProtos.DatanodeUsageInfoProto> usageInfoList =
              storageClient.getDatanodeUsageInfo(
                      dn.getIpAddress(), dn.getUuidString());

      assertEquals(1, usageInfoList.size());
      assertEquals(nodeManager.getContainers(dn).size(), usageInfoList.get(0).getContainerCount());
    }
  }

  @Test
  public void testHealthyNodesCount() throws Exception {
    List<HddsProtos.Node> nodes = storageClient.queryNode(null, HEALTHY,
        HddsProtos.QueryScope.CLUSTER, "");
    assertEquals(cluster().getHddsDatanodes().size(), nodes.size(), "Expected live nodes");
  }

  @Test
  public void testNodeOperationalStates() throws Exception {
    StorageContainerManager scm = cluster().getStorageContainerManager();
    NodeManager nm = scm.getScmNodeManager();
    final int numOfDatanodes = nm.getAllNodes().size();

    // Set one node to be something other than IN_SERVICE
    final DatanodeDetails node = nm.getAllNodes().get(0);
    HddsProtos.NodeOperationalState originalState = nm.getNodeStatus(node).getOperationalState();
    nm.setNodeOperationalState(node, DECOMMISSIONING);

    // Nodes not in DECOMMISSIONING state should be returned as they are in service
    int nodeCount = storageClient.queryNode(IN_SERVICE, HEALTHY,
        HddsProtos.QueryScope.CLUSTER, "").size();
    assertEquals(numOfDatanodes - 1, nodeCount);

    // null acts as wildcard for opState
    nodeCount = storageClient.queryNode(null, HEALTHY,
        HddsProtos.QueryScope.CLUSTER, "").size();
    assertEquals(numOfDatanodes, nodeCount);

    // null acts as wildcard for nodeState
    nodeCount = storageClient.queryNode(IN_SERVICE, null,
        HddsProtos.QueryScope.CLUSTER, "").size();
    assertEquals(numOfDatanodes - 1, nodeCount);

    // Both null - should return all nodes
    nodeCount = storageClient.queryNode(null, null,
        HddsProtos.QueryScope.CLUSTER, "").size();
    assertEquals(numOfDatanodes, nodeCount);

    // No node should be returned
    nodeCount = storageClient.queryNode(IN_MAINTENANCE, HEALTHY,
        HddsProtos.QueryScope.CLUSTER, "").size();
    assertEquals(0, nodeCount);

    // Test all operational states by looping over them all and setting the
    // state manually.
    try {
      for (HddsProtos.NodeOperationalState s :
          HddsProtos.NodeOperationalState.values()) {
        nm.setNodeOperationalState(node, s);
        nodeCount = storageClient.queryNode(s, HEALTHY,
            HddsProtos.QueryScope.CLUSTER, "").size();
        if (s == IN_SERVICE) {
          assertEquals(cluster().getHddsDatanodes().size(), nodeCount);
        } else {
          assertEquals(1, nodeCount);
        }
      }
    } finally {
      nm.setNodeOperationalState(node, originalState);
    }
  }

  @Test
  public void testCreateRatis() throws Exception {
    testCreateContainer(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
  }

  @Test
  public void testCreateEC() throws Exception {
    ECReplicationConfig ecConfig = new ECReplicationConfig(3, 2);
    testCreateContainer(ecConfig);
  }

  private void testCreateContainer(ReplicationConfig replicationConfig) throws Exception {
    ContainerWithPipeline container = storageClient.createContainer(replicationConfig, OzoneConsts.OZONE);
    assertEquals(container.getContainerInfo().getContainerID(),
        storageClient.getContainer(container.getContainerInfo().getContainerID()).getContainerID());
  }
}
