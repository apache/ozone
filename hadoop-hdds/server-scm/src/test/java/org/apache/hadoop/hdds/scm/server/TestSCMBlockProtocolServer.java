/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.server;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLog;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.block.SCMBlockDeletingService;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.net.NodeImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_LEVEL;
import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test class for @{@link SCMBlockProtocolServer}.
 */
public class TestSCMBlockProtocolServer {
  private OzoneConfiguration config;
  private SCMBlockProtocolServer server;
  private StorageContainerManager scm;
  private NodeManager nodeManager;
  private ScmBlockLocationProtocolServerSideTranslatorPB service;
  private static final int NODE_COUNT = 10;

  private static final Map<String, String> EDGE_NODES = ImmutableMap.of(
      "edge0", "/rack0",
      "edge1", "/rack1"
  );

  private static class BlockManagerStub implements BlockManager {

    private final List<DatanodeDetails> datanodes;

    BlockManagerStub(List<DatanodeDetails> datanodes) {
      assertNotNull(datanodes, "Datanodes cannot be null");
      this.datanodes = datanodes;
    }

    @Override
    public AllocatedBlock allocateBlock(long size,
        ReplicationConfig replicationConfig, String owner,
        ExcludeList excludeList) throws IOException, TimeoutException {
      List<DatanodeDetails> nodes = new ArrayList<>(datanodes);
      Collections.shuffle(nodes);
      Pipeline pipeline;

      if (replicationConfig !=
          RatisReplicationConfig.getInstance(ReplicationFactor.THREE)) {
        // Other replication config can be supported in the future
        return null;
      }

      pipeline = Pipeline.newBuilder()
          .setId(PipelineID.randomId())
          .setState(PipelineState.OPEN)
          .setReplicationConfig(replicationConfig)
          .setNodes(nodes.subList(0, 3))
          .build();

      long localID = ThreadLocalRandom.current().nextLong();
      long containerID = ThreadLocalRandom.current().nextLong();
      AllocatedBlock.Builder abb = new AllocatedBlock.Builder()
          .setContainerBlockID(new ContainerBlockID(containerID, localID))
          .setPipeline(pipeline);
      return abb.build();
    }

    @Override
    public void deleteBlocks(List<BlockGroup> blockIDs) throws IOException {

    }

    @Override
    public DeletedBlockLog getDeletedBlockLog() {
      return Mockito.mock(DeletedBlockLogImpl.class);
    }

    @Override
    public void start() throws IOException {

    }

    @Override
    public void stop() throws IOException {

    }

    @Override
    public SCMBlockDeletingService getSCMBlockDeletingService() {
      return null;
    }

    @Override
    public void close() throws IOException {

    }
  }

  @BeforeEach
  void setUp(@TempDir File dir) throws Exception {
    config = SCMTestUtils.getConf(dir);
    config.set(NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        StaticMapping.class.getName());
    List<DatanodeDetails> datanodes = new ArrayList<>(NODE_COUNT);
    List<String> nodeMapping = new ArrayList<>(NODE_COUNT);
    for (int i = 0; i < NODE_COUNT; i++) {
      DatanodeDetails dn = randomDatanodeDetails();
      final String rack = "/rack" + (i % 2);
      nodeMapping.add(dn.getHostName() + "=" + rack);
      nodeMapping.add(dn.getIpAddress() + "=" + rack);
      datanodes.add(dn);
    }
    EDGE_NODES.forEach((n, r) -> nodeMapping.add(n + "=" + r));
    config.set(StaticMapping.KEY_HADOOP_CONFIGURED_NODE_MAPPING,
        String.join(",", nodeMapping));

    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true));
    configurator.setScmContext(SCMContext.emptyContext());
    configurator.setScmBlockManager(new BlockManagerStub(datanodes));
    scm = HddsTestUtils.getScm(config, configurator);
    scm.start();
    scm.exitSafeMode();
    // add nodes to scm node manager
    nodeManager = scm.getScmNodeManager();
    datanodes.forEach(dn -> nodeManager.register(dn, null, null));
    server = scm.getBlockProtocolServer();
    service = new ScmBlockLocationProtocolServerSideTranslatorPB(server, scm,
        Mockito.mock(ProtocolMessageMetrics.class));
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (scm != null) {
      scm.stop();
      scm.join();
    }
  }

  @Test
  void sortDatanodesRelativeToDatanode() {
    List<String> nodes = getNetworkNames();
    for (DatanodeDetails dn : nodeManager.getAllNodes()) {
      Assertions.assertEquals(ROOT_LEVEL + 2, dn.getLevel());

      List<DatanodeDetails> sorted =
          server.sortDatanodes(nodes, nodeAddress(dn));

      Assertions.assertEquals(dn, sorted.get(0),
          "Source node should be sorted very first");

      assertRackOrder(dn.getNetworkLocation(), sorted);
    }
  }

  @Test
  void sortDatanodesRelativeToNonDatanode() {
    List<String> datanodes = getNetworkNames();

    for (Map.Entry<String, String> entry : EDGE_NODES.entrySet()) {
      assertRackOrder(entry.getValue(),
          server.sortDatanodes(datanodes, entry.getKey()));
    }
  }

  private static void assertRackOrder(String rack, List<DatanodeDetails> list) {
    int size = list.size();

    for (int i = 0; i < size / 2; i++) {
      Assertions.assertEquals(rack, list.get(i).getNetworkLocation(),
          "Nodes in the same rack should be sorted first");
    }

    for (int i = size / 2; i < size; i++) {
      Assertions.assertNotEquals(rack, list.get(i).getNetworkLocation(),
          "Nodes in the other rack should be sorted last");
    }
  }

  @Test
  public void testSortDatanodes() throws Exception {
    List<String> nodes = getNetworkNames();

    // sort normal datanodes
    String client;
    client = nodeManager.getAllNodes().get(0).getIpAddress();
    List<DatanodeDetails> datanodeDetails =
        server.sortDatanodes(nodes, client);
    System.out.println("client = " + client);
    datanodeDetails.stream().forEach(
        node -> System.out.println(node.toString()));
    Assertions.assertTrue(datanodeDetails.size() == NODE_COUNT);

    // illegal client 1
    client += "X";
    datanodeDetails = server.sortDatanodes(nodes, client);
    System.out.println("client = " + client);
    datanodeDetails.stream().forEach(
        node -> System.out.println(node.toString()));
    Assertions.assertTrue(datanodeDetails.size() == NODE_COUNT);
    // illegal client 2
    client = "/default-rack";
    datanodeDetails = server.sortDatanodes(nodes, client);
    System.out.println("client = " + client);
    datanodeDetails.stream().forEach(
        node -> System.out.println(node.toString()));
    Assertions.assertTrue(datanodeDetails.size() == NODE_COUNT);

    // unknown node to sort
    nodes.add(UUID.randomUUID().toString());
    client = nodeManager.getAllNodes().get(0).getIpAddress();
    ScmBlockLocationProtocolProtos.SortDatanodesRequestProto request =
        ScmBlockLocationProtocolProtos.SortDatanodesRequestProto
            .newBuilder()
            .addAllNodeNetworkName(nodes)
            .setClient(client)
            .build();
    ScmBlockLocationProtocolProtos.SortDatanodesResponseProto resp =
        service.sortDatanodes(request, ClientVersion.CURRENT_VERSION);
    Assertions.assertTrue(resp.getNodeList().size() == NODE_COUNT);
    System.out.println("client = " + client);
    resp.getNodeList().stream().forEach(
        node -> System.out.println(node.getNetworkName()));

    // all unknown nodes
    nodes.clear();
    nodes.add(UUID.randomUUID().toString());
    nodes.add(UUID.randomUUID().toString());
    nodes.add(UUID.randomUUID().toString());
    request = ScmBlockLocationProtocolProtos.SortDatanodesRequestProto
        .newBuilder()
        .addAllNodeNetworkName(nodes)
        .setClient(client)
        .build();
    resp = service.sortDatanodes(request, ClientVersion.CURRENT_VERSION);
    System.out.println("client = " + client);
    Assertions.assertTrue(resp.getNodeList().size() == 0);
    resp.getNodeList().stream().forEach(
        node -> System.out.println(node.getNetworkName()));
  }

  @Test
  void testAllocateBlockWithClientMachine() throws IOException {
    final DatanodeDetails clientDatanode = nodeManager.getAllNodes().get(0);
    final String clientAddress = clientDatanode.getIpAddress();
    final ReplicationConfig replicationConfig = RatisReplicationConfig
        .getInstance(ReplicationFactor.THREE);
    final long blockSize = 128 * MB;
    final int numOfBlocks = 5;

    List<AllocatedBlock> allocatedBlocks = server.allocateBlock(
        blockSize, numOfBlocks, replicationConfig, "o",
        new ExcludeList(), clientAddress);
    Assertions.assertEquals(numOfBlocks, allocatedBlocks.size());
    for (AllocatedBlock allocatedBlock: allocatedBlocks) {
      List<DatanodeDetails> nodesInOrder =
          allocatedBlock.getPipeline().getNodesInOrder();
      if (nodesInOrder.contains(clientDatanode)) {
        Assertions.assertEquals(clientDatanode, nodesInOrder.get(0),
            "Source node should be sorted very first");
      }
      String clientLocation = clientDatanode.getNetworkLocation();

      boolean stillSameRackAsClient = nodesInOrder.get(0).getNetworkLocation()
          .equals(clientLocation);
      for (int i = 1; i < nodesInOrder.size(); i++) {
        String nodeLocation = nodesInOrder.get(i).getNetworkLocation();
        if (stillSameRackAsClient) {
          if (!nodeLocation.equals(clientLocation)) {
            // First encounter of datanode under different rack
            stillSameRackAsClient = false;
          }
        } else {
          if (nodeLocation.equals(clientLocation)) {
            Assertions.fail("Node in the same rack as client " +
                "should not be sorted after nodes under different rack");
          }
        }
      }
    }
  }

  private List<String> getNetworkNames() {
    return nodeManager.getAllNodes().stream()
        .map(NodeImpl::getNetworkName)
        .collect(Collectors.toList());
  }

  private String nodeAddress(DatanodeDetails dn) {
    boolean useHostname = config.getBoolean(
        DFSConfigKeysLegacy.DFS_DATANODE_USE_DN_HOSTNAME,
        DFSConfigKeysLegacy.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
    return useHostname ? dn.getHostName() : dn.getIpAddress();
  }
}
