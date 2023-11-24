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

import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.net.NodeImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_LEVEL;

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
    config.set(StaticMapping.KEY_HADOOP_CONFIGURED_NODE_MAPPING,
        String.join(",", nodeMapping));

    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true));
    configurator.setScmContext(SCMContext.emptyContext());

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

      for (int i = 1; i < NODE_COUNT / 2; i++) {
        DatanodeDetails item = sorted.get(i);
        Assertions.assertEquals(dn.getNetworkLocation(),
            item.getNetworkLocation(),
            "Nodes in the same rack should be sorted first");
      }
      for (int i = NODE_COUNT / 2; i < NODE_COUNT; i++) {
        DatanodeDetails item = sorted.get(i);
        Assertions.assertNotEquals(dn.getNetworkLocation(),
            item.getNetworkLocation(),
            "Nodes in the other rack should be sorted last");
      }
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
