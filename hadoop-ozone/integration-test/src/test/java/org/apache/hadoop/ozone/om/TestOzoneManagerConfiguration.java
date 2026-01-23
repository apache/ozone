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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneClusterImpl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneIllegalArgumentException;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests OM related configurations.
 */
public class TestOzoneManagerConfiguration {

  private OzoneConfiguration conf;
  private MiniOzoneCluster cluster;

  private static final long RATIS_RPC_TIMEOUT = 500L;

  @BeforeEach
  void init(@TempDir Path metaDirPath) throws IOException {
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDirPath.toString());
    conf.setTimeDuration(OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
        RATIS_RPC_TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void startCluster() throws Exception {
    cluster =  MiniOzoneCluster.newBuilder(conf)
      .withoutDatanodes()
      .build();
    cluster.waitForClusterToBeReady();
  }

  /**
   * Test that if no OM address is specified, then the OM rpc server
   * is started on localhost.
   */
  @Test
  public void testNoConfiguredOMAddress() throws Exception {
    startCluster();
    OzoneManager om = cluster.getOzoneManager();

    assertTrue(NetUtils.isLocalAddress(
        om.getOmRpcServerAddr().getAddress()));
  }

  /**
   * Test that if only the hostname is specified for om address, then the
   * default port is used.
   */
  @Test
  public void testDefaultPortIfNotSpecified() throws Exception {

    String omNode1Id = "omNode1";
    String omNode2Id = "omNode2";
    String omNodesKeyValue = omNode1Id + "," + omNode2Id;
    String serviceID = "service1";
    conf.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, serviceID);
    conf.set(OMConfigKeys.OZONE_OM_NODES_KEY + "." + serviceID,
        omNodesKeyValue);

    String omNode1RpcAddrKey = getOMAddrKeyWithSuffix(serviceID, omNode1Id);
    String omNode2RpcAddrKey = getOMAddrKeyWithSuffix(serviceID, omNode2Id);

    conf.set(omNode1RpcAddrKey, "0.0.0.0");
    conf.set(omNode2RpcAddrKey, "122.0.0.122");

    // Set omNode1 as the current node. omNode1 address does not have a port
    // number specified. So the default port should be taken.
    conf.set(OMConfigKeys.OZONE_OM_NODE_ID_KEY, omNode1Id);

    startCluster();
    OzoneManager om = cluster.getOzoneManager();
    assertEquals("0.0.0.0",
        om.getOmRpcServerAddr().getHostName());
    assertEquals(OMConfigKeys.OZONE_OM_PORT_DEFAULT,
        om.getOmRpcServerAddr().getPort());

    // Verify that the 2nd OMs address stored in the current OM also has the
    // default port as the port is not specified
    InetSocketAddress omNode2Addr = om.getPeerNodes().get(0).getRpcAddress();
    assertEquals("122.0.0.122", omNode2Addr.getHostString());
    assertEquals(OMConfigKeys.OZONE_OM_PORT_DEFAULT,
        omNode2Addr.getPort());

  }

  /**
   * Test a single node OM service (default setting for MiniOzoneCluster).
   * @throws Exception
   */
  @Test
  public void testSingleNodeOMservice() throws Exception {
    // Default settings of MiniOzoneCluster start a sinle node OM service.
    startCluster();
    OzoneManager om = cluster.getOzoneManager();
    OzoneManagerRatisServer omRatisServer = om.getOmRatisServer();

    assertEquals(LifeCycle.State.RUNNING, om.getOmRatisServerState());
    // OM's Ratis server should have only 1 peer (itself) in its RaftGroup
    Collection<RaftPeer> peers = omRatisServer.getRaftGroup().getPeers();
    assertEquals(1, peers.size());

    // The RaftPeer id should match OM_DEFAULT_NODE_ID
    RaftPeer raftPeer = peers.toArray(new RaftPeer[1])[0];
    assertEquals(OzoneConsts.OM_DEFAULT_NODE_ID,
        raftPeer.getId().toString());
  }

  /**
   * Test configurating an OM service with three OM nodes.
   * @throws Exception
   */
  @Test
  public void testThreeNodeOMservice() throws Exception {
    // Set the configuration for 3 node OM service. Set one node's rpc
    // address to localhost. OM will parse all configurations and find the
    // nodeId representing the localhost

    final String omServiceId = "om-service-test1";
    final String omNode1Id = "omNode1";
    final String omNode2Id = "omNode2";
    final String omNode3Id = "omNode3";

    String omNodesKeyValue = omNode1Id + "," + omNode2Id + "," + omNode3Id;
    String omNodesKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);

    String omNode1RpcAddrKey = getOMAddrKeyWithSuffix(omServiceId, omNode1Id);
    String omNode2RpcAddrKey = getOMAddrKeyWithSuffix(omServiceId, omNode2Id);
    String omNode3RpcAddrKey = getOMAddrKeyWithSuffix(omServiceId, omNode3Id);

    String omNode3RatisPortKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_RATIS_PORT_KEY, omServiceId, omNode3Id);

    conf.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, omServiceId);
    conf.set(omNodesKey, omNodesKeyValue);

    // Set node2 to localhost and the other two nodes to dummy addresses
    conf.set(omNode1RpcAddrKey, "123.0.0.123:9862");
    conf.set(omNode2RpcAddrKey, "0.0.0.0:9862");
    conf.set(omNode3RpcAddrKey, "124.0.0.124:9862");

    conf.setInt(omNode3RatisPortKey, 9898);

    startCluster();
    OzoneManager om = cluster.getOzoneManager();
    OzoneManagerRatisServer omRatisServer = om.getOmRatisServer();

    assertEquals(LifeCycle.State.RUNNING, om.getOmRatisServerState());

    // OM's Ratis server should have 3 peers in its RaftGroup
    Collection<RaftPeer> peers = omRatisServer.getRaftGroup().getPeers();
    assertEquals(3, peers.size());

    // Ratis server RaftPeerId should match with omNode2 ID as node2 is the
    // localhost
    assertEquals(omNode2Id, omRatisServer.getRaftPeerId().toString());

    // Verify peer details
    for (RaftPeer peer : peers) {
      String expectedPeerAddress = null;
      switch (peer.getId().toString()) {
      case omNode1Id :
        // Ratis port is not set for node1. So it should take the default port
        expectedPeerAddress = "123.0.0.123:" +
            OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT;
        break;
      case omNode2Id :
        expectedPeerAddress = "0.0.0.0:" +
            OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT;
        break;
      case omNode3Id :
        // Ratis port is not set for node3. So it should take the default port
        expectedPeerAddress = "124.0.0.124:9898";
        break;
      default : fail("Unrecognized RaftPeerId");
      }
      assertEquals(expectedPeerAddress, peer.getAddress());
    }
  }

  /**
   * Test configurating an OM service with three OM nodes.
   * @throws Exception
   */
  @Test
  public void testOMHAWithUnresolvedAddresses() throws Exception {
    // Set the configuration for 3 node OM service. Set one node's rpc
    // address to localhost. OM will parse all configurations and find the
    // nodeId representing the localhost

    final String omServiceId = "om-test-unresolved-addresses";
    final String omNode1Id = "omNode1";
    final String omNode2Id = "omNode2";
    final String omNode3Id = "omNode3";
    final String node1Hostname = "node1.example.com";
    final String node3Hostname = "node3.example.com";

    String omNodesKeyValue = omNode1Id + "," + omNode2Id + "," + omNode3Id;
    String omNodesKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);

    String omNode1RpcAddrKey = getOMAddrKeyWithSuffix(omServiceId, omNode1Id);
    String omNode2RpcAddrKey = getOMAddrKeyWithSuffix(omServiceId, omNode2Id);
    String omNode3RpcAddrKey = getOMAddrKeyWithSuffix(omServiceId, omNode3Id);

    String omNode3RatisPortKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_RATIS_PORT_KEY, omServiceId, omNode3Id);

    conf.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, omServiceId);
    conf.set(omNodesKey, omNodesKeyValue);

    // Set node2 to localhost and the other two nodes to dummy addresses
    conf.set(omNode1RpcAddrKey, node1Hostname + ":9862");
    conf.set(omNode2RpcAddrKey, "0.0.0.0:9862");
    conf.set(omNode3RpcAddrKey, node3Hostname + ":9804");

    conf.setInt(omNode3RatisPortKey, 9898);

    startCluster();
    OzoneManager om = cluster.getOzoneManager();
    OzoneManagerRatisServer omRatisServer = om.getOmRatisServer();

    // Verify Peer details
    List<OMNodeDetails> peerNodes = om.getPeerNodes();
    for (OMNodeDetails peerNode : peerNodes) {
      assertTrue(peerNode.isHostUnresolved());
      assertNull(peerNode.getInetAddress());
    }

    assertEquals(LifeCycle.State.RUNNING, om.getOmRatisServerState());

    // OM's Ratis server should have 3 peers in its RaftGroup
    Collection<RaftPeer> peers = omRatisServer.getRaftGroup().getPeers();
    assertEquals(3, peers.size());

    // Ratis server RaftPeerId should match with omNode2 ID as node2 is the
    // localhost
    assertEquals(omNode2Id, omRatisServer.getRaftPeerId().toString());

    // Verify peer details
    for (RaftPeer peer : peers) {
      String expectedPeerAddress = null;

      switch (peer.getId().toString()) {
      case omNode1Id :
        // Ratis port is not set for node1. So it should take the default port
        expectedPeerAddress = node1Hostname + ":" +
            OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT;
        break;
      case omNode2Id :
        expectedPeerAddress = "0.0.0.0:" +
            OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT;
        break;
      case omNode3Id :
        // Ratis port is not set for node3. So it should take the default port
        expectedPeerAddress = node3Hostname + ":9898";
        break;
      default : fail("Unrecognized RaftPeerId");
      }
      assertEquals(expectedPeerAddress, peer.getAddress());
    }
  }

  /**
   * Test a wrong configuration for OM HA. A configuration with none of the
   * OM addresses matching the local address should throw an error.
   */
  @Test
  public void testWrongConfiguration() {
    String omServiceId = "om-service-test1";

    String omNode1Id = "omNode1";
    String omNode2Id = "omNode2";
    String omNode3Id = "omNode3";
    String omNodesKeyValue = omNode1Id + "," + omNode2Id + "," + omNode3Id;
    String omNodesKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);

    String omNode1RpcAddrKey = getOMAddrKeyWithSuffix(omServiceId, omNode1Id);
    String omNode2RpcAddrKey = getOMAddrKeyWithSuffix(omServiceId, omNode2Id);
    String omNode3RpcAddrKey = getOMAddrKeyWithSuffix(omServiceId, omNode3Id);

    conf.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, omServiceId);
    conf.set(omNodesKey, omNodesKeyValue);

    // Set node2 to localhost and the other two nodes to dummy addresses
    conf.set(omNode1RpcAddrKey, "123.0.0.123:9862");
    conf.set(omNode2RpcAddrKey, "125.0.0.2:9862");
    conf.set(omNode3RpcAddrKey, "124.0.0.124:9862");

    GenericTestUtils.withLogDisabled(MiniOzoneClusterImpl.class, () -> {
      Exception exception = assertThrows(OzoneIllegalArgumentException.class, this::startCluster);
      assertThat(exception).hasMessage(
          "Configuration has no " + OZONE_OM_ADDRESS_KEY + " address that matches local node's address.");
    });
  }

  /**
   * A configuration with an empty node list while service ID is configured.
   * Cluster should fail to start during config check.
   */
  @Test
  public void testNoOMNodes() {
    String omServiceId = "service1";
    conf.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, omServiceId);
    // Deliberately skip OZONE_OM_NODES_KEY and OZONE_OM_ADDRESS_KEY config
    GenericTestUtils.withLogDisabled(MiniOzoneClusterImpl.class, () -> {
      Exception e = assertThrows(OzoneIllegalArgumentException.class, this::startCluster);
      // Expect error message
      assertThat(e).hasMessageContaining("List of OM Node ID's should be specified");
    });
  }

  /**
   * A configuration with no OM addresses while service ID is configured.
   * Cluster should fail to start during config check.
   */
  @Test
  public void testNoOMAddrs() {
    String omServiceId = "service1";

    String omNode1Id = "omNode1";
    String omNode2Id = "omNode2";
    String omNode3Id = "omNode3";
    String omNodesKeyValue = omNode1Id + "," + omNode2Id + "," + omNode3Id;
    String omNodesKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);

    conf.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, omServiceId);
    conf.set(omNodesKey, omNodesKeyValue);
    // Deliberately skip OZONE_OM_ADDRESS_KEY config
    GenericTestUtils.withLogDisabled(MiniOzoneClusterImpl.class, () -> {
      Exception e = assertThrows(OzoneIllegalArgumentException.class, this::startCluster);
      // Expect error message
      assertThat(e).hasMessageContaining("OM RPC Address should be set for all node");
    });
  }

  /**
   * Test multiple OM service configuration.
   */
  @Test
  public void testMultipleOMServiceIds() throws Exception {
    // Set up OZONE_OM_SERVICES_KEY with 2 service Ids.
    String om1ServiceId = "om-service-test1";
    String om2ServiceId = "om-service-test2";
    String omServices = om1ServiceId + "," + om2ServiceId;
    conf.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, omServices);

    String omNode1Id = "omNode1";
    String omNode2Id = "omNode2";
    String omNode3Id = "omNode3";
    String omNodesKeyValue = omNode1Id + "," + omNode2Id + "," + omNode3Id;

    // Set the node Ids for the 2 services. The nodeIds need to be
    // distinch within one service. The ids can overlap between
    // different services.
    String om1NodesKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, om1ServiceId);
    String om2NodesKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, om2ServiceId);
    conf.set(om1NodesKey, omNodesKeyValue);
    conf.set(om2NodesKey, omNodesKeyValue);

    // Set the RPC addresses for all 6 OMs (3 for each service). Only one
    // node out of these must have the localhost address.
    conf.set(getOMAddrKeyWithSuffix(om1ServiceId, omNode1Id),
        "122.0.0.123:9862");
    conf.set(getOMAddrKeyWithSuffix(om1ServiceId, omNode2Id),
        "123.0.0.124:9862");
    conf.set(getOMAddrKeyWithSuffix(om1ServiceId, omNode3Id),
        "124.0.0.125:9862");
    conf.set(getOMAddrKeyWithSuffix(om2ServiceId, omNode1Id),
        "125.0.0.126:9862");
    conf.set(getOMAddrKeyWithSuffix(om2ServiceId, omNode2Id),
        "0.0.0.0:9862");
    conf.set(getOMAddrKeyWithSuffix(om2ServiceId, omNode3Id),
        "126.0.0.127:9862");

    startCluster();
    OzoneManager om = cluster.getOzoneManager();
    OzoneManagerRatisServer omRatisServer = om.getOmRatisServer();

    assertEquals(LifeCycle.State.RUNNING, om.getOmRatisServerState());

    // OM's Ratis server should have 3 peers in its RaftGroup
    Collection<RaftPeer> peers = omRatisServer.getRaftGroup().getPeers();
    assertEquals(3, peers.size());

    // Verify that the serviceId and nodeId match the node with the localhost
    // address - om-service-test2 and omNode2
    assertEquals(om2ServiceId, om.getOMServiceId());
    assertEquals(omNode2Id, omRatisServer.getRaftPeerId().toString());
  }

  private String getOMAddrKeyWithSuffix(String serviceId, String nodeId) {
    return ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
        serviceId, nodeId);
  }
}
