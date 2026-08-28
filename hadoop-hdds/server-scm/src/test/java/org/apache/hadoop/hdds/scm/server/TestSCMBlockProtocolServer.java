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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_LEVEL;
import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLog;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.block.SCMBlockDeletingService;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMHANodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.net.NodeImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager;
import org.apache.hadoop.hdds.scm.server.upgrade.ScmVersionManager;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.audit.SCMAction;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

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
      return mock(DeletedBlockLogImpl.class);
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
    service = new ScmBlockLocationProtocolServerSideTranslatorPB(server, scm, mock(ProtocolMessageMetrics.class));
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
      assertEquals(ROOT_LEVEL + 2, dn.getLevel());

      List<DatanodeDetails> sorted =
          server.sortDatanodes(nodes, nodeAddress(dn));

      assertEquals(dn, sorted.get(0), "Source node should be sorted very first");

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
      assertEquals(rack, list.get(i).getNetworkLocation(),
          "Nodes in the same rack should be sorted first");
    }

    for (int i = size / 2; i < size; i++) {
      assertNotEquals(rack, list.get(i).getNetworkLocation(),
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
    assertEquals(NODE_COUNT, datanodeDetails.size());

    // illegal client 1
    client += "X";
    datanodeDetails = server.sortDatanodes(nodes, client);
    System.out.println("client = " + client);
    datanodeDetails.stream().forEach(
        node -> System.out.println(node.toString()));
    assertEquals(NODE_COUNT, datanodeDetails.size());
    // illegal client 2
    client = "/default-rack";
    datanodeDetails = server.sortDatanodes(nodes, client);
    System.out.println("client = " + client);
    datanodeDetails.stream().forEach(
        node -> System.out.println(node.toString()));
    assertEquals(NODE_COUNT, datanodeDetails.size());

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
        service.sortDatanodes(request, ClientVersion.CURRENT);
    assertEquals(NODE_COUNT, resp.getNodeList().size());
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
    resp = service.sortDatanodes(request, ClientVersion.CURRENT);
    System.out.println("client = " + client);
    assertEquals(0, resp.getNodeList().size());
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
    assertEquals(numOfBlocks, allocatedBlocks.size());
    for (AllocatedBlock allocatedBlock: allocatedBlocks) {
      List<DatanodeDetails> nodesInOrder =
          allocatedBlock.getPipeline().getNodesInOrder();
      if (nodesInOrder.contains(clientDatanode)) {
        assertEquals(clientDatanode, nodesInOrder.get(0),
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
            fail("Node in the same rack as client " +
                "should not be sorted after nodes under different rack");
          }
        }
      }
    }
  }

  @Test
  public void testFinalizeProceedsWhenNoPeers() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    try (SCMBlockProtocolServer testServer =
        finalizeServer(finalizationManager, Collections.emptyList())) {
      testServer.finalizeUpgrade();
      verify(finalizationManager).finalizeUpgrade();
    }
  }

  @Test
  public void testFinalizeProceedsWhenAllPeersMatch() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    StorageContainerLocationProtocol matching = peerClient(HDDSVersion.SOFTWARE_VERSION);

    try (SCMBlockProtocolServer testServer =
             finalizeServer(finalizationManager, Arrays.asList(peerNode("scm2"), peerNode("scm3")));
         MockedStatic<HAUtils> haUtils = mockStatic(HAUtils.class)) {
      haUtils.when(() -> HAUtils.getScmContainerClientForNode(any(), any(), any())).thenReturn(matching);
      testServer.finalizeUpgrade();
    }
    verify(finalizationManager).finalizeUpgrade();
  }

  @Test
  public void testFinalizeRejectsOlderPeerUnlessForced() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    StorageContainerLocationProtocol matching = peerClient(HDDSVersion.SOFTWARE_VERSION);
    StorageContainerLocationProtocol older = peerClient(HDDSVersion.DEFAULT_VERSION);

    try (SCMBlockProtocolServer testServer =
             finalizeServer(finalizationManager, Arrays.asList(peerNode("scm2"), peerNode("scm3")));
         MockedStatic<HAUtils> haUtils = mockStatic(HAUtils.class)) {
      haUtils.when(() -> HAUtils.getScmContainerClientForNode(any(), any(), any())).thenReturn(matching, older);
      // A peer on an older version is rejected without force.
      assertThrows(SCMException.class, testServer::finalizeUpgrade);
      verify(finalizationManager, never()).finalizeUpgrade();
      // With force the peer version check is skipped and finalization proceeds.
      testServer.forceFinalizeUpgrade();
    }
    verify(finalizationManager).finalizeUpgrade();
  }

  @Test
  public void testFinalizeRejectsUnknownFuturePeerUnlessForced() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    StorageContainerLocationProtocol matching = peerClient(HDDSVersion.SOFTWARE_VERSION);
    // A version not recognized by this binary deserializes to UNKNOWN_VERSION in the client translator.
    StorageContainerLocationProtocol unknown = peerClient(HDDSVersion.UNKNOWN_VERSION);

    try (SCMBlockProtocolServer testServer =
             finalizeServer(finalizationManager, Arrays.asList(peerNode("scm2"), peerNode("scm3")));
         MockedStatic<HAUtils> haUtils = mockStatic(HAUtils.class)) {
      haUtils.when(() -> HAUtils.getScmContainerClientForNode(any(), any(), any()))
          .thenReturn(matching, unknown);
      // A peer on an unrecognized future version is rejected without force.
      assertThrows(SCMException.class, testServer::finalizeUpgrade);
      verify(finalizationManager, never()).finalizeUpgrade();
      // With force the peer version check is skipped and finalization proceeds.
      testServer.forceFinalizeUpgrade();
    }
    verify(finalizationManager).finalizeUpgrade();
  }

  @Test
  public void testFinalizeRejectsUnreachablePeerUnlessForced() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    StorageContainerLocationProtocol unreachable = mock(StorageContainerLocationProtocol.class);
    when(unreachable.getPeerUpgradeStatus()).thenThrow(new IOException("connection refused"));

    try (SCMBlockProtocolServer testServer =
             finalizeServer(finalizationManager, Collections.singletonList(peerNode("scm2")));
         MockedStatic<HAUtils> haUtils = mockStatic(HAUtils.class)) {
      haUtils.when(() -> HAUtils.getScmContainerClientForNode(any(), any(), any())).thenReturn(unreachable);
      // An unreachable peer is rejected without force.
      assertThrows(SCMException.class, testServer::finalizeUpgrade);
      verify(finalizationManager, never()).finalizeUpgrade();
      // With force the peer version check is skipped and finalization proceeds.
      testServer.forceFinalizeUpgrade();
    }
    verify(finalizationManager).finalizeUpgrade();
  }

  @Test
  public void testFinalizeProceedsWhenAllDatanodesMatchScmVersion() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    NodeManager.DatanodeFinalizationCounts datanodeCounts = NodeManager.DatanodeFinalizationCounts.newBuilder()
        .setNumFinalizedDatanodes(3)
        .setTotalHealthyDatanodes(3)
        .setMinApparentVersion(HDDSVersion.SOFTWARE_VERSION.serialize())
        .setMaxApparentVersion(HDDSVersion.SOFTWARE_VERSION.serialize())
        .setAllSoftwareVersionsMatchScm(true)
        .build();
    try (SCMBlockProtocolServer testServer = finalizeServer(finalizationManager,
        Collections.emptyList(), datanodeCounts)) {
      testServer.finalizeUpgrade();
      verify(finalizationManager).finalizeUpgrade();
    }
  }

  @Test
  public void testFinalizeRejectsDatanodeWithMismatchedVersionUnlessForced() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    NodeManager.DatanodeFinalizationCounts datanodeCounts = NodeManager.DatanodeFinalizationCounts.newBuilder()
        .setNumFinalizedDatanodes(3)
        .setTotalHealthyDatanodes(3)
        .setMinApparentVersion(HDDSVersion.DEFAULT_VERSION.serialize())
        .setMaxApparentVersion(HDDSVersion.SOFTWARE_VERSION.serialize())
        .setAllSoftwareVersionsMatchScm(false)
        .build();
    try (SCMBlockProtocolServer testServer = finalizeServer(finalizationManager,
        Collections.emptyList(), datanodeCounts)) {
      // A datanode on a mismatched version is rejected without force.
      assertThrows(SCMException.class, testServer::finalizeUpgrade);
      verify(finalizationManager, never()).finalizeUpgrade();
      // With force the datanode version check is skipped and finalization proceeds.
      testServer.forceFinalizeUpgrade();
      verify(finalizationManager).finalizeUpgrade();
    }
  }

  @Test
  public void testForceFinalizeAuditRecordsForceFlag() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    // Drive the failure audit path (logged at ERROR, captured by the default log4j2 config) so the
    // recorded audit map can be inspected.
    doThrow(new RuntimeException("test")).when(finalizationManager).finalizeUpgrade();

    LogCapturer auditLog = LogCapturer.log4j2("SCMAudit");
    try (SCMBlockProtocolServer testServer = finalizeServer(finalizationManager, Collections.emptyList())) {
      assertThrows(RuntimeException.class, testServer::forceFinalizeUpgrade);
    } finally {
      auditLog.stopCapturing();
    }

    String output = auditLog.getOutput();
    assertTrue(output.contains(SCMAction.FINALIZE_SCM_UPGRADE.getAction()),
        "audit log should record the finalize action: " + output);
    assertTrue(output.contains("\"force\":\"true\""),
        "audit log should record that force was passed: " + output);
  }

  @Test
  public void testQueryUpgradeStatus() throws Exception {
    // SCM starts already finalized in tests.
    HddsProtos.UpgradeStatus status = server.queryUpgradeStatus();
    assertEquals(HddsProtos.FinalizationStatus.FINALIZED, status.getScmFinalizationStatus());
  }

  @Test
  public void testQueryUpgradeStatusHddsInProgress() throws Exception {
    // SCM is finalized but not all datanodes are, so HDDS finalization is still in progress.
    ScmVersionManager mockVersionManager = mock(ScmVersionManager.class);
    when(mockVersionManager.needsFinalization()).thenReturn(false);
    ComponentVersion apparentVersion = mock(ComponentVersion.class);
    when(apparentVersion.serialize()).thenReturn(0);
    when(mockVersionManager.getApparentVersion()).thenReturn(apparentVersion);

    NodeManager.DatanodeFinalizationCounts datanodeCounts = NodeManager.DatanodeFinalizationCounts.newBuilder()
        .setNumFinalizedDatanodes(1)
        .setTotalHealthyDatanodes(3)
        .build();
    NodeManager mockNodeManager = mock(NodeManager.class);
    when(mockNodeManager.getDatanodeFinalizationCounts()).thenReturn(datanodeCounts);

    StorageContainerManager mockScm = mockScmForBlockServer();
    when(mockScm.getVersionManager()).thenReturn(mockVersionManager);
    when(mockScm.getScmNodeManager()).thenReturn(mockNodeManager);
    when(mockScm.getScmContext()).thenReturn(SCMContext.emptyContext());

    try (SCMBlockProtocolServer testServer = new SCMBlockProtocolServer(new OzoneConfiguration(), mockScm)) {
      HddsProtos.UpgradeStatus status = testServer.queryUpgradeStatus();
      assertEquals(HddsProtos.FinalizationStatus.FINALIZED, status.getScmFinalizationStatus());
      assertEquals(HddsProtos.FinalizationStatus.IN_PROGRESS, status.getHddsFinalizationStatus());
      assertEquals(1, status.getNumDatanodesFinalized());
      assertEquals(3, status.getNumDatanodesTotal());
    }
  }

  @Test
  public void testQueryUpgradeStatusInSafemode() {
    // Put SCM into safe mode via the context the server consults.
    scm.getScmContext().updateSafeModeStatus(SafeModeStatus.INITIAL);
    assertTrue(scm.getScmContext().isInSafeMode());

    // Querying upgrade status is blocked while SCM is in safe mode.
    SCMException ex = assertThrows(SCMException.class, () -> server.queryUpgradeStatus());
    assertEquals(SCMException.ResultCodes.SAFE_MODE_EXCEPTION, ex.getResult());
  }

  private SCMBlockProtocolServer finalizeServer(
      FinalizationManager finalizationManager, List<SCMNodeDetails> peers) throws IOException {
    // Default to all datanode versions matching SCM so the SCM peer checks are exercised in isolation.
    NodeManager.DatanodeFinalizationCounts datanodeCounts = NodeManager.DatanodeFinalizationCounts.newBuilder()
        .setNumFinalizedDatanodes(0)
        .setTotalHealthyDatanodes(0)
        .setMinApparentVersion(0)
        .setMaxApparentVersion(0)
        .setAllSoftwareVersionsMatchScm(true)
        .build();
    return finalizeServer(finalizationManager, peers, datanodeCounts);
  }

  private SCMBlockProtocolServer finalizeServer(
      FinalizationManager finalizationManager, List<SCMNodeDetails> peers,
      NodeManager.DatanodeFinalizationCounts datanodeCounts) throws IOException {
    StorageContainerManager mockScm = mockScmForBlockServer();
    when(mockScm.getFinalizationManager()).thenReturn(finalizationManager);
    when(mockScm.getConfiguration()).thenReturn(new OzoneConfiguration());

    SCMHANodeDetails haNodeDetails = mock(SCMHANodeDetails.class);
    when(haNodeDetails.getPeerNodeDetails()).thenReturn(peers);
    when(mockScm.getSCMHANodeDetails()).thenReturn(haNodeDetails);

    NodeManager mockNodeManager = mock(NodeManager.class);
    when(mockNodeManager.getDatanodeFinalizationCounts()).thenReturn(datanodeCounts);
    when(mockScm.getScmNodeManager()).thenReturn(mockNodeManager);
    return new SCMBlockProtocolServer(new OzoneConfiguration(), mockScm);
  }

  private StorageContainerManager mockScmForBlockServer() {
    StorageContainerManager mockScm = mock(StorageContainerManager.class);
    SCMNodeDetails scmNodeDetails = mock(SCMNodeDetails.class);
    when(scmNodeDetails.getBlockProtocolServerAddress()).thenReturn(new InetSocketAddress("localhost", 0));
    when(scmNodeDetails.getBlockProtocolServerAddressKey()).thenReturn("test");
    when(mockScm.getScmNodeDetails()).thenReturn(scmNodeDetails);
    return mockScm;
  }

  private StorageContainerLocationProtocol peerClient(HDDSVersion version) throws IOException {
    StorageContainerLocationProtocol client = mock(StorageContainerLocationProtocol.class);
    when(client.getPeerUpgradeStatus()).thenReturn(version);
    return client;
  }

  private SCMNodeDetails peerNode(String nodeId) {
    SCMNodeDetails node = mock(SCMNodeDetails.class);
    when(node.getNodeId()).thenReturn(nodeId);
    return node;
  }

  private List<String> getNetworkNames() {
    return nodeManager.getAllNodes().stream()
        .map(NodeImpl::getNetworkName)
        .collect(Collectors.toList());
  }

  private String nodeAddress(DatanodeDetails dn) {
    boolean useHostname = config.getBoolean(
        HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME,
        HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
    return useHostname ? dn.getHostName() : dn.getIpAddress();
  }
}
