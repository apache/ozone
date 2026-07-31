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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READONLY_ADMINISTRATORS;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.ALREADY_FINALIZED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManagerImpl;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMHANodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager;
import org.apache.hadoop.hdds.scm.server.upgrade.ScmVersionManager;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ozone.audit.SCMAction;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

/**
 * Unit tests to validate the SCMClientProtocolServer
 * servicing commands from the scm client.
 */
public class TestSCMClientProtocolServer {
  private static SCMClientProtocolServer server;
  private static StorageContainerManager scm;
  private static StorageContainerLocationProtocolServerSideTranslatorPB service;
  private static SCMSafeModeManager mockSafeModeManager;
  
  @BeforeAll
  static void setUp(@TempDir File testDir) throws Exception {
    OzoneConfiguration config = SCMTestUtils.getConf(testDir);

    mockSafeModeManager = mock(SCMSafeModeManager.class);

    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true));
    configurator.setScmContext(SCMContext.emptyContext());
    configurator.setScmSafeModeManager(mockSafeModeManager);
    config.set(OZONE_READONLY_ADMINISTRATORS, "testUser");
    scm = HddsTestUtils.getScm(config, configurator);
    scm.start();

    server = scm.getClientProtocolServer();
    service = new StorageContainerLocationProtocolServerSideTranslatorPB(server,
        scm, mock(ProtocolMessageMetrics.class));
  }

  @BeforeEach
  void setUp() {
    when(mockSafeModeManager.getInSafeMode()).thenReturn(false);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (scm != null) {
      scm.stop();
      scm.join();
    }
  }

  /**
   * Tests decommissioning of scm.
   */
  @Test
  public void testScmDecommissionRemoveScmErrors() throws Exception {
    String scmId = scm.getScmId();
    String err = "Cannot remove current leader.";

    DecommissionScmRequestProto request =
        DecommissionScmRequestProto.newBuilder()
            .setScmId(scmId)
            .build();

    DecommissionScmResponseProto resp =
        service.decommissionScm(request);

    // should have optional error message set in response
    assertTrue(resp.hasErrorMsg());
    assertEquals(err, resp.getErrorMsg());
  }

  @Test
  public void testReadOnlyAdmins() throws IOException {
    UserGroupInformation testUser = UserGroupInformation.
        createUserForTesting("testUser", new String[] {"testGroup"});

    try {
      // read operator
      server.getScm().checkAdminAccess(testUser, true);
      // write operator
      assertThrows(AccessControlException.class,
          () -> server.getScm().checkAdminAccess(testUser, false));
    } finally {
      UserGroupInformation.reset();
    }
  }

  /**
   * Tests listContainer of scm.
   */
  @Test
  public void testScmListContainer() throws Exception {
    SCMClientProtocolServer scmServer =
        new SCMClientProtocolServer(new OzoneConfiguration(),
            mockStorageContainerManager(), mock(ReconfigurationHandler.class));
    try {
      assertEquals(10, scmServer.listContainer(1, 10,
          null, HddsProtos.ReplicationType.RATIS, null).getContainerInfoList().size());
      // Test call from a legacy client, which uses a different method of listContainer
      assertEquals(10, scmServer.listContainer(1, 10, null,
          HddsProtos.ReplicationFactor.THREE).getContainerInfoList().size());
    } finally {
      scmServer.stop();
    }
  }

  @Test
  public void testScmGetContainerCount() throws IOException {
    SCMClientProtocolServer scmServer =
        new SCMClientProtocolServer(new OzoneConfiguration(),
            mockStorageContainerManager(), mock(ReconfigurationHandler.class));
    try {
      assertEquals(10, scmServer.getContainerCount(CLOSED));
    } finally {
      scmServer.stop();
    }
  }
  
  @Test
  public void testListContainerPaginationHasNoDuplicates() throws Exception {
    Instant base = Instant.parse("2026-01-01T00:00:00Z");
    List<ContainerInfo> infos = new ArrayList<>();
    infos.add(newContainerWithLastUsedTime(100, base));
    infos.add(newContainerWithLastUsedTime(5, base.plusMillis(1)));
    infos.add(newContainerWithLastUsedTime(10, base.plusMillis(2)));

    SCMClientProtocolServer scmServer = new SCMClientProtocolServer(new OzoneConfiguration(),
        mockStorageContainerManager(infos), mock(ReconfigurationHandler.class));
    try {
      List<Long> ids = new ArrayList<>();
      long start = 0;
      int batchSize = 2;
      while (true) {
        List<ContainerInfo> page =
            scmServer.listContainer(start, batchSize, null, null, null).getContainerInfoList();
        if (page.isEmpty()) {
          break;
        }
        for (ContainerInfo c : page) {
          ids.add(c.getContainerID());
        }
        start = page.get(page.size() - 1).getContainerID() + 1;
      }
      List<Long> expectedIds = Arrays.asList(5L, 10L, 100L);
      assertEquals(ids.size(), new HashSet<>(ids).size());
      assertEquals(expectedIds, ids);
    } finally {
      scmServer.stop();
    }
  }

  private StorageContainerManager mockStorageContainerManager() {
    List<ContainerInfo> infos = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      infos.add(newContainerInfoForTest());
    }
    return mockStorageContainerManager(infos);
  }

  private StorageContainerManager mockStorageContainerManager(List<ContainerInfo> infos) {
    ContainerManagerImpl containerManager = mock(ContainerManagerImpl.class);
    when(containerManager.getContainers()).thenReturn(infos);
    when(containerManager.getContainerStateCount(any(LifeCycleState.class))).thenReturn(infos.size());
    StorageContainerManager storageContainerManager = mock(StorageContainerManager.class);
    when(storageContainerManager.getContainerManager()).thenReturn(containerManager);

    SCMNodeDetails scmNodeDetails = mock(SCMNodeDetails.class);
    when(scmNodeDetails.getClientProtocolServerAddress()).thenReturn(new InetSocketAddress("localhost", 0));
    when(scmNodeDetails.getClientProtocolServerAddressKey()).thenReturn("test");
    when(storageContainerManager.getScmNodeDetails()).thenReturn(scmNodeDetails);
    return storageContainerManager;
  }

  @Test
  public void testLegacyFinalizeScmUpgradeAlreadyFinalized() throws Exception {
    FinalizationManager mockFinalizationManager = mock(FinalizationManager.class);
    SCMClientProtocolServer testServer = serverWithMockFinalization(false, mockFinalizationManager);
    try {
      StatusAndMessages result = testServer.finalizeScmUpgrade("testClientID");
      assertEquals(ALREADY_FINALIZED, result.status());
      assertTrue(result.msgs().isEmpty());
      verify(mockFinalizationManager, never()).finalizeUpgrade();
    } finally {
      testServer.stop();
    }
  }

  @Test
  public void testLegacyFinalizeScmUpgradeFinalizationRequired() throws Exception {
    FinalizationManager mockFinalizationManager = mock(FinalizationManager.class);
    SCMClientProtocolServer testServer = serverWithMockFinalization(true, mockFinalizationManager);
    try {
      StatusAndMessages result = testServer.finalizeScmUpgrade("testClientID");
      assertEquals(ALREADY_FINALIZED, result.status());
      assertTrue(result.msgs().isEmpty());
      verify(mockFinalizationManager, never()).finalizeUpgrade();
    } finally {
      testServer.stop();
    }
  }

  private SCMClientProtocolServer serverWithMockFinalization(
      boolean needsFinalization, FinalizationManager finalizationManager) throws IOException {
    ScmVersionManager mockVersionManager = mock(ScmVersionManager.class);
    when(mockVersionManager.needsFinalization()).thenReturn(needsFinalization);

    StorageContainerManager mockScm = mockStorageContainerManager();
    when(mockScm.getVersionManager()).thenReturn(mockVersionManager);
    when(mockScm.getFinalizationManager()).thenReturn(finalizationManager);

    return new SCMClientProtocolServer(
        new OzoneConfiguration(), mockScm, mock(ReconfigurationHandler.class));
  }

  @Test
  public void testGetPeerUpgradeStatusReturnsLocalVersion() throws IOException {
    assertEquals(HDDSVersion.SOFTWARE_VERSION, server.getPeerUpgradeStatus());
  }

  @Test
  public void testFinalizeProceedsWhenNoPeers() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    try (SCMClientProtocolServer testServer =
        peerCheckServer(finalizationManager, Collections.emptyList())) {
      testServer.finalizeUpgrade();
      verify(finalizationManager).finalizeUpgrade();
    }
  }

  @Test
  public void testFinalizeProceedsWhenAllPeersMatch() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    StorageContainerLocationProtocol matching = peerClient(HDDSVersion.SOFTWARE_VERSION);

    try (SCMClientProtocolServer testServer =
             peerCheckServer(finalizationManager, Arrays.asList(peerNode("scm2"), peerNode("scm3")));
         MockedStatic<HAUtils> haUtils = mockStatic(HAUtils.class)) {
      haUtils.when(() -> HAUtils.getScmContainerClientForNode(any(), any())).thenReturn(matching);
      testServer.finalizeUpgrade();
    }
    verify(finalizationManager).finalizeUpgrade();
  }

  @Test
  public void testFinalizeRejectsOlderPeer() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    StorageContainerLocationProtocol matching = peerClient(HDDSVersion.SOFTWARE_VERSION);
    StorageContainerLocationProtocol older = peerClient(HDDSVersion.DEFAULT_VERSION);

    try (SCMClientProtocolServer testServer =
             peerCheckServer(finalizationManager, Arrays.asList(peerNode("scm2"), peerNode("scm3")));
         MockedStatic<HAUtils> haUtils = mockStatic(HAUtils.class)) {
      haUtils.when(() -> HAUtils.getScmContainerClientForNode(any(), any())).thenReturn(matching, older);
      assertThrows(SCMException.class, testServer::finalizeUpgrade);
    }
    verify(finalizationManager, never()).finalizeUpgrade();
  }

  @Test
  public void testFinalizeRejectsUnknownFuturePeer() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    StorageContainerLocationProtocol matching = peerClient(HDDSVersion.SOFTWARE_VERSION);
    // A version not recognized by this binary deserializes to UNKNOWN_VERSION in the client translator.
    StorageContainerLocationProtocol unknown = peerClient(HDDSVersion.UNKNOWN_VERSION);

    try (SCMClientProtocolServer testServer =
             peerCheckServer(finalizationManager, Arrays.asList(peerNode("scm2"), peerNode("scm3")));
         MockedStatic<HAUtils> haUtils = mockStatic(HAUtils.class)) {
      haUtils.when(() -> HAUtils.getScmContainerClientForNode(any(), any()))
          .thenReturn(matching, unknown);
      assertThrows(SCMException.class, testServer::finalizeUpgrade);
    }
    verify(finalizationManager, never()).finalizeUpgrade();
  }

  @Test
  public void testFinalizeRejectsUnreachablePeer() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    StorageContainerLocationProtocol unreachable = mock(StorageContainerLocationProtocol.class);
    when(unreachable.getPeerUpgradeStatus()).thenThrow(new IOException("connection refused"));

    try (SCMClientProtocolServer testServer =
             peerCheckServer(finalizationManager, Collections.singletonList(peerNode("scm2")));
         MockedStatic<HAUtils> haUtils = mockStatic(HAUtils.class)) {
      haUtils.when(() -> HAUtils.getScmContainerClientForNode(any(), any())).thenReturn(unreachable);
      assertThrows(SCMException.class, testServer::finalizeUpgrade);
    }
    verify(finalizationManager, never()).finalizeUpgrade();
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
    try (SCMClientProtocolServer testServer = peerCheckServer(finalizationManager,
        Collections.emptyList(), datanodeCounts)) {
      testServer.finalizeUpgrade();
      verify(finalizationManager).finalizeUpgrade();
    }
  }

  @Test
  public void testFinalizeRejectsDatanodeWithMismatchedVersion() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    NodeManager.DatanodeFinalizationCounts datanodeCounts = NodeManager.DatanodeFinalizationCounts.newBuilder()
        .setNumFinalizedDatanodes(3)
        .setTotalHealthyDatanodes(3)
        .setMinApparentVersion(HDDSVersion.DEFAULT_VERSION.serialize())
        .setMaxApparentVersion(HDDSVersion.SOFTWARE_VERSION.serialize())
        .setAllSoftwareVersionsMatchScm(false)
        .build();
    try (SCMClientProtocolServer testServer = peerCheckServer(finalizationManager,
        Collections.emptyList(), datanodeCounts)) {
      assertThrows(SCMException.class, testServer::finalizeUpgrade);
    }
    verify(finalizationManager, never()).finalizeUpgrade();
  }

  @Test
  public void testForceFinalizeSkipsPeerVersionCheck() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    StorageContainerLocationProtocol matching = peerClient(HDDSVersion.SOFTWARE_VERSION);
    StorageContainerLocationProtocol older = peerClient(HDDSVersion.DEFAULT_VERSION);

    try (SCMClientProtocolServer testServer =
             peerCheckServer(finalizationManager, Arrays.asList(peerNode("scm2"), peerNode("scm3")));
         MockedStatic<HAUtils> haUtils = mockStatic(HAUtils.class)) {
      haUtils.when(() -> HAUtils.getScmContainerClientForNode(any(), any())).thenReturn(matching, older);
      testServer.forceFinalizeUpgrade();
    }
    verify(finalizationManager).finalizeUpgrade();
  }

  @Test
  public void testForceFinalizeSkipsUnreachablePeer() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    StorageContainerLocationProtocol unreachable = mock(StorageContainerLocationProtocol.class);
    when(unreachable.getPeerUpgradeStatus()).thenThrow(new IOException("connection refused"));

    try (SCMClientProtocolServer testServer =
             peerCheckServer(finalizationManager, Collections.singletonList(peerNode("scm2")));
         MockedStatic<HAUtils> haUtils = mockStatic(HAUtils.class)) {
      haUtils.when(() -> HAUtils.getScmContainerClientForNode(any(), any())).thenReturn(unreachable);
      // With force the peer version check is skipped, so an unreachable peer does not prevent
      // finalization and its version is never queried.
      testServer.forceFinalizeUpgrade();
    }
    verify(unreachable, never()).getPeerUpgradeStatus();
    verify(finalizationManager).finalizeUpgrade();
  }

  @Test
  public void testForceFinalizeSkipsDatanodeVersionCheck() throws IOException {
    FinalizationManager finalizationManager = mock(FinalizationManager.class);
    NodeManager.DatanodeFinalizationCounts datanodeCounts = NodeManager.DatanodeFinalizationCounts.newBuilder()
        .setNumFinalizedDatanodes(3)
        .setTotalHealthyDatanodes(3)
        .setMinApparentVersion(HDDSVersion.DEFAULT_VERSION.serialize())
        .setMaxApparentVersion(HDDSVersion.SOFTWARE_VERSION.serialize())
        .setAllSoftwareVersionsMatchScm(false)
        .build();
    try (SCMClientProtocolServer testServer = peerCheckServer(finalizationManager,
        Collections.emptyList(), datanodeCounts)) {
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
    try (SCMClientProtocolServer testServer = peerCheckServer(finalizationManager, Collections.emptyList())) {
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

  private SCMClientProtocolServer peerCheckServer(
      FinalizationManager finalizationManager, List<SCMNodeDetails> peers) throws IOException {
    // Default to all datanode versions matching SCM so the SCM peer checks are exercised in isolation.
    NodeManager.DatanodeFinalizationCounts datanodeCounts = NodeManager.DatanodeFinalizationCounts.newBuilder()
        .setNumFinalizedDatanodes(0)
        .setTotalHealthyDatanodes(0)
        .setMinApparentVersion(0)
        .setMaxApparentVersion(0)
        .setAllSoftwareVersionsMatchScm(true)
        .build();
    return peerCheckServer(finalizationManager, peers, datanodeCounts);
  }

  private SCMClientProtocolServer peerCheckServer(
      FinalizationManager finalizationManager, List<SCMNodeDetails> peers,
      NodeManager.DatanodeFinalizationCounts datanodeCounts) throws IOException {

    StorageContainerManager mockScm = mockStorageContainerManager();
    when(mockScm.getFinalizationManager()).thenReturn(finalizationManager);
    when(mockScm.getConfiguration()).thenReturn(new OzoneConfiguration());

    SCMHANodeDetails haNodeDetails = mock(SCMHANodeDetails.class);
    when(haNodeDetails.getPeerNodeDetails()).thenReturn(peers);
    when(mockScm.getSCMHANodeDetails()).thenReturn(haNodeDetails);

    NodeManager nodeManager = mock(NodeManager.class);
    when(nodeManager.getDatanodeFinalizationCounts()).thenReturn(datanodeCounts);
    when(mockScm.getScmNodeManager()).thenReturn(nodeManager);
    return new SCMClientProtocolServer(new OzoneConfiguration(), mockScm, mock(ReconfigurationHandler.class));
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

  @Test
  public void testQueryUpgradeStatus() throws Exception {
    HddsProtos.UpgradeStatus status = server.queryUpgradeStatus();

    // SCM starts already finalized in tests
    assertTrue(status.getScmFinalized());
    // No datanodes registered
    assertEquals(0, status.getNumDatanodesFinalized());
    assertEquals(0, status.getNumDatanodesTotal());
    assertTrue(status.getHddsFinalized());
  }

  @Test
  public void testQueryUpgradeStatusInSafemode() {
    // Put SCM into safe mode via the context the server consults.
    scm.getScmContext().updateSafeModeStatus(SafeModeStatus.INITIAL);
    try {
      assertTrue(scm.getScmContext().isInSafeMode());

      // Querying upgrade status is blocked while SCM is in safe mode.
      SCMException ex = assertThrows(SCMException.class, () -> server.queryUpgradeStatus());
      assertEquals(SCMException.ResultCodes.SAFE_MODE_EXCEPTION, ex.getResult());
    } finally {
      // Restore for other tests sharing the static SCM instance.
      scm.getScmContext().updateSafeModeStatus(SafeModeStatus.OUT_OF_SAFE_MODE);
    }
  }

  private ContainerInfo newContainerWithLastUsedTime(long containerId,
      Instant fixedLastUsedInstant) {
    return new ContainerInfo.Builder()
        .setContainerID(containerId)
        .setClock(Clock.fixed(fixedLastUsedInstant, ZoneOffset.UTC))
        .setPipelineID(PipelineID.randomId())
        .setReplicationConfig(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE))
        .build();
  }

  private ContainerInfo newContainerInfoForTest() {
    return new ContainerInfo.Builder()
        .setContainerID(1)
        .setPipelineID(PipelineID.randomId())
        .setReplicationConfig(
            RatisReplicationConfig
                .getInstance(HddsProtos.ReplicationFactor.THREE))
        .build();
  }
}
