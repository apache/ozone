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

import static org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils.getInternalState;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DB_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_FULL_CONTAINER_REPORT_RATE_LIMIT_ENABLED;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_MAX_FULL_CONTAINER_REPORT_LEASES;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.CONTAINER_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.NODE_REGISTRATION_CONT_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.NODE_REPORT;
import static org.apache.ozone.test.MetricsAsserts.assertGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DatanodeVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.FullContainerReportLeaseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.upgrade.ScmVersionManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/**
 * Tests FCR lease protocol handling during HDDS finalization.
 */
class TestSCMDatanodeProtocolServer {
  @TempDir
  private Path tempDir;

  private SCMDatanodeProtocolServer server;
  private OzoneConfiguration serverConf;
  private StorageContainerManager scm;
  private ScmVersionManager versionManager;
  private NodeManager nodeManager;
  private EventPublisher publisher;
  private DatanodeDetails datanode;

  @BeforeEach
  void setup() throws Exception {
    serverConf = new OzoneConfiguration();
    serverConf.set(OZONE_SCM_DB_DIRS, tempDir.toString());
    scm = mock(StorageContainerManager.class);
    SCMStorageConfig storage = new SCMStorageConfig(serverConf);
    storage.setScmId("scm");
    storage.setApparentVersion(HDDSVersion.STREAM_PUTBLOCK_WITHOUT_RAFT.serialize());
    storage.initialize();
    versionManager = new ScmVersionManager(storage, scm, HashMap::new);
    when(scm.getVersionManager()).thenReturn(versionManager);
    nodeManager = mock(NodeManager.class);
    when(scm.getScmNodeManager()).thenReturn(nodeManager);
    SCMNodeDetails details = new SCMNodeDetails.Builder()
        .setDatanodeProtocolServerAddress(new InetSocketAddress("localhost", 0))
        .setDatanodeAddressKey(OZONE_SCM_DATANODE_ADDRESS_KEY).build();
    when(scm.getScmNodeDetails()).thenReturn(details);
    publisher = mock(EventPublisher.class);
    datanode = MockDatanodeDetails.randomDatanodeDetails();
    when(nodeManager.isNodeRegistered(any())).thenReturn(true);
    when(nodeManager.register(any(), any(), any(), any())).thenReturn(
        new RegisteredCommand(SCMRegisteredResponseProto.ErrorCode.success, datanode, "cluster"));
    server = new SCMDatanodeProtocolServer(serverConf, scm, publisher, SCMContext.emptyContext());
  }

  @AfterEach
  void cleanup() {
    if (server != null) {
      server.stop();
    }
    if (versionManager != null) {
      versionManager.close();
    }
  }

  @Test
  void testRateLimitCanBeDisabledAtStartup() throws Exception {
    server.stop();
    serverConf.setBoolean(OZONE_SCM_FULL_CONTAINER_REPORT_RATE_LIMIT_ENABLED, false);
    serverConf.setInt(OZONE_SCM_MAX_FULL_CONTAINER_REPORT_LEASES, 1);
    server = new SCMDatanodeProtocolServer(serverConf, scm, publisher, SCMContext.emptyContext());
    versionManager.finalizeUpgrade();
    SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(datanode.getProtoBufMessage()).setRequestFullContainerReportLease(true).build();

    assertTrue(server.sendHeartbeat(request).hasFullContainerReportLease());
    assertTrue(server.sendHeartbeat(request.toBuilder()
        .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails().getProtoBufMessage()).build())
        .hasFullContainerReportLease());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testLeaseRequestsWaitForFinalization(boolean rateLimitEnabled) throws Exception {
    server.reconfigureFCRRateLimit(Boolean.toString(rateLimitEnabled));
    SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(datanode.getProtoBufMessage()).setRequestFullContainerReportLease(true).build();

    assertFalse(server.sendHeartbeat(request).hasFullContainerReportLease());

    versionManager.finalizeUpgrade();

    assertTrue(server.sendHeartbeat(request).hasFullContainerReportLease());
  }

  @Test
  void testBypassStillWaitsForLeaderReadiness() throws Exception {
    server.stop();
    serverConf.setBoolean(OZONE_SCM_FULL_CONTAINER_REPORT_RATE_LIMIT_ENABLED, false);
    SCMContext context = mock(SCMContext.class);
    when(context.isLeader()).thenReturn(true);
    when(context.getTermOfLeaderIfReady()).thenReturn(OptionalLong.empty());
    server = new SCMDatanodeProtocolServer(serverConf, scm, publisher, context);
    versionManager.finalizeUpgrade();
    SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(datanode.getProtoBufMessage()).setRequestFullContainerReportLease(true).build();

    assertFalse(server.sendHeartbeat(request).hasFullContainerReportLease());
    when(context.getTermOfLeaderIfReady()).thenReturn(OptionalLong.of(7L));
    when(context.getTermOfLeader()).thenReturn(7L);
    assertEquals(7L, server.sendHeartbeat(request).getFullContainerReportLease().getTerm());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "flase", "0", "truefalse"})
  void testMalformedRateLimitSettingRejectedAtStartup(String value) {
    server.stop();
    serverConf.set(OZONE_SCM_FULL_CONTAINER_REPORT_RATE_LIMIT_ENABLED, value);

    assertThrows(IllegalArgumentException.class,
        () -> new SCMDatanodeProtocolServer(serverConf, scm, publisher, SCMContext.emptyContext()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "flase", "0", "truefalse"})
  void testMalformedReconfigurationKeepsRateLimitMode(String value) {
    String source = SCMFullContainerReportLeaseMetrics.class.getSimpleName();
    for (boolean enabled : new boolean[] {true, false}) {
      server.reconfigureFCRRateLimit(Boolean.toString(enabled));

      assertThrows(IllegalArgumentException.class, () -> server.reconfigureFCRRateLimit(value));
      assertGauge("FCRRateLimitEnabled", enabled ? 1 : 0, getMetrics(source));
    }
  }

  @Test
  void testReconfigurationCanRestoreDefaultRateLimit() {
    String source = SCMFullContainerReportLeaseMetrics.class.getSimpleName();
    assertGauge("FCRRateLimitEnabled", 1, getMetrics(source));
    assertEquals("false", server.reconfigureFCRRateLimit(" FALSE "));
    assertGauge("FCRRateLimitEnabled", 0, getMetrics(source));
    assertEquals("true", server.reconfigureFCRRateLimit(null));
    assertGauge("FCRRateLimitEnabled", 1, getMetrics(source));
  }

  @Test
  void testDeferredRegistrationRejectedBeforeFinalization() {
    ContainerReportsProto deferred = ContainerReportsProto.newBuilder().setFullContainerReportDeferred(true).build();

    IOException exception = assertThrows(IOException.class, () -> register(deferred));

    assertTrue(exception.getMessage().contains("finaliz"));
    verify(nodeManager, never()).register(any(), any(), any(), any());
    verifyNoInteractions(publisher);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testIncompatibleDeferredRegistrationIsFenced(boolean newerSoftware) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    SCMStorageConfig storage = mock(SCMStorageConfig.class);
    when(storage.getClusterID()).thenReturn("cluster");
    DatanodeVersionProto version = DatanodeVersionProto.newBuilder()
        .setApparentVersion(newerSoftware ? HDDSVersion.STREAM_PUTBLOCK_WITHOUT_RAFT.serialize()
            : HDDSVersion.SOFTWARE_VERSION.serialize())
        .setSoftwareVersion(HDDSVersion.SOFTWARE_VERSION.serialize() + (newerSoftware ? 1 : 0)).build();
    ContainerReportsProto deferred = ContainerReportsProto.newBuilder().setFullContainerReportDeferred(true).build();
    try (SCMNodeManager realNodeManager = new SCMNodeManager(conf, storage, publisher, new NetworkTopologyImpl(conf),
        SCMContext.emptyContext(), versionManager)) {
      when(scm.getScmNodeManager()).thenReturn(realNodeManager);
      try {
        SCMRegisteredResponseProto response = server.register(datanode.getExtendedProtoBufMessage(),
            NodeReportProto.getDefaultInstance(), deferred, PipelineReportsProto.getDefaultInstance(), version);

        assertEquals(SCMRegisteredResponseProto.ErrorCode.errorNodeNotPermitted, response.getErrorCode());
        assertTrue(realNodeManager.getAllNodes().isEmpty());
        verifyNoInteractions(publisher);
      } finally {
        when(scm.getScmNodeManager()).thenReturn(nodeManager);
      }
    }
  }

  @Test
  void testDeferredRegistrationKeepsVersionRejectionDuringFinalization() throws Exception {
    AtomicReference<HDDSVersion> apparentVersion = new AtomicReference<>(HDDSVersion.ZDU);
    ScmVersionManager upgrading = mock(ScmVersionManager.class);
    when(upgrading.getSoftwareVersion()).thenReturn(HDDSVersion.SOFTWARE_VERSION);
    when(upgrading.getApparentVersion()).thenAnswer(invocation -> apparentVersion.get());
    when(upgrading.needsFinalization()).thenReturn(true);
    when(upgrading.isAllowed(any(ComponentVersion.class))).thenAnswer(invocation -> {
      ComponentVersion required = invocation.getArgument(0);
      boolean allowed = required.isSupportedBy(apparentVersion.get());
      if (required == HDDSVersion.STREAM_PUTBLOCK_WITHOUT_RAFT) {
        apparentVersion.compareAndSet(HDDSVersion.ZDU, HDDSVersion.STREAM_PUTBLOCK_WITHOUT_RAFT);
      }
      return allowed;
    });
    OzoneConfiguration conf = new OzoneConfiguration();
    SCMStorageConfig storage = mock(SCMStorageConfig.class);
    when(storage.getClusterID()).thenReturn("cluster");
    DatanodeVersionProto version = DatanodeVersionProto.newBuilder()
        .setApparentVersion(HDDSVersion.STREAM_PUTBLOCK_WITHOUT_RAFT.serialize())
        .setSoftwareVersion(HDDSVersion.SOFTWARE_VERSION.serialize()).build();
    ContainerReportsProto deferred = ContainerReportsProto.newBuilder().setFullContainerReportDeferred(true).build();
    try (SCMNodeManager realNodeManager = new SCMNodeManager(conf, storage, publisher, new NetworkTopologyImpl(conf),
        SCMContext.emptyContext(), upgrading)) {
      when(scm.getScmNodeManager()).thenReturn(realNodeManager);
      when(scm.getVersionManager()).thenReturn(upgrading);
      try {
        SCMRegisteredResponseProto response = server.register(datanode.getExtendedProtoBufMessage(),
            NodeReportProto.getDefaultInstance(), deferred, PipelineReportsProto.getDefaultInstance(), version);

        assertEquals(SCMRegisteredResponseProto.ErrorCode.errorNodeNotPermitted, response.getErrorCode());
        assertEquals(HDDSVersion.STREAM_PUTBLOCK_WITHOUT_RAFT, apparentVersion.get());
        assertFalse(upgrading.isAllowed(HDDSVersion.FULL_CONTAINER_REPORT_LEASE));
        assertTrue(realNodeManager.getAllNodes().isEmpty());
        verifyNoInteractions(publisher);
      } finally {
        when(scm.getScmNodeManager()).thenReturn(nodeManager);
        when(scm.getVersionManager()).thenReturn(versionManager);
      }
    }
  }

  @Test
  void testLegacyRegistrationAcceptedBeforeFinalization() throws Exception {
    assertEquals(SCMRegisteredResponseProto.ErrorCode.success,
        register(ContainerReportsProto.getDefaultInstance()).getErrorCode());
    verify(publisher).fireEvent(eq(CONTAINER_REPORT), any());
    verify(publisher).fireEvent(eq(NODE_REGISTRATION_CONT_REPORT), any());
  }

  @Test
  void testDeferredRegistrationAcceptedAfterFinalization() throws Exception {
    versionManager.finalizeUpgrade();
    ContainerReportsProto deferred = ContainerReportsProto.newBuilder().setFullContainerReportDeferred(true).build();
    assertEquals(SCMRegisteredResponseProto.ErrorCode.success, register(deferred).getErrorCode());
    verify(publisher, never()).fireEvent(eq(CONTAINER_REPORT), any());
    ArgumentCaptor<SCMDatanodeProtocolServer.NodeRegistrationContainerReport> registration =
        ArgumentCaptor.forClass(SCMDatanodeProtocolServer.NodeRegistrationContainerReport.class);
    verify(publisher).fireEvent(eq(NODE_REGISTRATION_CONT_REPORT), registration.capture());
    assertEquals(datanode.getID(), registration.getValue().getDatanodeDetails().getID());
    assertEquals(0, registration.getValue().getContainerIDs().length);
    SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(datanode.getProtoBufMessage()).setRequestFullContainerReportLease(true).build();
    FullContainerReportLeaseProto lease = server.sendHeartbeat(request).getFullContainerReportLease();

    assertFalse(server.sendHeartbeat(request.toBuilder().clearRequestFullContainerReportLease()
        .setContainerReport(ContainerReportsProto.newBuilder().setFullContainerReportLease(lease)).build())
        .getFullContainerReportLeaseRejected());

    ArgumentCaptor<ContainerReportFromDatanode> report = ArgumentCaptor.forClass(ContainerReportFromDatanode.class);
    verify(publisher).fireEvent(eq(CONTAINER_REPORT), report.capture());
    assertTrue(report.getValue().isRegister());
    verify(publisher).fireEvent(eq(NODE_REGISTRATION_CONT_REPORT), any());
    report.getValue().complete(true);
  }

  @Test
  void testLegacyHeartbeatAcceptedBeforeFinalization() throws Exception {
    SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(datanode.getProtoBufMessage())
        .setContainerReport(ContainerReportsProto.getDefaultInstance()).build();

    assertFalse(server.sendHeartbeat(request).getFullContainerReportLeaseRejected());
    verify(publisher).fireEvent(eq(CONTAINER_REPORT), any());
  }

  @Test
  void testLeasedReportRejectedWithoutDroppingHeartbeat() throws Exception {
    SCMFullContainerReportLeaseManager leases =
        (SCMFullContainerReportLeaseManager) getInternalState(server, "fcrLeaseManager");
    long leaseId = leases.requestLease(datanode, SCMContext.INVALID_TERM).getAsLong();
    SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(datanode.getProtoBufMessage()).setRequestFullContainerReportLease(true)
        .setContainerReport(ContainerReportsProto.newBuilder()
            .setFullContainerReportLease(
                FullContainerReportLeaseProto.newBuilder().setId(leaseId).setTerm(SCMContext.INVALID_TERM)))
        .setNodeReport(NodeReportProto.getDefaultInstance()).build();

    assertTrue(server.sendHeartbeat(request).getFullContainerReportLeaseRejected());
    verify(publisher).fireEvent(eq(NODE_REPORT), any());
    verify(publisher, never()).fireEvent(eq(CONTAINER_REPORT), any());

    versionManager.finalizeUpgrade();

    assertFalse(server.sendHeartbeat(request).getFullContainerReportLeaseRejected());
    verify(publisher).fireEvent(eq(CONTAINER_REPORT), any());
  }

  private SCMRegisteredResponseProto register(ContainerReportsProto report) throws IOException {
    return server.register(datanode.getExtendedProtoBufMessage(), NodeReportProto.getDefaultInstance(), report,
        PipelineReportsProto.getDefaultInstance(), DatanodeVersionProto.newBuilder()
            .setApparentVersion(versionManager.getApparentVersion().serialize())
            .setSoftwareVersion(HDDSVersion.SOFTWARE_VERSION.serialize()).build());
  }
}
