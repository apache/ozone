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
import static org.apache.hadoop.hdds.scm.events.SCMEvents.CONTAINER_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.NODE_REGISTRATION_CONT_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.NODE_REPORT;
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
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.upgrade.ScmVersionManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

/**
 * Tests FCR lease protocol handling during HDDS finalization.
 */
class TestSCMDatanodeProtocolServer {
  @TempDir
  private Path tempDir;

  private SCMDatanodeProtocolServer server;
  private ScmVersionManager versionManager;
  private NodeManager nodeManager;
  private EventPublisher publisher;
  private DatanodeDetails datanode;

  @BeforeEach
  void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_DB_DIRS, tempDir.toString());
    StorageContainerManager scm = mock(StorageContainerManager.class);
    SCMStorageConfig storage = new SCMStorageConfig(conf);
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
    server = new SCMDatanodeProtocolServer(conf, scm, publisher, SCMContext.emptyContext());
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
  void testLeaseRequestsWaitForFinalization() throws Exception {
    SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(datanode.getProtoBufMessage()).setRequestFullContainerReportLease(true).build();

    assertFalse(server.sendHeartbeat(request).hasFullContainerReportLease());

    versionManager.finalizeUpgrade();

    assertTrue(server.sendHeartbeat(request).hasFullContainerReportLease());
  }

  @Test
  void testDeferredRegistrationRejectedBeforeFinalization() {
    ContainerReportsProto deferred = ContainerReportsProto.newBuilder().setFullContainerReportDeferred(true).build();

    IOException exception = assertThrows(IOException.class, () -> register(deferred));

    assertTrue(exception.getMessage().contains("finaliz"));
    verify(nodeManager, never()).register(any(), any(), any(), any());
    verifyNoInteractions(publisher);
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
    verify(publisher, never()).fireEvent(eq(NODE_REGISTRATION_CONT_REPORT), any());
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
            .setApparentVersion(HDDSVersion.SOFTWARE_VERSION.serialize())
            .setSoftwareVersion(HDDSVersion.SOFTWARE_VERSION.serialize()).build());
  }
}
