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

package org.apache.hadoop.ozone.container.common.states.endpoint;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.reconcileContainerCommand;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.reconstructECContainersCommand;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.UnsafeByteOperations;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandQueueReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.FullContainerReportLeaseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.scm.net.HostAndPort;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine.DatanodeStates;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.upgrade.DatanodeVersionManager;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/**
 * This class tests the functionality of HeartbeatEndpointTask.
 */
public class TestHeartbeatEndpointTask {

  private static final HostAndPort TEST_SCM_ENDPOINT = new HostAndPort("test-scm-1", 9861);

  @Test
  public void handlesReconstructContainerCommand() throws Exception {
    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);

    List<DatanodeDetails> targetDns = new ArrayList<>();
    targetDns.add(MockDatanodeDetails.randomDatanodeDetails());
    targetDns.add(MockDatanodeDetails.randomDatanodeDetails());
    ReconstructECContainersCommand cmd = new ReconstructECContainersCommand(
        1, emptyList(), targetDns,
        UnsafeByteOperations.unsafeWrap(new byte[]{2, 5}),
        new ECReplicationConfig(3, 2));

    when(scm.sendHeartbeat(any()))
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto)invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .addCommands(SCMCommandProto.newBuilder()
                    .setCommandType(reconstructECContainersCommand)
                    .setReconstructECContainersCommandProto(cmd.getProto())
                    .build())
                .build());

    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine =
        mockDatanodeStateMachine();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "");

    when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));

    // WHEN
    HeartbeatEndpointTask task = getHeartbeatEndpointTask(conf, context, scm);
    task.call();

    // THEN
    assertEquals(1, context.getCommandQueueSummary()
        .get(reconstructECContainersCommand));
  }

  @Test
  public void testHandlesReconcileContainerCommand() throws Exception {
    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);

    Set<DatanodeDetails> peerDNs = new HashSet<>();
    peerDNs.add(MockDatanodeDetails.randomDatanodeDetails());
    peerDNs.add(MockDatanodeDetails.randomDatanodeDetails());
    ReconcileContainerCommand cmd = new ReconcileContainerCommand(1, peerDNs);

    when(scm.sendHeartbeat(any()))
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto)invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .addCommands(SCMCommandProto.newBuilder()
                    .setCommandType(reconcileContainerCommand)
                    .setReconcileContainerCommandProto(cmd.getProto())
                    .build())
                .build());

    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mockDatanodeStateMachine();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "");

    when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));

    // WHEN
    HeartbeatEndpointTask task = getHeartbeatEndpointTask(conf, context, scm);
    task.call();

    // THEN
    assertEquals(1, context.getCommandQueueSummary()
        .get(reconcileContainerCommand));
  }

  @Test
  public void testheartbeatWithoutReports() throws Exception {
    final long termInSCM = 42;
    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(
            StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto)invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .setTerm(termInSCM)
                .build());

    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mockDatanodeStateMachine();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "");

    when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));
    context.setTermOfLeaderSCM(1);
    HeartbeatEndpointTask endpointTask = getHeartbeatEndpointTask(
        conf, context, scm);
    endpointTask.call();
    SCMHeartbeatRequestProto heartbeat = argument.getValue();
    assertTrue(heartbeat.hasDatanodeDetails());
    assertFalse(heartbeat.hasNodeReport());
    assertFalse(heartbeat.hasContainerReport());
    assertEquals(0, heartbeat.getCommandStatusReportsCount());
    assertFalse(heartbeat.hasContainerActions());
    OptionalLong termInDatanode = context.getTermOfLeaderSCM();
    assertTrue(termInDatanode.isPresent());
    assertEquals(termInSCM, termInDatanode.getAsLong());
  }

  @Test
  public void testheartbeatWithNodeReports() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mockDatanodeStateMachine();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "");

    when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));
    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(
            StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto)invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .build());

    HeartbeatEndpointTask endpointTask = getHeartbeatEndpointTask(
        conf, context, scm);
    context.addEndpoint(TEST_SCM_ENDPOINT);
    context.refreshFullReport(NodeReportProto.getDefaultInstance());
    endpointTask.call();
    SCMHeartbeatRequestProto heartbeat = argument.getValue();
    assertTrue(heartbeat.hasDatanodeDetails());
    assertTrue(heartbeat.hasNodeReport());
    assertFalse(heartbeat.hasContainerReport());
    assertEquals(0, heartbeat.getCommandStatusReportsCount());
    assertFalse(heartbeat.hasContainerActions());
  }

  @Test
  public void testheartbeatWithContainerReports() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mockDatanodeStateMachine();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "");

    when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));

    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(
            StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto)invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .build());

    HeartbeatEndpointTask endpointTask = getHeartbeatEndpointTask(
        conf, context, scm);
    context.addEndpoint(TEST_SCM_ENDPOINT);
    context.refreshFullReport(ContainerReportsProto.getDefaultInstance());
    endpointTask.call();
    SCMHeartbeatRequestProto heartbeat = argument.getValue();
    assertTrue(heartbeat.hasDatanodeDetails());
    assertFalse(heartbeat.hasNodeReport());
    assertTrue(heartbeat.hasContainerReport());
    assertEquals(0, heartbeat.getCommandStatusReportsCount());
    assertFalse(heartbeat.hasContainerActions());
  }

  @Test
  public void leasedFCRRequestsLeaseBeforeSendingReport() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mock(DatanodeStateMachine.class);
    stubFullContainerReport(datanodeStateMachine,
        ContainerReportsProto.getDefaultInstance());
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "");

    when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));

    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto)invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .setTerm(7L)
                .setFullContainerReportLease(
                    FullContainerReportLeaseProto.newBuilder()
                        .setId(99L)
                        .setTerm(7L))
                .build());

    DatanodeDetails datanodeDetails = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .build();
    EndpointStateMachine endpointStateMachine = new EndpointStateMachine(
        TEST_SCM_ENDPOINT, scm, conf, "");
    endpointStateMachine.setVersion(VersionResponse.newBuilder()
        .setVersion(1)
        .addValue(OzoneConsts.SCM_FCR_LEASE_SUPPORTED, Boolean.TRUE.toString())
        .build());
    HDDSLayoutVersionManager layoutVersionManager =
        mock(HDDSLayoutVersionManager.class);
    when(layoutVersionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());
    when(layoutVersionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());

    HeartbeatEndpointTask endpointTask = HeartbeatEndpointTask.newBuilder()
        .setConfig(conf)
        .setDatanodeDetails(datanodeDetails)
        .setContext(context)
        .setLayoutVersionManager(layoutVersionManager)
        .setEndpointStateMachine(endpointStateMachine)
        .build();

    context.addEndpoint(TEST_SCM_ENDPOINT);
    context.refreshFullReport(ContainerReportsProto.getDefaultInstance());

    endpointTask.call();
    SCMHeartbeatRequestProto firstHeartbeat = argument.getValue();
    assertFalse(firstHeartbeat.hasContainerReport());
    assertTrue(firstHeartbeat.getRequestFullContainerReportLease());

    endpointTask.call();
    SCMHeartbeatRequestProto secondHeartbeat = argument.getValue();
    assertTrue(secondHeartbeat.hasContainerReport());
    assertEquals(99L, secondHeartbeat.getContainerReport()
        .getFullContainerReportLease().getId());
    assertEquals(7L, secondHeartbeat.getContainerReport()
        .getFullContainerReportLease().getTerm());
    assertFalse(secondHeartbeat.getRequestFullContainerReportLease());
  }

  @Test
  public void leasedFCRUsesFreshContainerSnapshot() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mock(DatanodeStateMachine.class);
    OzoneContainer ozoneContainer = mock(OzoneContainer.class);
    ContainerSet containerSet = mock(ContainerSet.class);
    ContainerReportsProto staleReport = ContainerReportsProto.newBuilder()
        .addReports(ContainerReplicaProto.newBuilder()
            .setContainerID(1)
            .setState(ContainerReplicaProto.State.CLOSING))
        .build();
    ContainerReportsProto freshReport = ContainerReportsProto.newBuilder()
        .addReports(ContainerReplicaProto.newBuilder()
            .setContainerID(1)
            .setState(ContainerReplicaProto.State.CLOSED))
        .build();
    when(datanodeStateMachine.getContainer()).thenReturn(ozoneContainer);
    when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
    when(containerSet.getContainerReport()).thenReturn(freshReport);
    when(ozoneContainer.getPipelineReport())
        .thenReturn(PipelineReportsProto.getDefaultInstance());
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "") {
      @Override
      public ContainerReportsProto getFullContainerReportDiscardPendingICR(
          HostAndPort endpoint) throws IOException {
        ContainerReportsProto report =
            super.getFullContainerReportDiscardPendingICR(endpoint);
        refreshFullReport(freshReport);
        return report;
      }
    };

    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto) invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .build());

    DatanodeDetails datanodeDetails = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .build();
    EndpointStateMachine endpointStateMachine = new EndpointStateMachine(
        TEST_SCM_ENDPOINT, scm, conf, "");
    endpointStateMachine.setVersion(VersionResponse.newBuilder()
        .setVersion(1)
        .addValue(OzoneConsts.SCM_FCR_LEASE_SUPPORTED, Boolean.TRUE.toString())
        .build());
    endpointStateMachine.setFullContainerReportLease(
        FullContainerReportLeaseProto.newBuilder()
            .setId(99L)
            .setTerm(7L)
            .build());
    HDDSLayoutVersionManager layoutVersionManager =
        mock(HDDSLayoutVersionManager.class);
    when(layoutVersionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());
    when(layoutVersionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());
    HeartbeatEndpointTask endpointTask = HeartbeatEndpointTask.newBuilder()
        .setConfig(conf)
        .setDatanodeDetails(datanodeDetails)
        .setContext(context)
        .setLayoutVersionManager(layoutVersionManager)
        .setEndpointStateMachine(endpointStateMachine)
        .build();
    context.addEndpoint(TEST_SCM_ENDPOINT);
    context.refreshFullReport(staleReport);

    endpointTask.call();

    ContainerReportsProto sentReport = argument.getValue().getContainerReport();
    assertEquals(ContainerReplicaProto.State.CLOSED,
        sentReport.getReports(0).getState());
    assertEquals(99L, sentReport.getFullContainerReportLease().getId());
    assertTrue(context.isFullContainerReportReady(TEST_SCM_ENDPOINT));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void leasedFCRSnapshotFailureDoesNotInterruptHeartbeat(boolean runtimeFailure) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mock(DatanodeStateMachine.class);
    ContainerReportsProto report = ContainerReportsProto.newBuilder()
        .addReports(ContainerReplicaProto.newBuilder().setContainerID(1).setState(ContainerReplicaProto.State.CLOSED))
        .build();
    stubFullContainerReport(datanodeStateMachine, report);
    Exception failure =
        runtimeFailure ? new IllegalStateException("snapshot failed") : new IOException("snapshot failed");
    when(datanodeStateMachine.getContainer().getContainerSet().getContainerReport())
        .thenThrow(failure).thenReturn(report);
    when(datanodeStateMachine.getQueuedCommandCount()).thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING, datanodeStateMachine, "");
    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor.forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(argument.capture())).thenAnswer(invocation -> SCMHeartbeatResponseProto.newBuilder()
        .setDatanodeUUID(((SCMHeartbeatRequestProto) invocation.getArgument(0)).getDatanodeDetails().getUuid())
        .build());
    HDDSLayoutVersionManager layoutVersionManager = mock(HDDSLayoutVersionManager.class);
    when(layoutVersionManager.getSoftwareLayoutVersion()).thenReturn(maxLayoutVersion());
    when(layoutVersionManager.getMetadataLayoutVersion()).thenReturn(maxLayoutVersion());
    IncrementalContainerReportProto icr = IncrementalContainerReportProto.newBuilder()
        .addReport(ContainerReplicaProto.newBuilder().setContainerID(1).setState(ContainerReplicaProto.State.CLOSING))
        .build();
    context.addEndpoint(TEST_SCM_ENDPOINT);
    context.refreshFullReport(ContainerReportsProto.getDefaultInstance());
    context.refreshFullReport(NodeReportProto.getDefaultInstance());
    context.refreshFullReport(PipelineReportsProto.getDefaultInstance());
    context.addIncrementalReport(icr);
    context.addIncrementalReport(CommandStatusReportsProto.getDefaultInstance());
    try (EndpointStateMachine endpoint = new EndpointStateMachine(TEST_SCM_ENDPOINT, scm, conf, "")) {
      endpoint.setState(EndpointStateMachine.EndPointStates.HEARTBEAT);
      endpoint.setVersion(VersionResponse.newBuilder().setVersion(1)
          .addValue(OzoneConsts.SCM_FCR_LEASE_SUPPORTED, Boolean.TRUE.toString()).build());
      endpoint.setFullContainerReportLease(FullContainerReportLeaseProto.newBuilder().setId(99).setTerm(7).build());
      HeartbeatEndpointTask task = HeartbeatEndpointTask.newBuilder().setConfig(conf).setContext(context)
          .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails()).setLayoutVersionManager(layoutVersionManager)
          .setEndpointStateMachine(endpoint).build();

      task.call();

      SCMHeartbeatRequestProto heartbeat = argument.getValue();
      assertFalse(heartbeat.hasContainerReport());
      assertEquals(singletonList(icr), heartbeat.getIncrementalContainerReportList());
      assertEquals(1, heartbeat.getCommandStatusReportsCount());
      assertTrue(heartbeat.hasNodeReport());
      assertTrue(heartbeat.hasPipelineReports());
      assertTrue(heartbeat.hasCommandQueueReport());
      assertTrue(context.isFullContainerReportReady(TEST_SCM_ENDPOINT));
      assertTrue(endpoint.hasFullContainerReportLease());
      assertEquals(0, endpoint.getMissedCount());
      assertTrue(endpoint.getLastSuccessfulHeartbeat() > 0);
      context.addIncrementalReport(icr);

      task.call();

      heartbeat = argument.getValue();
      assertEquals(report.getReportsList(), heartbeat.getContainerReport().getReportsList());
      assertEquals(99, heartbeat.getContainerReport().getFullContainerReportLease().getId());
      assertEquals(0, heartbeat.getIncrementalContainerReportCount());
      assertFalse(context.isFullContainerReportReady(TEST_SCM_ENDPOINT));
      assertFalse(endpoint.hasFullContainerReportLease());
    }
  }

  @Test
  public void leasedFCRIsExcludedWhenItBecomesReadyWhileBuildingHeartbeat()
      throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mock(DatanodeStateMachine.class);
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "") {
      @Override
      public boolean isFullContainerReportReady(HostAndPort endpoint) {
        refreshFullReport(ContainerReportsProto.getDefaultInstance());
        return false;
      }
    };
    when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));

    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto) invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .build());

    DatanodeDetails datanodeDetails = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .build();
    EndpointStateMachine endpointStateMachine = new EndpointStateMachine(
        TEST_SCM_ENDPOINT, scm, conf, "");
    endpointStateMachine.setVersion(VersionResponse.newBuilder()
        .setVersion(1)
        .addValue(OzoneConsts.SCM_FCR_LEASE_SUPPORTED, Boolean.TRUE.toString())
        .build());
    endpointStateMachine.setFullContainerReportLease(
        FullContainerReportLeaseProto.newBuilder()
            .setId(99L)
            .setTerm(7L)
            .build());
    HDDSLayoutVersionManager layoutVersionManager =
        mock(HDDSLayoutVersionManager.class);
    when(layoutVersionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());
    when(layoutVersionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());
    HeartbeatEndpointTask endpointTask = HeartbeatEndpointTask.newBuilder()
        .setConfig(conf)
        .setDatanodeDetails(datanodeDetails)
        .setContext(context)
        .setLayoutVersionManager(layoutVersionManager)
        .setEndpointStateMachine(endpointStateMachine)
        .build();
    context.addEndpoint(TEST_SCM_ENDPOINT);

    endpointTask.call();

    assertFalse(argument.getValue().hasContainerReport());
  }

  @Test
  public void leasedFCRIsRetriedAfterHeartbeatFailure() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mock(DatanodeStateMachine.class);
    stubFullContainerReport(datanodeStateMachine,
        ContainerReportsProto.getDefaultInstance());
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "");
    when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));

    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(argument.capture()))
        .thenThrow(new IOException("heartbeat failed"))
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto) invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .build());

    DatanodeDetails datanodeDetails = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .build();
    EndpointStateMachine endpointStateMachine = new EndpointStateMachine(
        TEST_SCM_ENDPOINT, scm, conf, "");
    endpointStateMachine.setVersion(VersionResponse.newBuilder()
        .setVersion(1)
        .addValue(OzoneConsts.SCM_FCR_LEASE_SUPPORTED, Boolean.TRUE.toString())
        .build());
    endpointStateMachine.setFullContainerReportLease(
        FullContainerReportLeaseProto.newBuilder()
            .setId(99L)
            .setTerm(7L)
            .build());
    HDDSLayoutVersionManager layoutVersionManager =
        mock(HDDSLayoutVersionManager.class);
    when(layoutVersionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());
    when(layoutVersionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());
    HeartbeatEndpointTask endpointTask = HeartbeatEndpointTask.newBuilder()
        .setConfig(conf)
        .setDatanodeDetails(datanodeDetails)
        .setContext(context)
        .setLayoutVersionManager(layoutVersionManager)
        .setEndpointStateMachine(endpointStateMachine)
        .build();
    context.addEndpoint(TEST_SCM_ENDPOINT);
    context.refreshFullReport(ContainerReportsProto.getDefaultInstance());

    endpointTask.call();
    assertTrue(context.isFullContainerReportReady(TEST_SCM_ENDPOINT));

    endpointTask.call();
    assertFalse(argument.getValue().hasContainerReport());
    assertTrue(argument.getValue().getRequestFullContainerReportLease());
  }

  @Test
  public void leasedFCRIsRetriedAfterLeaseRejection() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mock(DatanodeStateMachine.class);
    stubFullContainerReport(datanodeStateMachine,
        ContainerReportsProto.getDefaultInstance());
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "");
    when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));

    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto) invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .setFullContainerReportLeaseRejected(true)
                .build())
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto) invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .build());

    DatanodeDetails datanodeDetails = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .build();
    EndpointStateMachine endpointStateMachine = new EndpointStateMachine(
        TEST_SCM_ENDPOINT, scm, conf, "");
    endpointStateMachine.setVersion(VersionResponse.newBuilder()
        .setVersion(1)
        .addValue(OzoneConsts.SCM_FCR_LEASE_SUPPORTED, Boolean.TRUE.toString())
        .build());
    endpointStateMachine.setFullContainerReportLease(
        FullContainerReportLeaseProto.newBuilder()
            .setId(99L)
            .setTerm(7L)
            .build());
    HDDSLayoutVersionManager layoutVersionManager =
        mock(HDDSLayoutVersionManager.class);
    when(layoutVersionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());
    when(layoutVersionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());
    HeartbeatEndpointTask endpointTask = HeartbeatEndpointTask.newBuilder()
        .setConfig(conf)
        .setDatanodeDetails(datanodeDetails)
        .setContext(context)
        .setLayoutVersionManager(layoutVersionManager)
        .setEndpointStateMachine(endpointStateMachine)
        .build();
    context.addEndpoint(TEST_SCM_ENDPOINT);
    context.refreshFullReport(ContainerReportsProto.getDefaultInstance());

    endpointTask.call();
    assertTrue(context.isFullContainerReportReady(TEST_SCM_ENDPOINT));

    endpointTask.call();
    assertFalse(argument.getValue().hasContainerReport());
    assertTrue(argument.getValue().getRequestFullContainerReportLease());
  }

  @Test
  public void testheartbeatWithCommandStatusReports() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mockDatanodeStateMachine();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "");

    when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));

    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(
            StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto)invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .build());

    HeartbeatEndpointTask endpointTask = getHeartbeatEndpointTask(
        conf, context, scm);
    context.addEndpoint(TEST_SCM_ENDPOINT);
    context.addIncrementalReport(
        CommandStatusReportsProto.getDefaultInstance());
    endpointTask.call();
    SCMHeartbeatRequestProto heartbeat = argument.getValue();
    assertTrue(heartbeat.hasDatanodeDetails());
    assertFalse(heartbeat.hasNodeReport());
    assertFalse(heartbeat.hasContainerReport());
    assertNotEquals(0, heartbeat.getCommandStatusReportsCount());
    assertFalse(heartbeat.hasContainerActions());
  }

  @Test
  public void testheartbeatWithContainerActions() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mockDatanodeStateMachine();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "");

    when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));

    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(
            StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto)invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .build());

    HeartbeatEndpointTask endpointTask = getHeartbeatEndpointTask(
        conf, context, scm);
    context.addEndpoint(TEST_SCM_ENDPOINT);
    context.addContainerAction(getContainerAction());
    endpointTask.call();
    SCMHeartbeatRequestProto heartbeat = argument.getValue();
    assertTrue(heartbeat.hasDatanodeDetails());
    assertFalse(heartbeat.hasNodeReport());
    assertFalse(heartbeat.hasContainerReport());
    assertEquals(0, heartbeat.getCommandStatusReportsCount());
    assertTrue(heartbeat.hasContainerActions());
  }

  @Test
  public void testheartbeatWithAllReports() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine =
        mockDatanodeStateMachine();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "");

    // Return a Map of command counts when the heartbeat logic requests it
    final EnumCounters<SCMCommandProto.Type> commands = new EnumCounters<>(SCMCommandProto.Type.class);
    int count = 1;
    for (SCMCommandProto.Type cmd : SCMCommandProto.Type.values()) {
      commands.set(cmd, count++);
    }
    when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(commands);

    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(
            StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto)invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .build());

    HeartbeatEndpointTask endpointTask = getHeartbeatEndpointTask(
        conf, context, scm);
    context.addEndpoint(TEST_SCM_ENDPOINT);
    context.refreshFullReport(NodeReportProto.getDefaultInstance());
    context.refreshFullReport(ContainerReportsProto.getDefaultInstance());
    context.addIncrementalReport(
        CommandStatusReportsProto.getDefaultInstance());
    context.addContainerAction(getContainerAction());
    endpointTask.call();
    SCMHeartbeatRequestProto heartbeat = argument.getValue();
    assertTrue(heartbeat.hasDatanodeDetails());
    assertTrue(heartbeat.hasNodeReport());
    assertTrue(heartbeat.hasContainerReport());
    assertNotEquals(0, heartbeat.getCommandStatusReportsCount());
    assertTrue(heartbeat.hasContainerActions());
    assertTrue(heartbeat.hasCommandQueueReport());
    CommandQueueReportProto queueCount = heartbeat.getCommandQueueReport();
    int commandCount = 0;
    for (SCMCommandProto.Type type : SCMCommandProto.Type.values()) {
      if (commands.get(type) > 0) {
        commandCount++;
      }
    }
    assertEquals(queueCount.getCommandCount(), commandCount);
    assertEquals(queueCount.getCountCount(), commandCount);
    for (int i = 0; i < commandCount; i++) {
      assertEquals(commands.get(queueCount.getCommand(i)),
          queueCount.getCount(i));
    }
  }

  @Test
  public void testDatanodeCurrentVersionPassThrough() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mockDatanodeStateMachine();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "");

    // Set the expected versions to return.
    DatanodeVersionManager versionManager = datanodeStateMachine.getVersionManager();
    when(versionManager.getVersionForClient())
        .thenReturn(HDDSVersion.DEFAULT_VERSION)
        .thenReturn(HDDSVersion.ZDU)
        .thenReturn(HDDSVersion.SOFTWARE_VERSION);

    when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));

    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto)invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .build());

    HeartbeatEndpointTask endpointTask = getHeartbeatEndpointTask(
        conf, context, scm);

    // Assert the expected versions were returned.
    endpointTask.call();
    assertEquals(HDDSVersion.DEFAULT_VERSION.serialize(),
        argument.getValue().getDatanodeDetails().getCurrentVersion());
    endpointTask.call();
    assertEquals(HDDSVersion.ZDU.serialize(),
        argument.getValue().getDatanodeDetails().getCurrentVersion());
    endpointTask.call();
    assertEquals(HDDSVersion.SOFTWARE_VERSION.serialize(),
        argument.getValue().getDatanodeDetails().getCurrentVersion());
  }

  @Test
  public void testDatanodeApparentVersionPassThrough() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mockDatanodeStateMachine();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "");

    // Set the expected versions to return.
    DatanodeVersionManager versionManager = datanodeStateMachine.getVersionManager();
    when(versionManager.getApparentVersion())
        .thenReturn(HDDSLayoutFeature.INITIAL_VERSION)
        .thenReturn(HDDSVersion.ZDU)
        .thenReturn(HDDSVersion.SOFTWARE_VERSION);

    when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));

    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto)invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .build());

    HeartbeatEndpointTask endpointTask = getHeartbeatEndpointTask(
        conf, context, scm);

    // Assert the expected versions were returned.
    endpointTask.call();
    assertEquals(HDDSLayoutFeature.INITIAL_VERSION.serialize(),
        argument.getValue().getDatanodeVersion().getApparentVersion());
    endpointTask.call();
    assertEquals(HDDSVersion.ZDU.serialize(),
        argument.getValue().getDatanodeVersion().getApparentVersion());
    endpointTask.call();
    assertEquals(HDDSVersion.SOFTWARE_VERSION.serialize(),
        argument.getValue().getDatanodeVersion().getApparentVersion());
  }

  /**
   * Creates HeartbeatEndpointTask with the given conf, context and
   * StorageContainerManager client side proxy.
   *
   * @param conf Configuration
   * @param context StateContext
   * @param proxy StorageContainerDatanodeProtocolClientSideTranslatorPB
   *
   * @return HeartbeatEndpointTask
   */
  private HeartbeatEndpointTask getHeartbeatEndpointTask(
      ConfigurationSource conf,
      StateContext context,
      StorageContainerDatanodeProtocolClientSideTranslatorPB proxy) {
    EndpointStateMachine endpointStateMachine = mock(EndpointStateMachine.class);
    when(endpointStateMachine.getEndPoint()).thenReturn(proxy);
    when(endpointStateMachine.getAddress())
        .thenReturn(TEST_SCM_ENDPOINT);
    return HeartbeatEndpointTask.newBuilder()
        .setConfig(conf)
        .setContext(context)
        .setEndpointStateMachine(endpointStateMachine)
        .build();
  }

  private DatanodeStateMachine mockDatanodeStateMachine() {
    DatanodeDetails datanodeDetails = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .build();
    DatanodeStateMachine mockDSM = mock(DatanodeStateMachine.class);
    when(mockDSM.getDatanodeDetails()).thenReturn(datanodeDetails);
    DatanodeVersionManager versionManager = ContainerTestUtils.mockDnVersionManager();
    when(mockDSM.getVersionManager()).thenReturn(versionManager);
    return mockDSM;
  }

  private static void stubFullContainerReport(
      DatanodeStateMachine datanodeStateMachine,
      ContainerReportsProto containerReport) throws IOException {
    OzoneContainer ozoneContainer = mock(OzoneContainer.class);
    ContainerSet containerSet = mock(ContainerSet.class);
    when(datanodeStateMachine.getContainer()).thenReturn(ozoneContainer);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
    when(containerSet.getContainerReport()).thenReturn(containerReport);
    when(ozoneContainer.getPipelineReport())
        .thenReturn(PipelineReportsProto.getDefaultInstance());
  }

  private ContainerAction getContainerAction() {
    ContainerAction.Builder builder = ContainerAction.newBuilder();
    builder.setContainerID(1L)
        .setAction(ContainerAction.Action.CLOSE)
        .setReason(ContainerAction.Reason.CONTAINER_FULL);
    return builder.build();
  }
}
