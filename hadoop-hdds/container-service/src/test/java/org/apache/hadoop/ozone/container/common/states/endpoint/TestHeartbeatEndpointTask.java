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
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.UnsafeByteOperations;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.HddsConfigKeys;
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
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.DatanodeStorage;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine.DatanodeStates;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.upgrade.DatanodeVersionManager;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/**
 * This class tests the functionality of HeartbeatEndpointTask.
 */
public class TestHeartbeatEndpointTask {

  private static final HostAndPort TEST_SCM_ENDPOINT = new HostAndPort("test-scm-1", 9861);

  @Test
  void testVolumeFailurePreservesICRsWhileWaitingForLease() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanode = mockDatanodeStateMachine();
    ContainerSet containers = newContainerSet();
    HddsVolume healthyVolume = mock(HddsVolume.class);
    HddsVolume failedVolume = mock(HddsVolume.class);
    for (long containerId = 1; containerId <= 2; containerId++) {
      KeyValueContainerData data = new KeyValueContainerData(containerId, ContainerLayoutVersion.FILE_PER_CHUNK,
          1024, UUID.randomUUID().toString(), datanode.getDatanodeDetails().getUuidString());
      data.setVolume(containerId == 1 ? healthyVolume : failedVolume);
      data.closeContainer();
      containers.addContainer(new KeyValueContainer(data, conf));
    }
    OzoneContainer container = mock(OzoneContainer.class);
    when(datanode.getContainer()).thenReturn(container);
    when(container.getContainerSet()).thenReturn(containers);
    when(container.getPipelineReport()).thenReturn(PipelineReportsProto.getDefaultInstance());
    when(datanode.getQueuedCommandCount()).thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING, datanode, "");
    SCMConnectionManager connections = mock(SCMConnectionManager.class);
    when(datanode.getConnectionManager()).thenReturn(connections);
    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> requests = ArgumentCaptor.forClass(SCMHeartbeatRequestProto.class);
    AtomicBoolean grantLease = new AtomicBoolean();
    when(scm.sendHeartbeat(requests.capture())).thenAnswer(invocation -> {
      SCMHeartbeatRequestProto request = invocation.getArgument(0);
      SCMHeartbeatResponseProto.Builder response = SCMHeartbeatResponseProto.newBuilder()
          .setDatanodeUUID(request.getDatanodeDetails().getUuid());
      if (request.getRequestFullContainerReportLease() && grantLease.get()) {
        response.setFullContainerReportLease(FullContainerReportLeaseProto.newBuilder().setId(99));
      }
      return response.build();
    });
    HostAndPort otherAddress = new HostAndPort("other-scm", 9861);
    HostAndPort reconAddress = new HostAndPort("recon", 9861);
    try (EndpointStateMachine waiting = new EndpointStateMachine(TEST_SCM_ENDPOINT, scm, conf, "");
         EndpointStateMachine other = new EndpointStateMachine(otherAddress, scm, conf, "");
         EndpointStateMachine recon = new EndpointStateMachine(reconAddress, scm, conf, "")) {
      waiting.setState(EndpointStateMachine.EndPointStates.HEARTBEAT);
      recon.setPassive(true);
      List<EndpointStateMachine> endpoints = Arrays.asList(waiting, other, recon);
      when(connections.getValues()).thenReturn(endpoints);
      endpoints.forEach(endpoint -> context.addEndpoint(endpoint.getAddress()));
      IncrementalContainerReportProto icr = IncrementalContainerReportProto.newBuilder()
          .addReport(containers.getContainer(1).getContainerReport()).build();
      context.addIncrementalReport(icr);
      when(failedVolume.isFailed()).thenReturn(true);

      containers.handleVolumeFailures(context);

      assertTrue(containers.getMissingContainerSet().contains(2L));
      assertEquals(1, containers.containerCount());
      ContainerReportsProto expected = containers.getContainerReport();
      assertEquals(Collections.singletonList(expected), context.getAllAvailableReports(reconAddress));
      verify(datanode).triggerHeartbeat();
      HeartbeatEndpointTask task = HeartbeatEndpointTask.newBuilder().setConfig(conf).setContext(context)
          .setEndpointStateMachine(waiting).build();

      task.call();

      assertEquals(1, requests.getAllValues().size());
      assertTrue(requests.getValue().getRequestFullContainerReportLease());
      assertFalse(requests.getValue().hasContainerReport());
      assertEquals(Collections.singletonList(icr), requests.getValue().getIncrementalContainerReportList());
      assertTrue(context.isFullContainerReportReady(TEST_SCM_ENDPOINT));
      grantLease.set(true);

      task.call();

      assertEquals(3, requests.getAllValues().size());
      assertEquals(expected.getReportsList(), requests.getValue().getContainerReport().getReportsList());
      assertEquals(99, requests.getValue().getContainerReport().getFullContainerReportLease().getId());
      assertEquals(0, requests.getValue().getIncrementalContainerReportCount());
      assertFalse(context.isFullContainerReportReady(TEST_SCM_ENDPOINT));
      assertEquals(Collections.singletonList(icr),
          context.getAllAvailableReports(otherAddress, StateContext.CONTAINER_REPORTS_PROTO_NAME));
      assertTrue(context.isFullContainerReportReady(otherAddress));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testPrefinalizedFCRRetriesAfterHeartbeatFailure(boolean finalizeBeforeRetry) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanode = mockDatanodeStateMachine();
    ContainerReportsProto report = ContainerReportsProto.newBuilder()
        .addReports(ContainerReplicaProto.newBuilder().setContainerID(1).setState(ContainerReplicaProto.State.CLOSED)
            .setUsed(100)).build();
    stubFullContainerReport(datanode, report);
    when(datanode.getQueuedCommandCount()).thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));
    DatanodeVersionManager versionManager = datanode.getVersionManager();
    when(versionManager.isAllowed(HDDSVersion.FULL_CONTAINER_REPORT_LEASE)).thenReturn(false);
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING, datanode, "");
    context.addEndpoint(TEST_SCM_ENDPOINT);
    context.refreshFullReport(report);
    context.addIncrementalReport(IncrementalContainerReportProto.newBuilder()
        .addReport(report.getReports(0)).build());
    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> requests = ArgumentCaptor.forClass(SCMHeartbeatRequestProto.class);
    AtomicBoolean failHeartbeat = new AtomicBoolean(true);
    when(scm.sendHeartbeat(requests.capture())).thenAnswer(invocation -> {
      if (failHeartbeat.getAndSet(false)) {
        throw new IOException("heartbeat failed");
      }
      SCMHeartbeatRequestProto request = invocation.getArgument(0);
      SCMHeartbeatResponseProto.Builder response = SCMHeartbeatResponseProto.newBuilder()
          .setDatanodeUUID(request.getDatanodeDetails().getUuid());
      if (request.getRequestFullContainerReportLease()) {
        response.setFullContainerReportLease(FullContainerReportLeaseProto.newBuilder().setId(99));
      }
      return response.build();
    });
    try (EndpointStateMachine endpoint = new EndpointStateMachine(TEST_SCM_ENDPOINT, scm, conf, "")) {
      endpoint.setState(EndpointStateMachine.EndPointStates.HEARTBEAT);
      HeartbeatEndpointTask task = HeartbeatEndpointTask.newBuilder().setConfig(conf).setContext(context)
          .setEndpointStateMachine(endpoint).build();

      task.call();

      assertEquals(report, requests.getValue().getContainerReport());
      assertTrue(context.isFullContainerReportReady(TEST_SCM_ENDPOINT));
      assertEquals(0, requests.getValue().getIncrementalContainerReportCount());
      when(versionManager.isAllowed(HDDSVersion.FULL_CONTAINER_REPORT_LEASE)).thenReturn(finalizeBeforeRetry);
      ContainerReportsProto freshReport = ContainerReportsProto.newBuilder()
          .addReports(report.getReports(0).toBuilder().setUsed(200)).build();
      when(datanode.getContainer().getContainerSet().getContainerReport()).thenReturn(freshReport);

      task.call();

      assertEquals(finalizeBeforeRetry ? 3 : 2, requests.getAllValues().size());
      assertEquals(freshReport.getReportsList(), requests.getValue().getContainerReport().getReportsList());
      assertEquals(finalizeBeforeRetry, requests.getValue().getContainerReport().hasFullContainerReportLease());
      assertFalse(context.isFullContainerReportReady(TEST_SCM_ENDPOINT));
      assertEquals(0, endpoint.getMissedCount());
    }
  }

  @Test
  void testFinalizationDoesNotSendOlderICRsAfterFCR() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanode = mockDatanodeStateMachine();
    ContainerReplicaProto oldReplica = ContainerReplicaProto.newBuilder().setContainerID(1)
        .setState(ContainerReplicaProto.State.CLOSED).setUsed(100).setKeyCount(1).build();
    ContainerReportsProto report = ContainerReportsProto.newBuilder()
        .addReports(oldReplica.toBuilder().setUsed(200).setKeyCount(2)).build();
    stubFullContainerReport(datanode, report);
    when(datanode.getQueuedCommandCount()).thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));
    DatanodeVersionManager versionManager = datanode.getVersionManager();
    when(versionManager.isAllowed(HDDSVersion.FULL_CONTAINER_REPORT_LEASE)).thenReturn(false);
    StateContext context = spy(new StateContext(conf, DatanodeStates.RUNNING, datanode, ""));
    context.addEndpoint(TEST_SCM_ENDPOINT);
    context.refreshFullReport(ContainerReportsProto.getDefaultInstance());
    context.addIncrementalReport(IncrementalContainerReportProto.newBuilder().addReport(oldReplica).build());
    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> requests = ArgumentCaptor.forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(requests.capture())).thenAnswer(invocation ->
        SCMHeartbeatResponseProto.newBuilder()
            .setDatanodeUUID(datanode.getDatanodeDetails().getUuidString()).build());
    try (EndpointStateMachine endpoint = new EndpointStateMachine(TEST_SCM_ENDPOINT, scm, conf, "")) {
      endpoint.setState(EndpointStateMachine.EndPointStates.HEARTBEAT);
      SCMConnectionManager connections = mock(SCMConnectionManager.class);
      when(datanode.getConnectionManager()).thenReturn(connections);
      when(connections.getValues()).thenReturn(Collections.singletonList(endpoint));
      AtomicBoolean finalized = new AtomicBoolean();
      doAnswer(invocation -> {
        boolean enabled = (boolean) invocation.callRealMethod();
        if (finalized.compareAndSet(false, true)) {
          when(versionManager.isAllowed(HDDSVersion.FULL_CONTAINER_REPORT_LEASE)).thenReturn(true);
          context.refreshFullReport(context.getFullContainerReport());
        }
        return enabled;
      }).when(context).isFullContainerReportLeaseEnabled(endpoint);
      HeartbeatEndpointTask task = HeartbeatEndpointTask.newBuilder().setConfig(conf).setContext(context)
          .setEndpointStateMachine(endpoint).build();

      task.call();

      assertEquals(report, requests.getValue().getContainerReport());
      assertEquals(0, requests.getValue().getIncrementalContainerReportCount());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testFCRLeaseStartsAfterFinalization(boolean recon, @TempDir Path tempDir) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempDir.toString());
    DatanodeStateMachine datanode = mockDatanodeStateMachine();
    ContainerReportsProto report = ContainerReportsProto.newBuilder()
        .addReports(ContainerReplicaProto.newBuilder().setContainerID(1).setState(ContainerReplicaProto.State.CLOSED))
        .build();
    stubFullContainerReport(datanode, report);
    when(datanode.getQueuedCommandCount()).thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));
    DatanodeStorage storage = new DatanodeStorage(conf, datanode.getDatanodeDetails().getUuidString(),
        HDDSVersion.STREAM_PUTBLOCK_WITHOUT_RAFT.serialize());
    storage.initialize();
    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> requests = ArgumentCaptor.forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(requests.capture())).thenAnswer(invocation -> {
      SCMHeartbeatRequestProto request = invocation.getArgument(0);
      SCMHeartbeatResponseProto.Builder response = SCMHeartbeatResponseProto.newBuilder()
          .setDatanodeUUID(request.getDatanodeDetails().getUuid());
      if (request.getRequestFullContainerReportLease()) {
        response.setFullContainerReportLease(FullContainerReportLeaseProto.newBuilder().setId(99));
      }
      return response.build();
    });
    try (DatanodeVersionManager versionManager = new DatanodeVersionManager(storage, datanode, Collections::emptyMap);
         EndpointStateMachine endpoint = new EndpointStateMachine(TEST_SCM_ENDPOINT, scm, conf, "")) {
      when(datanode.getVersionManager()).thenReturn(versionManager);
      StateContext context = new StateContext(conf, DatanodeStates.RUNNING, datanode, "");
      context.addEndpoint(TEST_SCM_ENDPOINT);
      endpoint.setState(EndpointStateMachine.EndPointStates.HEARTBEAT);
      endpoint.setPassive(recon);
      endpoint.setVersion(VersionResponse.newBuilder().setVersion(1).build());
      HeartbeatEndpointTask task = HeartbeatEndpointTask.newBuilder().setConfig(conf).setContext(context)
          .setEndpointStateMachine(endpoint).build();

      context.refreshFullReport(report);
      task.call();

      assertEquals(1, requests.getAllValues().size());
      assertEquals(report, requests.getValue().getContainerReport());
      assertFalse(requests.getValue().getRequestFullContainerReportLease());

      versionManager.finalizeUpgrade();
      task.call();

      assertEquals(2, requests.getAllValues().size());
      assertFalse(requests.getValue().hasContainerReport());
      assertFalse(requests.getValue().getRequestFullContainerReportLease());
      assertEquals(EndpointStateMachine.EndPointStates.HEARTBEAT, endpoint.getState());

      context.refreshFullReport(report);
      task.call();

      assertEquals(recon ? 3 : 4, requests.getAllValues().size());
      assertEquals(!recon, requests.getValue().getContainerReport().hasFullContainerReportLease());
      assertEquals(report.getReportsList(), requests.getValue().getContainerReport().getReportsList());
      verify(scm, never()).getVersion(any());
      verify(scm, never()).register(any(), any(), any(), any(), any());
    }
  }

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
    when(datanodeStateMachine.getVersionManager().isAllowed(HDDSVersion.FULL_CONTAINER_REPORT_LEASE)).thenReturn(false);
    stubFullContainerReport(datanodeStateMachine, ContainerReportsProto.getDefaultInstance());
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
    assertFalse(heartbeat.getRequestFullContainerReportLease());
    assertEquals(0, heartbeat.getCommandStatusReportsCount());
    assertFalse(heartbeat.hasContainerActions());
  }

  @Test
  public void leasedFCRRequestsLeaseBeforeSendingReport() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mockDatanodeStateMachine();
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
        .thenAnswer(invocation -> {
          SCMHeartbeatRequestProto request = invocation.getArgument(0);
          SCMHeartbeatResponseProto.Builder response = SCMHeartbeatResponseProto.newBuilder()
              .setDatanodeUUID(request.getDatanodeDetails().getUuid()).setTerm(7L);
          if (request.getRequestFullContainerReportLease()) {
            response.setFullContainerReportLease(
                FullContainerReportLeaseProto.newBuilder().setId(99L));
          }
          return response.build();
        });

    EndpointStateMachine endpointStateMachine = new EndpointStateMachine(
        TEST_SCM_ENDPOINT, scm, conf, "");
    endpointStateMachine.setState(EndpointStateMachine.EndPointStates.HEARTBEAT);
    HeartbeatEndpointTask endpointTask = HeartbeatEndpointTask.newBuilder()
        .setConfig(conf)
        .setContext(context)
        .setEndpointStateMachine(endpointStateMachine)
        .build();

    context.addEndpoint(TEST_SCM_ENDPOINT);
    context.refreshFullReport(ContainerReportsProto.getDefaultInstance());

    endpointTask.call();
    assertEquals(2, argument.getAllValues().size());
    SCMHeartbeatRequestProto firstHeartbeat = argument.getAllValues().get(0);
    assertFalse(firstHeartbeat.hasContainerReport());
    assertTrue(firstHeartbeat.getRequestFullContainerReportLease());

    SCMHeartbeatRequestProto secondHeartbeat = argument.getAllValues().get(1);
    assertTrue(secondHeartbeat.hasContainerReport());
    assertEquals(99L, secondHeartbeat.getContainerReport()
        .getFullContainerReportLease().getId());
    assertFalse(secondHeartbeat.getRequestFullContainerReportLease());
    assertFalse(context.isFullContainerReportReady(TEST_SCM_ENDPOINT));
    assertFalse(endpointStateMachine.hasFullContainerReportLease());

    endpointTask.call();
    assertEquals(3, argument.getAllValues().size());
    assertFalse(argument.getValue().hasContainerReport());
    assertFalse(argument.getValue().getRequestFullContainerReportLease());
    verify(datanodeStateMachine, never()).triggerHeartbeat();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void leasedFCRSurvivesHigherHeartbeatTermUnlessReregistered(boolean reregister) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanode = mockDatanodeStateMachine();
    stubFullContainerReport(datanode, ContainerReportsProto.getDefaultInstance());
    when(datanode.getQueuedCommandCount()).thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING, datanode, "");
    context.addEndpoint(TEST_SCM_ENDPOINT);
    context.getFullContainerReportDiscardPendingICR(TEST_SCM_ENDPOINT);
    context.setTermOfLeaderSCM(7);
    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> requests = ArgumentCaptor.forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(requests.capture())).thenAnswer(invocation -> {
      SCMHeartbeatRequestProto request = invocation.getArgument(0);
      SCMHeartbeatResponseProto.Builder response = SCMHeartbeatResponseProto.newBuilder()
          .setDatanodeUUID(request.getDatanodeDetails().getUuid()).setTerm(8);
      if (reregister) {
        response.addCommands(SCMCommandProto.newBuilder().setCommandType(SCMCommandProto.Type.reregisterCommand));
      }
      return response.build();
    });
    try (EndpointStateMachine endpoint = new EndpointStateMachine(TEST_SCM_ENDPOINT, scm, conf, "")) {
      endpoint.setState(EndpointStateMachine.EndPointStates.HEARTBEAT);
      endpoint.setFullContainerReportLease(FullContainerReportLeaseProto.newBuilder().setId(99).build());
      HeartbeatEndpointTask task = HeartbeatEndpointTask.newBuilder().setConfig(conf).setContext(context)
          .setEndpointStateMachine(endpoint).build();

      task.call();

      assertEquals(1, requests.getAllValues().size());
      assertFalse(requests.getValue().hasContainerReport());
      assertFalse(requests.getValue().getRequestFullContainerReportLease());
      assertEquals(OptionalLong.of(8), context.getTermOfLeaderSCM());
      assertEquals(!reregister, endpoint.hasFullContainerReportLease());
      if (reregister) {
        assertEquals(EndpointStateMachine.EndPointStates.GETVERSION, endpoint.getState());
      } else {
        context.refreshFullReport(ContainerReportsProto.getDefaultInstance());

        task.call();

        assertEquals(2, requests.getAllValues().size());
        assertTrue(requests.getValue().hasContainerReport());
        assertEquals(99, requests.getValue().getContainerReport().getFullContainerReportLease().getId());
        assertFalse(requests.getValue().getRequestFullContainerReportLease());
        assertFalse(endpoint.hasFullContainerReportLease());
        assertFalse(context.isFullContainerReportReady(TEST_SCM_ENDPOINT));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false, 0, false", "true, 0, false", "true, 99, true"})
  public void leasedFCRDoesNotTriggerHeartbeatWithoutUsableGrant(boolean grantPresent, long leaseId,
      boolean reregister) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanode = mockDatanodeStateMachine();
    stubFullContainerReport(datanode, ContainerReportsProto.getDefaultInstance());
    when(datanode.getQueuedCommandCount()).thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING, datanode, "");
    context.addEndpoint(TEST_SCM_ENDPOINT);
    context.refreshFullReport(ContainerReportsProto.getDefaultInstance());
    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor.forClass(SCMHeartbeatRequestProto.class);
    when(scm.sendHeartbeat(argument.capture())).thenAnswer(invocation -> {
      SCMHeartbeatResponseProto.Builder response = SCMHeartbeatResponseProto.newBuilder()
          .setDatanodeUUID(((SCMHeartbeatRequestProto) invocation.getArgument(0)).getDatanodeDetails().getUuid());
      if (grantPresent) {
        response.setFullContainerReportLease(
            FullContainerReportLeaseProto.newBuilder().setId(leaseId));
      }
      if (reregister) {
        response.addCommands(SCMCommandProto.newBuilder().setCommandType(SCMCommandProto.Type.reregisterCommand));
      }
      return response.build();
    });
    try (EndpointStateMachine endpoint = new EndpointStateMachine(TEST_SCM_ENDPOINT, scm, conf, "")) {
      endpoint.setState(EndpointStateMachine.EndPointStates.HEARTBEAT);
      HeartbeatEndpointTask task = HeartbeatEndpointTask.newBuilder().setConfig(conf).setContext(context)
          .setEndpointStateMachine(endpoint).build();

      task.call();

      assertTrue(argument.getValue().getRequestFullContainerReportLease());
      assertFalse(argument.getValue().hasContainerReport());
      assertFalse(endpoint.hasFullContainerReportLease());
      assertTrue(context.isFullContainerReportReady(TEST_SCM_ENDPOINT));
      if (reregister) {
        assertEquals(EndpointStateMachine.EndPointStates.GETVERSION, endpoint.getState());
      } else {
        task.call();
        assertTrue(argument.getValue().getRequestFullContainerReportLease());
        assertFalse(argument.getValue().hasContainerReport());
      }
      verify(datanode, never()).triggerHeartbeat();
    }
  }

  @Test
  public void leasedFCRUsesFreshContainerSnapshot() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mockDatanodeStateMachine();
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

    EndpointStateMachine endpointStateMachine = new EndpointStateMachine(
        TEST_SCM_ENDPOINT, scm, conf, "");
    endpointStateMachine.setFullContainerReportLease(
        FullContainerReportLeaseProto.newBuilder()
            .setId(99L)
            .build());
    HeartbeatEndpointTask endpointTask = HeartbeatEndpointTask.newBuilder()
        .setConfig(conf)
        .setContext(context)
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
    DatanodeStateMachine datanodeStateMachine = mockDatanodeStateMachine();
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
      endpoint.setFullContainerReportLease(FullContainerReportLeaseProto.newBuilder().setId(99).build());
      HeartbeatEndpointTask task = HeartbeatEndpointTask.newBuilder().setConfig(conf).setContext(context)
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
    DatanodeStateMachine datanodeStateMachine = mockDatanodeStateMachine();
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

    EndpointStateMachine endpointStateMachine = new EndpointStateMachine(
        TEST_SCM_ENDPOINT, scm, conf, "");
    endpointStateMachine.setFullContainerReportLease(
        FullContainerReportLeaseProto.newBuilder()
            .setId(99L)
            .build());
    HeartbeatEndpointTask endpointTask = HeartbeatEndpointTask.newBuilder()
        .setConfig(conf)
        .setContext(context)
        .setEndpointStateMachine(endpointStateMachine)
        .build();
    context.addEndpoint(TEST_SCM_ENDPOINT);

    endpointTask.call();

    assertFalse(argument.getValue().hasContainerReport());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void leasedFCRIsRetriedAfterFailure(boolean communicationFailure) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine = mockDatanodeStateMachine();
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
        .thenAnswer(invocation -> {
          if (communicationFailure) {
            throw new IOException("heartbeat failed");
          }
          return SCMHeartbeatResponseProto.newBuilder()
              .setDatanodeUUID(((SCMHeartbeatRequestProto) invocation.getArgument(0)).getDatanodeDetails().getUuid())
              .setFullContainerReportLeaseRejected(true)
              .build();
        })
        .thenAnswer(invocation -> {
          SCMHeartbeatRequestProto request = invocation.getArgument(0);
          SCMHeartbeatResponseProto.Builder response = SCMHeartbeatResponseProto.newBuilder()
              .setDatanodeUUID(request.getDatanodeDetails().getUuid());
          if (request.getRequestFullContainerReportLease()) {
            response.setFullContainerReportLease(FullContainerReportLeaseProto.newBuilder().setId(100L));
          }
          return response.build();
        });

    EndpointStateMachine endpointStateMachine = new EndpointStateMachine(
        TEST_SCM_ENDPOINT, scm, conf, "");
    endpointStateMachine.setFullContainerReportLease(
        FullContainerReportLeaseProto.newBuilder()
            .setId(99L)
            .build());
    HeartbeatEndpointTask endpointTask = HeartbeatEndpointTask.newBuilder()
        .setConfig(conf)
        .setContext(context)
        .setEndpointStateMachine(endpointStateMachine)
        .build();
    context.addEndpoint(TEST_SCM_ENDPOINT);
    context.refreshFullReport(ContainerReportsProto.getDefaultInstance());

    endpointTask.call();
    assertEquals(1, argument.getAllValues().size());
    assertEquals(99L, argument.getValue().getContainerReport().getFullContainerReportLease().getId());
    assertTrue(context.isFullContainerReportReady(TEST_SCM_ENDPOINT));
    assertFalse(endpointStateMachine.hasFullContainerReportLease());
    assertEquals(communicationFailure ? 1 : 0, endpointStateMachine.getMissedCount());

    endpointTask.call();
    assertEquals(3, argument.getAllValues().size());
    SCMHeartbeatRequestProto leaseRequest = argument.getAllValues().get(1);
    assertFalse(leaseRequest.hasContainerReport());
    assertTrue(leaseRequest.getRequestFullContainerReportLease());
    assertEquals(100L, argument.getValue().getContainerReport().getFullContainerReportLease().getId());
    assertFalse(argument.getValue().getRequestFullContainerReportLease());
    assertFalse(context.isFullContainerReportReady(TEST_SCM_ENDPOINT));
    assertFalse(endpointStateMachine.hasFullContainerReportLease());
    assertEquals(0, endpointStateMachine.getMissedCount());
    verify(datanodeStateMachine, never()).triggerHeartbeat();
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
    when(datanodeStateMachine.getVersionManager().isAllowed(HDDSVersion.FULL_CONTAINER_REPORT_LEASE)).thenReturn(false);
    stubFullContainerReport(datanodeStateMachine, ContainerReportsProto.getDefaultInstance());
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
    assertFalse(heartbeat.getRequestFullContainerReportLease());
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
    when(versionManager.isAllowed(HDDSVersion.FULL_CONTAINER_REPORT_LEASE)).thenReturn(true);
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
