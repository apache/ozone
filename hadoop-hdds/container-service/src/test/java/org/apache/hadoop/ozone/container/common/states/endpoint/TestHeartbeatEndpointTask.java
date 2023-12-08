/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.states.endpoint;

import static java.util.Collections.emptyList;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.reconstructECContainersCommand;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.mockito.ArgumentMatchers.any;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;

import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandQueueReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine.DatanodeStates;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * This class tests the functionality of HeartbeatEndpointTask.
 */
public class TestHeartbeatEndpointTask {

  private static final InetSocketAddress TEST_SCM_ENDPOINT =
      new InetSocketAddress("test-scm-1", 9861);

  @Test
  public void handlesReconstructContainerCommand() throws Exception {
    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        Mockito.mock(
            StorageContainerDatanodeProtocolClientSideTranslatorPB.class);

    List<DatanodeDetails> targetDns = new ArrayList<>();
    targetDns.add(MockDatanodeDetails.randomDatanodeDetails());
    targetDns.add(MockDatanodeDetails.randomDatanodeDetails());
    ReconstructECContainersCommand cmd = new ReconstructECContainersCommand(
        1, emptyList(), targetDns, new byte[]{2, 5},
        new ECReplicationConfig(3, 2));

    Mockito.when(scm.sendHeartbeat(any()))
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
        Mockito.mock(DatanodeStateMachine.class);
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "");

    // WHEN
    HeartbeatEndpointTask task = getHeartbeatEndpointTask(conf, context, scm);
    task.call();

    // THEN
    Assertions.assertEquals(1, context.getCommandQueueSummary()
        .get(reconstructECContainersCommand).intValue());
  }

  @Test
  public void testheartbeatWithoutReports() throws Exception {
    final long termInSCM = 42;
    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        Mockito.mock(
            StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    Mockito.when(scm.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation ->
            SCMHeartbeatResponseProto.newBuilder()
                .setDatanodeUUID(
                    ((SCMHeartbeatRequestProto)invocation.getArgument(0))
                        .getDatanodeDetails().getUuid())
                .setTerm(termInSCM)
                .build());

    OzoneConfiguration conf = new OzoneConfiguration();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        Mockito.mock(DatanodeStateMachine.class), "");
    context.setTermOfLeaderSCM(1);
    HeartbeatEndpointTask endpointTask = getHeartbeatEndpointTask(
        conf, context, scm);
    endpointTask.call();
    SCMHeartbeatRequestProto heartbeat = argument.getValue();
    Assertions.assertTrue(heartbeat.hasDatanodeDetails());
    Assertions.assertFalse(heartbeat.hasNodeReport());
    Assertions.assertFalse(heartbeat.hasContainerReport());
    Assertions.assertTrue(heartbeat.getCommandStatusReportsCount() == 0);
    Assertions.assertFalse(heartbeat.hasContainerActions());
    OptionalLong termInDatanode = context.getTermOfLeaderSCM();
    Assertions.assertTrue(termInDatanode.isPresent());
    Assertions.assertEquals(termInSCM, termInDatanode.getAsLong());
  }

  @Test
  public void testheartbeatWithNodeReports() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        Mockito.mock(DatanodeStateMachine.class), "");

    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        Mockito.mock(
            StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    Mockito.when(scm.sendHeartbeat(argument.capture()))
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
    Assertions.assertTrue(heartbeat.hasDatanodeDetails());
    Assertions.assertTrue(heartbeat.hasNodeReport());
    Assertions.assertFalse(heartbeat.hasContainerReport());
    Assertions.assertTrue(heartbeat.getCommandStatusReportsCount() == 0);
    Assertions.assertFalse(heartbeat.hasContainerActions());
  }

  @Test
  public void testheartbeatWithContainerReports() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        Mockito.mock(DatanodeStateMachine.class), "");

    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        Mockito.mock(
            StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    Mockito.when(scm.sendHeartbeat(argument.capture()))
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
    Assertions.assertTrue(heartbeat.hasDatanodeDetails());
    Assertions.assertFalse(heartbeat.hasNodeReport());
    Assertions.assertTrue(heartbeat.hasContainerReport());
    Assertions.assertTrue(heartbeat.getCommandStatusReportsCount() == 0);
    Assertions.assertFalse(heartbeat.hasContainerActions());
  }

  @Test
  public void testheartbeatWithCommandStatusReports() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        Mockito.mock(DatanodeStateMachine.class), "");

    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        Mockito.mock(
            StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    Mockito.when(scm.sendHeartbeat(argument.capture()))
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
    Assertions.assertTrue(heartbeat.hasDatanodeDetails());
    Assertions.assertFalse(heartbeat.hasNodeReport());
    Assertions.assertFalse(heartbeat.hasContainerReport());
    Assertions.assertTrue(heartbeat.getCommandStatusReportsCount() != 0);
    Assertions.assertFalse(heartbeat.hasContainerActions());
  }

  @Test
  public void testheartbeatWithContainerActions() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        Mockito.mock(DatanodeStateMachine.class), "");

    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        Mockito.mock(
            StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    Mockito.when(scm.sendHeartbeat(argument.capture()))
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
    Assertions.assertTrue(heartbeat.hasDatanodeDetails());
    Assertions.assertFalse(heartbeat.hasNodeReport());
    Assertions.assertFalse(heartbeat.hasContainerReport());
    Assertions.assertTrue(heartbeat.getCommandStatusReportsCount() == 0);
    Assertions.assertTrue(heartbeat.hasContainerActions());
  }

  @Test
  public void testheartbeatWithAllReports() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachine =
        Mockito.mock(DatanodeStateMachine.class);
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        datanodeStateMachine, "");

    // Return a Map of command counts when the heartbeat logic requests it
    final Map<SCMCommandProto.Type, Integer> commands = new HashMap<>();
    int count = 1;
    for (SCMCommandProto.Type cmd : SCMCommandProto.Type.values()) {
      commands.put(cmd, count++);
    }
    Mockito.when(datanodeStateMachine.getQueuedCommandCount())
        .thenReturn(commands);

    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        Mockito.mock(
            StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMHeartbeatRequestProto> argument = ArgumentCaptor
        .forClass(SCMHeartbeatRequestProto.class);
    Mockito.when(scm.sendHeartbeat(argument.capture()))
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
    Assertions.assertTrue(heartbeat.hasDatanodeDetails());
    Assertions.assertTrue(heartbeat.hasNodeReport());
    Assertions.assertTrue(heartbeat.hasContainerReport());
    Assertions.assertTrue(heartbeat.getCommandStatusReportsCount() != 0);
    Assertions.assertTrue(heartbeat.hasContainerActions());
    Assertions.assertTrue(heartbeat.hasCommandQueueReport());
    CommandQueueReportProto queueCount = heartbeat.getCommandQueueReport();
    Assertions.assertEquals(queueCount.getCommandCount(), commands.size());
    Assertions.assertEquals(queueCount.getCountCount(), commands.size());
    for (int i = 0; i < commands.size(); i++) {
      Assertions.assertEquals(commands.get(queueCount.getCommand(i)).intValue(),
          queueCount.getCount(i));
    }
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
    DatanodeDetails datanodeDetails = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .build();
    EndpointStateMachine endpointStateMachine = Mockito
        .mock(EndpointStateMachine.class);
    Mockito.when(endpointStateMachine.getEndPoint()).thenReturn(proxy);
    Mockito.when(endpointStateMachine.getAddress())
        .thenReturn(TEST_SCM_ENDPOINT);
    HDDSLayoutVersionManager layoutVersionManager =
        Mockito.mock(HDDSLayoutVersionManager.class);
    Mockito.when(layoutVersionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());
    Mockito.when(layoutVersionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());
    return HeartbeatEndpointTask.newBuilder()
        .setConfig(conf)
        .setDatanodeDetails(datanodeDetails)
        .setContext(context)
        .setLayoutVersionManager(layoutVersionManager)
        .setEndpointStateMachine(endpointStateMachine)
        .build();
  }

  private ContainerAction getContainerAction() {
    ContainerAction.Builder builder = ContainerAction.newBuilder();
    builder.setContainerID(1L)
        .setAction(ContainerAction.Action.CLOSE)
        .setReason(ContainerAction.Reason.CONTAINER_FULL);
    return builder.build();
  }
}
