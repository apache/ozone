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

import java.net.InetSocketAddress;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine.DatanodeStates;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * This class tests the functionality of HeartbeatEndpointTask.
 */
public class TestHeartbeatEndpointTask {

  private static final InetSocketAddress TEST_SCM_ENDPOINT =
      new InetSocketAddress("test-scm-1", 9861);

  @Test
  public void testheartbeatWithoutReports() throws Exception {
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

    HeartbeatEndpointTask endpointTask = getHeartbeatEndpointTask(scm);
    endpointTask.call();
    SCMHeartbeatRequestProto heartbeat = argument.getValue();
    Assert.assertTrue(heartbeat.hasDatanodeDetails());
    Assert.assertFalse(heartbeat.hasNodeReport());
    Assert.assertFalse(heartbeat.hasContainerReport());
    Assert.assertTrue(heartbeat.getCommandStatusReportsCount() == 0);
    Assert.assertFalse(heartbeat.hasContainerActions());
  }

  @Test
  public void testheartbeatWithNodeReports() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        Mockito.mock(DatanodeStateMachine.class));

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
    context.addReport(NodeReportProto.getDefaultInstance());
    endpointTask.call();
    SCMHeartbeatRequestProto heartbeat = argument.getValue();
    Assert.assertTrue(heartbeat.hasDatanodeDetails());
    Assert.assertTrue(heartbeat.hasNodeReport());
    Assert.assertFalse(heartbeat.hasContainerReport());
    Assert.assertTrue(heartbeat.getCommandStatusReportsCount() == 0);
    Assert.assertFalse(heartbeat.hasContainerActions());
  }

  @Test
  public void testheartbeatWithContainerReports() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        Mockito.mock(DatanodeStateMachine.class));

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
    context.addReport(ContainerReportsProto.getDefaultInstance());
    endpointTask.call();
    SCMHeartbeatRequestProto heartbeat = argument.getValue();
    Assert.assertTrue(heartbeat.hasDatanodeDetails());
    Assert.assertFalse(heartbeat.hasNodeReport());
    Assert.assertTrue(heartbeat.hasContainerReport());
    Assert.assertTrue(heartbeat.getCommandStatusReportsCount() == 0);
    Assert.assertFalse(heartbeat.hasContainerActions());
  }

  @Test
  public void testheartbeatWithCommandStatusReports() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        Mockito.mock(DatanodeStateMachine.class));

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
    context.addReport(CommandStatusReportsProto.getDefaultInstance());
    endpointTask.call();
    SCMHeartbeatRequestProto heartbeat = argument.getValue();
    Assert.assertTrue(heartbeat.hasDatanodeDetails());
    Assert.assertFalse(heartbeat.hasNodeReport());
    Assert.assertFalse(heartbeat.hasContainerReport());
    Assert.assertTrue(heartbeat.getCommandStatusReportsCount() != 0);
    Assert.assertFalse(heartbeat.hasContainerActions());
  }

  @Test
  public void testheartbeatWithContainerActions() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        Mockito.mock(DatanodeStateMachine.class));

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
    Assert.assertTrue(heartbeat.hasDatanodeDetails());
    Assert.assertFalse(heartbeat.hasNodeReport());
    Assert.assertFalse(heartbeat.hasContainerReport());
    Assert.assertTrue(heartbeat.getCommandStatusReportsCount() == 0);
    Assert.assertTrue(heartbeat.hasContainerActions());
  }

  @Test
  public void testheartbeatWithAllReports() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        Mockito.mock(DatanodeStateMachine.class));

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
    context.addReport(NodeReportProto.getDefaultInstance());
    context.addReport(ContainerReportsProto.getDefaultInstance());
    context.addReport(CommandStatusReportsProto.getDefaultInstance());
    context.addContainerAction(getContainerAction());
    endpointTask.call();
    SCMHeartbeatRequestProto heartbeat = argument.getValue();
    Assert.assertTrue(heartbeat.hasDatanodeDetails());
    Assert.assertTrue(heartbeat.hasNodeReport());
    Assert.assertTrue(heartbeat.hasContainerReport());
    Assert.assertTrue(heartbeat.getCommandStatusReportsCount() != 0);
    Assert.assertTrue(heartbeat.hasContainerActions());
  }

  /**
   * Creates HeartbeatEndpointTask for the given StorageContainerManager proxy.
   *
   * @param proxy StorageContainerDatanodeProtocolClientSideTranslatorPB
   *
   * @return HeartbeatEndpointTask
   */
  private HeartbeatEndpointTask getHeartbeatEndpointTask(
      StorageContainerDatanodeProtocolClientSideTranslatorPB proxy) {
    OzoneConfiguration conf = new OzoneConfiguration();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING,
        Mockito.mock(DatanodeStateMachine.class));
    return getHeartbeatEndpointTask(conf, context, proxy);

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
    return HeartbeatEndpointTask.newBuilder()
        .setConfig(conf)
        .setDatanodeDetails(datanodeDetails)
        .setContext(context)
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