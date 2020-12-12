/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.CreatePipelineCommandProto;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine
    .SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server
    .XceiverServerSpi;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CreatePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.GroupManagementApi;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test cases to verify CreatePipelineCommandHandler.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(RaftClient.class)
public class TestCreatePipelineCommandHandler {

  private OzoneContainer ozoneContainer;
  private StateContext stateContext;
  private SCMConnectionManager connectionManager;
  private RaftClient raftClient;
  private GroupManagementApi raftClientGroupManager;

  @Before
  public void setup() throws Exception {
    ozoneContainer = Mockito.mock(OzoneContainer.class);
    stateContext = Mockito.mock(StateContext.class);
    connectionManager = Mockito.mock(SCMConnectionManager.class);
    raftClient = Mockito.mock(RaftClient.class);
    raftClientGroupManager = Mockito.mock(GroupManagementApi.class);
    final RaftClient.Builder builder = mockRaftClientBuilder();
    Mockito.when(builder.build()).thenReturn(raftClient);
    Mockito.when(raftClient.getGroupManagementApi(
        Mockito.any(RaftPeerId.class))).thenReturn(raftClientGroupManager);
    PowerMockito.mockStatic(RaftClient.class);
    PowerMockito.when(RaftClient.newBuilder()).thenReturn(builder);
  }

  private RaftClient.Builder mockRaftClientBuilder() {
    final RaftClient.Builder builder = Mockito.mock(RaftClient.Builder.class);
    Mockito.when(builder.setClientId(Mockito.any(ClientId.class)))
        .thenReturn(builder);
    Mockito.when(builder.setRaftGroup(Mockito.any(RaftGroup.class)))
        .thenReturn(builder);
    Mockito.when(builder.setLeaderId(Mockito.any(RaftPeerId.class)))
        .thenReturn(builder);
    Mockito.when(builder.setProperties(Mockito.any(RaftProperties.class)))
        .thenReturn(builder);
    Mockito.when(builder.setRetryPolicy(Mockito.any(RetryPolicy.class)))
        .thenReturn(builder);
    return builder;
  }

  @Test
  public void testPipelineCreation() throws IOException {

    final List<DatanodeDetails> datanodes = getDatanodes();
    final PipelineID pipelineID = PipelineID.randomId();
    final SCMCommand<CreatePipelineCommandProto> command =
        new CreatePipelineCommand(pipelineID, HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, datanodes);

    final XceiverServerSpi writeChanel = Mockito.mock(XceiverServerSpi.class);
    final DatanodeStateMachine dnsm = Mockito.mock(DatanodeStateMachine.class);

    Mockito.when(stateContext.getParent()).thenReturn(dnsm);
    Mockito.when(dnsm.getDatanodeDetails()).thenReturn(datanodes.get(0));
    Mockito.when(ozoneContainer.getWriteChannel()).thenReturn(writeChanel);
    Mockito.when(writeChanel.isExist(pipelineID.getProtobuf()))
        .thenReturn(false);

    final CreatePipelineCommandHandler commandHandler =
        new CreatePipelineCommandHandler(new OzoneConfiguration());
    commandHandler.handle(command, ozoneContainer, stateContext,
        connectionManager);

    List<Integer> priorityList =
        new ArrayList<>(Collections.nCopies(datanodes.size(), 0));

    Mockito.verify(writeChanel, Mockito.times(1))
        .addGroup(pipelineID.getProtobuf(), datanodes, priorityList);

    Mockito.verify(raftClientGroupManager, Mockito.times(2))
        .add(Mockito.any(RaftGroup.class));
  }

  @Test
  public void testCommandIdempotency() throws IOException {
    final List<DatanodeDetails> datanodes = getDatanodes();
    final PipelineID pipelineID = PipelineID.randomId();
    final SCMCommand<CreatePipelineCommandProto> command =
        new CreatePipelineCommand(pipelineID, HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, datanodes);

    final XceiverServerSpi writeChanel = Mockito.mock(XceiverServerSpi.class);
    final DatanodeStateMachine dnsm = Mockito.mock(DatanodeStateMachine.class);

    Mockito.when(stateContext.getParent()).thenReturn(dnsm);
    Mockito.when(dnsm.getDatanodeDetails()).thenReturn(datanodes.get(0));
    Mockito.when(ozoneContainer.getWriteChannel()).thenReturn(writeChanel);
    Mockito.when(writeChanel.isExist(pipelineID.getProtobuf()))
        .thenReturn(true);

    final CreatePipelineCommandHandler commandHandler =
        new CreatePipelineCommandHandler(new OzoneConfiguration());
    commandHandler.handle(command, ozoneContainer, stateContext,
        connectionManager);

    Mockito.verify(writeChanel, Mockito.times(0))
        .addGroup(pipelineID.getProtobuf(), datanodes);

    Mockito.verify(raftClientGroupManager, Mockito.times(0))
        .add(Mockito.any(RaftGroup.class));
  }

  private List<DatanodeDetails> getDatanodes() {
    final DatanodeDetails dnOne = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnTwo = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnThree = MockDatanodeDetails.randomDatanodeDetails();
    return Arrays.asList(dnOne, dnTwo, dnThree);
  }

}