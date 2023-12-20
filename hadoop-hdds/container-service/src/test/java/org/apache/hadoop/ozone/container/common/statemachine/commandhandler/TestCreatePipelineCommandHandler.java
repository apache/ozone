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

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CreatePipelineCommandProto;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CreatePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.GroupManagementApi;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test cases to verify CreatePipelineCommandHandler.
 */
@ExtendWith(MockitoExtension.class)
public class TestCreatePipelineCommandHandler {

  private OzoneContainer ozoneContainer;
  private StateContext stateContext;
  private SCMConnectionManager connectionManager;
  private RaftClient raftClient;
  private GroupManagementApi raftClientGroupManager;
  private OzoneConfiguration conf;

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    ozoneContainer = Mockito.mock(OzoneContainer.class);
    connectionManager = Mockito.mock(SCMConnectionManager.class);
    raftClient = Mockito.mock(RaftClient.class);
    raftClientGroupManager = Mockito.mock(GroupManagementApi.class);
    Mockito.lenient().when(raftClient.getGroupManagementApi(
        Mockito.any(RaftPeerId.class))).thenReturn(raftClientGroupManager);
  }

  @Test
  public void testPipelineCreation() throws IOException {

    final List<DatanodeDetails> datanodes = getDatanodes();
    final PipelineID pipelineID = PipelineID.randomId();
    final SCMCommand<CreatePipelineCommandProto> command =
        new CreatePipelineCommand(pipelineID, HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, datanodes);
    stateContext = ContainerTestUtils.getMockContext(datanodes.get(0), conf);

    final XceiverServerSpi writeChanel = Mockito.mock(XceiverServerSpi.class);
    Mockito.when(ozoneContainer.getWriteChannel()).thenReturn(writeChanel);
    Mockito.when(writeChanel.isExist(pipelineID.getProtobuf()))
        .thenReturn(false);

    final CreatePipelineCommandHandler commandHandler =
        new CreatePipelineCommandHandler((leader, tls) -> raftClient,
            MoreExecutors.directExecutor());
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
    stateContext = ContainerTestUtils.getMockContext(datanodes.get(0), conf);
    Mockito.when(ozoneContainer.getWriteChannel()).thenReturn(writeChanel);
    Mockito.when(writeChanel.isExist(pipelineID.getProtobuf()))
        .thenReturn(true);

    final CreatePipelineCommandHandler commandHandler =
        new CreatePipelineCommandHandler(conf, MoreExecutors.directExecutor());
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
