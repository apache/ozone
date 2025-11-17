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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Test cases to verify CreatePipelineCommandHandler.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestCreatePipelineCommandHandler {

  private OzoneContainer ozoneContainer;
  private SCMConnectionManager connectionManager;
  private RaftClient raftClient;
  private GroupManagementApi raftClientGroupManager;
  private OzoneConfiguration conf;

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    ozoneContainer = mock(OzoneContainer.class);
    connectionManager = mock(SCMConnectionManager.class);
    raftClient = mock(RaftClient.class);
    raftClientGroupManager = mock(GroupManagementApi.class);
    when(raftClient.getGroupManagementApi(
        any(RaftPeerId.class))).thenReturn(raftClientGroupManager);
  }

  @Test
  public void testPipelineCreation() throws IOException {

    final List<DatanodeDetails> datanodes = getDatanodes();
    final PipelineID pipelineID = PipelineID.randomId();
    final SCMCommand<CreatePipelineCommandProto> command =
        new CreatePipelineCommand(pipelineID, HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, datanodes);
    StateContext stateContext = ContainerTestUtils.getMockContext(datanodes.get(0), conf);

    final XceiverServerSpi writeChanel = mock(XceiverServerSpi.class);
    when(ozoneContainer.getWriteChannel()).thenReturn(writeChanel);
    when(writeChanel.isExist(pipelineID.getProtobuf()))
        .thenReturn(false);

    final CreatePipelineCommandHandler commandHandler =
        new CreatePipelineCommandHandler((leader, tls) -> raftClient,
            MoreExecutors.directExecutor());
    commandHandler.handle(command, ozoneContainer, stateContext,
        connectionManager);

    List<Integer> priorityList =
        new ArrayList<>(Collections.nCopies(datanodes.size(), 0));

    verify(writeChanel, times(1))
        .addGroup(pipelineID.getProtobuf(), datanodes, priorityList);

    verify(raftClientGroupManager, times(2))
        .add(any(RaftGroup.class));
  }

  @Test
  public void testCommandIdempotency() throws IOException {
    final List<DatanodeDetails> datanodes = getDatanodes();
    final PipelineID pipelineID = PipelineID.randomId();
    final SCMCommand<CreatePipelineCommandProto> command =
        new CreatePipelineCommand(pipelineID, HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, datanodes);

    final XceiverServerSpi writeChanel = mock(XceiverServerSpi.class);
    StateContext stateContext = ContainerTestUtils.getMockContext(datanodes.get(0), conf);
    when(ozoneContainer.getWriteChannel()).thenReturn(writeChanel);
    when(writeChanel.isExist(pipelineID.getProtobuf()))
        .thenReturn(true);

    final CreatePipelineCommandHandler commandHandler =
        new CreatePipelineCommandHandler(conf, MoreExecutors.directExecutor());
    commandHandler.handle(command, ozoneContainer, stateContext,
        connectionManager);

    verify(writeChanel, times(0))
        .addGroup(pipelineID.getProtobuf(), datanodes);

    verify(raftClientGroupManager, times(0))
        .add(any(RaftGroup.class));
  }

  private List<DatanodeDetails> getDatanodes() {
    final DatanodeDetails dnOne = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnTwo = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnThree = MockDatanodeDetails.randomDatanodeDetails();
    return Arrays.asList(dnOne, dnTwo, dnThree);
  }

}
