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
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.ClosePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ClosePipelineCommandProto;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.GroupManagementApi;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test cases to verify ClosePipelineCommandHandler.
 */
@ExtendWith(MockitoExtension.class)
public class TestClosePipelineCommandHandler {

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
  void testPipelineClose() throws IOException {
    final List<DatanodeDetails> datanodes = getDatanodes();
    final DatanodeDetails currentDatanode = datanodes.get(0);
    final PipelineID pipelineID = PipelineID.randomId();
    final SCMCommand<ClosePipelineCommandProto> command =
        new ClosePipelineCommand(pipelineID);
    stateContext = ContainerTestUtils.getMockContext(currentDatanode, conf);

    final boolean shouldDeleteRatisLogDirectory = true;
    XceiverServerRatis writeChannel = Mockito.mock(XceiverServerRatis.class);
    Mockito.when(ozoneContainer.getWriteChannel()).thenReturn(writeChannel);
    Mockito.when(writeChannel.getShouldDeleteRatisLogDirectory()).thenReturn(shouldDeleteRatisLogDirectory);
    Mockito.when(writeChannel.isExist(pipelineID.getProtobuf())).thenReturn(true);
    Collection<RaftPeer> raftPeers = datanodes.stream()
        .map(RatisHelper::toRaftPeer)
        .collect(Collectors.toList());
    Mockito.when(writeChannel.getServer()).thenReturn(Mockito.mock(RaftServer.class));
    Mockito.when(writeChannel.getServer().getId()).thenReturn(RatisHelper.toRaftPeerId(currentDatanode));
    Mockito.when(writeChannel.getRaftPeersInPipeline(pipelineID)).thenReturn(raftPeers);

    final ClosePipelineCommandHandler commandHandler =
        new ClosePipelineCommandHandler((leader, tls) -> raftClient, MoreExecutors.directExecutor());
    commandHandler.handle(command, ozoneContainer, stateContext, connectionManager);

    Mockito.verify(writeChannel, Mockito.times(1))
        .removeGroup(pipelineID.getProtobuf());

    Mockito.verify(raftClientGroupManager, Mockito.times(2))
        .remove(Mockito.any(), Mockito.eq(shouldDeleteRatisLogDirectory), Mockito.eq(!shouldDeleteRatisLogDirectory));
  }

  @Test
  void testCommandIdempotency() throws IOException {
    final List<DatanodeDetails> datanodes = getDatanodes();
    final DatanodeDetails currentDatanode = datanodes.get(0);
    final PipelineID pipelineID = PipelineID.randomId();
    final SCMCommand<ClosePipelineCommandProto> command =
        new ClosePipelineCommand(pipelineID);
    stateContext = ContainerTestUtils.getMockContext(currentDatanode, conf);

    XceiverServerRatis writeChannel = Mockito.mock(XceiverServerRatis.class);
    Mockito.when(ozoneContainer.getWriteChannel()).thenReturn(writeChannel);
    // When the pipeline has been closed earlier by other datanode that received a close pipeline command
    Mockito.when(writeChannel.isExist(pipelineID.getProtobuf())).thenReturn(false);

    final ClosePipelineCommandHandler commandHandler =
        new ClosePipelineCommandHandler(conf, MoreExecutors.directExecutor());
    commandHandler.handle(command, ozoneContainer, stateContext, connectionManager);

    Mockito.verify(writeChannel, Mockito.times(0))
        .removeGroup(pipelineID.getProtobuf());

    Mockito.verify(raftClientGroupManager, Mockito.times(0))
        .remove(Mockito.any(), Mockito.anyBoolean(), Mockito.anyBoolean());
  }

  private List<DatanodeDetails> getDatanodes() {
    final DatanodeDetails dnOne = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnTwo = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnThree = MockDatanodeDetails.randomDatanodeDetails();
    return Arrays.asList(dnOne, dnTwo, dnThree);
  }

}
