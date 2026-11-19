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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ClosePipelineCommandProto;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.ClosePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test cases to verify ClosePipelineCommandHandler.
 */
public class TestClosePipelineCommandHandler {

  private OzoneContainer ozoneContainer;
  private SCMConnectionManager connectionManager;
  private GrpcTlsConfig tlsConfig;
  private OzoneConfiguration conf;

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    ozoneContainer = mock(OzoneContainer.class);
    connectionManager = mock(SCMConnectionManager.class);
    tlsConfig = mock(GrpcTlsConfig.class);
    when(ozoneContainer.getTlsClientConfig()).thenReturn(tlsConfig);
  }

  /** The peer group removal for RATIS and CHAINED is tested in TestXceiverServerRatisReplicationType. */
  @Test
  void testPipelineClose() throws IOException {
    final List<DatanodeDetails> datanodes = getDatanodes();
    final DatanodeDetails currentDatanode = datanodes.get(0);
    final PipelineID pipelineID = PipelineID.randomId();
    final SCMCommand<ClosePipelineCommandProto> command =
        new ClosePipelineCommand(pipelineID);
    StateContext stateContext = ContainerTestUtils.getMockContext(currentDatanode, conf);

    XceiverServerRatis writeChannel = mock(XceiverServerRatis.class);
    when(ozoneContainer.getWriteChannel()).thenReturn(writeChannel);
    when(writeChannel.isExist(pipelineID)).thenReturn(true);

    final ClosePipelineCommandHandler commandHandler =
        new ClosePipelineCommandHandler(MoreExecutors.directExecutor());
    commandHandler.handle(command, ozoneContainer, stateContext, connectionManager);

    verify(writeChannel, times(1))
        .removeGroup(pipelineID.getGroupId(), tlsConfig);
  }

  @Test
  void testCommandIdempotency() throws IOException {
    final List<DatanodeDetails> datanodes = getDatanodes();
    final DatanodeDetails currentDatanode = datanodes.get(0);
    final PipelineID pipelineID = PipelineID.randomId();
    final SCMCommand<ClosePipelineCommandProto> command =
        new ClosePipelineCommand(pipelineID);
    StateContext stateContext = ContainerTestUtils.getMockContext(currentDatanode, conf);

    XceiverServerRatis writeChannel = mock(XceiverServerRatis.class);
    when(ozoneContainer.getWriteChannel()).thenReturn(writeChannel);
    // When the pipeline has been closed earlier by other datanode that received a close pipeline command
    when(writeChannel.isExist(pipelineID)).thenReturn(false);

    final ClosePipelineCommandHandler commandHandler =
        new ClosePipelineCommandHandler(MoreExecutors.directExecutor());
    commandHandler.handle(command, ozoneContainer, stateContext, connectionManager);

    verify(writeChannel, times(0))
        .removeGroup(any(), any());
  }

  @Test
  void testPendingPipelineClose() throws IOException, InterruptedException {
    final List<DatanodeDetails> datanodes = getDatanodes();
    final DatanodeDetails currentDatanode = datanodes.get(0);
    final PipelineID pipelineID = PipelineID.randomId();
    final UUID pipelineUUID = pipelineID.getId();
    final SCMCommand<ClosePipelineCommandProto> command1 = new ClosePipelineCommand(pipelineID);
    final SCMCommand<ClosePipelineCommandProto> command2 = new ClosePipelineCommand(pipelineID);
    StateContext stateContext = ContainerTestUtils.getMockContext(currentDatanode, conf);

    XceiverServerRatis writeChannel = mock(XceiverServerRatis.class);
    when(ozoneContainer.getWriteChannel()).thenReturn(writeChannel);
    when(writeChannel.isExist(pipelineID)).thenReturn(true);

    CountDownLatch firstCommandStarted = new CountDownLatch(1);
    CountDownLatch secondCommandSubmitted = new CountDownLatch(1);

    doAnswer(invocation -> {
      firstCommandStarted.countDown();
      secondCommandSubmitted.await();
      return null;
    }).when(writeChannel).removeGroup(pipelineID.getGroupId(), tlsConfig);

    ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

    final ClosePipelineCommandHandler commandHandler =
        new ClosePipelineCommandHandler(singleThreadExecutor);
    assertFalse(commandHandler.isPipelineCloseInProgress(pipelineUUID));
    commandHandler.handle(command1, ozoneContainer, stateContext, connectionManager);
    assertTrue(firstCommandStarted.await(5, TimeUnit.SECONDS));
    commandHandler.handle(command2, ozoneContainer, stateContext, connectionManager);
    secondCommandSubmitted.countDown();

    singleThreadExecutor.shutdown();
    assertTrue(singleThreadExecutor.awaitTermination(10, TimeUnit.SECONDS));
    
    // Only one command should have been processed due to duplicate prevention
    assertEquals(1, commandHandler.getInvocationCount());
    assertFalse(commandHandler.isPipelineCloseInProgress(pipelineUUID));
  }

  private List<DatanodeDetails> getDatanodes() {
    final DatanodeDetails dnOne = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnTwo = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnThree = MockDatanodeDetails.randomDatanodeDetails();
    return Arrays.asList(dnOne, dnTwo, dnThree);
  }

}
