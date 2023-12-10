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

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ClosePipelineInfo;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineActionsProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.PipelineActionsFromDatanode;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * Test-cases to verify the functionality of PipelineActionHandler.
 */
public class TestPipelineActionHandler {


  @Test
  public void testPipelineActionHandlerForValidPipeline() throws IOException {

    final PipelineManager manager = Mockito.mock(PipelineManager.class);
    final EventQueue queue = Mockito.mock(EventQueue.class);
    final PipelineActionHandler actionHandler = new PipelineActionHandler(
        manager, SCMContext.emptyContext(), null);
    final Pipeline pipeline = HddsTestUtils.getRandomPipeline();

    actionHandler.onMessage(getPipelineActionsFromDatanode(
        pipeline.getId()), queue);
    Mockito.verify(manager, Mockito.times(1))
        .closePipeline(pipeline.getId());
  }

  @Test
  public void testPipelineActionHandlerForValidPipelineInFollower()
      throws IOException {
    final PipelineManager manager = Mockito.mock(PipelineManager.class);
    final EventQueue queue = Mockito.mock(EventQueue.class);
    final SCMContext context = SCMContext.emptyContext();
    final PipelineActionHandler actionHandler = new PipelineActionHandler(
        manager, context, null);
    final Pipeline pipeline = HddsTestUtils.getRandomPipeline();

    context.updateLeaderAndTerm(false, 1);
    actionHandler.onMessage(getPipelineActionsFromDatanode(
        pipeline.getId()), queue);
    Mockito.verify(manager, Mockito.times(0))
        .closePipeline(pipeline.getId());
    Mockito.verify(queue, Mockito.times(0))
        .fireEvent(Mockito.eq(SCMEvents.DATANODE_COMMAND),
            Mockito.any(CommandForDatanode.class));
  }

  @Test
  public void testPipelineActionHandlerForUnknownPipeline() throws IOException {
    final PipelineManager manager = Mockito.mock(PipelineManager.class);
    final EventQueue queue = Mockito.mock(EventQueue.class);
    final PipelineActionHandler actionHandler = new PipelineActionHandler(
        manager, SCMContext.emptyContext(), null);
    final Pipeline pipeline = HddsTestUtils.getRandomPipeline();

    Mockito.doThrow(new PipelineNotFoundException())
        .when(manager).closePipeline(pipeline.getId());
    actionHandler.onMessage(getPipelineActionsFromDatanode(
        pipeline.getId()), queue);
    Mockito.verify(queue, Mockito.times(1))
        .fireEvent(Mockito.eq(SCMEvents.DATANODE_COMMAND),
            Mockito.any(CommandForDatanode.class));
  }

  @Test
  public void testPipelineActionHandlerForUnknownPipelineInFollower()
      throws IOException {

    final PipelineManager manager = Mockito.mock(PipelineManager.class);
    final EventQueue queue = Mockito.mock(EventQueue.class);
    final SCMContext context = SCMContext.emptyContext();
    final PipelineActionHandler actionHandler = new PipelineActionHandler(
        manager, context, null);
    final Pipeline pipeline = HddsTestUtils.getRandomPipeline();

    context.updateLeaderAndTerm(false, 1);
    Mockito.doThrow(new PipelineNotFoundException())
        .when(manager).closePipeline(pipeline.getId());
    actionHandler.onMessage(getPipelineActionsFromDatanode(
        pipeline.getId()), queue);
    Mockito.verify(queue, Mockito.times(0))
        .fireEvent(Mockito.eq(SCMEvents.DATANODE_COMMAND),
            Mockito.any(CommandForDatanode.class));

  }

  private PipelineActionsFromDatanode getPipelineActionsFromDatanode(
      PipelineID pipelineID) {
    final PipelineActionsProto actionsProto = PipelineActionsProto.newBuilder()
        .addPipelineActions(PipelineAction.newBuilder()
            .setClosePipeline(ClosePipelineInfo.newBuilder()
                .setPipelineID(pipelineID.getProtobuf())
                .setReason(ClosePipelineInfo.Reason.PIPELINE_FAILED))
            .setAction(PipelineAction.Action.CLOSE).build())
        .build();
    return new PipelineActionsFromDatanode(
        MockDatanodeDetails.randomDatanodeDetails(), actionsProto);
  }

}
