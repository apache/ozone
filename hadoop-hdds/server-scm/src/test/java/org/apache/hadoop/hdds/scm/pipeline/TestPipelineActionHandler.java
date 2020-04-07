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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ClosePipelineInfo;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineActionsProto;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.PipelineActionsFromDatanode;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.UUID;

/**
 * Test-cases to verify the functionality of PipelineActionHandler.
 */
public class TestPipelineActionHandler {

  @Test
  public void testCloseActionForMissingPipeline()
      throws PipelineNotFoundException {
    final PipelineManager manager = Mockito.mock(PipelineManager.class);
    final EventQueue queue = Mockito.mock(EventQueue.class);

    Mockito.when(manager.getPipeline(Mockito.any(PipelineID.class)))
        .thenThrow(new PipelineNotFoundException());

    final PipelineActionHandler actionHandler =
        new PipelineActionHandler(manager, null);

    final PipelineActionsProto actionsProto = PipelineActionsProto.newBuilder()
        .addPipelineActions(PipelineAction.newBuilder()
        .setClosePipeline(ClosePipelineInfo.newBuilder()
            .setPipelineID(HddsProtos.PipelineID.newBuilder()
                .setId(UUID.randomUUID().toString()).build())
            .setReason(ClosePipelineInfo.Reason.PIPELINE_FAILED))
            .setAction(PipelineAction.Action.CLOSE).build())
        .build();
    final PipelineActionsFromDatanode pipelineActions =
        new PipelineActionsFromDatanode(
            MockDatanodeDetails.randomDatanodeDetails(), actionsProto);

    actionHandler.onMessage(pipelineActions, queue);

    Mockito.verify(queue, Mockito.times(1))
        .fireEvent(Mockito.any(), Mockito.any(CommandForDatanode.class));

  }

}