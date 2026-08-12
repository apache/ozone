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

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus.Status.EXECUTED;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus.Status.FAILED;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus.Status.PENDING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.junit.jupiter.api.Test;

/**
 * Test cases to verify {@link CommandDispatcher}.
 */
public class TestCommandDispatcher {

  @Test
  public void testHandlerExceptionFailsPendingReplicationCommand() {
    assertStatusAfterHandlerThrows(PENDING, FAILED);
  }

  @Test
  public void testHandlerExceptionKeepsStatusReportedBySupervisor() {
    assertStatusAfterHandlerThrows(EXECUTED, EXECUTED);
  }

  private void assertStatusAfterHandlerThrows(Status initial, Status expected) {
    ReplicateContainerCommand command =
        ReplicateContainerCommand.toTarget(1L, MockDatanodeDetails.randomDatanodeDetails());
    CommandStatus status = CommandStatus.CommandStatusBuilder.newBuilder()
        .setCmdId(command.getId())
        .setType(SCMCommandProto.Type.replicateContainerCommand)
        .setStatus(initial)
        .build();

    StateContext context = mock(StateContext.class);
    doAnswer(invocation -> {
      invocation.<Consumer<CommandStatus>>getArgument(1).accept(status);
      return true;
    }).when(context).updateCommandStatus(eq(command.getId()), any());

    CommandHandler handler = mock(CommandHandler.class);
    when(handler.getCommandType()).thenReturn(SCMCommandProto.Type.replicateContainerCommand);
    doAnswer(invocation -> {
      throw new IllegalStateException("handler failed");
    }).when(handler).handle(any(), any(), any(), any());

    CommandDispatcher.newBuilder()
        .addHandler(handler)
        .setConnectionManager(mock(SCMConnectionManager.class))
        .setContainer(mock(OzoneContainer.class))
        .setContext(context)
        .build()
        .handle(command);

    assertEquals(expected, status.getStatus());
  }
}
