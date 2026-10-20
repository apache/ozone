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

package org.apache.hadoop.ozone.jobworker.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandResultCode;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.jobworker.JobworkerConnectionManager;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for AbstractJobworkerCommandHandler class.
 */
public class TestAbstractJobworkerCommandHandler {

  private JobworkerStateContext context;
  private JobworkerConnectionManager connectionManager;
  private MockCommandHandler handler;
  private JobworkerCommandManager commandManager;
  private ExecutorService executorService;

  @BeforeEach
  public void setUp() {
    context = mock(JobworkerStateContext.class);
    connectionManager = mock(JobworkerConnectionManager.class);
    OzoneConfiguration conf = new OzoneConfiguration();
    commandManager = new JobworkerCommandManager(conf);
    executorService = Executors.newSingleThreadScheduledExecutor();
    handler = new MockCommandHandler(OMJobworkerCommandProto.Type.mockCommand, executorService);
  }

  @AfterEach
  public void tearDown() {
    ServerUtils.executorServiceShutdownGraceful(executorService);
  }

  @Test
  public void testBasicHandling() throws ExecutionException, InterruptedException, TimeoutException {
    handler.setProcessDelayMs(2);
    handler.enablePause();
    MockJobworkerCommand command =
        new MockJobworkerCommand(1L, "omServiceId", OMJobworkerCommandProto.Type.mockCommand);

    commandManager.addCommand(command);
    JobworkerCommandStatus cmdStatus = commandManager.getCmdStatus(command.getOmServiceId(), command.getId());

    when(context.getCommandManager()).thenReturn(commandManager);

    CompletableFuture<?> future = CompletableFuture.runAsync(() -> {
      handler.handle(command, context, connectionManager);
    });
    GenericTestUtils.waitFor(() -> handler.getQueuedCount() == 1, 50, 1000);
    handler.releasePause();
    future.get();
    GenericTestUtils.waitFor(() -> handler.getQueuedCount() == 0, 50, 1000);
    assertTrue(handler.wasProcessCommandCalled());
    assertEquals(1, handler.getInvocationCount());
    assertTrue(handler.getTotalRunTime() > 0);

    assertEquals(CommandStatus.Status.SUCCEEDED, cmdStatus.getStatus());
  }

  @Test
  public void testExpiredCommand() {
    // Create a command with an expired deadline
    MockJobworkerCommand command = new MockJobworkerCommand(1L, "omServiceId", 0L,
        System.currentTimeMillis() - 1000, OMJobworkerCommandProto.Type.mockCommand);

    commandManager.addCommand(command);
    JobworkerCommandStatus cmdStatus = commandManager.getCmdStatus(command.getOmServiceId(), command.getId());

    when(context.getCommandManager()).thenReturn(commandManager);

    handler.handle(command, context, connectionManager);
    assertFalse(handler.wasProcessCommandCalled());
    assertEquals(CommandStatus.Status.FAILED, cmdStatus.getStatus());
    assertEquals(CommandResultCode.COMMAND_EXPIRED, cmdStatus.getProtobufMessage().getResultCode());
  }

  @Test
  public void testExceptionHandling() throws InterruptedException, TimeoutException {
    MockJobworkerCommand command =
        new MockJobworkerCommand(1L, "omServiceId", OMJobworkerCommandProto.Type.mockCommand);

    Exception testException = new RuntimeException("Test exception");
    handler.setExceptionToThrow(testException);

    commandManager.addCommand(command);
    JobworkerCommandStatus cmdStatus = commandManager.getCmdStatus(command.getOmServiceId(), command.getId());

    when(context.getCommandManager()).thenReturn(commandManager);

    handler.handle(command, context, connectionManager);
    GenericTestUtils.waitFor(() -> cmdStatus.getStatus() == CommandStatus.Status.FAILED, 50, 1000);
    assertTrue(handler.wasProcessCommandCalled());
    assertEquals(CommandStatus.Status.FAILED, cmdStatus.getStatus());
    assertEquals(1, handler.getInvocationCount());
    assertEquals(CommandResultCode.OTHER_ERROR, cmdStatus.getProtobufMessage().getResultCode());
  }

  @Test
  public void testUpdateCommandStatus() {
    MockJobworkerCommand command =
        new MockJobworkerCommand(1L, "omServiceId", OMJobworkerCommandProto.Type.mockCommand);

    commandManager.addCommand(command);
    JobworkerCommandStatus cmdStatus = commandManager.getCmdStatus(command.getOmServiceId(), command.getId());

    when(context.getCommandManager()).thenReturn(commandManager);

    Consumer<JobworkerCommandStatus> statusUpdater = status -> {
      status.updateStatusAndMessage(CommandStatus.Status.SUCCEEDED, "Success", CommandResultCode.OTHER_ERROR);
    };

    handler.updateCommandStatus(context, command, statusUpdater);
    // Command should be updated normally
    assertEquals(CommandStatus.Status.SUCCEEDED, cmdStatus.getStatus());
    assertEquals("Success", cmdStatus.getMessage());
  }

  @Test
  public void testUpdateCommandStatusWithResultCode() throws InterruptedException, TimeoutException {
    // Test the new CommandResultCode parameter in updateCommandStatus
    MockJobworkerCommand command =
        new MockJobworkerCommand(1L, "omServiceId", OMJobworkerCommandProto.Type.mockCommand);

    commandManager.addCommand(command);
    JobworkerCommandStatus cmdStatus = commandManager.getCmdStatus(command.getOmServiceId(), command.getId());

    when(context.getCommandManager()).thenReturn(commandManager);

    // Test updating status with specific CommandResultCode
    handler.updateCommandStatus(context, command, CommandStatus.Status.FAILED,
        "Command failed", CommandResultCode.KEY_NOT_FOUND);

    GenericTestUtils.waitFor(() -> CommandStatus.Status.FAILED == cmdStatus.getStatus(), 50, 1000);
    assertEquals("Command failed", cmdStatus.getMessage());
    assertEquals(CommandResultCode.KEY_NOT_FOUND, cmdStatus.getProtobufMessage().getResultCode());
  }

  @Test
  public void testCommandTypeMismatchWithResultCode() throws InterruptedException, TimeoutException {
    MockJobworkerCommand command =
        new MockJobworkerCommand(1L, "omServiceId", OMJobworkerCommandProto.Type.unknownCommand);

    commandManager.addCommand(command);
    JobworkerCommandStatus cmdStatus = commandManager.getCmdStatus(command.getOmServiceId(), command.getId());
    when(context.getCommandManager()).thenReturn(commandManager);
    handler.handle(command, context, connectionManager);

    // Should fail due to type mismatch and set TYPE_MISMATCH result code
    GenericTestUtils.waitFor(() -> CommandStatus.Status.FAILED == cmdStatus.getStatus(), 50, 1000);
    assertEquals(CommandResultCode.TYPE_MISMATCH, cmdStatus.getProtobufMessage().getResultCode());
    assertFalse(handler.wasProcessCommandCalled());
  }

  @Test
  public void testRejectedExecutionException() throws InterruptedException, TimeoutException {
    MockJobworkerCommand command =
        new MockJobworkerCommand(1L, "omServiceId", OMJobworkerCommandProto.Type.mockCommand);
    commandManager.addCommand(command);
    JobworkerCommandStatus cmdStatus = commandManager.getCmdStatus(command.getOmServiceId(), command.getId());
    when(context.getCommandManager()).thenReturn(commandManager);

    // Shutdown the executor service to cause RejectedExecutionException
    executorService.shutdown();

    // Create a new handler with the shutdown executor service
    MockCommandHandler rejectedHandler =
        new MockCommandHandler(OMJobworkerCommandProto.Type.mockCommand, executorService);
    rejectedHandler.handle(command, context, connectionManager);

    // Command should fail with COMMAND_REJECTED result code
    GenericTestUtils.waitFor(() -> CommandStatus.Status.FAILED == cmdStatus.getStatus(), 50, 1000);
    assertEquals(CommandResultCode.COMMAND_REJECTED, cmdStatus.getProtobufMessage().getResultCode());
    assertEquals("Command execution rejected", cmdStatus.getMessage());
    assertFalse(rejectedHandler.wasProcessCommandCalled());
    assertEquals(1, rejectedHandler.getInvocationCount());
    assertEquals(0, rejectedHandler.getQueuedCount()); // Should be decremented after rejection
  }

  @Test
  public void testSubmissionUnexpectedError() throws InterruptedException, TimeoutException {
    MockJobworkerCommand command =
        new MockJobworkerCommand(1L, "omServiceId", OMJobworkerCommandProto.Type.mockCommand);
    commandManager.addCommand(command);
    JobworkerCommandStatus cmdStatus = commandManager.getCmdStatus(command.getOmServiceId(), command.getId());
    when(context.getCommandManager()).thenReturn(commandManager);

    // Create a handler with a mock executor service that throws unexpected error
    ExecutorService faultyExecutor = mock(ExecutorService.class);
    RuntimeException unexpectedError = new RuntimeException("Unexpected submission error");
    when(faultyExecutor.submit(any(Runnable.class))).thenThrow(unexpectedError);
    MockCommandHandler faultyHandler = new MockCommandHandler(OMJobworkerCommandProto.Type.mockCommand, faultyExecutor);
    faultyHandler.handle(command, context, connectionManager);

    // Command should fail with UNEXPECTED_ERROR result code
    GenericTestUtils.waitFor(() -> CommandStatus.Status.FAILED == cmdStatus.getStatus(), 50, 1000);
    assertEquals(CommandResultCode.UNEXPECTED_ERROR, cmdStatus.getProtobufMessage().getResultCode());
    assertEquals("Unexpected submission error", cmdStatus.getMessage());
    assertFalse(faultyHandler.wasProcessCommandCalled());
    assertEquals(1, faultyHandler.getInvocationCount());
    assertEquals(0, faultyHandler.getQueuedCount()); // Should be decremented after error
  }
}
