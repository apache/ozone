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

import static org.apache.hadoop.ozone.jobworker.commands.CommandTestUtils.waitTillFinishExecution;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.jobworker.JobworkerConnectionManager;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.ozone.jobworker.JobworkerStateMachine;
import org.apache.hadoop.ozone.jobworker.JobworkerStates;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for JobworkerCommandDispatcher class.
 */
public class TestJobworkerCommandDispatcher {

  private JobworkerStateContext context;
  private JobworkerConnectionManager connectionManager;
  private JobworkerCommandDispatcher dispatcher;
  private MockCommandHandler reRegisterHandler;
  private MockCommandHandler mockCommandHandler;
  private ExecutorService executorService;

  @BeforeEach
  public void setUp() {
    OzoneConfiguration conf = new OzoneConfiguration();
    JobworkerDetails jobworkerDetails = MockJobworkerDetails.randomJobworkerDetails();
    context = mock(JobworkerStateContext.class);
    connectionManager = mock(JobworkerConnectionManager.class);

    // Create handlers for different command types
    executorService = Executors.newSingleThreadScheduledExecutor();
    reRegisterHandler = new MockCommandHandler(OMJobworkerCommandProto.Type.reregisterCommand, executorService);
    mockCommandHandler = new MockCommandHandler(OMJobworkerCommandProto.Type.mockCommand, executorService);
    context = new JobworkerStateContext(conf, JobworkerStates.RUNNING,
        jobworkerDetails, "jobworker-test-", mock(JobworkerStateMachine.class), new JobworkerCommandManager(conf)
    );

    dispatcher = spy(JobworkerCommandDispatcher.newBuilder()
        .addHandler(reRegisterHandler)
        .addHandler(mockCommandHandler)
        .setConnectionManager(connectionManager)
        .setContext(context)
        .build());
  }

  @AfterEach
  public void tearDown() {
    ServerUtils.executorServiceShutdownGraceful(executorService);
  }

  @Test
  public void testHandlerRegistration() {
    JobworkerCommandHandler retrievedHandler1 =
        dispatcher.getHandler(OMJobworkerCommandProto.Type.reregisterCommand);
    JobworkerCommandHandler retrievedHandler2 =
        dispatcher.getHandler(OMJobworkerCommandProto.Type.mockCommand);

    assertNotNull(retrievedHandler1);
    assertNotNull(retrievedHandler2);
    assertEquals(reRegisterHandler, retrievedHandler1);
    assertEquals(mockCommandHandler, retrievedHandler2);
  }

  @Test
  public void testCommandDispatching() throws InterruptedException, TimeoutException {
    JobworkerCommand<?> command1 =
        new MockJobworkerCommand(1L, OMJobworkerCommandProto.Type.reregisterCommand);
    JobworkerCommand<?> command2 =
        new MockJobworkerCommand(2L, OMJobworkerCommandProto.Type.mockCommand);
    // Did not add Handler for the unknownCommand command
    JobworkerCommand<?> command3 =
        new MockJobworkerCommand(3L, OMJobworkerCommandProto.Type.unknownCommand);

    dispatcher.handle(command1);
    dispatcher.handle(command2);
    dispatcher.handle(command3); // Should log but not throw exception

    // Verify handlers were called
    assertEquals(1, reRegisterHandler.getInvocationCount());
    assertEquals(1, mockCommandHandler.getInvocationCount());
    waitTillFinishExecution(reRegisterHandler);
    waitTillFinishExecution(mockCommandHandler);
    assertEquals(command1, reRegisterHandler.getLastCommand());
    assertEquals(command2, mockCommandHandler.getLastCommand());
  }

  @Test
  public void testHandlerThrowsException() throws InterruptedException, TimeoutException {
    RuntimeException exception = new RuntimeException("Test exception");
    reRegisterHandler.setExceptionToThrow(exception);
    JobworkerCommand<?> command =
        new MockJobworkerCommand(1L, OMJobworkerCommandProto.Type.reregisterCommand);

    dispatcher.handle(command);

    // reRegisterHandler should still be invoked
    waitTillFinishExecution(reRegisterHandler);
    assertEquals(1, reRegisterHandler.getInvocationCount());
    assertEquals(0, mockCommandHandler.getInvocationCount());
  }

  @Test
  public void testGetQueuedCommandCount() {
    // Set queued count in handlers
    reRegisterHandler.setQueuedCount(5);
    mockCommandHandler.setQueuedCount(3);
    Map<OMJobworkerCommandProto.Type, Integer> counts = dispatcher.getQueuedCommandCount();

    assertEquals(5, counts.get(OMJobworkerCommandProto.Type.reregisterCommand).intValue());
    assertEquals(3, counts.get(OMJobworkerCommandProto.Type.mockCommand).intValue());
    // Verify count for type without handler
    assertEquals(0,
        counts.getOrDefault(OMJobworkerCommandProto.Type.unknownCommand, 0).intValue());
  }

  @Test
  public void testGetCommandHandlerSummary() throws InterruptedException, TimeoutException {
    dispatcher.handle(new MockJobworkerCommand(1L, OMJobworkerCommandProto.Type.reregisterCommand));
    dispatcher.handle(new MockJobworkerCommand(2L, OMJobworkerCommandProto.Type.reregisterCommand));
    dispatcher.handle(new MockJobworkerCommand(3L, OMJobworkerCommandProto.Type.mockCommand));

    Map<OMJobworkerCommandProto.Type, Integer> summary = dispatcher.getCommandHandlerSummary();
    waitTillFinishExecution(reRegisterHandler);
    waitTillFinishExecution(mockCommandHandler);
    assertEquals(2, summary.get(OMJobworkerCommandProto.Type.reregisterCommand).intValue());
    assertEquals(1, summary.get(OMJobworkerCommandProto.Type.mockCommand).intValue());
  }

  @Test
  public void testStop() {
    dispatcher.stop();
    // Verify stop was called on handlers
    verify(dispatcher, times(1)).stop();
    verify(dispatcher, times(1)).stop();
  }

  @Test
  public void testDispatcherCommandStatusUpdateOnException() throws InterruptedException, TimeoutException {
    OzoneConfiguration conf = new OzoneConfiguration();
    JobworkerCommandManager commandManager = new JobworkerCommandManager(conf);
    JobworkerDetails jobworkerDetails = MockJobworkerDetails.randomJobworkerDetails();
    context = new JobworkerStateContext(conf, JobworkerStates.RUNNING,
        jobworkerDetails, "jobworker-test-", mock(JobworkerStateMachine.class), commandManager);

    dispatcher = JobworkerCommandDispatcher.newBuilder()
        .addHandler(reRegisterHandler)
        .setConnectionManager(connectionManager)
        .setContext(context)
        .build();

    RuntimeException exception = new RuntimeException("Test exception");
    reRegisterHandler.setExceptionToThrow(exception);

    MockJobworkerCommand command =
        new MockJobworkerCommand(1L, "omServiceId", OMJobworkerCommandProto.Type.reregisterCommand);
    commandManager.addCommand(command);
    JobworkerCommandStatus cmdStatus = commandManager.getCmdStatus(command.getOmServiceId(), command.getId());

    dispatcher.handle(command);

    waitTillFinishExecution(reRegisterHandler);

    assertEquals(CommandStatus.Status.FAILED, cmdStatus.getStatus());
    assertTrue(cmdStatus.getMessage().contains("Test exception"));
  }
}
