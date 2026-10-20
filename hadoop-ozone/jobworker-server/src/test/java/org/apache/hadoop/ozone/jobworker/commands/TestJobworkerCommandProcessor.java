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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto.Type;
import org.apache.hadoop.ozone.jobworker.JobworkerConnectionManager;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.ozone.jobworker.JobworkerStateMachine;
import org.apache.hadoop.ozone.jobworker.JobworkerStates;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

/**
 * Tests for JobworkerCommandProcessor class.
 */
public class TestJobworkerCommandProcessor {

  private JobworkerStateContext context;
  private JobworkerCommandManager commandManager;
  private JobworkerCommandDispatcher commandDispatcher;
  private OzoneConfiguration conf;
  private AtomicLong nextHB;
  private JobworkerCommandProcessor processor;

  @BeforeEach
  public void setUp() {
    context = mock(JobworkerStateContext.class);
    when(context.getState()).thenReturn(JobworkerStates.RUNNING);
    commandManager = mock(JobworkerCommandManager.class);
    commandDispatcher =
        spy(JobworkerCommandDispatcher
            .newBuilder()
            .setContext(context)
            .setConnectionManager(mock(JobworkerConnectionManager.class))
            .build());
    conf = new OzoneConfiguration();
    nextHB = new AtomicLong(0);
    processor = new JobworkerCommandProcessor(
        context, commandManager, commandDispatcher, conf, "test-", nextHB);
  }

  @AfterEach
  public void tearDown() throws IOException {
    processor.close();
  }

  @Test
  public void testStartStopProcessor() throws IOException, InterruptedException {
    processor.start();
    Thread cmdThread = processor.getCommandProcessThread();

    assertNotNull(cmdThread);
    assertTrue(cmdThread.isAlive());
    processor.stop();
    cmdThread.join(1000);
    assertTrue(!cmdThread.isAlive() || cmdThread.isInterrupted());
  }

  @Test
  @Timeout(5)
  public void testCommandProcessing() throws InterruptedException {
    JobworkerCommand<?> command1 = new MockJobworkerCommand(1L, Type.mockCommand);
    JobworkerCommand<?> command2 = new MockJobworkerCommand(2L, Type.mockCommand);

    // First call returns command1, second call returns command2, third call returns null
    when(commandManager.getNextCommand())
        .thenReturn(command1)
        .thenReturn(command2)
        .thenReturn(null);

    processor.start();
    // Allow time for commands to be processed
    Thread.sleep(500);

    ArgumentCaptor<JobworkerCommand> commandCaptor = ArgumentCaptor.forClass(JobworkerCommand.class);

    // Verify dispatcher.handle was called for each command in order
    verify(commandDispatcher, times(2)).handle(commandCaptor.capture());
    assertEquals(2, commandCaptor.getAllValues().size());
    assertEquals(command1.getId(), commandCaptor.getAllValues().get(0).getId());
    assertEquals(command2.getId(), commandCaptor.getAllValues().get(1).getId());
    assertEquals(2, processor.getCommandsHandled());
  }

  @Test
  public void testExceptionHandling() throws InterruptedException, TimeoutException {
    // Setup mock command manager to throw exception
    when(commandManager.getNextCommand())
        .thenThrow(new RuntimeException("Test exception"));
    when(context.getState()).thenReturn(JobworkerStates.RUNNING);
    processor.start();
    // Allow time for exception to occur
    Thread.sleep(500);

    Thread cmdThread = processor.getCommandProcessThread();
    assertNotNull(cmdThread);
    // The old thread should exit
    GenericTestUtils.waitFor(() -> !cmdThread.isAlive(),
        200, 10000);
    reset(commandManager);
    when(commandManager.getNextCommand()).thenReturn(null);
    // Process will start a now thread
    GenericTestUtils.waitFor(() -> processor.getCommandProcessThread().isAlive(),
        200, 10000);
  }

  @Test
  public void testShutdownState() throws InterruptedException {
    when(context.getState()).thenReturn(JobworkerStates.SHUTDOWN);
    processor.start();
    Thread.sleep(500);
    verify(commandManager, times(0)).getNextCommand();
  }

  @Test
  public void testUnhandledCommandStatusUpdate() throws InterruptedException {
    conf = new OzoneConfiguration();
    commandManager = new JobworkerCommandManager(conf);
    JobworkerDetails jobworkerDetails = MockJobworkerDetails.randomJobworkerDetails();
    context = new JobworkerStateContext(conf, JobworkerStates.RUNNING,
        jobworkerDetails, "test-", mock(JobworkerStateMachine.class), commandManager);

    // The unknownCommand no handler, so we can use it to simulate a no handler case
    JobworkerCommand<?> command = new MockJobworkerCommand(1L, "omServiceId",
        OMJobworkerCommandProto.Type.unknownCommand);
    commandManager.addCommand(command);
    JobworkerCommandStatus cmdStatus = commandManager.getCmdStatus(command.getOmServiceId(), command.getId());
    processor = new JobworkerCommandProcessor(
        context, commandManager, commandDispatcher, conf, "test-", nextHB);

    processor.start();

    Thread.sleep(500);
    assertEquals(CommandStatus.Status.FAILED, cmdStatus.getStatus());
    assertEquals("Command cannot be handled", cmdStatus.getMessage());
  }
}
