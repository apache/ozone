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

package org.apache.hadoop.ozone.om.jobworker.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandExecutionResultsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandResultCode;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.MockCommandResultsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.jobworker.client.JobworkerClient;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerCommand;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.jobworker.JobworkerHeartbeatDispatcher.CommandStatusReportFromJobworker;
import org.apache.hadoop.ozone.om.jobworker.OMJobworkerEvents;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeManager;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration test for OMJobworkerCommandManager with JobworkerCommandListener
 * and JobworkerCommandStatusReportHandler using the event system.
 */

public class TestOMJobworkerCommandManager {

  private JobworkerNodeManager nodeManager;
  private OMJobworkerCommandManager commandManager;
  private TestCommandListener testReregisterCommandListener;
  private TestCommandListener testMockCommandListener;
  private UUID jobworkerUuid;
  private JobworkerDetails jobworkerDetails;
  private EventQueue eventQueue;
  private JobworkerClient client;
  private final int timeoutCheckIntervalSeconds = 2;

  @TempDir
  private Path folder;
  private OzoneManager ozoneManager;
  private String omServiceId;

  @BeforeEach
  public void setUp() throws Exception {
    OzoneConfiguration conf = createNewTestPath();
    OMJobworkerCommandManager.CommandTimeoutChecker.setCheckIntervalForTesting(timeoutCheckIntervalSeconds);
    OmTestManagers omTestManagers = new OmTestManagers(conf);
    ozoneManager = omTestManagers.getOzoneManager();
    omServiceId = ozoneManager.getOMServiceId();
    // Commands are only sent by the ready leader.
    GenericTestUtils.waitFor(() -> ozoneManager.getJobworkerLeaderTracker().isLeaderReady(), 100, 30000);

    conf.setInt("ozone.om.jobworker.grpc.port", ozoneManager.getJobworkerGrpcPort());
    client = new JobworkerClient("localhost", conf);

    nodeManager = ozoneManager.getJobworkerNodemanager();
    commandManager = ozoneManager.getOMJobworkerCommandManager();

    jobworkerUuid = UUID.randomUUID();
    jobworkerDetails = MockJobworkerDetails.createJobworkerDetails(jobworkerUuid.toString());
    eventQueue = ozoneManager.getJobworkerEventQueue();

    // Overwrite both command types with a testMockCommandListener
    testReregisterCommandListener = new TestCommandListener();
    testMockCommandListener = new TestCommandListener();
    commandManager.registerHandler(OMJobworkerCommandProto.Type.reregisterCommand, testReregisterCommandListener);
    commandManager.registerHandler(OMJobworkerCommandProto.Type.mockCommand, testMockCommandListener);

    // Send a register command so that heartbeat in the test will implicitly trigger a reregister command
    // when handling a heartbeat (used to trigger onSendCommand)and simplifying the test assertions
    sendRegister();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (ozoneManager != null) {
      ozoneManager.close();
    }
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void testCommandStateTransitions() throws IOException, InterruptedException, TimeoutException {
    OMJobworkerCommand command = new MockOMJobworkerCommand(
        1L, OMJobworkerCommandProto.Type.mockCommand);

    long commandId = commandManager.sendCommand(jobworkerUuid, command);
    assertEquals(1L, commandId);

    // Sent will be incremented only when heartbeat is returned
    assertEquals(0, testReregisterCommandListener.sentCount.get());
    assertEquals(0, testMockCommandListener.sentCount.get());
    sendHeartbeat();
    waitAndAssert(testMockCommandListener.sentLatch, testMockCommandListener.sentCount, 1);
    // The reregister sentCount remains zero since the jobworker has been registered by calling sendRegister
    // during the test setup
    assertEquals(0, testReregisterCommandListener.sentCount.get());

    // 1. PENDING -> EXECUTING
    CommandStatus executingStatus = createCommandStatus(
        1L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.EXECUTING);
    CommandStatusReportsProto executingReport = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(executingStatus)
        .build();

    eventQueue.fireEvent(OMJobworkerEvents.JW_COMMAND_STATUS_REPORT,
        new CommandStatusReportFromJobworker(jobworkerDetails, executingReport));

    // Wait for the handler to be called
    waitAndAssert(testMockCommandListener.executingLatch, testMockCommandListener.executingCount, 1);
    assertEquals(0, testMockCommandListener.successCount.get());
    assertEquals(0, testMockCommandListener.failureCount.get());
    assertEquals(1, testMockCommandListener.sentCount.get());

    // 2. EXECUTING -> SUCCEEDED
    CommandStatus succeededStatus = createCommandStatus(
        1L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.SUCCEEDED, null,
        null,
        CommandExecutionResultsProto.newBuilder()
            .setMockCommandResults(MockCommandResultsProto
                .newBuilder()
                .setResultCode(CommandResultCode.SUCCESS)
                .build())
            .build());
    CommandStatusReportsProto succeededReport = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(succeededStatus)
        .build();

    eventQueue.fireEvent(OMJobworkerEvents.JW_COMMAND_STATUS_REPORT,
        new CommandStatusReportFromJobworker(jobworkerDetails, succeededReport));

    // Wait for the handler to be called
    waitAndAssert(testMockCommandListener.successLatch, testMockCommandListener.successCount, 1);
    waitAndAssert(testMockCommandListener.executingLatch, testMockCommandListener.executingCount, 1);
    assertEquals(0, testMockCommandListener.failureCount.get());
    assertEquals(1, testMockCommandListener.sentCount.get());
    assertNotNull(testMockCommandListener.lastExecutionResultsProto);
    assertEquals(CommandResultCode.SUCCESS,
        testMockCommandListener.lastExecutionResultsProto.getMockCommandResults().getResultCode());
  }

  @Test
  public void testMultipleCommandStates() throws IOException, InterruptedException, TimeoutException {
    // Create multiple Command
    OMJobworkerCommand command1 = new MockOMJobworkerCommand(
        1L, OMJobworkerCommandProto.Type.mockCommand);
    OMJobworkerCommand command2 = new MockOMJobworkerCommand(
        2L, OMJobworkerCommandProto.Type.reregisterCommand);
    OMJobworkerCommand command3 = new MockOMJobworkerCommand(
        3L, OMJobworkerCommandProto.Type.mockCommand);
    commandManager.sendCommand(jobworkerUuid, command1);
    commandManager.sendCommand(jobworkerUuid, command2);
    commandManager.sendCommand(jobworkerUuid, command3);
    sendHeartbeat();

    GenericTestUtils.waitFor(() -> testMockCommandListener.sentCount.get() == 2
        && testReregisterCommandListener.sentCount.get() == 1, 100, 5000);
    List<CommandStatus> statuses = Arrays.asList(
        // Command 1: PENDING -> EXECUTING
        createCommandStatus(1L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.EXECUTING),

        // Command 2: PENDING -> EXECUTING
        createCommandStatus(2L, OMJobworkerCommandProto.Type.reregisterCommand, CommandStatus.Status.EXECUTING),

        // Command 3: PENDING -> FAILED (direct failure)
        createCommandStatus(3L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.FAILED)
    );

    CommandStatusReportsProto multiReport = CommandStatusReportsProto.newBuilder()
        .addAllCmdStatus(statuses)
        .build();

    eventQueue.fireEvent(OMJobworkerEvents.JW_COMMAND_STATUS_REPORT,
        new CommandStatusReportFromJobworker(jobworkerDetails, multiReport));

    waitAndAssert(testMockCommandListener.executingLatch, testMockCommandListener.executingCount, 1);
    waitAndAssert(testReregisterCommandListener.executingLatch, testReregisterCommandListener.executingCount, 1);
    waitAndAssert(testMockCommandListener.failureLatch, testMockCommandListener.failureCount, 1);
    assertEquals(0, testMockCommandListener.successCount.get());
    assertEquals(0, testReregisterCommandListener.successCount.get());
    assertEquals(2, testMockCommandListener.sentCount.get());
    assertEquals(1, testReregisterCommandListener.sentCount.get());

    CommandExecutionResultsProto failedResult = CommandExecutionResultsProto.newBuilder()
        .setMockCommandResults(MockCommandResultsProto
            .newBuilder()
            .setResultCode(CommandResultCode.BUCKET_NOT_FOUND)
            .build())
        .build();

    // Now update command states to final states
    List<CommandStatus> finalStatuses = Arrays.asList(
        // Command 1: EXECUTING -> SUCCEEDED
        createCommandStatus(1L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.SUCCEEDED),

        // Command 2: EXECUTING -> FAILED
        createCommandStatus(2L, OMJobworkerCommandProto.Type.reregisterCommand, CommandStatus.Status.FAILED,
            "error msg", CommandResultCode.UNKNOWN_CODE, failedResult)
    );

    CommandStatusReportsProto finalReport = CommandStatusReportsProto.newBuilder()
        .addAllCmdStatus(finalStatuses)
        .build();
    eventQueue.fireEvent(OMJobworkerEvents.JW_COMMAND_STATUS_REPORT,
        new CommandStatusReportFromJobworker(jobworkerDetails, finalReport));

    // Wait for success handler to be called
    waitAndAssert(testMockCommandListener.successLatch, testMockCommandListener.successCount, 1); // Command 1
    waitAndAssert(testMockCommandListener.executingLatch, testMockCommandListener.executingCount, 1);
    waitAndAssert(testReregisterCommandListener.executingLatch, testReregisterCommandListener.executingCount, 1);
    waitAndAssert(testReregisterCommandListener.failureLatch, testReregisterCommandListener.failureCount, 1);
    waitAndAssert(testMockCommandListener.failureLatch, testMockCommandListener.failureCount, 1);  // Commands 3
    waitAndAssert(testMockCommandListener.executingLatch, testMockCommandListener.executingCount, 1); // Commands 1
    assertEquals("error msg", testReregisterCommandListener.lastCommandInfo.getMessage());
    assertEquals(CommandResultCode.UNKNOWN_CODE, testReregisterCommandListener.lastCommandInfo.getResultCode());
    assertEquals(CommandResultCode.BUCKET_NOT_FOUND,
        testReregisterCommandListener.lastExecutionResultsProto.getMockCommandResults().getResultCode());
    assertEquals(2, testMockCommandListener.sentCount.get());
    assertEquals(1, testReregisterCommandListener.executingCount.get());
  }

  @Test
  public void testInvalidStateTransition() throws IOException, InterruptedException, TimeoutException {
    OMJobworkerCommand command = new MockOMJobworkerCommand(
        1L, OMJobworkerCommandProto.Type.mockCommand);

    commandManager.sendCommand(jobworkerUuid, command);

    sendHeartbeat();
    waitAndAssert(testMockCommandListener.sentLatch, testMockCommandListener.sentCount, 1);
    // First make a valid transition: PENDING -> EXECUTING
    CommandStatus executingStatus = createCommandStatus(
        1L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.EXECUTING);

    CommandStatusReportsProto validReport = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(executingStatus)
        .build();
    eventQueue.fireEvent(OMJobworkerEvents.JW_COMMAND_STATUS_REPORT,
        new CommandStatusReportFromJobworker(jobworkerDetails, validReport));

    waitAndAssert(testMockCommandListener.executingLatch, testMockCommandListener.executingCount, 1);

    // Now try an invalid transition: EXECUTING -> PENDING (should not be allowed)
    CommandStatus pendingStatus = createCommandStatus(
        1L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.PENDING);

    CommandStatusReportsProto invalidReport = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(pendingStatus)
        .build();
    eventQueue.fireEvent(OMJobworkerEvents.JW_COMMAND_STATUS_REPORT,
        new CommandStatusReportFromJobworker(jobworkerDetails, invalidReport));

    // Verify that the listener counts didn't change (invalid transition was ignored)
    waitAndAssert(testMockCommandListener.executingLatch, testMockCommandListener.executingCount, 1);
    assertEquals(0, testMockCommandListener.successCount.get());
    assertEquals(0, testMockCommandListener.failureCount.get());
    assertEquals(1, testMockCommandListener.sentCount.get());

    // Now try a valid transition: EXECUTING -> SUCCEEDED
    CommandStatus succeededStatus = createCommandStatus(
        1L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.SUCCEEDED);
    CommandStatusReportsProto finalReport = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(succeededStatus)
        .build();
    eventQueue.fireEvent(OMJobworkerEvents.JW_COMMAND_STATUS_REPORT,
        new CommandStatusReportFromJobworker(jobworkerDetails, finalReport));

    // Verify that the success listener was called
    waitAndAssert(testMockCommandListener.successLatch, testMockCommandListener.successCount, 1);
    assertEquals(1, testMockCommandListener.sentCount.get());
  }

  @Test
  public void testCommandIsStampedWithLeaderTerm() throws Exception {
    OMJobworkerCommand command = new MockOMJobworkerCommand(1L, OMJobworkerCommandProto.Type.mockCommand);
    commandManager.sendCommand(jobworkerUuid, command);

    long term = ozoneManager.getJobworkerLeaderTracker().getTermIfReady().getAsLong();
    assertTrue(term > 0);
    assertEquals(term, command.getTerm());
  }

  @Test
  public void testStatusFromDifferentTermIsIgnored() throws Exception {
    OMJobworkerCommand command = new MockOMJobworkerCommand(1L, OMJobworkerCommandProto.Type.mockCommand);
    commandManager.sendCommand(jobworkerUuid, command);

    CommandStatus staleTerm = createCommandStatus(1L, OMJobworkerCommandProto.Type.mockCommand,
        CommandStatus.Status.SUCCEEDED).toBuilder().setTerm(command.getTerm() + 1).build();
    commandManager.processStatusUpdate(jobworkerDetails, staleTerm);
    assertEquals(0, testMockCommandListener.successCount.get());
    assertTrue(commandManager.isCommandInFlight(OMJobworkerCommandProto.Type.mockCommand, 1L));

    CommandStatus sameTerm = staleTerm.toBuilder().setTerm(command.getTerm()).build();
    commandManager.processStatusUpdate(jobworkerDetails, sameTerm);
    assertEquals(1, testMockCommandListener.successCount.get());
    assertFalse(commandManager.isCommandInFlight(OMJobworkerCommandProto.Type.mockCommand, 1L));
  }

  @Test
  public void testLeadershipChangeDropsInFlightCommands() throws Exception {
    commandManager.sendCommand(jobworkerUuid, new MockOMJobworkerCommand(1L, OMJobworkerCommandProto.Type.mockCommand));
    commandManager.sendCommand(jobworkerUuid, new MockOMJobworkerCommand(2L, OMJobworkerCommandProto.Type.mockCommand));
    assertEquals(2, commandManager.getInFlightCommandCount(OMJobworkerCommandProto.Type.mockCommand));

    commandManager.onLeadershipChange();

    assertEquals(0, commandManager.getInFlightCommandCount(OMJobworkerCommandProto.Type.mockCommand));
    assertEquals(2, testMockCommandListener.timeoutCount.get());
    // A late status of a dropped command is ignored.
    commandManager.processStatusUpdate(jobworkerDetails,
        createCommandStatus(1L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.SUCCEEDED));
    assertEquals(0, testMockCommandListener.successCount.get());
  }

  /**
   * Test implementation of JobworkerCommandListener.
   */
  private static class TestCommandListener implements JobworkerCommandListener {
    private final AtomicInteger sentCount = new AtomicInteger(0);
    private final AtomicInteger executingCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger timeoutCount = new AtomicInteger(0);

    private final CountDownLatch sentLatch = new CountDownLatch(1);
    private final CountDownLatch executingLatch = new CountDownLatch(1);
    private final CountDownLatch successLatch = new CountDownLatch(1);
    private final CountDownLatch failureLatch = new CountDownLatch(1);
    private final CountDownLatch timeoutLatch = new CountDownLatch(1);
    private volatile CommandExecutionResultsProto lastExecutionResultsProto = null;
    private volatile JobworkerCommandInfo lastCommandInfo = null;

    @Override
    public void onSendCommand(OMJobworkerCommandProto command, UUID jobworkerUuid) {
      sentCount.incrementAndGet();
      sentLatch.countDown();
    }

    @Override
    public void onCommandSucceeded(JobworkerCommandInfo statusInfo,
        CommandExecutionResultsProto executionResultsProto, JobworkerDetails jobworkerDetails) {
      successCount.incrementAndGet();
      successLatch.countDown();
      lastExecutionResultsProto = executionResultsProto;
      lastCommandInfo = statusInfo;
    }

    @Override
    public void onCommandFailed(JobworkerCommandInfo statusInfo,
        CommandExecutionResultsProto executionResultsProto, JobworkerDetails jobworkerDetails) {
      failureCount.incrementAndGet();
      failureLatch.countDown();
      lastExecutionResultsProto = executionResultsProto;
      lastCommandInfo = statusInfo;
    }

    @Override
    public void onCommandExecuting(JobworkerCommandInfo statusInfo,
                                   JobworkerDetails jobworkerDetails) {
      executingCount.incrementAndGet();
      executingLatch.countDown();
      lastCommandInfo = statusInfo;
    }

    @Override
    public void onStatusUpdateTimeout(JobworkerCommandInfo statusInfo, UUID jobworkerUuid) {
      timeoutCount.incrementAndGet();
      timeoutLatch.countDown();
      lastCommandInfo = statusInfo;
    }
  }

  private CommandStatus createCommandStatus(long cmdId, OMJobworkerCommandProto.Type type,
      CommandStatus.Status status) {
    return createCommandStatus(cmdId, type, status, null, null, null);
  }

  private CommandStatus createCommandStatus(long cmdId, OMJobworkerCommandProto.Type type,
      CommandStatus.Status status, String msg, CommandResultCode resultCode,
      CommandExecutionResultsProto commandExecutionResults) {
    CommandStatus.Builder builder = CommandStatus.newBuilder()
        .setCmdId(cmdId)
        .setType(type)
        .setStatus(status)
        .setOmServiceId(omServiceId);
    if (commandExecutionResults != null) {
      builder.setExecutionResults(commandExecutionResults);
    }
    if (msg != null) {
      builder.setMsg(msg);
    }
    if (resultCode != null) {
      builder.setResultCode(resultCode);
    }
    return builder.build();
  }

  /**
   * Wait for a latch and assert the counter value.
   *
   * @param latch         the latch to wait for
   * @param counter       the counter to check
   * @param expectedValue the expected value of the counter
   * @throws InterruptedException if interrupted while waiting
   */
  private void waitAndAssert(CountDownLatch latch, AtomicInteger counter,
                             int expectedValue) throws InterruptedException, TimeoutException {
    assertTrue(latch.await(10, TimeUnit.SECONDS),
        "Latch should have been released within " + 10 + " seconds, current latch count: " +
            latch.getCount());
    if (expectedValue != counter.get()) {
      GenericTestUtils.waitFor(() -> expectedValue == counter.get(), 500, 5000);
    }
  }

  private void sendHeartbeat() throws IOException {
    JobworkerServiceProtocolProtos.SendHeartbeatRequest heartbeatRequest =
        JobworkerServiceProtocolProtos.SendHeartbeatRequest.newBuilder()
        .setJobworkerDetails(jobworkerDetails.getProtoBufMessage())
        .setOmServiceId(omServiceId)
        .build();
    JobworkerServiceProtocolProtos.SendHeartbeatResponseProto heartbeatResponse =
        client.sendHeartbeat(heartbeatRequest);
    assertNotNull(heartbeatResponse);
  }

  private void sendRegister() throws IOException {
    JobworkerServiceProtocolProtos.RegisterJobworkerRequest registerJobworkerRequest =
        JobworkerServiceProtocolProtos.RegisterJobworkerRequest.newBuilder()
        .setExtendedJobWorkDetailsProto(jobworkerDetails.getExtendedProtoBufMessage())
        .build();
    JobworkerServiceProtocolProtos.RegisterJobworkerResponse registerJobworkerResponse =
        client.register(registerJobworkerRequest);
    assertNotNull(registerJobworkerResponse);
  }

  private OzoneConfiguration createNewTestPath() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean("ozone.om.jobworker.service.enabled", true);
    conf.setInt("ozone.om.jobworker.grpc.port", 0);
    File newFolder = folder.toFile();
    if (!newFolder.exists()) {
      Assertions.assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    return conf;
  }
}
