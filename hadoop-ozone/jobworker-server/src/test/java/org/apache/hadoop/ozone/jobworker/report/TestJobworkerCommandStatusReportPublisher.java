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

package org.apache.hadoop.ozone.jobworker.report;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.protobuf.Message;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandResultCode;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto.Type;
import org.apache.hadoop.ozone.jobworker.JobworkerConfiguration;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.ozone.jobworker.JobworkerStateMachine;
import org.apache.hadoop.ozone.jobworker.JobworkerStates;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommand;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommandManager;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommandStatus;
import org.apache.hadoop.ozone.jobworker.commands.MockJobworkerCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the JobworkerCommandStatusReportPublisher class.
 */
public class TestJobworkerCommandStatusReportPublisher {

  private JobworkerCommandStatusReportPublisher publisher;
  private JobworkerStateContext context;
  private JobworkerCommandManager commandManager;
  private OzoneConfiguration conf;
  private JobworkerReportManager reportManager;
  private ScheduledExecutorService executorService;

  @BeforeEach
  public void setUp() {
    conf = new OzoneConfiguration();
    JobworkerConfiguration clientConf = conf.getObject(JobworkerConfiguration.class);
    clientConf.setCommandStatusReportInterval(Duration.ofMillis(100));
    clientConf.setHeartbeatInterval(Duration.ofMillis(50));
    conf.setFromObject(clientConf);
    commandManager = new JobworkerCommandManager(conf);
    JobworkerDetails jobworkerDetails = MockJobworkerDetails.randomJobworkerDetails();
    context = new JobworkerStateContext(
        conf, JobworkerStates.RUNNING, jobworkerDetails, "test-",
        mock(JobworkerStateMachine.class), commandManager);
    publisher = new JobworkerCommandStatusReportPublisher();
    publisher.setConf(conf);
    this.reportManager = JobworkerReportManager.newBuilder(conf)
        .setStateContext(context)
        .addThreadNamePrefix(context.getThreadNamePrefix())
        .addPublisherFor(JobworkerServiceProtocolProtos.CommandStatusReportsProto.class)
        .build();
    executorService = Executors.newScheduledThreadPool(1);
    publisher.init(context, executorService, reportManager);
  }

  @AfterEach
  public void tearDown() {
    executorService.shutdownNow();
  }

  @Test
  public void testCommandReportGeneration() throws Exception {
    // Add two test commands
    String serviceId = "test-service";
    JobworkerCommand<?> command1 = new MockJobworkerCommand(1L, serviceId, Type.mockCommand);
    JobworkerCommand<?> command2 = new MockJobworkerCommand(2L, serviceId, Type.reregisterCommand);
    commandManager.addCommand(command1);
    commandManager.addCommand(command2);
    Map<String, Map<Long, JobworkerCommandStatus>> commandMap = commandManager.getCommandStatusMap();

    assertNotNull(commandMap.get(serviceId));
    assertEquals(2, commandMap.get(serviceId).size());
    CommandStatusReportsProto report = publisher.getReport();
    assertNotNull(report);
    assertEquals(2, report.getCmdStatusCount());
    // The default command is PENDING
    assertEquals(CommandStatus.Status.PENDING, report.getCmdStatus(0).getStatus());
    assertEquals(CommandStatus.Status.PENDING, report.getCmdStatus(1).getStatus());
    // PENDING Status Command will not be removed from commandManager after getting
    assertNotNull(publisher.getReport());
    Set<Long> reportedIds = report.getCmdStatusList().stream()
        .map(CommandStatus::getCmdId)
        .collect(Collectors.toSet());
    assertTrue(reportedIds.contains(1L));
    assertTrue(reportedIds.contains(2L));

    // Update the status of one command to a terminal state (SUCCEEDED)
    JobworkerCommandStatus status1 = commandMap.get(serviceId).get(1L);
    status1.updateStatusAndMessage(CommandStatus.Status.SUCCEEDED, null, CommandResultCode.OTHER_ERROR);
    report = publisher.getReport();

    // Verify the report still contains both commands
    assertNotNull(report);
    assertEquals(2, report.getCmdStatusCount());
    for (CommandStatus commandStatus : report.getCmdStatusList()) {
      if (commandStatus.getCmdId() == 1L) {
        assertEquals(CommandStatus.Status.SUCCEEDED, commandStatus.getStatus());
      }
      if (commandStatus.getCmdId() == 2L) {
        assertEquals(CommandStatus.Status.PENDING, commandStatus.getStatus());
      }
    }
    // Verify the terminal command is removed from the command map
    assertEquals(1, commandMap.get(serviceId).size());
    assertNull(commandMap.get(serviceId).get(1L));
    assertNotNull(commandMap.get(serviceId).get(2L));
  }

  @Test
  public void testConcurrentCommandProcessing() throws Exception {
    String serviceId = "test-service";
    int commandCount = 50;
    CountDownLatch latch = new CountDownLatch(commandCount);

    // Create and add commands in multiple threads
    ExecutorService testExecutor = Executors.newFixedThreadPool(10);
    for (int i = 0; i < commandCount; i++) {
      final long id = i;
      testExecutor.submit(() -> {
        try {
          JobworkerCommand<?> command = new MockJobworkerCommand(id, serviceId, Type.mockCommand);
          commandManager.addCommand(command);

          // Randomly update some statuses to terminal states
          if (id % 3 == 0) {
            JobworkerCommandStatus status = commandManager.getCmdStatus(serviceId, id);
            status.updateStatusAndMessage(CommandStatus.Status.SUCCEEDED, null,
                CommandResultCode.OTHER_ERROR);
          }
        } finally {
          latch.countDown();
        }
      });
    }

    // Wait for all commands to be added
    latch.await(5, TimeUnit.SECONDS);
    testExecutor.shutdown();
    CommandStatusReportsProto report = publisher.getReport();
    assertNotNull(report);

    long expectedRemainingCommands = IntStream.range(0, commandCount)
        .filter(i -> i % 3 != 0)
        .count();

    Map<String, Map<Long, JobworkerCommandStatus>> commandMap = commandManager.getCommandStatusMap();
    assertEquals(expectedRemainingCommands, commandMap.get(serviceId).size());
  }

  @Test
  public void testAddCommandStatusReportToSpecificEndpoint() throws Exception {
    // Create a report manager to test with
    JobworkerReportManager testReportManager = JobworkerReportManager.newBuilder(conf)
        .setStateContext(context)
        .addThreadNamePrefix(context.getThreadNamePrefix())
        .addPublisherFor(JobworkerServiceProtocolProtos.CommandStatusReportsProto.class)
        .build();

    String serviceId = "test-service";
    InetSocketAddress endpoint = new InetSocketAddress("localhost", 9862);
    testReportManager.registerEndpoint(endpoint, serviceId);

    // Create a command status report
    CommandStatus.Builder statusBuilder = CommandStatus.newBuilder()
        .setCmdId(1L)
        .setStatus(CommandStatus.Status.SUCCEEDED)
        .setType(Type.mockCommand)
        .setOmServiceId(serviceId);
    CommandStatusReportsProto report = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(statusBuilder.build())
        .build();

    testReportManager.addCommandStatusReport(serviceId, endpoint, report);

    // Verify the report was added correctly by retrieving it
    List<Message> retrievedReports = testReportManager.getLimitedCountAvailableReports(
        serviceId, endpoint);
    assertEquals(1, retrievedReports.size());
    assertInstanceOf(CommandStatusReportsProto.class, retrievedReports.get(0));

    CommandStatusReportsProto retrievedReport = (CommandStatusReportsProto) retrievedReports.get(0);
    assertEquals(1, retrievedReport.getCmdStatusCount(), "Should have one command status");
    CommandStatus retrievedStatus = retrievedReport.getCmdStatus(0);
    assertEquals(1L, retrievedStatus.getCmdId(), "Command ID should match");
    // The report should have been removed, so cannot get new reports.
    assertEquals(0, testReportManager.getLimitedCountAvailableReports(serviceId, endpoint).size());
  }

  @Test
  public void testCommandResultCodeInReports() throws Exception {
    // Test that CommandResultCode is properly included in command status reports
    String serviceId = "test-service";
    JobworkerCommand<?> command1 = new MockJobworkerCommand(1L, serviceId, Type.mockCommand);
    JobworkerCommand<?> command2 = new MockJobworkerCommand(2L, serviceId, Type.reregisterCommand);

    commandManager.addCommand(command1);
    commandManager.addCommand(command2);

    Map<String, Map<Long, JobworkerCommandStatus>> commandMap = commandManager.getCommandStatusMap();

    // Update command statuses with different CommandResultCode values
    JobworkerCommandStatus status1 = commandMap.get(serviceId).get(1L);
    JobworkerCommandStatus status2 = commandMap.get(serviceId).get(2L);

    status1.updateStatusAndMessage(CommandStatus.Status.SUCCEEDED, "Success", CommandResultCode.SUCCESS);
    status2.updateStatusAndMessage(CommandStatus.Status.FAILED, "Key not found", CommandResultCode.KEY_NOT_FOUND);

    CommandStatusReportsProto report = publisher.getReport();
    assertNotNull(report);
    assertEquals(2, report.getCmdStatusCount());

    // Verify CommandResultCode is included in the reports
    for (CommandStatus commandStatus : report.getCmdStatusList()) {
      assertTrue(commandStatus.hasResultCode(), "CommandResultCode should be present");

      if (commandStatus.getCmdId() == 1L) {
        assertEquals(CommandStatus.Status.SUCCEEDED, commandStatus.getStatus());
        assertEquals(CommandResultCode.SUCCESS, commandStatus.getResultCode());
        assertEquals("Success", commandStatus.getMsg());
      } else if (commandStatus.getCmdId() == 2L) {
        assertEquals(CommandStatus.Status.FAILED, commandStatus.getStatus());
        assertEquals(CommandResultCode.KEY_NOT_FOUND, commandStatus.getResultCode());
        assertEquals("Key not found", commandStatus.getMsg());
      }
    }
  }
}
