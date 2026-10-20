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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.Message;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto.Type;
import org.apache.hadoop.ozone.jobworker.JobworkerConfiguration;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommand;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommandManager;
import org.apache.hadoop.ozone.jobworker.commands.MockJobworkerCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for JobworkerReportManager integration with CommandStatusReportsProto.
 */
public class TestJobworkerReportCommandStatus {

  private JobworkerReportManager reportManager;
  private JobworkerStateContext mockContext;
  private OzoneConfiguration conf;

  // Test data
  private final String omServiceId1 = "om-service-1";
  private final String omServiceId2 = "om-service-2";
  private final InetSocketAddress endpoint1 = new InetSocketAddress("om1.example.com", 9862);
  private final InetSocketAddress endpoint2 = new InetSocketAddress("om2.example.com", 9862);

  @BeforeEach
  public void setup() {
    conf = new OzoneConfiguration();

    // Configure the report manager
    JobworkerConfiguration jwConf = conf.getObject(JobworkerConfiguration.class);
    jwConf.setHeartbeatInterval(Duration.ofSeconds(10));
    jwConf.setCommandStatusReportInterval(Duration.ofSeconds(30));
    conf.setFromObject(jwConf);

    mockContext = mock(JobworkerStateContext.class);

    reportManager = JobworkerReportManager.newBuilder(conf)
        .setStateContext(mockContext)
        .addPublisherFor(CommandStatusReportsProto.class)
        .build();

    reportManager.init();

    reportManager.registerEndpoint(endpoint1, omServiceId1);
    reportManager.registerEndpoint(endpoint2, omServiceId2);
  }

  @Test
  public void testAddCommandStatusReport() {
    // Create command status reports
    CommandStatus status1 = CommandStatus.newBuilder()
        .setCmdId(1L)
        .setType(Type.reregisterCommand)
        .setStatus(Status.SUCCEEDED)
        .setOmServiceId(omServiceId1)
        .build();

    CommandStatus status2 = CommandStatus.newBuilder()
        .setCmdId(2L)
        .setType(Type.mockCommand)
        .setStatus(Status.SUCCEEDED)
        .setOmServiceId(omServiceId2)
        .build();

    CommandStatus status3 = CommandStatus.newBuilder()
        .setCmdId(3L)
        .setType(Type.mockCommand)
        .setStatus(Status.FAILED)
        .setOmServiceId(omServiceId2)
        .build();

    CommandStatusReportsProto report = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(status1)
        .addCmdStatus(status2)
        .addCmdStatus(status3)
        .build();

    // Add the report to the manager
    reportManager.addReport(report);

    // Get reports for each endpoint
    List<Message> om1Reports =
        reportManager.getLimitedCountAvailableReports(omServiceId1, endpoint1);
    List<Message> om2Reports =
        reportManager.getLimitedCountAvailableReports(omServiceId2, endpoint2);

    // Verify that reports were routed correctly
    assertEquals(1, om1Reports.size());
    assertEquals(1, om2Reports.size());

    CommandStatusReportsProto om1Report = (CommandStatusReportsProto) om1Reports.get(0);
    CommandStatusReportsProto om2Report = (CommandStatusReportsProto) om2Reports.get(0);

    assertEquals(1, om1Report.getCmdStatusCount());
    assertEquals(2, om2Report.getCmdStatusCount());

    assertEquals(1L, om1Report.getCmdStatus(0).getCmdId());
    assertEquals(2L, om2Report.getCmdStatus(0).getCmdId());
    assertEquals(3L, om2Report.getCmdStatus(1).getCmdId());

    assertEquals(omServiceId1, om1Report.getCmdStatus(0).getOmServiceId());
    assertEquals(omServiceId2, om2Report.getCmdStatus(0).getOmServiceId());
    assertEquals(omServiceId2, om2Report.getCmdStatus(1).getOmServiceId());
  }

  @Test
  public void testReportLimitSize() {
    // Create a large size of command status reports
    int reportSizeLimited = 1024 * 1024;
    String msg = generateStringOfBytes(1024);
    long totalSize = 0;
    long i = 0L;
    // This will add a report that exceeds the report size limit
    while (totalSize <= reportSizeLimited) {
      CommandStatusReportsProto.Builder reportBuilder = CommandStatusReportsProto.newBuilder();
      CommandStatus status = CommandStatus.newBuilder()
          .setCmdId(i)
          .setType(Type.mockCommand)
          .setStatus(Status.PENDING)
          .setMsg(msg)
          .setOmServiceId(omServiceId1)
          .build();
      totalSize += status.getSerializedSize();
      i++;
      reportBuilder.addCmdStatus(status);
      reportManager.addReport(reportBuilder.build());
    }

    List<Message> reports =
        reportManager.getAllAvailableReportsUpToLimit(omServiceId1, endpoint1, Integer.MAX_VALUE, reportSizeLimited);

    // Verify that the number of reports is limited to maxReportCount
    totalSize = 0;
    for (Message report : reports) {
      totalSize += report.getSerializedSize();
    }
    assertTrue(totalSize <= reportSizeLimited);

    // Verify reports are consumed
    List<Message> remainingReports =
        reportManager.getAllAvailableReportsUpToLimit(omServiceId1, endpoint1, Integer.MAX_VALUE, reportSizeLimited);
    assertFalse(remainingReports.isEmpty());
  }

  public static String generateStringOfBytes(int byteLength) {
    StringBuilder sb = new StringBuilder();
    while (sb.toString().getBytes(StandardCharsets.UTF_8).length < byteLength) {
      sb.append("a");
    }
    // Cut off the excess
    byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
    return new String(bytes, 0, byteLength, StandardCharsets.UTF_8);
  }

  @Test
  public void testReportLimitCount() {
    // Create a large number of command status reports
    int reportCount = 1000;

    // Exceed the limit of 1 report
    for (int i = 0; i < reportCount + 1; i++) {
      CommandStatusReportsProto.Builder reportBuilder = CommandStatusReportsProto.newBuilder();
      CommandStatus status = CommandStatus.newBuilder()
          .setCmdId(i + 1)
          .setType(Type.mockCommand)
          .setStatus(Status.PENDING)
          .setOmServiceId(omServiceId1)
          .build();

      reportBuilder.addCmdStatus(status);
      reportManager.addReport(reportBuilder.build());
    }

    List<Message> reports =
        reportManager.getAllAvailableReportsUpToLimit(omServiceId1, endpoint1, reportCount, Integer.MAX_VALUE);

    // Verify that the number of reports is limited to reportCount
    assertEquals(reportCount, reports.size());

    // Verify reports are consumed 1
    List<Message> remainingReports =
        reportManager.getLimitedCountAvailableReports(omServiceId1, endpoint1);
    assertEquals(1, remainingReports.size());
  }

  @Test
  public void testMultipleEndpointsForSameOMService() {
    // Register additional endpoint for the same OM service
    InetSocketAddress endpoint1b = new InetSocketAddress("om1b.example.com", 9862);
    reportManager.registerEndpoint(endpoint1b, omServiceId1);

    CommandStatus status = CommandStatus.newBuilder()
        .setCmdId(1L)
        .setType(Type.reregisterCommand)
        .setStatus(Status.SUCCEEDED)
        .setOmServiceId(omServiceId1)
        .build();

    CommandStatusReportsProto report = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(status)
        .build();

    reportManager.addReport(report);

    // Verify that both endpoints for the same service receive the report
    List<Message> reportsEndpoint1 =
        reportManager.getLimitedCountAvailableReports(omServiceId1, endpoint1);
    List<Message> reportsEndpoint1b =
        reportManager.getLimitedCountAvailableReports(omServiceId1, endpoint1b);

    assertEquals(1, reportsEndpoint1.size());
    assertEquals(1, reportsEndpoint1b.size());

    CommandStatusReportsProto report1 = (CommandStatusReportsProto) reportsEndpoint1.get(0);
    CommandStatusReportsProto report1b = (CommandStatusReportsProto) reportsEndpoint1b.get(0);

    assertEquals(1, report1.getCmdStatusCount());
    assertEquals(1, report1b.getCmdStatusCount());
    assertEquals(1L, report1.getCmdStatus(0).getCmdId());
    assertEquals(1L, report1b.getCmdStatus(0).getCmdId());
  }

  @Test
  public void testMixedReportTypes() {
    CommandStatus status1 = CommandStatus.newBuilder()
        .setCmdId(1L)
        .setType(Type.reregisterCommand)
        .setStatus(Status.SUCCEEDED)
        .setOmServiceId(omServiceId1)
        .build();

    CommandStatus status2 = CommandStatus.newBuilder()
        .setCmdId(2L)
        .setType(Type.mockCommand)
        .setStatus(Status.FAILED)
        .setOmServiceId(omServiceId2)
        .build();

    CommandStatusReportsProto report = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(status1)
        .addCmdStatus(status2)
        .build();

    // Add a non-command status report for all services
    JobworkerServiceProtocolProtos.JobworkerNodeReportProto nodeReport =
        JobworkerServiceProtocolProtos.JobworkerNodeReportProto.getDefaultInstance();

    reportManager.addReport(report);
    reportManager.addReport(nodeReport);

    // Verify that command status reports are routed correctly
    // and node reports are sent to all endpoints
    List<Message> om1Reports =
        reportManager.getLimitedCountAvailableReports(omServiceId1, endpoint1);
    List<Message> om2Reports =
        reportManager.getLimitedCountAvailableReports(omServiceId2, endpoint2);

    assertEquals(2, om1Reports.size()); // Command status + node report
    assertEquals(2, om2Reports.size()); // Command status + node report

    // Check that each list contains the right types
    boolean om1HasNodeReport = false;
    boolean om1HasCommandStatus = false;
    boolean om2HasNodeReport = false;
    boolean om2HasCommandStatus = false;

    for (Message msg : om1Reports) {
      if (msg instanceof CommandStatusReportsProto) {
        om1HasCommandStatus = true;
        CommandStatusReportsProto cmdReport = (CommandStatusReportsProto) msg;
        assertEquals(1, cmdReport.getCmdStatusCount());
        assertEquals(1L, cmdReport.getCmdStatus(0).getCmdId());
      } else {
        om1HasNodeReport = true;
      }
    }

    for (Message msg : om2Reports) {
      if (msg instanceof CommandStatusReportsProto) {
        om2HasCommandStatus = true;
        CommandStatusReportsProto cmdReport = (CommandStatusReportsProto) msg;
        assertEquals(1, cmdReport.getCmdStatusCount());
        assertEquals(2L, cmdReport.getCmdStatus(0).getCmdId());
      } else {
        om2HasNodeReport = true;
      }
    }

    assertTrue(om1HasNodeReport);
    assertTrue(om1HasCommandStatus);
    assertTrue(om2HasNodeReport);
    assertTrue(om2HasCommandStatus);
  }

  @Test
  public void testRegisterEndpoint() {
    // Unregister existing endpoints
    reportManager = JobworkerReportManager.newBuilder(conf)
        .setStateContext(mockContext)
        .addPublisherFor(CommandStatusReportsProto.class)
        .build();

    reportManager.init();

    // Register endpoints
    reportManager.registerEndpoint(endpoint1, omServiceId1);

    // Create command status report
    CommandStatus status = CommandStatus.newBuilder()
        .setCmdId(1L)
        .setType(Type.reregisterCommand)
        .setStatus(Status.SUCCEEDED)
        .setOmServiceId(omServiceId1)
        .build();

    CommandStatusReportsProto report = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(status)
        .build();

    reportManager.addReport(report);

    List<Message> reports =
        reportManager.getLimitedCountAvailableReports(omServiceId1, endpoint1);
    assertEquals(1, reports.size());

    reportManager.registerEndpoint(endpoint1, omServiceId1);
    CommandStatus status2 = CommandStatus.newBuilder()
        .setCmdId(2L)
        .setType(Type.reregisterCommand)
        .setStatus(Status.SUCCEEDED)
        .setOmServiceId(omServiceId1)
        .build();

    CommandStatusReportsProto report2 = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(status2)
        .build();

    reportManager.addReport(report2);

    // Verify the report is available (only one copy, not duplicated)
    reports = reportManager.getLimitedCountAvailableReports(omServiceId1, endpoint1);

    assertEquals(1, reports.size());
    CommandStatusReportsProto retrievedReport = (CommandStatusReportsProto) reports.get(0);
    assertEquals(1, retrievedReport.getCmdStatusCount());
    assertEquals(2L, retrievedReport.getCmdStatus(0).getCmdId());
  }

  @Test
  public void testReportPublisher() {
    // Create a publisher
    JobworkerCommandStatusReportPublisher publisher = new JobworkerCommandStatusReportPublisher();
    publisher.setConf(conf);

    // Initialize with mock objects
    publisher.init(mockContext, mock(ScheduledExecutorService.class), reportManager);
    JobworkerCommandManager jobworkerCommandManager = new JobworkerCommandManager(conf);
    when(mockContext.getCommandManager()).thenReturn(jobworkerCommandManager);

    // Create a command status for each service
    JobworkerCommand command1 = new MockJobworkerCommand(1L, omServiceId1, 1L, 0L, Type.mockCommand);
    JobworkerCommand command2 = new MockJobworkerCommand(2L, omServiceId2, 1L, 0L, Type.mockCommand);
    jobworkerCommandManager.addCommand(command1);
    jobworkerCommandManager.addCommand(command2);

    publisher.run();

    // Verify reports were sent to the correct endpoints
    List<Message> om1Reports =
        reportManager.getLimitedCountAvailableReports(omServiceId1, endpoint1);
    List<Message> om2Reports =
        reportManager.getLimitedCountAvailableReports(omServiceId2, endpoint2);
    assertEquals(1, om1Reports.size());
    assertEquals(1, om2Reports.size());

    CommandStatusReportsProto report1 = (CommandStatusReportsProto) om1Reports.get(0);
    CommandStatusReportsProto report2 = (CommandStatusReportsProto) om2Reports.get(0);

    assertEquals(1, report1.getCmdStatusCount());
    assertEquals(1, report2.getCmdStatusCount());

    CommandStatus commandStatus1 = report1.getCmdStatus(0);
    CommandStatus commandStatus2 = report2.getCmdStatus(0);

    assertEquals(1L, commandStatus1.getCmdId());
    assertEquals(2L, commandStatus2.getCmdId());

    assertEquals(omServiceId1, commandStatus1.getOmServiceId());
    assertEquals(omServiceId2, commandStatus2.getOmServiceId());
  }
}
