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

package org.apache.hadoop.ozone.om.jobworker;

import static org.apache.hadoop.ozone.om.jobworker.OMJobworkerEvents.JW_COMMAND_STATUS_REPORT;
import static org.apache.hadoop.ozone.om.jobworker.OMJobworkerEvents.JW_NODE_REPORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerNodeReportProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerStorageReportProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatRequest;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerDetailsProto;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerReregisterCommand;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerCommand;
import org.apache.hadoop.ozone.om.jobworker.JobworkerHeartbeatDispatcher.NodeReportFromJobworker;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Tests for {@link JobworkerHeartbeatDispatcher}.
 */
public class TestJobworkerHeartbeatDispatcher {

  private JobworkerNodeManager mockNodeManager;
  private EventPublisher mockEventPublisher;
  private JobworkerHeartbeatDispatcher dispatcher;
  private UUID jobworkerUuid;
  private static final String OM_SERVICE_ID_1 = "om-service-1";

  @BeforeEach
  public void setup() {
    mockNodeManager = Mockito.mock(JobworkerNodeManager.class);
    mockEventPublisher = Mockito.mock(EventPublisher.class);
    dispatcher = new JobworkerHeartbeatDispatcher(mockNodeManager, mockEventPublisher);
    jobworkerUuid = UUID.randomUUID();
  }

  /**
   * Test that when a registered jobworker sends a heartbeat, the node manager
   * processes the heartbeat and fires event for any node reports.
   */
  @Test
  public void testHeartbeatFromRegisteredJobworker() {
    // GIVEN
    JobworkerDetailsProto jobworkerProto =
        MockJobworkerDetails.createJobworkerDetails(jobworkerUuid.toString()).getProtoBufMessage();
    JobworkerDetails jobworkerDetails = JobworkerDetails.getFromProtoBuf(jobworkerProto);
    JobworkerNodeReportProto nodeReport = createNodeReportProto();
    SendHeartbeatRequest heartbeat = createHeartbeatRequest(jobworkerProto, nodeReport);
    when(mockNodeManager.isJobworkerNodeRegistered(jobworkerUuid)).thenReturn(true);
    List<OMJobworkerCommand> commands = new ArrayList<>();
    when(mockNodeManager.pollJobworkerCommand(jobworkerUuid)).thenReturn(commands);

    // WHEN
    List<OMJobworkerCommand> returnedCommands = dispatcher.dispatch(heartbeat);
    verify(mockNodeManager).isJobworkerNodeRegistered(jobworkerUuid);
    verify(mockNodeManager).processHeartbeat(jobworkerDetails);
    verify(mockNodeManager).pollJobworkerCommand(jobworkerUuid);

    // Verify the event was fired for the node report
    ArgumentCaptor<NodeReportFromJobworker> nodeReportCaptor =
        ArgumentCaptor.forClass(NodeReportFromJobworker.class);
    verify(mockEventPublisher).fireEvent(
        Mockito.eq(JW_NODE_REPORT), nodeReportCaptor.capture());

    NodeReportFromJobworker capturedReport = nodeReportCaptor.getValue();
    assertEquals(jobworkerDetails, capturedReport.getJobworkerDetails());
    assertEquals(nodeReport, capturedReport.getReport());

    // Verify we return the commands
    assertEquals(commands, returnedCommands);
  }

  @Test
  public void testHeartbeatFromUnregisteredJobworker() {
    // GIVEN
    JobworkerDetailsProto jobworkerProto =
        MockJobworkerDetails.createJobworkerDetails(jobworkerUuid.toString()).getProtoBufMessage();
    SendHeartbeatRequest heartbeat = createHeartbeatRequest(jobworkerProto);
    when(mockNodeManager.isJobworkerNodeRegistered(jobworkerUuid)).thenReturn(false);
    List<OMJobworkerCommand> commands = new ArrayList<>();
    commands.add(new JobworkerReregisterCommand());
    when(mockNodeManager.pollJobworkerCommand(jobworkerUuid)).thenReturn(commands);

    // WHEN
    List<OMJobworkerCommand> returnedCommands = dispatcher.dispatch(heartbeat);

    // Verify nodeReport will not be processed
    verify(mockNodeManager).isJobworkerNodeRegistered(jobworkerUuid);
    verify(mockNodeManager, times(0)).processHeartbeat(any(JobworkerDetails.class));
    // Verify a reregister command was added
    ArgumentCaptor<JobworkerReregisterCommand> commandCaptor =
        ArgumentCaptor.forClass(JobworkerReregisterCommand.class);
    verify(mockNodeManager).addOMJobworkerCommand(Mockito.eq(jobworkerUuid), commandCaptor.capture());
    verify(mockNodeManager).pollJobworkerCommand(jobworkerUuid);
    assertEquals(commands, returnedCommands);
  }

  @Test
  public void testDispatchMultipleEvents() {
    AtomicInteger eventCount = new AtomicInteger(0);
    // Create a custom event publisher that counts events
    EventPublisher countingPublisher = new EventPublisher() {
      @Override
      public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void fireEvent(
          EVENT_TYPE event, PAYLOAD payload) {
        eventCount.incrementAndGet();
      }
    };
    JobworkerHeartbeatDispatcher localDispatcher =
        new JobworkerHeartbeatDispatcher(mockNodeManager, countingPublisher);
    JobworkerDetailsProto jobworkerProto =
        MockJobworkerDetails.createJobworkerDetails(jobworkerUuid.toString()).getProtoBufMessage();
    SendHeartbeatRequest heartbeat = createHeartbeatRequest(jobworkerProto);
    when(mockNodeManager.isJobworkerNodeRegistered(jobworkerUuid)).thenReturn(true);

    localDispatcher.dispatch(heartbeat);

    // Verify one NodeReport event was fired
    assertEquals(1, eventCount.get());
  }

  @Test
  public void testHeartbeatWithCommandStatusReports() {
    // GIVEN
    JobworkerDetailsProto jobworkerProto =
        MockJobworkerDetails.createJobworkerDetails(jobworkerUuid.toString()).getProtoBufMessage();
    JobworkerDetails jobworkerDetails = JobworkerDetails.getFromProtoBuf(jobworkerProto);
    JobworkerNodeReportProto nodeReport = createNodeReportProto();
    CommandStatusReportsProto statusReport = createCommandStatusReportProto();

    // Create heartbeat with both node report and command status report
    SendHeartbeatRequest heartbeat = SendHeartbeatRequest.newBuilder()
        .setJobworkerDetails(jobworkerProto)
        .setJobworkerNodeReport(nodeReport)
        .addCommandStatusReports(statusReport)
        .setOmServiceId(OM_SERVICE_ID_1)
        .build();

    when(mockNodeManager.isJobworkerNodeRegistered(jobworkerUuid)).thenReturn(true);
    List<OMJobworkerCommand> commands = new ArrayList<>();
    when(mockNodeManager.pollJobworkerCommand(jobworkerUuid)).thenReturn(commands);

    // WHEN
    List<OMJobworkerCommand> returnedCommands = dispatcher.dispatch(heartbeat);

    // THEN
    verify(mockNodeManager).isJobworkerNodeRegistered(jobworkerUuid);
    verify(mockNodeManager).processHeartbeat(jobworkerDetails);
    verify(mockNodeManager).pollJobworkerCommand(jobworkerUuid);

    // Verify the event was fired for the node report
    ArgumentCaptor<NodeReportFromJobworker> nodeReportCaptor =
        ArgumentCaptor.forClass(NodeReportFromJobworker.class);
    verify(mockEventPublisher).fireEvent(
        Mockito.eq(JW_NODE_REPORT), nodeReportCaptor.capture());

    NodeReportFromJobworker capturedNodeReport = nodeReportCaptor.getValue();
    assertEquals(jobworkerDetails, capturedNodeReport.getJobworkerDetails());
    assertEquals(nodeReport, capturedNodeReport.getReport());

    // Verify the event was fired for the command status report
    ArgumentCaptor<JobworkerHeartbeatDispatcher.CommandStatusReportFromJobworker> statusReportCaptor =
        ArgumentCaptor.forClass(JobworkerHeartbeatDispatcher.CommandStatusReportFromJobworker.class);
    verify(mockEventPublisher).fireEvent(
        Mockito.eq(JW_COMMAND_STATUS_REPORT), statusReportCaptor.capture());

    JobworkerHeartbeatDispatcher.CommandStatusReportFromJobworker capturedStatusReport = statusReportCaptor.getValue();
    assertEquals(jobworkerDetails, capturedStatusReport.getJobworkerDetails());
    assertEquals(statusReport, capturedStatusReport.getReport());
    assertEquals(commands, returnedCommands);
  }

  private JobworkerNodeReportProto createNodeReportProto() {
    JobworkerStorageReportProto storageReport = JobworkerStorageReportProto.newBuilder()
        .setStorageUuid(UUID.randomUUID().toString())
        .setStorageLocation("/data/jobworker")
        .setCapacity(1000000)
        .setRemaining(800000)
        .setFailed(false)
        .build();

    return JobworkerNodeReportProto.newBuilder()
        .addStorageReport(storageReport)
        .build();
  }

  private CommandStatusReportsProto createCommandStatusReportProto() {
    return CommandStatusReportsProto.newBuilder()
        .addCmdStatus(CommandStatus.newBuilder()
            .setCmdId(1L)
            .setType(OMJobworkerCommandProto.Type.mockCommand)
            .setStatus(CommandStatus.Status.PENDING)
            .setOmServiceId(OM_SERVICE_ID_1)
            .build()
        ).build();
  }

  private SendHeartbeatRequest createHeartbeatRequest(JobworkerDetailsProto jobworkerProto) {
    return SendHeartbeatRequest.newBuilder()
        .setJobworkerDetails(jobworkerProto)
        .setJobworkerNodeReport(createNodeReportProto())
        .setOmServiceId(OM_SERVICE_ID_1)
        .build();
  }

  private SendHeartbeatRequest createHeartbeatRequest(JobworkerDetailsProto jobworkerProto,
                                                      JobworkerNodeReportProto nodeReportProto) {
    return SendHeartbeatRequest.newBuilder()
        .setJobworkerDetails(jobworkerProto)
        .setJobworkerNodeReport(nodeReportProto)
        .setOmServiceId(OM_SERVICE_ID_1)
        .build();
  }
}
