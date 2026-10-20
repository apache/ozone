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

package org.apache.hadoop.ozone.jobworker.states.endpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerReregisterCommandProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatResponseProto;
import org.apache.hadoop.ozone.jobworker.JobworkerConfiguration;
import org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine;
import org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine.EndpointStates;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.ozone.jobworker.JobworkerStateMachine;
import org.apache.hadoop.ozone.jobworker.JobworkerStates;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommandManager;
import org.apache.hadoop.ozone.jobworker.protocol.JobworkerProtocol;
import org.apache.hadoop.ozone.jobworker.report.JobworkerReportManager;
import org.apache.hadoop.ozone.util.ProtobufUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * This class tests the functionality of Jobworker HeartbeatEndpointTask.
 */
public class TestHeartbeatEndpointTask {

  private static final InetSocketAddress TEST_OM_ENDPOINT =
      new InetSocketAddress("test-om-1", 9862);

  private JobworkerDetails jobworkerDetails;
  private OzoneConfiguration conf;
  private JobworkerStateContext context;
  private static final String OM_SERVICE_ID = "om-service-1";

  @BeforeEach
  public void setup() {
    jobworkerDetails = MockJobworkerDetails.randomJobworkerDetails();
    conf = new OzoneConfiguration();
    JobworkerConfiguration jwConf = new JobworkerConfiguration();
    jwConf.setHeartbeatInterval(Duration.of(1, ChronoUnit.SECONDS));
    conf.setFromObject(jwConf);
    JobworkerReportManager jobworkerReportManager = mock(JobworkerReportManager.class);
    JobworkerStateMachine stateMachine = mock(JobworkerStateMachine.class);
    when(jobworkerReportManager.getLimitedCountAvailableReports(any(), any())).thenReturn(new ArrayList<>());
    when(stateMachine.getReportManager()).thenReturn(jobworkerReportManager);
    JobworkerCommandManager commandManager = new JobworkerCommandManager(conf);
    context = spy(new JobworkerStateContext(conf, JobworkerStates.RUNNING,
        jobworkerDetails, "jobworker-test-", stateMachine, commandManager));
  }

  @Test
  public void testHeartbeatWithoutCommands() throws Exception {
    // GIVEN
    JobworkerProtocol protocol = mock(JobworkerProtocol.class);
    ArgumentCaptor<SendHeartbeatRequest> argument =
        ArgumentCaptor.forClass(SendHeartbeatRequest.class);
    when(protocol.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation -> {
          SendHeartbeatRequest request =
              (SendHeartbeatRequest)invocation.getArgument(0);
          return SendHeartbeatResponseProto.newBuilder()
              .setJobworkerUUID(request.getJobworkerDetails().getUuid128())
              .setOmServiceId(request.getOmServiceId())
              .build();
        });
    JobworkerEndpointStateMachine endpointStateMachine =
        new JobworkerEndpointStateMachine(TEST_OM_ENDPOINT, protocol, conf,
            "test-", OM_SERVICE_ID);
    endpointStateMachine.setOmServiceId(OM_SERVICE_ID);
    endpointStateMachine.setState(EndpointStates.HEARTBEAT);

    HeartbeatEndpointTask endpointTask = getHeartbeatEndpointTask(
        context, endpointStateMachine);

    // WHEN
    endpointTask.call();

    // THEN
    SendHeartbeatRequest heartbeat = argument.getValue();
    assertNotNull(heartbeat);
    assertTrue(heartbeat.hasJobworkerDetails());
    assertEquals(OM_SERVICE_ID, heartbeat.getOmServiceId());
    verify(protocol).sendHeartbeat(any(SendHeartbeatRequest.class));
  }

  @Test
  public void testHeartbeatWithReregisterCommand() throws Exception {
    // GIVEN
    JobworkerProtocol protocol = mock(JobworkerProtocol.class);
    ArgumentCaptor<SendHeartbeatRequest> argument =
        ArgumentCaptor.forClass(SendHeartbeatRequest.class);
    when(protocol.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation -> {
          SendHeartbeatRequest request =
              (SendHeartbeatRequest)invocation.getArgument(0);
          return SendHeartbeatResponseProto.newBuilder()
              .setJobworkerUUID(request.getJobworkerDetails().getUuid128())
              .setOmServiceId(request.getOmServiceId())
              .addCommands(OMJobworkerCommandProto.newBuilder()
                  .setCommandType(OMJobworkerCommandProto.Type.reregisterCommand)
                  .setJobworkerReregisterCommandProto(JobworkerReregisterCommandProto.getDefaultInstance())
                  .build())
              .build();
        });

    JobworkerEndpointStateMachine endpointStateMachine =
        new JobworkerEndpointStateMachine(TEST_OM_ENDPOINT, protocol, conf,
            "test-", OM_SERVICE_ID);
    endpointStateMachine.setState(EndpointStates.HEARTBEAT);
    endpointStateMachine.setOmServiceId(OM_SERVICE_ID);

    HeartbeatEndpointTask endpointTask = getHeartbeatEndpointTask(
        context, endpointStateMachine);

    // WHEN
    endpointTask.call();

    // THEN
    SendHeartbeatRequest heartbeat = argument.getValue();
    assertNotNull(heartbeat);
    assertEquals(OM_SERVICE_ID, heartbeat.getOmServiceId());

    // The state should be changed to GETVERSION after receiving reregister command
    assertEquals(EndpointStates.GETVERSION, endpointStateMachine.getState());

    // And the heartbeat frequency should be changed
    assertTrue(context.getHeartbeatFrequencyMs() > 0);
  }

  @Test
  public void testHeartbeatWithServiceIdMismatch() throws Exception {
    // GIVEN
    JobworkerProtocol protocol = mock(JobworkerProtocol.class);
    ArgumentCaptor<SendHeartbeatRequest> argument =
        ArgumentCaptor.forClass(SendHeartbeatRequest.class);

    when(protocol.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation -> {
          SendHeartbeatRequest request =
              (SendHeartbeatRequest)invocation.getArgument(0);
          return SendHeartbeatResponseProto.newBuilder()
              .setJobworkerUUID(request.getJobworkerDetails().getUuid128())
              .setOmServiceId("wrong-service-id") // Different from what we expect
              .build();
        });

    JobworkerEndpointStateMachine endpointStateMachine =
        new JobworkerEndpointStateMachine(TEST_OM_ENDPOINT, protocol, conf,
            "test-", OM_SERVICE_ID);
    endpointStateMachine.setOmServiceId(OM_SERVICE_ID);
    endpointStateMachine.setState(EndpointStates.HEARTBEAT);

    HeartbeatEndpointTask endpointTask = getHeartbeatEndpointTask(
        context, endpointStateMachine);

    // WHEN - this should throw an IllegalStateException but it's caught in the task
    endpointTask.call();

    // THEN - the endpoint should still be in HEARTBEAT state (no state change)
    assertEquals(EndpointStates.HEARTBEAT, endpointStateMachine.getState());
  }

  @Test
  public void testHeartbeatWithUUIDMismatch() throws Exception {
    // GIVEN
    JobworkerProtocol protocol = mock(JobworkerProtocol.class);
    ArgumentCaptor<SendHeartbeatRequest> argument =
        ArgumentCaptor.forClass(SendHeartbeatRequest.class);

    UUID differentUUID = UUID.randomUUID();
    when(protocol.sendHeartbeat(argument.capture()))
        .thenAnswer(invocation -> {
          SendHeartbeatRequest request =
              (SendHeartbeatRequest)invocation.getArgument(0);
          return SendHeartbeatResponseProto.newBuilder()
              .setJobworkerUUID(ProtobufUtils.toProtobuf(differentUUID)) // Different UUID
              .setOmServiceId(request.getOmServiceId())
              .build();
        });

    JobworkerEndpointStateMachine endpointStateMachine =
        new JobworkerEndpointStateMachine(TEST_OM_ENDPOINT, protocol, conf,
            "test-", OM_SERVICE_ID);
    endpointStateMachine.setOmServiceId(OM_SERVICE_ID);
    endpointStateMachine.setState(EndpointStates.HEARTBEAT);

    HeartbeatEndpointTask endpointTask = getHeartbeatEndpointTask(
        context, endpointStateMachine);
    // WHEN - this should throw an IllegalStateException but it's caught in the task
    endpointTask.call();
    // THEN - the endpoint should still be in HEARTBEAT state (no state change)
    assertEquals(EndpointStates.HEARTBEAT, endpointStateMachine.getState());
  }

  @Test
  public void testHeartbeatWithTerm() throws Exception {
    // GIVEN
    JobworkerProtocol protocol = mock(JobworkerProtocol.class);
    ArgumentCaptor<SendHeartbeatRequest> heartbeatCaptor =
        ArgumentCaptor.forClass(SendHeartbeatRequest.class);

    final long termInOM = 42L;
    Mockito.when(protocol.sendHeartbeat(heartbeatCaptor.capture()))
        .thenReturn(SendHeartbeatResponseProto.newBuilder()
            .setJobworkerUUID(ProtobufUtils.toProtobuf(jobworkerDetails.getUuid()))
            .setTerm(termInOM)
            .setOmServiceId(OM_SERVICE_ID)
            .build());

    JobworkerEndpointStateMachine endpointStateMachine =
        new JobworkerEndpointStateMachine(TEST_OM_ENDPOINT, protocol, conf,
            "test-", OM_SERVICE_ID);
    endpointStateMachine.setOmServiceId(OM_SERVICE_ID);
    endpointStateMachine.setState(EndpointStates.HEARTBEAT);

    // Run heartbeat task
    HeartbeatEndpointTask task = getHeartbeatEndpointTask(context, endpointStateMachine);
    task.call();

    // Verify term update in context
    Mockito.verify(context).updateTermOfLeaderOM(OM_SERVICE_ID, termInOM);
    assertEquals(termInOM,
        context.getCommandManager().getTermOfLeaderOMByServiceId(OM_SERVICE_ID).orElse(-1));
  }

  private HeartbeatEndpointTask getHeartbeatEndpointTask(
      JobworkerStateContext stateContext,
      JobworkerEndpointStateMachine endpointStateMachine) {
    return HeartbeatEndpointTask.newBuilder()
        .setContext(stateContext)
        .setJobworkerDetails(jobworkerDetails)
        .setEndpointStateMachine(endpointStateMachine)
        .build();
  }
}
