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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.om.jobworker.JobworkerHeartbeatDispatcher.CommandStatusReportFromJobworker;
import org.junit.jupiter.api.Test;

/**
 * Tests that only the ready OM leader processes command status reports.
 */
class TestJobworkerCommandStatusReportHandler {

  @Test
  void testReportIsOnlyProcessedOnTheReadyLeader() {
    OMJobworkerCommandManager commandManager = mock(OMJobworkerCommandManager.class);
    AtomicBoolean leaderReady = new AtomicBoolean(false);
    JobworkerCommandStatusReportHandler handler =
        new JobworkerCommandStatusReportHandler(commandManager, leaderReady::get);
    JobworkerDetails worker = MockJobworkerDetails.randomJobworkerDetails();
    CommandStatus status = CommandStatus.newBuilder()
        .setCmdId(1L)
        .setType(OMJobworkerCommandProto.Type.mockCommand)
        .setStatus(CommandStatus.Status.SUCCEEDED)
        .setOmServiceId("om-service")
        .build();
    CommandStatusReportFromJobworker report = new CommandStatusReportFromJobworker(worker,
        CommandStatusReportsProto.newBuilder().addCmdStatus(status).build());

    handler.onMessage(report, mock(EventPublisher.class));
    verify(commandManager, never()).processStatusUpdate(any(), any());

    leaderReady.set(true);
    handler.onMessage(report, mock(EventPublisher.class));
    verify(commandManager).processStatusUpdate(worker, status);
  }
}
