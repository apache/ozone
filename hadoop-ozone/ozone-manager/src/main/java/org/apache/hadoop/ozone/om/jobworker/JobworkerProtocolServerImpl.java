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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerMockCommandProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerReregisterCommandProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto.Type;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerDetailsProto;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerCommand;
import org.apache.hadoop.ozone.jobworker.protocol.JobworkerProtocol;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeManager;
import org.apache.hadoop.ozone.util.ProtobufUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server-side implementation of the Jobworker protocol that processes client requests.
 */

public class JobworkerProtocolServerImpl implements JobworkerProtocol {

  private static final Logger LOG = LoggerFactory.getLogger(
      JobworkerProtocolServerImpl.class);
  private static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.OMLOGGER);

  private final OzoneManager om;
  private final JobworkerHeartbeatDispatcher jobworkerHeartbeatDispatcher;

  private final JobworkerNodeManager jobworkerNodeManager;
  private final OMJobworkerLeaderTracker leaderTracker;

  public JobworkerProtocolServerImpl(OzoneManager om, JobworkerNodeManager jobworkerNodeManager,
                                     EventPublisher eventPublisher, OMJobworkerLeaderTracker leaderTracker) {
    this.om = om;
    this.jobworkerNodeManager = jobworkerNodeManager;
    this.leaderTracker = leaderTracker;
    jobworkerHeartbeatDispatcher =
        new JobworkerHeartbeatDispatcher(jobworkerNodeManager, eventPublisher);
  }

  @Override
  public GetOMVersionResponse getOMVersion(GetOMVersionRequest getOMVersionRequest)
      throws IOException {
    return om.getJobworkerNodemanager().getVersion(getOMVersionRequest);
  }

  @Override
  public RegisterJobworkerResponse registerJobworker(
      RegisterJobworkerRequest registerJobworkerRequest) throws IOException {
    JobworkerDetails jobworkerDetails =
        JobworkerDetails.getFromProtoBuf(registerJobworkerRequest.getExtendedJobWorkDetailsProto());
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("JobworkerDetails", jobworkerDetails.toString());
    try {
      RegisterJobworkerResponse response =
          jobworkerNodeManager.registerJobworker(jobworkerDetails);
      AUDIT.logWriteSuccess(om.buildAuditMessageForSuccess(OMAction.JW_REGISTER, auditMap));
      return response;
    } catch (Exception ex) {
      AUDIT.logWriteFailure(om.buildAuditMessageForFailure(OMAction.JW_REGISTER, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public SendHeartbeatResponseProto sendHeartbeat(SendHeartbeatRequest sendHeartbeatRequest) throws IOException {
    List<OMJobworkerCommandProto> responseCommands = new ArrayList<>();
    Map<String, String> auditMap = Maps.newHashMap();
    JobworkerDetailsProto jobworkerDetailProto = sendHeartbeatRequest.getJobworkerDetails();

    try {
      Preconditions.checkState(sendHeartbeatRequest.getOmServiceId().equals(om.getOMServiceId()),
          String.format("OM received a heartbeat with mismatched OMServiceId: expected=%s, actual=%s.",
              om.getOMServiceId(), sendHeartbeatRequest.getOmServiceId()));
      List<OMJobworkerCommand> commands = jobworkerHeartbeatDispatcher.dispatch(sendHeartbeatRequest);
      // Only the ready leader hands out work. Re-registration is safe from any OM.
      boolean leaderReady = leaderTracker.isLeaderReady();
      for (OMJobworkerCommand command : commands) {
        if (leaderReady || command.getType() == Type.reregisterCommand) {
          responseCommands.add(getCommandResponse(command));
        }
      }
      final OptionalLong term = leaderTracker.getTermIfLeader();
      auditMap.put("JobworkerUUID", ProtobufUtils.fromProtobuf(jobworkerDetailProto.getUuid128()).toString());
      auditMap.put("JobworkerHostname", jobworkerDetailProto.getHostName());
      term.ifPresent(t -> auditMap.put("Term", String.valueOf(t)));
      SendHeartbeatResponseProto.Builder builder =
          SendHeartbeatResponseProto.newBuilder()
              .setJobworkerUUID(sendHeartbeatRequest.getJobworkerDetails().getUuid128())
              .setOmServiceId(om.getOMServiceId())
              .addAllCommands(responseCommands);
      term.ifPresent(builder::setTerm);
      if (LOG.isDebugEnabled()) {
        StringBuilder sb = new StringBuilder();
        commands.forEach(command -> sb.append(command.getType()).append(", "));
        LOG.debug("Sending heartbeat {} to {}: ", sb,
            ProtobufUtils.fromProtobuf(jobworkerDetailProto.getUuid128()));
      }
      AUDIT.logWriteSuccess(om.buildAuditMessageForSuccess(OMAction.JW_HEARTBEAT, auditMap));
      return builder.build();
    } catch (Exception ex) {
      AUDIT.logWriteFailure(om.buildAuditMessageForFailure(OMAction.JW_HEARTBEAT, auditMap, ex));
      throw ex;
    }
  }

  public static OMJobworkerCommandProto getCommandResponse(OMJobworkerCommand command) {
    if (command.getProto() instanceof OMJobworkerCommandProto) {
      OMJobworkerCommandProto proto = (OMJobworkerCommandProto) command.getProto();
      return command.getTerm() > 0 ? proto.toBuilder().setTerm(command.getTerm()).build() : proto;
    }
    OMJobworkerCommandProto.Builder builder =
        OMJobworkerCommandProto
            .newBuilder()
            .setExpirationTimestampMs(command.getExpirationTimestampMs());
    if (command.getTerm() > 0) {
      builder.setTerm(command.getTerm());
    }

    switch (command.getType()) {
    case reregisterCommand:
      return builder
          .setCommandType(Type.reregisterCommand)
          .setJobworkerReregisterCommandProto(JobworkerReregisterCommandProto.getDefaultInstance())
          .build();
    case mockCommand: // This is only for test
      return builder
          .setCommandType(Type.mockCommand)
          .setJobworkerMockCommandProto(JobworkerMockCommandProto.getDefaultInstance())
          .build();

    case unknownCommand:
      throw new IllegalArgumentException("Unknown OMJobworker command");
    default:
      throw new IllegalArgumentException("OMJobworker command " + command.getType() + " is not implemented");
    }
  }

  @Override
  public void close() throws IOException {
  }
}
