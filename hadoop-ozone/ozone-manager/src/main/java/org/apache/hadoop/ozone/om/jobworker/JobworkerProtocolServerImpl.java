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
import java.util.UUID;
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
import org.apache.hadoop.ozone.om.jobworker.command.OMJobworkerJobManager;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeManager;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.util.ProtobufUtils;
import org.apache.ratis.server.DivisionInfo;
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

  public JobworkerProtocolServerImpl(OzoneManager om, JobworkerNodeManager jobworkerNodeManager,
                                     EventPublisher eventPublisher) {
    this.om = om;
    this.jobworkerNodeManager = jobworkerNodeManager;
    jobworkerHeartbeatDispatcher =
        new JobworkerHeartbeatDispatcher(jobworkerNodeManager, eventPublisher);
  }

  @Override
  public GetOMVersionResponse getOMVersion(GetOMVersionRequest getOMVersionRequest)
      throws IOException {
    GetOMVersionResponse version = om.getJobworkerNodemanager().getVersion(getOMVersionRequest);
    if (om.getJobworkerJobManager() == null) {
      return version;
    }
    return version.toBuilder()
        .setOmNodeId(om.getOMNodeId())
        .setDurableJobProtocolVersion(om.getVersionManager().isAllowed(
            OMLayoutFeature.JOBWORKER_HA) ? 1 : 0).build();
  }

  @Override
  public RegisterJobworkerResponse registerJobworker(
      RegisterJobworkerRequest registerJobworkerRequest) throws IOException {
    if (om.getJobworkerJobManager() != null && registerJobworkerRequest.getDurableJobProtocolVersion() != 1) {
      throw new IOException("Jobworker must support durable assignment and result acknowledgement protocol version 1");
    }
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
      OMJobworkerJobManager jobs = om.getJobworkerJobManager();
      List<OMJobworkerCommand> commands = jobworkerHeartbeatDispatcher.dispatch(jobs == null
          ? sendHeartbeatRequest : sendHeartbeatRequest.toBuilder().clearCommandStatusReports().build());
      for (OMJobworkerCommand command : commands) {
        responseCommands.add(getCommandResponse(command));
      }
      final OptionalLong term = getTermIfLeader();
      auditMap.put("JobworkerUUID", ProtobufUtils.fromProtobuf(jobworkerDetailProto.getUuid128()).toString());
      auditMap.put("JobworkerHostname", jobworkerDetailProto.getHostName());
      term.ifPresent(t -> auditMap.put("Term", String.valueOf(t)));
      SendHeartbeatResponseProto.Builder builder =
          SendHeartbeatResponseProto.newBuilder()
              .setJobworkerUUID(sendHeartbeatRequest.getJobworkerDetails().getUuid128())
              .setOmServiceId(om.getOMServiceId())
              .addAllCommands(responseCommands);
      term.ifPresent(builder::setTerm);
      if (jobs != null && jobs.isAvailable()
          && jobworkerNodeManager.isJobworkerNodeRegistered(
              ProtobufUtils.fromProtobuf(jobworkerDetailProto.getUuid128()))) {
        UUID workerId = ProtobufUtils.fromProtobuf(jobworkerDetailProto.getUuid128());
        builder.addAllCommittedResults(jobs.reconcile(JobworkerDetails.getFromProtoBuf(jobworkerDetailProto),
            sendHeartbeatRequest.getCommandStatusReportsList()));
        builder.addAllCommands(jobs.commandsFor(workerId));
      }
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
      return (OMJobworkerCommandProto) command.getProto();
    }
    OMJobworkerCommandProto.Builder builder =
        OMJobworkerCommandProto
            .newBuilder()
            .setExpirationTimestampMs(command.getExpirationTimestampMs());

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

  private OptionalLong getTermIfLeader() {
    if (om != null &&  om.getOmRatisServer() != null) {
      try {
        DivisionInfo divisionInfo = om.getOmRatisServer().getServerDivision().getInfo();
        if (divisionInfo.isLeader()) {
          return OptionalLong.of(divisionInfo.getCurrentTerm());
        }
      } catch (Exception e) {
        LOG.debug("Exception when getting leader current term ", e);
      }
    }
    return OptionalLong.empty();
  }

  @Override
  public void close() throws IOException {
  }
}
