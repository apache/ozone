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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.closeContainerCommand;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.closePipelineCommand;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.createPipelineCommand;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.deleteBlocksCommand;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.deleteContainerCommand;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.finalizeNewDatanodeVersionCommand;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.reconcileContainerCommand;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.reconstructECContainersCommand;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.refreshVolumeUsageInfo;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.replicateContainerCommand;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.reregisterCommand;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.setNodeOperationalStateCommand;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_READ_THREADPOOL_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_READ_THREADPOOL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.CONTAINER_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.PIPELINE_REPORT;
import static org.apache.hadoop.hdds.scm.server.StorageContainerManager.startRpcServer;
import static org.apache.hadoop.hdds.server.ServerUtils.getRemoteUserName;
import static org.apache.hadoop.hdds.server.ServerUtils.updateRPCListenAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.protobuf.BlockingService;
import com.google.protobuf.TextFormat;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DatanodeVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.FullContainerReportLeaseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReconstructECContainersCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReregisterCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;
import org.apache.hadoop.hdds.ratis.ServerNotLeaderException;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMFullContainerReportLeaseManager.LeaseClaim;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.Auditor;
import org.apache.hadoop.ozone.audit.SCMAction;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ClosePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.CreatePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.FinalizeVersionCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.RefreshVolumeUsageCommand;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.commands.SetNodeOperationalStateCommand;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Protocol handler for datanode registration and heartbeats.
 *
 * <p>The SCM FCR lease flow is:
 * <ol>
 * <li>Registration supplies an actual FCR or records that it is deferred. Successful node registration is notified
 * independently of whether its registration FCR has been processed.</li>
 * <li>A heartbeat requests a grant from {@link SCMFullContainerReportLeaseManager}, when allowed by SCM leadership
 * and upgrade state.</li>
 * <li>An incoming leased FCR is validated and claimed before dispatch; unleased reports retain legacy behavior.
 * Invalid leased FCRs are rejected without suppressing other heartbeat reports.</li>
 * <li>The dispatcher transfers an accepted report to the queue. Processing or discard completes its claim and releases
 * capacity; container SafeMode rules receive container IDs only after successful registration FCR processing.</li>
 * </ol>
 */
public class SCMDatanodeProtocolServer implements
    StorageContainerDatanodeProtocol, Auditor {

  private static final Logger LOG = LoggerFactory.getLogger(
      SCMDatanodeProtocolServer.class);

  private static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.SCMLOGGER);

  /**
   * The RPC server that listens to requests from DataNodes.
   */
  private RPC.Server datanodeRpcServer;

  private final OzoneStorageContainerManager scm;
  private final InetSocketAddress datanodeRpcAddress;
  private final SCMDatanodeHeartbeatDispatcher heartbeatDispatcher;
  private final EventPublisher eventPublisher;
  // Owned by SCM; null for Recon, which does not participate in FCR leasing.
  @Nullable
  private final SCMFullContainerReportLeaseManager fcrLeaseManager;
  private ProtocolMessageMetrics<StorageContainerDatanodeProtocolProtos.Type> protocolMessageMetrics;

  private final SCMContext scmContext;

  public SCMDatanodeProtocolServer(final OzoneConfiguration conf,
                                   OzoneStorageContainerManager scm,
                                   EventPublisher eventPublisher,
                                   SCMContext scmContext,
                                   SCMFullContainerReportLeaseManager fcrLeaseManager)
      throws IOException {

    // This constructor has broken down to smaller methods so that Recon's
    // passive SCM server can override them.
    Objects.requireNonNull(scm, "SCM cannot be null");
    Objects.requireNonNull(eventPublisher, "EventPublisher cannot be null");

    this.scm = scm;
    this.eventPublisher = eventPublisher;
    this.scmContext = scmContext;
    this.fcrLeaseManager = scmContext == null ? fcrLeaseManager
        : Objects.requireNonNull(fcrLeaseManager, "FCR lease manager cannot be null for SCM");

    heartbeatDispatcher = new SCMDatanodeHeartbeatDispatcher(
        scm.getScmNodeManager(), eventPublisher);

    InetSocketAddress datanodeRpcAddr = getDataNodeBindAddress(
        conf, scm.getScmNodeDetails());

    protocolMessageMetrics = getProtocolMessageMetrics();
    final int handlerCount = conf.getInt(OZONE_SCM_DATANODE_HANDLER_COUNT_KEY,
        OZONE_SCM_HANDLER_COUNT_KEY, OZONE_SCM_HANDLER_COUNT_DEFAULT,
            LOG::info);
    final int readThreads = conf.getInt(OZONE_SCM_DATANODE_READ_THREADPOOL_KEY,
        OZONE_SCM_DATANODE_READ_THREADPOOL_DEFAULT);

    RPC.setProtocolEngine(conf, getProtocolClass(), ProtobufRpcEngine.class);

    BlockingService dnProtoPbService =
        StorageContainerDatanodeProtocolProtos
            .StorageContainerDatanodeProtocolService
            .newReflectiveBlockingService(
                new StorageContainerDatanodeProtocolServerSideTranslatorPB(
                    this, protocolMessageMetrics));

    datanodeRpcServer =  startRpcServer(
        conf,
        datanodeRpcAddr,
        getProtocolClass(),
        dnProtoPbService,
        handlerCount,
        readThreads);

    datanodeRpcAddress = updateRPCListenAddress(
            conf, getDatanodeAddressKey(), datanodeRpcAddr,
            datanodeRpcServer);

    if (conf.getBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      datanodeRpcServer.refreshServiceAcl(conf, getPolicyProvider());
    }

    HddsServerUtil.addSuppressedLoggingExceptions(datanodeRpcServer);
  }

  public void start() {
    LOG.info(
        StorageContainerManager.buildRpcServerStartMessage(
            "ScmDatanodeProtocol RPC server for DataNodes",
            datanodeRpcAddress));
    protocolMessageMetrics.register();
    datanodeRpcServer.start();
  }

  public InetSocketAddress getDatanodeRpcAddress() {
    return datanodeRpcAddress;
  }

  @Override
  public SCMVersionResponseProto getVersion(SCMVersionRequestProto
      versionRequest)
      throws IOException {
    boolean auditSuccess = true;
    try {
      return scm.getScmNodeManager().getVersion(versionRequest)
              .getProtobufMessage();
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(SCMAction.GET_VERSION, null, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.GET_VERSION, null));
      }
    }
  }

  @Override
  public SCMRegisteredResponseProto register(
      HddsProtos.ExtendedDatanodeDetailsProto extendedDatanodeDetailsProto,
      NodeReportProto nodeReport,
      ContainerReportsProto containerReportsProto,
      PipelineReportsProto pipelineReportsProto,
      DatanodeVersionProto versionInfo)
      throws IOException {
    DatanodeDetails datanodeDetails = DatanodeDetails
        .getFromProtoBuf(extendedDatanodeDetailsProto);
    boolean auditSuccess = true;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("datanodeDetails", datanodeDetails.toString());

    // Read before the decision, so that a rejection made before this SCM became the ready leader is not final.
    final boolean leaderReady = scmContext == null || scmContext.isLeaderReady();
    // TODO : Return the list of Nodes that forms the SCM HA.
    RegisteredCommand registeredCommand;
    // Reject unsupported deferral before register() can change node state. Prefer the structured version error for
    // incompatible datanodes, and preserve rejection even if finalization advances during version validation.
    if (containerReportsProto.getFullContainerReportDeferred()
        && (fcrLeaseManager == null || !scm.getVersionManager().isAllowed(HDDSVersion.FULL_CONTAINER_REPORT_LEASE))) {
      registeredCommand = scm.getScmNodeManager().getRegistrationVersionError(datanodeDetails, versionInfo)
          .orElseThrow(() -> new IOException(
              "Full container report leases must be supported and finalized before deferring registration reports."));
    } else {
      registeredCommand = scm.getScmNodeManager()
          .register(datanodeDetails, nodeReport, pipelineReportsProto, versionInfo);
    }
    if (registeredCommand.getError()
        == SCMRegisteredResponseProto.ErrorCode.success) {
      // A deferred FCR does not describe an empty datanode; record it for a later leased heartbeat.
      if (containerReportsProto.getFullContainerReportDeferred()) {
        fcrLeaseManager.markFullContainerReportDeferred(datanodeDetails);
      } else {
        if (fcrLeaseManager != null) {
          fcrLeaseManager.clearFullContainerReportDeferred(datanodeDetails);
        }
        eventPublisher.fireEvent(CONTAINER_REPORT,
            new SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode(
                datanodeDetails, containerReportsProto, true));
      }
      // Count successful node registration independently of container SafeMode progress after actual FCR processing.
      eventPublisher.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
          new NodeRegistrationContainerReport(datanodeDetails));
      eventPublisher.fireEvent(PIPELINE_REPORT,
              new PipelineReportFromDatanode(datanodeDetails,
                      pipelineReportsProto));
    }
    try {
      if (registeredCommand.getError() == SCMRegisteredResponseProto.ErrorCode.errorNodeNotPermitted && !leaderReady) {
        // The datanode shuts down on this error. An SCM that is not the ready leader may not have applied the
        // latest state yet, for example a finalization the leader already sent to the datanode, so let the
        // datanode retry and leave the final decision to the leader. The RPC server does not log this exception.
        throw new ServerNotLeaderException("SCM is not the ready leader, so its rejection of datanode "
            + datanodeDetails + " is not final. The datanode should retry the registration.");
      }
      return getRegisteredResponse(registeredCommand);
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(SCMAction.REGISTER, auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logWriteSuccess(
            buildAuditMessageForSuccess(SCMAction.REGISTER, auditMap));
      }
    }
  }

  @VisibleForTesting
  public static SCMRegisteredResponseProto getRegisteredResponse(
      RegisteredCommand cmd) {
    return cmd.getProtoBufMessage();
  }

  private String constructCommandAuditMap(List<SCMCommandProto> cmds) {
    StringBuilder auditMap = new StringBuilder();
    auditMap.append('[');
    for (SCMCommandProto cmd : cmds) {
      if (cmd.getCommandType().equals(deleteBlocksCommand)) {
        auditMap.append("commandType: ").append(cmd.getCommandType())
            .append(" deleteTransactionsCount: ")
            .append(cmd.getDeleteBlocksCommandProto().getDeletedBlocksTransactionsCount())
            .append(" cmdID: ").append(cmd.getDeleteBlocksCommandProto().getCmdId())
            .append(" encodedToken: \"").append(cmd.getEncodedToken()).append('"')
            .append(" deadlineMsSinceEpoch: ").append(cmd.getDeadlineMsSinceEpoch());
      } else {
        auditMap.append(TextFormat.shortDebugString(cmd));
      }
      auditMap.append(", ");
    }
    int len = auditMap.length();
    if (len > 2) {
      auditMap.delete(len - 2, len);
    }
    auditMap.append(']');
    return auditMap.toString();
  }

  @Override
  public SCMHeartbeatResponseProto sendHeartbeat(
      SCMHeartbeatRequestProto heartbeat) throws IOException, TimeoutException {
    ContainerReportFromDatanode containerReport = null;
    boolean leasedReportRejected = false;
    if (heartbeat.hasContainerReport()) {
      DatanodeDetails datanodeDetails = DatanodeDetails.getFromProtoBuf(heartbeat.getDatanodeDetails());
      ContainerReportsProto report = heartbeat.getContainerReport();
      if (report.hasFullContainerReportLease()) {
        Optional<LeaseClaim> claim =
            validateAndClaimFullContainerReportLease(datanodeDetails, report.getFullContainerReportLease());
        if (claim.isPresent()) {
          containerReport = new ContainerReportFromDatanode(datanodeDetails, report, claim.get());
        } else {
          leasedReportRejected = true;
        }
      } else {
        containerReport = new ContainerReportFromDatanode(datanodeDetails, report);
      }
    }
    List<SCMCommandProto> cmdResponses = new ArrayList<>();
    for (SCMCommand<?> cmd : heartbeatDispatcher.dispatch(heartbeat, containerReport)) {
      cmdResponses.add(getCommandResponse(cmd, scm));
    }
    final OptionalLong term = getTermIfLeader();
    boolean auditSuccess = true;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("datanodeUUID", heartbeat.getDatanodeDetails().getUuid());
    auditMap.put("command", constructCommandAuditMap(cmdResponses));
    term.ifPresent(t -> auditMap.put("term", String.valueOf(t)));
    try {
      SCMHeartbeatResponseProto.Builder builder =
          SCMHeartbeatResponseProto.newBuilder()
              .setDatanodeUUID(heartbeat.getDatanodeDetails().getUuid())
              .addAllCommands(cmdResponses);
      term.ifPresent(builder::setTerm);
      builder.setFullContainerReportLeaseRejected(leasedReportRejected);
      addFCRLeaseIfRequested(heartbeat, builder);
      return builder.build();
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(SCMAction.SEND_HEARTBEAT, auditMap, ex)
      );
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logWriteSuccess(
            buildAuditMessageForSuccess(SCMAction.SEND_HEARTBEAT, auditMap)
        );
      }
    }
  }

  /**
   * Validates and claims the lease attached to an incoming FCR, retaining capacity until the report completes.
   *
   * @return the report's lease claim, or empty if leasing is unavailable or the grant is invalid
   */
  private Optional<LeaseClaim> validateAndClaimFullContainerReportLease(
      DatanodeDetails datanodeDetails, FullContainerReportLeaseProto lease) {
    OptionalLong currentTerm = getFCRLeaseTermIfAllowed();
    Optional<LeaseClaim> claim = Optional.empty();
    if (fcrLeaseManager != null) {
      if (currentTerm.isPresent()) {
        claim = fcrLeaseManager.claimLease(datanodeDetails, currentTerm.getAsLong(), lease.getTerm(), lease.getId());
      } else {
        fcrLeaseManager.incrementInvalidLeaseReports();
      }
    }
    if (claim.isEmpty()) {
      LOG.warn("Ignoring full container report from datanode {} with invalid "
              + "lease id {} and term {}.", datanodeDetails.getUuidString(), lease.getId(), lease.getTerm());
    }
    return claim;
  }

  private void addFCRLeaseIfRequested(SCMHeartbeatRequestProto heartbeat,
      SCMHeartbeatResponseProto.Builder builder) {
    if (fcrLeaseManager == null || !heartbeat.getRequestFullContainerReportLease()) {
      return;
    }

    OptionalLong term = getFCRLeaseTermIfAllowed();
    if (term.isEmpty()) {
      return;
    }

    DatanodeDetails datanodeDetails = DatanodeDetails.getFromProtoBuf(
        heartbeat.getDatanodeDetails());
    OptionalLong leaseId = fcrLeaseManager.requestLease(datanodeDetails,
        term.getAsLong());
    if (leaseId.isPresent()) {
      builder.setFullContainerReportLease(
          FullContainerReportLeaseProto.newBuilder()
              .setId(leaseId.getAsLong())
              .setTerm(term.getAsLong()));
    }
  }

  /**
   * Selects the term used to grant leases and validate incoming FCR leases. Capacity-limit bypass does not affect
   * these SCM leadership and upgrade checks. This is not necessarily the leader term attached to SCM commands.
   *
   * <ul>
   * <li>Recon (no SCM context) or an unfinalized feature: no lease term.</li>
   * <li>HA leader that is not ready: no lease term.</li>
   * <li>Ready HA leader: its current term.</li>
   * <li>HA follower or non-HA SCM: {@link SCMContext#INVALID_TERM}, identifying leases local to this SCM.</li>
   * </ul>
   */
  private OptionalLong getFCRLeaseTermIfAllowed() {
    if (scmContext == null || !scm.getVersionManager().isAllowed(HDDSVersion.FULL_CONTAINER_REPORT_LEASE)) {
      return OptionalLong.empty();
    }

    OptionalLong leaderTerm = scmContext.getTermOfLeaderIfReady();
    if (leaderTerm.isPresent()) {
      return leaderTerm;
    }
    return scmContext.isLeader()
        ? OptionalLong.empty() : OptionalLong.of(SCMContext.INVALID_TERM);
  }

  private OptionalLong getTermIfLeader() {
    if (scmContext != null && scmContext.isLeader()) {
      try {
        return OptionalLong.of(scmContext.getTermOfLeader());
      } catch (NotLeaderException e) {
        // only leader should distribute current term
      }
    }
    return OptionalLong.empty();
  }

  /**
   * Returns a SCMCommandRepose from the SCM Command.
   *
   * @param cmd - Cmd
   * @return SCMCommandResponseProto
   * @throws IOException
   */
  @VisibleForTesting
  public static SCMCommandProto getCommandResponse(SCMCommand<?> cmd,
      OzoneStorageContainerManager scm) throws IOException, TimeoutException {
    SCMCommandProto.Builder builder = SCMCommandProto.newBuilder()
        .setEncodedToken(cmd.getEncodedToken());

    // In HA mode, it is the term of current leader SCM.
    // In non-HA mode, it is the default value 0.
    builder.setTerm(cmd.getTerm());
    // The default deadline is 0, which means no deadline. Individual commands
    // may have a deadline set.
    builder.setDeadlineMsSinceEpoch(cmd.getDeadline());

    switch (cmd.getType()) {
    case reregisterCommand:
      return builder
          .setCommandType(reregisterCommand)
          .setReregisterCommandProto(ReregisterCommandProto
              .getDefaultInstance())
          .build();
    case deleteBlocksCommand:
      return builder
          .setCommandType(deleteBlocksCommand)
          .setDeleteBlocksCommandProto(
              ((DeleteBlocksCommand) cmd).getProto())
          .build();
    case closeContainerCommand:
      return builder
          .setCommandType(closeContainerCommand)
          .setCloseContainerCommandProto(
              ((CloseContainerCommand) cmd).getProto())
          .build();
    case deleteContainerCommand:
      return builder.setCommandType(deleteContainerCommand)
          .setDeleteContainerCommandProto(
              ((DeleteContainerCommand) cmd).getProto())
          .build();
    case replicateContainerCommand:
      return builder
          .setCommandType(replicateContainerCommand)
          .setReplicateContainerCommandProto(
              ((ReplicateContainerCommand)cmd).getProto())
          .build();
    case reconstructECContainersCommand:
      return builder
          .setCommandType(reconstructECContainersCommand)
          .setReconstructECContainersCommandProto(
              (ReconstructECContainersCommandProto) cmd.getProto())
          .build();
    case createPipelineCommand:
      return builder
          .setCommandType(createPipelineCommand)
          .setCreatePipelineCommandProto(
              ((CreatePipelineCommand)cmd).getProto())
          .build();
    case closePipelineCommand:
      return builder
          .setCommandType(closePipelineCommand)
          .setClosePipelineCommandProto(
              ((ClosePipelineCommand)cmd).getProto())
          .build();
    case setNodeOperationalStateCommand:
      return builder
            .setCommandType(setNodeOperationalStateCommand)
            .setSetNodeOperationalStateCommandProto(
                ((SetNodeOperationalStateCommand)cmd).getProto())
            .build();
    case finalizeNewDatanodeVersionCommand:
      return builder
            .setCommandType(finalizeNewDatanodeVersionCommand)
            .setFinalizeNewDatanodeVersionCommandProto(
                ((FinalizeVersionCommand)cmd).getProto())
            .build();
    case refreshVolumeUsageInfo:
      return builder
          .setCommandType(refreshVolumeUsageInfo)
          .setRefreshVolumeUsageCommandProto(
              ((RefreshVolumeUsageCommand)cmd).getProto())
          .build();
    case reconcileContainerCommand:
      return builder
          .setCommandType(reconcileContainerCommand)
          .setReconcileContainerCommandProto(
              ((ReconcileContainerCommand)cmd).getProto())
          .build();
    default:
      throw new IllegalArgumentException("Scm command " +
          cmd.getType().toString() + " is not implemented");
    }
  }

  public void join() throws InterruptedException {
    LOG.trace("Join RPC server for DataNodes");
    datanodeRpcServer.join();
  }

  public void stop() {
    try {
      LOG.info("Stopping the RPC server for DataNodes");
      datanodeRpcServer.stop();
    } catch (Exception ex) {
      LOG.error(" datanodeRpcServer stop failed.", ex);
    }
    IOUtils.cleanupWithLogger(LOG, scm.getScmNodeManager());
    protocolMessageMetrics.unregister();
  }

  @Override
  public AuditMessage buildAuditMessageForSuccess(
      AuditAction op, Map<String, String> auditMap) {

    return new AuditMessage.Builder()
        .setUser(getRemoteUserName())
        .atIp(Server.getRemoteAddress())
        .forOperation(op)
        .withParams(auditMap)
        .withResult(AuditEventStatus.SUCCESS)
        .build();
  }

  @Override
  public AuditMessage buildAuditMessageForFailure(AuditAction op, Map<String,
      String> auditMap, Throwable throwable) {

    return new AuditMessage.Builder()
        .setUser(getRemoteUserName())
        .atIp(Server.getRemoteAddress())
        .forOperation(op)
        .withParams(auditMap)
        .withResult(AuditEventStatus.FAILURE)
        .withException(throwable)
        .build();
  }

  /**
   * Get the ProtocolMessageMetrics for this server.
   * @return ProtocolMessageMetrics
   */
  protected ProtocolMessageMetrics<StorageContainerDatanodeProtocolProtos.Type>
        getProtocolMessageMetrics() {
    return ProtocolMessageMetrics
        .create("SCMDatanodeProtocol", "SCM Datanode protocol",
            StorageContainerDatanodeProtocolProtos.Type.class);
  }

  /**
   * Get Key associated with Datanode address for this server.
   */
  protected String getDatanodeAddressKey() {
    return this.scm.getScmNodeDetails().getDatanodeAddressKey();
  }

  /**
   * Get Datanode bind address for SCM.
   * @param conf ozone configuration
   * @return InetSocketAddress
   */
  protected InetSocketAddress getDataNodeBindAddress(
      OzoneConfiguration conf, SCMNodeDetails scmNodeDetails) {
    return scmNodeDetails.getDatanodeProtocolServerAddress();
  }

  /**
   * Get the authorization provider for this SCM server.
   * @return SCM policy provider.
   */
  protected PolicyProvider getPolicyProvider() {
    return SCMPolicyProvider.getInstance();
  }

  /**
   * Get protocol class type for this RPC Server.
   * @return Class type.
   */
  protected Class getProtocolClass() {
    return StorageContainerDatanodeProtocolPB.class;
  }

  /**
   * Datanode details and container IDs for node registration and registration FCR processing events.
   * {@link SCMEvents#NODE_REGISTRATION_CONT_REPORT} needs only datanode identity after successful registration.
   * {@link SCMEvents#CONTAINER_REGISTRATION_REPORT} carries container IDs after successful registration FCR processing.
   */
  public static class NodeRegistrationContainerReport {
    private final DatanodeDetails datanodeDetails;
    private final long[] containerIDs;

    public NodeRegistrationContainerReport(DatanodeDetails datanodeDetails) {
      this(datanodeDetails, null);
    }

    public NodeRegistrationContainerReport(DatanodeDetails datanodeDetails,
        ContainerReportsProto report) {
      this.datanodeDetails = datanodeDetails;
      int numContainers = report == null ? 0 : report.getReportsCount();
      containerIDs = new long[numContainers];
      for (int i = 0; i < numContainers; i++) {
        containerIDs[i] = report.getReports(i).getContainerID();
      }
    }

    public DatanodeDetails getDatanodeDetails() {
      return datanodeDetails;
    }

    public long[] getContainerIDs() {
      return containerIDs.clone();
    }
  }

}
