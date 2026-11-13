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

package org.apache.hadoop.ozone.container.common.states.endpoint;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_ACTION_MAX_LIMIT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_ACTION_MAX_LIMIT_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_ADDRESS_REFRESH_MISSED_COUNT_THRESHOLD;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_ADDRESS_REFRESH_MISSED_COUNT_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_COMMAND_STATUS_REPORT_MAX_SIZE;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_COMMAND_STATUS_REPORT_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_REPORT_MAX_LIMIT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_REPORT_MAX_LIMIT_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_IPC_MAXIMUM_DATA_LENGTH;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_IPC_MAXIMUM_DATA_LENGTH_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_ACTION_MAX_LIMIT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_ACTION_MAX_LIMIT_DEFAULT;
import static org.apache.hadoop.hdds.conf.StorageUnit.BYTES;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY;
import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.toVersionProto;

import com.google.common.base.Preconditions;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandQueueReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerActionsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DatanodeVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineActionsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.utils.ConnectionFailureUtils;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.container.common.helpers.DeletedContainerBlocksSummary;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine.EndPointStates;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext.CommandStatusReportSelection;
import org.apache.hadoop.ozone.container.upgrade.DatanodeVersionManager;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ClosePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.CreatePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.FinalizeVersionCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.RefreshVolumeUsageCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.commands.SetNodeOperationalStateCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Heartbeat class for SCMs.
 */
public class HeartbeatEndpointTask
    implements Callable<EndpointStateMachine.EndPointStates> {
  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatEndpointTask.class);
  private static final double IPC_SIZE_SAFETY_FACTOR = 0.9;
  private final EndpointStateMachine rpcEndpoint;
  private final DatanodeDetails datanodeDetails;
  private StateContext context;
  private int maxContainerActionsPerHB;
  private int maxPipelineActionsPerHB;
  private int maxReportsPerHB;
  private final DatanodeVersionManager versionManager;
  // Test-only override for the version this datanode advertises to clients; null in production.
  private final HDDSVersion testCurrentVersion;
  private final boolean resolveOnFailureEnabled;
  private final int refreshThreshold;
  private final long maxCommandStatusReportBytes;
  private final long safeHeartbeatSize;

  /**
   * Constructs a SCM heart beat.
   *
   * @param rpcEndpoint rpc Endpoint
   * @param conf Config.
   * @param context State context
   */
  public HeartbeatEndpointTask(EndpointStateMachine rpcEndpoint,
                               ConfigurationSource conf, StateContext context) {
    this.rpcEndpoint = rpcEndpoint;
    this.context = context;
    this.maxContainerActionsPerHB = conf.getInt(HDDS_CONTAINER_ACTION_MAX_LIMIT,
        HDDS_CONTAINER_ACTION_MAX_LIMIT_DEFAULT);
    this.maxPipelineActionsPerHB = conf.getInt(HDDS_PIPELINE_ACTION_MAX_LIMIT,
        HDDS_PIPELINE_ACTION_MAX_LIMIT_DEFAULT);
    this.maxReportsPerHB = Math.max(1, conf.getInt(HDDS_HEARTBEAT_REPORT_MAX_LIMIT,
        HDDS_HEARTBEAT_REPORT_MAX_LIMIT_DEFAULT));
    this.maxCommandStatusReportBytes = (long) conf.getStorageSize(
        HDDS_HEARTBEAT_COMMAND_STATUS_REPORT_MAX_SIZE,
        HDDS_HEARTBEAT_COMMAND_STATUS_REPORT_MAX_SIZE_DEFAULT, BYTES);
    Preconditions.checkArgument(maxCommandStatusReportBytes > 0,
        HDDS_HEARTBEAT_COMMAND_STATUS_REPORT_MAX_SIZE + " must be positive");
    int ipcMaximumDataLength = conf.getInt(
        HDDS_IPC_MAXIMUM_DATA_LENGTH,
        HDDS_IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    Preconditions.checkArgument(ipcMaximumDataLength > 0,
        HDDS_IPC_MAXIMUM_DATA_LENGTH + " must be positive");
    this.safeHeartbeatSize =
        (long) Math.floor(ipcMaximumDataLength * IPC_SIZE_SAFETY_FACTOR);
    this.datanodeDetails = context.getParent().getDatanodeDetails();
    this.versionManager = context.getParent().getVersionManager();

    if (conf.isConfigured(HddsDatanodeService.TESTING_DATANODE_VERSION_CURRENT)) {
      int configuredVersion = conf.getInt(HddsDatanodeService.TESTING_DATANODE_VERSION_CURRENT,
          HDDSVersion.SOFTWARE_VERSION.serialize());
      testCurrentVersion = HDDSVersion.deserialize(configuredVersion);
    } else {
      testCurrentVersion = null;
    }

    this.resolveOnFailureEnabled = conf.getBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY,
        OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_DEFAULT);
    this.refreshThreshold = Math.max(1, conf.getInt(HDDS_HEARTBEAT_ADDRESS_REFRESH_MISSED_COUNT_THRESHOLD,
        HDDS_HEARTBEAT_ADDRESS_REFRESH_MISSED_COUNT_THRESHOLD_DEFAULT));
  }

  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  @Override
  public EndpointStateMachine.EndPointStates call() throws Exception {
    rpcEndpoint.lock();
    SCMHeartbeatRequestProto.Builder requestBuilder = null;
    CommandStatusReportSelection commandStatusSelection = null;
    try {
      DatanodeVersionProto versionInfo = toVersionProto(
          versionManager.getApparentVersion(),
          versionManager.getSoftwareVersion());

      datanodeDetails.setCurrentVersion(
          testCurrentVersion != null ? testCurrentVersion : versionManager.getVersionForClient());
      DatanodeDetailsProto datanodeDetailsProto = datanodeDetails.getProtoBufMessage();

      requestBuilder = SCMHeartbeatRequestProto.newBuilder()
          .setDatanodeDetails(datanodeDetailsProto)
          .setDatanodeVersion(versionInfo);
      SerializedSizeAccumulator heartbeatSize = new SerializedSizeAccumulator();
      heartbeatSize.add(SCMHeartbeatRequestProto.DATANODEDETAILS_FIELD_NUMBER, datanodeDetailsProto);
      heartbeatSize.add(SCMHeartbeatRequestProto.DATANODEVERSION_FIELD_NUMBER, versionInfo);
      int reportCount = addNonCommandStatusReports(requestBuilder, heartbeatSize);
      addContainerActions(requestBuilder, heartbeatSize);
      addPipelineActions(requestBuilder, heartbeatSize);
      addQueuedCommandCounts(requestBuilder, heartbeatSize);
      commandStatusSelection = addCommandStatusReports(
          requestBuilder, maxReportsPerHB - reportCount, heartbeatSize.get());
      SCMHeartbeatRequestProto request = requestBuilder.build();
      LOG.debug("Sending heartbeat message : {}", request);
      SCMHeartbeatResponseProto response = rpcEndpoint.getEndPoint()
          .sendHeartbeat(request);
      processResponse(response, datanodeDetailsProto);
      rpcEndpoint.setLastSuccessfulHeartbeat(ZonedDateTime.now());
      rpcEndpoint.zeroMissedCount();
      if (context.getQueueMetrics() != null) {
        context.getQueueMetrics().incrCommandStatusReportBytesSent(
            rpcEndpoint.getAddress(),
            commandStatusSelection.getSelectedBytes());
      }
    } catch (IOException ex) {
      Preconditions.checkState(requestBuilder != null);
      // put back the reports which failed to be sent
      putBackIncrementalReports(requestBuilder);
      if (commandStatusSelection != null &&
          context.getQueueMetrics() != null) {
        context.getQueueMetrics().incrCommandStatusReportBytesRequeued(
            rpcEndpoint.getAddress(),
            commandStatusSelection.getSelectedBytes());
      }
      rpcEndpoint.logIfNeeded(ex);
      maybeRefreshScmAddress(ex);
    } finally {
      rpcEndpoint.unlock();
    }
    return rpcEndpoint.getState();
  }

  /**
   * On a connection-class heartbeat failure past the threshold (and when resolve-needed is on),
   * asks the connection manager to re-resolve this SCM peer and rebuild the endpoint.
   */
  private void maybeRefreshScmAddress(IOException heartbeatFailure) {
    if (!resolveOnFailureEnabled
        || rpcEndpoint.isPassive()
        || rpcEndpoint.getMissedCount() < refreshThreshold
        || !ConnectionFailureUtils.isConnectionFailure(heartbeatFailure)) {
      return;
    }
    try {
      context.getParent().getConnectionManager()
          .refreshSCMServer(rpcEndpoint.getAddress(), context.getThreadNamePrefix());
    } catch (IOException ex) {
      LOG.warn("Failed to refresh SCM address {} after {} missed heartbeats",
          rpcEndpoint.getAddress(), rpcEndpoint.getMissedCount(), ex);
    }
  }

  // TODO: Make it generic.
  private void putBackIncrementalReports(
      SCMHeartbeatRequestProto.Builder requestBuilder) {
    List<Message> reports = new LinkedList<>();
    // We only put back CommandStatusReports and IncrementalContainerReport
    // because those are incremental. Container/Node/PipelineReport are
    // accumulative so we can keep only the latest of each.
    if (requestBuilder.getCommandStatusReportsCount() != 0) {
      reports.addAll(requestBuilder.getCommandStatusReportsList());
    }
    if (requestBuilder.getIncrementalContainerReportCount() != 0) {
      reports.addAll(requestBuilder.getIncrementalContainerReportList());
    }
    context.putBackReports(reports, rpcEndpoint.getAddress());
  }

  private int addNonCommandStatusReports(SCMHeartbeatRequestProto.Builder requestBuilder,
      SerializedSizeAccumulator heartbeatSize) {
    int reportCount = 0;
    for (Message report :
        context.getAvailableNonCommandStatusReports(
            rpcEndpoint.getAddress(), maxReportsPerHB)) {
      reportCount++;
      String reportName = report.getDescriptorForType().getFullName();
      for (Descriptors.FieldDescriptor descriptor :
          SCMHeartbeatRequestProto.getDescriptor().getFields()) {
        String heartbeatFieldName = descriptor.getMessageType().getFullName();
        if (heartbeatFieldName.equals(reportName)) {
          if (descriptor.isRepeated()) {
            requestBuilder.addRepeatedField(descriptor, report);
          } else {
            requestBuilder.setField(descriptor, report);
          }
          heartbeatSize.add(descriptor.getNumber(), report);
          break;
        }
      }
    }
    return reportCount;
  }

  private CommandStatusReportSelection addCommandStatusReports(
      SCMHeartbeatRequestProto.Builder requestBuilder,
      int remainingReportCount, long currentHeartbeatSize) {
    long remainingSafeBytes = Math.max(
        0, safeHeartbeatSize - currentHeartbeatSize);
    long commandStatusReportBudget = Math.min(maxCommandStatusReportBytes, remainingSafeBytes);
    long oversizedThreshold = Math.min(maxCommandStatusReportBytes, safeHeartbeatSize);
    CommandStatusReportSelection selection =
        context.getCommandStatusReports(rpcEndpoint.getAddress(),
            remainingReportCount, commandStatusReportBudget,
            remainingSafeBytes, oversizedThreshold);
    for (CommandStatusReportsProto report : selection.getReports()) {
      requestBuilder.addCommandStatusReports(report);
    }

    if (currentHeartbeatSize > safeHeartbeatSize) {
      LOG.warn("Heartbeat for {} is already {} bytes before command status "
              + "reports, exceeding the safe size of {} bytes",
          rpcEndpoint.getAddress(), currentHeartbeatSize, safeHeartbeatSize);
    }
    if (selection.getOversizedBytes() > 0) {
      LOG.warn("A command status for {} is {} bytes, exceeding the effective "
              + "limit of {} bytes (configured limit: {}, safe heartbeat size: {})",
          rpcEndpoint.getAddress(), selection.getOversizedBytes(),
          oversizedThreshold, maxCommandStatusReportBytes, safeHeartbeatSize);
    }
    if (context.getQueueMetrics() != null) {
      context.getQueueMetrics().setOversizedCommandStatusReportBytes(
          rpcEndpoint.getAddress(), selection.getOversizedBytes());
    }
    return selection;
  }

  /**
   * Adds all the pending ContainerActions to the heartbeat.
   *
   * @param requestBuilder builder to which the report has to be added.
   */
  private void addContainerActions(SCMHeartbeatRequestProto.Builder requestBuilder,
      SerializedSizeAccumulator heartbeatSize) {
    List<ContainerAction> actions = context.getPendingContainerAction(
        rpcEndpoint.getAddress(), maxContainerActionsPerHB);
    if (!actions.isEmpty()) {
      ContainerActionsProto cap = ContainerActionsProto.newBuilder()
          .addAllContainerActions(actions)
          .build();
      requestBuilder.setContainerActions(cap);
      heartbeatSize.add(SCMHeartbeatRequestProto.CONTAINERACTIONS_FIELD_NUMBER, cap);
    }
  }

  /**
   * Adds all the pending PipelineActions to the heartbeat.
   *
   * @param requestBuilder builder to which the report has to be added.
   */
  private void addPipelineActions(SCMHeartbeatRequestProto.Builder requestBuilder,
      SerializedSizeAccumulator heartbeatSize) {
    List<PipelineAction> actions = context.getPendingPipelineAction(
        rpcEndpoint.getAddress(), maxPipelineActionsPerHB);
    if (!actions.isEmpty()) {
      PipelineActionsProto pap = PipelineActionsProto.newBuilder()
          .addAllPipelineActions(actions)
          .build();
      requestBuilder.setPipelineActions(pap);
      heartbeatSize.add(SCMHeartbeatRequestProto.PIPELINEACTIONS_FIELD_NUMBER, pap);
    }
  }

  /**
   * Adds the count of all queued commands to the heartbeat.
   * @param requestBuilder Builder to which the details will be added.
   */
  private void addQueuedCommandCounts(SCMHeartbeatRequestProto.Builder requestBuilder,
      SerializedSizeAccumulator heartbeatSize) {
    EnumCounters<SCMCommandProto.Type> commandCount =
        context.getParent().getQueuedCommandCount();
    CommandQueueReportProto.Builder reportProto =
        CommandQueueReportProto.newBuilder();
    for (SCMCommandProto.Type type : SCMCommandProto.Type.values()) {
      long count = commandCount.get(type);
      if (count > 0) {
        reportProto.addCommand(type)
            .addCount((int) count);
      }
    }
    CommandQueueReportProto commandQueueReport = reportProto.build();
    requestBuilder.setCommandQueueReport(commandQueueReport);
    heartbeatSize.add(SCMHeartbeatRequestProto.COMMANDQUEUEREPORT_FIELD_NUMBER, commandQueueReport);
  }

  private static final class SerializedSizeAccumulator {
    private long size;

    private void add(int fieldNumber, Message message) {
      size += CodedOutputStream.computeMessageSize(fieldNumber, message);
    }

    private long get() {
      return size;
    }
  }

  /**
   * Returns a builder class for HeartbeatEndpointTask task.
   * @return   Builder.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Add this command to command processing Queue.
   *
   * @param response - SCMHeartbeat response.
   */
  private void processResponse(SCMHeartbeatResponseProto response,
      final DatanodeDetailsProto datanodeDetailsProto) {
    Preconditions.checkState(response.getDatanodeUUID()
            .equalsIgnoreCase(datanodeDetailsProto.getUuid()),
        "Unexpected datanode ID in the response.");
    if (response.hasTerm()) {
      context.updateTermOfLeaderSCM(response.getTerm());
    }
    // Verify the response is indeed for this datanode.
    for (SCMCommandProto commandResponseProto : response.getCommandsList()) {
      switch (commandResponseProto.getCommandType()) {
      case reregisterCommand:
        processReregisterCommand();
        break;
      case deleteBlocksCommand:
        DeleteBlocksCommand deleteBlocksCommand = DeleteBlocksCommand
            .getFromProtobuf(
                commandResponseProto.getDeleteBlocksCommandProto());
        if (!deleteBlocksCommand.blocksTobeDeleted().isEmpty()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(DeletedContainerBlocksSummary
                .getFrom(deleteBlocksCommand.blocksTobeDeleted())
                .toString());
          }
          processCommonCommand(commandResponseProto, deleteBlocksCommand);
        }
        break;
      case closeContainerCommand:
        CloseContainerCommand closeContainer =
            CloseContainerCommand.getFromProtobuf(
                commandResponseProto.getCloseContainerCommandProto());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received SCM container close request for container {}",
              closeContainer.getContainerID());
        }
        processCommonCommand(commandResponseProto, closeContainer);
        break;
      case replicateContainerCommand:
        ReplicateContainerCommand replicateContainerCommand =
            ReplicateContainerCommand.getFromProtobuf(
                commandResponseProto.getReplicateContainerCommandProto());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received SCM container replicate request for container {}",
              replicateContainerCommand.getContainerID());
        }
        processCommonCommand(commandResponseProto, replicateContainerCommand);
        break;
      case reconstructECContainersCommand:
        ReconstructECContainersCommand reccc =
            ReconstructECContainersCommand.getFromProtobuf(
                commandResponseProto.getReconstructECContainersCommandProto());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received SCM reconstruct request for container {}",
              reccc.getContainerID());
        }
        processCommonCommand(commandResponseProto, reccc);
        break;
      case deleteContainerCommand:
        DeleteContainerCommand deleteContainerCommand =
            DeleteContainerCommand.getFromProtobuf(
                commandResponseProto.getDeleteContainerCommandProto());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received SCM delete container request for container {}",
              deleteContainerCommand.getContainerID());
        }
        processCommonCommand(commandResponseProto, deleteContainerCommand);
        break;
      case createPipelineCommand:
        CreatePipelineCommand createPipelineCommand =
            CreatePipelineCommand.getFromProtobuf(
                commandResponseProto.getCreatePipelineCommandProto());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received SCM create pipeline request {}",
              createPipelineCommand.getPipelineID());
        }
        processCommonCommand(commandResponseProto, createPipelineCommand);
        break;
      case closePipelineCommand:
        ClosePipelineCommand closePipelineCommand =
            ClosePipelineCommand.getFromProtobuf(
                commandResponseProto.getClosePipelineCommandProto());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received SCM close pipeline request {}",
              closePipelineCommand.getPipelineID());
        }
        processCommonCommand(commandResponseProto, closePipelineCommand);
        break;
      case setNodeOperationalStateCommand:
        SetNodeOperationalStateCommand setNodeOperationalStateCommand =
            SetNodeOperationalStateCommand.getFromProtobuf(
                commandResponseProto.getSetNodeOperationalStateCommandProto());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received SCM set operational state command. State: {} " +
              "Expiry: {}", setNodeOperationalStateCommand.getOpState(),
              setNodeOperationalStateCommand.getStateExpiryEpochSeconds());
        }
        processCommonCommand(commandResponseProto,
            setNodeOperationalStateCommand);
        break;
      case finalizeNewDatanodeVersionCommand:
        FinalizeVersionCommand finalizeVersionCommand =
            FinalizeVersionCommand.getFromProtobuf(commandResponseProto.getFinalizeNewDatanodeVersionCommandProto());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received SCM finalize command {}", finalizeVersionCommand.getId());
        }
        processCommonCommand(commandResponseProto, finalizeVersionCommand);
        break;
      case refreshVolumeUsageInfo:
        RefreshVolumeUsageCommand refreshVolumeUsageCommand =
            RefreshVolumeUsageCommand.getFromProtobuf(
            commandResponseProto.getRefreshVolumeUsageCommandProto());
        processCommonCommand(commandResponseProto, refreshVolumeUsageCommand);
        break;
      case reconcileContainerCommand:
        ReconcileContainerCommand reconcileContainerCommand =
            ReconcileContainerCommand.getFromProtobuf(commandResponseProto.getReconcileContainerCommandProto());
        processCommonCommand(commandResponseProto, reconcileContainerCommand);
        break;
      default:
        throw new IllegalArgumentException("Unknown response : "
            + commandResponseProto.getCommandType().name());
      }
    }
  }

  /**
   * Common processing for SCM commands.
   *  - set term
   *  - set encoded token
   *  - any deadline which is relevant to the command
   *  - add to context's queue
   */
  private void processCommonCommand(
      SCMCommandProto response, SCMCommand<?> cmd) {
    if (response.hasTerm()) {
      cmd.setTerm(response.getTerm());
    }
    if (response.hasEncodedToken()) {
      cmd.setEncodedToken(response.getEncodedToken());
    }
    if (response.hasDeadlineMsSinceEpoch()) {
      cmd.setDeadline(response.getDeadlineMsSinceEpoch());
    }
    context.addCommand(cmd);
  }

  private void processReregisterCommand() {
    if (rpcEndpoint.getState() == EndPointStates.HEARTBEAT) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received SCM notification to register."
            + " Interrupt HEARTBEAT and transit to GETVERSION state.");
      }
      rpcEndpoint.setState(EndPointStates.GETVERSION);
      // trigger immediate GETVERSION
      context.getParent().setNextHB(Time.monotonicNow());
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Illegal state {} found, expecting {}.",
            rpcEndpoint.getState().name(), EndPointStates.HEARTBEAT);
      }
    }
  }

  /**
   * Builder class for HeartbeatEndpointTask.
   */
  public static class Builder {
    private EndpointStateMachine endPointStateMachine;
    private ConfigurationSource conf;
    private StateContext context;

    /**
     * Constructs the builder class.
     */
    public Builder() {
    }

    /**
     * Sets the endpoint state machine.
     *
     * @param rpcEndPoint - Endpoint state machine.
     * @return Builder
     */
    public Builder setEndpointStateMachine(EndpointStateMachine rpcEndPoint) {
      this.endPointStateMachine = rpcEndPoint;
      return this;
    }

    /**
     * Sets the Config.
     *
     * @param config - config
     * @return Builder
     */
    public Builder setConfig(ConfigurationSource config) {
      this.conf = config;
      return this;
    }

    /**
     * Sets the context.
     * @param stateContext - State context.
     * @return this.
     */
    public Builder setContext(StateContext stateContext) {
      this.context = stateContext;
      return this;
    }

    public HeartbeatEndpointTask build() {
      if (endPointStateMachine == null) {
        LOG.error("No endpoint specified.");
        throw new IllegalArgumentException("A valid endpoint state machine is" +
            " needed to construct HeartbeatEndpointTask task");
      }

      if (conf == null) {
        LOG.error("No config specified.");
        throw new IllegalArgumentException("A valid configuration is needed to" +
            " construct HeartbeatEndpointTask task");
      }

      return new HeartbeatEndpointTask(this
          .endPointStateMachine, this.conf, this.context);
    }
  }
}
