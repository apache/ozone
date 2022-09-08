/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.states.endpoint;

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandQueueReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineActionsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerActionsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.container.common.helpers
    .DeletedContainerBlocksSummary;
import org.apache.hadoop.ozone.container.common.statemachine
    .EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine
    .EndpointStateMachine.EndPointStates;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ClosePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.CreatePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.FinalizeNewLayoutVersionCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.RefreshVolumeUsageCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;

import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.commands.SetNodeOperationalStateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_CONTAINER_ACTION_MAX_LIMIT;
import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_CONTAINER_ACTION_MAX_LIMIT_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_PIPELINE_ACTION_MAX_LIMIT;
import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_PIPELINE_ACTION_MAX_LIMIT_DEFAULT;
import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.toLayoutVersionProto;

/**
 * Heartbeat class for SCMs.
 */
public class HeartbeatEndpointTask
    implements Callable<EndpointStateMachine.EndPointStates> {
  static final Logger LOG =
      LoggerFactory.getLogger(HeartbeatEndpointTask.class);
  private final EndpointStateMachine rpcEndpoint;
  private final ConfigurationSource conf;
  private DatanodeDetailsProto datanodeDetailsProto;
  private StateContext context;
  private int maxContainerActionsPerHB;
  private int maxPipelineActionsPerHB;
  private HDDSLayoutVersionManager layoutVersionManager;

  /**
   * Constructs a SCM heart beat.
   *
   * @param rpcEndpoint rpc Endpoint
   * @param conf Config.
   * @param context State context
   */
  public HeartbeatEndpointTask(EndpointStateMachine rpcEndpoint,
                               ConfigurationSource conf, StateContext context) {
    this(rpcEndpoint, conf, context,
        context.getParent().getLayoutVersionManager());
  }

  /**
   * Constructs a SCM heart beat.
   *
   * @param rpcEndpoint rpc Endpoint
   * @param conf Config.
   * @param context State context
   * @param versionManager Layout version Manager
   */
  public HeartbeatEndpointTask(EndpointStateMachine rpcEndpoint,
                               ConfigurationSource conf, StateContext context,
                               HDDSLayoutVersionManager versionManager) {
    this.rpcEndpoint = rpcEndpoint;
    this.conf = conf;
    this.context = context;
    this.maxContainerActionsPerHB = conf.getInt(HDDS_CONTAINER_ACTION_MAX_LIMIT,
        HDDS_CONTAINER_ACTION_MAX_LIMIT_DEFAULT);
    this.maxPipelineActionsPerHB = conf.getInt(HDDS_PIPELINE_ACTION_MAX_LIMIT,
        HDDS_PIPELINE_ACTION_MAX_LIMIT_DEFAULT);
    if (versionManager != null) {
      this.layoutVersionManager = versionManager;
    } else {
      this.layoutVersionManager = context.getParent().getLayoutVersionManager();
    }
  }

  /**
   * Get the container Node ID proto.
   *
   * @return ContainerNodeIDProto
   */
  public DatanodeDetailsProto getDatanodeDetailsProto() {
    return datanodeDetailsProto;
  }

  /**
   * Set container node ID proto.
   *
   * @param datanodeDetailsProto - the node id.
   */
  public void setDatanodeDetailsProto(DatanodeDetailsProto
      datanodeDetailsProto) {
    this.datanodeDetailsProto = datanodeDetailsProto;
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
    try {
      Preconditions.checkState(this.datanodeDetailsProto != null);

      LayoutVersionProto layoutinfo = toLayoutVersionProto(
          layoutVersionManager.getMetadataLayoutVersion(),
          layoutVersionManager.getSoftwareLayoutVersion());

      requestBuilder = SCMHeartbeatRequestProto.newBuilder()
          .setDatanodeDetails(datanodeDetailsProto)
          .setDataNodeLayoutVersion(layoutinfo);
      addReports(requestBuilder);
      addContainerActions(requestBuilder);
      addPipelineActions(requestBuilder);
      addQueuedCommandCounts(requestBuilder);
      SCMHeartbeatRequestProto request = requestBuilder.build();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sending heartbeat message :: {}", request.toString());
      }
      SCMHeartbeatResponseProto response = rpcEndpoint.getEndPoint()
          .sendHeartbeat(request);
      processResponse(response, datanodeDetailsProto);
      rpcEndpoint.setLastSuccessfulHeartbeat(ZonedDateTime.now());
      rpcEndpoint.zeroMissedCount();
    } catch (IOException ex) {
      Preconditions.checkState(requestBuilder != null);
      // put back the reports which failed to be sent
      putBackIncrementalReports(requestBuilder);
      rpcEndpoint.logIfNeeded(ex);
    } finally {
      rpcEndpoint.unlock();
    }
    return rpcEndpoint.getState();
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

  /**
   * Adds all the available reports to heartbeat.
   *
   * @param requestBuilder builder to which the report has to be added.
   */
  private void addReports(SCMHeartbeatRequestProto.Builder requestBuilder) {
    for (Message report :
        context.getAllAvailableReports(rpcEndpoint.getAddress())) {
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
          break;
        }
      }
    }
  }

  /**
   * Adds all the pending ContainerActions to the heartbeat.
   *
   * @param requestBuilder builder to which the report has to be added.
   */
  private void addContainerActions(
      SCMHeartbeatRequestProto.Builder requestBuilder) {
    List<ContainerAction> actions = context.getPendingContainerAction(
        rpcEndpoint.getAddress(), maxContainerActionsPerHB);
    if (!actions.isEmpty()) {
      ContainerActionsProto cap = ContainerActionsProto.newBuilder()
          .addAllContainerActions(actions)
          .build();
      requestBuilder.setContainerActions(cap);
    }
  }

  /**
   * Adds all the pending PipelineActions to the heartbeat.
   *
   * @param requestBuilder builder to which the report has to be added.
   */
  private void addPipelineActions(
      SCMHeartbeatRequestProto.Builder requestBuilder) {
    List<PipelineAction> actions = context.getPendingPipelineAction(
        rpcEndpoint.getAddress(), maxPipelineActionsPerHB);
    if (!actions.isEmpty()) {
      PipelineActionsProto pap = PipelineActionsProto.newBuilder()
          .addAllPipelineActions(actions)
          .build();
      requestBuilder.setPipelineActions(pap);
    }
  }

  /**
   * Adds the count of all queued commands to the heartbeat.
   * @param requestBuilder Builder to which the details will be added.
   */
  private void addQueuedCommandCounts(
      SCMHeartbeatRequestProto.Builder requestBuilder) {
    Map<SCMCommandProto.Type, Integer> commandCount =
        context.getParent().getQueuedCommandCount();
    CommandQueueReportProto.Builder reportProto =
        CommandQueueReportProto.newBuilder();
    for (Map.Entry<SCMCommandProto.Type, Integer> entry
        : commandCount.entrySet()) {
      reportProto.addCommand(entry.getKey())
          .addCount(entry.getValue());
    }
    requestBuilder.setCommandQueueReport(reportProto.build());
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
      final DatanodeDetailsProto datanodeDetails) {
    Preconditions.checkState(response.getDatanodeUUID()
            .equalsIgnoreCase(datanodeDetails.getUuid()),
        "Unexpected datanode ID in the response.");
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
      case finalizeNewLayoutVersionCommand:
        FinalizeNewLayoutVersionCommand finalizeNewLayoutVersionCommand =
            FinalizeNewLayoutVersionCommand.getFromProtobuf(
                commandResponseProto.getFinalizeNewLayoutVersionCommandProto());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received SCM finalize command {}",
              finalizeNewLayoutVersionCommand.getId());
        }
        processCommonCommand(commandResponseProto,
            finalizeNewLayoutVersionCommand);
        break;
      case refreshVolumeUsageInfo:
        RefreshVolumeUsageCommand refreshVolumeUsageCommand =
            RefreshVolumeUsageCommand.getFromProtobuf(
            commandResponseProto.getRefreshVolumeUsageCommandProto());
        processCommonCommand(commandResponseProto, refreshVolumeUsageCommand);
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
    context.addCommand(cmd);
  }

  private void processReregisterCommand() {
    if (rpcEndpoint.getState() == EndPointStates.HEARTBEAT) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received SCM notification to register."
            + " Interrupt HEARTBEAT and transit to REGISTER state.");
      }
      rpcEndpoint.setState(EndPointStates.REGISTER);
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
    private DatanodeDetails datanodeDetails;
    private StateContext context;
    private HDDSLayoutVersionManager versionManager;

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
     * Sets the LayoutVersionManager.
     *
     * @param versionMgr - config
     * @return Builder
     */
    public Builder setLayoutVersionManager(HDDSLayoutVersionManager lvm) {
      this.versionManager = lvm;
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
     * Sets the NodeID.
     *
     * @param dnDetails - NodeID proto
     * @return Builder
     */
    public Builder setDatanodeDetails(DatanodeDetails dnDetails) {
      this.datanodeDetails = dnDetails;
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
        throw new IllegalArgumentException("A valid configration is needed to" +
            " construct HeartbeatEndpointTask task");
      }

      if (datanodeDetails == null) {
        LOG.error("No datanode specified.");
        throw new IllegalArgumentException("A valid Node ID is needed to " +
            "construct HeartbeatEndpointTask task");
      }

      HeartbeatEndpointTask task = new HeartbeatEndpointTask(this
          .endPointStateMachine, this.conf, this.context, this.versionManager);
      task.setDatanodeDetailsProto(datanodeDetails.getProtoBufMessage());
      return task;
    }
  }
}
