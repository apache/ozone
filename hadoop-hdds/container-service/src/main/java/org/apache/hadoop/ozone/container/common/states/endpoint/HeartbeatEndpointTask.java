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
import com.google.protobuf.GeneratedMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.ozone.container.common.helpers
    .DeletedContainerBlocksSummary;
import org.apache.hadoop.ozone.container.common.statemachine
    .EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine
    .EndpointStateMachine.EndPointStates;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.concurrent.Callable;

/**
 * Heartbeat class for SCMs.
 */
public class HeartbeatEndpointTask
    implements Callable<EndpointStateMachine.EndPointStates> {
  static final Logger LOG =
      LoggerFactory.getLogger(HeartbeatEndpointTask.class);
  private final EndpointStateMachine rpcEndpoint;
  private final Configuration conf;
  private DatanodeDetailsProto datanodeDetailsProto;
  private StateContext context;

  /**
   * Constructs a SCM heart beat.
   *
   * @param conf Config.
   */
  public HeartbeatEndpointTask(EndpointStateMachine rpcEndpoint,
      Configuration conf, StateContext context) {
    this.rpcEndpoint = rpcEndpoint;
    this.conf = conf;
    this.context = context;
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
    try {
      Preconditions.checkState(this.datanodeDetailsProto != null);

      SCMHeartbeatRequestProto.Builder requestBuilder =
          SCMHeartbeatRequestProto.newBuilder()
              .setDatanodeDetails(datanodeDetailsProto);
      addReports(requestBuilder);

      SCMHeartbeatResponseProto reponse = rpcEndpoint.getEndPoint()
          .sendHeartbeat(requestBuilder.build());
      processResponse(reponse, datanodeDetailsProto);
      rpcEndpoint.setLastSuccessfulHeartbeat(ZonedDateTime.now());
      rpcEndpoint.zeroMissedCount();
    } catch (IOException ex) {
      rpcEndpoint.logIfNeeded(ex);
    } finally {
      rpcEndpoint.unlock();
    }
    return rpcEndpoint.getState();
  }

  /**
   * Adds all the available reports to heartbeat.
   *
   * @param requestBuilder builder to which the report has to be added.
   */
  private void addReports(SCMHeartbeatRequestProto.Builder requestBuilder) {
    for (GeneratedMessage report : context.getAllAvailableReports()) {
      requestBuilder.setField(
          SCMHeartbeatRequestProto.getDescriptor().findFieldByName(
              report.getDescriptorForType().getName()), report);
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
      final DatanodeDetailsProto datanodeDetails) {
    Preconditions.checkState(response.getDatanodeUUID()
            .equalsIgnoreCase(datanodeDetails.getUuid()),
        "Unexpected datanode ID in the response.");
    // Verify the response is indeed for this datanode.
    for (SCMCommandProto commandResponseProto : response
        .getCommandsList()) {
      switch (commandResponseProto.getCommandType()) {
      case reregisterCommand:
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
        break;
      case deleteBlocksCommand:
        DeleteBlocksCommand db = DeleteBlocksCommand
            .getFromProtobuf(
                commandResponseProto.getDeleteBlocksCommandProto());
        if (!db.blocksTobeDeleted().isEmpty()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(DeletedContainerBlocksSummary
                .getFrom(db.blocksTobeDeleted())
                .toString());
          }
          this.context.addCommand(db);
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
        this.context.addCommand(closeContainer);
        break;
      default:
        throw new IllegalArgumentException("Unknown response : "
            + commandResponseProto.getCommandType().name());
      }
    }
  }

  /**
   * Builder class for HeartbeatEndpointTask.
   */
  public static class Builder {
    private EndpointStateMachine endPointStateMachine;
    private Configuration conf;
    private DatanodeDetails datanodeDetails;
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
    public Builder setConfig(Configuration config) {
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
        throw new IllegalArgumentException("A vaild Node ID is needed to " +
            "construct HeartbeatEndpointTask task");
      }

      HeartbeatEndpointTask task = new HeartbeatEndpointTask(this
          .endPointStateMachine, this.conf, this.context);
      task.setDatanodeDetailsProto(datanodeDetails.getProtoBufMessage());
      return task;
    }
  }
}
