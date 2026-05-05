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

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto.ErrorCode.success;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Register a datanode with SCM.
 */
public final class RegisterEndpointTask implements
    Callable<EndpointStateMachine.EndPointStates> {
  static final Logger LOG = LoggerFactory.getLogger(RegisterEndpointTask.class);

  private final EndpointStateMachine rpcEndPoint;
  private DatanodeDetails datanodeDetails;
  private final OzoneContainer datanodeContainerManager;
  private StateContext stateContext;
  private HDDSLayoutVersionManager layoutVersionManager;

  /**
   * Creates a register endpoint task.
   *
   * @param rpcEndPoint - endpoint
   * @param ozoneContainer - container
   * @param context - State context
   * @param versionManager - layout version Manager
   */
  @VisibleForTesting
  public RegisterEndpointTask(EndpointStateMachine rpcEndPoint,
      OzoneContainer ozoneContainer,
      StateContext context, HDDSLayoutVersionManager versionManager) {
    this.rpcEndPoint = rpcEndPoint;
    this.datanodeContainerManager = ozoneContainer;
    this.stateContext = context;
    if (versionManager != null) {
      this.layoutVersionManager = versionManager;
    } else {
      this.layoutVersionManager =
          context.getParent().getLayoutVersionManager();
    }
  }

  /**
   * Get the DatanodeDetails.
   *
   * @return DatanodeDetailsProto
   */
  public DatanodeDetails getDatanodeDetails() {
    return datanodeDetails;
  }

  /**
   * Set the contiainerNodeID Proto.
   *
   * @param datanodeDetails - Container Node ID.
   */
  public void setDatanodeDetails(
      DatanodeDetails datanodeDetails) {
    this.datanodeDetails = datanodeDetails;
  }

  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  @Override
  public EndpointStateMachine.EndPointStates call() throws Exception {

    if (getDatanodeDetails() == null) {
      LOG.error("DatanodeDetails cannot be null in RegisterEndpoint task, "
          + "shutting down the endpoint.");
      return rpcEndPoint.setState(EndpointStateMachine.EndPointStates.SHUTDOWN);
    }

    rpcEndPoint.lock();
    try {

      if (rpcEndPoint.getState()
          .equals(EndpointStateMachine.EndPointStates.REGISTER)) {
        LayoutVersionProto layoutInfo = LayoutVersionProto.newBuilder()
            .setMetadataLayoutVersion(
                layoutVersionManager.getMetadataLayoutVersion())
            .setSoftwareLayoutVersion(
                layoutVersionManager.getSoftwareLayoutVersion())
            .build();
        ContainerReportsProto containerReport =
            datanodeContainerManager.getController().getContainerReport();
        NodeReportProto nodeReport = datanodeContainerManager.getNodeReport();
        PipelineReportsProto pipelineReportsProto =
            datanodeContainerManager.getPipelineReport();
        // TODO : Add responses to the command Queue.
        SCMRegisteredResponseProto response = rpcEndPoint.getEndPoint()
            .register(datanodeDetails.getExtendedProtoBufMessage(),
            nodeReport, containerReport, pipelineReportsProto, layoutInfo);
        Preconditions.assertEquals(datanodeDetails.getUuidString(), response.getDatanodeUUID(), "datanodeID");
        Preconditions.assertTrue(!StringUtils.isBlank(response.getClusterID()),
            "Invalid cluster ID in the response.");
        Preconditions.assertSame(success, response.getErrorCode(), "ErrorCode");
        if (response.hasHostname() && response.hasIpAddress()) {
          datanodeDetails.setHostName(response.getHostname());
          datanodeDetails.setIpAddress(response.getIpAddress());
        }
        if (response.hasNetworkName() && response.hasNetworkLocation()) {
          datanodeDetails.setNetworkName(response.getNetworkName());
          datanodeDetails.setNetworkLocation(response.getNetworkLocation());
        }
        EndpointStateMachine.EndPointStates nextState =
            rpcEndPoint.getState().getNextState();
        rpcEndPoint.setState(nextState);
        rpcEndPoint.zeroMissedCount();
        if (rpcEndPoint.isPassive()) {
          this.stateContext.configureReconHeartbeatFrequency();
        } else {
          this.stateContext.configureHeartbeatFrequency();
        }
      }
    } catch (IOException ex) {
      rpcEndPoint.logIfNeeded(ex);
    } finally {
      rpcEndPoint.unlock();
    }

    return rpcEndPoint.getState();
  }

  /**
   * Returns a builder class for RegisterEndPoint task.
   *
   * @return Builder.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for RegisterEndPoint task.
   */
  public static class Builder {
    private EndpointStateMachine endPointStateMachine;
    private ConfigurationSource conf;
    private DatanodeDetails datanodeDetails;
    private OzoneContainer container;
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
     * Sets the Config.
     *
     * @param config - config
     * @return Builder.
     */
    public Builder setConfig(ConfigurationSource config) {
      this.conf = config;
      return this;
    }

    /**
     * Sets the LayoutVersionManager.
     *
     * @param lvm config
     * @return Builder.
     */
    public Builder setLayoutVersionManager(HDDSLayoutVersionManager lvm) {
      this.versionManager = lvm;
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
     * Sets the ozonecontainer.
     * @param ozoneContainer
     * @return Builder
     */
    public Builder setOzoneContainer(OzoneContainer ozoneContainer) {
      this.container = ozoneContainer;
      return this;
    }

    public Builder setContext(StateContext stateContext) {
      this.context = stateContext;
      return this;
    }

    public RegisterEndpointTask build() {
      if (endPointStateMachine == null) {
        LOG.error("No endpoint specified.");
        throw new IllegalArgumentException("A valid endpoint state machine is" +
            " needed to construct RegisterEndPoint task");
      }

      if (conf == null) {
        LOG.error("No config specified.");
        throw new IllegalArgumentException(
            "A valid configuration is needed to construct RegisterEndpoint "
                + "task");
      }

      if (datanodeDetails == null) {
        LOG.error("No datanode specified.");
        throw new IllegalArgumentException("A valid Node ID is needed to " +
            "construct RegisterEndpoint task");
      }

      if (container == null) {
        LOG.error("Container is not specified");
        throw new IllegalArgumentException("Container is not specified to " +
            "construct RegisterEndpoint task");
      }

      if (context == null) {
        LOG.error("StateContext is not specified");
        throw new IllegalArgumentException("Container is not specified to " +
            "construct RegisterEndpoint task");
      }

      RegisterEndpointTask task = new RegisterEndpointTask(this
          .endPointStateMachine, this.container, this.context,
          this.versionManager);
      task.setDatanodeDetails(datanodeDetails);
      return task;
    }
  }
}
