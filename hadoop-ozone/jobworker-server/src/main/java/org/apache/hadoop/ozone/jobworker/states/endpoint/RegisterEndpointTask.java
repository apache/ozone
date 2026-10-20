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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse;
import org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine;
import org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine.EndpointStates;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.ozone.util.ProtobufUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Register a jobworker with OM.
 */
public final class RegisterEndpointTask implements Callable<EndpointStates> {
  static final Logger LOG = LoggerFactory.getLogger(RegisterEndpointTask.class);
  // Used to verify the correctness of the OM Service ID configuration.
  // Multiple OM nodes of one OM Service in the configuration should have the same OM Service ID on the OM side.
  private final JobworkerEndpointStateMachine rpcEndPoint;
  private final ConfigurationSource conf;
  private final JobworkerStateContext stateContext;
  private JobworkerDetails jobworkerDetails;

  /**
   * Creates a register endpoint task.
   *
   * @param rpcEndPoint - endpoint
   * @param conf        - conf
   * @param context     - State context
   */
  @VisibleForTesting
  public RegisterEndpointTask(JobworkerEndpointStateMachine rpcEndPoint,
                              ConfigurationSource conf,
                              JobworkerStateContext context) {
    this.rpcEndPoint = rpcEndPoint;
    this.conf = conf;
    this.stateContext = context;
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
   * Get the JobworkerDetails.
   *
   * @return JobworkerDetail
   */
  public JobworkerDetails getJobworkerDetails() {
    return jobworkerDetails;
  }

  /**
   * Set the JobworkerDetails.
   *
   * @param details - JobworkerDetail
   */
  public void setJobworkerDetails(JobworkerDetails details) {
    this.jobworkerDetails = details;
  }

  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  @Override
  public EndpointStates call() throws Exception {
    if (getJobworkerDetails() == null) {
      LOG.error("JobworkerDetails cannot be null in RegisterEndpoint task, " +
          "shutting down the endpoint.");
      return rpcEndPoint.setState(EndpointStates.SHUTDOWN);
    }

    rpcEndPoint.lock();
    try {
      if (rpcEndPoint.getState().equals(EndpointStates.REGISTER)) {
        // TODO jobworker Add Node report (jobworker volume info)
        RegisterJobworkerRequest.Builder requestBuilder = RegisterJobworkerRequest
            .newBuilder()
            .setExtendedJobWorkDetailsProto(jobworkerDetails.getExtendedProtoBufMessage());
        // TODO jobworker support save om version
        RegisterJobworkerResponse response = rpcEndPoint.getEndPoint()
            .registerJobworker(requestBuilder.build());

        Preconditions.checkState(ProtobufUtils.fromProtobuf(response.getJobworkerUUID())
                .equals(jobworkerDetails.getUuid()),
            "Unexpected jobworker ID in the response.");
        Preconditions.checkState(!StringUtils.isBlank(response.getClusterID()),
            "Invalid cluster ID in the response.");
        Preconditions.checkState(!StringUtils.isBlank(response.getOmServiceId()),
            "Invalid OmService ID in the response.");
        Preconditions.checkArgument(response.getOmServiceId().equals(rpcEndPoint.getOMServiceId()),
            "Response OmService ID: " + response.getOmServiceId() + " mismatch current OmService ID: " +
                rpcEndPoint.getOMServiceId());
        if (response.hasHostname()) {
          jobworkerDetails.setHostName(response.getHostname());
        }
        if (response.hasIpAddress()) {
          jobworkerDetails.setIpAddress(response.getIpAddress());
        }
        if (response.hasNetworkName()) {
          jobworkerDetails.setNetworkName(response.getNetworkName());
        }
        if (response.hasNetworkLocation()) {
          jobworkerDetails.setNetworkLocation(response.getNetworkLocation());
        }
        stateContext.addEndpoint(rpcEndPoint.getAddress(), rpcEndPoint.getOMServiceId());
        EndpointStates nextState = rpcEndPoint.getState().getNextState();
        rpcEndPoint.setState(nextState);
        rpcEndPoint.zeroMissedCount();
        // Configure the heartbeat frequency as configuration after the jobworker successfully registered
        this.stateContext.configureHeartbeatFrequency();
      }
    } catch (Exception ex) {
      rpcEndPoint.logIfNeeded(ex);
    } finally {
      rpcEndPoint.unlock();
    }
    return rpcEndPoint.getState();
  }

  /**
   * Builder class for RegisterEndPoint task.
   */
  public static class Builder {
    private JobworkerEndpointStateMachine endPointStateMachine;
    private ConfigurationSource conf;
    private JobworkerDetails jobworkerDetails;
    private JobworkerStateContext context;

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
    public Builder setEndpointStateMachine(JobworkerEndpointStateMachine rpcEndPoint) {
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
     * Sets the JobWorker details.
     *
     * @param details - JobworkerDetail
     * @return Builder
     */
    public Builder setJobworkerDetails(JobworkerDetails details) {
      this.jobworkerDetails = details;
      return this;
    }

    /**
     * Sets the context.
     *
     * @param stateContext - State context.
     * @return Builder
     */
    public Builder setContext(JobworkerStateContext stateContext) {
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

      if (jobworkerDetails == null) {
        LOG.error("No jobworker specified.");
        throw new IllegalArgumentException("A valid JobworkerDetails is needed to " +
            "construct RegisterEndpoint task");
      }

      if (context == null) {
        LOG.error("StateContext is not specified");
        throw new IllegalArgumentException("StateContext is not specified to " +
            "construct RegisterEndpoint task");
      }

      RegisterEndpointTask task = new RegisterEndpointTask(
          this.endPointStateMachine, this.conf, this.context);
      task.setJobworkerDetails(jobworkerDetails);
      return task;
    }
  }
}
