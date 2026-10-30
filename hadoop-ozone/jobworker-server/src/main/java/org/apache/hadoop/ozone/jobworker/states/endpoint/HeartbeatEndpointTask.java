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

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatResponseProto;
import org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine;
import org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine.EndpointStates;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommand;
import org.apache.hadoop.ozone.util.ProtobufUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Heartbeat endpoint task for JobWorker.
 */

public class HeartbeatEndpointTask implements Callable<EndpointStates> {

  public static final Logger LOG = LoggerFactory.getLogger(HeartbeatEndpointTask.class);

  private final JobworkerEndpointStateMachine rpcEndpoint;
  private final JobworkerStateContext context;
  private JobworkerDetails jobworkerDetailsProto;
  private static final Map<String, Descriptors.FieldDescriptor> REPORT_TYPE_TO_FIELD_MAP;

  static {
    REPORT_TYPE_TO_FIELD_MAP = new HashMap<>();
    for (Descriptors.FieldDescriptor descriptor : SendHeartbeatRequest.getDescriptor().getFields()) {
      if (descriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
        String heartbeatFieldName = descriptor.getMessageType().getFullName();
        REPORT_TYPE_TO_FIELD_MAP.put(heartbeatFieldName, descriptor);
      }
    }
  }

  /**
   * Constructs an OM heartbeat task.
   *
   * @param rpcEndpoint - rpc Endpoint
   * @param context     - State context
   */
  public HeartbeatEndpointTask(JobworkerEndpointStateMachine rpcEndpoint,
                               JobworkerStateContext context) {
    this.rpcEndpoint = rpcEndpoint;
    this.context = context;
  }

  /**
   * Returns a builder class for HeartbeatEndpointTask.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Get the JobworkerDetails proto.
   *
   * @return JobworkerDetail
   */
  public JobworkerDetails getJobworkerDetails() {
    return jobworkerDetailsProto;
  }

  /**
   * Set JobworkerDetails.
   *
   * @param jobworkerDetails - the jobworker detail
   */
  public void setJobworkerDetails(JobworkerDetails jobworkerDetails) {
    this.jobworkerDetailsProto = jobworkerDetails;
  }

  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  @Override
  public EndpointStates call() throws Exception {
    rpcEndpoint.lock();
    SendHeartbeatRequest request = null;
    try {
      Preconditions.checkState(this.jobworkerDetailsProto != null);

      SendHeartbeatRequest.Builder requestBuilder = SendHeartbeatRequest.newBuilder()
          .setJobworkerDetails(jobworkerDetailsProto.getProtoBufMessage())
          .setOmServiceId(rpcEndpoint.getOMServiceId());
      addLimitedReports(requestBuilder);

      request = requestBuilder.build();
      LOG.debug("Sending heartbeat message to {}: {}",
          rpcEndpoint.getAddress(), request);
      SendHeartbeatResponseProto response = rpcEndpoint.getEndPoint()
          .sendHeartbeat(request);
      processResponse(response);
      rpcEndpoint.setLastSuccessfulHeartbeat(ZonedDateTime.now());
      rpcEndpoint.zeroMissedCount();
    } catch (Exception ex) {
      if (request != null) {
        putBackCommandStatusReports(request);
      }
      rpcEndpoint.logIfNeeded(ex);
    } finally {
      rpcEndpoint.unlock();
    }
    return rpcEndpoint.getState();
  }

  private void putBackCommandStatusReports(SendHeartbeatRequest request) {
    context.putBackCommandStatusReports(request.getOmServiceId(), rpcEndpoint.getAddress(),
        request.getCommandStatusReportsList());
  }

  /**
   * Process the heartbeat response from OM.
   *
   * @param response the response from OM
   */
  private void processResponse(SendHeartbeatResponseProto response) throws IOException {
    Preconditions.checkState(ProtobufUtils.fromProtobuf(response.getJobworkerUUID())
            .equals(jobworkerDetailsProto.getUuid()),
        "Unexpected jobworker ID in the response, expected %s but %s.",
        jobworkerDetailsProto.getUuidString(), ProtobufUtils.fromProtobuf(response.getJobworkerUUID()).toString());
    Preconditions.checkState(response.getOmServiceId().equals(rpcEndpoint.getOMServiceId()),
        "Unexpected OM Service ID, expected %s but %s.",
        rpcEndpoint.getOMServiceId(), response.getOmServiceId());
    LOG.debug("heartbeat from Service ID, {} ", rpcEndpoint.getOMServiceId());
    // Handle term if HA is being used
    if (response.hasTerm()) {
      context.updateTermOfLeaderOM(rpcEndpoint.getOMServiceId(), response.getTerm());
    }
    // Process commands
    for (OMJobworkerCommandProto commandProto : response.getCommandsList()) {
      processCommandProto(commandProto, response.getOmServiceId());
    }
  }

  /**
   * Process the command proto from the OM.
   *
   * @param commandProto the command proto
   */
  private void processCommandProto(OMJobworkerCommandProto commandProto, String omServiceId) {
    OMJobworkerCommandProto.Type cmdType = commandProto.getCommandType();
    try {
      switch (cmdType) {
      case reregisterCommand:
        processReregisterCommand();
        break;
      default:
        // Each command carries the term of the leader that issued it, so a late command from an
        // older leader is dropped even if it arrives in the response of a newer leader.
        if (!commandProto.hasTerm()) {
          LOG.warn("Ignoring command {} type {} from omServiceId {} without a leader term",
              commandProto.getCmdId(), cmdType, omServiceId);
          return;
        }
        context.addCommand(JobworkerCommand.fromEnvelope(commandProto, omServiceId, commandProto.getTerm()));
      }
    } catch (Exception e) {
      LOG.error("Error processing command type {} omServiceId {}", cmdType, omServiceId, e);
    }
  }

  /**
   * Process a reregister command.
   */
  private void processReregisterCommand() {
    if (rpcEndpoint.getState() == EndpointStates.HEARTBEAT) {
      LOG.info("Received OM notification to register. "
          + "Interrupt HEARTBEAT and transit to GETVERSION state.");
      // Prevents high OM stress caused by bulk registration.
      // Based on InitializeHeartbeatFrequency to add a random offset, spread the registration time.
      long baseFrequency = context.getInitializeHeartbeatFrequencyMs();
      long randomOffset = ThreadLocalRandom.current().nextLong(baseFrequency * 2);
      this.context.configureHeartbeatHeartbeatFrequencyMs(baseFrequency + randomOffset);
      rpcEndpoint.setState(EndpointStates.GETVERSION);
    } else {
      LOG.debug("Illegal state {} found, expecting {}.",
          rpcEndpoint.getState().name(), EndpointStates.HEARTBEAT);
    }
  }

  /**
   * Builder class for HeartbeatEndpointTask.
   */
  public static class Builder {
    private JobworkerEndpointStateMachine endPointStateMachine;
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

    /**
     * Build the HeartbeatEndpointTask.
     *
     * @return HeartbeatEndpointTask
     */
    public HeartbeatEndpointTask build() {
      if (endPointStateMachine == null) {
        LOG.error("No endpoint specified.");
        throw new IllegalArgumentException("A valid endpoint state machine is" +
            " needed to construct HeartbeatEndpointTask task");
      }

      if (jobworkerDetails == null) {
        LOG.error("No jobworker specified.");
        throw new IllegalArgumentException("A valid JobworkerDetails is needed to " +
            "construct HeartbeatEndpointTask task");
      }

      if (context == null) {
        LOG.error("StateContext is not specified");
        throw new IllegalArgumentException("StateContext is not specified to " +
            "construct HeartbeatEndpointTask task");
      }

      HeartbeatEndpointTask task = new HeartbeatEndpointTask(
          this.endPointStateMachine, this.context);
      task.setJobworkerDetails(jobworkerDetails);
      return task;
    }
  }

  /**
   * Add a limited number of reports to heartbeat.
   *
   * @param requestBuilder builder to which the report has to be added.
   */
  private void addLimitedReports(SendHeartbeatRequest.Builder requestBuilder) {
    for (Message report : context.getParent().getReportManager()
        .getLimitedCountAvailableReports(rpcEndpoint.getOMServiceId(), rpcEndpoint.getAddress())) {
      String reportName = report.getDescriptorForType().getFullName();
      Descriptors.FieldDescriptor descriptor = REPORT_TYPE_TO_FIELD_MAP.get(reportName);

      if (descriptor != null) {
        if (descriptor.isRepeated()) {
          requestBuilder.addRepeatedField(descriptor, report);
        } else {
          requestBuilder.setField(descriptor, report);
        }
      }
    }
  }
}
