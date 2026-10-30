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

import com.google.protobuf.ServiceException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceGrpc;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerCommandType;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ozone.jobworker.protocol.JobworkerProtocol;
import org.apache.hadoop.ozone.om.jobworker.command.OMJobworkerCommandManager;
import org.apache.hadoop.ozone.util.ProtobufUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles gRPC requests for the Jobworker protocol and
 * dispatches them to the protocol implementation.
 */
public class JobworkerGrpcRequestHandler extends
    JobworkerServiceGrpc.JobworkerServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(JobworkerGrpcRequestHandler.class);

  private final JobworkerProtocol jobworkerProtocol;
  private final OzoneProtocolMessageDispatcher<JobworkerRequest,
      JobworkerResponse, JobworkerCommandType> dispatcher;
  private final OMJobworkerCommandManager omJobworkerCommandManager;

  public JobworkerGrpcRequestHandler(
      JobworkerProtocol jobworkerProtocol,
      ProtocolMessageMetrics<JobworkerCommandType> protocolMessageMetrics,
      OMJobworkerCommandManager omJobworkerCommandManager) {
    this.jobworkerProtocol = jobworkerProtocol;
    dispatcher = new OzoneProtocolMessageDispatcher<>("OMJobworkerProtocol",
        protocolMessageMetrics, LOG);
    this.omJobworkerCommandManager = Objects.requireNonNull(omJobworkerCommandManager);
  }

  @Override
  public void submitRequest(JobworkerRequest request,
                            StreamObserver<JobworkerResponse> responseObserver) {
    LOG.debug("JobworkerServiceGrpc: processing request {}", request.getCmdType().name());

    try {
      JobworkerResponse response = dispatcher.processRequest(request, this::processMessage,
          request.getCmdType(), request.getTraceID());
      responseObserver.onNext(response);
      handlePostResponse(request, response);
    } catch (ServiceException e) {
      LOG.error("Failed to process Jobworker request", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
      return;
    }
    responseObserver.onCompleted();
  }

  public JobworkerResponse processMessage(JobworkerRequest request)
      throws ServiceException {
    try {
      JobworkerResponse.Builder responseBuilder = JobworkerResponse.newBuilder()
          .setCmdType(request.getCmdType())
          .setStatus(JobworkerResponse.Status.OK);
      LOG.debug("Received Jobworker request: {}", request);
      switch (request.getCmdType()) {
      case SEND_HEARTBEAT:
        responseBuilder.setSendHeartbeatResponseProto(
            jobworkerProtocol.sendHeartbeat(request.getSendHeartbeatRequest()));
        break;
      case REGISTER_JOBWORKER:
        responseBuilder.setRegisterJobworkerResponse(
            jobworkerProtocol.registerJobworker(request.getRegisterJobworkerRequest()));
        break;
      case GET_OM_VERSION:
        responseBuilder.setGetOMVersionResponse(
            jobworkerProtocol.getOMVersion(request.getGetOMVersionRequest()));
        break;
      default:
        LOG.warn("Received an unknown Jobworker request type: {}", request.getCmdType());
        throw new ServiceException("Unknown command type: " + request.getCmdType());
      }
      return responseBuilder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  private void handlePostResponse(JobworkerRequest request, JobworkerResponse response) {
    try {
      if (request.hasSendHeartbeatRequest() && response.hasSendHeartbeatResponseProto() &&
          !response.getSendHeartbeatResponseProto().getCommandsList().isEmpty()) {

        List<OMJobworkerCommandProto> sentCommands = response.getSendHeartbeatResponseProto().getCommandsList();
        HddsProtos.UUID uuid = request.getSendHeartbeatRequest().getJobworkerDetails().getUuid128();
        UUID jobworkerUuid = ProtobufUtils.fromProtobuf(uuid);
        for (OMJobworkerCommandProto sentCommand : sentCommands) {
          // TODO(JW): We might need to add cmdId to OMJobworkerCommandProto (similar to OMJobworkerCommand), instead
          //  of adding it in the underlying command implementation proto
          LOG.debug("Sent command of type {} to JobWorker {}", sentCommand.getCommandType(),
              jobworkerUuid);
          omJobworkerCommandManager.markCommandSentForJobworker(sentCommand, jobworkerUuid);
        }
      }
    } catch (Exception e) {
      LOG.error("Error when handling command of type {}", request.getCmdType(), e);
    }
  }
}
