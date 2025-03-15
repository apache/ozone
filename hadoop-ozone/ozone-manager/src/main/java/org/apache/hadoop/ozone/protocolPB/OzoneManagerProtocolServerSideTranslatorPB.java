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

package org.apache.hadoop.ozone.protocolPB;

import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.LEADER_AND_READY;
import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.NOT_LEADER;
import static org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils.createClientRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.PrepareStatus;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ipc.ProcessingDetails.Timing;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNodeIdMismatchException;
import org.apache.hadoop.ozone.om.helpers.OMAuditLogger;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.validation.RequestValidations;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.S3SecurityUtil;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the server-side translator that forwards requests received
 * from {@link OzoneManagerProtocolPB} to {@link OzoneManager}.
 */
public class OzoneManagerProtocolServerSideTranslatorPB implements OzoneManagerProtocolPB {
  private static final Logger LOG = LoggerFactory .getLogger(OzoneManagerProtocolServerSideTranslatorPB.class);
  private static final String OM_REQUESTS_PACKAGE = "org.apache.hadoop.ozone";

  private final OzoneManagerRatisServer omRatisServer;
  private final RequestHandler handler;
  private final OzoneManager ozoneManager;
  private final OzoneProtocolMessageDispatcher<OMRequest, OMResponse,
      ProtocolMessageEnum> dispatcher;
  private final RequestValidations requestValidations;
  private final OMPerformanceMetrics perfMetrics;

  private OMRequest lastRequestToSubmit;


  /**
   * Constructs an instance of the server handler.
   *
   * @param impl OzoneManagerProtocolPB
   */
  public OzoneManagerProtocolServerSideTranslatorPB(
      OzoneManager impl,
      OzoneManagerRatisServer ratisServer,
      ProtocolMessageMetrics<ProtocolMessageEnum> metrics) {
    this.ozoneManager = impl;
    this.perfMetrics = impl.getPerfMetrics();

    this.handler = new OzoneManagerRequestHandler(impl);
    this.omRatisServer = ratisServer;
    dispatcher = new OzoneProtocolMessageDispatcher<>("OzoneProtocol",
        metrics, LOG, OMPBHelper::processForDebug, OMPBHelper::processForDebug);

    // TODO: make this injectable for testing...
    this.requestValidations = new RequestValidations()
        .fromPackage(OM_REQUESTS_PACKAGE)
        .withinContext(ValidationContext.of(ozoneManager.getVersionManager(), ozoneManager.getMetadataManager()))
        .load();
  }

  /**
   * Submit mutating requests to Ratis server in OM, and process read requests.
   */
  @Override
  public OMResponse submitRequest(RpcController controller,
      OMRequest request) throws ServiceException {
    OMRequest validatedRequest;
    try {
      validatedRequest = captureLatencyNs(
          perfMetrics.getValidateRequestLatencyNs(),
          () -> requestValidations.validateRequest(request));
    } catch (Exception e) {
      if (e instanceof OMException) {
        return createErrorResponse(request, (OMException) e);
      }
      throw new ServiceException(e);
    }

    OMResponse response = dispatcher.processRequest(validatedRequest,
        this::processRequest, request.getCmdType(), request.getTraceID());

    return captureLatencyNs(perfMetrics.getValidateResponseLatencyNs(),
        () -> requestValidations.validateResponse(request, response));
  }

  @VisibleForTesting
  public OMResponse processRequest(OMRequest request) throws ServiceException {
    OMResponse response = internalProcessRequest(request);
    if (response.hasOmLockDetails()) {
      OzoneManagerProtocolProtos.OMLockDetailsProto omLockDetailsProto =
          response.getOmLockDetails();
      Server.Call call = Server.getCurCall().get();
      if (call != null) {
        call.getProcessingDetails().add(Timing.LOCKWAIT,
            omLockDetailsProto.getWaitLockNanos(), TimeUnit.NANOSECONDS);
        call.getProcessingDetails().add(Timing.LOCKSHARED,
            omLockDetailsProto.getReadLockNanos(), TimeUnit.NANOSECONDS);
        call.getProcessingDetails().add(Timing.LOCKEXCLUSIVE,
            omLockDetailsProto.getWriteLockNanos(), TimeUnit.NANOSECONDS);
      }
    }
    return response;
  }

  private OMResponse internalProcessRequest(OMRequest request) throws ServiceException {
    boolean s3Auth = false;

    try {
      if (request.hasS3Authentication()) {
        OzoneManager.setS3Auth(request.getS3Authentication());
        try {
          s3Auth = true;
          // If request has S3Authentication, validate S3 credentials.
          // If current OM is leader and then proceed with the request.
          S3SecurityUtil.validateS3Credential(request, ozoneManager);
        } catch (IOException ex) {
          return createErrorResponse(request, ex);
        }
      }

      if (OmUtils.isReadOnly(request)) {
        return submitReadRequestToOM(request);
      }

      // To validate credentials we have already verified leader status.
      // This will skip of checking leader status again if request has S3Auth.
      if (!s3Auth) {
        OzoneManagerRatisUtils.checkLeaderStatus(ozoneManager);
      }

      // check retry cache
      final OMResponse cached = omRatisServer.checkRetryCache();
      if (cached != null) {
        return cached;
      }

      // process new request
      OMClientRequest omClientRequest = null;
      final OMRequest requestToSubmit;
      try {
        omClientRequest = createClientRequest(request, ozoneManager);
        // TODO: Note: Due to HDDS-6055, createClientRequest() could now
        //  return null, which triggered the findbugs warning.
        //  Added the assertion.
        assert (omClientRequest != null);
        OMClientRequest finalOmClientRequest = omClientRequest;

        requestToSubmit = preExecute(finalOmClientRequest);
        this.lastRequestToSubmit = requestToSubmit;
      } catch (IOException ex) {
        if (omClientRequest != null) {
          OMAuditLogger.log(omClientRequest.getAuditBuilder());
          omClientRequest.handleRequestFailure(ozoneManager);
        }
        return createErrorResponse(request, ex);
      }

      final OMResponse response = omRatisServer.submitRequest(requestToSubmit);
      if (!response.getSuccess()) {
        omClientRequest.handleRequestFailure(ozoneManager);
      }
      return response;
    } finally {
      OzoneManager.setS3Auth(null);
    }
  }

  private OMRequest preExecute(OMClientRequest finalOmClientRequest)
      throws IOException {
    return captureLatencyNs(perfMetrics.getPreExecuteLatencyNs(),
        () -> finalOmClientRequest.preExecute(ozoneManager));
  }

  @VisibleForTesting
  public OMRequest getLastRequestToSubmit() {
    return lastRequestToSubmit;
  }

  private OMResponse submitReadRequestToOM(OMRequest request)
      throws ServiceException {
    // Check if this OM is the leader.
    RaftServerStatus raftServerStatus = omRatisServer.checkLeaderStatus();
    boolean isNodeIdSpecified = validateNodeId(request);
    if (raftServerStatus == LEADER_AND_READY || request.getCmdType().equals(PrepareStatus)) {
      return handler.handleReadRequest(request);
    } else if (raftServerStatus == NOT_LEADER && isNodeIdSpecified && allowSnapshotReadFromFollower(request)) {
      return handler.handleReadRequest(request);
    } else {
      throw createLeaderErrorException(raftServerStatus);
    }
  }


  /**
   * Check whether the OM request specified an OM node ID.
   * @param omRequest OM request.
   * @return true if the request specified an OM node ID and it is the same as the current OM, false otherwise.
   * @throws ServiceException exception if there is a mismatch between the OM request's node ID and the
   *         current OM Node ID.
   */
  private boolean validateNodeId(OMRequest omRequest) throws ServiceException {
    if (!omRequest.hasOmNodeId()) {
      return false;
    }

    if (!omRequest.getOmNodeId().equals(ozoneManager.getOMNodeId())) {
      throw new ServiceException(new OMNodeIdMismatchException("OM request node ID is " + omRequest.getOmNodeId() +
          ", but the current OM node ID is " + ozoneManager.getOMNodeId()));
    }

    return true;
  }

  private boolean allowSnapshotReadFromFollower(OMRequest omRequest) {
    switch (omRequest.getCmdType()) {
    case SnapshotDiff:
    case ListSnapshot:
    case GetSnapshotInfo:
    case ListSnapshotDiffJobs:
    case CancelSnapshotDiff:
      return true;
    default:
      return false;
    }
  }

  private ServiceException createLeaderErrorException(
      RaftServerStatus raftServerStatus) {
    if (raftServerStatus == NOT_LEADER) {
      return new ServiceException(omRatisServer.newOMNotLeaderException());
    } else {
      return createLeaderNotReadyException();
    }
  }

  private ServiceException createLeaderNotReadyException() {
    RaftPeerId raftPeerId = omRatisServer.getRaftPeerId();

    OMLeaderNotReadyException leaderNotReadyException =
        new OMLeaderNotReadyException(raftPeerId.toString() + " is Leader " +
            "but not ready to process request yet.");

    LOG.debug(leaderNotReadyException.getMessage());

    return new ServiceException(leaderNotReadyException);
  }

  /** @return an {@link OMResponse} from the given {@link OMRequest} and the given exception. */
  private OMResponse createErrorResponse(
      OMRequest omRequest, IOException exception) {
    // Added all write command types here, because in future if any of the
    // preExecute is changed to return IOException, we can return the error
    // OMResponse to the client.
    OMResponse.Builder omResponse = OMResponse.newBuilder()
        .setStatus(OzoneManagerRatisUtils.exceptionToResponseStatus(exception))
        .setCmdType(omRequest.getCmdType())
        .setTraceID(omRequest.getTraceID())
        .setSuccess(false);
    if (exception.getMessage() != null) {
      omResponse.setMessage(exception.getMessage());
    }
    return omResponse.build();
  }

  public static Logger getLog() {
    return LOG;
  }
}
