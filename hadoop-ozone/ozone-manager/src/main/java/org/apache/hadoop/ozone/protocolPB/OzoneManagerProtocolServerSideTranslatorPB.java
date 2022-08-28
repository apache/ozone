/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.protocolPB;

import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.LEADER_AND_READY;
import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.NOT_LEADER;
import static org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils.createClientRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.PrepareStatus;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.OMClientRequestUtils;
import org.apache.hadoop.ozone.om.request.util.ObjectParser;
import org.apache.hadoop.ozone.om.request.validation.RequestValidations;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneObj.ObjectType;

import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.ozone.security.S3SecurityUtil;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link OzoneManagerProtocolPB}
 * to the OzoneManagerService server implementation.
 */
public class OzoneManagerProtocolServerSideTranslatorPB implements
    OzoneManagerProtocolPB {
  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneManagerProtocolServerSideTranslatorPB.class);
  private static final String OM_REQUESTS_PACKAGE = 
      "org.apache.hadoop.ozone";
  
  private final OzoneManagerRatisServer omRatisServer;
  private final RequestHandler handler;
  private final boolean isRatisEnabled;
  private final OzoneManager ozoneManager;
  private final OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer;
  private final AtomicLong transactionIndex;
  private final OzoneProtocolMessageDispatcher<OMRequest, OMResponse,
      ProtocolMessageEnum> dispatcher;
  private final RequestValidations requestValidations;

  /**
   * Constructs an instance of the server handler.
   *
   * @param impl OzoneManagerProtocolPB
   */
  public OzoneManagerProtocolServerSideTranslatorPB(
      OzoneManager impl,
      OzoneManagerRatisServer ratisServer,
      ProtocolMessageMetrics<ProtocolMessageEnum> metrics,
      boolean enableRatis,
      long lastTransactionIndexForNonRatis) {
    this.ozoneManager = impl;
    this.isRatisEnabled = enableRatis;
    // Update the transactionIndex with the last TransactionIndex read from DB.
    // New requests should have transactionIndex incremented from this index
    // onwards to ensure unique objectIDs.
    this.transactionIndex = new AtomicLong(lastTransactionIndexForNonRatis);

    if (isRatisEnabled) {
      // In case of ratis is enabled, handler in ServerSideTransaltorPB is used
      // only for read requests and read requests does not require
      // double-buffer to be initialized.
      this.ozoneManagerDoubleBuffer = null;
      handler = new OzoneManagerRequestHandler(impl, null);
    } else {
      this.ozoneManagerDoubleBuffer = new OzoneManagerDoubleBuffer.Builder()
          .setOmMetadataManager(ozoneManager.getMetadataManager())
          // Do nothing.
          // For OM NON-HA code, there is no need to save transaction index.
          // As we wait until the double buffer flushes DB to disk.
          .setOzoneManagerRatisSnapShot((i) -> {
          })
          .enableRatis(isRatisEnabled)
          .enableTracing(TracingUtil.isTracingEnabled(
              ozoneManager.getConfiguration()))
          .build();
      handler = new OzoneManagerRequestHandler(impl, ozoneManagerDoubleBuffer);
    }
    this.omRatisServer = ratisServer;
    dispatcher = new OzoneProtocolMessageDispatcher<>("OzoneProtocol",
        metrics, LOG, OMPBHelper::processForDebug, OMPBHelper::processForDebug);
    // TODO: make this injectable for testing...
    requestValidations =
        new RequestValidations()
            .fromPackage(OM_REQUESTS_PACKAGE)
            .withinContext(
                ValidationContext.of(ozoneManager.getVersionManager(),
                    ozoneManager.getMetadataManager()))
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
      // Note:
      // We need to call associateBucketIdWithRequest before we run any
      // decision-making / validation logic. This is because we do not
      // yet hold any locks on any buckets, and if we perform validation
      // before bucket ID association - we might end up with a situation
      // where we have validated a bucket's properties, which end up changing
      // before we enter validateAndUpdateCache. This would case false
      // positive validations.
      // We want to associate a bucket ID before we do any of this, so that
      // even if the underlying bucket changes, we catch that change
      // inside the validateAndUpdateCache method.
      OMRequest requestWithBucketId = associateBucketIdWithRequest(request);
      validatedRequest =
          requestValidations.validateRequest(requestWithBucketId);
    } catch (Exception e) {
      if (e instanceof OMException) {
        return createErrorResponse(request, (OMException) e);
      }
      throw new ServiceException(e);
    }

    OMResponse response = 
        dispatcher.processRequest(validatedRequest, this::processRequest,
        request.getCmdType(), request.getTraceID());
    
    return requestValidations.validateResponse(request, response);
  }

  private OMResponse processRequest(OMRequest request) throws
      ServiceException {
    OMClientRequest omClientRequest = null;

    boolean s3Auth = false;
    try {
      if (request.hasS3Authentication()) {
        OzoneManager.setS3Auth(request.getS3Authentication());
        try {
          s3Auth = true;
          // If Request has S3Authentication validate S3 credentials
          // if current OM is leader and then proceed with
          // processing the request.
          S3SecurityUtil.validateS3Credential(request, ozoneManager);
        } catch (IOException ex) {
          // If validate credentials fail return error OM Response.
          return createErrorResponse(request, ex);
        }
      }
      if (isRatisEnabled) {
        // Check if the request is a read only request
        if (OmUtils.isReadOnly(request)) {
          return submitReadRequestToOM(request);
        } else {
          // To validate credentials we have already verified leader status.
          // This will skip of checking leader status again if request has
          // S3Auth.
          if (!s3Auth) {
            OzoneManagerRatisUtils.checkLeaderStatus(ozoneManager);
          }
          try {
            omClientRequest =
                createClientRequest(request, ozoneManager);
            // TODO: Note: Due to HDDS-6055, createClientRequest() could now
            //  return null, which triggered the findbugs warning.
            //  Added the assertion.
            assert (omClientRequest != null);
            request = omClientRequest.preExecute(ozoneManager);
          } catch (IOException ex) {
            // As some of the preExecute returns error. So handle here.
            if (omClientRequest != null) {
              omClientRequest.handleRequestFailure(ozoneManager);
            }
            return createErrorResponse(request, ex);
          }
          OMResponse response = submitRequestToRatis(request);
          if (!response.getSuccess()) {
            omClientRequest.handleRequestFailure(ozoneManager);
          }
          return response;
        }
      } else {
        return submitRequestDirectlyToOM(request);
      }
    } finally {
      OzoneManager.setS3Auth(null);
    }
  }

  /**
   * Submits request to OM's Ratis server.
   */
  private OMResponse submitRequestToRatis(OMRequest request)
      throws ServiceException {
    return omRatisServer.submitRequest(request);
  }

  private OMResponse submitReadRequestToOM(OMRequest request)
      throws ServiceException {
    // Check if this OM is the leader.
    RaftServerStatus raftServerStatus = omRatisServer.checkLeaderStatus();
    if (raftServerStatus == LEADER_AND_READY ||
        request.getCmdType().equals(PrepareStatus)) {
      return handler.handleReadRequest(request);
    } else {
      throw createLeaderErrorException(raftServerStatus);
    }
  }

  private ServiceException createLeaderErrorException(
      RaftServerStatus raftServerStatus) {
    if (raftServerStatus == NOT_LEADER) {
      return createNotLeaderException();
    } else {
      return createLeaderNotReadyException();
    }
  }

  private ServiceException createNotLeaderException() {
    RaftPeerId raftPeerId = omRatisServer.getRaftPeerId();

    // TODO: Set suggest leaderID. Right now, client is not using suggest
    // leaderID. Need to fix this.

    OMNotLeaderException notLeaderException =
        new OMNotLeaderException(raftPeerId);

    LOG.debug(notLeaderException.getMessage());

    return new ServiceException(notLeaderException);
  }

  private ServiceException createLeaderNotReadyException() {
    RaftPeerId raftPeerId = omRatisServer.getRaftPeerId();

    OMLeaderNotReadyException leaderNotReadyException =
        new OMLeaderNotReadyException(raftPeerId.toString() + " is Leader " +
            "but not ready to process request yet.");

    LOG.debug(leaderNotReadyException.getMessage());

    return new ServiceException(leaderNotReadyException);
  }

  /**
   * Submits request directly to OM.
   */
  private OMResponse submitRequestDirectlyToOM(OMRequest request) throws
      ServiceException {
    OMClientResponse omClientResponse = null;
    long index = 0L;
    try {
      if (OmUtils.isReadOnly(request)) {
        return handler.handleReadRequest(request);
      } else {
        OMClientRequest omClientRequest =
            createClientRequest(request, ozoneManager);
        request = omClientRequest.preExecute(ozoneManager);
        index = transactionIndex.incrementAndGet();
        omClientResponse = handler.handleWriteRequest(request, index);
      }
    } catch (IOException ex) {
      // As some of the preExecute returns error. So handle here.
      return createErrorResponse(request, ex);
    }
    try {
      omClientResponse.getFlushFuture().get();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Future for {} is completed", request);
      }
    } catch (ExecutionException | InterruptedException ex) {
      // terminate OM. As if we are in this stage means, while getting
      // response from flush future, we got an exception.
      String errorMessage = "Got error during waiting for flush to be " +
          "completed for " + "request" + request.toString();
      ExitUtils.terminate(1, errorMessage, ex, LOG);
      Thread.currentThread().interrupt();
    }
    return omClientResponse.getOMResponse();
  }

  /**
   * Create OMResponse from the specified OMRequest and exception.
   *
   * @param omRequest
   * @param exception
   * @return OMResponse
   */
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

  /**
   * Associate bucket ID with requests that have an associated bucket.
   * Returns a new OMRequest object with the bucket ID associated.
   *
   * @param omRequest OMRequest
   * @return OMRequest with bucket ID associated.
   * @throws IOException
   */
  @SuppressWarnings("checkstyle:MethodLength")
  private OMRequest associateBucketIdWithRequest(OMRequest omRequest)
      throws IOException {
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    String volumeName = "";
    String bucketName = "";

    OzoneManagerProtocolProtos.KeyArgs keyArgs;
    OzoneObj obj;
    ObjectParser objectParser;
    ObjectType type;

    switch (omRequest.getCmdType()) {
    case AddAcl:
      type = omRequest.getAddAclRequest().getObj().getResType();
      // No need for bucket ID validation in case of volume ACL
      if (ObjectType.VOLUME == type) {
        break;
      }

      obj =
          OzoneObjInfo.fromProtobuf(omRequest.getAddAclRequest().getObj());
      objectParser = new ObjectParser(obj.getPath(), type);

      volumeName = objectParser.getVolume();
      bucketName = objectParser.getBucket();
      break;
    case RemoveAcl:
      type = omRequest.getAddAclRequest().getObj().getResType();
      // No need for bucket ID validation in case of volume ACL
      if (ObjectType.VOLUME == type) {
        break;
      }

      obj =
          OzoneObjInfo.fromProtobuf(omRequest.getRemoveAclRequest().getObj());
      objectParser = new ObjectParser(obj.getPath(), type);

      volumeName = objectParser.getVolume();
      bucketName = objectParser.getBucket();
      break;
    case SetAcl:
      type = omRequest.getAddAclRequest().getObj().getResType();
      // No need for bucket ID validation in case of volume ACL
      if (ObjectType.VOLUME == type) {
        break;
      }

      obj =
          OzoneObjInfo.fromProtobuf(omRequest.getSetAclRequest().getObj());
      objectParser = new ObjectParser(obj.getPath(), type);

      volumeName = objectParser.getVolume();
      bucketName = objectParser.getBucket();
      break;
    case DeleteBucket:
      volumeName = omRequest.getDeleteBucketRequest().getVolumeName();
      bucketName = omRequest.getDeleteBucketRequest().getBucketName();
      break;
    case CreateDirectory:
      keyArgs = omRequest.getCreateDirectoryRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case CreateFile:
      keyArgs = omRequest.getCreateFileRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case CreateKey:
      keyArgs = omRequest.getCreateKeyRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case AllocateBlock:
      keyArgs = omRequest.getAllocateBlockRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case CommitKey:
      keyArgs = omRequest.getCommitKeyRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case DeleteKey:
      keyArgs = omRequest.getDeleteKeyRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case DeleteKeys:
      OzoneManagerProtocolProtos.DeleteKeyArgs deleteKeyArgs =
          omRequest.getDeleteKeysRequest()
              .getDeleteKeys();
      volumeName = deleteKeyArgs.getVolumeName();
      bucketName = deleteKeyArgs.getBucketName();
      break;
    case RenameKey:
      keyArgs = omRequest.getRenameKeyRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case RenameKeys:
      OzoneManagerProtocolProtos.RenameKeysArgs renameKeysArgs =
          omRequest.getRenameKeysRequest().getRenameKeysArgs();
      volumeName = renameKeysArgs.getVolumeName();
      bucketName = renameKeysArgs.getBucketName();
      break;
    case InitiateMultiPartUpload:
      keyArgs = omRequest.getInitiateMultiPartUploadRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case CommitMultiPartUpload:
      keyArgs = omRequest.getCommitMultiPartUploadRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case AbortMultiPartUpload:
      keyArgs = omRequest.getAbortMultiPartUploadRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case CompleteMultiPartUpload:
      keyArgs = omRequest.getCompleteMultiPartUploadRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    default:
      // do nothing in case of other requests.
      LOG.debug(
          "Bucket ID validation is not enabled for " + omRequest.getCmdType() +
              ". Bucket ID will not be associated with this request.");
      break;
    }

    // Check if there is any bucket associated with this request.
    if (bucketName.equals("") && volumeName.equals("")) {
      return omRequest;
    }

    long bucketId;
    try {
      // Note: Even though this block of code is not executing under a bucket
      // lock - it is still safe.
      // For instance, consider the following link bucket chain:
      // l1 -> l2 -> l3 -> abc (ID: 1000)
      // Let's say we fetch the resolved bucket name for l1. This would mean
      // the source bucket is resolved as 'abc'.
      // Now, let's assume that before the next line of code (to fetch bucket
      // ID) executes, the link structure changes as follows:
      // l1- > l2 -> l3 -> xyz (ID: 1001)
      // And we end up associating the bucket ID for l1 with abc (1000) even
      // though the actual link has changed.
      // This is not a problem, since we will anyway be validating this bucket
      // ID inside validateAndUpdateCache method - and it will be caught there.
      // This is a fail-slow approach.
      bucketId =
          OMClientRequestUtils.getResolvedBucketId(ozoneManager, volumeName,
              bucketName);
    } catch (OMException oe) {
      // Ignore exceptions at this stage, let respective classes handle them.
      LOG.debug(
          "There was an error while fetching bucket ID for " + volumeName +
              "/" + bucketName + ".", oe);
      return omRequest;
    }

    return OMRequest.newBuilder(omRequest)
        .setAssociatedBucketId(bucketId)
        .build();
  }

  public void stop() {
    if (!isRatisEnabled) {
      ozoneManagerDoubleBuffer.stop();
    }
  }

  public static Logger getLog() {
    return LOG;
  }
}
