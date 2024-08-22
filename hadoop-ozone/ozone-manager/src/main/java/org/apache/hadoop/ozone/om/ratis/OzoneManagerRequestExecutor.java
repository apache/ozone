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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.ratis;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.apache.hadoop.ozone.protocolPB.RequestHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The OM StateMachine is the state machine for OM Ratis server. It is
 * responsible for applying ratis committed transactions to
 * {@link OzoneManager}.
 */
public class OzoneManagerRequestExecutor {

  public static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerRequestExecutor.class);
  private final SimpleStateMachineStorage storage =
      new SimpleStateMachineStorage();
  private final OzoneManager ozoneManager;
  private RequestHandler handler;
  private final ExecutorService executorService;
  private final String threadPrefix;
  private final AtomicLong cacheIndex = new AtomicLong();

  public OzoneManagerRequestExecutor(OzoneManagerRatisServer ratisServer) throws IOException {
    this.ozoneManager = ratisServer.getOzoneManager();
    this.threadPrefix = ozoneManager.getThreadNamePrefix();

    this.handler = new OzoneManagerRequestHandler(ozoneManager);

    ThreadFactory build = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat(threadPrefix +
            "OMStateMachineApplyTransactionThread - %d").build();
    this.executorService = HadoopExecutors.newSingleThreadExecutor(build);
  }

  /**
   * Validate the incoming update request.
   * @throws IOException when validation fails
   */
  public void validate(OMRequest omRequest) throws IOException {
    handler.validateRequest(omRequest);

    // validate prepare state
    OzoneManagerProtocolProtos.Type cmdType = omRequest.getCmdType();
    OzoneManagerPrepareState prepareState = ozoneManager.getPrepareState();
    if (cmdType == OzoneManagerProtocolProtos.Type.Prepare) {
      // Must authenticate prepare requests here, since we must determine
      // whether or not to apply the prepare gate before proceeding with the
      // prepare request.
      UserGroupInformation userGroupInformation =
          UserGroupInformation.createRemoteUser(
              omRequest.getUserInfo().getUserName());
      if (ozoneManager.getAclsEnabled()
          && !ozoneManager.isAdmin(userGroupInformation)) {
        String message = "Access denied for user " + userGroupInformation
            + ". "
            + "Superuser privilege is required to prepare ozone managers.";
        throw new OMException(message, OMException.ResultCodes.ACCESS_DENIED);
      } else {
        prepareState.enablePrepareGate();
      }
    }

    // In prepare mode, only prepare and cancel requests are allowed to go
    // through.
    if (!prepareState.requestAllowed(cmdType)) {
      String message = "Cannot apply write request " +
          omRequest.getCmdType().name() + " when OM is in prepare mode.";
      throw new OMException(message,
          OMException.ResultCodes.NOT_SUPPORTED_OPERATION_WHEN_PREPARED);
    }
  }

  /*
   * Apply a committed log entry to the state machine.
   */
  public CompletableFuture<OMResponse> submit(OMRequest omRequest) {
    // TODO: throttling max number of request
    try {
      return CompletableFuture.supplyAsync(() -> runCommand(omRequest), executorService);
    } catch (Exception e) {
      return completeExceptionally(e);
    }
  }

  /**
   * Submits write request to OM and returns the response Message.
   * @param request OMRequest
   * @return response from OM
   */
  private OMResponse runCommand(OMRequest request) {
    try {
      validate(request);
      // TODO term Index consistency in HA
      TermIndex termIndex = TermIndex.valueOf(1, cacheIndex.incrementAndGet());
      LOG.warn("sumit..start...{}--{}", request.getCmdType().name(), cacheIndex.get());
      final OMClientResponse omClientResponse = handler.handleWriteRequestImpl(request, termIndex);
      OMLockDetails omLockDetails = omClientResponse.getOmLockDetails();
      OMResponse omResponse = omClientResponse.getOMResponse();
      if (omResponse.getSuccess()) {
        OMResponse dbUpdateRsp = applyDbUpdateViaRatis(request, termIndex, omClientResponse);
        if (dbUpdateRsp != null) {
          // in case of failure, return db update failure response
          return dbUpdateRsp;
        }
      }
      if (omLockDetails != null) {
        return omResponse.toBuilder().setOmLockDetails(omLockDetails.toProtobufBuilder()).build();
      } else {
        return omResponse;
      }
    } catch (IOException e) {
      LOG.warn("Failed to write, Exception occurred ", e);
      return createErrorResponse(request, e);
    } catch (Throwable e) {
      LOG.warn("Failed to write, Exception occurred ", e);
      return createErrorResponse(request, new IOException(e));
    } finally {
      LOG.warn("sumit..end...{}--{}", request.getCmdType().name(), cacheIndex.get());
    }
  }

  @Nullable
  private OMResponse applyDbUpdateViaRatis(OMRequest request, TermIndex termIndex, OMClientResponse omClientResponse)
      throws IOException, ServiceException {
    try (BatchOperation batchOperation = ozoneManager.getMetadataManager().getStore()
        .initBatchOperation()) {
      omClientResponse.checkAndUpdateDB(ozoneManager.getMetadataManager(), batchOperation);
      // get db update and raise request to flush
      OzoneManagerProtocolProtos.PersistDbRequest.Builder reqBuilder
          = OzoneManagerProtocolProtos.PersistDbRequest.newBuilder();
      Map<String, Map<byte[], byte[]>> cachedDbTxs = ((RDBBatchOperation) batchOperation).getCachedTransaction();
      for (Map.Entry<String, Map<byte[], byte[]>> tblEntry : cachedDbTxs.entrySet()) {
        OzoneManagerProtocolProtos.DBTableUpdate.Builder tblBuilder
            = OzoneManagerProtocolProtos.DBTableUpdate.newBuilder();
        tblBuilder.setTableName(tblEntry.getKey());
        for (Map.Entry<byte[], byte[]> kvEntry : tblEntry.getValue().entrySet()) {
          OzoneManagerProtocolProtos.DBTableRecord.Builder kvBuild
              = OzoneManagerProtocolProtos.DBTableRecord.newBuilder();
          kvBuild.setKey(ByteString.copyFrom(kvEntry.getKey()));
          if (kvEntry.getValue() != null) {
            kvBuild.setValue(ByteString.copyFrom(kvEntry.getValue()));
          }
          tblBuilder.addRecords(kvBuild.build());
        }
        reqBuilder.addTableUpdates(tblBuilder.build());
      }
      reqBuilder.setCacheIndex(termIndex.getIndex());
      OMRequest.Builder omReqBuilder = OMRequest.newBuilder().setPersistDbRequest(reqBuilder.build())
          .setCmdType(OzoneManagerProtocolProtos.Type.PersistDb).setClientId(request.getClientId());
      OMResponse response = ozoneManager.getOmRatisServer().submitRequest(omReqBuilder.build());
      if (!response.getSuccess()) {
        // Do update to DB failure need crash OM?
        return response;
      }
    }
    return null;
  }

  private OMResponse createErrorResponse(OMRequest omRequest, IOException exception) {
    OMResponse.Builder omResponseBuilder = OMResponse.newBuilder()
        .setStatus(OzoneManagerRatisUtils.exceptionToResponseStatus(exception))
        .setCmdType(omRequest.getCmdType())
        .setTraceID(omRequest.getTraceID())
        .setSuccess(false);
    if (exception.getMessage() != null) {
      omResponseBuilder.setMessage(exception.getMessage());
    }
    OMResponse omResponse = omResponseBuilder.build();
    return omResponse;
  }

  private static <T> CompletableFuture<T> completeExceptionally(Exception e) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }
}
