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
package org.apache.hadoop.ozone.om.ratis.execution;

import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.ipc.ProcessingDetails;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.lock.OmLockOpr;
import org.apache.hadoop.ozone.om.ratis.execution.factory.OmRequestFactory;
import org.apache.hadoop.ozone.om.ratis.execution.request.OMRequestBase;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ipc.RpcConstants.DUMMY_CLIENT_ID;
import static org.apache.hadoop.ipc.RpcConstants.INVALID_CALL_ID;

/**
 * entry for request execution.
 */
public class OMGateway {
  private static final Logger LOG = LoggerFactory.getLogger(OMGateway.class);
  private final LeaderRequestExecutor leaderExecutor;
  private final OzoneManager om;
  private final OmLockOpr omLockOpr;
  private final AtomicLong requestInProgress = new AtomicLong(0);
  private final AtomicLong uniqueIndex = new AtomicLong();

  public OMGateway(OzoneManager om) throws IOException {
    this.om = om;
    this.omLockOpr = new OmLockOpr(om.getThreadNamePrefix());
    this.leaderExecutor = new LeaderRequestExecutor(om, uniqueIndex);
    if (om.isRatisEnabled()) {
      om.getOmRatisServer().getOmStateMachine().getPostExecutor().registerIndexNotifier(this::indexNotifier);
    } else {
      uniqueIndex.set(PostExecutor.initLeaderIndex(om));
    }
  }

  public void start() {
    omLockOpr.start();
    leaderExecutor.start();
  }
  public void stop() {
    leaderExecutor.setEnabled(false);
    leaderExecutor.stop();
    omLockOpr.stop();
  }

  public OMResponse submit(OMRequest omRequest, byte[] clientId, long callId) throws ServiceException {
    // TODO handle replay of request for external request
    return submitInternal(omRequest, getClientId(clientId), getCallId(callId));
  }

  public OMResponse submit(OMRequest omRequest) throws ServiceException {
    return submitInternal(omRequest, null, 0L);
  }

  public OMResponse submitInternal(OMRequest omRequest, String clientId, long callId) throws ServiceException {
    requestInProgress.incrementAndGet();
    RequestContext requestContext = new RequestContext();
    requestContext.setRequest(omRequest);
    requestContext.setFuture(new CompletableFuture<>());
    CompletableFuture<OMResponse> f = requestContext.getFuture().whenComplete(
        (r, th) -> handleAfterExecution(requestContext, th));
    OmLockOpr.OmLockInfo lockInfo = null;
    try {
      OMRequestBase requestBase = OmRequestFactory.createClientRequest(omRequest, om);
      OMRequest request = requestBase.preProcess(om);
      requestContext.setRequest(request);
      requestContext.setUuidClientId(clientId);
      requestContext.setCallId(callId);
      requestContext.setRequestBase(requestBase);
      requestContext.getRequestBase().authorize(om);
      lockInfo = requestContext.getRequestBase().lock(omLockOpr);
      validate(requestContext.getRequest());
      ensurePreviousRequestCompletionForPrepare(requestContext.getRequest());

      // submit request
      leaderExecutor.submit(requestContext);

      try {
        return f.get();
      } catch (ExecutionException ex) {
        if (ex.getCause() != null) {
          throw new ServiceException(ex.getMessage(), ex.getCause());
        } else {
          throw new ServiceException(ex.getMessage(), ex);
        }
      }
    } catch (InterruptedException e) {
      LOG.error("Interrupted while handling request", e);
      Thread.currentThread().interrupt();
      throw new ServiceException(e.getMessage(), e);
    } catch (ServiceException e) {
      throw e;
    } catch (Throwable e) {
      LOG.error("Exception occurred while handling request", e);
      throw new ServiceException(e.getMessage(), e);
    } finally {
      if (null != lockInfo) {
        requestContext.getRequestBase().unlock(omLockOpr, lockInfo);
        Server.Call call = Server.getCurCall().get();
        if (null != call) {
          call.getProcessingDetails().add(ProcessingDetails.Timing.LOCKWAIT,
              lockInfo.getWaitLockNanos(), TimeUnit.NANOSECONDS);
          call.getProcessingDetails().add(ProcessingDetails.Timing.LOCKSHARED,
              lockInfo.getReadLockNanos(), TimeUnit.NANOSECONDS);
          call.getProcessingDetails().add(ProcessingDetails.Timing.LOCKEXCLUSIVE,
              lockInfo.getWriteLockNanos(), TimeUnit.NANOSECONDS);
        }
      }
    }
  }

  private void ensurePreviousRequestCompletionForPrepare(OMRequest omRequest) throws InterruptedException {
    // if a prepare request, other request will be discarded before calling this
    if (omRequest.getCmdType() == OzoneManagerProtocolProtos.Type.Prepare) {
      for (int cnt = 0; cnt < 60 && requestInProgress.get() > 1; ++cnt) {
        Thread.sleep(1000);
      }
      if (requestInProgress.get() > 1) {
        LOG.warn("Still few requests {} are in progress, continuing with prepare", (requestInProgress.get() - 1));
      }
    }
  }

  private synchronized void validate(OMRequest omRequest) throws IOException {
    OzoneManagerRequestHandler.requestParamValidation(omRequest);
    // validate prepare state
    OzoneManagerProtocolProtos.Type cmdType = omRequest.getCmdType();
    OzoneManagerPrepareState prepareState = om.getPrepareState();
    if (cmdType == OzoneManagerProtocolProtos.Type.Prepare) {
      // Must authenticate prepare requests here, since we must determine
      // whether or not to apply the prepare gate before proceeding with the
      // prepare request.
      UserGroupInformation userGroupInformation =
          UserGroupInformation.createRemoteUser(omRequest.getUserInfo().getUserName());
      if (om.getAclsEnabled() && !om.isAdmin(userGroupInformation)) {
        String message = "Access denied for user " + userGroupInformation + ". "
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
      throw new OMException(message, OMException.ResultCodes.NOT_SUPPORTED_OPERATION_WHEN_PREPARED);
    }
  }
  private void handleAfterExecution(RequestContext ctx, Throwable th) {
    requestInProgress.decrementAndGet();
  }

  public void indexNotifier(long index) {
    long lastIdx = uniqueIndex.get();
    if (lastIdx < index) {
      uniqueIndex.getAndAdd(index - lastIdx);
    }
  }

  private String getClientId(byte[] clientIdBytes) {
    if (!om.isTestSecureOmFlag()) {
      Preconditions.checkArgument(clientIdBytes != DUMMY_CLIENT_ID);
    }
    return UUID.nameUUIDFromBytes(clientIdBytes).toString();
  }

  private long getCallId(long callId) {
    if (!om.isTestSecureOmFlag()) {
      Preconditions.checkArgument(callId != INVALID_CALL_ID);
    }
    return callId;
  }
}
