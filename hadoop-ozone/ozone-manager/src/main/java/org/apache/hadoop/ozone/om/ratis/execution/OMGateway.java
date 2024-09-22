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

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ipc.ProcessingDetails;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.lock.OmLockOpr;
import org.apache.hadoop.ozone.om.lock.OmRequestLockUtils;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * entry for request execution.
 */
public class OMGateway {
  private static final Logger LOG = LoggerFactory.getLogger(OMGateway.class);
  private final LeaderRequestExecutor leaderExecutor;
  private final FollowerRequestExecutor followerExecutor;
  private final OzoneManager om;
  private final AtomicLong requestInProgress = new AtomicLong(0);
  private final AtomicInteger leaderExecCurrIdx = new AtomicInteger(0);
  /**
   * uniqueIndex is used to generate index used in objectId creation uniquely accross OM nodes.
   * This makes use of termIndex for init shifted within 54 bits.
   */
  private AtomicLong uniqueIndex = new AtomicLong();

  public OMGateway(OzoneManager om) throws IOException {
    this.om = om;
    OmLockOpr.init(om.getThreadNamePrefix());
    OmRequestLockUtils.init();
    this.leaderExecutor = new LeaderRequestExecutor(om, uniqueIndex);
    this.followerExecutor = new FollowerRequestExecutor(om, uniqueIndex);
    if (om.isLeaderExecutorEnabled() && om.isRatisEnabled()) {
      OzoneManagerRatisServer ratisServer = om.getOmRatisServer();
      ratisServer.getOmBasicStateMachine().registerLeaderNotifier(this::leaderChangeNotifier);
      TransactionInfo transactionInfo = om.getTransactionInfo();
      if (transactionInfo != null) {
        if (transactionInfo.getIndex() != null) {
          uniqueIndex.set(transactionInfo.getIndex());
        } else if (transactionInfo.getTransactionIndex() >= 0) {
          uniqueIndex.set(transactionInfo.getTransactionIndex());
        }
      }
    } else {
      // for non-ratis flow, init with last index
      uniqueIndex.set(om.getLastTrxnIndexForNonRatis());
    }
    if (om.isLeaderExecutorEnabled()) {
      BucketQuotaResource.instance().enableTrack();
    }
  }
  public void stop() {
    leaderExecutor.stop();
    followerExecutor.stop();
    OmLockOpr.stop();
  }
  public OMResponse submit(OMRequest omRequest) throws ServiceException {
    if (!om.isLeaderReady()) {
      try {
        om.checkLeaderStatus();
      } catch (IOException e) {
        throw new ServiceException(e);
      }
    }
    executorEnable();
    RequestContext requestContext = new RequestContext();
    requestContext.setRequest(omRequest);
    requestInProgress.incrementAndGet();
    requestContext.setFuture(new CompletableFuture<>());
    CompletableFuture<OMResponse> f = requestContext.getFuture()
        .whenComplete((r, th) -> handleAfterExecution(requestContext, th));
    OmLockOpr lockOperation = OmRequestLockUtils.getLockOperation(om, omRequest);
    try {
      // TODO scheduling of request to pool
      OMClientRequest omClientRequest = OzoneManagerRatisUtils.createClientRequest(omRequest, om);
      lockOperation.lock(om);
      requestContext.setClientRequest(omClientRequest);

      validate(omRequest);
      ensurePreviousRequestCompletionForPrepare(omRequest);

      // submit request
      ExecutorType executorType = executorSelector(omRequest);
      if (executorType == ExecutorType.LEADER_COMPATIBLE) {
        int idx = Math.abs(leaderExecCurrIdx.getAndIncrement() % leaderExecutor.batchSize());
        leaderExecutor.submit(idx, requestContext);
      } else if (executorType == ExecutorType.FOLLOWER) {
        followerExecutor.submit(0, requestContext);
      } else {
        leaderExecutor.submit(0, requestContext);
      }

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
      lockOperation.unlock();
      Server.Call call = Server.getCurCall().get();
      if (null != call) {
        OMLockDetails lockDetails = lockOperation.getLockDetails();
        call.getProcessingDetails().add(ProcessingDetails.Timing.LOCKWAIT,
            lockDetails.getWaitLockNanos(), TimeUnit.NANOSECONDS);
        call.getProcessingDetails().add(ProcessingDetails.Timing.LOCKSHARED,
            lockDetails.getReadLockNanos(), TimeUnit.NANOSECONDS);
        call.getProcessingDetails().add(ProcessingDetails.Timing.LOCKEXCLUSIVE,
            lockDetails.getWriteLockNanos(), TimeUnit.NANOSECONDS);
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

  public void leaderChangeNotifier(String newLeaderId) {
    boolean isLeader = om.getOMNodeId().equals(newLeaderId);
    if (isLeader) {
      resetUniqueIndex();
    } else {
      leaderExecutor.disableProcessing();
    }
  }

  private void resetUniqueIndex() {
    Long index = null;
    try {
      TransactionInfo transactionInfo = TransactionInfo.readTransactionInfo(om.getMetadataManager());
      if (null != transactionInfo) {
        index = transactionInfo.getIndex();
      }
    } catch (IOException e) {
      throw new IllegalStateException("Unable to initialized index from TransactionInfoTable");
    }
    if (null != index) {
      uniqueIndex.set(index);
    }
  }

  public void executorEnable() throws ServiceException {
    if (leaderExecutor.isProcessing()) {
      return;
    }
    if (requestInProgress.get() == 0) {
      leaderExecutor.enableProcessing();
    } else {
      LOG.warn("Executor is not enabled, previous request {} is still not cleaned", requestInProgress.get());
      String msg = "Request processing is disabled due to error";
      throw new ServiceException(msg, new OMException(msg, OMException.ResultCodes.INTERNAL_ERROR));
    }
  }

  private ExecutorType executorSelector(OMRequest req) {
    switch (req.getCmdType()) {
    case EchoRPC:
      return ExecutorType.LEADER_OPTIMIZED;
    /* cases with Secret manager cache */
    case GetS3Secret:
    case SetS3Secret:
    case RevokeS3Secret:
    case TenantAssignUserAccessId:
    case TenantRevokeUserAccessId:
    case TenantAssignAdmin:
    case TenantRevokeAdmin:
    /* cases for upgrade */
    case FinalizeUpgrade:
    case Prepare:
    case CancelPrepare:
    /* cases for snapshot db update */
    case PurgeKeys:
    case PurgeDirectories:
    case RenameKey:
    case RenameKeys:
    /* cases for snapshot */
    case SnapshotMoveDeletedKeys:
    case SnapshotPurge:
    case SetSnapshotProperty:
    case CreateSnapshot:
    case DeleteSnapshot:
    case RenameSnapshot:
      return ExecutorType.FOLLOWER;
    default:
      return ExecutorType.LEADER_COMPATIBLE;
    }
  }

  enum ExecutorType {
    LEADER_COMPATIBLE,
    FOLLOWER,
    LEADER_OPTIMIZED
  }
}
