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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.helpers.OMAuditLogger;
import org.apache.hadoop.ozone.om.ratis.execution.request.ExecutionContext;
import org.apache.hadoop.ozone.om.ratis.execution.request.OMRequestExecutor;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ozone manager request executor.
 * Execute in below stage:
 * 1. Request is executed (process) and submit to merge pool
 * 2. request is picked in bulk, db changes are merged to single request
 * 3. merged single request is submitted to ratis to persist in db
 */
public class LeaderRequestExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(LeaderRequestExecutor.class);
  private final OzoneManager ozoneManager;
  private final AtomicLong uniqueIndex;
  private final AtomicBoolean isEnabled = new AtomicBoolean(true);
  private final OzoneManagerRequestHandler handler;
  private final int ratisByteLimit;
  private final int mergeTaskPoolSize;
  private final AtomicInteger mergeCurrentPool = new AtomicInteger(0);
  private final PoolExecutor<RequestContext, RatisContext> requestMerger;
  private final ClientId clientId = ClientId.randomId();

  public LeaderRequestExecutor(OzoneManager om, AtomicLong uniqueIndex) {
    this.ozoneManager = om;
    this.uniqueIndex = uniqueIndex;
    this.handler = new OzoneManagerRequestHandler(ozoneManager);
    this.mergeTaskPoolSize = om.getConfiguration().getInt("ozone.om.leader.merge.pool.size", 1);
    int mergeTaskQueueSize = om.getConfiguration().getInt("ozone.om.leader.merge.queue.size", 1000);
    requestMerger = new PoolExecutor<>(mergeTaskPoolSize, mergeTaskQueueSize,
        ozoneManager.getThreadNamePrefix() + "-LeaderMerger", this::requestMergeCommand, this::ratisSubmit);
    int limit = (int) ozoneManager.getConfiguration().getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    // always go to 90% of max limit for request as other header will be added
    this.ratisByteLimit = (int) (limit * 0.8);
  }

  public void start() {
    isEnabled.set(true);
    requestMerger.start();
  }
  public void stop() {
    requestMerger.stop();
  }

  public void setEnabled(boolean enabled) {
    isEnabled.set(enabled);
  }

  public boolean isEnabled() {
    return isEnabled.get();
  }

  public void submit(RequestContext ctx) throws InterruptedException, IOException {
    if (!isEnabled.get()) {
      rejectRequest(Collections.singletonList(ctx));
      return;
    }
    executeRequest(ctx, this::mergeSubmit);
  }

  private void rejectRequest(Collection<RequestContext> ctxs) {
    Throwable th;
    if (!ozoneManager.isLeaderReady()) {
      String peerId = ozoneManager.isRatisEnabled() ? ozoneManager.getOmRatisServer().getRaftPeerId().toString()
          : ozoneManager.getOMNodeId();
      th = new OMLeaderNotReadyException(peerId + " is not ready to process request yet.");
    } else {
      th = new OMException("Request processing is disabled due to error", OMException.ResultCodes.INTERNAL_ERROR);
    }
    handleBatchUpdateComplete(ctxs, th, null);
  }

  private void executeRequest(RequestContext ctx, PoolExecutor.CheckedConsumer<RequestContext> nxtStage) {
    OMRequest request = ctx.getRequest();
    ExecutionContext executionContext = new ExecutionContext();
    executionContext.setIndex(uniqueIndex.incrementAndGet());
    ctx.setExecutionContext(executionContext);
    try {
      handleRequest(ctx, executionContext);
    } catch (IOException e) {
      LOG.warn("Failed to write, Exception occurred ", e);
      ctx.setResponse(createErrorResponse(request, e));
    } catch (Throwable e) {
      LOG.warn("Failed to write, Exception occurred ", e);
      ctx.setResponse(createErrorResponse(request, new IOException(e)));
    } finally {
      if (!ctx.getRequestExecutor().changeRecorder().getTableRecordsMap().isEmpty()) {
        // submit to next stage for db changes
        try {
          nxtStage.accept(ctx);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      } else {
        handleBatchUpdateComplete(Collections.singletonList(ctx), null, null);
      }
    }
  }

  private void handleRequest(RequestContext ctx, ExecutionContext exeCtx) throws IOException {
    OMRequestExecutor omClientRequest = ctx.getRequestExecutor();
    try {
      OMClientResponse omClientResponse = omClientRequest.process(ozoneManager, exeCtx);
      ctx.setResponse(omClientResponse.getOMResponse());
      if (!omClientResponse.getOMResponse().getSuccess()) {
        omClientRequest.changeRecorder().clear();
        OMAuditLogger.log(omClientRequest.getAuditBuilder(), exeCtx);
      } else {
        if (omClientRequest.changeRecorder().getTableRecordsMap().isEmpty()) {
          // if no update, audit log the response
          OMAuditLogger.log(omClientRequest.getAuditBuilder(), exeCtx);
        }
      }
    } catch (Throwable th) {
      omClientRequest.changeRecorder().clear();
      OMAuditLogger.log(omClientRequest.getAuditBuilder(), omClientRequest, ozoneManager, exeCtx, th);
      throw th;
    }
  }

  private void mergeSubmit(RequestContext ctx) throws InterruptedException {
    if (mergeTaskPoolSize == 0) {
      requestMergeCommand(Collections.singletonList(ctx), this::ratisSubmit);
      return;
    }
    int nxtIndex = Math.abs(mergeCurrentPool.getAndIncrement() % mergeTaskPoolSize);
    requestMerger.submit(nxtIndex, ctx);
  }

  private void requestMergeCommand(
      Collection<RequestContext> ctxs, PoolExecutor.CheckedConsumer<RatisContext> ratisStage) {
    if (!isEnabled.get()) {
      rejectRequest(ctxs);
      return;
    }
    requestMergeCommandInternal(ctxs, ratisStage);
  }

  private void requestMergeCommandInternal(
      Collection<RequestContext> ctxs, PoolExecutor.CheckedConsumer<RatisContext> ratisStage) {
    Map<Long, OzoneManagerProtocolProtos.BucketQuotaCount.Builder> bucketChangeMap = new HashMap<>();
    List<RequestContext> sendList = new ArrayList<>();
    OzoneManagerProtocolProtos.PersistDbRequest.Builder reqBuilder
        = OzoneManagerProtocolProtos.PersistDbRequest.newBuilder();
    OzoneManagerProtocolProtos.ExecutionControlRequest.Builder controlReq
        = OzoneManagerProtocolProtos.ExecutionControlRequest.newBuilder();
    long size = 0;
    for (RequestContext ctx : ctxs) {
      DbChangesRecorder recorder = ctx.getRequestExecutor().changeRecorder();
      int tmpSize = recorder.getSerializedSize();
      if ((tmpSize + size) > ratisByteLimit) {
        // send current batched request
        appendBucketQuotaChanges(reqBuilder, bucketChangeMap);
        prepareAndSendRequest(sendList, reqBuilder, controlReq, ratisStage);

        // reinit and continue
        reqBuilder = OzoneManagerProtocolProtos.PersistDbRequest.newBuilder();
        controlReq = OzoneManagerProtocolProtos.ExecutionControlRequest.newBuilder();
        size = 0;
        sendList.clear();
        bucketChangeMap.clear();
      }

      // keep adding to batch list
      size += tmpSize;
      recorder.serialize(reqBuilder);
      recorder.serializeBucketQuota(bucketChangeMap);
      controlReq.addRequestInfo(OzoneManagerProtocolProtos.ClientRequestInfo.newBuilder()
          .setUuidClientId(ctx.getUuidClientId()).setCallId(ctx.getCallId())
          .setResponse(ctx.getResponse()).setIndex(ctx.getExecutionContext().getIndex()).build());
      sendList.add(ctx);
    }
    if (!sendList.isEmpty()) {
      appendBucketQuotaChanges(reqBuilder, bucketChangeMap);
      prepareAndSendRequest(sendList, reqBuilder, controlReq, ratisStage);
    }
  }

  private void appendBucketQuotaChanges(
      OzoneManagerProtocolProtos.PersistDbRequest.Builder req,
      Map<Long, OzoneManagerProtocolProtos.BucketQuotaCount.Builder> quotaMap) {
    for (Map.Entry<Long, OzoneManagerProtocolProtos.BucketQuotaCount.Builder> entry : quotaMap.entrySet()) {
      if (entry.getValue().getDiffUsedBytes() == 0 && entry.getValue().getDiffUsedNamespace() == 0) {
        continue;
      }
      req.addBucketQuotaCount(entry.getValue().build());
    }
  }

  private void prepareAndSendRequest(
      List<RequestContext> sendList, OzoneManagerProtocolProtos.PersistDbRequest.Builder reqBuilder,
      OzoneManagerProtocolProtos.ExecutionControlRequest.Builder controlReq,
      PoolExecutor.CheckedConsumer<RatisContext> ratisStage) {
    RequestContext lastReqCtx = sendList.get(sendList.size() - 1);
    OMRequest.Builder omReqBuilder = OMRequest.newBuilder().setPersistDbRequest(reqBuilder.build())
        .setExecutionControlRequest(controlReq.build())
        .setCmdType(OzoneManagerProtocolProtos.Type.PersistDb)
        .setClientId(lastReqCtx.getRequest().getClientId());
    OMRequest reqBatch = omReqBuilder.build();
    try {
      ratisStage.accept(new RatisContext(sendList, reqBatch));
    } catch (InterruptedException e) {
      handleBatchUpdateComplete(sendList, e, null);
      Thread.currentThread().interrupt();
    }
  }

  private void ratisSubmit(RatisContext ctx) {
    ratisCommand(ctx);
  }
  private void ratisCommand(RatisContext ctx) {
    if (!isEnabled.get()) {
      rejectRequest(ctx.getRequestContexts());
      return;
    }
    List<RequestContext> sendList = ctx.getRequestContexts();
    RequestContext lastReqCtx = sendList.get(sendList.size() - 1);
    OMRequest reqBatch = ctx.getRequest();
    try {
      OMResponse dbUpdateRsp = sendDbUpdateRequest(reqBatch, lastReqCtx.getExecutionContext());
      if (!dbUpdateRsp.getSuccess()) {
        throw new OMException(dbUpdateRsp.getMessage(),
            OMException.ResultCodes.values()[dbUpdateRsp.getStatus().ordinal()]);
      }
      handleBatchUpdateComplete(sendList, null, dbUpdateRsp.getLeaderOMNodeId());
    } catch (Throwable e) {
      LOG.warn("Failed to write, Exception occurred ", e);
      handleBatchUpdateComplete(sendList, e, null);
    }
  }

  private OMResponse sendDbUpdateRequest(OMRequest nextRequest, ExecutionContext executionContext) throws Exception {
    if (!ozoneManager.isRatisEnabled()) {
      return new PostExecutor(ozoneManager, false).runCommandNonRatis(nextRequest, executionContext);
    }
    return ozoneManager.getOmRatisServer().submitRequest(nextRequest, clientId, executionContext.getIndex());
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
    return omResponseBuilder.build();
  }
  private void handleBatchUpdateComplete(Collection<RequestContext> ctxs, Throwable th, String leaderOMNodeId) {
    for (RequestContext ctx : ctxs) {
      // reset quota resource and release memory for change update
      ctx.getRequestExecutor().changeRecorder().clear();

      if (th != null) {
        OMAuditLogger.log(ctx.getRequestExecutor().getAuditBuilder(), ctx.getRequestExecutor(), ozoneManager,
            ctx.getExecutionContext(), th);
        if (th instanceof IOException) {
          ctx.getFuture().complete(createErrorResponse(ctx.getRequest(), (IOException)th));
        } else {
          ctx.getFuture().complete(createErrorResponse(ctx.getRequest(), new IOException(th)));
        }
      } else {
        OMAuditLogger.log(ctx.getRequestExecutor().getAuditBuilder(), ctx.getExecutionContext());
        OMResponse newRsp = ctx.getResponse();
        if (leaderOMNodeId != null) {
          newRsp = OMResponse.newBuilder(newRsp).setLeaderOMNodeId(leaderOMNodeId).build();
        }
        ctx.getFuture().complete(newRsp);
      }
    }
  }

  static class RatisContext {
    private List<RequestContext> ctxs;
    private OMRequest req;
    RatisContext(List<RequestContext> ctxs, OMRequest req) {
      this.ctxs = ctxs;
      this.req = req;
    }
    public List<RequestContext> getRequestContexts() {
      return ctxs;
    }

    public OMRequest getRequest() {
      return req;
    }
  }
}
