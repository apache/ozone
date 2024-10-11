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

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.helpers.OMAuditLogger;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * om executor.
 */
public class LeaderCompatibleRequestExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(LeaderCompatibleRequestExecutor.class);
  private static final int REQUEST_EXECUTOR_POOL_SIZE = 1;
  private static final int REQUEST_EXECUTOR_QUEUE_SIZE = 1000;
  private static final int MERGE_TASK_POOL_SIZE = 1;
  private static final int MERGE_TASK_QUEUE_SIZE = 1000;
  private static final int RATIS_TASK_POOL_SIZE = 10;
  private static final int RATIS_TASK_QUEUE_SIZE = 1000;
  private static final long DUMMY_TERM = -1;
  private final AtomicLong uniqueIndex;
  private final int ratisByteLimit;
  private final OzoneManager ozoneManager;
  private final PoolExecutor<RatisContext, Void> ratisSubmitter;
  private final PoolExecutor<RequestContext, RatisContext> requestMerger;
  private final PoolExecutor<RequestContext, RequestContext> leaderExecutor;
  private final OzoneManagerRequestHandler handler;
  private final AtomicBoolean isEnabled = new AtomicBoolean(true);
  private final AtomicInteger ratisCurrentPool = new AtomicInteger(0);

  public LeaderCompatibleRequestExecutor(OzoneManager om, AtomicLong uniqueIndex) {
    this.ozoneManager = om;
    this.handler = new OzoneManagerRequestHandler(ozoneManager);
    ratisSubmitter = new PoolExecutor<>(RATIS_TASK_POOL_SIZE, RATIS_TASK_QUEUE_SIZE,
        ozoneManager.getThreadNamePrefix() + "-LeaderRatis", this::ratisCommand, null);
    requestMerger = new PoolExecutor<>(MERGE_TASK_POOL_SIZE, MERGE_TASK_QUEUE_SIZE,
        ozoneManager.getThreadNamePrefix() + "-LeaderMerger", this::requestMergeCommand, this::ratisSubmit);
    leaderExecutor = new PoolExecutor<>(REQUEST_EXECUTOR_POOL_SIZE, REQUEST_EXECUTOR_QUEUE_SIZE,
        ozoneManager.getThreadNamePrefix() + "-LeaderExecutor", this::runExecuteCommand, this::mergeSubmit);
    int limit = (int) ozoneManager.getConfiguration().getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    // always go to 90% of max limit for request as other header will be added
    this.ratisByteLimit = (int) (limit * 0.8);
    this.uniqueIndex = uniqueIndex;
  }
  public void stop() {
    leaderExecutor.stop();
    requestMerger.stop();
    ratisSubmitter.stop();
  }
  public int batchSize() {
    return REQUEST_EXECUTOR_POOL_SIZE;
  }
  public boolean isProcessing() {
    return isEnabled.get();
  }
  public void disableProcessing() {
    isEnabled.set(false);
  }
  public void enableProcessing() {
    isEnabled.set(true);
  }

  public void submit(int idx, RequestContext ctx) throws InterruptedException {
    if (!isEnabled.get()) {
      rejectRequest(Collections.singletonList(ctx));
      return;
    }
    executeRequest(ctx, this::mergeSubmit);
    //leaderExecutor.submit(idx, ctx);
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

  private void runExecuteCommand(
      Collection<RequestContext> ctxs, PoolExecutor.CheckedConsumer<RequestContext> nxtPool) {
    if (!isEnabled.get()) {
      rejectRequest(ctxs);
      return;
    }
    for (RequestContext ctx : ctxs) {
      if (!isEnabled.get()) {
        rejectRequest(Collections.singletonList(ctx));
        continue;
      }
      executeRequest(ctx, nxtPool);
    }
  }

  private void mergeSubmit(RequestContext ctx) throws InterruptedException {
    requestMerger.submit(0, ctx);
  }

  private void executeRequest(RequestContext ctx, PoolExecutor.CheckedConsumer<RequestContext> nxtPool) {
    OMRequest request = ctx.getRequest();
    TermIndex termIndex = TermIndex.valueOf(DUMMY_TERM, uniqueIndex.incrementAndGet());
    ctx.setIndex(termIndex);
    try {
      handleRequest(ctx, termIndex);
    } catch (IOException e) {
      LOG.warn("Failed to write, Exception occurred ", e);
      ctx.setResponse(createErrorResponse(request, e));
    } catch (Throwable e) {
      LOG.warn("Failed to write, Exception occurred ", e);
      ctx.setResponse(createErrorResponse(request, new IOException(e)));
    } finally {
      if (ctx.getNextRequest() != null) {
        try {
          nxtPool.accept(ctx);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      } else {
        handleBatchUpdateComplete(Collections.singletonList(ctx), null, null);
      }
    }
  }

  private void handleRequest(RequestContext ctx, TermIndex termIndex) throws IOException {
    OMClientRequest omClientRequest = ctx.getClientRequest();
    try {
      OMClientResponse omClientResponse = handler.handleLeaderWriteRequest(omClientRequest, termIndex);
      ctx.setResponse(omClientResponse.getOMResponse());
      if (!omClientResponse.getOMResponse().getSuccess()) {
        OMAuditLogger.log(omClientRequest.getAuditBuilder(), termIndex);
      } else {
        OzoneManagerProtocolProtos.PersistDbRequest.Builder nxtRequest
            = retrieveDbChanges(ctx, termIndex, omClientResponse);
        if (nxtRequest != null) {
          OMRequest.Builder omReqBuilder = OMRequest.newBuilder().setPersistDbRequest(nxtRequest.build())
              .setCmdType(OzoneManagerProtocolProtos.Type.PersistDb);
          omReqBuilder.setClientId(ctx.getRequest().getClientId());
          ctx.setNextRequest(nxtRequest);
        } else {
          OMAuditLogger.log(omClientRequest.getAuditBuilder(), termIndex);
        }
      }
    } catch (Throwable th) {
      OMAuditLogger.log(omClientRequest.getAuditBuilder(), omClientRequest, ozoneManager, termIndex, th);
      throw th;
    }
  }

  private OzoneManagerProtocolProtos.PersistDbRequest.Builder retrieveDbChanges(
      RequestContext ctx, TermIndex termIndex, OMClientResponse omClientResponse) throws IOException {
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    String name = metadataManager.getBucketTable().getName();
    boolean isDbChanged = false;
    try (BatchOperation batchOperation = metadataManager.getStore()
        .initBatchOperation()) {
      omClientResponse.checkAndUpdateDB(metadataManager, batchOperation);
      // get db update and create request to flush
      OzoneManagerProtocolProtos.PersistDbRequest.Builder reqBuilder
          = OzoneManagerProtocolProtos.PersistDbRequest.newBuilder();
      Map<String, Map<ByteBuffer, ByteBuffer>> cachedDbTxs
          = ((RDBBatchOperation) batchOperation).getCachedTransaction();
      for (Map.Entry<String, Map<ByteBuffer, ByteBuffer>> tblEntry : cachedDbTxs.entrySet()) {
        isDbChanged = true;
        if (tblEntry.getKey().equals(name)) {
          if (ctx.getClientRequest().getWrappedBucketInfo() instanceof OmBucketInfoQuotaTracker) {
            continue;
          }
        }
        OzoneManagerProtocolProtos.DBTableUpdate.Builder tblBuilder
            = OzoneManagerProtocolProtos.DBTableUpdate.newBuilder();
        tblBuilder.setTableName(tblEntry.getKey());
        for (Map.Entry<ByteBuffer, ByteBuffer> kvEntry : tblEntry.getValue().entrySet()) {
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
      if (!isDbChanged) {
        return null;
      }
      reqBuilder.addIndex(termIndex.getIndex());
      return reqBuilder;
    }
  }

  private void requestMergeCommand(
      Collection<RequestContext> ctxs, PoolExecutor.CheckedConsumer<RatisContext> nxtPool) {
    if (!isEnabled.get()) {
      rejectRequest(ctxs);
      return;
    }
    Map<Long, OzoneManagerProtocolProtos.BucketQuotaCount.Builder> bucketChangeMap = new HashMap<>();
    List<RequestContext> sendList = new ArrayList<>();
    OzoneManagerProtocolProtos.PersistDbRequest.Builder reqBuilder
        = OzoneManagerProtocolProtos.PersistDbRequest.newBuilder();
    long size = 0;
    for (RequestContext ctx : ctxs) {
      List<OzoneManagerProtocolProtos.DBTableUpdate> tblList = ctx.getNextRequest().getTableUpdatesList();
      int tmpSize = 0;
      for (OzoneManagerProtocolProtos.DBTableUpdate tblUpdates : tblList) {
        tmpSize += tblUpdates.getSerializedSize();
      }
      if ((tmpSize + size) > ratisByteLimit) {
        // send current batched request
        appendBucketQuotaChanges(reqBuilder, bucketChangeMap);
        prepareAndSendRequest(sendList, reqBuilder, nxtPool);

        // reinit and continue
        reqBuilder = OzoneManagerProtocolProtos.PersistDbRequest.newBuilder();
        size = 0;
        sendList.clear();
        bucketChangeMap.clear();
      }

      // keep adding to batch list
      size += tmpSize;
      addBucketQuotaChanges(ctx, bucketChangeMap);
      for (OzoneManagerProtocolProtos.DBTableUpdate tblUpdates : tblList) {
        OzoneManagerProtocolProtos.DBTableUpdate.Builder tblBuilder
            = OzoneManagerProtocolProtos.DBTableUpdate.newBuilder();
        tblBuilder.setTableName(tblUpdates.getTableName());
        tblBuilder.addAllRecords(tblUpdates.getRecordsList());
        reqBuilder.addTableUpdates(tblBuilder.build());
      }
      reqBuilder.addIndex(ctx.getIndex().getIndex());
      sendList.add(ctx);
    }
    if (sendList.size() > 0) {
      appendBucketQuotaChanges(reqBuilder, bucketChangeMap);
      prepareAndSendRequest(sendList, reqBuilder, nxtPool);
    }
  }

  private void ratisSubmit(RatisContext ctx) throws InterruptedException {
    // follow simple strategy to submit to ratis for next set of merge request
    int nxtIndex = Math.abs(ratisCurrentPool.getAndIncrement() % RATIS_TASK_POOL_SIZE);
    ratisSubmitter.submit(nxtIndex, ctx);
  }

  private void addBucketQuotaChanges(
      RequestContext ctx, Map<Long, OzoneManagerProtocolProtos.BucketQuotaCount.Builder> quotaMap) {
    if (ctx.getClientRequest().getWrappedBucketInfo() instanceof OmBucketInfoQuotaTracker) {
      OmBucketInfoQuotaTracker info = (OmBucketInfoQuotaTracker) ctx.getClientRequest().getWrappedBucketInfo();
      OzoneManagerProtocolProtos.BucketQuotaCount.Builder quotaBuilder = quotaMap.computeIfAbsent(
          info.getObjectID(), k -> OzoneManagerProtocolProtos.BucketQuotaCount.newBuilder()
              .setVolName(info.getVolumeName()).setBucketName(info.getBucketName())
              .setBucketObjectId(info.getObjectID()).setSupportOldQuota(false));
      quotaBuilder.setDiffUsedBytes(quotaBuilder.getDiffUsedBytes() + info.getIncUsedBytes());
      quotaBuilder.setDiffUsedNamespace(quotaBuilder.getDiffUsedNamespace() + info.getIncUsedNamespace());
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
      PoolExecutor.CheckedConsumer<RatisContext> nxtPool) {
    RequestContext lastReqCtx = sendList.get(sendList.size() - 1);
    OMRequest.Builder omReqBuilder = OMRequest.newBuilder().setPersistDbRequest(reqBuilder.build())
        .setCmdType(OzoneManagerProtocolProtos.Type.PersistDb)
        .setClientId(lastReqCtx.getRequest().getClientId());
    OMRequest reqBatch = omReqBuilder.build();
    try {
      nxtPool.accept(new RatisContext(sendList, reqBatch));
    } catch (InterruptedException e) {
      handleBatchUpdateComplete(sendList, e, null);
      Thread.currentThread().interrupt();
    }
  }

  private void ratisCommand(Collection<RatisContext> ctxs, PoolExecutor.CheckedConsumer<Void> nxtPool) {
    if (!isEnabled.get()) {
      for (RatisContext ctx : ctxs) {
        rejectRequest(ctx.getRequestContexts());
      }
      return;
    }
    for (RatisContext ctx : ctxs) {
      List<RequestContext> sendList = ctx.getRequestContexts();
      RequestContext lastReqCtx = sendList.get(sendList.size() - 1);
      OMRequest reqBatch = ctx.getRequest();
      try {
        OMResponse dbUpdateRsp = sendDbUpdateRequest(reqBatch, lastReqCtx.getIndex());
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
  }
  private OMResponse sendDbUpdateRequest(OMRequest nextRequest, TermIndex termIndex) throws Exception {
    try {
      if (!ozoneManager.isRatisEnabled()) {
        return OMBasicStateMachine.runCommand(nextRequest, termIndex, handler, ozoneManager);
      }
      OMResponse response = ozoneManager.getOmRatisServer().submitRequest(nextRequest, ClientId.randomId(),
          termIndex.getIndex());
      return response;
    } catch (Exception ex) {
      throw ex;
    }
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
  private void handleBatchUpdateComplete(Collection<RequestContext> ctxs, Throwable th, String leaderOMNodeId) {
    // TODO: no-cache switch, no need cleanup cache
    Map<String, List<Long>> cleanupMap = new HashMap<>();
    for (RequestContext ctx : ctxs) {
      // cache cleanup
      if (null != ctx.getNextRequest()) {
        List<OzoneManagerProtocolProtos.DBTableUpdate> tblList = ctx.getNextRequest().getTableUpdatesList();
        for (OzoneManagerProtocolProtos.DBTableUpdate tblUpdate : tblList) {
          List<Long> epochs = cleanupMap.computeIfAbsent(tblUpdate.getTableName(), k -> new ArrayList<>());
          epochs.add(ctx.getIndex().getIndex());
        }
      }
      for (Map.Entry<String, List<Long>> entry : cleanupMap.entrySet()) {
        ozoneManager.getMetadataManager().getTable(entry.getKey()).cleanupCache(entry.getValue());
      }
    }

    for (RequestContext ctx : ctxs) {
      if (ctx.getClientRequest().getWrappedBucketInfo() instanceof OmBucketInfoQuotaTracker) {
        // reset to be done to update resource quota for both success and failure
        // for success also, its added here as it's difficult to ensure its execution at nodes
        ((OmBucketInfoQuotaTracker) ctx.getClientRequest().getWrappedBucketInfo()).reset();
      }

      if (th != null) {
        OMAuditLogger.log(ctx.getClientRequest().getAuditBuilder(), ctx.getClientRequest(), ozoneManager,
            ctx.getIndex(), th);
        if (th instanceof IOException) {
          ctx.getFuture().complete(createErrorResponse(ctx.getRequest(), (IOException)th));
        } else {
          ctx.getFuture().complete(createErrorResponse(ctx.getRequest(), new IOException(th)));
        }
      } else {
        OMAuditLogger.log(ctx.getClientRequest().getAuditBuilder(), ctx.getIndex());
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
