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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OMAuditLogger;
import org.apache.hadoop.ozone.om.ratis.execution.factory.OmRequestFactory;
import org.apache.hadoop.ozone.om.ratis.execution.request.OMRequestBase;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

/**
 * execution at node for sync execution changes at every node.
 */
public class PostExecutor {
  public static final Logger LOG = LoggerFactory.getLogger(PostExecutor.class);
  private static final String LEADER_INDEX_KEY = "#LEADERINDEX";
  private final AtomicLong leaderIndex = new AtomicLong();
  private final OzoneManager ozoneManager;
  private Consumer<Long> indexNotifier = null;


  public PostExecutor(OzoneManager ozoneManager) throws IOException {
    this(ozoneManager, true);
  }
  public PostExecutor(OzoneManager ozoneManager, boolean ratis) throws IOException {
    this.ozoneManager = ozoneManager;
    if (ratis) {
      leaderIndex.set(initLeaderIndex(ozoneManager));
    }
  }
  /**
   * Submits write request to OM and returns the response Message.
   * @param omRequest OMRequest
   * @return response from OM
   */
  public OMResponse runCommand(OzoneManagerProtocolProtos.OMRequest omRequest, TermIndex termIndex) {
    // leader index shared to follower for follower execution
    if (!omRequest.hasExecutionControlRequest()
        || omRequest.getExecutionControlRequest().getRequestInfoList().isEmpty()) {
      LOG.warn("Failed to apply command, Invalid request as do not have control information ");
      return createErrorResponse(omRequest,
          new OMException("Request do not have control information", OMException.ResultCodes.INVALID_REQUEST));
    }

    try {
      saveLeaderIndex(omRequest);
      OMRequestBase omClientRequest = OmRequestFactory.createClientRequest(omRequest, ozoneManager);
      try {
        OMClientResponse omClientResponse = captureLatencyNs(
            ozoneManager.getPerfMetrics().getValidateAndUpdateCacheLatencyNs(),
            () -> Objects.requireNonNull(omClientRequest.process(ozoneManager, termIndex),
                "omClientResponse returned cannot be null"));
        OMAuditLogger.log(omClientRequest.getAuditBuilder(), termIndex);
        return omClientResponse.getOMResponse();
      } catch (Throwable th) {
        OMAuditLogger.log(omClientRequest.getAuditBuilder(), omClientRequest, ozoneManager, termIndex, th);
        throw th;
      }
    } catch (IOException e) {
      LOG.warn("Failed to apply command, Exception occurred ", e);
      return createErrorResponse(omRequest, e);
    } catch (Throwable e) {
      // For any Runtime exceptions, terminate OM.
      String errorMessage = "Request " + omRequest + " failed with exception";
      ExitUtils.terminate(1, errorMessage, e, LOG);
    }
    return null;
  }

  /**
   * run non ratis request at node.
   * @param request OMRequest
   * @return response from OM
   */
  public OMResponse runCommandNonRatis(OzoneManagerProtocolProtos.OMRequest request, TermIndex termIndex)
      throws IOException {
    OMResponse omResponse = runCommand(request, termIndex);
    try (BatchOperation batchOperation = ozoneManager.getMetadataManager().getStore().initBatchOperation()) {
      ozoneManager.getMetadataManager().getTransactionInfoTable().putWithBatch(
          batchOperation, TRANSACTION_INFO_KEY, TransactionInfo.valueOf(termIndex));
      ozoneManager.getMetadataManager().getStore().commitBatchOperation(batchOperation);
    }
    return omResponse;
  }

  private void saveLeaderIndex(OzoneManagerProtocolProtos.OMRequest request) throws IOException {
    Long index = request.getExecutionControlRequest().getRequestInfoList().stream().map(e -> e.getIndex())
        .max(Long::compareTo).orElse(0L);
    leaderIndex.set(index > leaderIndex.get() ? index : leaderIndex.get());
    ozoneManager.getMetadataManager().getTransactionInfoTable().put(LEADER_INDEX_KEY,
        TransactionInfo.valueOf(-1, leaderIndex.get()));
    if (null != indexNotifier) {
      indexNotifier.accept(leaderIndex.get());
    }
  }
  public static long initLeaderIndex(OzoneManager ozoneManager) throws IOException {
    TransactionInfo transactionInfo = ozoneManager.getMetadataManager().getTransactionInfoTable().get(LEADER_INDEX_KEY);
    if (null == transactionInfo) {
      // read ratis transaction for first time upgrade
      transactionInfo = TransactionInfo.readTransactionInfo(ozoneManager.getMetadataManager());
    }
    if (null != transactionInfo) {
      return transactionInfo.getTransactionIndex();
    }
    return 0;
  }

  public void registerIndexNotifier(Consumer<Long> idxNotifier) {
    this.indexNotifier = idxNotifier;
    indexNotifier.accept(leaderIndex.get());
  }

  private OzoneManagerProtocolProtos.OMResponse createErrorResponse(
      OzoneManagerProtocolProtos.OMRequest omRequest, IOException exception) {
    OzoneManagerProtocolProtos.OMResponse.Builder omResponseBuilder = OzoneManagerProtocolProtos.OMResponse.newBuilder()
        .setStatus(OzoneManagerRatisUtils.exceptionToResponseStatus(exception))
        .setCmdType(omRequest.getCmdType())
        .setTraceID(omRequest.getTraceID())
        .setSuccess(false);
    if (exception.getMessage() != null) {
      omResponseBuilder.setMessage(exception.getMessage());
    }
    return omResponseBuilder.build();
  }
}
