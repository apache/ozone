/*
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

package org.apache.hadoop.ozone.om.request.upgrade;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ratis.OMTransactionInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.upgrade.OMPrepareResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;

/**
 * OM Request used to flush all transactions to disk, take a DB snapshot, and
 * purge the logs, leaving Ratis in a clean state without unapplied log
 * entries. This prepares the OM for upgrades/downgrades so that no request
 * in the log is applied to the database in the old version of the code in one
 * OM, and the new version of the code on another OM.
 */
public class OMPrepareRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMPrepareRequest.class);

  public OMPrepareRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    LOG.info("OM {} Received prepare request with log index {}",
        ozoneManager.getOMNodeId(), transactionLogIndex);

    OMRequest omRequest = getOmRequest();
    OzoneManagerProtocolProtos.PrepareRequestArgs args =
        omRequest.getPrepareRequest().getArgs();
    OMResponse.Builder responseBuilder =
        OmResponseUtil.getOMResponseBuilder(omRequest);
    responseBuilder.setCmdType(Type.Prepare);
    OMClientResponse response = null;

    // Allow double buffer this many seconds to flush all transactions before
    // returning an error to the caller.
    Duration flushTimeout =
        Duration.of(args.getTxnApplyWaitTimeoutSeconds(), ChronoUnit.SECONDS);
    // Time between checks to see if double buffer finished flushing.
    Duration flushCheckInterval =
        Duration.of(args.getTxnApplyCheckIntervalSeconds(), ChronoUnit.SECONDS);

    try {
      // Create response.
      PrepareResponse omResponse = PrepareResponse.newBuilder()
              .setTxnID(transactionLogIndex)
              .build();
      responseBuilder.setPrepareResponse(omResponse);
      response = new OMPrepareResponse(responseBuilder.build());

      // Add response to double buffer before clearing logs.
      // This guarantees the log index of this request will be the same as
      // the snapshot index in the prepared state.
      ozoneManagerDoubleBufferHelper.add(response, transactionLogIndex);

      OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();
      RaftServer.Division division =
          omRatisServer.getServer()
              .getDivision(omRatisServer.getRaftGroup().getGroupId());

      // Wait for outstanding double buffer entries to flush to disk,
      // so they will not be purged from the log before being persisted to
      // the DB.
      // Since the response for this request was added to the double buffer
      // already, once this index reaches the state machine, we know all
      // transactions have been flushed.
      waitForLogIndex(transactionLogIndex,
          ozoneManager.getMetadataManager(), division,
          flushTimeout, flushCheckInterval);

      // TODO: After the snapshot index update fix, the prepare index will
      //  always be 1 more than the prepare txn index, because the Ratis
      //  entry to commit the prepare request will be included in the snapshot.
      long prepareIndex = takeSnapshotAndPurgeLogs(division);

      // Save transaction log index to a marker file, so if the OM restarts,
      // it will remain in prepare mode on that index as long as the file
      // exists.
      ozoneManager.getPrepareState().finishPrepare(prepareIndex);

      LOG.info("OM {} prepared at log index {}. Returning response {}",
          ozoneManager.getOMNodeId(),
          ozoneManager.getRatisSnapshotIndex(), omResponse);
    } catch (OMException e) {
      LOG.error("Prepare Request Apply failed in {}. ",
          ozoneManager.getOMNodeId(), e);
      response = new OMPrepareResponse(
          createErrorOMResponse(responseBuilder, e));
    } catch (InterruptedException | IOException e) {
      // Set error code so that prepare failure does not cause the OM to
      // terminate.
      LOG.error("Prepare Request Apply failed in {}. ",
          ozoneManager.getOMNodeId(), e);
      response = new OMPrepareResponse(
          createErrorOMResponse(responseBuilder, new OMException(e,
              OMException.ResultCodes.PREPARE_FAILED)));
    }

    return response;
  }

  /**
   * Waits for the specified index to be flushed to the state machine on
   * disk, and to be updated in memory in Ratis.
   */
  private static void waitForLogIndex(long indexToWaitFor,
      OMMetadataManager metadataManager, RaftServer.Division division,
      Duration flushTimeout, Duration flushCheckInterval)
      throws InterruptedException, IOException {

    long endTime = System.currentTimeMillis() + flushTimeout.toMillis();
    boolean success = false;

    while (!success && System.currentTimeMillis() < endTime) {
      // If no transactions have been persisted to the DB, transaction info
      // will be null, not zero, causing a null pointer exception within
      // ozoneManager#getRatisSnaphotIndex.
      // Get the transaction directly instead to handle the case when it is
      // null.
      OMTransactionInfo dbTxnInfo = metadataManager
          .getTransactionInfoTable().get(TRANSACTION_INFO_KEY);
      long ratisTxnIndex =
          division.getStateMachine().getLastAppliedTermIndex().getIndex();

      // Ratis may apply meta transactions after the prepare request, causing
      // its in memory index to always be greater than the DB index.
      if (dbTxnInfo == null) {
        // If there are no transactions in the DB, we are prepared to log
        // index 0 only.
        success = (indexToWaitFor == 0)
            && (ratisTxnIndex >= indexToWaitFor);
      } else {
        success = (dbTxnInfo.getTransactionIndex() == indexToWaitFor)
            && (ratisTxnIndex >= indexToWaitFor);
      }

      if (!success) {
        Thread.sleep(flushCheckInterval.toMillis());
      }
    }

    // If the timeout waiting for all transactions to reach the state machine
    // is exceeded, the exception is propagated, resulting in an error response
    // to the client. They can retry the prepare request.
    if (!success) {
      throw new IOException(String.format("After waiting for %d seconds, " +
              "State Machine has not applied  all the transactions.",
          flushTimeout.getSeconds()));
    }
  }

  /**
   * Take a snapshot of the state machine at the last index, and purge ALL logs.
   * @param division Raft server division.
   * @return The index the snapshot was taken on.
   * @throws IOException on Error.
   */
  public static long takeSnapshotAndPurgeLogs(RaftServer.Division division)
      throws IOException {

    StateMachine stateMachine = division.getStateMachine();
    long snapshotIndex = stateMachine.takeSnapshot();
    RaftLog raftLog = division.getRaftLog();
    long raftLogIndex = raftLog.getLastEntryTermIndex().getIndex();

    // We can have a case where the log has a meta transaction after the
    // prepare request or another prepare request. If there is another
    // prepare request, this one will end up purging that request.
    // This means that an OM cannot support 2 prepare requests in the
    // transaction pipeline (un-applied) at the same time.
    if (raftLogIndex > snapshotIndex) {
      LOG.warn("Snapshot index {} does not " +
          "match last log index {}.", snapshotIndex, raftLogIndex);
      snapshotIndex = raftLogIndex;
    }

    CompletableFuture<Long> purgeFuture =
        raftLog.syncWithSnapshot(snapshotIndex);

    try {
      Long purgeIndex = purgeFuture.get();
      if (purgeIndex != snapshotIndex) {
        throw new IOException("Purge index " + purgeIndex +
            " does not match last index " + snapshotIndex);
      }
    } catch (Exception e) {
      throw new IOException("Unable to purge logs.", e);
    }

    return snapshotIndex;
  }

  public static String getRequestType() {
    return Type.Prepare.name();
  }
}
