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

import java.util.HashMap;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerStateMachine;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.upgrade.OMPrepareResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, TermIndex termIndex) {
    final long transactionLogIndex = termIndex.getIndex();

    LOG.info("OM {} Received prepare request with log {}", ozoneManager.getOMNodeId(), termIndex);

    OMRequest omRequest = getOmRequest();
    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = omRequest.getUserInfo();
    OzoneManagerProtocolProtos.PrepareRequestArgs args =
        omRequest.getPrepareRequest().getArgs();
    OMResponse.Builder responseBuilder =
        OmResponseUtil.getOMResponseBuilder(omRequest);
    responseBuilder.setCmdType(Type.Prepare);
    OMClientResponse response = null;
    Exception exception = null;

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
      response = new OMPrepareResponse(responseBuilder.build(),
          transactionLogIndex);

      // Add response to double buffer before clearing logs.
      // This guarantees the log index of this request will be the same as
      // the snapshot index in the prepared state.
      OzoneManagerDoubleBuffer doubleBuffer =
          ozoneManager.getOmRatisServer().getOmStateMachine().getOzoneManagerDoubleBuffer();
      doubleBuffer.add(response, termIndex);

      OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();
      final RaftServer.Division division = omRatisServer.getServerDivision();
      final OzoneManagerStateMachine stateMachine = (OzoneManagerStateMachine) division.getStateMachine();

      // Wait for outstanding double buffer entries
      // - to be flushed to db, and
      // - to be notified by Ratis.
      // The log index returned, will be used as the prepare index, is the last Ratis commit index
      // which can be higher than the transactionLogIndex of this request.
      final long prepareIndex = waitForLogIndex(transactionLogIndex, ozoneManager, stateMachine,
          flushTimeout, flushCheckInterval);
      Preconditions.assertTrue(prepareIndex >= transactionLogIndex);
      takeSnapshotAndPurgeLogs(prepareIndex, division);

      // Save prepare index to a marker file, so if the OM restarts,
      // it will remain in prepare mode as long as the file exists and its
      // log indices are >= the one in the file.
      ozoneManager.getPrepareState().finishPrepare(transactionLogIndex);

      LOG.info("OM {} prepared at log index {}. Returning response {} with " +
          "log index {}", ozoneManager.getOMNodeId(), transactionLogIndex,
          omResponse, omResponse.getTxnID());
    } catch (OMException e) {
      exception = e;
      LOG.error("Prepare Request Apply failed in {}. ",
          ozoneManager.getOMNodeId(), e);
      response = new OMPrepareResponse(
          createErrorOMResponse(responseBuilder, e));
    } catch (InterruptedException | IOException e) {
      // Set error code so that prepare failure does not cause the OM to
      // terminate.
      exception = e;
      LOG.error("Prepare Request Apply failed in {}. ",
          ozoneManager.getOMNodeId(), e);
      response = new OMPrepareResponse(
          createErrorOMResponse(responseBuilder, new OMException(e,
              OMException.ResultCodes.PREPARE_FAILED)));

      // Disable prepare gate and attempt to delete prepare marker file.
      // Whether marker file delete fails or succeeds, we will return the
      // above error response to the caller.
      try {
        ozoneManager.getPrepareState().cancelPrepare();
      } catch (IOException ex) {
        LOG.error("Failed to delete prepare marker file.", ex);
      }
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    }

    markForAudit(auditLogger, buildAuditMessage(OMAction.UPGRADE_PREPARE,
        new HashMap<>(), exception, userInfo));
    return response;
  }

  /**
   * Waits for the specified index to be applied to {@link OzoneManagerStateMachine}.
   * Note that
   * - the applied index is updated after the transaction is flushed to db.
   * - after a transaction (i) is committed, ratis will append another ratis-metadata transaction (i+1).
   *
   * @return the last Ratis commit index
   */
  private static long waitForLogIndex(long minOMDBFlushIndex,
      OzoneManager om, OzoneManagerStateMachine stateMachine,
      Duration flushTimeout, Duration flushCheckInterval)
      throws InterruptedException, IOException {

    long endTime = System.currentTimeMillis() + flushTimeout.toMillis();

    boolean omDBFlushed = false;
    boolean ratisStateMachineApplied = false;

    // Wait for Ratis commit index after the specified index to be applied to
    // Ratis' state machine. This index will not appear in the OM DB until a
    // snapshot is taken.
    // If we purge logs without waiting for this index, it may not make it to
    // the RocksDB snapshot, and then the log entry is lost on this OM.
    long minRatisStateMachineIndex = minOMDBFlushIndex + 1; // for the ratis-metadata transaction
    long lastRatisCommitIndex = RaftLog.INVALID_LOG_INDEX;

    // Wait OM state machine to apply the given index.
    long lastOMDBFlushIndex = RaftLog.INVALID_LOG_INDEX;

    LOG.info("{} waiting for index {} to flush to OM DB and index {} to flush" +
            " to Ratis state machine.", om.getOMNodeId(), minOMDBFlushIndex,
        minRatisStateMachineIndex);
    while (!(omDBFlushed && ratisStateMachineApplied) &&
        System.currentTimeMillis() < endTime) {
      // Check OM DB.
      lastOMDBFlushIndex = om.getRatisSnapshotIndex();
      omDBFlushed = (lastOMDBFlushIndex >= minOMDBFlushIndex);
      LOG.debug("{} Current DB transaction index {}.", om.getOMNodeId(),
          lastOMDBFlushIndex);

      // Check ratis state machine.
      lastRatisCommitIndex = stateMachine.getLastNotifiedTermIndex().getIndex();
      ratisStateMachineApplied = (lastRatisCommitIndex >=
          minRatisStateMachineIndex);
      LOG.debug("{} Current Ratis state machine transaction index {}.",
          om.getOMNodeId(), lastRatisCommitIndex);

      if (!(omDBFlushed && ratisStateMachineApplied)) {
        Thread.sleep(flushCheckInterval.toMillis());
      }
    }

    // If the timeout waiting for all transactions to reach the state machine
    // is exceeded, the exception is propagated, resulting in an error response
    // to the client. They can retry the prepare request.
    if (!omDBFlushed) {
      throw new IOException(String.format("After waiting for %d seconds, " +
              "OM database flushed index %d which is less than the minimum " +
              "required index %d.",
          flushTimeout.getSeconds(), lastOMDBFlushIndex, minOMDBFlushIndex));
    } else if (!ratisStateMachineApplied) {
      throw new IOException(String.format("After waiting for %d seconds, " +
              "Ratis state machine applied index %d which is less than" +
              " the minimum required index %d.",
          flushTimeout.getSeconds(), lastRatisCommitIndex,
          minRatisStateMachineIndex));
    }
    return lastRatisCommitIndex;
  }

  /**
   * Take a snapshot of the state machine at the last index, and purge at
   * least all log with indices less than or equal to the prepare index.
   * If there is another prepare request or cancel prepare request,
   * this one will end up purging that request since it was allowed through
   * the pre-append prepare gate.
   * This means that an OM cannot support 2 prepare requests in the
   * transaction pipeline (un-applied) at the same time.
   */
  public static void takeSnapshotAndPurgeLogs(long prepareIndex,
      RaftServer.Division division) throws IOException {
    StateMachine stateMachine = division.getStateMachine();
    long snapshotIndex = stateMachine.takeSnapshot();
    LOG.info("takeSnapshot at {} for prepareIndex {}", snapshotIndex, prepareIndex);

    if (snapshotIndex < prepareIndex) {
      throw new IOException(String.format("OM DB snapshot index %d is less " +
          "than prepare index %d. Some required logs may not have" +
          "been persisted to the state machine.", snapshotIndex,
          prepareIndex));
    }

    CompletableFuture<Long> purgeFuture =
        division.getRaftLog().onSnapshotInstalled(snapshotIndex);

    try {
      long actualPurgeIndex = purgeFuture.get();

      if (actualPurgeIndex != snapshotIndex) {
        LOG.warn("Actual purge index {} does not " +
              "match specified purge index {}. ", actualPurgeIndex,
            snapshotIndex);
      }
    } catch (ExecutionException e) {
      // Ozone manager error handler does not respect exception chaining and
      // only displays the message of the top level exception.
      throw new IOException("Unable to purge logs: " + e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Unable to purge logs: " + e.getMessage());
    }
  }
}
