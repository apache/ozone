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

import org.apache.hadoop.hdds.ratis.RatisUpgradeUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerStateMachine;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.upgrade.OMPrepareResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

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

  // Allow double buffer this many seconds to flush all transactions before
  // returning an error to the caller.
  private static final long DOUBLE_BUFFER_FLUSH_TIMEOUT_SECONDS = 5 * 60;
  // Time between checks to see if double buffer finished flushing.
  private static final long DOUBLE_BUFFER_FLUSH_CHECK_SECONDS = 1;

  public OMPrepareRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    LOG.info("Received prepare request with log index {}", transactionLogIndex);

    OMResponse.Builder responseBuilder =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    responseBuilder.setCmdType(Type.Prepare);
    OMClientResponse response = null;

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

      // Wait for outstanding double buffer entries to flush to disk,
      // so they will not be purged from the log before being persisted to
      // the DB.
      waitForDoubleBufferFlush(ozoneManager, transactionLogIndex,
          DOUBLE_BUFFER_FLUSH_TIMEOUT_SECONDS,
          DOUBLE_BUFFER_FLUSH_CHECK_SECONDS);

      // TODO: Create marker file with txn index.

      LOG.info("OM prepared at log index {}. Returning response {}",
          ozoneManager.getRatisSnapshotIndex(), omResponse);
    } catch (IOException e) {
      response = new OMPrepareResponse(
          createErrorOMResponse(responseBuilder, e));
    } catch (InterruptedException e) {
      response = new OMPrepareResponse(
          createErrorOMResponse(responseBuilder, new OMException(e,
              OMException.ResultCodes.INTERNAL_ERROR)));
    }

    return response;
  }

  private static void waitForDoubleBufferFlush(
      OzoneManager ozoneManager,
      long txnLogIndex,
      long maxTimeToWaitSeconds,
      long timeBetweenRetryInSeconds)
      throws InterruptedException, IOException {

    long intervalTime = TimeUnit.SECONDS.toMillis(timeBetweenRetryInSeconds);
    long endTime = System.currentTimeMillis() +
        TimeUnit.SECONDS.toMillis(maxTimeToWaitSeconds);
    boolean success = false;
    while (System.currentTimeMillis() < endTime) {
      success = (ozoneManager.getRatisSnapshotIndex() == txnLogIndex);
      if (ozoneManager.getRatisSnapshotIndex() == txnLogIndex) {
        break;
      }
      Thread.sleep(intervalTime);
    }

    if (!success) {
      throw new IOException(String.format("After waiting for %d seconds, " +
              "State Machine has not applied  all the transactions.",
          maxTimeToWaitSeconds));
    }
  }

  public static String getRequestType() {
    return Type.Prepare.name();
  }
}
