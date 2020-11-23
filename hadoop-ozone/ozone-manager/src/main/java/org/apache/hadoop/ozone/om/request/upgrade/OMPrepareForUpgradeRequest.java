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
import org.apache.hadoop.ozone.om.response.upgrade.OMPrepareForUpgradeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.PrepareForUpgrade;

import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * OM Request used to flush all transactions to disk, take a DB snapshot, and
 * purge the logs, leaving Ratis in a clean state without unapplied log
 * entries. This prepares the OM for upgrades/downgrades so that no request
 * in the log is applied to the database in the old version of the code in one
 * OM, and the new version of the code on another OM.
 */
public class OMPrepareForUpgradeRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMPrepareForUpgradeRequest.class);

  // Allow double buffer this many seconds to flush all transactions before
  // returning an error to the caller.
  private static final long DOUBLE_BUFFER_FLUSH_TIMEOUT_SECONDS = 5 * 60;
  // Time between checks to see if double buffer finished flushing.
  private static final long DOUBLE_BUFFER_FLUSH_CHECK_SECONDS = 1;

  public OMPrepareForUpgradeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    LOG.info("Received prepare request with log index {}", transactionLogIndex);

    OMResponse.Builder responseBuilder =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    responseBuilder.setCmdType(PrepareForUpgrade);
    OMClientResponse response = null;

    try {
      // Create response.
      OzoneManagerProtocolProtos.PrepareForUpgradeResponse omResponse =
          OzoneManagerProtocolProtos.PrepareForUpgradeResponse.newBuilder()
              .setTxnID(transactionLogIndex)
              .build();
      responseBuilder.setPrepareForUpgradeResponse(omResponse);
      response = new OMPrepareForUpgradeResponse(responseBuilder.build());

      // Add response to double buffer before clearing logs.
      // This guarantees the log index of this request will be the same as
      // the snapshot index in the prepared state.
      ozoneManagerDoubleBufferHelper.add(response, transactionLogIndex);

      OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();
      OzoneManagerStateMachine omStateMachine =
          omRatisServer.getOmStateMachine();
      RaftServerProxy server = (RaftServerProxy) omRatisServer.getServer();
      RaftServerImpl serverImpl =
          server.getImpl(omRatisServer.getRaftGroup().getGroupId());

      // Wait for outstanding double buffer entries to flush to disk,
      // so they will not be purged from the log before being persisted to
      // the DB.
      RatisUpgradeUtils.waitForAllTxnsApplied(omStateMachine, serverImpl,
          DOUBLE_BUFFER_FLUSH_TIMEOUT_SECONDS,
          DOUBLE_BUFFER_FLUSH_CHECK_SECONDS);

      // Take a snapshot, then purge the Ratis log.
      RatisUpgradeUtils.takeSnapshotAndPurgeLogs(server.getImpl(
          omRatisServer.getRaftGroup().getGroupId()), omStateMachine);

      // TODO: Create marker file with txn index.

      LOG.info("OM prepared at log index {}. Returning response {}",
          ozoneManager.getRatisSnapshotIndex(), omResponse);
    } catch (IOException e) {
      response = new OMPrepareForUpgradeResponse(
          createErrorOMResponse(responseBuilder, e));
    } catch (InterruptedException e) {
      response = new OMPrepareForUpgradeResponse(
          createErrorOMResponse(responseBuilder, new OMException(e,
              OMException.ResultCodes.INTERNAL_ERROR)));
    }

    return response;
  }

  public static String getRequestType() {
    return PrepareForUpgrade.name();
  }
}
