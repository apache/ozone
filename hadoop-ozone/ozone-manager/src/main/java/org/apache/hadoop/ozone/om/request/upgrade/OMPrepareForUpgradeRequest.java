package org.apache.hadoop.ozone.om.request.upgrade;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.upgrade.OMFinalizeUpgradeResponse;
import org.apache.hadoop.ozone.om.response.upgrade.OMPrepareForUpgradeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.PrepareForUpgrade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OMPrepareForUpgradeRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMPrepareForUpgradeRequest.class);

  public OMPrepareForUpgradeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    LOG.trace("Request: {}", getOmRequest());
    OMResponse.Builder responseBuilder =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    responseBuilder.setCmdType(PrepareForUpgrade);
    OMClientResponse response = null;

    try {
      // Flush the Ratis log and take a snapshot.
      ozoneManager.prepare();

      // TODO: Create marker file with txn index.
      OzoneManagerProtocolProtos.PrepareForUpgradeResponse omResponse =
          OzoneManagerProtocolProtos.PrepareForUpgradeResponse.newBuilder()
              .setTxnID(transactionLogIndex)
              .build();
      responseBuilder.setPrepareForUpgradeResponse(omResponse);
      response = new OMPrepareForUpgradeResponse(responseBuilder.build());
      LOG.trace("Returning response: {}", response);
    } catch (IOException e) {
      response = new OMFinalizeUpgradeResponse(
          createErrorOMResponse(responseBuilder, e));
    } catch (InterruptedException e) {
      OMException omEx = new OMException(e,
          OMException.ResultCodes.INTERNAL_ERROR);
      response = new OMFinalizeUpgradeResponse(
          createErrorOMResponse(responseBuilder, omEx));
    }

    return response;
  }

  public static String getRequestType() {
    return PrepareForUpgrade.name();
  }
}
