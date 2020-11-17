package org.apache.hadoop.ozone.om.request.upgrade;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.upgrade.OMFinalizeUpgradeResponse;
import org.apache.hadoop.ozone.om.response.upgrade.OMPrepareForUpgradeResponse;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.PrepareForUpgrade;

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
      OMPrepareForUpgradeRequest request =
          getOmRequest().getPrepareForUgradeRequest();

      // TODO:
      //  Set in memory upgrade flag
      //  Take snapshot at index and purge
      //  Create marker file with txn index.

      // TODO: Determine if upgrade client ID should be pressent in
      //  request/response.
      PrepareForUpgraddeResponse omResponse =
          PrepareForUpgradeResponse.newBuilder()
              .setTrxnID(transactionLogIndex)
              .build();
      responseBuilder.setPrepareForUpgradeResponse(omResponse);
      response = new OMPrepareForUpgradeResponse(responseBuilder.build());
      LOG.trace("Returning response: {}", response);
    } catch (IOException e) {
      response = new OMFinalizeUpgradeResponse(
          createErrorOMResponse(responseBuilder, e));
    }

    return response;
  }

  public static String getRequestType() {
    return PrepareForUpgrade.name();
  }
}
