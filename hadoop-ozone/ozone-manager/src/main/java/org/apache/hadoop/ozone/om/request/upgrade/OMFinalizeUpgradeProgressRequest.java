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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.request.upgrade;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.upgrade.OMFinalizeUpgradeProgressResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.FinalizeUpgradeProgressRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.FinalizeUpgradeProgressResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpgradeFinalizationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Handles finalizeUpgradeProgress request that serves to query the status
 * of the async finalization progress.
 */
public class OMFinalizeUpgradeProgressRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMFinalizeUpgradeProgressRequest.class);

  public OMFinalizeUpgradeProgressRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override public OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    LOG.info("Finalization progress check's validateAndUpdateCache"
        + "called and started.");
    LOG.trace("Request: {}", getOmRequest());
    OzoneManagerProtocolProtos.OMResponse.Builder responseBuilder =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    responseBuilder
        .setCmdType(OzoneManagerProtocolProtos.Type.FinalizeUpgradeProgress);
    OMClientResponse response = null;

    try {
      FinalizeUpgradeProgressRequest finalizeUpgradeProgressRequest =
          getOmRequest().getFinalizeUpgradeProgressRequest();
      String upgradeClientID =
          finalizeUpgradeProgressRequest.getUpgradeClientId();
      boolean takeover = finalizeUpgradeProgressRequest.getTakeover();

      UpgradeFinalizationStatus status =
          ozoneManager
              .queryUpgradeFinalizationProgress(upgradeClientID, takeover);

      FinalizeUpgradeProgressResponse omResponse =
          FinalizeUpgradeProgressResponse.newBuilder()
              .setStatus(status)
              .build();

      responseBuilder.setFinalizeUpgradeProgressResponse(omResponse);
      response = new OMFinalizeUpgradeProgressResponse(responseBuilder.build());
      LOG.trace("Returning response: {}", response);
    } catch (IOException e) {
      response = new OMFinalizeUpgradeProgressResponse(
          createErrorOMResponse(responseBuilder, e));
    }

    return response;
  }
}
