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

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.upgrade.OMCancelPrepareResponse;
import org.apache.hadoop.ozone.om.response.upgrade.OMPrepareResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CancelPrepareResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *  OM request class to cancel preparation.
 */
public class OMCancelPrepareRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMCancelPrepareRequest.class);

  public OMCancelPrepareRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    LOG.info("OM {} Received cancel prepare request with log index {}",
        ozoneManager.getOMNodeId(), transactionLogIndex);

    OMRequest omRequest = getOmRequest();
    OMResponse.Builder responseBuilder =
        OmResponseUtil.getOMResponseBuilder(omRequest);
    responseBuilder.setCmdType(Type.CancelPrepare);
    OMClientResponse response = null;

    try {
      if (ozoneManager.getAclsEnabled() && !ozoneManager.isAdmin(createUGI())) {
        throw new OMException("Access denied for user "
            + createUGI() + ". " +
            "Superuser privilege is required to cancel ozone manager " +
            "preparation.",
            OMException.ResultCodes.ACCESS_DENIED);
      }

      // Create response.
      CancelPrepareResponse omResponse = CancelPrepareResponse.newBuilder()
          .build();
      responseBuilder.setCancelPrepareResponse(omResponse);
      response = new OMCancelPrepareResponse(responseBuilder.build());

      // Deletes on disk marker file, does not update DB and therefore does
      // not update cache.
      ozoneManager.getPrepareState().cancelPrepare();
      ozoneManagerDoubleBufferHelper.add(response, transactionLogIndex);

      LOG.info("OM {} prepare state cancelled at log index {}. Returning " +
              "response {}",
          ozoneManager.getOMNodeId(), transactionLogIndex, omResponse);
    } catch (IOException e) {
      LOG.error("Cancel Prepare Request apply failed in {}. ",
          ozoneManager.getOMNodeId(), e);
      response = new OMPrepareResponse(
          createErrorOMResponse(responseBuilder, e));
    }

    return response;
  }

}
