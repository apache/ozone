/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.execution;

import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OMAuditLogger;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * entry for execution flow for write request.
 */
public class OMGateway {
  private static final Logger LOG = LoggerFactory.getLogger(OMGateway.class);

  private final OzoneManager ozoneManager;
  private final OMPerformanceMetrics perfMetrics;

  public OMGateway(OzoneManager om) throws IOException {
    this.ozoneManager = om;
    this.perfMetrics = ozoneManager.getPerfMetrics();
  }

  public void start() {
    // TODO: with pre-ratis execution flow, this is required to manage flow
  }

  public void stop() {
  }

  /**
   * External request handling.
   * 
   * @param omRequest the request
   * @return OMResponse the response of execution
   * @throws ServiceException the exception on execution
   */
  public OMResponse submit(OMRequest omRequest) throws ServiceException {
    // TODO: currently have only execution after ratis submission, but with new flow can have switch later
    return submitExecutionToRatis(omRequest);
  }

  /**
   * Internal request to be directly executed.
   * 
   * @param omRequest the request
   * @param clientId clientId of request
   * @param callId callId of request
   * @return the response of execution
   * @throws ServiceException the exception on execution
   */
  public OMResponse submitInternal(OMRequest omRequest, ClientId clientId, long callId) throws ServiceException {
    return ozoneManager.getOmRatisServer().submitRequest(omRequest, clientId, callId);
  }

  private OMResponse submitExecutionToRatis(OMRequest request) throws ServiceException {
    // 1. create client request and preExecute
    OMClientRequest omClientRequest = null;
    final OMRequest requestToSubmit;
    try {
      omClientRequest = OzoneManagerRatisUtils.createClientRequest(request, ozoneManager);
      assert (omClientRequest != null);
      final OMClientRequest finalOmClientRequest = omClientRequest;
      requestToSubmit = captureLatencyNs(perfMetrics.getPreExecuteLatencyNs(),
          () -> finalOmClientRequest.preExecute(ozoneManager));
    } catch (IOException ex) {
      if (omClientRequest != null) {
        OMAuditLogger.log(omClientRequest.getAuditBuilder());
        omClientRequest.handleRequestFailure(ozoneManager);
      }
      return OzoneManagerRatisUtils.createErrorResponse(request, ex);
    }

    // 2. submit request to ratis
    OMResponse response = ozoneManager.getOmRatisServer().submitRequest(requestToSubmit);
    if (!response.getSuccess()) {
      omClientRequest.handleRequestFailure(ozoneManager);
    }
    return response;
  }
}
