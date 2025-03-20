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
import java.util.function.Supplier;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OMAuditLogger;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.protocol.ClientId;

/**
 * entry for execution flow for write request.
 */
public class OMExecutionFlow {

  private final OzoneManager ozoneManager;
  private final OMPerformanceMetrics perfMetrics;
  private final Supplier<Long> indexGenerator;

  public OMExecutionFlow(OzoneManager om) throws IOException {
    this.ozoneManager = om;
    this.perfMetrics = ozoneManager.getPerfMetrics();
    indexGenerator = om.getOmRatisServer().getOmStateMachine().getIndexManager()::nextIndex;
  }

  /**
   * Internal request handling with defined clientId and callId.
   *
   * @param omRequest the request
   * @param clientId the clientId
   * @param callId the callId
   * @return OMResponse the response of execution
   * @throws ServiceException the exception on execution
   */
  public OMResponse submitInternal(OMRequest omRequest, ClientId clientId, long callId) throws ServiceException {
    return ozoneManager.getOmRatisServer().submitRequest(updateControlRequest(omRequest), clientId, callId);
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

  private OMResponse submitExecutionToRatis(OMRequest request) throws ServiceException {
    // 1. create client request and preExecute
    OMClientRequest omClientRequest = null;
    OMRequest requestToSubmit;
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
    requestToSubmit = updateControlRequest(requestToSubmit);
    OMResponse response = ozoneManager.getOmRatisServer().submitRequest(requestToSubmit);
    if (!response.getSuccess()) {
      omClientRequest.handleRequestFailure(ozoneManager);
    }
    return response;
  }

  private OMRequest updateControlRequest(OMRequest requestToSubmit) {
    OzoneManagerProtocolProtos.ExecutionControlRequest controlRequest =
        OzoneManagerProtocolProtos.ExecutionControlRequest.newBuilder().setIndex(indexGenerator.get()).build();
    requestToSubmit = requestToSubmit.toBuilder().setExecutionControlRequest(controlRequest).build();
    return requestToSubmit;
  }
}
