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

/**
 * entry for execution flow for write request.
 */
public class OMExecutionFlow {

  private final OzoneManager ozoneManager;
  private final OMPerformanceMetrics perfMetrics;

  public OMExecutionFlow(OzoneManager om) {
    this.ozoneManager = om;
    this.perfMetrics = ozoneManager.getPerfMetrics();
  }

  /**
   * External request handling.
   * 
   * @param omRequest the request
   * @return OMResponse the response of execution
   * @throws ServiceException the exception on execution
   */
  public OMResponse submit(OMRequest omRequest, boolean isWrite) throws ServiceException {
    // TODO: currently have only execution after ratis submission, but with new flow can have switch later
    return submitExecutionToRatis(omRequest, isWrite);
  }

  private OMResponse submitExecutionToRatis(OMRequest request, boolean isWrite) throws ServiceException {
    // 1. create client request and preExecute
    OMClientRequest omClientRequest = null;
    final OMRequest requestToSubmit;
    if (isWrite) {
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
    } else {
      requestToSubmit = request;
    }

    // 2. submit request to ratis
    OMResponse response = ozoneManager.getOmRatisServer().submitRequest(requestToSubmit, isWrite);
    if (!response.getSuccess() && omClientRequest != null) {
      omClientRequest.handleRequestFailure(ozoneManager);
    }
    return response;
  }
}
