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

package org.apache.hadoop.ozone.protocolPB;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.server.protocol.TermIndex;

import java.io.IOException;

/**
 * Handler to handleRequest the OmRequests.
 */
public interface RequestHandler {

  /**
   * Handle the read requests, and returns OmResponse.
   * @param request
   * @return OmResponse
   */
  OMResponse handleReadRequest(OMRequest request);

  /**
   * Validates that the incoming OM request has required parameters.
   * TODO: Add more validation checks before writing the request to Ratis log.
   *
   * @param omRequest client request to OM
   * @throws OMException thrown if required parameters are set to null.
   */
  void validateRequest(OMRequest omRequest) throws OMException;

  /**
   * Handle write requests.
   * In HA this will be called from OzoneManagerStateMachine applyTransaction method.
   * In non-HA this will be called from {@link OzoneManagerProtocolServerSideTranslatorPB}.
   *
   * @param omRequest the write request
   * @param termIndex - ratis transaction term and index
   * @param ozoneManagerDoubleBuffer for adding response
   * @return OMClientResponse
   */
  default OMClientResponse handleWriteRequest(OMRequest omRequest, TermIndex termIndex,
      OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer) throws IOException {
    final OMClientResponse response = handleWriteRequestImpl(omRequest, termIndex);
    if (omRequest.getCmdType() != Type.Prepare) {
      ozoneManagerDoubleBuffer.add(response, termIndex);
    }
    return response;
  }

  /**
   * Implementation of {@link #handleWriteRequest}.
   *
   * @param omRequest the write request
   * @param termIndex - ratis transaction term and index
   * @return OMClientResponse
   */
  OMClientResponse handleWriteRequestImpl(OMRequest omRequest, TermIndex termIndex) throws IOException;

  /**
   * Handle write request at leader execution.
   *
   * @param omClientRequest the write cleitn request
   * @param termIndex - ratis transaction term and index
   * @return OMClientResponse
   */
  OMClientResponse handleLeaderWriteRequest(OMClientRequest omClientRequest, TermIndex termIndex) throws IOException;
}
