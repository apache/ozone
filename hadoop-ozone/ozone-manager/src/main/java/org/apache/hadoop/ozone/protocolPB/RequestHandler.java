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
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.
    OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.
    OMResponse;

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
   * Handle write requests. In HA this will be called from
   * OzoneManagerStateMachine applyTransaction method. In non-HA this will be
   * called from {@link OzoneManagerProtocolServerSideTranslatorPB} for write
   * requests.
   * @param omRequest
   * @param transactionLogIndex - ratis transaction log index
   * @return OMClientResponse
   */
  OMClientResponse handleWriteRequest(OMRequest omRequest,
      long transactionLogIndex);

  /**
   * Update the OzoneManagerDoubleBuffer. This will be called when
   * stateMachine is unpaused and set with new doublebuffer object.
   * @param ozoneManagerDoubleBuffer
   */
  void updateDoubleBuffer(OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer);

}
