/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.util;

import com.google.protobuf.ByteString;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.hadoop.ozone.util.PayloadUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.util.OMEchoRPCWriteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EchoRPCRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EchoRPCResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Handles EchoRPC request (write).
 */
public class OMEchoRPCWriteRequest extends OMClientRequest {

  public OMEchoRPCWriteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, TermIndex termIndex) {

    EchoRPCRequest echoRPCRequest = getOmRequest().getEchoRPCRequest();

    final ByteString payloadBytes = PayloadUtils.generatePayloadProto2(echoRPCRequest.getPayloadSizeResp());
    EchoRPCResponse echoRPCResponse = EchoRPCResponse.newBuilder()
        .setPayload(payloadBytes)
        .build();

    OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OMClientResponse omClientResponse = new OMEchoRPCWriteResponse(
        omResponse.setEchoRPCResponse(echoRPCResponse).build());

    return omClientResponse;
  }
}
