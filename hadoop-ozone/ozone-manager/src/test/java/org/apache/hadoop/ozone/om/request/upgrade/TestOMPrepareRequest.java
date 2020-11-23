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

package org.apache.hadoop.ozone.om.request.upgrade;

import org.apache.hadoop.ozone.om.request.key.TestOMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareRequest;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class TestOMPrepareRequest extends TestOMKeyRequest {
  @Test
  public void testRequest() {
    final int trxnLogIndex = 1;

    PrepareRequest requestProto =
        PrepareRequest.newBuilder().build();

    OMRequest omRequest = OMRequest.newBuilder()
        .setPrepareRequest(requestProto)
        .setCmdType(Type.Prepare)
        .setClientId(UUID.randomUUID().toString())
        .build();

    OMPrepareRequest prepareRequest =
        new OMPrepareRequest(omRequest);

    OMClientResponse omClientResponse =
        prepareRequest.validateAndUpdateCache(ozoneManager,
            trxnLogIndex, ozoneManagerDoubleBufferHelper);
    OMResponse omResponse = omClientResponse.getOMResponse();

    Assert.assertEquals(Status.OK, omResponse.getStatus());
    Assert.assertEquals(trxnLogIndex,
        omResponse.getPrepareForUpgradeResponse().getTxnID());
  }
}
