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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class TestOMPrepareForUpgradeRequest extends TestOMKeyRequest {
  @Test
  public void testRequest() {
    final int trxnLogIndex = 1;

    OzoneManagerProtocolProtos.PrepareForUpgradeRequest requestProto =
        OzoneManagerProtocolProtos.PrepareForUpgradeRequest.newBuilder().build();

    OzoneManagerProtocolProtos.OMRequest omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setPrepareForUpgradeRequest(requestProto)
        .setCmdType(OzoneManagerProtocolProtos.Type.PrepareForUpgrade)
        .setClientId(UUID.randomUUID().toString())
        .build();

    OMPrepareForUpgradeRequest prepareRequest =
        new OMPrepareForUpgradeRequest(omRequest);

    OMClientResponse omClientResponse =
        prepareRequest.validateAndUpdateCache(ozoneManager,
            trxnLogIndex, ozoneManagerDoubleBufferHelper);
    OzoneManagerProtocolProtos.OMResponse omResponse = omClientResponse.getOMResponse();

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
    Assert.assertEquals(trxnLogIndex,
        omResponse.getPrepareForUpgradeResponse().getTxnID());
  }
}
