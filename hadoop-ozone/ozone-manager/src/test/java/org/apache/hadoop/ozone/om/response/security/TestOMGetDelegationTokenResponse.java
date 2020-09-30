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

package org.apache.hadoop.ozone.om.response.security;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.om.request.security.OMGetDelegationTokenRequest;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateGetDelegationTokenRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import java.io.IOException;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** The class tests OMGetDelegationTokenResponse. */
public class TestOMGetDelegationTokenResponse extends
    TestOMDelegationTokenResponse {

  private OzoneTokenIdentifier identifier;
  private UpdateGetDelegationTokenRequest updateGetDelegationTokenRequest;

  @Before
  public void setupGetDelegationToken() {
    Text tester = new Text("tester");
    identifier = new OzoneTokenIdentifier(tester, tester, tester);
    identifier.setOmCertSerialId("certID");

    GetDelegationTokenRequestProto getDelegationTokenRequestProto =
        GetDelegationTokenRequestProto.newBuilder()
        .setRenewer(identifier.getRenewer().toString())
        .build();

    OMRequest omRequest = OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.GetDelegationToken)
        .setGetDelegationTokenRequest(getDelegationTokenRequestProto)
        .build();

    updateGetDelegationTokenRequest =
        new OMGetDelegationTokenRequest(omRequest)
            .getOmRequest()
            .getUpdateGetDelegationTokenRequest();
  }

  @Test
  public void testAddToDBBatch() throws IOException {
    OMResponse omResponse = OMResponse.newBuilder()
        .setCmdType(Type.GetDelegationToken)
        .setStatus(Status.OK)
        .setSuccess(true)
        .setGetDelegationTokenResponse(
            updateGetDelegationTokenRequest
                .getGetDelegationTokenResponse())
        .build();

    long renewTime = 1000L;
    OMGetDelegationTokenResponse getDelegationTokenResponse =
        new OMGetDelegationTokenResponse(identifier, renewTime, omResponse);

    getDelegationTokenResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    long rowNumInTable = 1;
    long rowNumInTokenTable = omMetadataManager
        .countRowsInTable(omMetadataManager.getDelegationTokenTable());
    Assert.assertEquals(rowNumInTable, rowNumInTokenTable);

    long renewTimeInTable = omMetadataManager.getDelegationTokenTable()
        .get(identifier);
    Assert.assertEquals(renewTime, renewTimeInTable);
  }
}
