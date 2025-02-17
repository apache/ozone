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

package org.apache.hadoop.ozone.om.request.upgrade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.om.request.key.OMOpenKeysDeleteRequest;
import org.apache.hadoop.ozone.om.request.key.TestOMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;

/**
 * Unit testing of cancel prepare request. Cancel prepare response does not
 * perform an action, so it has no unit testing.
 */
public class TestOMCancelPrepareRequest extends TestOMKeyRequest {
  private static final long LOG_INDEX = 1;

  @Test
  public void testCancelPrepare() throws Exception {
    assertNotPrepared();
    ozoneManager.getPrepareState().finishPrepare(LOG_INDEX);
    assertPrepared(LOG_INDEX);
    submitCancelPrepareRequest();
    assertNotPrepared();

    // Another cancel prepare should be able to be submitted without error.
    submitCancelPrepareRequest();
    assertNotPrepared();
  }

  private void assertPrepared(long logIndex) {
    OzoneManagerPrepareState prepareState = ozoneManager.getPrepareState();
    OzoneManagerPrepareState.State state = prepareState.getState();

    assertEquals(state.getStatus(), PrepareStatus.PREPARE_COMPLETED);
    assertEquals(state.getIndex(), logIndex);
    assertTrue(prepareState.getPrepareMarkerFile().exists());
    assertFalse(prepareState.requestAllowed(Type.CreateVolume));
  }

  private void assertNotPrepared() {
    OzoneManagerPrepareState prepareState = ozoneManager.getPrepareState();
    OzoneManagerPrepareState.State state = prepareState.getState();

    assertEquals(state.getStatus(), PrepareStatus.NOT_PREPARED);
    assertEquals(state.getIndex(),
        OzoneManagerPrepareState.NO_PREPARE_INDEX);
    assertFalse(prepareState.getPrepareMarkerFile().exists());
    assertTrue(prepareState.requestAllowed(Type.CreateVolume));
  }

  private void submitCancelPrepareRequest() throws Exception {
    OMRequest omRequest =
        doPreExecute(createCancelPrepareRequest());

    OMCancelPrepareRequest cancelPrepareRequest =
        new OMCancelPrepareRequest(omRequest);

    OMClientResponse omClientResponse =
        cancelPrepareRequest.validateAndUpdateCache(ozoneManager, LOG_INDEX);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
  }

  private OMRequest doPreExecute(OMRequest originalOmRequest) throws Exception {
    OMOpenKeysDeleteRequest omOpenKeysDeleteRequest =
        new OMOpenKeysDeleteRequest(originalOmRequest, getBucketLayout());

    OMRequest modifiedOmRequest =
        omOpenKeysDeleteRequest.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set.
    assertNotEquals(originalOmRequest, modifiedOmRequest);

    return modifiedOmRequest;
  }

  private OMRequest createCancelPrepareRequest() {
    OzoneManagerProtocolProtos.CancelPrepareRequest cancelPrepareRequest =
        OzoneManagerProtocolProtos.CancelPrepareRequest.newBuilder().build();

    OzoneManagerProtocolProtos.UserInfo userInfo =
        OzoneManagerProtocolProtos.UserInfo.newBuilder()
            .setUserName("user")
            .setHostName("host")
            .setRemoteAddress("0.0.0.0")
            .build();

    return OMRequest.newBuilder()
        .setCancelPrepareRequest(cancelPrepareRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.CancelPrepare)
        .setClientId(UUID.randomUUID().toString())
        .setUserInfo(userInfo)
        .build();
  }
}
