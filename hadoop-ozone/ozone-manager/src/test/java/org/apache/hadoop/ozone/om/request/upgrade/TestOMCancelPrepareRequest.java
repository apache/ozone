/*
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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.request.upgrade;

import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.om.request.key.TestOMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMOpenKeysDeleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

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

    Assert.assertEquals(state.getStatus(), PrepareStatus.PREPARE_COMPLETED);
    Assert.assertEquals(state.getIndex(), logIndex);
    Assert.assertTrue(prepareState.getPrepareMarkerFile().exists());
    Assert.assertFalse(prepareState.requestAllowed(Type.CreateVolume));
  }

  private void assertNotPrepared() {
    OzoneManagerPrepareState prepareState = ozoneManager.getPrepareState();
    OzoneManagerPrepareState.State state = prepareState.getState();

    Assert.assertEquals(state.getStatus(), PrepareStatus.NOT_PREPARED);
    Assert.assertEquals(state.getIndex(),
        OzoneManagerPrepareState.NO_PREPARE_INDEX);
    Assert.assertFalse(prepareState.getPrepareMarkerFile().exists());
    Assert.assertTrue(prepareState.requestAllowed(Type.CreateVolume));
  }

  private void submitCancelPrepareRequest() throws Exception {
    OMRequest omRequest =
        doPreExecute(createCancelPrepareRequest());

    OMCancelPrepareRequest cancelPrepareRequest =
        new OMCancelPrepareRequest(omRequest);

    OMClientResponse omClientResponse =
        cancelPrepareRequest.validateAndUpdateCache(ozoneManager,
            LOG_INDEX, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
  }

  private OMRequest doPreExecute(OMRequest originalOmRequest) throws Exception {
    OMOpenKeysDeleteRequest omOpenKeysDeleteRequest =
        new OMOpenKeysDeleteRequest(originalOmRequest);

    OMRequest modifiedOmRequest =
        omOpenKeysDeleteRequest.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set.
    Assert.assertNotEquals(originalOmRequest, modifiedOmRequest);

    return modifiedOmRequest;
  }

  private OMRequest createCancelPrepareRequest() {
    OzoneManagerProtocolProtos.CancelPrepareRequest cancelPrepareRequest =
        OzoneManagerProtocolProtos.CancelPrepareRequest.newBuilder().build();

    return OMRequest.newBuilder()
        .setCancelPrepareRequest(cancelPrepareRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.CancelPrepare)
        .setClientId(UUID.randomUUID().toString()).build();
  }
}
