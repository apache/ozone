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
import static org.mockito.Mockito.doNothing;

import java.io.IOException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.UpgradeFinalizationStatus;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.protocol.ClientId;
import org.junit.jupiter.api.Test;

/**
 * Tests for OMStartFinalizeUpgradeRequestLegacy, the old CLI ({@code ozone admin om finalizeupgrade}) initiate
 * request. Shared initiate behavior is covered by {@link TestOMStartFinalizeUpgradeRequestBase}; the case here
 * asserts the {@code STARTING_FINALIZATION} response the old CLI requires before it polls progress.
 */
public class TestOMStartFinalizeUpgradeRequestLegacy extends TestOMStartFinalizeUpgradeRequestBase {

  @Override
  protected OMFinalizeUpgradeRequestBase newRequest() {
    return new OMStartFinalizeUpgradeRequestLegacy(buildRequest());
  }

  @Test
  public void testValidateAndUpdateCacheReturnsStartingFinalization() throws IOException {
    doNothing().when(scmContainerLocationProtocol).finalizeUpgrade();

    OMClientResponse response = submitRequest();
    OMResponse omResponse = response.getOMResponse();

    // The old CLI requires STARTING_FINALIZATION back before it polls progress.
    assertEquals(UpgradeFinalizationStatus.Status.STARTING_FINALIZATION,
        omResponse.getFinalizeUpgradeResponse().getStatus().getStatus());
  }

  private OzoneManagerProtocolProtos.OMRequest buildRequest() {
    return OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.FinalizeUpgrade)
        .setClientId(ClientId.randomId().toString())
        .setFinalizeUpgradeRequest(OzoneManagerProtocolProtos.FinalizeUpgradeRequest.newBuilder()
            .setUpgradeClientId("client-id"))
        .build();
  }
}
