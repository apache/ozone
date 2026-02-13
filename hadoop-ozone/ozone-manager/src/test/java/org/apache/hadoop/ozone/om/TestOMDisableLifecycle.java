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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FEATURE_NOT_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.junit.jupiter.api.Test;

/**
 * Try different configs against OzoneManager#checkLifecycleEnabled and verify its response.
 */
public class TestOMDisableLifecycle {

  @Test
  public void testLifecycleWhenDisabled() throws Exception {
    final OzoneManager om = mock(OzoneManager.class);
    doCallRealMethod().when(om).checkLifecycleEnabled();
    final String volumeName = "vol1";
    final String bucketName = "bucket1";

    // Check that Lifecycle writes requests are blocked when not enabled
    expectWriteRequestToFail(om, OMRequestTestUtils.setLifecycleConfigurationRequest(volumeName, bucketName));
    expectWriteRequestToFail(om, OMRequestTestUtils.deleteLifecycleConfigurationRequest(volumeName, bucketName));
    expectWriteRequestToFail(om, OMRequestTestUtils.setLifecycleServiceStatus());

    // Check that Lifecycle read requests are blocked when not enabled

    final OzoneManagerRequestHandler ozoneManagerRequestHandler =
        new OzoneManagerRequestHandler(om);

    expectReadRequestToFail(ozoneManagerRequestHandler,
        OMRequestTestUtils.getLifecycleConfigurationRequest(volumeName, bucketName));
    expectReadRequestToFail(ozoneManagerRequestHandler,
        OMRequestTestUtils.getLifecycleServiceStatus());
  }

  /**
   * Helper function for testLifecycleRPCWhenDisabled.
   */
  private void expectWriteRequestToFail(OzoneManager om, OMRequest request)
      throws IOException {
    OMException e =
        assertThrows(OMException.class, () -> OzoneManagerRatisUtils.createClientRequest(request, om));
    assertEquals(FEATURE_NOT_ENABLED, e.getResult());
  }

  /**
   * Helper function for tesLifecycleRPCWhenDisabled.
   */
  private void expectReadRequestToFail(OzoneManagerRequestHandler handler,
      OMRequest omRequest) {

    // handleReadRequest does not throw
    OMResponse omResponse = handler.handleReadRequest(omRequest);
    assertFalse(omResponse.getSuccess());
    assertEquals(Status.FEATURE_NOT_ENABLED, omResponse.getStatus());
    assertTrue(omResponse.getMessage()
        .startsWith("OM Lifecycle feature is not enabled."));
  }

}
