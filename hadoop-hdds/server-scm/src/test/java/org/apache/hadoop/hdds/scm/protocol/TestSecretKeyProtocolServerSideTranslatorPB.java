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

package org.apache.hadoop.hdds.scm.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocolScm;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.SCMSecretKeyRequest;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.SCMSecretKeyResponse;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.Status;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.Type;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.exception.SCMSecretKeyException;
import org.apache.hadoop.hdds.tracing.OzoneTracingAttributes;
import org.apache.hadoop.hdds.tracing.TestSpanCollector;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests tracing of {@link SecretKeyProtocolServerSideTranslatorPB}.
 */
class TestSecretKeyProtocolServerSideTranslatorPB {

  private static final SCMSecretKeyRequest REQUEST = SCMSecretKeyRequest.newBuilder()
      .setCmdType(Type.GetCurrentSecretKey)
      .build();

  private SecretKeyProtocolScm impl;
  private SecretKeyProtocolServerSideTranslatorPB service;

  @BeforeEach
  void setUp() {
    impl = mock(SecretKeyProtocolScm.class);
    StorageContainerManager scm = mock(StorageContainerManager.class);
    when(scm.checkLeader()).thenReturn(true);
    service = new SecretKeyProtocolServerSideTranslatorPB(impl, scm, mock(ProtocolMessageMetrics.class));
  }

  @Test
  void testSuccessfulResponseLeavesServerSpanWithoutStatus() throws Exception {
    SCMSecretKeyRequest request = SCMSecretKeyRequest.newBuilder().setCmdType(Type.GetAllSecretKeys).build();

    try (TestSpanCollector collector = TestSpanCollector.install("scm")) {
      assertEquals(Status.OK, service.submitRequest(null, request).getStatus());
      collector.assertNotError("GetAllSecretKeys");
      collector.assertNoAttribute("GetAllSecretKeys", OzoneTracingAttributes.ERROR_STATUS);
    }
  }

  @Test
  void testFailedResponseMarksServerSpanError() throws Exception {
    when(impl.getCurrentSecretKey()).thenThrow(
        new SCMSecretKeyException("not initialized", SCMSecretKeyException.ErrorCode.SECRET_KEY_NOT_INITIALIZED));

    try (TestSpanCollector collector = TestSpanCollector.install("scm")) {
      SCMSecretKeyResponse response = service.submitRequest(null, REQUEST);
      assertEquals(Status.SECRET_KEY_NOT_INITIALIZED, response.getStatus());
      collector.assertError("GetCurrentSecretKey", "not initialized");
      collector.assertAttribute("GetCurrentSecretKey", OzoneTracingAttributes.ERROR_STATUS,
          "SECRET_KEY_NOT_INITIALIZED");
      collector.assertExceptionRecorded("GetCurrentSecretKey", SCMSecretKeyException.class);
    }
  }

  @Test
  void testRethrownExceptionIsRecordedOnlyByDispatcher() throws Exception {
    when(impl.getCurrentSecretKey()).thenThrow(new StateMachineException("apply failed"));

    try (TestSpanCollector collector = TestSpanCollector.install("scm")) {
      assertThrows(ServiceException.class, () -> service.submitRequest(null, REQUEST));
      collector.assertExceptionRecorded("GetCurrentSecretKey", ServiceException.class);
      collector.assertNoAttribute("GetCurrentSecretKey", OzoneTracingAttributes.ERROR_STATUS);
    }
  }
}
