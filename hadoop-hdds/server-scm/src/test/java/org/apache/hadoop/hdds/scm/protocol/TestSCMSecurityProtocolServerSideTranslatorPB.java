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
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityRequest;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityResponse;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.Status;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.Type;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.tracing.OzoneTracingAttributes;
import org.apache.hadoop.hdds.tracing.TestSpanCollector;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests tracing of {@link SCMSecurityProtocolServerSideTranslatorPB}.
 */
class TestSCMSecurityProtocolServerSideTranslatorPB {

  private static final SCMSecurityRequest REQUEST = SCMSecurityRequest.newBuilder()
      .setCmdType(Type.ListCACertificate)
      .build();

  private SCMSecurityProtocol impl;
  private SCMSecurityProtocolServerSideTranslatorPB service;

  @BeforeEach
  void setUp() {
    impl = mock(SCMSecurityProtocol.class);
    StorageContainerManager scm = mock(StorageContainerManager.class);
    when(scm.checkLeader()).thenReturn(true);
    service = new SCMSecurityProtocolServerSideTranslatorPB(impl, scm, mock(ProtocolMessageMetrics.class));
  }

  @Test
  void testSuccessfulResponseLeavesServerSpanWithoutStatus() throws Exception {
    try (TestSpanCollector collector = TestSpanCollector.install("scm")) {
      assertEquals(Status.OK, service.submitRequest(null, REQUEST).getStatus());
      collector.assertNotError("ListCACertificate");
      collector.assertNoAttribute("ListCACertificate", OzoneTracingAttributes.STATUS);
    }
  }

  @Test
  void testFailedResponseMarksServerSpanError() throws Exception {
    when(impl.listCACertificate()).thenThrow(
        new SCMSecurityException("no CA", SCMSecurityException.ErrorCode.GET_CA_CERT_FAILED));

    try (TestSpanCollector collector = TestSpanCollector.install("scm")) {
      SCMSecurityResponse response = service.submitRequest(null, REQUEST);
      assertEquals(Status.GET_CA_CERT_FAILED, response.getStatus());
      collector.assertError("ListCACertificate", "no CA");
      collector.assertAttribute("ListCACertificate", OzoneTracingAttributes.STATUS, "GET_CA_CERT_FAILED");
      collector.assertExceptionRecorded("ListCACertificate", SCMSecurityException.class);
    }
  }

  @Test
  void testRethrownExceptionIsRecordedOnlyByDispatcher() throws Exception {
    // checkRatisException turns this error code into a ServiceException so that the client retries.
    when(impl.listCACertificate()).thenThrow(
        new SCMSecurityException("retry", SCMSecurityException.ErrorCode.GET_CERTIFICATE_FAILED));

    try (TestSpanCollector collector = TestSpanCollector.install("scm")) {
      assertThrows(ServiceException.class, () -> service.submitRequest(null, REQUEST));
      collector.assertExceptionRecorded("ListCACertificate", ServiceException.class);
      collector.assertNoAttribute("ListCACertificate", OzoneTracingAttributes.STATUS);
    }
  }
}
