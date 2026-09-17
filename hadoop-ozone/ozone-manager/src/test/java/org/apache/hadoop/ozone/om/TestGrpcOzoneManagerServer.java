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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.createRequestWithS3Credentials;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.ServiceException;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.grpc.metrics.GrpcMetrics;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.junit.jupiter.api.Test;

/**
 * Tests for GrpcOzoneManagerServer.
 */
public class TestGrpcOzoneManagerServer {

  @Test
  public void testStartStop() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneManager ozoneManager = mock(OzoneManager.class);
    OzoneManagerProtocolServerSideTranslatorPB omServerProtocol = ozoneManager.getOmServerProtocol();

    GrpcOzoneManagerServer server = new GrpcOzoneManagerServer(conf,
        omServerProtocol,
        ozoneManager.getDelegationTokenMgr(),
        ozoneManager.getCertificateClient(),
        "");

    try {
      server.start();
    } finally {
      server.stop();
    }
  }

  @Test
  public void testRequestWithoutS3AuthRejectedWhenAuthRequired() {
    OzoneManagerProtocolServerSideTranslatorPB omTranslator =
        mock(OzoneManagerProtocolServerSideTranslatorPB.class);
    OzoneManagerServiceGrpc service =
        new OzoneManagerServiceGrpc(omTranslator, true);
    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.GetS3VolumeContext)
        .setClientId("test-client")
        .build();
    @SuppressWarnings("unchecked")
    StreamObserver<OMResponse> observer = mock(StreamObserver.class);

    service.submitRequest(request, observer);

    verify(observer).onError(any());
    verify(observer, never()).onNext(any());
    verifyNoInteractions(omTranslator);
  }

  @Test
  public void testServiceListWithoutS3AuthAllowedForS3gBootstrap()
      throws ServiceException {
    // ServiceList is the identity-free bootstrap read the S3 gateway issues
    // before any S3Auth exists (see S3_AUTH_EXEMPT_CMD_TYPES); it must pass
    // even when enforcement is on, or the gateway cannot create its OM client.
    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.ServiceList)
        .setClientId("test-client")
        .build();

    assertRequestPassedThrough(request, true);
  }

  @Test
  public void testRequestWithS3AuthAllowedWhenAuthRequired()
      throws ServiceException {
    // A request that carries S3 authentication is passed through to the
    // translator, which validates the credential; the gate only rejects
    // requests that carry no S3 authentication at all.
    OMRequest request = createRequestWithS3Credentials("accessId", "signature",
        "stringToSign").toBuilder()
        .setCmdType(Type.GetS3VolumeContext)
        .build();

    assertRequestPassedThrough(request, true);
  }

  @Test
  public void testRequestWithoutS3AuthAllowedWhenAuthNotRequired()
      throws ServiceException {
    // With enforcement off (non-secure mode, or the knob disabled) a request
    // without S3 authentication is passed through unchanged.
    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.GetS3VolumeContext)
        .setClientId("test-client")
        .build();

    assertRequestPassedThrough(request, false);
  }

  /**
   * Asserts the request is forwarded to the translator and its response is
   * relayed back to the client (onNext + onCompleted, no onError).
   */
  private static void assertRequestPassedThrough(OMRequest request,
      boolean s3AuthRequired) throws ServiceException {
    OzoneManagerProtocolServerSideTranslatorPB omTranslator =
        mock(OzoneManagerProtocolServerSideTranslatorPB.class);
    OMResponse response = OMResponse.newBuilder()
        .setCmdType(request.getCmdType())
        .setStatus(Status.OK)
        .build();
    when(omTranslator.submitRequest(any(), any())).thenReturn(response);
    OzoneManagerServiceGrpc service =
        new OzoneManagerServiceGrpc(omTranslator, s3AuthRequired);
    @SuppressWarnings("unchecked")
    StreamObserver<OMResponse> observer = mock(StreamObserver.class);

    service.submitRequest(request, observer);

    verify(omTranslator).submitRequest(any(), any());
    verify(observer).onNext(response);
    verify(observer, never()).onError(any());
    verify(observer).onCompleted();
  }

  @Test
  public void testTlsSetupFailureFailsClosed() throws Exception {
    // In secure mode with gRPC TLS enabled, a failure setting up TLS must fail
    // OM startup rather than silently exposing a plaintext, unauthenticated
    // endpoint, and must not leak the GrpcMetrics registration on the way out.
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.setBoolean(HDDS_GRPC_TLS_ENABLED, true);
    OzoneManager ozoneManager = mock(OzoneManager.class);
    CertificateClient caClient = mock(CertificateClient.class);
    when(caClient.getKeyManager())
        .thenThrow(new CertificateException("injected TLS setup failure"));

    assertThrows(IllegalStateException.class, () ->
        new GrpcOzoneManagerServer(conf, ozoneManager.getOmServerProtocol(),
            ozoneManager.getDelegationTokenMgr(), caClient, ""));
    assertNull(DefaultMetricsSystem.instance()
        .getSource(GrpcMetrics.class.getSimpleName()));
  }

  @Test
  public void testS3AuthNotRequiredWhenSecurityDisabled() {
    // The gate is off in non-secure mode even if the knob is left on.
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OMConfigKeys.OZONE_OM_S3_GRPC_AUTH_REQUIRED, true);
    assertFalse(GrpcOzoneManagerServer.isS3AuthRequired(conf));
  }

  @Test
  public void testS3AuthRequiredByDefaultInSecureMode() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    assertTrue(GrpcOzoneManagerServer.isS3AuthRequired(conf));
  }

  @Test
  public void testS3AuthCanBeDisabledInSecureMode() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.setBoolean(OMConfigKeys.OZONE_OM_S3_GRPC_AUTH_REQUIRED, false);
    assertFalse(GrpcOzoneManagerServer.isS3AuthRequired(conf));
  }

}
