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

package org.apache.hadoop.ozone.nfs.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.net.ssl.SSLSession;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Tests peer/export authorization; certificate chain authentication requires the TLS transport contract. */
class TestNfsTlsPeerInterceptor {
  private static final UUID EXPORT_ID = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");

  private ByteString encoded(UUID export) {
    return ByteString.copyFrom(ByteBuffer.allocate(16).putLong(export.getMostSignificantBits())
        .putLong(export.getLeastSignificantBits()).array());
  }

  @Test
  void allowsOnlyPinnedPeerAndItsExport() throws Exception {
    X509Certificate certificate = mock(X509Certificate.class);
    when(certificate.getEncoded()).thenReturn(new byte[] {1, 2, 3});
    NfsTlsPeerInterceptor interceptor = new NfsTlsPeerInterceptor(
        Map.of(NfsTlsPeerInterceptor.fingerprint(certificate), Set.of(EXPORT_ID)));
    SSLSession session = mock(SSLSession.class);
    when(session.getPeerCertificates()).thenReturn(new Certificate[] {certificate});
    @SuppressWarnings("unchecked")
    ServerCall<String, String> call = mock(ServerCall.class);
    InetSocketAddress address = new InetSocketAddress("127.0.0.1", 12345);
    when(call.getAttributes()).thenReturn(Attributes.newBuilder().set(Grpc.TRANSPORT_ATTR_SSL_SESSION, session)
        .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, address).build());
    ServerCallHandler<String, String> handler = (ignoredCall, ignoredHeaders) -> new ServerCall.Listener<String>() {
      @Override
      public void onMessage(String ignored) {
        assertEquals(EXPORT_ID, NfsTlsPeerInterceptor.requireExport(encoded(EXPORT_ID)));
        assertEquals(address, NfsTlsPeerInterceptor.getPeerAddress());
        assertEquals(Status.Code.PERMISSION_DENIED, assertThrows(StatusRuntimeException.class,
            () -> NfsTlsPeerInterceptor.requireExport(encoded(UUID.randomUUID()))).getStatus().getCode());
        assertEquals(Status.Code.INVALID_ARGUMENT, assertThrows(StatusRuntimeException.class,
            () -> NfsTlsPeerInterceptor.requireExport(ByteString.EMPTY)).getStatus().getCode());
      }
    };
    interceptor.interceptCall(call, new Metadata(), handler).onMessage("test");
    assertEquals(Status.Code.UNAUTHENTICATED, assertThrows(StatusRuntimeException.class,
        () -> NfsTlsPeerInterceptor.requireExport(encoded(EXPORT_ID))).getStatus().getCode());
  }

  @Test
  void rejectsPlaintext() {
    rejectsPeer(Attributes.EMPTY, Status.Code.UNAUTHENTICATED);
  }

  @Test
  void rejectsUnlistedCertificate() throws Exception {
    X509Certificate certificate = mock(X509Certificate.class);
    when(certificate.getEncoded()).thenReturn(new byte[] {4, 5, 6});
    rejectsCertificate(certificate, "127.0.0.1", Status.Code.PERMISSION_DENIED);
  }

  @Test
  void rejectsExpiredCertificate() throws Exception {
    X509Certificate certificate = mock(X509Certificate.class);
    doThrow(new CertificateExpiredException()).when(certificate).checkValidity();
    rejectsCertificate(certificate, "127.0.0.1", Status.Code.UNAUTHENTICATED);
  }

  @Test
  void rejectsNonLoopbackPeer() throws Exception {
    rejectsCertificate(mock(X509Certificate.class), "192.0.2.1", Status.Code.UNAUTHENTICATED);
  }

  private void rejectsCertificate(X509Certificate certificate, String host, Status.Code expected) throws Exception {
    SSLSession session = mock(SSLSession.class);
    when(session.getPeerCertificates()).thenReturn(new Certificate[] {certificate});
    rejectsPeer(Attributes.newBuilder().set(Grpc.TRANSPORT_ATTR_SSL_SESSION, session)
        .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, new InetSocketAddress(host, 12345)).build(), expected);
  }

  private void rejectsPeer(Attributes attributes, Status.Code expected) {
    NfsTlsPeerInterceptor interceptor = new NfsTlsPeerInterceptor(Map.of("0".repeat(64), Set.of(EXPORT_ID)));
    @SuppressWarnings("unchecked")
    ServerCall<String, String> call = mock(ServerCall.class);
    @SuppressWarnings("unchecked")
    ServerCallHandler<String, String> next = mock(ServerCallHandler.class);
    when(call.getAttributes()).thenReturn(attributes);
    interceptor.interceptCall(call, new Metadata(), next);
    ArgumentCaptor<Status> status = ArgumentCaptor.forClass(Status.class);
    verify(call).close(status.capture(), any(Metadata.class));
    assertEquals(expected, status.getValue().getCode());
    verifyNoInteractions(next);
  }

  @Test
  void requiresExplicitNonemptyAuthorization() {
    assertThrows(IllegalArgumentException.class, () -> new NfsTlsPeerInterceptor(Collections.emptyMap()));
    assertThrows(IllegalArgumentException.class,
        () -> new NfsTlsPeerInterceptor(Map.of("0".repeat(64), Collections.emptySet())));
    assertThrows(IllegalArgumentException.class,
        () -> new NfsTlsPeerInterceptor(Map.of("invalid", Set.of(EXPORT_ID))));
    Map<String, Set<UUID>> duplicate = new HashMap<>();
    duplicate.put("a".repeat(64), Set.of(EXPORT_ID));
    duplicate.put("A".repeat(64), Set.of(EXPORT_ID));
    assertThrows(IllegalArgumentException.class, () -> new NfsTlsPeerInterceptor(duplicate));
  }
}
