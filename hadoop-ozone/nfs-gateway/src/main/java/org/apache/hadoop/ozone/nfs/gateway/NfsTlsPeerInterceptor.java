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

import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

/** Authorize the authenticated TLS peer independently from the NFS caller it asserts. */
public final class NfsTlsPeerInterceptor implements ServerInterceptor {
  private static final Context.Key<Set<UUID>> EXPORTS = Context.key("ozone-nfs-peer-exports");
  private static final Context.Key<InetSocketAddress> ADDRESS = Context.key("ozone-nfs-peer-address");
  private final Map<String, Set<UUID>> allowedPeers;

  public NfsTlsPeerInterceptor(Map<String, Set<UUID>> allowedPeers) {
    Objects.requireNonNull(allowedPeers, "allowedPeers");
    if (allowedPeers.isEmpty()) {
      throw new IllegalArgumentException("An NFS gateway requires explicit TLS peer authorization");
    }
    Map<String, Set<UUID>> copy = new HashMap<>();
    allowedPeers.forEach((fingerprint, exports) -> {
      if (fingerprint == null || !fingerprint.matches("[0-9a-fA-F]{64}") || exports == null || exports.isEmpty()
          || exports.stream().anyMatch(Objects::isNull)) {
        throw new IllegalArgumentException("Invalid NFS peer fingerprint or export allowlist");
      }
      String normalized = fingerprint.toLowerCase(Locale.ROOT);
      if (copy.put(normalized, Collections.unmodifiableSet(new HashSet<>(exports))) != null) {
        throw new IllegalArgumentException("Duplicate NFS peer fingerprint");
      }
    });
    this.allowedPeers = Collections.unmodifiableMap(copy);
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {
    SSLSession session = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
    Object remote = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
    if (session == null || !(remote instanceof InetSocketAddress)
        || ((InetSocketAddress) remote).getAddress() == null
        || !((InetSocketAddress) remote).getAddress().isLoopbackAddress()) {
      return reject(call, Status.UNAUTHENTICATED.withDescription("Authenticated loopback TLS peer required"));
    }
    try {
      Certificate[] chain = session.getPeerCertificates();
      if (chain.length == 0 || !(chain[0] instanceof X509Certificate)) {
        return reject(call, Status.UNAUTHENTICATED.withDescription("Client certificate required"));
      }
      X509Certificate certificate = (X509Certificate) chain[0];
      certificate.checkValidity();
      Set<UUID> exports = allowedPeers.get(fingerprint(certificate));
      if (exports == null) {
        return reject(call, Status.PERMISSION_DENIED.withDescription("TLS peer is not authorized for NFS"));
      }
      Context context = Context.current().withValue(EXPORTS, exports)
          .withValue(ADDRESS, (InetSocketAddress) remote);
      return Contexts.interceptCall(context, call, headers, next);
    } catch (SSLPeerUnverifiedException | CertificateException ex) {
      return reject(call, Status.UNAUTHENTICATED.withDescription("Client certificate verification failed"));
    }
  }

  private static <ReqT, RespT> ServerCall.Listener<ReqT> reject(ServerCall<ReqT, RespT> call, Status status) {
    call.close(status, new Metadata());
    return new ServerCall.Listener<ReqT>() { };
  }

  /** Called by each service operation before using an export or trusting an asserted caller. */
  public static UUID requireExport(ByteString encoded) {
    Set<UUID> exports = EXPORTS.get();
    if (exports == null) {
      throw Status.UNAUTHENTICATED.withDescription("Missing authenticated NFS peer").asRuntimeException();
    }
    if (encoded == null || encoded.size() != 16) {
      throw Status.INVALID_ARGUMENT.withDescription("Export UUID must contain 16 bytes").asRuntimeException();
    }
    ByteBuffer bytes = encoded.asReadOnlyByteBuffer();
    UUID export = new UUID(bytes.getLong(), bytes.getLong());
    if (!exports.contains(export)) {
      throw Status.PERMISSION_DENIED.withDescription("TLS peer cannot access this export").asRuntimeException();
    }
    return export;
  }

  public static InetSocketAddress getPeerAddress() {
    InetSocketAddress address = ADDRESS.get();
    if (address == null) {
      throw Status.UNAUTHENTICATED.withDescription("Missing authenticated NFS peer").asRuntimeException();
    }
    return address;
  }

  public static String fingerprint(X509Certificate certificate) throws CertificateException {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded());
      StringBuilder result = new StringBuilder(64);
      for (byte value : digest) {
        result.append(Character.forDigit((value >>> 4) & 15, 16)).append(Character.forDigit(value & 15, 16));
      }
      return result.toString();
    } catch (NoSuchAlgorithmException ex) {
      throw new IllegalStateException("SHA-256 is unavailable", ex);
    }
  }
}
