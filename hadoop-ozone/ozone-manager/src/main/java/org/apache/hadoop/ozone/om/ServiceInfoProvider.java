/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.slf4j.Logger;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * A helper class to handle the construction of
 * {@link org.apache.hadoop.ozone.om.helpers.ServiceInfoEx} objects,
 * and refresh of the things that are necessary for constructing it.
 */
public final class ServiceInfoProvider {

  private static final Logger LOG = getLogger(ServiceInfoProvider.class);

  private final OzoneManagerProtocol om;
  private final CertificateClient certClient;

  private String caCertPem;
  private List<String> caCertPemList;

  public ServiceInfoProvider(
      OzoneConfiguration config,
      OzoneManagerProtocol om,
      CertificateClient certClient
  ) throws IOException {
    this(config, om, certClient, false);
  }

  public ServiceInfoProvider(
      OzoneConfiguration config,
      OzoneManagerProtocol om,
      CertificateClient certClient,
      boolean skipInitializationForTesting
  ) throws IOException {
    this.om = om;
    if (new SecurityConfig(config).isSecurityEnabled()
        && !skipInitializationForTesting) {
      this.certClient = certClient;
      Set<X509Certificate> certs = getCACertificates();
      caCertPem = toPEMEncodedString(newestOf(certs));
      caCertPemList = toPEMEncodedStrings(certs);
      this.certClient.registerRootCARotationListener(onRootCAChange());
    } else {
      this.certClient = null;
      caCertPem = null;
      caCertPemList = emptyList();
    }
  }

  private Function<List<X509Certificate>, CompletableFuture<Void>>
      onRootCAChange() {
    return certs -> {
      CompletableFuture<Void> returnedFuture = new CompletableFuture<>();
      try {
        caCertPem = toPEMEncodedString(newestOf(certs));
        caCertPemList = toPEMEncodedStrings(certs);
        returnedFuture.complete(null);
      } catch (Exception e) {
        LOG.info("Unable to refresh cached PEM formatted CA " +
            "certificates.", e);
        returnedFuture.completeExceptionally(e);
      }
      return returnedFuture;
    };
  }

  public ServiceInfoEx provide() throws IOException {
    return new ServiceInfoEx(om.getServiceList(), caCertPem, caCertPemList);
  }

  private Set<X509Certificate> getCACertificates() {
    return !certClient.getAllRootCaCerts().isEmpty()
        ? certClient.getAllRootCaCerts()
        : certClient.getAllCaCerts();
  }

  private X509Certificate newestOf(Collection<X509Certificate> certs) {
    return certs.stream()
        .max((c1, c2) -> expiry(c1).compareTo(expiry(c2)))
        .orElse(null);
  }

  private String toPEMEncodedString(X509Certificate cert)
      throws SCMSecurityException {
    return cert == null ? null : CertificateCodec.getPEMEncodedString(cert);
  }

  private String toPEMEncodedStringUnsafe(X509Certificate cert) {
    try {
      return toPEMEncodedString(cert);
    } catch (SCMSecurityException e) {
      return null;
    }
  }

  private List<String> toPEMEncodedStrings(Collection<X509Certificate> certs) {
    return certs.stream()
        .map(this::toPEMEncodedStringUnsafe)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private Instant expiry(X509Certificate cert) {
    return cert.getNotAfter().toInstant();
  }
}
