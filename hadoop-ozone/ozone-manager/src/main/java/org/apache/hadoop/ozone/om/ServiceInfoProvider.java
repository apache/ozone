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

import static java.util.Collections.emptyList;
import static java.util.Comparator.comparing;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.slf4j.Logger;

/**
 * A helper class for Ozone Manager, to take on the responsibility of caching
 * the actual CA certificates in PEM format, and handle the update of these
 * cached values, so that OM can provide this information to clients that needs
 * to trust the DataNode certificates in a secure environment.
 */
final class ServiceInfoProvider {

  private static final Logger LOG = getLogger(ServiceInfoProvider.class);

  private final OzoneManagerProtocol om;
  private final CertificateClient certClient;

  private String caCertPEM;
  private List<String> caCertPEMList;

  /**
   * Initializes the provider.
   * The OzoneManagerProtocol implementation is used to provide the service
   * list part of the ServiceInfoEx object this class provides for OM, while
   * the SecurityConfig and the CertificateClient is used to provide security
   * and trust related information within the same object.
   *
   * @param config the current security configuration
   * @param om the OzoneManagerProtocol provides the service list
   * @param certClient the CertificateClient provides certificate information
   */
  ServiceInfoProvider(SecurityConfig config, OzoneManagerProtocol om,
      CertificateClient certClient) {
    this(config, om, certClient, false);
  }

  /**
   * Initializes the provider.
   * The OzoneManagerProtocol implementation is used to provide the service
   * list part of the ServiceInfoEx object this class provides for OM, while
   * the SecurityConfig and the CertificateClient is used to provide security
   * and trust related information within the same object.
   *
   * In some cases OM is initializing this class before a properly set up
   * CertificateClient is given to the OM for tests. In this case the
   * initialization code would fail, so this is handled in OM when a new
   * CertificateClient is set the provider is re-created, but for this to work,
   * the initial initialization is disabled when we are in a test process.
   *
   * @param config the current security configuration
   * @param om the OzoneManagerProtocol provides the service list
   * @param certClient the CertificateClient provides certificate information
   * @param skipInitializationForTesting if we are testing OM in secure env this
   *                                     might need to be true
   */
  ServiceInfoProvider(SecurityConfig config, OzoneManagerProtocol om,
      CertificateClient certClient, boolean skipInitializationForTesting) {
    this.om = om;
    if (config.isSecurityEnabled() && !skipInitializationForTesting) {
      this.certClient = certClient;
      Set<X509Certificate> certs = getCACertificates();
      caCertPEM = toPEMEncodedString(newestOf(certs));
      caCertPEMList = toPEMEncodedStrings(certs);
      this.certClient.registerRootCARotationListener(onRootCAChange());
    } else {
      this.certClient = null;
      caCertPEM = null;
      caCertPEMList = emptyList();
    }
  }

  private Function<List<X509Certificate>, CompletableFuture<Void>>
      onRootCAChange() {
    return certs -> {
      CompletableFuture<Void> returnedFuture = new CompletableFuture<>();
      try {
        synchronized (this) {
          caCertPEM = toPEMEncodedString(newestOf(certs));
          caCertPEMList = toPEMEncodedStrings(certs);
        }
        returnedFuture.complete(null);
      } catch (Exception e) {
        LOG.error("Unable to refresh cached PEM formatted CA certificates.", e);
        returnedFuture.completeExceptionally(e);
      }
      return returnedFuture;
    };
  }

  public ServiceInfoEx provide() throws IOException {
    String returnedCaCertPEM;
    List<String> returnedCaCertPEMList;
    synchronized (this) {
      returnedCaCertPEM = caCertPEM;
      returnedCaCertPEMList = new ArrayList<>(caCertPEMList);
    }
    return new ServiceInfoEx(
        om.getServiceList(), returnedCaCertPEM, returnedCaCertPEMList);
  }

  private Set<X509Certificate> getCACertificates() {
    Set<X509Certificate> rootCerts = certClient.getAllRootCaCerts();
    return !rootCerts.isEmpty() ? rootCerts : certClient.getAllCaCerts();
  }

  private X509Certificate newestOf(Collection<X509Certificate> certs) {
    return certs.stream()
        .max(comparing(X509Certificate::getNotAfter))
        .orElse(null);
  }

  private String toPEMEncodedString(X509Certificate cert) {
    try {
      return cert == null ? null : CertificateCodec.getPEMEncodedString(cert);
    } catch (SCMSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> toPEMEncodedStrings(Collection<X509Certificate> certs) {
    return certs.stream()
        .map(this::toPEMEncodedString)
        .collect(Collectors.toList());
  }
}
