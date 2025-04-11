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

package org.apache.hadoop.hdds.security.ssl;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TrustManager} implementation that exposes a method,
 * {@link #init(List)} to reload its configuration for
 * example when the truststore file on disk changes.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ReloadingX509TrustManager implements X509TrustManager, CertificateNotification {

  private static final Logger LOG = LoggerFactory.getLogger(ReloadingX509TrustManager.class);

  static final String RELOAD_ERROR_MESSAGE = "Could not reload truststore (keep using existing one) : ";

  private static final X509Certificate[] EMPTY = new X509Certificate[0];

  private final String type;
  private final AtomicReference<X509TrustManager> trustManagerRef;

  /**
   * Current Root CA cert in trustManager, to detect if certificate is changed.
   */
  private List<X509Certificate> currentRootCACerts = new ArrayList<>();

  /**
   * Creates a reloadable trustmanager. The trustmanager reloads itself
   * if the underlying truststore materials have changed.
   *
   * @param type           type of truststore file, typically 'jks'.
   * @param newRootCaCerts the newest known trusted certificates.
   * @throws IOException              thrown if the truststore could not be initialized due
   *                                  to an IO error.
   * @throws GeneralSecurityException thrown if the truststore could not be
   *                                  initialized due to a security error.
   */
  public ReloadingX509TrustManager(String type, List<X509Certificate> newRootCaCerts)
      throws GeneralSecurityException, IOException {
    this.type = type;
    trustManagerRef = new AtomicReference<X509TrustManager>();
    trustManagerRef.set(init(newRootCaCerts));
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      try {
        tm.checkClientTrusted(chain, authType);
      } catch (CertificateException e) {
        LOG.info("Client certificate chain {} for authType {} is not trusted",
            chain == null ? "" : Arrays.stream(chain)
                .map(X509Certificate::getSubjectX500Principal)
                .map(X500Principal::toString)
                .collect(Collectors.joining(",")), authType);
        throw e;
      }
    } else {
      throw new CertificateException("Unknown client chain certificate: " +
          chain[0].toString());
    }
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      try {
        tm.checkServerTrusted(chain, authType);
      } catch (CertificateException e) {
        LOG.info("Client certificate chain {} for authType {} is not trusted",
            chain == null ? "" : Arrays.stream(chain)
                .map(X509Certificate::getSubjectX500Principal)
                .map(X500Principal::toString)
                .collect(Collectors.joining(",")), authType);
        throw e;
      }
    } else {
      throw new CertificateException("Unknown server chain certificate: " +
          chain[0].toString());
    }
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    X509Certificate[] issuers = EMPTY;
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      issuers = tm.getAcceptedIssuers();
    }
    return issuers;
  }

  private X509TrustManager init(List<X509Certificate> newRootCaCerts)
      throws GeneralSecurityException, IOException {
    // Certificate keeps the same.
    if (isAlreadyUsing(newRootCaCerts)) {
      return null;
    }

    X509TrustManager trustManager = null;
    KeyStore ks = KeyStore.getInstance(type);
    ks.load(null, null);
    insertCertsToKeystore(newRootCaCerts, ks);

    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
        TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(ks);
    TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
    for (TrustManager trustManager1 : trustManagers) {
      if (trustManager1 instanceof X509TrustManager) {
        trustManager = (X509TrustManager) trustManager1;
        break;
      }
    }
    currentRootCACerts = newRootCaCerts;
    return trustManager;
  }

  private boolean isAlreadyUsing(List<X509Certificate> newRootCaCerts) {
    return !newRootCaCerts.isEmpty() &&
        currentRootCACerts.size() == newRootCaCerts.size() &&
        newRootCaCerts.stream()
            .allMatch(
                newCert -> currentRootCACerts.stream()
                    .anyMatch(currentCert -> currentCert.getSerialNumber().equals(newCert.getSerialNumber()))
            );
  }

  private void insertCertsToKeystore(Iterable<X509Certificate> certs,
      KeyStore ks) throws KeyStoreException {
    LOG.info("Trust manager is loaded with certificates");
    for (X509Certificate certToInsert : certs) {
      String certId = certToInsert.getSerialNumber().toString();
      ks.setCertificateEntry(certId, certToInsert);
      LOG.info(certToInsert.toString());
    }
  }

  @Override
  public synchronized void notifyCertificateRenewed(
      CertificateClient certClient, String oldCertId, String newCertId) {
    LOG.info("{} notify certificate renewed", certClient.getComponentName());
    Set<X509Certificate> certList = certClient.getAllRootCaCerts();
    Set<X509Certificate> rootCaCerts = certList.isEmpty() ? certClient.getAllCaCerts() : certList;
    try {
      X509TrustManager manager = init(new ArrayList<>(rootCaCerts));
      if (manager != null) {
        this.trustManagerRef.set(manager);
        LOG.info("ReloadingX509TrustManager is reloaded.");
      }
    } catch (Exception ex) {
      // The Consumer.accept interface forces us to convert to unchecked
      throw new RuntimeException(RELOAD_ERROR_MESSAGE, ex);
    }
  }
}
