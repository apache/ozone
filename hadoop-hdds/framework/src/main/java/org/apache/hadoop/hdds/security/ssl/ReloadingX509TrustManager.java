/**
 * * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.security.ssl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link TrustManager} implementation that exposes a method,
 * {@link #loadFrom(CertificateClient)} to reload its configuration for
 * example when the truststore file on disk changes.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ReloadingX509TrustManager implements X509TrustManager {

  public static final Logger LOG =
      LoggerFactory.getLogger(ReloadingX509TrustManager.class);

  static final String RELOAD_ERROR_MESSAGE =
      "Could not reload truststore (keep using existing one) : ";

  private final String type;
  private final AtomicReference<X509TrustManager> trustManagerRef;
  /**
   * Current CA cert in trustManager, to detect if certificate is changed.
   */
  private String currentCACertId = null;

  /**
   * Creates a reloadable trustmanager. The trustmanager reloads itself
   * if the underlying truststore materials have changed.
   *
   * @param type type of truststore file, typically 'jks'.
   * @param caClient client to get trust certificates.
   * @throws IOException thrown if the truststore could not be initialized due
   * to an IO error.
   * @throws GeneralSecurityException thrown if the truststore could not be
   * initialized due to a security error.
   */
  public ReloadingX509TrustManager(String type, CertificateClient caClient)
      throws GeneralSecurityException, IOException {
    this.type = type;
    trustManagerRef = new AtomicReference<X509TrustManager>();
    trustManagerRef.set(loadTrustManager(caClient));
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      tm.checkClientTrusted(chain, authType);
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
      tm.checkServerTrusted(chain, authType);
    } else {
      throw new CertificateException("Unknown server chain certificate: " +
          chain[0].toString());
    }
  }

  private static final X509Certificate[] EMPTY = new X509Certificate[0];
  @Override
  public X509Certificate[] getAcceptedIssuers() {
    X509Certificate[] issuers = EMPTY;
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      issuers = tm.getAcceptedIssuers();
    }
    return issuers;
  }

  public ReloadingX509TrustManager loadFrom(CertificateClient caClient) {
    try {
      X509TrustManager manager = loadTrustManager(caClient);
      if (manager != null) {
        this.trustManagerRef.set(manager);
        LOG.info("ReloadingX509TrustManager is reloaded.");
      }
    } catch (Exception ex) {
      // The Consumer.accept interface forces us to convert to unchecked
      throw new RuntimeException(RELOAD_ERROR_MESSAGE, ex);
    }
    return this;
  }

  X509TrustManager loadTrustManager(CertificateClient caClient)
      throws GeneralSecurityException, IOException {
    X509Certificate cert = caClient.getCACertificate();
    String certId = cert.getSerialNumber().toString();
    // Certificate keeps the same.
    if (currentCACertId != null && currentCACertId.equals(certId)) {
      return null;
    }

    X509TrustManager trustManager = null;
    KeyStore ks = KeyStore.getInstance(type);
    ks.load(null, null);
    ks.setCertificateEntry(certId, cert);

    TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(SSLFactory.SSLCERTIFICATE);
    trustManagerFactory.init(ks);
    TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
    for (TrustManager trustManager1 : trustManagers) {
      if (trustManager1 instanceof X509TrustManager) {
        trustManager = (X509TrustManager) trustManager1;
        break;
      }
    }
    currentCACertId = certId;
    return trustManager;
  }
}
