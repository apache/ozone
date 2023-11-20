/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.hdds.scm.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;

/**
 * tbd.
 */
public class ClientTrustManager extends X509ExtendedTrustManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ClientTrustManager.class);

  private final CACertificateProvider client;
  private X509ExtendedTrustManager trustManager;

  /**
   * tbd.
   */
  @FunctionalInterface
  public interface CACertificateProvider {
    List<X509Certificate> provideCACerts() throws IOException;
  }

  public ClientTrustManager(CACertificateProvider remoteProvider,
      CACertificateProvider inMemoryProvider)
      throws IOException {
    this.client = remoteProvider;
    try {
      initialize(loadCerts(inMemoryProvider));
    } catch (CertificateException e) {
      throw new IOException(e);
    }
  }

  private void initialize(List<X509Certificate> caCerts)
      throws CertificateException {
    try {
      KeyStore ks = KeyStore.getInstance("jks");
      ks.load(null);

      for (X509Certificate cert : caCerts) {
        String serial = cert.getSerialNumber().toString();
        ks.setCertificateEntry(serial, cert);
      }

      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
          TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(ks);
      trustManager = Arrays.stream(trustManagerFactory.getTrustManagers())
          .filter(tm -> tm instanceof X509ExtendedTrustManager)
          .map(tm -> (X509ExtendedTrustManager) tm)
          .findFirst()
          .orElse(null);
      if (trustManager == null) {
        throw new GeneralSecurityException("Could not load TrustManager.");
      }
    } catch (GeneralSecurityException | IOException e) {
      throw new CertificateException(e);
    }
  }

  private List<X509Certificate> loadCerts(CACertificateProvider caCertsProvider)
      throws CertificateException {
    try {
      LOG.info("Loading certificates for client.");
      if (caCertsProvider == null) {
        return client.provideCACerts();
      }
      return caCertsProvider.provideCACerts();
    } catch (IOException e) {
      throw new CertificateException(e);
    }
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType,
      Socket socket) throws CertificateException {
    try {
      trustManager.checkServerTrusted(chain, authType, socket);
    } catch (CertificateException e) {
      LOG.info("CheckServerTrusted call failed, trying to re-fetch " +
          "rootCA certificate", e);
      initialize(loadCerts(null));
      trustManager.checkServerTrusted(chain, authType, socket);
    }
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType,
      SSLEngine engine) throws CertificateException {
    try {
      trustManager.checkServerTrusted(chain, authType, engine);
    } catch (CertificateException e) {
      LOG.info("CheckServerTrusted call failed, trying to re-fetch " +
          "rootCA certificate", e);
      initialize(loadCerts(null));
      trustManager.checkServerTrusted(chain, authType, engine);
    }
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    try {
      trustManager.checkServerTrusted(chain, authType);
    } catch (CertificateException e) {
      LOG.info("CheckServerTrusted call failed, trying to re-fetch " +
          "rootCA certificate", e);
      initialize(loadCerts(null));
      trustManager.checkServerTrusted(chain, authType);
    }
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return trustManager.getAcceptedIssuers();
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType,
      Socket socket) throws CertificateException {
    throw new CertificateException(
        new UnsupportedOperationException("ClientTrustManager should not" +
            " be used as a trust manager of a server socket."));
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType,
      SSLEngine engine) throws CertificateException {
    throw new CertificateException(
        new UnsupportedOperationException("ClientTrustManager should not" +
            " be used as a trust manager of a server socket."));
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    throw new CertificateException(
        new UnsupportedOperationException("ClientTrustManager should not" +
            " be used as a trust manager of a server socket."));
  }
}
