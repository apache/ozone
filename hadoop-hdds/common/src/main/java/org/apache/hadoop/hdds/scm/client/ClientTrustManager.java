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

package org.apache.hadoop.hdds.scm.client;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import org.apache.hadoop.hdds.security.x509.certificate.client.CACertificateProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link javax.net.ssl.TrustManager} implementation for gRPC and Ratis
 * clients.
 *
 * This TrustManager instance is holding a reference to an externally supplied
 * TrustManager instance, and forwards all requests to that one.
 * This class is designed within the context of XceiverClientManager, where
 * we have the TrustManager initialized based on a ServiceInfo object, and
 * later on if the root of trust expires we need to refresh the rootCA
 * certificate for long-running clients to deal with a rootCA rotation if
 * necessary.
 *
 * The broader context where this class is usable is generally any place, where
 * clients are created based on a factory, and that factory can cache the
 * initial root of trust in this TrustManager, and a refresh mechanism as
 * necessary if the root of trust is expected to change for the certificate
 * of the server side.
 *
 * Note that in-memory provider is used to get the initial list of CA
 * certificates, that will be used to verify server side certificates.
 * In case of a certificate verification failure, the remote provider is used
 * to fetch a new list of CA certificates that will be used to verify server
 * side certificate from then on until the next failure.
 * Failures expected to happen only after a CA certificate that issued
 * the certificate of the servers is expired, or when the servers in preparation
 * for this expiration event renewed their certificates with a new CA.
 *
 * Important to note that this logic without additional efforts is weak against
 * a sophisticated attack, and should only be used with extra protection within
 * the remote provider. In Ozone's case the supplied remote provider is
 * an OzoneManager client, that verifies the identity of the server side
 * via Kerberos and expects the other side to be identified as an Ozone Manager.
 *
 * The checkClientTrusted methods throw Unsupported operation exceptions,
 * as this TrustManager instance is designed to be used only with client side
 * SSL channels.
 */
public class ClientTrustManager extends X509ExtendedTrustManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ClientTrustManager.class);

  private final CACertificateProvider remoteProvider;
  private X509ExtendedTrustManager trustManager;

  /**
   * Creates a ClientTrustManager instance based on an in-memory and a remote
   * trust anchor provider.
   *
   * The TrustManager first loads itself utilizing the in-memory provider to
   * provide a trust anchor (CA certificate) to be present in the trust store.
   *
   * Once the trust can not be established it uses the remote trust anchor
   * provider to refresh the locally known list of certificates.
   *
   * Any provider is allowed to be null, but not both.
   * If any of them is null, then the mechanism will not be used to get the
   * certificate list to be trusted.
   *
   * @param remoteProvider the provider to call once the root of trust has to
   *                       be renewed potentially as certificate verification
   *                       failed.
   * @param inMemoryProvider the initial provider of the trusted certificates.
   * @throws IOException in case an IO operation fails.
   */
  public ClientTrustManager(CACertificateProvider remoteProvider,
      CACertificateProvider inMemoryProvider)
      throws IOException {
    checkArgument(remoteProvider != null || inMemoryProvider != null,
        "Client trust configuration error, no mechanism present to find the" +
            " rootCA certificate of the cluster.");
    this.remoteProvider = remoteProvider;
    try {
      initialize(loadCerts(inMemoryProvider));
    } catch (CertificateException e) {
      throw new IOException(e);
    }
  }

  private void initialize(List<X509Certificate> caCerts)
      throws CertificateException {
    try {
      KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
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
      LOG.debug("Loading certificates for client.");
      if (caCertsProvider == null) {
        return remoteProvider.provideCACerts();
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
      initialize(loadCerts(remoteProvider));
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
      initialize(loadCerts(remoteProvider));
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
      initialize(loadCerts(remoteProvider));
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
