/**
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
package org.apache.hadoop.hdds.security.ssl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Timer;

/**
 * {@link KeyStoresFactory} implementation that reads the certificates from
 * private key pem file and certificate pem file.
 * <p>
 * If either the truststore or the keystore certificates file changes, it
 * would be refreshed under the corresponding wrapper implementation -
 * {@link ReloadingX509KeyManager} or {@link ReloadingX509TrustManager}.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class PemFileBasedKeyStoresFactory implements KeyStoresFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(PemFileBasedKeyStoresFactory.class);

  /**
   * The name of the timer thread monitoring file changes.
   */
  public static final String SSL_MONITORING_THREAD_NAME =
      "SSL Certificates Store Monitor";

  /**
   * Default format of the keystore files.
   */
  public static final String DEFAULT_KEYSTORE_TYPE = "jks";

  private KeyManager[] keyManagers;
  private TrustManager[] trustManagers;
  private Timer monitoringTimer;
  private final CertificateClient caClient;
  private final SecurityConfig secConfig;

  public PemFileBasedKeyStoresFactory(SecurityConfig securityConfig,
      CertificateClient client) {
    this.caClient = client;
    this.secConfig = securityConfig;
  }

  /**
   * Implements logic of initializing the TrustManagers with the options
   * to reload truststore.
   * @param mode client or server
   */
  private void createTrustManagers(Mode mode) throws
      GeneralSecurityException, IOException {
    long truststoreReloadInterval = secConfig.getSslTruststoreReloadInterval();
    LOG.info(mode.toString() + " TrustStore reloading at " +
        truststoreReloadInterval + " millis.");

    ReloadingX509TrustManager trustManager = new ReloadingX509TrustManager(
        DEFAULT_KEYSTORE_TYPE, caClient);

    if (truststoreReloadInterval > 0) {
      monitoringTimer.schedule(
          new MonitoringTimerTask(caClient,
              p -> trustManager.loadFrom(caClient),
              exception -> LOG.error(
                  ReloadingX509TrustManager.RELOAD_ERROR_MESSAGE, exception)),
          truststoreReloadInterval,
          truststoreReloadInterval);
    }

    trustManagers = new TrustManager[] {trustManager};
  }

  /**
   * Implements logic of initializing the KeyManagers with the options
   * to reload keystores.
   * @param mode client or server
   */
  private void createKeyManagers(Mode mode) throws
      GeneralSecurityException, IOException {
    long keystoreReloadInterval = secConfig.getSslKeystoreReloadInterval();
    LOG.info(mode.toString() + " KeyStore reloading at " +
        keystoreReloadInterval + " millis.");

    ReloadingX509KeyManager keystoreManager =
        new ReloadingX509KeyManager(DEFAULT_KEYSTORE_TYPE, caClient);

    if (keystoreReloadInterval > 0) {
      monitoringTimer.schedule(
          new MonitoringTimerTask(caClient,
              p -> keystoreManager.loadFrom(caClient),
              exception ->
                  LOG.error(ReloadingX509KeyManager.RELOAD_ERROR_MESSAGE,
                      exception)),
          keystoreReloadInterval,
          keystoreReloadInterval);
    }

    keyManagers = new KeyManager[] {keystoreManager};
  }

  /**
   * Initializes the keystores of the factory.
   *
   * @param mode if the keystores are to be used in client or server mode.
   * @param requireClientAuth whether client authentication is required. Ignore
   *                         for client mode.
   * @throws IOException thrown if the keystores could not be initialized due
   * to an IO error.
   * @throws GeneralSecurityException thrown if the keystores could not be
   * initialized due to a security error.
   */
  public synchronized void init(Mode mode, boolean requireClientAuth)
      throws IOException, GeneralSecurityException {

    monitoringTimer = new Timer(caClient.getComponentName() + "-" + mode + "-"
        + SSL_MONITORING_THREAD_NAME, true);

    // key manager
    if (requireClientAuth || mode == Mode.SERVER) {
      createKeyManagers(mode);
    } else {
      KeyStore keystore = KeyStore.getInstance(DEFAULT_KEYSTORE_TYPE);
      keystore.load(null, null);
      KeyManagerFactory keyMgrFactory = KeyManagerFactory
          .getInstance(KeyManagerFactory.getDefaultAlgorithm());

      keyMgrFactory.init(keystore, null);
      keyManagers = keyMgrFactory.getKeyManagers();
    }

    // trust manager
    createTrustManagers(mode);
  }

  /**
   * Releases any resources being used.
   */
  @Override
  public synchronized void destroy() {
    if (monitoringTimer != null) {
      monitoringTimer.cancel();
    }

    if (keyManagers != null) {
      keyManagers = null;
    }

    if (trustManagers != null) {
      trustManagers = null;
    }
  }

  /**
   * Returns the keymanagers for owned certificates.
   */
  @SuppressFBWarnings("EI_EXPOSE_REP")
  @Override
  public synchronized KeyManager[] getKeyManagers() {
    return keyManagers;
  }

  /**
   * Returns the trustmanagers for trusted certificates.
   */
  @SuppressFBWarnings("EI_EXPOSE_REP")
  @Override
  public synchronized TrustManager[] getTrustManagers() {
    return trustManagers;
  }
}
