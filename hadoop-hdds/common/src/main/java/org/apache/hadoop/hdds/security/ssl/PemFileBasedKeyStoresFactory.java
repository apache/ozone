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

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

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
public class PemFileBasedKeyStoresFactory implements KeyStoresFactory,
    CertificateNotification {

  private static final Logger LOG =
      LoggerFactory.getLogger(PemFileBasedKeyStoresFactory.class);

  /**
   * Default format of the keystore files.
   */
  public static final String DEFAULT_KEYSTORE_TYPE = "jks";

  private KeyManager[] keyManagers;
  private TrustManager[] trustManagers;
  private final CertificateClient caClient;

  public PemFileBasedKeyStoresFactory(SecurityConfig securityConfig,
      CertificateClient client) {
    this.caClient = client;
  }

  /**
   * Implements logic of initializing the TrustManagers with the options
   * to reload truststore.
   */
  private void createTrustManagers() throws
      GeneralSecurityException, IOException {
    ReloadingX509TrustManager trustManager = new ReloadingX509TrustManager(
        DEFAULT_KEYSTORE_TYPE, caClient);
    trustManagers = new TrustManager[] {trustManager};
  }

  /**
   * Implements logic of initializing the KeyManagers with the options
   * to reload keystores.
   */
  private void createKeyManagers() throws
      GeneralSecurityException, IOException {
    ReloadingX509KeyManager keystoreManager =
        new ReloadingX509KeyManager(DEFAULT_KEYSTORE_TYPE, caClient);
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

    // key manager
    if (requireClientAuth || mode == Mode.SERVER) {
      createKeyManagers();
    } else {
      KeyStore keystore = KeyStore.getInstance(DEFAULT_KEYSTORE_TYPE);
      keystore.load(null, null);
      KeyManagerFactory keyMgrFactory = KeyManagerFactory
          .getInstance(KeyManagerFactory.getDefaultAlgorithm());

      keyMgrFactory.init(keystore, null);
      keyManagers = keyMgrFactory.getKeyManagers();
    }

    // trust manager
    createTrustManagers();
    caClient.registerNotificationReceiver(this);
  }

  /**
   * Releases any resources being used.
   */
  @Override
  public synchronized void destroy() {
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
  @Override
  public synchronized KeyManager[] getKeyManagers() {
    KeyManager[] copy = new KeyManager[keyManagers.length];
    System.arraycopy(keyManagers, 0, copy, 0, keyManagers.length);
    return copy;
  }

  /**
   * Returns the trustmanagers for trusted certificates.
   */
  @Override
  public synchronized TrustManager[] getTrustManagers() {
    TrustManager[] copy = new TrustManager[trustManagers.length];
    System.arraycopy(trustManagers, 0, copy, 0, trustManagers.length);
    return copy;
  }

  @Override
  public synchronized void notifyCertificateRenewed(
      CertificateClient certClient, String oldCertId, String newCertId) {
    LOG.info("{} notify certificate renewed", certClient.getComponentName());
    if (keyManagers != null) {
      for (KeyManager km: keyManagers) {
        if (km instanceof ReloadingX509KeyManager) {
          ((ReloadingX509KeyManager) km).loadFrom(certClient);
        }
      }
    }

    if (trustManagers != null) {
      for (TrustManager tm: trustManagers) {
        if (tm instanceof ReloadingX509TrustManager) {
          ((ReloadingX509TrustManager) tm).loadFrom(certClient);
        }
      }
    }
  }
}
