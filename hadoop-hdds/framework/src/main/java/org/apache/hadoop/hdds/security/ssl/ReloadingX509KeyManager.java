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
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An implementation of <code>X509KeyManager</code> that exposes a method,
 * {@link #loadFrom(CertificateClient)} to reload its configuration.
 * Note that it is necessary to implement the
 * <code>X509ExtendedKeyManager</code> to properly delegate
 * the additional methods, otherwise the SSL handshake will fail.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReloadingX509KeyManager extends X509ExtendedKeyManager {

  public static final Logger LOG =
      LoggerFactory.getLogger(ReloadingX509KeyManager.class);

  static final String RELOAD_ERROR_MESSAGE =
      "Could not reload keystore (keep using existing one) : ";

  private final String type;
  /**
   * Default password. KeyStore and trustStore will not persist to disk, just in
   * memory.
   */
  static final char[] EMPTY_PASSWORD = new char[0];
  private final AtomicReference<X509ExtendedKeyManager> keyManagerRef;
  /**
   * Current private key and cert used in keyManager. Used to detect if these
   * materials are changed.
   */
  private PrivateKey currentPrivateKey;
  private String currentCertId;

  /**
   * Construct a <code>Reloading509KeystoreManager</code>.
   *
   * @param type type of keystore file, typically 'jks'.
   * @param caClient client to get the private key and certificate materials.
   * @throws IOException
   * @throws GeneralSecurityException
   */
  public ReloadingX509KeyManager(String type, CertificateClient caClient)
      throws GeneralSecurityException, IOException {
    this.type = type;
    keyManagerRef = new AtomicReference<>();
    keyManagerRef.set(loadKeyManager(caClient));
  }

  @Override
  public String chooseEngineClientAlias(String[] strings,
      Principal[] principals, SSLEngine sslEngine) {
    return keyManagerRef.get()
        .chooseEngineClientAlias(strings, principals, sslEngine);
  }

  @Override
  public String chooseEngineServerAlias(String s, Principal[] principals,
      SSLEngine sslEngine) {
    return keyManagerRef.get()
        .chooseEngineServerAlias(s, principals, sslEngine);
  }

  @Override
  public String[] getClientAliases(String s, Principal[] principals) {
    return keyManagerRef.get().getClientAliases(s, principals);
  }

  @Override
  public String chooseClientAlias(String[] strings, Principal[] principals,
      Socket socket) {
    return keyManagerRef.get().chooseClientAlias(strings, principals, socket);
  }

  @Override
  public String[] getServerAliases(String s, Principal[] principals) {
    return keyManagerRef.get().getServerAliases(s, principals);
  }

  @Override
  public String chooseServerAlias(String s, Principal[] principals,
      Socket socket) {
    return keyManagerRef.get().chooseServerAlias(s, principals, socket);
  }

  @Override
  public X509Certificate[] getCertificateChain(String s) {
    // see https://bugs.openjdk.org/browse/JDK-4891485
    // the KeyManager stores the chain in a case-insensitive way making the
    // alias lowercase upon initialization.
    return keyManagerRef.get().getCertificateChain(s.toLowerCase(Locale.ROOT));
  }

  @Override
  public PrivateKey getPrivateKey(String s) {
    // see: https://bugs.openjdk.org/browse/JDK-4891485
    // the KeyManager stores the chain in a case-insensitive way making the
    // alias lowercase upon initialization.
    return keyManagerRef.get().getPrivateKey(s.toLowerCase(Locale.ROOT));
  }

  public ReloadingX509KeyManager loadFrom(CertificateClient caClient) {
    try {
      X509ExtendedKeyManager manager = loadKeyManager(caClient);
      if (manager != null) {
        this.keyManagerRef.set(manager);
        LOG.info("ReloadingX509KeyManager is reloaded");
      }
    } catch (Exception ex) {
      // The Consumer.accept interface forces us to convert to unchecked
      throw new RuntimeException(ex);
    }
    return this;
  }

  private X509ExtendedKeyManager loadKeyManager(CertificateClient caClient)
      throws GeneralSecurityException, IOException {
    PrivateKey privateKey = caClient.getPrivateKey();
    X509Certificate cert = caClient.getCertificate();
    String certId = cert.getSerialNumber().toString();
    // Security materials keep the same
    if (currentCertId != null && currentPrivateKey != null &&
        currentCertId.equals(certId) && currentPrivateKey.equals(privateKey)) {
      return null;
    }

    X509ExtendedKeyManager keyManager = null;
    KeyStore keystore = KeyStore.getInstance(type);
    keystore.load(null, null);

    keystore.setKeyEntry(caClient.getComponentName() + "_key",
        privateKey, EMPTY_PASSWORD, new Certificate[]{cert});

    KeyManagerFactory keyMgrFactory = KeyManagerFactory.getInstance(
        KeyManagerFactory.getDefaultAlgorithm());
    keyMgrFactory.init(keystore, EMPTY_PASSWORD);
    for (KeyManager candidate: keyMgrFactory.getKeyManagers()) {
      if (candidate instanceof X509ExtendedKeyManager) {
        keyManager = (X509ExtendedKeyManager)candidate;
        break;
      }
    }

    currentPrivateKey = privateKey;
    currentCertId = cert.getSerialNumber().toString();
    return keyManager;
  }
}
