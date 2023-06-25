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
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

  private final String type;
  /**
   * Default password. KeyStore and trustStore will not persist to disk, just in
   * memory.
   */
  static final char[] EMPTY_PASSWORD = new char[0];
  private final AtomicReference<X509ExtendedKeyManager> keyManagerRef;
  // Keep the old key managers, for currently we find that the netty
  // tc-native component always query the first root certificate through
  // chooseEngineClientAlias after the key manager is reloaded with a new one.
  private final List<X509ExtendedKeyManager> oldKeyManagerRef;

  /**
   * Current private key and cert used in keyManager. Used to detect if these
   * materials are changed.
   */
  private PrivateKey currentPrivateKey;
  private List<String> currentCertIdsList = new ArrayList<>();
  private final Pattern dnPattern =
      Pattern.compile("^.+, CN=scm-\\d{1}@.+");
  private final String alias = "scm/sub-ca_key";

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
    oldKeyManagerRef = new ArrayList<>();
  }

  @Override
  public String chooseEngineClientAlias(String[] strings,
      Principal[] principals, SSLEngine sslEngine) {
    String ret = keyManagerRef.get()
        .chooseEngineClientAlias(strings, principals, sslEngine);
    if (ret == null && oldKeyManagerRef.size() != 0) {
      for (X509ExtendedKeyManager manager: oldKeyManagerRef) {
        ret = manager.chooseEngineClientAlias(strings, principals, sslEngine);
        if (ret != null) {
          break;
        }
      }
    }
    if (ret == null) {
      LOG.info("Engine client aliases for {}, {}, {} is null",
          strings == null ? "" : Arrays.stream(strings).map(Object::toString)
              .collect(Collectors.joining(", ")),
          principals == null ? "" : Arrays.stream(principals)
              .map(Object::toString).collect(Collectors.joining(", ")),
          sslEngine == null ? "" : sslEngine.toString());
        /*
        Workaround to address that netty tc-native doesn't handle the dynamic
        KeyManager re-loading well.

        TODO: If this issue is fixed in tc-native in future version, following
        logic can be removed.

        Example error message:
        Engine client aliases for RSA, DH_RSA, EC, EC_RSA, EC_EC,
        O=CID-f9f2b2cf-a784-49d7-8577-5d3b13bf0b46,
        OU=9f52487c-f8f9-45ee-bb56-aca60b56327f,
        CN=scm-1@scm1.org,
        org.apache.ratis.thirdparty.io.netty.handler.ssl.OpenSslEngine@5eec0d10
        is null

        Example success message:
        Engine client aliases for RSA, DH_RSA, EC, EC_RSA, EC_EC,
        O=CID-f9f2b2cf-a784-49d7-8577-5d3b13bf0b46,
        OU=9f52487c-f8f9-45ee-bb56-aca60b56327f,
        CN=scm-1@scm1.org,
        org.apache.ratis.thirdparty.io.netty.handler.ssl.OpenSslEngine@5eec0d10
        is scm/sub-ca_key
       */
      if (principals != null) {
        String dn = principals[0].toString();
        Matcher matcher = dnPattern.matcher(dn);
        if (matcher.matches()) {
          ret = alias;
        }
        LOG.warn("Engine client aliases for {}, {}, {} is changed to {}",
            strings == null ? "" : Arrays.stream(strings).map(Object::toString)
                .collect(Collectors.joining(",")),
            Arrays.stream(principals)
                .map(Object::toString).collect(Collectors.joining(",")),
            sslEngine == null ? "" : sslEngine.toString(), ret);
      }
    }
    return ret;
  }

  @Override
  public String chooseEngineServerAlias(String s, Principal[] principals,
      SSLEngine sslEngine) {
    String ret = keyManagerRef.get()
        .chooseEngineServerAlias(s, principals, sslEngine);
    if (ret == null && oldKeyManagerRef.size() != 0) {
      for (X509ExtendedKeyManager manager: oldKeyManagerRef) {
        ret = manager.chooseEngineServerAlias(s, principals, sslEngine);
        if (ret != null) {
          break;
        }
      }
    }
    if (ret == null && LOG.isDebugEnabled()) {
      LOG.debug("Engine server aliases for {}, {}, {} is null", s,
          principals == null ? "" : Arrays.stream(principals)
              .map(Object::toString).collect(Collectors.joining(", ")),
          sslEngine == null ? "" : sslEngine.toString());
    }
    return ret;
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
        oldKeyManagerRef.add(keyManagerRef.get());
        keyManagerRef.set(manager);
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
    List<X509Certificate> newCertList = caClient.getTrustChain();
    if (currentPrivateKey != null && currentPrivateKey.equals(privateKey) &&
        currentCertIdsList.size() > 0 &&
        newCertList.size() == currentCertIdsList.size() &&
        !newCertList.stream().filter(
            c -> !currentCertIdsList.contains(c.getSerialNumber().toString()))
            .findAny().isPresent()) {
      // Security materials(key and certificates) keep the same.
      return null;
    }

    X509ExtendedKeyManager keyManager = null;
    KeyStore keystore = KeyStore.getInstance(type);
    keystore.load(null, null);

    keystore.setKeyEntry(caClient.getComponentName() + "_key",
        privateKey, EMPTY_PASSWORD,
        newCertList.toArray(new X509Certificate[0]));

    LOG.info("New key manager is loaded with certificate chain");
    for (int i = 0; i < newCertList.size(); i++) {
      LOG.info(newCertList.get(i).toString());
    }

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
    currentCertIdsList.clear();
    for (X509Certificate cert: newCertList) {
      currentCertIdsList.add(cert.getSerialNumber().toString());
    }
    return keyManager;
  }
}
