/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
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
package org.apache.hadoop.hdds.security.x509.keys;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.ssl.KeyStoresFactory;
import org.apache.hadoop.hdds.security.ssl.PemFileBasedKeyStoresFactory;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;

/**
 * Utility functions for Security modules for Ozone.
 */
public final class SecurityUtil {

  private SecurityUtil() {
  }

  /**
   * Returns private key created from encoded key.
   *
   * @return private key if successful else returns null.
   */
  public static PrivateKey getPrivateKey(byte[] encodedKey,
      SecurityConfig secureConfig) {
    PrivateKey pvtKey = null;
    if (encodedKey == null || encodedKey.length == 0) {
      return null;
    }

    try {
      KeyFactory kf = null;

      kf = KeyFactory.getInstance(secureConfig.getKeyAlgo(),
          secureConfig.getProvider());
      pvtKey = kf.generatePrivate(new PKCS8EncodedKeySpec(encodedKey));

    } catch (NoSuchAlgorithmException | InvalidKeySpecException |
        NoSuchProviderException e) {
      return null;
    }
    return pvtKey;
  }

  /**
   * Returns public key created from encoded key.
   *
   * @return public key if successful else returns null.
   */
  public static PublicKey getPublicKey(byte[] encodedKey,
      SecurityConfig secureConfig) {
    PublicKey key = null;
    if (encodedKey == null || encodedKey.length == 0) {
      return null;
    }

    try {
      KeyFactory kf = null;
      kf = KeyFactory.getInstance(secureConfig.getKeyAlgo(),
          secureConfig.getProvider());
      key = kf.generatePublic(new X509EncodedKeySpec(encodedKey));

    } catch (NoSuchAlgorithmException | InvalidKeySpecException |
        NoSuchProviderException e) {
      return null;
    }
    return key;
  }

  public static KeyStoresFactory getServerKeyStoresFactory(
      SecurityConfig securityConfig, CertificateClient client,
      boolean requireClientAuth) throws CertificateException {
    PemFileBasedKeyStoresFactory factory =
        new PemFileBasedKeyStoresFactory(securityConfig, client);
    try {
      factory.init(KeyStoresFactory.Mode.SERVER, requireClientAuth);
    } catch (IOException | GeneralSecurityException e) {
      throw new CertificateException("Failed to init keyStoresFactory", e,
          CertificateException.ErrorCode.KEYSTORE_ERROR);
    }
    return factory;
  }

  public static KeyStoresFactory getClientKeyStoresFactory(
      SecurityConfig securityConfig, CertificateClient client,
      boolean requireClientAuth) throws CertificateException {
    PemFileBasedKeyStoresFactory factory =
        new PemFileBasedKeyStoresFactory(securityConfig, client);

    try {
      factory.init(KeyStoresFactory.Mode.CLIENT, requireClientAuth);
    } catch (IOException | GeneralSecurityException e) {
      throw new CertificateException("Failed to init keyStoresFactory", e,
          CertificateException.ErrorCode.KEYSTORE_ERROR);
    }
    return factory;
  }
}
