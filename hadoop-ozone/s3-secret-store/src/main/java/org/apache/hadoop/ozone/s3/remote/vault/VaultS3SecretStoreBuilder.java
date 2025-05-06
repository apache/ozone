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

package org.apache.hadoop.ozone.s3.remote.vault;

import com.bettercloud.vault.SslConfig;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import org.apache.hadoop.ozone.s3.remote.vault.auth.Auth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder for {@link VaultS3SecretStore}.
 */
public class VaultS3SecretStoreBuilder {
  private static final Logger LOG =
      LoggerFactory.getLogger(VaultS3SecretStoreBuilder.class);

  private String address;
  private String nameSpace;
  private String secretPath;
  private int engineVersion = 1;
  private String keyStoreType;
  private String keyStore;
  private String keyStorePassword;
  private String trustStoreType;
  private String trustStore;
  private String trustStorePassword;
  private Auth auth;

  public VaultS3SecretStoreBuilder setAddress(String address) {
    this.address = address;
    return this;
  }

  public VaultS3SecretStoreBuilder setEngineVersion(int engineVersion) {
    this.engineVersion = engineVersion;
    return this;
  }

  public VaultS3SecretStoreBuilder setKeyStoreType(String keyStoreType) {
    this.keyStoreType = keyStoreType;
    return this;
  }

  public VaultS3SecretStoreBuilder setKeyStore(String keyStore) {
    this.keyStore = keyStore;
    return this;
  }

  public VaultS3SecretStoreBuilder setKeyStorePassword(String ksPassword) {
    this.keyStorePassword = ksPassword;
    return this;
  }

  public VaultS3SecretStoreBuilder setTrustStoreType(String trustStoreType) {
    this.trustStoreType = trustStoreType;
    return this;
  }

  public VaultS3SecretStoreBuilder setTrustStore(String trustStore) {
    this.trustStore = trustStore;
    return this;
  }

  public VaultS3SecretStoreBuilder setTrustStorePassword(String tsPassword) {
    this.trustStorePassword = tsPassword;
    return this;
  }

  public VaultS3SecretStoreBuilder setAuth(Auth auth) {
    this.auth = auth;
    return this;
  }

  public VaultS3SecretStoreBuilder setSecretPath(String secretPath) {
    this.secretPath = secretPath;
    return this;
  }

  public VaultS3SecretStoreBuilder setNameSpace(String nameSpace) {
    this.nameSpace = nameSpace;
    return this;
  }

  public VaultS3SecretStore build() throws IOException {
    SslConfig sslConfig = loadKeyStore(null);
    sslConfig = loadTrustStore(sslConfig);
    return new VaultS3SecretStore(
        address,
        nameSpace,
        secretPath,
        engineVersion,
        auth,
        sslConfig);
  }

  private SslConfig loadKeyStore(SslConfig sslConfig) {
    if (sslConfig == null) {
      sslConfig = new SslConfig();
    }
    if (keyStoreType != null) {
      try {
        KeyStore store = loadStore(keyStoreType,
            keyStore, keyStorePassword);
        return sslConfig.keyStore(store, keyStorePassword);
      } catch (CertificateException
          | KeyStoreException
          | IOException
          | NoSuchAlgorithmException e) {
        LOG.error("Failed to load keystore for S3 remote secret store", e);
      }
    }
    return null;
  }

  private SslConfig loadTrustStore(SslConfig sslConfig) {
    if (sslConfig == null) {
      sslConfig = new SslConfig();
    }
    if (trustStoreType != null) {
      try {
        KeyStore store = loadStore(trustStoreType,
            trustStore, trustStorePassword);
        return sslConfig.trustStore(store);
      } catch (CertificateException
          | KeyStoreException
          | IOException
          | NoSuchAlgorithmException e) {
        LOG.error("Failed to load keystore for S3 remote secret store", e);
      }
    }
    return null;
  }

  private static KeyStore loadStore(String storeType,
                                    String storePath,
                                    String password)
      throws KeyStoreException, IOException,
      CertificateException, NoSuchAlgorithmException {
    char[] pass = password == null ? null : password.toCharArray();

    KeyStore ks = KeyStore.getInstance(storeType);
    if (storePath != null) {
      try (InputStream is = Files.newInputStream(Paths.get(storePath))) {
        ks.load(is, pass);
      }
    }
    return ks;
  }
}
