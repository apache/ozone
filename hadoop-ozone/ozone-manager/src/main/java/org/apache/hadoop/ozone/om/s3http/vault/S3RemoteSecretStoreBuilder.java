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
package org.apache.hadoop.ozone.om.s3http.vault;

import com.bettercloud.vault.SslConfig;
import org.apache.hadoop.ozone.om.s3http.vault.auth.Auth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

/**
 * Builder for {@link S3RemoteSecretStore}.
 */
public class S3RemoteSecretStoreBuilder {
  private static final Logger LOG =
      LoggerFactory.getLogger(S3RemoteSecretStoreBuilder.class);

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

  public S3RemoteSecretStoreBuilder setAddress(String address) {
    this.address = address;
    return this;
  }

  public S3RemoteSecretStoreBuilder setEngineVersion(int engineVersion) {
    this.engineVersion = engineVersion;
    return this;
  }

  public S3RemoteSecretStoreBuilder setKeyStoreType(String keyStoreType) {
    this.keyStoreType = keyStoreType;
    return this;
  }

  public S3RemoteSecretStoreBuilder setKeyStore(String keyStore) {
    this.keyStore = keyStore;
    return this;
  }

  public S3RemoteSecretStoreBuilder setKeyStorePassword(String ksPassword) {
    this.keyStorePassword = ksPassword;
    return this;
  }

  public S3RemoteSecretStoreBuilder setTrustStoreType(String trustStoreType) {
    this.trustStoreType = trustStoreType;
    return this;
  }

  public S3RemoteSecretStoreBuilder setTrustStore(String trustStore) {
    this.trustStore = trustStore;
    return this;
  }

  public S3RemoteSecretStoreBuilder setTrustStorePassword(String tsPassword) {
    this.trustStorePassword = tsPassword;
    return this;
  }

  public S3RemoteSecretStoreBuilder setAuth(Auth auth) {
    this.auth = auth;
    return this;
  }

  public S3RemoteSecretStoreBuilder setSecretPath(String secretPath) {
    this.secretPath = secretPath;
    return this;
  }


  public S3RemoteSecretStoreBuilder setNameSpace(String nameSpace) {
    this.nameSpace = nameSpace;
    return this;
  }

  public S3RemoteSecretStore build() throws IOException {
    SslConfig sslConfig = loadKeyStore(null);
    sslConfig = loadTrustStore(sslConfig);
    return new S3RemoteSecretStore(address, nameSpace,
        secretPath, engineVersion, auth, sslConfig);
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
        LOG.error("Failed to load keystore for S3 remove secret store", e);
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
        LOG.error("Failed to load keystore for S3 remove secret store", e);
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
