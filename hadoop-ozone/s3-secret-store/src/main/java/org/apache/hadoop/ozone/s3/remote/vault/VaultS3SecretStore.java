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
package org.apache.hadoop.ozone.s3.remote.vault;

import com.bettercloud.vault.SslConfig;
import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.LookupResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.om.S3Batcher;
import org.apache.hadoop.ozone.om.S3SecretStore;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.s3.remote.vault.auth.Auth;
import org.apache.hadoop.ozone.s3.remote.vault.auth.AuthType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.apache.hadoop.ozone.s3.remote.S3SecretRemoteStoreConfigurationKeys.ADDRESS;
import static org.apache.hadoop.ozone.s3.remote.S3SecretRemoteStoreConfigurationKeys.ENGINE_VER;
import static org.apache.hadoop.ozone.s3.remote.S3SecretRemoteStoreConfigurationKeys.KEY_STORE_PASSWORD;
import static org.apache.hadoop.ozone.s3.remote.S3SecretRemoteStoreConfigurationKeys.KEY_STORE_PATH;
import static org.apache.hadoop.ozone.s3.remote.S3SecretRemoteStoreConfigurationKeys.KEY_STORE_TYPE;
import static org.apache.hadoop.ozone.s3.remote.S3SecretRemoteStoreConfigurationKeys.NAMESPACE;
import static org.apache.hadoop.ozone.s3.remote.S3SecretRemoteStoreConfigurationKeys.SECRET_PATH;
import static org.apache.hadoop.ozone.s3.remote.S3SecretRemoteStoreConfigurationKeys.TRUST_STORE_PASSWORD;
import static org.apache.hadoop.ozone.s3.remote.S3SecretRemoteStoreConfigurationKeys.TRUST_STORE_PATH;
import static org.apache.hadoop.ozone.s3.remote.S3SecretRemoteStoreConfigurationKeys.TRUST_STORE_TYPE;

/**
 * Based on HashiCorp Vault secret storage.
 * Documentation link {@code https://developer.hashicorp.com/vault}.
 */
public class VaultS3SecretStore implements S3SecretStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(VaultS3SecretStore.class);

  private final VaultConfig config;
  private Vault vault;
  private final String secretPath;
  private final Auth auth;

  public VaultS3SecretStore(String vaultAddress,
                            String nameSpace,
                            String secretPath,
                            int engineVersion,
                            Auth auth,
                            SslConfig sslConfig) throws IOException {
    try {
      config = new VaultConfig()
          .address(vaultAddress)
          .engineVersion(engineVersion)
          .nameSpace(nameSpace)
          .sslConfig(sslConfig)
          .build();

      this.auth = auth;
      vault = auth.auth(config);
      this.secretPath = secretPath.endsWith("/")
          ? secretPath.substring(0, secretPath.length() - 1)
          : secretPath;
    } catch (VaultException e) {
      throw new IOException("Failed to initialize remote secret store", e);
    }
  }

  private void auth() throws VaultException {
    vault = auth.auth(config);
  }

  @Override
  public void storeSecret(String kerberosId, S3SecretValue secret)
      throws IOException {
    try {
      checkAuth();
      vault.logical().write(secretPath + '/' + kerberosId,
              Collections.singletonMap(kerberosId, secret.getAwsSecret()));
    } catch (VaultException e) {
      LOG.error("Failed to store secret", e);
      throw new IOException("Failed to store secret", e);
    }
  }

  @Override
  public S3SecretValue getSecret(String kerberosID) throws IOException {
    try {
      checkAuth();

      Map<String, String> data = vault.logical()
              .read(secretPath + '/' + kerberosID).getData();

      if (data == null) {
        return null;
      }

      String s3Secret = data.get(kerberosID);
      if (s3Secret == null) {
        return null;
      }

      return new S3SecretValue(kerberosID, s3Secret);
    } catch (VaultException e) {
      LOG.error("Failed to read secret", e);
      throw new IOException("Failed to read secret", e);
    }
  }

  @Override
  public void revokeSecret(String kerberosId) throws IOException {
    try {
      checkAuth();
      vault.logical().delete(secretPath + '/' + kerberosId);
    } catch (VaultException e) {
      LOG.error("Failed to delete secret", e);
      throw new IOException("Failed to revoke secret", e);
    }
  }

  private void checkAuth() throws VaultException {
    try {
      doCheck();
    } catch (VaultException e) {
      processEx(e, this::auth);
      try {
        doCheck();
      } catch (VaultException ex) {
        processEx(ex, () -> {
          throw new VaultException("Failed to re-authenticate",
              ex.getHttpStatusCode());
        });
      }
    }
  }

  private void doCheck() throws VaultException {
    LookupResponse lookupResponse = vault.auth().lookupSelf();

    if (!Objects.equals(lookupResponse.getId(), config.getToken())) {
      LOG.error("Lookup token is not the same as Vault client configuration.");
      throw new VaultException("Failed to re-authenticate", 401);
    }
  }

  private void processEx(VaultException e, Call callback)
      throws VaultException {
    int status = e.getHttpStatusCode();
    if (status == 403 || status == 401) {
      callback.run();
    }
  }

  private interface Call {
    void run() throws VaultException;
  }

  @Override
  //Not implemented.
  public S3Batcher batcher() {
    return null;
  }

  public static VaultS3SecretStore fromConf(Configuration conf)
      throws IOException {
    VaultS3SecretStoreBuilder builder = VaultS3SecretStore.builder()
        .setAuth(AuthType.fromConf(conf))
        .setAddress(conf.get(ADDRESS))
        .setNameSpace(conf.get(NAMESPACE))
        .setSecretPath(conf.get(SECRET_PATH))
        .setEngineVersion(conf.getInt(ENGINE_VER, 1));

    String trustStoreType = conf.get(TRUST_STORE_TYPE);
    if (trustStoreType != null) {
      builder.setTrustStoreType(trustStoreType)
          .setTrustStore(conf.get(TRUST_STORE_PATH))
          .setTrustStorePassword(conf.get(TRUST_STORE_PASSWORD));
    }

    String keyStoreType = conf.get(KEY_STORE_TYPE);
    if (keyStoreType != null) {
      builder.setKeyStoreType(keyStoreType)
          .setKeyStore(conf.get(KEY_STORE_PATH))
          .setKeyStorePassword(conf.get(KEY_STORE_PASSWORD));
    }
    return builder.build();
  }

  public static VaultS3SecretStoreBuilder builder() {
    return new VaultS3SecretStoreBuilder();
  }
}
