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
import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.LogicalResponse;
import org.apache.hadoop.ozone.om.S3Batcher;
import org.apache.hadoop.ozone.om.S3SecretStore;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.s3http.vault.auth.Auth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

/**
 * Based on HashiCorp Vault secret storage.
 * Documentation link {@code https://developer.hashicorp.com/vault}.
 */
public class S3RemoteSecretStore implements S3SecretStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(S3RemoteSecretStore.class);

  private final VaultConfig config;
  private Vault vault;
  private final String secretPath;
  private final boolean reAuth = true;
  private final Auth auth;

  public S3RemoteSecretStore(String vaultAddress,
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
      this.secretPath = secretPath;
    } catch (VaultException e) {
      throw new IOException("Failed to initialize remote secret store", e);
    }
  }

  private void auth() {
    try {
      vault = auth.auth(config);
    } catch (VaultException e) {
      LOG.error("Failed to s3 secret store auth", e);
    }
  }

  @Override
  public void storeSecret(String kerberosId, S3SecretValue secret)
      throws IOException {
    try {
      LogicalResponse s3Secret = vault.logical()
          .write(secretPath,
              Collections.singletonMap(kerberosId, secret.getAwsSecret()));
      System.out.println(s3Secret.getData());
    } catch (VaultException e) {
      throw new IOException("Failed to store secret", e);
    }
  }

  @Override
  public S3SecretValue getSecret(String kerberosID) throws IOException {
    try {
      String s3Secret = vault.logical()
          .read(secretPath).getData().get(kerberosID);
      return new S3SecretValue(kerberosID, s3Secret);
    } catch (VaultException e) {
      throw new IOException("Failed to read secret", e);
    }
  }

  @Override
  public void revokeSecret(String kerberosId) throws IOException {
    try {
      vault.logical().delete(secretPath);
    } catch (VaultException e) {
      throw new IOException("Failed to revoke secret", e);
    }
  }

  @Override
  //Not implemented.
  public S3Batcher batcher() {
    return null;
  }

  public static S3RemoteSecretStoreBuilder builder() {
    return new S3RemoteSecretStoreBuilder();
  }
}
