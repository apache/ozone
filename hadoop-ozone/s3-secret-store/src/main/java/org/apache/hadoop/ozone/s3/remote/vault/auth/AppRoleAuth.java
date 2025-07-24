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

package org.apache.hadoop.ozone.s3.remote.vault.auth;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.AuthResponse;
import jakarta.annotation.Nullable;

/**
 * Authentication method via app role.
 *   https://developer.hashicorp.com/vault/docs/auth/approle
 */
public class AppRoleAuth implements Auth {
  private final String path;
  private final String roleId;
  private final String secretId;

  /**
   * Constructor.
   *
   * @param path Role authentication path. May be null.
   * @param roleId Role identifier.
   * @param secretId Role secret.
   */
  public AppRoleAuth(@Nullable String path, String roleId, String secretId) {
    this.path = path;
    this.roleId = roleId;
    this.secretId = secretId;
  }

  @Override
  public Vault auth(VaultConfig config) throws VaultException {
    Vault vault = new Vault(config);
    AuthResponse response = path != null
        ? vault.auth().loginByAppRole(path, roleId, secretId)
        : vault.auth().loginByAppRole(roleId, secretId);
    return new Vault(config.token(response.getAuthClientToken()));
  }
}
