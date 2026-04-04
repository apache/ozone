/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.chatbot.security;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Centralised utility for reading secrets from the Hadoop Credential
 * Provider (JCEKS). Every chatbot component that needs a secret
 * (API keys, encryption keys, etc.) should use this helper instead
 * of calling {@code configuration.getPassword()} directly.
 *
 * <p>
 * Resolution order:
 * </p>
 * <ol>
 * <li>JCEKS credential store (if configured via
 * {@code hadoop.security.credential.provider.path})</li>
 * <li>Plaintext value from {@code ozone-site.xml} (backward
 * compatibility fallback)</li>
 * </ol>
 */
@Singleton
public class CredentialHelper {

  private static final Logger LOG = LoggerFactory.getLogger(CredentialHelper.class);

  private final OzoneConfiguration configuration;

  @Inject
  public CredentialHelper(OzoneConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Reads a secret identified by {@code configKey} from the Hadoop
   * Credential Provider. Falls back to a plaintext read from
   * {@code ozone-site.xml} when no provider is configured or the key
   * is not present in the provider.
   *
   * @param configKey the Hadoop configuration key that names the secret
   * @return the secret value, or an empty string if not found anywhere
   */
  public String getSecret(String configKey) {
    // 1. Try the JCEKS credential provider first.
    try {
      char[] keyChars = configuration.getPassword(configKey);
      if (keyChars != null && keyChars.length > 0) {
        LOG.debug("Resolved '{}' from credential provider", configKey);
        return new String(keyChars);
      }
    } catch (IOException e) {
      LOG.warn("Failed to read '{}' from credential provider, "
          + "falling back to plaintext config", configKey, e);
    }

    // 2. Fallback: backward-compatible plaintext read.
    String plaintext = configuration.get(configKey, "");
    if (plaintext != null && !plaintext.isEmpty()) {
      LOG.debug("Resolved '{}' from plaintext configuration", configKey);
    }
    return plaintext;
  }

  /**
   * Checks whether a secret exists for the given config key (in
   * either JCEKS or plaintext config).
   *
   * @param configKey the configuration key to check
   * @return {@code true} if a non-empty secret is available
   */
  public boolean hasSecret(String configKey) {
    String value = getSecret(configKey);
    return value != null && !value.isEmpty();
  }
}
