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

package org.apache.hadoop.hdds.security.token;

import java.time.Instant;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;

/**
 * Base class for short-lived token secret managers (block, container).
 * @param <T> token identifier type
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class ShortLivedTokenSecretManager
    <T extends ShortLivedTokenIdentifier> {
  private final long tokenMaxLifetime;
  private SecretKeySignerClient secretKeyClient;

  protected ShortLivedTokenSecretManager(
      long tokenLifetime, SecretKeySignerClient secretKeyClient) {
    this.tokenMaxLifetime = tokenLifetime;
    this.secretKeyClient = secretKeyClient;
  }

  protected byte[] createPassword(T tokenId) {
    ManagedSecretKey secretKey = secretKeyClient.getCurrentSecretKey();
    tokenId.setSecretKeyId(secretKey.getId());
    return secretKey.sign(tokenId);
  }

  /**
   * Returns expiry time by adding configured expiry time with current time.
   *
   * @return Expiry time.
   */
  protected Instant getTokenExpiryTime() {
    return Instant.now().plusMillis(tokenMaxLifetime);
  }

  public Token<T> generateToken(T tokenIdentifier) {
    byte[] password = createPassword(tokenIdentifier);
    return new Token<>(tokenIdentifier.getBytes(),
        password, tokenIdentifier.getKind(),
        new Text(tokenIdentifier.getService()));
  }

  /**
   * Allows integration-test to inject a custom implementation of
   * SecretKeyClient to test without fully setting up a working secure cluster.
   */
  public void setSecretKeyClient(SecretKeySignerClient client) {
    this.secretKeyClient = client;
  }
}
