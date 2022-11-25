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
package org.apache.hadoop.hdds.security.token;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.security.OzoneSecretManager;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;

import java.time.Instant;

/**
 * Base class for short-lived token secret managers (block, container).
 * @param <T> token identifier type
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class
    ShortLivedTokenSecretManager<T extends ShortLivedTokenIdentifier>
    extends OzoneSecretManager<T> {

  private static final Text SERVICE = new Text("HDDS_SERVICE");

  private final String certSerialId;

  protected ShortLivedTokenSecretManager(SecurityConfig conf,
      long tokenLifetime, String certSerialId, Logger logger) {
    super(conf, tokenLifetime, tokenLifetime, SERVICE, logger);
    this.certSerialId = certSerialId;
  }

  @Override
  public T createIdentifier() {
    throw new SecurityException("Short-lived token requires additional " +
        "information (owner, etc.).");
  }

  @Override
  public long renewToken(Token<T> token, String renewer) {
    throw new UnsupportedOperationException("Renew token operation is not " +
        "supported for short-lived tokens.");
  }

  @Override
  public T cancelToken(Token<T> token, String canceller) {
    throw new UnsupportedOperationException("Cancel token operation is not " +
        "supported for short-lived tokens.");
  }

  @Override
  public byte[] retrievePassword(T identifier) throws InvalidToken {
    validateToken(identifier);
    return createPassword(identifier);
  }

  /**
   * Find the OzoneBlockTokenInfo for the given token id, and verify that if the
   * token is not expired.
   */
  protected boolean validateToken(T identifier) throws InvalidToken {
    Instant now = Instant.now();
    if (identifier.isExpired(now)) {
      throw new InvalidToken("token " + formatTokenId(identifier) + " is " +
          "expired, current time: " + now +
          " expiry time: " + identifier.getExpiry());
    }

    return true;
  }

  /**
   * Returns expiry time by adding configured expiry time with current time.
   *
   * @return Expiry time.
   */
  protected Instant getTokenExpiryTime() {
    return Instant.now().plusMillis(getTokenMaxLifetime());
  }

  protected String getCertSerialId() {
    return certSerialId;
  }

  public Token<T> generateToken(T tokenIdentifier) {
    return new Token<>(tokenIdentifier.getBytes(),
        createPassword(tokenIdentifier), tokenIdentifier.getKind(),
        new Text(tokenIdentifier.getService()));
  }
}
