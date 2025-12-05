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

package org.apache.hadoop.ozone.security;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.hdds.security.token.ShortLivedTokenSecretManager;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;

/**
 * Secret manager for STS (Security Token Service) tokens.
 * This class extends ShortLivedTokenSecretManager to make use of functionality such as signing tokens, etc.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class STSTokenSecretManager extends ShortLivedTokenSecretManager<STSTokenIdentifier> {

  private static final long TOKEN_MAX_LIFETIME = 43200 * 1000L; // 12 hours in milliseconds

  // Store reference to secret key client for encryption key access
  private final SecretKeySignerClient secretKeyClient;

  /**
   * Create a new STS token secret manager.
   *
   * @param secretKeyClient client for accessing secret keys from SCM
   */
  public STSTokenSecretManager(SecretKeySignerClient secretKeyClient) {
    super(TOKEN_MAX_LIFETIME, secretKeyClient);
    this.secretKeyClient = secretKeyClient;
  }

  /**
   * Override token generation so that we first compute the identifier bytes, then sign exactly those bytes, and
   * return a Token that contains those same identifier bytes. This avoids non-determinism from multiple serializations
   * which would break unit tests.  If we used the inherited generateToken() in ShortLivedTokenSecretManager, it
   * would have made two serialization calls:
   *   1) in the call to secretKey.sign() in the createPassword() method
   *   2) in the call to tokenIdentifier.getBytes() for the Token constructor
   * These two calls would produce different secretAccessKey encrypted values because of the random initialization
   * vector and random salt and hence give non-deterministic return value, so here we are only serializing once.
   */
  @Override
  public Token<STSTokenIdentifier> generateToken(STSTokenIdentifier tokenIdentifier) {
    final ManagedSecretKey secretKey = secretKeyClient.getCurrentSecretKey();
    tokenIdentifier.setSecretKeyId(secretKey.getId());
    final byte[] identifierBytes = tokenIdentifier.getBytes();
    final byte[] password = secretKey.sign(identifierBytes);
    return new Token<>(identifierBytes, password, tokenIdentifier.getKind(), new Text(tokenIdentifier.getService()));
  }

  /**
   * Create an STS token and return it as an encoded string.
   *
   * @param tempAccessKeyId     the temporary access key ID
   * @param originalAccessKeyId the original long-lived access key ID
   * @param roleArn             the ARN of the assumed role
   * @param durationSeconds     how long the token should be valid for
   * @param secretAccessKey     the secret access key associated with the temporary access key ID
   * @param sessionPolicy       an optional opaque identifier that further limits the scope of
   *                            the permissions granted by the role
   * @param clock               the system clock
   * @return base64 encoded token string
   */
  public String createSTSTokenString(String tempAccessKeyId, String originalAccessKeyId, String roleArn,
      int durationSeconds, String secretAccessKey, String sessionPolicy, Clock clock) throws IOException {
    final Instant expiration = clock.instant().plusSeconds(durationSeconds);

    // Get the current secret key for encryption
    final ManagedSecretKey currentSecretKey = secretKeyClient.getCurrentSecretKey();
    final byte[] encryptionKey = currentSecretKey.getSecretKey().getEncoded();

    // Note - the encryptionKey will NOT be encoded in the token.  When generateToken() is called, it eventually calls
    // the write() method in STSTokenIdentifier which calls toProtoBuf(), and the encryptionKey is not
    // serialized there.
    final STSTokenIdentifier identifier = new STSTokenIdentifier(
        tempAccessKeyId, originalAccessKeyId, roleArn, expiration, secretAccessKey, sessionPolicy, encryptionKey);

    final Token<STSTokenIdentifier> token = generateToken(identifier);
    return token.encodeToUrlString();
  }
}


