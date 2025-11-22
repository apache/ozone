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
import java.time.Instant;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.hdds.security.token.ShortLivedTokenSecretManager;
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
   * Create an STS token and return it as an encoded string.
   *
   * @param tempAccessKeyId     the temporary access key ID
   * @param originalAccessKeyId the original long-lived access key ID
   * @param roleArn             the ARN of the assumed role
   * @param durationSeconds     how long the token should be valid for
   * @param secretAccessKey     the secret access key associated with the temporary access key ID
   * @param sessionPolicy       an optional opaque identifier that further limits the scope of
   *                            the permissions granted by the role
   * @return base64 encoded token string
   */
  public String createSTSTokenString(String tempAccessKeyId, String originalAccessKeyId, String roleArn,
      int durationSeconds, String secretAccessKey, String sessionPolicy) throws IOException {
    final Instant expiration = Instant.now().plusSeconds(durationSeconds);

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


