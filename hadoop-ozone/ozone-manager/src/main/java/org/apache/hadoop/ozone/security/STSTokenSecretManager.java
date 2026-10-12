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
import java.util.Objects;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
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
    // Note - the ManagedSecretKey will NOT be encoded in the token. When generateToken() is called,
    // it eventually calls the write() method in STSTokenIdentifier which calls toProtoBuf(), and the
    // ManagedSecretKey is not serialized there.
    Objects.requireNonNull(
        tokenIdentifier.getManagedSecretKey(), "ManagedSecretKey must be set on the token identifier before signing");
    final byte[] identifierBytes = tokenIdentifier.getBytes();
    final byte[] password = tokenIdentifier.sign(identifierBytes);
    return new Token<>(identifierBytes, password, tokenIdentifier.getKind(), new Text(tokenIdentifier.getService()));
  }

  /**
   * Create an STS token and return it as an encoded string.
   *
   * @param params  the STS token creation parameters
   * @return base64 encoded token string
   */
  public String createSTSTokenString(CreateSTSTokenParams params) throws IOException {
    final Instant creationTime = params.getCreationTime();
    final Instant expiration = creationTime.plusSeconds(params.getDurationSeconds());

    final STSTokenIdentifier identifier = new STSTokenIdentifier(STSTokenIdentifier.Params.newBuilder()
        .setTempAccessKeyId(params.getTempAccessKeyId())
        .setOriginalAccessKeyId(params.getOriginalAccessKeyId())
        .setRoleArn(params.getRoleArn())
        .setCreationTime(creationTime)
        .setExpiry(expiration)
        .setSecretAccessKey(params.getSecretAccessKey())
        .setSessionPolicy(params.getSessionPolicy())
        .setManagedSecretKey(secretKeyClient.getCurrentSecretKey())
        .setAssumedRoleId(params.getAssumedRoleId())
        .setAssumedRoleUserArn(params.getAssumedRoleUserArn())
        .build());

    final Token<STSTokenIdentifier> token = generateToken(identifier);
    return token.encodeToUrlString();
  }

  /**
   * Parameters for {@link #createSTSTokenString(CreateSTSTokenParams)}.
   */
  public static final class CreateSTSTokenParams {
    private final String tempAccessKeyId;
    private final String originalAccessKeyId;
    private final String roleArn;
    private final int durationSeconds;
    private final String secretAccessKey;
    private final String sessionPolicy;
    private final String assumedRoleId;
    private final String assumedRoleUserArn;
    private final Instant creationTime;

    private CreateSTSTokenParams(Builder builder) {
      this.tempAccessKeyId = builder.tempAccessKeyId;
      this.originalAccessKeyId = builder.originalAccessKeyId;
      this.roleArn = builder.roleArn;
      this.durationSeconds = builder.durationSeconds;
      this.secretAccessKey = builder.secretAccessKey;
      this.sessionPolicy = builder.sessionPolicy;
      this.assumedRoleId = builder.assumedRoleId;
      this.assumedRoleUserArn = builder.assumedRoleUserArn;
      this.creationTime = builder.creationTime;
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public String getTempAccessKeyId() {
      return tempAccessKeyId;
    }

    public String getOriginalAccessKeyId() {
      return originalAccessKeyId;
    }

    public String getRoleArn() {
      return roleArn;
    }

    public int getDurationSeconds() {
      return durationSeconds;
    }

    public String getSecretAccessKey() {
      return secretAccessKey;
    }

    public String getSessionPolicy() {
      return sessionPolicy;
    }

    public String getAssumedRoleId() {
      return assumedRoleId;
    }

    public String getAssumedRoleUserArn() {
      return assumedRoleUserArn;
    }

    public Instant getCreationTime() {
      return creationTime;
    }

    /**
     * Builder for {@link CreateSTSTokenParams}.
     */
    public static final class Builder {
      private String tempAccessKeyId;
      private String originalAccessKeyId;
      private String roleArn;
      private int durationSeconds;
      private String secretAccessKey;
      private String sessionPolicy;
      private String assumedRoleId;
      private String assumedRoleUserArn;
      private Instant creationTime;

      public Builder setTempAccessKeyId(String value) {
        this.tempAccessKeyId = value;
        return this;
      }

      public Builder setOriginalAccessKeyId(String value) {
        this.originalAccessKeyId = value;
        return this;
      }

      public Builder setRoleArn(String value) {
        this.roleArn = value;
        return this;
      }

      public Builder setDurationSeconds(int value) {
        this.durationSeconds = value;
        return this;
      }

      public Builder setSecretAccessKey(String value) {
        this.secretAccessKey = value;
        return this;
      }

      public Builder setSessionPolicy(String value) {
        this.sessionPolicy = value;
        return this;
      }

      public Builder setAssumedRoleId(String value) {
        this.assumedRoleId = value;
        return this;
      }

      public Builder setAssumedRoleUserArn(String value) {
        this.assumedRoleUserArn = value;
        return this;
      }

      public Builder setCreationTime(Instant creationTime) {
        this.creationTime = creationTime;
        return this;
      }

      public CreateSTSTokenParams build() {
        return new CreateSTSTokenParams(this);
      }
    }
  }
}


