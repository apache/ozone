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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyVerifierClient;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.jupiter.api.Test;

/**
 * Common test cases for {@link ShortLivedTokenVerifier} implementations.
 */
public abstract class TokenVerifierTests<T extends ShortLivedTokenIdentifier> {

  protected static final UUID SECRET_KEY_ID = UUID.randomUUID();

  /**
   * Create the specific kind of TokenVerifier.
   */
  protected abstract TokenVerifier newTestSubject(
      SecurityConfig secConf, SecretKeyVerifierClient secretKeyClient);

  /**
   * @return the config key to enable/disable the specific kind of tokens
   */
  protected abstract String tokenEnabledConfigKey();

  /**
   * Create a request for which the verifier being tested does not require
   * tokens (eg. reading blocks does not require container token and vice versa)
   */
  protected abstract ContainerCommandRequestProto unverifiedRequest()
      throws IOException;

  /**
   * Create a request for which token should be required.
   */
  protected abstract ContainerCommandRequestProto verifiedRequest(T tokenId)
      throws IOException;

  protected abstract T newTokenId();

  @Test
  public void skipsVerificationIfDisabled() throws IOException {
    // GIVEN
    SecretKeyVerifierClient secretKeyClient = mock(
        SecretKeyVerifierClient.class);
    TokenVerifier subject = newTestSubject(tokenDisabled(), secretKeyClient);

    // WHEN
    subject.verify(anyToken(), verifiedRequest(newTokenId()));

    // THEN
    verify(secretKeyClient, never()).getSecretKey(any());
  }

  @Test
  public void skipsVerificationForMiscCommands() throws IOException {
    // GIVEN
    SecretKeyVerifierClient secretKeyClient = mock(
        SecretKeyVerifierClient.class);
    TokenVerifier subject = newTestSubject(tokenEnabled(), secretKeyClient);

    // WHEN
    subject.verify(anyToken(), unverifiedRequest());

    // THEN
    verify(secretKeyClient, never()).getSecretKey(any());
  }

  @Test
  public void rejectsExpiredSecretKey() throws Exception {
    // GIVEN
    SecretKeyVerifierClient secretKeyClient =
        mock(SecretKeyVerifierClient.class);

    Instant past = Instant.now().minus(Duration.ofHours(1));
    ManagedSecretKey expiredSecretKey = new ManagedSecretKey(UUID.randomUUID(),
        past, past, mock(SecretKey.class));

    when(secretKeyClient.getSecretKey(SECRET_KEY_ID))
        .thenReturn(expiredSecretKey);
    T tokenId = newTokenId();
    ContainerCommandRequestProto cmd = verifiedRequest(tokenId);
    TokenVerifier subject = newTestSubject(tokenEnabled(), secretKeyClient);

    // WHEN+THEN
    ShortLivedTokenSecretManager<T> secretManager = new MockTokenManager();
    Token<T> token = secretManager.generateToken(tokenId);
    BlockTokenException ex = assertThrows(BlockTokenException.class, () ->
        subject.verify(token, cmd));
    assertThat(ex.getMessage()).contains("expired secret key");
  }

  @Test
  public void rejectsTokenWithInvalidSecretId() throws Exception {
    // GIVEN
    SecretKeyVerifierClient secretKeyClient =
        mock(SecretKeyVerifierClient.class);

    when(secretKeyClient.getSecretKey(SECRET_KEY_ID)).thenReturn(null);
    T tokenId = newTokenId();
    ContainerCommandRequestProto cmd = verifiedRequest(tokenId);
    TokenVerifier subject = newTestSubject(tokenEnabled(), secretKeyClient);

    // WHEN+THEN
    ShortLivedTokenSecretManager<T> secretManager = new MockTokenManager();
    Token<T> token = secretManager.generateToken(tokenId);
    BlockTokenException ex = assertThrows(BlockTokenException.class, () ->
        subject.verify(token, cmd));
    assertThat(ex.getMessage())
        .contains("Can't find the signing secret key");
  }

  @Test
  public void rejectsInvalidSignature() throws Exception {
    // GIVEN
    SecretKeyVerifierClient secretKeyClient =
        mockSecretKeyClient(false);

    ShortLivedTokenSecretManager<T> secretManager = new MockTokenManager();
    T tokenId = newTokenId();
    Token<?> invalidToken = secretManager.generateToken(tokenId);
    ContainerCommandRequestProto cmd = verifiedRequest(tokenId);
    TokenVerifier subject = newTestSubject(tokenEnabled(), secretKeyClient);

    // WHEN+THEN
    BlockTokenException ex =
        assertThrows(BlockTokenException.class, () ->
            subject.verify(invalidToken, cmd));
    assertThat(ex.getMessage())
        .contains("Invalid token for user");
  }

  @Nonnull
  private SecretKeyVerifierClient mockSecretKeyClient(boolean validSignature)
      throws IOException {
    SecretKeyVerifierClient secretKeyClient =
        mock(SecretKeyVerifierClient.class);
    ManagedSecretKey validSecretKey = mock(ManagedSecretKey.class);
    when(secretKeyClient.getSecretKey(SECRET_KEY_ID))
        .thenReturn(validSecretKey);
    when(validSecretKey.isValidSignature((TokenIdentifier) any(), any()))
        .thenReturn(validSignature);
    return secretKeyClient;
  }

  @Test
  public void rejectsExpiredToken() throws Exception {
    // GIVEN
    SecretKeyVerifierClient secretKeyClient = mockSecretKeyClient(true);

    ShortLivedTokenSecretManager<T> secretManager = new MockTokenManager();
    T tokenId = expired(newTokenId());
    ContainerCommandRequestProto cmd = verifiedRequest(tokenId);
    Token<?> token = secretManager.generateToken(tokenId);
    TokenVerifier subject = newTestSubject(tokenEnabled(), secretKeyClient);

    // WHEN+THEN
    BlockTokenException ex =
        assertThrows(BlockTokenException.class, () ->
            subject.verify(token, cmd));
    assertThat(ex.getMessage())
        .contains("Expired token for user");
  }

  @Test
  public void acceptsValidToken() throws Exception {
    // GIVEN
    SecurityConfig conf = tokenEnabled();
    SecretKeyVerifierClient secretKeyClient = mockSecretKeyClient(true);

    ShortLivedTokenSecretManager<T> secretManager = new MockTokenManager();
    T tokenId = valid(newTokenId());
    ContainerCommandRequestProto cmd = verifiedRequest(tokenId);
    Token<?> token = secretManager.generateToken(tokenId);
    TokenVerifier subject = newTestSubject(conf, secretKeyClient);

    // WHEN+THEN
    subject.verify(token, cmd);
  }

  private T expired(T tokenId) {
    tokenId.setExpiry(Instant.now().minusSeconds(3600));
    return tokenId;
  }

  private T valid(T tokenId) {
    tokenId.setExpiry(Instant.now().plusSeconds(3600));
    return tokenId;
  }

  protected SecurityConfig tokenDisabled() {
    return getSecurityConfig(false);
  }

  protected SecurityConfig tokenEnabled() {
    return getSecurityConfig(true);
  }

  private SecurityConfig getSecurityConfig(boolean tokenEnabled) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(tokenEnabledConfigKey(), tokenEnabled);
    return new SecurityConfig(conf);
  }

  private static Token<?> anyToken() {
    return new Token<>();
  }

  /**
   * Mock secret manager for test.  No private key etc.
   */
  private class MockTokenManager extends ShortLivedTokenSecretManager<T> {

    MockTokenManager() {
      super(TimeUnit.HOURS.toMillis(1),
          mock(SecretKeySignerClient.class));
    }

    @Override
    public byte[] createPassword(T identifier) {
      return "asdf".getBytes(UTF_8);
    }
  }
}
