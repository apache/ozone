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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for STSSecurityUtil.
 */
public class TestSTSSecurityUtil {
  private static final String TEMP_ACCESS_KEY = "temp-access-key";
  private static final String ORIGINAL_ACCESS_KEY = "original-access-key";
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";
  private static final String SECRET_ACCESS_KEY = "test-secret-access-key";
  private static final String SESSION_POLICY = "test-session-policy";
  private static final int DURATION_SECONDS = 3600;

  private final SecretKeyTestClient secretKeyClient = new SecretKeyTestClient();
  private final STSTokenSecretManager tokenSecretManager = new STSTokenSecretManager(secretKeyClient);
  private final UUID secretKeyId = secretKeyClient.getCurrentSecretKey().getId();
  private final TestClock clock = new TestClock(Instant.ofEpochMilli(1764819000), ZoneOffset.UTC);

  @Test
  public void testConstructValidateAndDecryptSTSTokenInvalidProtobuf() throws IOException {
    // Create a token whose identifier bytes are not a valid OMTokenProto
    final Token<STSTokenIdentifier> token = new Token<>(
        new byte[] {0x01, 0x02, 0x03}, new byte[] {0x04}, STSTokenIdentifier.KIND_NAME,
        new Text(STSTokenIdentifier.STS_SERVICE));

    final String tokenString = token.encodeToUrlString();

    assertThatThrownBy(() ->
        STSSecurityUtil.constructValidateAndDecryptSTSToken(tokenString, secretKeyClient, clock))
        .isInstanceOf(OMException.class)
        .hasMessage(
            "Invalid STS token format: Invalid STS token - could not parse protocol buffer: Protocol message " +
            "contained an invalid tag (zero).");
  }

  @Test
  public void testConstructValidateAndDecryptSTSTokenSuccess() throws IOException {
    // Create a valid token
    final String tokenString = tokenSecretManager.createSTSTokenString(
        TEMP_ACCESS_KEY, ORIGINAL_ACCESS_KEY, ROLE_ARN, DURATION_SECONDS, SECRET_ACCESS_KEY, SESSION_POLICY, clock);

    // Validate and decrypt the token
    final STSTokenIdentifier result = STSSecurityUtil.constructValidateAndDecryptSTSToken(
        tokenString, secretKeyClient, clock);

    // Verify the result
    assertThat(result.getOwnerId()).isEqualTo(TEMP_ACCESS_KEY);
    assertThat(result.getOriginalAccessKeyId()).isEqualTo(ORIGINAL_ACCESS_KEY);
    assertThat(result.getRoleArn()).isEqualTo(ROLE_ARN);
    assertThat(result.getSecretAccessKey()).isEqualTo(SECRET_ACCESS_KEY);
    assertThat(result.getSessionPolicy()).isEqualTo(SESSION_POLICY);
    assertThat(result.isExpired(clock.instant())).isFalse();
    final long expirationEpochMillis = result.getExpiry().toEpochMilli();
    assertThat(expirationEpochMillis).isEqualTo(clock.millis() + (DURATION_SECONDS * 1000));
  }

  @Test
  public void testConstructValidateAndDecryptSTSTokenSuccessWithNullSessionPolicy() throws Exception {
    // Create a valid token with null session policy
    final String tokenString = tokenSecretManager.createSTSTokenString(
        TEMP_ACCESS_KEY, ORIGINAL_ACCESS_KEY, ROLE_ARN, DURATION_SECONDS, SECRET_ACCESS_KEY, null, clock);

    // Validate and decrypt the token
    final STSTokenIdentifier result = STSSecurityUtil.constructValidateAndDecryptSTSToken(
        tokenString, secretKeyClient, clock);

    // Verify the result
    assertThat(result.getSessionPolicy()).isEmpty();
  }

  @Test
  public void testConstructValidateAndDecryptSTSTokenInvalidFormat() {
    // Try to decrypt an invalid token string
    assertThatThrownBy(() ->
        STSSecurityUtil.constructValidateAndDecryptSTSToken("invalid-token-format", secretKeyClient, clock))
        .isInstanceOf(OMException.class)
        .hasMessageContaining("Invalid STS token format: Failed to decode STS token string");
  }

  @Test
  public void testConstructValidateAndDecryptSTSTokenInvalidKind() throws Exception {
    // Create a valid identifier to use as base
    final String validTokenString = tokenSecretManager.createSTSTokenString(
        TEMP_ACCESS_KEY, ORIGINAL_ACCESS_KEY, ROLE_ARN, DURATION_SECONDS, SECRET_ACCESS_KEY, SESSION_POLICY, clock);

    final Token<STSTokenIdentifier> validToken = new Token<>();
    validToken.decodeFromUrlString(validTokenString);

    // Create token with wrong kind
    final Token<STSTokenIdentifier> token = new Token<>(
        validToken.getIdentifier(), validToken.getPassword(), new Text("WRONG_KIND"),
        new Text(STSTokenIdentifier.STS_SERVICE));

    final String invalidTokenString = token.encodeToUrlString();

    // Try to validate the token with wrong kind
    assertThatThrownBy(() ->
        STSSecurityUtil.constructValidateAndDecryptSTSToken(invalidTokenString, secretKeyClient, clock))
        .isInstanceOf(OMException.class)
        .hasMessage("Invalid STS token format: Invalid STS token - kind is incorrect: WRONG_KIND");
  }

  @Test
  public void testConstructValidateAndDecryptSTSTokenInvalidService() throws Exception {
    // Create a token with incorrect service
    final String validTokenString = tokenSecretManager.createSTSTokenString(
        TEMP_ACCESS_KEY, ORIGINAL_ACCESS_KEY, ROLE_ARN, DURATION_SECONDS, SECRET_ACCESS_KEY, SESSION_POLICY, clock);

    final Token<STSTokenIdentifier> validToken = new Token<>();
    validToken.decodeFromUrlString(validTokenString);

    final Token<STSTokenIdentifier> token = new Token<>(
        validToken.getIdentifier(), validToken.getPassword(), validToken.getKind(), new Text("WRONG_SERVICE"));

    final String invalidTokenString = token.encodeToUrlString();

    // Try to validate the token with wrong service
    assertThatThrownBy(() ->
        STSSecurityUtil.constructValidateAndDecryptSTSToken(invalidTokenString, secretKeyClient, clock))
        .isInstanceOf(OMException.class)
        .hasMessage("Invalid STS token format: Invalid STS token - service is incorrect: WRONG_SERVICE");
  }

  @Test
  public void testConstructValidateAndDecryptSTSTokenExpired() throws Exception {
    // Create a token that expires immediately (durationSeconds of 0)
    final String tokenString = tokenSecretManager.createSTSTokenString(
        TEMP_ACCESS_KEY, ORIGINAL_ACCESS_KEY, ROLE_ARN, 0, SECRET_ACCESS_KEY, SESSION_POLICY, clock);

    // Fast-forward time to ensure token is expired
    clock.fastForward(100);

    // Try to validate the expired token
    assertThatThrownBy(() ->
        STSSecurityUtil.constructValidateAndDecryptSTSToken(tokenString, secretKeyClient, clock))
        .isInstanceOf(OMException.class)
        .hasMessageContaining("Invalid STS token format: Invalid STS token - token expired at");
  }

  @Test
  public void testConstructValidateAndDecryptSTSTokenSecretKeyNotFound() throws Exception {
    // Create a valid token string
    final String validTokenString = tokenSecretManager.createSTSTokenString(
        TEMP_ACCESS_KEY, ORIGINAL_ACCESS_KEY, ROLE_ARN, DURATION_SECONDS, SECRET_ACCESS_KEY, SESSION_POLICY, clock);

    // Create a mock secret key client that returns null for the key
    final SecretKeyClient mockKeyClient = mock(SecretKeyClient.class);
    when(mockKeyClient.getSecretKey(any())).thenReturn(null);

    // Try to validate the token when secret key is not found
    assertThatThrownBy(() ->
        STSSecurityUtil.constructValidateAndDecryptSTSToken(validTokenString, mockKeyClient, clock))
        .isInstanceOf(OMException.class)
        .hasMessage(
            "Invalid STS token format: Invalid STS token - could not readFromByteArray: Secret key not found for " +
            "STS token secretKeyId: " + secretKeyId);
  }

  @Test
  public void testConstructValidateAndDecryptSTSTokenInvalidSecretKeyId() throws Exception {
    // Create a valid identifier to use as base
    final String validTokenString = tokenSecretManager.createSTSTokenString(
        TEMP_ACCESS_KEY, ORIGINAL_ACCESS_KEY, ROLE_ARN, DURATION_SECONDS, SECRET_ACCESS_KEY, SESSION_POLICY, clock);

    final Token<STSTokenIdentifier> validToken = new Token<>();
    validToken.decodeFromUrlString(validTokenString);

    // Rewrite the identifier with an invalid secretKeyId in the protobuf
    final byte[] identifierBytes = validToken.getIdentifier();
    final OMTokenProto proto = OMTokenProto.parseFrom(identifierBytes);

    final OMTokenProto invalidProto = proto.toBuilder().setSecretKeyId("not-a-uuid").build();

    final Token<STSTokenIdentifier> brokenToken = new Token<>(
        invalidProto.toByteArray(), validToken.getPassword(), validToken.getKind(), validToken.getService());

    final String invalidTokenString = brokenToken.encodeToUrlString();

    assertThatThrownBy(() ->
        STSSecurityUtil.constructValidateAndDecryptSTSToken(invalidTokenString, secretKeyClient, clock))
        .isInstanceOf(OMException.class)
        .hasMessage("Invalid STS token format: Invalid STS token - secretKeyId was not valid: not-a-uuid");
  }

  @Test
  public void testConstructValidateAndDecryptSTSTokenExpiredSecretKey() throws Exception {
    // Create a valid token string
    final String validTokenString = tokenSecretManager.createSTSTokenString(
        TEMP_ACCESS_KEY, ORIGINAL_ACCESS_KEY, ROLE_ARN, DURATION_SECONDS, SECRET_ACCESS_KEY, SESSION_POLICY, clock);

    // Create a mock secret key that is expired
    final ManagedSecretKey expiredSecretKey = mock(ManagedSecretKey.class);
    when(expiredSecretKey.isExpired()).thenReturn(true);

    final SecretKeyClient mockKeyClient = mock(SecretKeyClient.class);
    when(mockKeyClient.getSecretKey(any())).thenReturn(expiredSecretKey);

    // Try to validate the token with expired secret key
    assertThatThrownBy(() ->
        STSSecurityUtil.constructValidateAndDecryptSTSToken(validTokenString, mockKeyClient, clock))
        .isInstanceOf(OMException.class)
        .hasMessage(
            "Invalid STS token format: Invalid STS token - could not readFromByteArray: Token cannot be " +
            "verified due to expired secret key " + secretKeyId);
  }

  @Test
  public void testConstructValidateAndDecryptSTSTokenSecretKeyRetrievalException() throws Exception {
    // Create a valid token string
    final String validTokenString = tokenSecretManager.createSTSTokenString(
        TEMP_ACCESS_KEY, ORIGINAL_ACCESS_KEY, ROLE_ARN, DURATION_SECONDS, SECRET_ACCESS_KEY, SESSION_POLICY, clock);

    // Create a mock secret key client that throws an exception
    final SecretKeyClient mockKeyClient = mock(SecretKeyClient.class);
    when(mockKeyClient.getSecretKey(any())).thenThrow(new SCMSecurityException("something went wrong"));

    // Try to validate the token when secret key retrieval fails
    assertThatThrownBy(() ->
        STSSecurityUtil.constructValidateAndDecryptSTSToken(validTokenString, mockKeyClient, clock))
        .isInstanceOf(OMException.class)
        .hasMessage(
            "Invalid STS token format: Invalid STS token - could not readFromByteArray: Failed to retrieve secret " +
            "key: something went wrong");
  }

  @Test
  public void testConstructValidateAndDecryptSTSTokenInvalidSignature() throws Exception {
    // Create a valid token string
    final String validTokenString = tokenSecretManager.createSTSTokenString(
        TEMP_ACCESS_KEY, ORIGINAL_ACCESS_KEY, ROLE_ARN, DURATION_SECONDS, SECRET_ACCESS_KEY, SESSION_POLICY, clock);

    final Token<STSTokenIdentifier> validToken = new Token<>();
    validToken.decodeFromUrlString(validTokenString);

    // Create a token with invalid signature (wrong password)
    final Token<STSTokenIdentifier> invalidToken = new Token<>(
        validToken.getIdentifier(), "wrong-signature".getBytes(StandardCharsets.UTF_8), validToken.getKind(),
        validToken.getService());

    final String invalidTokenString = invalidToken.encodeToUrlString();

    // Try to validate the token with invalid signature
    assertThatThrownBy(() ->
        STSSecurityUtil.constructValidateAndDecryptSTSToken(invalidTokenString, secretKeyClient, clock))
        .isInstanceOf(OMException.class)
        .hasMessageContaining("Invalid STS token format: Invalid STS token - signature is not correct for token");
  }

  @Test
  public void testConstructValidateAndDecryptSTSTokenEmptyString() {
    // Try to decrypt an empty token string
    assertThatThrownBy(() ->
        STSSecurityUtil.constructValidateAndDecryptSTSToken("", secretKeyClient, clock))
        .isInstanceOf(OMException.class)
        .hasMessage("Invalid STS token format: Failed to decode STS token string: java.io.EOFException");
  }

  @Test
  public void testConstructValidateAndDecryptMultipleTokens() throws Exception {
    // Create multiple tokens and validate them all
    final String token1 = tokenSecretManager.createSTSTokenString(
        "temp-key-1", "orig-key-1", "role-arn-1", DURATION_SECONDS,
        "secret-key-1", "policy-1", clock);

    final String token2 = tokenSecretManager.createSTSTokenString(
        "temp-key-2", "orig-key-2", "role-arn-2", DURATION_SECONDS,
        "secret-key-2", "policy-2", clock);

    final STSTokenIdentifier result1 = STSSecurityUtil.constructValidateAndDecryptSTSToken(
        token1, secretKeyClient, clock);
    final STSTokenIdentifier result2 = STSSecurityUtil.constructValidateAndDecryptSTSToken(
        token2, secretKeyClient, clock);

    assertThat(result1.getOwnerId()).isEqualTo("temp-key-1");
    assertThat(result1.getOriginalAccessKeyId()).isEqualTo("orig-key-1");
    assertThat(result2.getOwnerId()).isEqualTo("temp-key-2");
    assertThat(result2.getOriginalAccessKeyId()).isEqualTo("orig-key-2");
  }
}

