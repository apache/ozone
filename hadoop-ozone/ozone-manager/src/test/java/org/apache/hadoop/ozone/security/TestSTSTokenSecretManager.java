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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.ozone.test.MockClock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for STSTokenSecretManager.
 */
public class TestSTSTokenSecretManager {
  private STSTokenSecretManager secretManager;
  private MockClock clock;

  private static final String TEMP_ACCESS_KEY = "temp-access-key";
  private static final String ORIGINAL_ACCESS_KEY = "original-access-key";
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";
  private static final String SECRET_ACCESS_KEY = "test-secret-access-key";
  private static final String SESSION_POLICY = "test-session-policy";
  private static final String ASSUMED_ROLE_ID = "AROATEST123456789:testsess";
  private static final String ASSUMED_ROLE_USER_ARN = "arn:aws:sts::123456789012:assumed-role/test-role/testsess";
  private static final int DURATION_SECONDS = 3600;

  private static SecretKey sharedSecretKey;

  @BeforeAll
  public static void setUpClass() {
    final byte[] keyBytes = "01234567890123456789012345678901".getBytes(StandardCharsets.US_ASCII);
    sharedSecretKey = new SecretKeySpec(keyBytes, "HmacSHA256");
  }

  @BeforeEach
  public void setUp() throws Exception {
    final SecretKeySignerClient mockSecretKeyClient = mock(SecretKeySignerClient.class);
    final ManagedSecretKey mockSecretKey = mock(ManagedSecretKey.class);

    final UUID keyId = UUID.fromString("00000000-0000-0000-0000-000000000000");
    when(mockSecretKey.getId()).thenReturn(keyId);
    when(mockSecretKey.getSecretKey()).thenReturn(sharedSecretKey);
    when(mockSecretKey.sign(any(byte[].class)))
        .thenReturn("mock-signature".getBytes(StandardCharsets.UTF_8));
    when(mockSecretKeyClient.getCurrentSecretKey()).thenReturn(mockSecretKey);

    secretManager = new STSTokenSecretManager(mockSecretKeyClient);
    clock = new MockClock(Instant.ofEpochMilli(1764819000), ZoneOffset.UTC);
  }

  @Test
  public void testCreateSTSTokenStringContainsCorrectFields() throws IOException {
    final String tokenString = secretManager.createSTSTokenString(createStsTokenParamsBuilder().build());

    // Decode the token
    final Token<STSTokenIdentifier> token = new Token<>();
    token.decodeFromUrlString(tokenString);

    // Verify the token identifier fields
    final STSTokenIdentifier identifier = new STSTokenIdentifier();
    identifier.setManagedSecretKey(createManagedSecretKey(
        UUID.fromString("00000000-0000-0000-0000-000000000000"),
        sharedSecretKey.getEncoded(), Instant.now()));
    identifier.readFromByteArray(token.getIdentifier());
    final Instant expiration = identifier.getExpiry();

    assertEquals(TEMP_ACCESS_KEY, identifier.getTempAccessKeyId());
    assertEquals(ORIGINAL_ACCESS_KEY, identifier.getOriginalAccessKeyId());
    assertEquals(ROLE_ARN, identifier.getRoleArn());
    assertEquals(SECRET_ACCESS_KEY, identifier.getSecretAccessKey());
    assertEquals(SESSION_POLICY, identifier.getSessionPolicy());
    assertEquals(ASSUMED_ROLE_ID, identifier.getAssumedRoleId());
    assertEquals(ASSUMED_ROLE_USER_ARN, identifier.getAssumedRoleUserArn());
    assertEquals(clock.instant(), identifier.getCreationTime());
    assertNotNull(identifier.getSecretKeyId());
    assertEquals(new Text("STSToken"), identifier.getKind());
    assertEquals("STS", identifier.getService());
    assertEquals(clock.millis() + (DURATION_SECONDS * 1000), expiration.toEpochMilli());
  }

  @Test
  public void testCreateSTSTokenStringWithNullSessionPolicy() throws IOException {
    final String tokenString = secretManager.createSTSTokenString(
        createStsTokenParamsBuilder().setSessionPolicy(null).build());

    // Decode the token
    final Token<STSTokenIdentifier> token = new Token<>();
    token.decodeFromUrlString(tokenString);

    final STSTokenIdentifier identifier = new STSTokenIdentifier();
    identifier.setManagedSecretKey(createManagedSecretKey(
        UUID.fromString("00000000-0000-0000-0000-000000000000"),
        sharedSecretKey.getEncoded(), Instant.now()));
    identifier.readFromByteArray(token.getIdentifier());
    assertTrue(identifier.getSessionPolicy().isEmpty());
  }

  /**
   * createSTSTokenString() must use a single getCurrentSecretKey() for encryption, secretKeyId, and signing. If a
   * second fetch happened during signing, a key rotation between calls would encrypt with the old key but stamp the
   * token with the new key id.
   */
  @Test
  public void testCreateSTSTokenStringValidatesWhenSecretKeyRotatesDuringCreation() throws Exception {
    // ManagedSecretKey.isExpired() uses Instant.now(), not the test clock.
    final Instant keyCreationTime = Instant.now();
    final ManagedSecretKey encryptionKey = createManagedSecretKey(
        UUID.fromString("11111111-1111-1111-1111-111111111111"),
        "encryption-key-material-012345678901".getBytes(StandardCharsets.US_ASCII),
        keyCreationTime);
    final ManagedSecretKey signingKey = createManagedSecretKey(
        UUID.fromString("22222222-2222-2222-2222-222222222222"),
        "signing-key-material-01234567890123".getBytes(StandardCharsets.US_ASCII),
        keyCreationTime);

    final RotatingSecretKeyTestClient rotatingSecretKeyClient = new RotatingSecretKeyTestClient(
        encryptionKey, signingKey);
    final STSTokenSecretManager rotatingSecretManager = new STSTokenSecretManager(rotatingSecretKeyClient);

    final String tokenString = rotatingSecretManager.createSTSTokenString(createStsTokenParamsBuilder().build());

    final STSTokenIdentifier result = STSSecurityUtil.constructValidateAndDecryptSTSToken(
        tokenString, rotatingSecretKeyClient, clock);
    assertEquals(SECRET_ACCESS_KEY, result.getSecretAccessKey());
    assertEquals(encryptionKey.getId(), result.getSecretKeyId());
    assertEquals(1, rotatingSecretKeyClient.getCurrentSecretKeyCallCount());
  }

  private STSTokenSecretManager.CreateSTSTokenParams.Builder createStsTokenParamsBuilder() {
    return STSTokenSecretManager.CreateSTSTokenParams.newBuilder()
        .setTempAccessKeyId(TEMP_ACCESS_KEY)
        .setOriginalAccessKeyId(ORIGINAL_ACCESS_KEY)
        .setRoleArn(ROLE_ARN)
        .setDurationSeconds(DURATION_SECONDS)
        .setSecretAccessKey(SECRET_ACCESS_KEY)
        .setSessionPolicy(SESSION_POLICY)
        .setAssumedRoleId(ASSUMED_ROLE_ID)
        .setAssumedRoleUserArn(ASSUMED_ROLE_USER_ARN)
        .setClock(clock);
  }

  private static ManagedSecretKey createManagedSecretKey(UUID id, byte[] keyBytes, Instant creationTime) {
    final SecretKey secretKey = new SecretKeySpec(keyBytes, "HmacSHA256");
    return new ManagedSecretKey(id, creationTime, creationTime.plus(Duration.ofHours(1)), secretKey);
  }

  /**
   * Returns different current keys on consecutive getCurrentSecretKey() calls to simulate rotation.
   */
  private static final class RotatingSecretKeyTestClient implements SecretKeyClient {
    private final ManagedSecretKey firstKey;
    private final ManagedSecretKey secondKey;
    private final Map<UUID, ManagedSecretKey> keysById = new HashMap<>();
    private int getCurrentSecretKeyCallCount;

    private RotatingSecretKeyTestClient(ManagedSecretKey firstKey, ManagedSecretKey secondKey) {
      this.firstKey = firstKey;
      this.secondKey = secondKey;
      keysById.put(firstKey.getId(), firstKey);
      keysById.put(secondKey.getId(), secondKey);
    }

    @Override
    public synchronized ManagedSecretKey getCurrentSecretKey() {
      return getCurrentSecretKeyCallCount++ == 0 ? firstKey : secondKey;
    }

    @Override
    public ManagedSecretKey getSecretKey(UUID id) {
      return keysById.get(id);
    }

    private int getCurrentSecretKeyCallCount() {
      return getCurrentSecretKeyCallCount;
    }
  }
}
