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
import java.time.Instant;
import java.util.UUID;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for STSTokenSecretManager.
 */
public class TestSTSTokenSecretManager {
  private STSTokenSecretManager secretManager;
  private static final String TEMP_ACCESS_KEY = "temp-access-key";
  private static final String ORIGINAL_ACCESS_KEY = "original-access-key";
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";
  private static final String SECRET_ACCESS_KEY = "test-secret-access-key";
  private static final String SESSION_POLICY = "test-session-policy";
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
    when(mockSecretKey.sign(any(STSTokenIdentifier.class)))
        .thenReturn("mock-signature".getBytes(StandardCharsets.UTF_8));
    when(mockSecretKeyClient.getCurrentSecretKey()).thenReturn(mockSecretKey);

    secretManager = new STSTokenSecretManager(mockSecretKeyClient);
  }

  @Test
  public void testCreateSTSTokenStringContainsCorrectFields() throws IOException {
    final Instant beforeCreation = Instant.now();

    final String tokenString = secretManager.createSTSTokenString(
        TEMP_ACCESS_KEY, ORIGINAL_ACCESS_KEY, ROLE_ARN, DURATION_SECONDS, SECRET_ACCESS_KEY, SESSION_POLICY);

    // Decode the token
    final Token<STSTokenIdentifier> token = new Token<>();
    token.decodeFromUrlString(tokenString);

    // Verify the token identifier fields
    final STSTokenIdentifier identifier = new STSTokenIdentifier();
    identifier.setEncryptionKey(sharedSecretKey.getEncoded());
    identifier.readFromByteArray(token.getIdentifier());
    final Instant afterCreation = Instant.now();
    final Instant expiration = identifier.getExpiry();

    assertEquals(TEMP_ACCESS_KEY, identifier.getTempAccessKeyId());
    assertEquals(ORIGINAL_ACCESS_KEY, identifier.getOriginalAccessKeyId());
    assertEquals(ROLE_ARN, identifier.getRoleArn());
    assertEquals(SECRET_ACCESS_KEY, identifier.getSecretAccessKey());
    assertEquals(SESSION_POLICY, identifier.getSessionPolicy());
    assertNotNull(identifier.getSecretKeyId());
    assertEquals(new Text("STSToken"), identifier.getKind());
    assertEquals("STS", identifier.getService());
    // Verify expiration is approximately durationSeconds in the future
    assertTrue(expiration.isAfter(beforeCreation.plusSeconds(DURATION_SECONDS - 1)));
    assertTrue(expiration.isBefore(afterCreation.plusSeconds(DURATION_SECONDS + 1)));
  }

  @Test
  public void testCreateSTSTokenStringWithNullSessionPolicy() throws IOException {
    final String tokenString = secretManager.createSTSTokenString(
        TEMP_ACCESS_KEY, ORIGINAL_ACCESS_KEY, ROLE_ARN, DURATION_SECONDS, SECRET_ACCESS_KEY, null);

    // Decode the token
    final Token<STSTokenIdentifier> token = new Token<>();
    token.decodeFromUrlString(tokenString);

    final STSTokenIdentifier identifier = new STSTokenIdentifier();
    identifier.setEncryptionKey(sharedSecretKey.getEncoded());
    identifier.readFromByteArray(token.getIdentifier());
    assertTrue(identifier.getSessionPolicy().isEmpty());
  }
}
