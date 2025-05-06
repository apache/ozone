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

import static java.time.Duration.ofDays;
import static java.time.Instant.now;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.EnumSet;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyTestUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link OzoneBlockTokenIdentifier}.
 */
public class TestOzoneBlockTokenIdentifier {
  private long expiryTime;
  private ManagedSecretKey secretKey;

  @BeforeEach
  public void setUp() throws Exception {
    expiryTime = Time.monotonicNow() + 60 * 60 * 24;

    secretKey = SecretKeyTestUtil.generateHmac(now(), ofDays(1));
  }

  @Test
  public void testSignToken() {
    OzoneBlockTokenIdentifier tokenId = new OzoneBlockTokenIdentifier(
        "testUser", "84940",
        EnumSet.allOf(AccessModeProto.class),
        expiryTime, 128L);
    tokenId.setSecretKeyId(secretKey.getId());
    byte[] signedToken = secretKey.sign(tokenId);

    // Verify a valid signed OzoneMaster Token with Ozone Master.
    assertTrue(secretKey.isValidSignature(tokenId.getBytes(), signedToken));

    // Verify an invalid signed OzoneMaster Token with Ozone Master.
    assertFalse(secretKey.isValidSignature(tokenId.getBytes(),
        RandomUtils.secure().randomBytes(128)));
  }

  @Test
  public void testTokenSerialization() throws
      IOException {
    long maxLength = 128L;

    OzoneBlockTokenIdentifier tokenId = new OzoneBlockTokenIdentifier(
        "testUser", "84940",
        EnumSet.allOf(AccessModeProto.class),
        expiryTime, maxLength);
    tokenId.setSecretKeyId(secretKey.getId());
    byte[] signedToken = secretKey.sign(tokenId);

    Token<OzoneBlockTokenIdentifier> token = new Token<>(tokenId.getBytes(),
        signedToken, tokenId.getKind(), new Text("host:port"));

    String encodeToUrlString = token.encodeToUrlString();

    Token<OzoneBlockTokenIdentifier>decodedToken = new Token<>();
    decodedToken.decodeFromUrlString(encodeToUrlString);

    OzoneBlockTokenIdentifier decodedTokenId = new OzoneBlockTokenIdentifier();
    decodedTokenId.readFields(new DataInputStream(
        new ByteArrayInputStream(decodedToken.getIdentifier())));

    assertEquals(tokenId, decodedTokenId);
    assertEquals(maxLength, decodedTokenId.getMaxLength());

    // Verify a decoded signed Token
    assertTrue(secretKey.isValidSignature(decodedTokenId,
        decodedToken.getPassword()));
  }
}
