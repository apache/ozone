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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.STSTokenEncryption.STSTokenEncryptionException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test class for STS token encryption functionality.
 */
public class TestSTSTokenEncryption {

  // These must match the constants in STSTokenEncryption.
  private static final int HKDF_SALT_LENGTH = 16; // 128 bits

  private static SecretKey sharedSecretKey;

  @BeforeAll
  public static void setUpClass() {
    final byte[] keyBytes = "01234567890123456789012345678901".getBytes(StandardCharsets.US_ASCII);
    sharedSecretKey = new SecretKeySpec(keyBytes, "HmacSHA256");
  }

  @Test
  public void testEncryptDecryptRoundTrip() throws Exception {
    final byte[] keyBytes = sharedSecretKey.getEncoded();

    final String originalSecret = "mySecretAccessKey123456";
    final byte[] aad = "test-aad".getBytes(StandardCharsets.UTF_8);
    
    // Encrypt the secret
    final String encrypted = STSTokenEncryption.encrypt(originalSecret, keyBytes, aad);
    assertNotNull(encrypted);
    assertNotEquals(originalSecret, encrypted);
    
    // Decrypt the secret
    final String decrypted = STSTokenEncryption.decrypt(encrypted, keyBytes, aad);
    assertEquals(originalSecret, decrypted);
  }
  
  @Test
  public void testSTSTokenIdentifierEncryption() throws Exception {
    final byte[] keyBytes = sharedSecretKey.getEncoded();

    final String tempAccessKeyId = "ASIA123TEMPKEY";
    final String originalAccessKeyId = "AKIA123ORIGINAL";
    final String roleArn = "arn:aws:iam::123456789012:role/TestRole";
    final String secretAccessKey = "mySecretAccessKey123456";
    // Use millisecond precision to match serialization format
    final Instant expiry = Instant.ofEpochMilli(Instant.now().plusSeconds(3600).toEpochMilli());
    final String sessionPolicy = "test-session-policy";
    
    // Create token identifier with encryption
    final STSTokenIdentifier tokenId = new STSTokenIdentifier(
        tempAccessKeyId, originalAccessKeyId, roleArn, expiry, secretAccessKey, sessionPolicy, keyBytes);
    tokenId.setSecretKeyId(UUID.randomUUID());
    
    // Convert to protobuf
    final OzoneManagerProtocolProtos.OMTokenProto omTokenProto = tokenId.toProtoBuf();
    assertNotEquals(secretAccessKey, omTokenProto.getSecretAccessKey()); // ensure secretAccessKey is encrypted
    final byte[] protobufBytes = omTokenProto.toByteArray();
    
    // Create new token identifier from protobuf with decryption key
    final STSTokenIdentifier decodedTokenId = new STSTokenIdentifier();
    decodedTokenId.setEncryptionKey(keyBytes);
    decodedTokenId.readFromByteArray(protobufBytes);
    
    // Verify all fields are correctly decrypted
    assertEquals(tempAccessKeyId, decodedTokenId.getTempAccessKeyId());
    assertEquals(originalAccessKeyId, decodedTokenId.getOriginalAccessKeyId());
    assertEquals(roleArn, decodedTokenId.getRoleArn());
    assertEquals(secretAccessKey, decodedTokenId.getSecretAccessKey());
    assertEquals(expiry, decodedTokenId.getExpiry());
  }
  
  @Test
  public void testDecryptionWithWrongKey() throws Exception {
    // Generate two different keys
    final KeyGenerator keyGen = KeyGenerator.getInstance("HmacSHA256");
    keyGen.init(256);
    final SecretKey key1 = keyGen.generateKey();
    final SecretKey key2 = keyGen.generateKey();

    final String originalSecret = "mySecretAccessKey123456";
    final byte[] aad = "key-aad".getBytes(StandardCharsets.UTF_8);
    
    // Encrypt with key1
    final String encrypted = STSTokenEncryption.encrypt(originalSecret, key1.getEncoded(), aad);

    // Try to decrypt with key2 - should fail
    assertThrows(
        STSTokenEncryptionException.class, () -> STSTokenEncryption.decrypt(encrypted, key2.getEncoded(), aad));
  }

  @Test
  public void testDecryptionFailsWhenCiphertextIsCorrupted() throws Exception {
    final byte[] keyBytes = sharedSecretKey.getEncoded();

    final String originalSecret = "mySecretAccessKey123456";
    final byte[] aad = "ciphertext-aad".getBytes(StandardCharsets.UTF_8);

    // Encrypt the secret
    final String encrypted = STSTokenEncryption.encrypt(originalSecret, keyBytes, aad);
    final byte[] data = Base64.getDecoder().decode(encrypted);

    // Corrupt the last byte of the ciphertext segment
    data[data.length - 1] ^= 0x01;

    final String tampered = Base64.getEncoder().encodeToString(data);

    // Decryption must fail with corrupted ciphertext
    assertThrows(
        STSTokenEncryptionException.class,
        () -> STSTokenEncryption.decrypt(tampered, keyBytes, aad));
  }

  @Test
  public void testDecryptionFailsWhenSaltIsCorrupted() throws Exception {
    final byte[] keyBytes = sharedSecretKey.getEncoded();

    final String originalSecret = "mySecretAccessKey123456";
    final byte[] aad = "salt-aad".getBytes(StandardCharsets.UTF_8);

    // Encrypt the secret
    final String encrypted = STSTokenEncryption.encrypt(originalSecret, keyBytes, aad);
    final byte[] data = Base64.getDecoder().decode(encrypted);

    // Corrupt the first byte of the salt segment
    data[0] ^= 0x01;

    final String tampered = Base64.getEncoder().encodeToString(data);

    // Decryption must fail with corrupted salt (derives wrong AES key)
    assertThrows(STSTokenEncryptionException.class, () -> STSTokenEncryption.decrypt(tampered, keyBytes, aad));
  }

  @Test
  public void testDecryptionFailsWhenIvIsCorrupted() throws Exception {
    final byte[] keyBytes = sharedSecretKey.getEncoded();

    final String originalSecret = "mySecretAccessKey123456";
    final byte[] aad = "iv-aad".getBytes(StandardCharsets.UTF_8);

    // Encrypt the secret
    final String encrypted = STSTokenEncryption.encrypt(originalSecret, keyBytes, aad);
    final byte[] data = Base64.getDecoder().decode(encrypted);

    // Corrupt the first byte of the IV segment
    data[HKDF_SALT_LENGTH] ^= 0x01;

    final String tampered = Base64.getEncoder().encodeToString(data);

    // Decryption must fail with corrupted IV
    assertThrows(STSTokenEncryptionException.class, () -> STSTokenEncryption.decrypt(tampered, keyBytes, aad));
  }

  @Test
  public void testDecryptionFailsWhenAadIsModified() throws Exception {
    final byte[] keyBytes = sharedSecretKey.getEncoded();

    final String originalSecret = "mySecretAccessKey123456";
    final byte[] aadOriginal = "aad-original".getBytes(StandardCharsets.UTF_8);
    final byte[] aadModified = "aad-modified".getBytes(StandardCharsets.UTF_8);

    // Encrypt with original AAD
    final String encrypted = STSTokenEncryption.encrypt(originalSecret, keyBytes, aadOriginal);

    // Decrypt with modified AAD - authentication must fail
    assertThrows(STSTokenEncryptionException.class, () -> STSTokenEncryption.decrypt(encrypted, keyBytes, aadModified));
  }
}
