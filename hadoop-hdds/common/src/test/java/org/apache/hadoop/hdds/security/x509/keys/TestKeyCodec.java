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

package org.apache.hadoop.hdds.security.x509.keys;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.hdds.security.SecurityConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test class for HDDS pem writer.
 */
public class TestKeyCodec {

  @Test
  public void unkownEncodingThrows() {
    assertThrows(NoSuchAlgorithmException.class, () -> new KeyCodec("Unknown"));
    assertThrows(NoSuchAlgorithmException.class, () -> new KeyCodec(""));
    assertThrows(NullPointerException.class, () -> new KeyCodec(null));
  }

  @ParameterizedTest
  @ValueSource(strings = {"RSA", "DSA", "RSASSA-PSS", "EC", "XDH", "X25519", "X448", "DiffieHellman"})
  public void testEncodeDecodePublicKey(String algorithm) throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance(algorithm);
    KeyPair keys = generator.generateKeyPair();

    KeyCodec codec = new KeyCodec(algorithm);
    byte[] encodedKey = codec.encodePublicKey(keys.getPublic());
    assertTrue(encodedKey.length > 0);
    String pemStr = new String(encodedKey, StandardCharsets.UTF_8);
    assertThat(pemStr.trim()).startsWith(SecurityConstants.PEM_PRE_ENCAPSULATION_BOUNDARY_PUBLIC_KEY);
    assertThat(pemStr.trim()).endsWith(SecurityConstants.PEM_POST_ENCAPSULATION_BOUNDARY_PUBLIC_KEY);
    assertEquals(keys.getPublic(), codec.decodePublicKey(encodedKey));
  }

  @ParameterizedTest
  @ValueSource(strings = {"RSA", "DSA", "RSASSA-PSS", "EC", "XDH", "X25519", "X448", "DiffieHellman"})
  public void testEncodeDecodePrivateKey(String algorithm) throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance(algorithm);
    KeyPair keys = generator.generateKeyPair();

    KeyCodec codec = new KeyCodec(algorithm);
    byte[] encodedKey = codec.encodePrivateKey(keys.getPrivate());
    assertTrue(encodedKey.length > 0);
    String pemStr = new String(encodedKey, StandardCharsets.UTF_8);
    assertThat(pemStr.trim()).startsWith(SecurityConstants.PEM_PRE_ENCAPSULATION_BOUNDARY_PRIVATE_KEY);
    assertThat(pemStr.trim()).endsWith(SecurityConstants.PEM_POST_ENCAPSULATION_BOUNDARY_PRIVATE_KEY);
    assertEquals(keys.getPrivate(), codec.decodePrivateKey(encodedKey));
  }
}
