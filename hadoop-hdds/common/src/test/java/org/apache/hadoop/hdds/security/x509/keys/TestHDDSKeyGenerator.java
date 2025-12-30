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

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for HDDS Key Generator.
 */
public class TestHDDSKeyGenerator {
  private SecurityConfig config;
  @TempDir
  private File tempPath;

  @BeforeEach
  public void init() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS,  tempPath.getPath());
    config = new SecurityConfig(conf);
  }

  /**
   * In this test we verify that we are able to create a key pair, then get
   * bytes of that and use ASN1. parser to parse it back to a private key.
   * @throws NoSuchProviderException - On Error, due to missing Java
   * dependencies.
   * @throws NoSuchAlgorithmException - On Error,  due to missing Java
   * dependencies.
   */
  @Test
  public void testGenerateKey()
      throws NoSuchProviderException, NoSuchAlgorithmException {
    HDDSKeyGenerator keyGen = new HDDSKeyGenerator(config);
    KeyPair keyPair = keyGen.generateKey();
    assertEquals(config.getKeyAlgo(), keyPair.getPrivate().getAlgorithm());
    PKCS8EncodedKeySpec keySpec =
        new PKCS8EncodedKeySpec(keyPair.getPrivate().getEncoded());
    assertEquals("PKCS#8", keySpec.getFormat());
  }

  /**
   * In this test we assert that size that we specified is used for Key
   * generation.
   * @throws NoSuchProviderException - On Error, due to missing Java
   * dependencies.
   * @throws NoSuchAlgorithmException - On Error,  due to missing Java
   * dependencies.
   */
  @Test
  public void testGenerateKeyWithSize() throws NoSuchProviderException,
      NoSuchAlgorithmException {
    HDDSKeyGenerator keyGen = new HDDSKeyGenerator(config);
    KeyPair keyPair = keyGen.generateKey(4096);
    PublicKey publicKey = keyPair.getPublic();
    if (publicKey instanceof RSAPublicKey) {
      assertEquals(4096, ((RSAPublicKey)(publicKey)).getModulus().bitLength());
    }
  }
}
