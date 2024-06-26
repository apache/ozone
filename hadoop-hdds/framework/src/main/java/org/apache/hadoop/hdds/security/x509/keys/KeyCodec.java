/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.security.x509.keys;

import org.apache.hadoop.hdds.security.SecurityConfig;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.bouncycastle.util.io.pem.PemWriter;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

/**
 * KeyCodec for encoding and decoding private and public keys.
 */
public class KeyCodec {
  public static final String PRIVATE_KEY = "PRIVATE KEY";
  public static final String PUBLIC_KEY = "PUBLIC KEY";
  private final SecurityConfig securityConfig;

  public KeyCodec(SecurityConfig config) {
    this.securityConfig = config;
  }

  public String encodePublicKey(PublicKey key) throws IOException {
    return encodeKey(PUBLIC_KEY, key);
  }

  public String encodePrivateKey(PrivateKey key) throws IOException {
    return encodeKey(PRIVATE_KEY, key);
  }

  private String encodeKey(String keyType, Key key) throws IOException {
    try (StringWriter sw = new StringWriter(); PemWriter publicKeyWriter = new PemWriter(sw)) {
      publicKeyWriter.writeObject(new PemObject(keyType, key.getEncoded()));
      publicKeyWriter.flush();
      return sw.toString();
    }
  }

  public PrivateKey decodePrivateKey(String encodedKey) throws IOException, NoSuchAlgorithmException,
      InvalidKeySpecException {
    try (PemReader pemReader = new PemReader(new StringReader(encodedKey))) {
      PemObject keyObject = pemReader.readPemObject();
      PKCS8EncodedKeySpec pkcs8EncodedKeySpec = new PKCS8EncodedKeySpec(keyObject.getContent());
      return getKeyFactory().generatePrivate(pkcs8EncodedKeySpec);
    }
  }

  public PublicKey decodePublicKey(String encodedKey) throws IOException, NoSuchAlgorithmException,
      InvalidKeySpecException {
    try (PemReader pemReader = new PemReader(new StringReader(encodedKey))) {
      PemObject keyObject = pemReader.readPemObject();
      PKCS8EncodedKeySpec pkcs8EncodedKeySpec = new PKCS8EncodedKeySpec(keyObject.getContent());
      return getKeyFactory().generatePublic(new X509EncodedKeySpec(pkcs8EncodedKeySpec.getEncoded()));
    }
  }

  private KeyFactory getKeyFactory() throws NoSuchAlgorithmException {
    return KeyFactory.getInstance(securityConfig.getKeyAlgo());
  }
}
