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

// We used UTF-8 before, but a PEM file do contain only readable characters that are in the US_ASCII character set,
// and UTF-8 is interoperable with US_ASCII in this case.
// Based on general considerations of RFC-7468 , we stick to the US_ASCII charset for encoding and decoding.
// See: (https://datatracker.ietf.org/doc/html/rfc7468#section-2)
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.apache.hadoop.hdds.security.SecurityConstants.PEM_ENCAPSULATION_BOUNDARY_LABEL_PRIVATE_KEY;
import static org.apache.hadoop.hdds.security.SecurityConstants.PEM_ENCAPSULATION_BOUNDARY_LABEL_PUBLIC_KEY;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import org.apache.ratis.util.function.CheckedFunction;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.bouncycastle.util.io.pem.PemWriter;

/**
 * KeyCodec for encoding and decoding private and public keys.
 */
public class KeyCodec {
  private static final int BUFFER_LEN = 8192;

  private final KeyFactory keyFactory;

  /**
   * Creates a KeyCodec based on the security configuration.
   *
   * @param keyAlgorithm the key algorithm to use.
   * @throws NoSuchAlgorithmException if the key algorithm specified in the configuration is not available.
   *
   * @see <A href="https://docs.oracle.com/en/java/javase/11/docs/specs/security/standard-names.html#keypairgenerator-algorithms">
   *   Java Security Standard Algorithm Names</A>
   */
  public KeyCodec(String keyAlgorithm) throws NoSuchAlgorithmException {
    keyFactory = KeyFactory.getInstance(keyAlgorithm);
  }

  /**
   * Encodes the given public key to PEM format.
   * @param key the key to encode.
   * @return the PEM encoded key.
   * @throws IOException if the encoding fails.
   */
  public byte[] encodePublicKey(PublicKey key) throws IOException {
    return encodeKey(PEM_ENCAPSULATION_BOUNDARY_LABEL_PUBLIC_KEY, key);
  }

  /**
   * Encodes the given private key to PEM format.
   * @param key the key to encode.
   * @return the PEM encoded key.
   * @throws IOException if the encoding fails.
   */
  public byte[] encodePrivateKey(PrivateKey key) throws IOException {
    return encodeKey(PEM_ENCAPSULATION_BOUNDARY_LABEL_PRIVATE_KEY, key);
  }

  /**
   * Decodes a {@link PrivateKey} from PEM encoded format.
   * @param encodedKey the PEM encoded key as byte[].
   * @return a {@link PrivateKey} instance representing the key in the PEM data.
   * @throws IOException if the decoding fails.
   */
  public PrivateKey decodePrivateKey(byte[] encodedKey) throws IOException {
    return decodeKey(encodedKey, keyFactory::generatePrivate);
  }

  /**
   * Decodes a {@link PublicKey} from PEM encoded format.
   * @param encodedKey the PEM encoded key as byte[].
   * @return a {@link PublicKey} instance representing the key in the PEM data.
   * @throws IOException if the decoding fails.
   */
  public PublicKey decodePublicKey(byte[] encodedKey) throws IOException {
    return decodeKey(encodedKey, ks -> keyFactory.generatePublic(new X509EncodedKeySpec(ks.getEncoded())));
  }

  private byte[] encodeKey(String keyType, Key key) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(BUFFER_LEN);
    try (OutputStreamWriter w = new OutputStreamWriter(bytes, US_ASCII); PemWriter pemWriter = new PemWriter(w)) {
      pemWriter.writeObject(new PemObject(keyType, key.getEncoded()));
      pemWriter.flush();
      return bytes.toByteArray();
    }
  }

  private <T> T decodeKey(byte[] encodedKey, CheckedFunction<PKCS8EncodedKeySpec, T, InvalidKeySpecException> generator)
      throws IOException {
    try (PemReader pemReader = new PemReader(new InputStreamReader(new ByteArrayInputStream(encodedKey), US_ASCII))) {
      PemObject keyObject = pemReader.readPemObject();
      PKCS8EncodedKeySpec pkcs8EncodedKeySpec = new PKCS8EncodedKeySpec(keyObject.getContent());
      return generator.apply(pkcs8EncodedKeySpec);
    } catch (InvalidKeySpecException e) {
      throw new IOException(e);
    }
  }
}
