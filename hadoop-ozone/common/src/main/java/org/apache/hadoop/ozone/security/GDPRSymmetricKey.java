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

import com.google.common.base.Preconditions;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.function.BiConsumer;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Symmetric Key structure for GDPR.
 */
public class GDPRSymmetricKey {
  private static final ThreadLocal<SecureRandom> RANDOM
      = ThreadLocal.withInitial(SecureRandom::new);

  private final SecretKeySpec secretKey;
  private final Cipher cipher;
  private final String algorithm;
  private final String secret;

  /** @return a new instance with default parameters. */
  public static GDPRSymmetricKey newDefaultInstance() {
    try {
      return new GDPRSymmetricKey(RANDOM.get());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create default "
          + GDPRSymmetricKey.class.getSimpleName(), e);
    }
  }

  static String randomSecret(SecureRandom secureRandom) {
    return RandomStringUtils.random(
        OzoneConsts.GDPR_DEFAULT_RANDOM_SECRET_LENGTH,
        0, 0, true, true, null, secureRandom);
  }

  public SecretKeySpec getSecretKey() {
    return secretKey;
  }

  public Cipher getCipher() {
    return cipher;
  }

  /**
   * Construct a key with default algorithm and a random secret.
   */
  public GDPRSymmetricKey(SecureRandom secureRandom)
      throws NoSuchPaddingException, NoSuchAlgorithmException {
    this(randomSecret(secureRandom), OzoneConsts.GDPR_ALGORITHM_NAME);
  }

  /**
   * Construct a key with the given secret and the given algorithm.
   */
  public GDPRSymmetricKey(String secret, String algorithm)
      throws NoSuchPaddingException, NoSuchAlgorithmException {
    Objects.requireNonNull(secret, "Secret cannot be null");
    //TODO: When we add feature to allow users to customize the secret length,
    // we need to update this length check Precondition
    Preconditions.checkArgument(secret.length() == 16,
        "Secret must be exactly 16 characters");
    Objects.requireNonNull(algorithm, "Algorithm cannot be null");
    this.secret = secret;
    this.algorithm = algorithm;
    this.secretKey = new SecretKeySpec(
        secret.getBytes(OzoneConsts.GDPR_CHARSET), algorithm);
    this.cipher = Cipher.getInstance(algorithm);
  }

  public void acceptKeyDetails(BiConsumer<String, String> consumer) {
    consumer.accept(OzoneConsts.GDPR_SECRET, this.secret);
    consumer.accept(OzoneConsts.GDPR_ALGORITHM, this.algorithm);
  }
}
