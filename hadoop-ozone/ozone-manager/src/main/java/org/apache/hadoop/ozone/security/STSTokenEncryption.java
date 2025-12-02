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
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.generators.HKDFBytesGenerator;
import org.bouncycastle.crypto.params.HKDFParameters;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

/**
 * Utility class for encrypting and decrypting sensitive data in STS tokens.
 * Uses HKDF to derive an AES encryption key from the SCM ManagedSecretKey,
 * then uses AES-GCM for authenticated encryption.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class STSTokenEncryption {
  
  // HKDF parameters
  private static final byte[] HKDF_INFO = "STS-TOKEN-ENCRYPTION".getBytes(StandardCharsets.UTF_8);
  private static final int HKDF_SALT_LENGTH = 16; // 128 bits
  private static final int AES_KEY_LENGTH = 32; // 256 bits
  
  // AES-GCM parameters
  private static final int GCM_IV_LENGTH = 12; // 96 bits
  private static final int GCM_AUTHENTICATION_TAG_LENGTH_IN_BITS = 128;
  private static final String AES_ALGORITHM = "AES";
  private static final String AES_CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";
  
  private static final SecureRandom SECURE_RANDOM;
  private static final BouncyCastleProvider BC_PROVIDER = new BouncyCastleProvider();
  
  private STSTokenEncryption() {
  }

  static {
    SecureRandom secureRandom;
    try {
      // Prefer non-blocking native PRNG where available
      secureRandom = SecureRandom.getInstance("NativePRNGNonBlocking");
    } catch (Exception e) {
      // Fallback to default SecureRandom implementation
      secureRandom = new SecureRandom();
    }
    SECURE_RANDOM = secureRandom;
  }

  /**
   * Encrypt sensitive data using AES-GCM with a key derived from the secret key via HKDF,
   * binding the provided AAD to the authentication tag.
   *
   * @param plaintext         the sensitive data to encrypt
   * @param secretKeyBytes    the secret key bytes from ManagedSecretKey
   * @param aad               additional authenticated data to bind
   * @return base64-encoded encrypted data with Salt and IV prepended
   * @throws STSTokenEncryptionException if encryption fails
   */
  public static String encrypt(String plaintext, byte[] secretKeyBytes, byte[] aad) throws STSTokenEncryptionException {
    Preconditions.checkArgument(
        secretKeyBytes != null && secretKeyBytes.length > 0, "The secretKeyBytes must not be null nor empty");
    Preconditions.checkArgument(aad != null && aad.length > 0, "The aad must not be null nor empty");
    // Don't encrypt null/empty strings
    if (plaintext == null || plaintext.isEmpty()) {
      return plaintext;
    }
    
    byte[] aesKey;
    byte[] iv;
    byte[] salt;
    try {
      // Generate random salt
      salt = new byte[HKDF_SALT_LENGTH];
      SECURE_RANDOM.nextBytes(salt);

      // Derive AES key using HKDF with random salt
      aesKey = deriveKey(secretKeyBytes, salt);
      
      // Generate random IV
      iv = new byte[GCM_IV_LENGTH];
      SECURE_RANDOM.nextBytes(iv);

      // Initialize AES-GCM cipher
      final Cipher cipher = Cipher.getInstance(AES_CIPHER_TRANSFORMATION, BC_PROVIDER);
      final GCMParameterSpec spec = new GCMParameterSpec(GCM_AUTHENTICATION_TAG_LENGTH_IN_BITS, iv);
      cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(aesKey, AES_ALGORITHM), spec);
      cipher.updateAAD(aad);
      
      // Encrypt the plaintext
      final byte[] ciphertext = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

      // Combine salt, IV and ciphertext
      final byte[] result = org.bouncycastle.util.Arrays.concatenate(salt, iv, ciphertext);

      return Base64.getEncoder().encodeToString(result);
    } catch (Exception e) {
      throw new STSTokenEncryptionException("Failed to encrypt sensitive data", e);
    }
  }

  /**
   * Decrypt sensitive data using AES-GCM with a key derived from the secret key via HKDF,
   * verifying the provided AAD bound to the authentication tag.
   *
   * @param encryptedData         base64-encoded encrypted data with Salt and IV prepended
   * @param secretKeyBytes        the secret key bytes from ManagedSecretKey
   * @param aad                   additional authenticated data to verify
   * @return decrypted plaintext
   * @throws STSTokenEncryptionException if decryption fails
   */
  public static String decrypt(String encryptedData, byte[] secretKeyBytes, byte[] aad)
      throws STSTokenEncryptionException {
    Preconditions.checkArgument(
        secretKeyBytes != null && secretKeyBytes.length > 0, "The secretKeyBytes must not be null nor empty");
    Preconditions.checkArgument(aad != null && aad.length > 0, "The aad must not be null nor empty");
    // Don't decrypt null/empty strings
    if (encryptedData == null || encryptedData.isEmpty()) {
      return encryptedData;
    }
    
    byte[] aesKey;
    try {
      // Decode base64
      final byte[] data = Base64.getDecoder().decode(encryptedData);
      
      if (data.length < HKDF_SALT_LENGTH + GCM_IV_LENGTH) {
        throw new STSTokenEncryptionException("Invalid encrypted data");
      }
      
      // Extract salt, IV and ciphertext
      final byte[] salt = new byte[HKDF_SALT_LENGTH];
      final byte[] iv = new byte[GCM_IV_LENGTH];
      final byte[] ciphertext = new byte[data.length - HKDF_SALT_LENGTH - GCM_IV_LENGTH];
      
      System.arraycopy(data, 0, salt, 0, HKDF_SALT_LENGTH);
      System.arraycopy(data, HKDF_SALT_LENGTH, iv, 0, GCM_IV_LENGTH);
      System.arraycopy(data, HKDF_SALT_LENGTH + GCM_IV_LENGTH, ciphertext, 0, ciphertext.length);
      
      // Derive AES key using HKDF with extracted salt
      aesKey = deriveKey(secretKeyBytes, salt);
      
      // Initialize AES-GCM cipher
      final Cipher cipher = Cipher.getInstance(AES_CIPHER_TRANSFORMATION, BC_PROVIDER);
      final GCMParameterSpec spec = new GCMParameterSpec(GCM_AUTHENTICATION_TAG_LENGTH_IN_BITS, iv);
      cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(aesKey, AES_ALGORITHM), spec);
      cipher.updateAAD(aad);
      
      // Decrypt the ciphertext
      final byte[] output = cipher.doFinal(ciphertext);
      
      return new String(output, StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new STSTokenEncryptionException("Failed to decrypt sensitive data", e);
    }
  }
  
  /**
   * Derive AES key using HKDF-SHA256.
   */
  private static byte[] deriveKey(byte[] secretKeyBytes, byte[] salt) {
    final HKDFBytesGenerator hkdf = new HKDFBytesGenerator(new SHA256Digest());
    hkdf.init(new HKDFParameters(secretKeyBytes, salt, HKDF_INFO));

    final byte[] aesKey = new byte[AES_KEY_LENGTH];
    hkdf.generateBytes(aesKey, 0, AES_KEY_LENGTH);
    return aesKey;
  }
  
  /**
   * Exception thrown when encryption/decryption operations fail.
   */
  public static class STSTokenEncryptionException extends Exception {
    public STSTokenEncryptionException(String message) {
      super(message);
    }
    
    public STSTokenEncryptionException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}

