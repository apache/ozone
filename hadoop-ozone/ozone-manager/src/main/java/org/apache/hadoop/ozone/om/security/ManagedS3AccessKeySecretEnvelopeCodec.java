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

package org.apache.hadoop.ozone.om.security;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ManagedS3AccessKeySecretEnvelope;
import org.apache.hadoop.security.SecurityUtil;

/**
 * Versioned envelope codec for OM-managed local S3 access-key secrets.
 */
public final class ManagedS3AccessKeySecretEnvelopeCodec {

  public static final int ENVELOPE_VERSION = 1;
  public static final int AAD_CONTEXT_VERSION = 1;
  public static final String ALGORITHM = "AES/GCM/NoPadding";

  private static final String PURPOSE = "ozone-managed-s3-access-key";
  private static final String AES = "AES";
  private static final int GCM_TAG_LENGTH_BITS = 128;
  private static final int GCM_IV_LENGTH_BYTES = 12;

  private ManagedS3AccessKeySecretEnvelopeCodec() {
  }

  public static EncryptedSecret encrypt(String accessKeyId,
      String effectiveUser, long createdAt, String keyName,
      byte[] plaintextSecret, KeyProviderCryptoExtension provider,
      SecureRandom random) throws IOException {
    requireNonEmpty(accessKeyId, "accessKeyId");
    requireNonEmpty(effectiveUser, "effectiveUser");
    requireNonEmpty(keyName, "keyName");
    requireBytes(plaintextSecret, "plaintextSecret");
    Objects.requireNonNull(provider, "provider == null");
    Objects.requireNonNull(random, "random == null");

    byte[] plaintextDek = null;
    try {
      EncryptedKeyVersion edek = generateEncryptedKey(provider, keyName);
      validateEdek(edek, keyName);
      String keyVersionName =
          requireNonEmpty(edek.getEncryptionKeyVersionName(),
              "keyVersionName");

      KeyProvider.KeyVersion dekVersion =
          decryptEncryptedKey(provider, edek);
      plaintextDek = requireBytes(dekVersion.getMaterial(), "plaintextDek");

      byte[] dataIv = new byte[GCM_IV_LENGTH_BYTES];
      random.nextBytes(dataIv);

      byte[] ciphertext = encryptSecret(plaintextSecret, plaintextDek, dataIv,
          aad(accessKeyId, effectiveUser, createdAt, keyName, keyVersionName));

      ManagedS3AccessKeySecretEnvelope envelope =
          ManagedS3AccessKeySecretEnvelope.newBuilder()
              .setEnvelopeVersion(ENVELOPE_VERSION)
              .setAlgorithm(ALGORITHM)
              .setKeyName(keyName)
              .setKeyVersionName(keyVersionName)
              .setEncryptedDataKey(
                  ByteString.copyFrom(requireBytes(
                      edek.getEncryptedKeyVersion().getMaterial(),
                      "encryptedDataKey")))
              .setEdekIv(ByteString.copyFrom(requireBytes(
                  edek.getEncryptedKeyIv(), "edekIv")))
              .setDataIv(ByteString.copyFrom(dataIv))
              .setCiphertext(ByteString.copyFrom(ciphertext))
              .setAadContextVersion(AAD_CONTEXT_VERSION)
              .setCreatedAt(createdAt)
              .build();
      return new EncryptedSecret(envelope.toByteString(), keyVersionName);
    } catch (GeneralSecurityException | RuntimeException e) {
      throw new IOException("Failed to encrypt managed S3 access key secret",
          e);
    } finally {
      clear(plaintextDek);
    }
  }

  public static byte[] decrypt(String accessKeyId, String effectiveUser,
      long createdAt, ByteString encryptedSecretKey,
      KeyProviderCryptoExtension provider) throws IOException {
    requireNonEmpty(accessKeyId, "accessKeyId");
    requireNonEmpty(effectiveUser, "effectiveUser");
    Objects.requireNonNull(encryptedSecretKey, "encryptedSecretKey == null");
    Objects.requireNonNull(provider, "provider == null");

    byte[] plaintextDek = null;
    try {
      ManagedS3AccessKeySecretEnvelope envelope =
          parseAndValidate(encryptedSecretKey);
      EncryptedKeyVersion edek = EncryptedKeyVersion.createForDecryption(
          envelope.getKeyName(), envelope.getKeyVersionName(),
          envelope.getEdekIv().toByteArray(),
          envelope.getEncryptedDataKey().toByteArray());
      KeyProvider.KeyVersion dekVersion =
          decryptEncryptedKey(provider, edek);
      plaintextDek = requireBytes(dekVersion.getMaterial(), "plaintextDek");

      return decryptSecret(envelope.getCiphertext().toByteArray(),
          plaintextDek, envelope.getDataIv().toByteArray(),
          aad(accessKeyId, effectiveUser, createdAt, envelope.getKeyName(),
              envelope.getKeyVersionName()));
    } catch (GeneralSecurityException | RuntimeException e) {
      throw new IOException("Failed to decrypt managed S3 access key secret",
          e);
    } finally {
      clear(plaintextDek);
    }
  }

  public static ManagedS3AccessKeySecretEnvelope parseAndValidate(
      ByteString encryptedSecretKey) throws IOException {
    ManagedS3AccessKeySecretEnvelope envelope;
    try {
      envelope = ManagedS3AccessKeySecretEnvelope.parseFrom(
          encryptedSecretKey);
    } catch (InvalidProtocolBufferException e) {
      throw new IOException("Malformed managed S3 access key secret envelope",
          e);
    }

    if (!envelope.isInitialized()) {
      throw new IOException("Malformed managed S3 access key secret envelope");
    }
    if (envelope.getEnvelopeVersion() != ENVELOPE_VERSION) {
      throw new IOException("Unsupported managed S3 access key secret " +
          "envelope version");
    }
    if (!ALGORITHM.equals(envelope.getAlgorithm())) {
      throw new IOException("Unsupported managed S3 access key secret " +
          "envelope algorithm");
    }
    if (envelope.getAadContextVersion() != AAD_CONTEXT_VERSION) {
      throw new IOException("Unsupported managed S3 access key secret AAD " +
          "context version");
    }
    requireNonEmpty(envelope.getKeyName(), "keyName");
    requireNonEmpty(envelope.getKeyVersionName(), "keyVersionName");
    requireBytes(envelope.getEncryptedDataKey().toByteArray(),
        "encryptedDataKey");
    requireBytes(envelope.getEdekIv().toByteArray(), "edekIv");
    requireBytes(envelope.getDataIv().toByteArray(), "dataIv");
    requireBytes(envelope.getCiphertext().toByteArray(), "ciphertext");
    return envelope;
  }

  public static void clear(byte[] secret) {
    if (secret != null) {
      Arrays.fill(secret, (byte) 0);
    }
  }

  @VisibleForTesting
  static byte[] aad(String accessKeyId, String effectiveUser, long createdAt,
      String keyName, String keyVersionName) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytes);
      writeField(out, "purpose", PURPOSE);
      writeField(out, "envelopeVersion",
          Integer.toString(ENVELOPE_VERSION));
      writeField(out, "aadContextVersion",
          Integer.toString(AAD_CONTEXT_VERSION));
      writeField(out, "accessKeyId", accessKeyId);
      writeField(out, "effectiveUser", effectiveUser);
      writeField(out, "createdAt", Long.toString(createdAt));
      writeField(out, "keyName", keyName);
      writeField(out, "keyVersionName", keyVersionName);
      out.flush();
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to build managed S3 access key " +
          "secret AAD", e);
    }
  }

  private static EncryptedKeyVersion generateEncryptedKey(
      KeyProviderCryptoExtension provider, String keyName) throws IOException,
      GeneralSecurityException {
    return SecurityUtil.doAsLoginUser(
        (PrivilegedExceptionAction<EncryptedKeyVersion>)
            () -> provider.generateEncryptedKey(keyName));
  }

  private static KeyProvider.KeyVersion decryptEncryptedKey(
      KeyProviderCryptoExtension provider, EncryptedKeyVersion edek)
      throws IOException, GeneralSecurityException {
    return SecurityUtil.doAsLoginUser(
        (PrivilegedExceptionAction<KeyProvider.KeyVersion>)
            () -> provider.decryptEncryptedKey(edek));
  }

  private static byte[] encryptSecret(byte[] plaintextSecret,
      byte[] plaintextDek, byte[] dataIv, byte[] aad)
      throws GeneralSecurityException {
    Cipher cipher = Cipher.getInstance(ALGORITHM);
    cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(plaintextDek, AES),
        new GCMParameterSpec(GCM_TAG_LENGTH_BITS, dataIv));
    cipher.updateAAD(aad);
    return cipher.doFinal(plaintextSecret);
  }

  private static byte[] decryptSecret(byte[] ciphertext, byte[] plaintextDek,
      byte[] dataIv, byte[] aad) throws GeneralSecurityException {
    Cipher cipher = Cipher.getInstance(ALGORITHM);
    cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(plaintextDek, AES),
        new GCMParameterSpec(GCM_TAG_LENGTH_BITS, dataIv));
    cipher.updateAAD(aad);
    return cipher.doFinal(ciphertext);
  }

  private static void validateEdek(EncryptedKeyVersion edek, String keyName)
      throws IOException {
    Objects.requireNonNull(edek, "edek == null");
    if (!keyName.equals(edek.getEncryptionKeyName())) {
      throw new IOException("Managed S3 access key EDEK key name mismatch");
    }
    requireNonEmpty(edek.getEncryptionKeyVersionName(), "keyVersionName");
    requireBytes(edek.getEncryptedKeyIv(), "edekIv");
    Objects.requireNonNull(edek.getEncryptedKeyVersion(),
        "encrypted key version == null");
    requireBytes(edek.getEncryptedKeyVersion().getMaterial(),
        "encryptedDataKey");
  }

  private static String requireNonEmpty(String value, String field)
      throws IOException {
    if (value == null || value.isEmpty()) {
      throw new IOException("Managed S3 access key secret envelope missing " +
          field);
    }
    return value;
  }

  private static byte[] requireBytes(byte[] value, String field)
      throws IOException {
    if (value == null || value.length == 0) {
      throw new IOException("Managed S3 access key secret envelope missing " +
          field);
    }
    return value;
  }

  private static void writeField(DataOutputStream out, String name,
      String value) throws IOException {
    byte[] nameBytes = name.getBytes(UTF_8);
    byte[] valueBytes = value.getBytes(UTF_8);
    out.writeInt(nameBytes.length);
    out.write(nameBytes);
    out.writeInt(valueBytes.length);
    out.write(valueBytes);
  }

  /**
   * Result of encrypting a managed S3 access-key secret.
   */
  public static final class EncryptedSecret {
    private final ByteString envelope;
    private final String secretKeyId;

    private EncryptedSecret(ByteString envelope, String secretKeyId) {
      this.envelope = Objects.requireNonNull(envelope);
      this.secretKeyId = Objects.requireNonNull(secretKeyId);
    }

    public ByteString getEnvelope() {
      return envelope;
    }

    public String getSecretKeyId() {
      return secretKeyId;
    }

    @Override
    public String toString() {
      return "EncryptedSecret{" +
          "envelope=<redacted>" +
          ", secretKeyId='" + secretKeyId + '\'' +
          '}';
    }
  }
}
