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
import static org.apache.hadoop.ozone.om.security.ManagedS3AccessKeySecretEnvelopeCodec.AAD_CONTEXT_VERSION;
import static org.apache.hadoop.ozone.om.security.ManagedS3AccessKeySecretEnvelopeCodec.ALGORITHM;
import static org.apache.hadoop.ozone.om.security.ManagedS3AccessKeySecretEnvelopeCodec.ENVELOPE_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.ozone.om.security.ManagedS3AccessKeySecretEnvelopeCodec.EncryptedSecret;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ManagedS3AccessKeySecretEnvelope;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Tests for {@link ManagedS3AccessKeySecretEnvelopeCodec}.
 */
public class TestManagedS3AccessKeySecretEnvelopeCodec {

  private static final String ACCESS_KEY_ID = "managed-access-key";
  private static final String EFFECTIVE_USER = "alice";
  private static final long CREATED_AT = 123456789L;
  private static final String KEY_NAME = "ozone-s3-managed-access-keys";
  private static final String KEY_VERSION_NAME =
      "ozone-s3-managed-access-keys@0";
  private static final byte[] SECRET =
      "plain-secret-sentinel-1234567890".getBytes(UTF_8);
  private static final byte[] PLAIN_DEK =
      "plain-dek-sentinel-1234567890123".getBytes(UTF_8);
  private static final byte[] EDEK =
      "wrapped-edek-sentinel".getBytes(UTF_8);
  private static final byte[] EDEK_IV =
      "edek-iv-sentinel".getBytes(UTF_8);
  private static final byte[] DATA_IV =
      "123456789012".getBytes(UTF_8);

  @Test
  public void encryptDecryptRoundTripAfterRestart() throws Exception {
    EncryptedSecret encrypted = encrypt(providerReturningDek(PLAIN_DEK));

    byte[] decrypted = ManagedS3AccessKeySecretEnvelopeCodec.decrypt(
        ACCESS_KEY_ID, EFFECTIVE_USER, CREATED_AT, encrypted.getEnvelope(),
        providerReturningDek(PLAIN_DEK));

    assertArrayEquals(SECRET, decrypted);
    assertThat(encrypted.getSecretKeyId()).isEqualTo(KEY_VERSION_NAME);
    assertThat(encrypted.toString()).contains("envelope=<redacted>");
  }

  @Test
  public void encryptCallsGenerateEncryptedKeyBeforeDecryptEncryptedKey()
      throws Exception {
    KeyProviderCryptoExtension provider = providerReturningDek(PLAIN_DEK);

    encrypt(provider);

    InOrder inOrder = inOrder(provider);
    inOrder.verify(provider).generateEncryptedKey(eq(KEY_NAME));
    inOrder.verify(provider).decryptEncryptedKey(any());
  }

  @Test
  public void envelopeDoesNotContainPlaintextSecretOrPlaintextDek()
      throws Exception {
    byte[] dek = PLAIN_DEK.clone();
    EncryptedSecret encrypted = encrypt(providerReturningDek(dek));
    byte[] envelope = encrypted.getEnvelope().toByteArray();

    assertThat(contains(envelope, SECRET)).isFalse();
    assertThat(contains(envelope, dek)).isFalse();
  }

  @Test
  public void encryptClearsPlaintextDekOnSuccess() throws Exception {
    byte[] dek = PLAIN_DEK.clone();
    KeyProviderCryptoExtension provider = providerReturningSameDek(dek);

    encrypt(provider);

    assertThat(dek).containsOnly((byte) 0);
  }

  @Test
  public void encryptClearsPlaintextDekOnException() throws Exception {
    byte[] dek = PLAIN_DEK.clone();
    KeyProviderCryptoExtension provider = providerReturningSameDek(dek);
    SecureRandom random = mock(SecureRandom.class);
    doAnswer(invocation -> {
      throw new RuntimeException("rng failure");
    }).when(random).nextBytes(any(byte[].class));

    assertThrows(IOException.class,
        () -> ManagedS3AccessKeySecretEnvelopeCodec.encrypt(
            ACCESS_KEY_ID, EFFECTIVE_USER, CREATED_AT, KEY_NAME,
            SECRET.clone(), provider, random));
    assertThat(dek).containsOnly((byte) 0);
  }

  @Test
  public void encryptFailsClosedWhenGenerateEncryptedKeyFails()
      throws Exception {
    KeyProviderCryptoExtension provider =
        mock(KeyProviderCryptoExtension.class);
    when(provider.generateEncryptedKey(KEY_NAME))
        .thenThrow(new GeneralSecurityException("denied"));

    IOException exception = assertThrows(IOException.class,
        () -> encrypt(provider));

    assertThat(exception).hasMessageContaining("Failed to encrypt");
  }

  @Test
  public void encryptFailsClosedWhenDecryptEncryptedKeyFails()
      throws Exception {
    KeyProviderCryptoExtension provider =
        mock(KeyProviderCryptoExtension.class);
    when(provider.generateEncryptedKey(KEY_NAME)).thenReturn(edek());
    when(provider.decryptEncryptedKey(any()))
        .thenThrow(new GeneralSecurityException("denied"));

    IOException exception = assertThrows(IOException.class,
        () -> encrypt(provider));

    assertThat(exception).hasMessageContaining("Failed to encrypt");
  }

  @Test
  public void malformedEnvelopeFailsClosed() {
    assertThrows(IOException.class,
        () -> ManagedS3AccessKeySecretEnvelopeCodec.decrypt(
            ACCESS_KEY_ID, EFFECTIVE_USER, CREATED_AT,
            ByteString.copyFromUtf8("not-a-valid-envelope"),
            providerReturningDek(PLAIN_DEK)));
  }

  @Test
  public void missingEdekFailsClosed() {
    ManagedS3AccessKeySecretEnvelope envelope = envelopeBuilder()
        .clearEncryptedDataKey()
        .buildPartial();

    assertThrows(IOException.class,
        () -> ManagedS3AccessKeySecretEnvelopeCodec.decrypt(
            ACCESS_KEY_ID, EFFECTIVE_USER, CREATED_AT,
            envelope.toByteString(), providerReturningDek(PLAIN_DEK)));
  }

  @Test
  public void missingKeyVersionFailsClosed() {
    ManagedS3AccessKeySecretEnvelope envelope = envelopeBuilder()
        .clearKeyVersionName()
        .buildPartial();

    assertThrows(IOException.class,
        () -> ManagedS3AccessKeySecretEnvelopeCodec.decrypt(
            ACCESS_KEY_ID, EFFECTIVE_USER, CREATED_AT,
            envelope.toByteString(), providerReturningDek(PLAIN_DEK)));
  }

  @Test
  public void unsupportedEnvelopeVersionFailsClosed() {
    ManagedS3AccessKeySecretEnvelope envelope = envelopeBuilder()
        .setEnvelopeVersion(ENVELOPE_VERSION + 1)
        .build();

    assertThrows(IOException.class,
        () -> ManagedS3AccessKeySecretEnvelopeCodec.decrypt(
            ACCESS_KEY_ID, EFFECTIVE_USER, CREATED_AT,
            envelope.toByteString(), providerReturningDek(PLAIN_DEK)));
  }

  @Test
  public void tamperedCiphertextFailsClosed() throws Exception {
    EncryptedSecret encrypted = encrypt(providerReturningDek(PLAIN_DEK));
    ManagedS3AccessKeySecretEnvelope envelope =
        ManagedS3AccessKeySecretEnvelope.parseFrom(encrypted.getEnvelope());
    byte[] tamperedCiphertext = envelope.getCiphertext().toByteArray();
    tamperedCiphertext[tamperedCiphertext.length - 1] ^= 1;
    ByteString tampered = envelope.toBuilder()
        .setCiphertext(ByteString.copyFrom(tamperedCiphertext))
        .build()
        .toByteString();

    assertThrows(IOException.class,
        () -> ManagedS3AccessKeySecretEnvelopeCodec.decrypt(
            ACCESS_KEY_ID, EFFECTIVE_USER, CREATED_AT, tampered,
            providerReturningDek(PLAIN_DEK)));
  }

  @Test
  public void aadBindsImmutableMetadata() throws Exception {
    EncryptedSecret encrypted = encrypt(providerReturningDek(PLAIN_DEK));

    assertThrows(IOException.class,
        () -> ManagedS3AccessKeySecretEnvelopeCodec.decrypt(
            ACCESS_KEY_ID, "bob", CREATED_AT, encrypted.getEnvelope(),
            providerReturningDek(PLAIN_DEK)));
  }

  private static EncryptedSecret encrypt(KeyProviderCryptoExtension provider)
      throws IOException {
    return ManagedS3AccessKeySecretEnvelopeCodec.encrypt(
        ACCESS_KEY_ID, EFFECTIVE_USER, CREATED_AT, KEY_NAME, SECRET.clone(),
        provider, fixedRandom());
  }

  private static KeyProviderCryptoExtension providerReturningDek(byte[] dek)
      throws IOException, GeneralSecurityException {
    KeyProviderCryptoExtension provider =
        mock(KeyProviderCryptoExtension.class);
    when(provider.generateEncryptedKey(KEY_NAME)).thenReturn(edek());
    when(provider.decryptEncryptedKey(any())).thenAnswer(invocation ->
        keyVersion(dek.clone()));
    return provider;
  }

  private static KeyProviderCryptoExtension providerReturningSameDek(byte[] dek)
      throws IOException, GeneralSecurityException {
    KeyProviderCryptoExtension provider =
        mock(KeyProviderCryptoExtension.class);
    KeyProvider.KeyVersion keyVersion = keyVersion(dek);
    when(provider.generateEncryptedKey(KEY_NAME)).thenReturn(edek());
    when(provider.decryptEncryptedKey(any())).thenReturn(keyVersion);
    return provider;
  }

  private static KeyProvider.KeyVersion keyVersion(byte[] material) {
    KeyProvider.KeyVersion keyVersion = mock(KeyProvider.KeyVersion.class);
    when(keyVersion.getMaterial()).thenReturn(material);
    return keyVersion;
  }

  private static EncryptedKeyVersion edek() {
    return EncryptedKeyVersion.createForDecryption(KEY_NAME, KEY_VERSION_NAME,
        EDEK_IV.clone(), EDEK.clone());
  }

  private static ManagedS3AccessKeySecretEnvelope.Builder envelopeBuilder() {
    return ManagedS3AccessKeySecretEnvelope.newBuilder()
        .setEnvelopeVersion(ENVELOPE_VERSION)
        .setAlgorithm(ALGORITHM)
        .setKeyName(KEY_NAME)
        .setKeyVersionName(KEY_VERSION_NAME)
        .setEncryptedDataKey(ByteString.copyFrom(EDEK))
        .setEdekIv(ByteString.copyFrom(EDEK_IV))
        .setDataIv(ByteString.copyFrom(DATA_IV))
        .setCiphertext(ByteString.copyFromUtf8("ciphertext"))
        .setAadContextVersion(AAD_CONTEXT_VERSION)
        .setCreatedAt(CREATED_AT);
  }

  private static SecureRandom fixedRandom() {
    SecureRandom random = mock(SecureRandom.class);
    doAnswer(invocation -> {
      byte[] target = invocation.getArgument(0);
      System.arraycopy(DATA_IV, 0, target, 0, target.length);
      return null;
    }).when(random).nextBytes(any(byte[].class));
    return random;
  }

  private static boolean contains(byte[] haystack, byte[] needle) {
    for (int i = 0; i <= haystack.length - needle.length; i++) {
      if (Arrays.equals(Arrays.copyOfRange(haystack, i, i + needle.length),
          needle)) {
        return true;
      }
    }
    return false;
  }
}
