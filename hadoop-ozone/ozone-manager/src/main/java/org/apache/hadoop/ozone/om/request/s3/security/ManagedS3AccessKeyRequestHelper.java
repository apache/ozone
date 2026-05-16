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

package org.apache.hadoop.ozone.om.request.s3.security;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.MANAGED_S3_ACCESS_KEY_OPERATION_NOT_SUPPORTED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.UNAUTHORIZED;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.S3ManagedAccessKeyInfo;
import org.apache.hadoop.ozone.om.security.ManagedS3AccessKeySecretEnvelopeCodec;
import org.apache.hadoop.ozone.om.security.ManagedS3AccessKeySecretEnvelopeCodec.EncryptedSecret;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ManagedS3AccessKeyMetadataProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.ozone.security.ManagedS3AccessKeyConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;

/**
 * Shared validation and conversion helpers for managed local S3 access keys.
 */
public final class ManagedS3AccessKeyRequestHelper {

  static final int GENERATED_ACCESS_KEY_ID_BYTES = 16;
  static final int GENERATED_SECRET_BYTES = 40;
  static final String AUDIT_ACCESS_KEY_ID = "accessKeyId";
  static final String AUDIT_EFFECTIVE_USER = "effectiveUser";
  static final String AUDIT_CREATED_BY = "createdBy";
  static final String AUDIT_POLICY_SHA256 = "policySha256";
  static final String AUDIT_RETRIEVAL_HANDLE_HASH = "retrievalHandleHash";

  private static final SecureRandom SECURE_RANDOM = new SecureRandom();
  private static final byte[] SECRET_ALPHABET =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz/+"
          .getBytes(UTF_8);
  private static final String ACCESS_KEY_PREFIX = "O3MA";

  private ManagedS3AccessKeyRequestHelper() {
  }

  static void checkEnabled(OzoneManager ozoneManager) throws OMException {
    if (!ozoneManager.getManagedS3AccessKeyConfig().isEnabled()) {
      throw new OMException("Managed S3 access keys are disabled",
          MANAGED_S3_ACCESS_KEY_OPERATION_NOT_SUPPORTED);
    }
  }

  static void checkLayoutFinalized(OzoneManager ozoneManager)
      throws OMException {
    if (!ozoneManager.getVersionManager()
        .isAllowed(OMLayoutFeature.MANAGED_LOCAL_S3_ACCESS_KEYS)) {
      throw new OMException("Managed S3 access-key operations are not " +
          "allowed before layout finalization",
          NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
    }
  }

  static void checkAdmin(OzoneManager ozoneManager, UserInfo userInfo)
      throws IOException {
    if (!ozoneManager.isAdminAuthorizationEnabled()) {
      return;
    }
    String caller = caller(userInfo);
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(caller);
    if (!ozoneManager.isAdmin(ugi)) {
      throw new OMException("Only Ozone administrators may manage S3 " +
          "access keys", UNAUTHORIZED);
    }
  }

  static String caller(UserInfo userInfo) throws OMException {
    if (userInfo == null || StringUtils.isBlank(userInfo.getUserName())) {
      throw new OMException("Missing request user", UNAUTHORIZED);
    }
    return userInfo.getUserName();
  }

  static String required(String value, String field) throws OMException {
    if (StringUtils.isBlank(value)) {
      throw new OMException(field + " must not be empty", INVALID_REQUEST);
    }
    return value.trim();
  }

  static String optional(String value) {
    return value == null ? "" : value.trim();
  }

  static String requireNoPolicyDocument(String policyDocument)
      throws OMException {
    if (StringUtils.isNotBlank(policyDocument)) {
      throw new OMException("Managed S3 access-key policy documents are " +
          "not supported until local-json policy evaluation is implemented",
          MANAGED_S3_ACCESS_KEY_OPERATION_NOT_SUPPORTED);
    }
    return "";
  }

  static long effectiveExpiresAt(ManagedS3AccessKeyConfig config,
      long createdAt, long requestedExpiresAt) throws OMException {
    long defaultExpiresAt = createdAt + config.getDefaultLifetime().toMillis();
    long expiresAt = requestedExpiresAt > 0 ? requestedExpiresAt :
        defaultExpiresAt;
    long maxExpiresAt = createdAt + config.getMaxLifetime().toMillis();
    if (expiresAt <= createdAt) {
      throw new OMException("expiresAt must be in the future",
          INVALID_REQUEST);
    }
    if (expiresAt > maxExpiresAt) {
      throw new OMException("expiresAt exceeds managed S3 access-key max " +
          "lifetime", INVALID_REQUEST);
    }
    return expiresAt;
  }

  static long rotateExpiresAt(ManagedS3AccessKeyConfig config,
      S3ManagedAccessKeyInfo existing, long now, long requestedExpiresAt)
      throws OMException {
    if (requestedExpiresAt == 0) {
      return existing.getExpiresAt();
    }
    long maxExpiresAt = now + config.getMaxLifetime().toMillis();
    if (requestedExpiresAt <= now) {
      throw new OMException("expiresAt must be in the future",
          INVALID_REQUEST);
    }
    if (requestedExpiresAt > maxExpiresAt) {
      throw new OMException("expiresAt exceeds managed S3 access-key max " +
          "lifetime", INVALID_REQUEST);
    }
    return requestedExpiresAt;
  }

  static byte[] secretFromRequest(String customSecret,
      ManagedS3AccessKeyConfig config) throws OMException {
    if (StringUtils.isBlank(customSecret)) {
      byte[] secret = new byte[Math.max(GENERATED_SECRET_BYTES,
          config.getSecretMinLength())];
      for (int i = 0; i < secret.length; i++) {
        secret[i] = SECRET_ALPHABET[
            SECURE_RANDOM.nextInt(SECRET_ALPHABET.length)];
      }
      return secret;
    }
    if (!config.isAllowCustomSecret()) {
      throw new OMException("Custom managed S3 access-key secrets are not " +
          "enabled", MANAGED_S3_ACCESS_KEY_OPERATION_NOT_SUPPORTED);
    }
    byte[] secret = customSecret.getBytes(UTF_8);
    validateSecretLength(secret, config);
    return secret;
  }

  static void validateSecretLength(byte[] secret,
      ManagedS3AccessKeyConfig config) throws OMException {
    if (secret.length < config.getSecretMinLength()) {
      throw new OMException("Managed S3 access-key secret length must be at " +
          "least " + config.getSecretMinLength() + " bytes", INVALID_REQUEST);
    }
  }

  static String generatedAccessKeyId() {
    byte[] random = new byte[GENERATED_ACCESS_KEY_ID_BYTES];
    SECURE_RANDOM.nextBytes(random);
    return ACCESS_KEY_PREFIX +
        Base64.getUrlEncoder().withoutPadding().encodeToString(random);
  }

  static List<String> snapshotGroups(String effectiveUser) throws IOException {
    return UserGroupInformation.createRemoteUser(effectiveUser).getGroups();
  }

  static EncryptedSecret encryptSecret(OzoneManager ozoneManager,
      String accessKeyId, String effectiveUser, long createdAt,
      byte[] plaintextSecret) throws IOException {
    ManagedS3AccessKeyConfig config =
        ozoneManager.getManagedS3AccessKeyConfig();
    String keyName = config.getEncryptionKeyName();
    KeyProviderCryptoExtension provider = ozoneManager.getKmsProvider();
    if (provider == null) {
      throw new OMException("Managed S3 access-key encryption provider is " +
          "unavailable", MANAGED_S3_ACCESS_KEY_OPERATION_NOT_SUPPORTED);
    }
    KeyProvider.KeyVersion currentKey = provider.getCurrentKey(keyName);
    if (currentKey == null) {
      throw new OMException("Managed S3 access-key encryption key has no " +
          "current version", MANAGED_S3_ACCESS_KEY_OPERATION_NOT_SUPPORTED);
    }
    return ManagedS3AccessKeySecretEnvelopeCodec.encrypt(accessKeyId,
        effectiveUser, createdAt, keyName, plaintextSecret, provider,
        SECURE_RANDOM);
  }

  public static ManagedS3AccessKeyMetadataProto toMetadata(
      S3ManagedAccessKeyInfo info) {
    ManagedS3AccessKeyMetadataProto.Builder builder =
        ManagedS3AccessKeyMetadataProto.newBuilder()
            .setAccessKeyId(info.getAccessKeyId())
            .setSecretKeyId(info.getSecretKeyId())
            .setEffectiveUser(info.getEffectiveUser())
            .addAllGroups(info.getGroups())
            .setDescription(info.getDescription())
            .setCreatedAt(info.getCreatedAt())
            .setExpiresAt(info.getExpiresAt())
            .setCreatedBy(info.getCreatedBy())
            .setPolicySha256(policySha256(info.getPolicyDocument()));
    if (info.isDisabled()) {
      builder.setDisabled(true);
    }
    return builder.build();
  }

  static Map<String, String> auditMap(S3ManagedAccessKeyInfo info) {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(AUDIT_ACCESS_KEY_ID, info.getAccessKeyId());
    auditMap.put(AUDIT_EFFECTIVE_USER, info.getEffectiveUser());
    auditMap.put(AUDIT_CREATED_BY, info.getCreatedBy());
    auditMap.put(AUDIT_POLICY_SHA256, policySha256(info.getPolicyDocument()));
    return auditMap;
  }

  static Map<String, String> auditMap(String accessKeyId) {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(AUDIT_ACCESS_KEY_ID, accessKeyId);
    return auditMap;
  }

  static String policySha256(String policyDocument) {
    if (StringUtils.isBlank(policyDocument)) {
      return "";
    }
    return DigestUtils.sha256Hex(policyDocument);
  }

  static ByteString sha256(ByteString value) {
    return ByteString.copyFrom(DigestUtils.sha256(value.toByteArray()));
  }

  static ByteString toByteString(byte[] value) {
    return ByteString.copyFrom(value);
  }

  static long now() {
    return Time.now();
  }

  static Duration retrievalTtl(OzoneManager ozoneManager) {
    return ozoneManager.getManagedS3AccessKeyConfig().getRetrievalHandleTtl();
  }
}
