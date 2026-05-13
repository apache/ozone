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
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.security.token.ShortLivedTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto;

/**
 * Token identifier for STS (Security Token Service) tokens.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class STSTokenIdentifier extends ShortLivedTokenIdentifier {
  public static final Text KIND_NAME = new Text("STSToken");

  /**
   * Identifies the authentication path that produced an STS token.
   */
  public enum AuthType {
    ASSUME_ROLE,
    WEB_IDENTITY
  }

  // STS-specific fields
  private AuthType authType = AuthType.ASSUME_ROLE;
  private String roleArn;
  private String originalAccessKeyId;
  private String secretAccessKey;
  private String sessionPolicy;
  private String effectiveUser;
  private String issuer;
  private String subject;
  private String audience;
  private Set<String> groups = Collections.emptySet();
  private Set<String> roles = Collections.emptySet();
  private String roleSessionName;
  private String providerId;
  private String tokenFingerprint;

  // Encryption key derived from ManagedSecretKey for this token
  private transient byte[] encryptionKey;

  // Service name for STS tokens
  public static final String STS_SERVICE = "STS";

  /**
   * Create an empty STS token identifier.
   */
  public STSTokenIdentifier() {
    super();
  }

  /**
   * Create a new STS token identifier with encryption support.
   *
   * @param tempAccessKeyId     the temporary access key ID (owner)
   * @param originalAccessKeyId the original long-lived access key ID that created this token
   * @param roleArn             the ARN of the assumed role
   * @param expiry              the token expiration time
   * @param secretAccessKey     the secret access key associated with the temporary access key ID
   * @param sessionPolicy       an optional opaque identifier that further limits the scope of
   *                            the permissions granted by the role
   * @param encryptionKey       the key bytes for encrypting sensitive fields
   */
  public STSTokenIdentifier(String tempAccessKeyId, String originalAccessKeyId, String roleArn, Instant expiry,
      String secretAccessKey, String sessionPolicy, byte[] encryptionKey) {
    super(tempAccessKeyId, expiry);
    this.authType = AuthType.ASSUME_ROLE;
    this.originalAccessKeyId = originalAccessKeyId;
    this.roleArn = roleArn;
    this.secretAccessKey = secretAccessKey;
    this.sessionPolicy = sessionPolicy;
    this.encryptionKey = encryptionKey != null ? encryptionKey.clone() : null;
  }

  /**
   * Create a new WebIdentity-backed STS token identifier.
   */
  public STSTokenIdentifier(String tempAccessKeyId, String roleArn,
      Instant expiry, String secretAccessKey, String sessionPolicy,
      String effectiveUser, String issuer, String subject, String audience,
      Set<String> groups, Set<String> roles, String roleSessionName,
      String providerId, String tokenFingerprint, byte[] encryptionKey) {
    super(tempAccessKeyId, expiry);
    this.authType = AuthType.WEB_IDENTITY;
    this.roleArn = roleArn;
    this.secretAccessKey = secretAccessKey;
    this.sessionPolicy = sessionPolicy;
    this.effectiveUser = effectiveUser;
    this.issuer = issuer;
    this.subject = subject;
    this.audience = audience;
    this.groups = immutableSet(groups);
    this.roles = immutableSet(roles);
    this.roleSessionName = roleSessionName;
    this.providerId = providerId;
    this.tokenFingerprint = tokenFingerprint;
    this.encryptionKey = encryptionKey != null ? encryptionKey.clone() : null;
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  @Override
  public String getService() {
    return STS_SERVICE;
  }

  @Override
  public void readFromByteArray(byte[] bytes) throws IOException {
    final DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.write(toProtoBuf().toByteArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    final OMTokenProto token = OMTokenProto.parseFrom((DataInputStream) in);
    fromProtoBuf(token);
  }

  /**
   * Convert this identifier to protobuf format.
   */
  public OMTokenProto toProtoBuf() {
    Preconditions.checkArgument(this.encryptionKey != null, "The encryption key must not be null");

    final OMTokenProto.Builder builder = OMTokenProto.newBuilder();
    // Note: secretKeyId must be set before attempting to decrypt secretAccessKey
    if (getSecretKeyId() != null) {
      builder.setSecretKeyId(getSecretKeyId().toString());
    }

    builder
        .setType(OMTokenProto.Type.S3_STS_TOKEN)
        .setMaxDate(getExpiry().toEpochMilli())
        .setOwner(getOwnerId() != null ? getOwnerId() : "")
        .setAccessKeyId(getOwnerId() != null ? getOwnerId() : "")
        .setStsAuthType(toProtoAuthType(authType));

    setIfNotEmpty(builder::setOriginalAccessKeyId, originalAccessKeyId);
    setIfNotEmpty(builder::setRoleArn, roleArn);
    if (secretAccessKey != null) {
      builder.setSecretAccessKey(encryptSensitiveField(secretAccessKey));
    }
    if (sessionPolicy != null) {
      builder.setSessionPolicy(sessionPolicy);
    }
    setIfNotEmpty(builder::setEffectiveUser, effectiveUser);
    setIfNotEmpty(builder::setIssuer, issuer);
    setIfNotEmpty(builder::setSubject, subject);
    setIfNotEmpty(builder::setAudience, audience);
    builder.addAllGroups(groups);
    builder.addAllRoles(roles);
    setIfNotEmpty(builder::setRoleSessionName, roleSessionName);
    setIfNotEmpty(builder::setProviderId, providerId);
    setIfNotEmpty(builder::setTokenFingerprint, tokenFingerprint);

    return builder.build();
  }

  /**
   * Initialize this identifier from protobuf.
   */
  public void fromProtoBuf(OMTokenProto token) throws IOException {
    Preconditions.checkArgument(
        token.getType() == OMTokenProto.Type.S3_STS_TOKEN,
        "Invalid token type for STSTokenIdentifier: " + token.getType());
    Preconditions.checkArgument(this.encryptionKey != null, "The encryption key must not be null");

    setOwnerId(token.getOwner());
    setExpiry(Instant.ofEpochMilli(token.getMaxDate()));
    this.authType = fromProtoAuthType(token.getStsAuthType());

    if (token.hasOriginalAccessKeyId()) {
      this.originalAccessKeyId = emptyToNull(token.getOriginalAccessKeyId());
    }
    if (token.hasRoleArn()) {
      this.roleArn = emptyToNull(token.getRoleArn());
    }
    if (token.hasSecretKeyId()) {
      try {
        setSecretKeyId(UUID.fromString(token.getSecretKeyId()));
      } catch (IllegalArgumentException e) {
        // Handle invalid UUID format gracefully
        throw new IOException(
            "Invalid secretKeyId format in STS token: " + token.getSecretKeyId(), e);
      }
    }
    // Note: secretKeyId must be set before attempting to decrypt secretAccessKey
    if (token.hasSecretAccessKey()) {
      this.secretAccessKey = decryptSensitiveField(token.getSecretAccessKey());
    }

    if (token.hasSessionPolicy()) {
      this.sessionPolicy = token.getSessionPolicy();
    } else {
      this.sessionPolicy = "";
    }
    if (token.hasEffectiveUser()) {
      this.effectiveUser = emptyToNull(token.getEffectiveUser());
    }
    if (token.hasIssuer()) {
      this.issuer = emptyToNull(token.getIssuer());
    }
    if (token.hasSubject()) {
      this.subject = emptyToNull(token.getSubject());
    }
    if (token.hasAudience()) {
      this.audience = emptyToNull(token.getAudience());
    }
    this.groups = immutableSet(token.getGroupsList());
    this.roles = immutableSet(token.getRolesList());
    if (token.hasRoleSessionName()) {
      this.roleSessionName = emptyToNull(token.getRoleSessionName());
    }
    if (token.hasProviderId()) {
      this.providerId = emptyToNull(token.getProviderId());
    }
    if (token.hasTokenFingerprint()) {
      this.tokenFingerprint = emptyToNull(token.getTokenFingerprint());
    }
  }

  /**
   * Encrypt a sensitive field using the configured encryption key.
   */
  private String encryptSensitiveField(String value) {
    if (encryptionKey == null) {
      throw new IllegalStateException("Encryption key must be set before encrypting sensitive fields");
    }

    try {
      final byte[] aad = computeAadBytes();
      return STSTokenEncryption.encrypt(value, encryptionKey, aad);
    } catch (STSTokenEncryption.STSTokenEncryptionException e) {
      throw new RuntimeException("Token encryption failed", e);
    }
  }

  /**
   * Decrypt a sensitive field using the configured encryption key.
   */
  private String decryptSensitiveField(String encryptedValue) {
    if (encryptionKey == null) {
      throw new IllegalStateException("Encryption key must be set before decrypting sensitive fields");
    }

    try {
      final byte[] aad = computeAadBytes();
      return STSTokenEncryption.decrypt(encryptedValue, encryptionKey, aad);
    } catch (STSTokenEncryption.STSTokenEncryptionException e) {
      throw new RuntimeException("Token decryption failed", e);
    }
  }

  /**
   * Compute additional authenticated data to bind token context to encryption.
   * Includes token type, ownerId, expiry millis, and secretKeyId.
   */
  private byte[] computeAadBytes() {
    final StringBuilder stringBuilder = new StringBuilder("v1|S3_STS_TOKEN|");
    stringBuilder.append(getOwnerId());
    stringBuilder.append('|');
    stringBuilder.append(getExpiry().toEpochMilli());
    stringBuilder.append('|');
    stringBuilder.append(getSecretKeyId().toString());
    final String aad = stringBuilder.toString();
    return aad.getBytes(StandardCharsets.UTF_8);
  }

  public String getRoleArn() {
    return roleArn;
  }

  public AuthType getAuthType() {
    return authType;
  }

  public boolean isWebIdentity() {
    return authType == AuthType.WEB_IDENTITY;
  }

  public String getSecretAccessKey() {
    return secretAccessKey;
  }

  public String getOriginalAccessKeyId() {
    return originalAccessKeyId;
  }

  public String getEffectiveUser() {
    return effectiveUser;
  }

  public String getIssuer() {
    return issuer;
  }

  public String getSubject() {
    return subject;
  }

  public String getAudience() {
    return audience;
  }

  public Set<String> getGroups() {
    return groups;
  }

  public Set<String> getRoles() {
    return roles;
  }

  public String getRoleSessionName() {
    return roleSessionName;
  }

  public String getProviderId() {
    return providerId;
  }

  public String getTokenFingerprint() {
    return tokenFingerprint;
  }

  /**
   * Get the temporary access key ID (same as owner).
   */
  public String getTempAccessKeyId() {
    return getOwnerId();
  }

  /**
   * Optional session policy associated with this STS token, or null/empty if none.
   */
  public String getSessionPolicy() {
    return sessionPolicy;
  }

  public void setEncryptionKey(byte[] encryptionKey) {
    this.encryptionKey = encryptionKey.clone();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    if (!super.equals(o)) {
      return false;
    }

    final STSTokenIdentifier that = (STSTokenIdentifier) o;
    return authType == that.authType &&
        Objects.equals(roleArn, that.roleArn) && Objects.equals(secretAccessKey, that.secretAccessKey) &&
        Objects.equals(originalAccessKeyId, that.originalAccessKeyId) &&
        Objects.equals(sessionPolicy, that.sessionPolicy) &&
        Objects.equals(effectiveUser, that.effectiveUser) &&
        Objects.equals(issuer, that.issuer) &&
        Objects.equals(subject, that.subject) &&
        Objects.equals(audience, that.audience) &&
        Objects.equals(groups, that.groups) &&
        Objects.equals(roles, that.roles) &&
        Objects.equals(roleSessionName, that.roleSessionName) &&
        Objects.equals(providerId, that.providerId) &&
        Objects.equals(tokenFingerprint, that.tokenFingerprint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), authType, roleArn, secretAccessKey, originalAccessKeyId,
        sessionPolicy, effectiveUser, issuer, subject, audience, groups, roles,
        roleSessionName, providerId, tokenFingerprint);
  }

  @Override
  public String toString() {
    // Intentionally left off secretAccessKey
    return "STSTokenIdentifier{" + "tempAccessKeyId='" + getOwnerId() + "'" +
        ", authType=" + authType +
        ", originalAccessKeyId='" + originalAccessKeyId + "', roleArn='" + roleArn + "'" +
        ", effectiveUser='" + effectiveUser + "', issuer='" + issuer + "'" +
        ", subject='" + subject + "', audience='" + audience + "'" +
        ", groups=" + groups + ", roles=" + roles +
        ", roleSessionName='" + roleSessionName + "', providerId='" + providerId + "'" +
        ", tokenFingerprint='" + tokenFingerprint + "'" +
        ", expiry='" + getExpiry() + "', secretKeyId='" + getSecretKeyId() + "'" +
        ", sessionPolicy='" + sessionPolicy + "'}";
  }

  private static OMTokenProto.STSAuthType toProtoAuthType(AuthType value) {
    return value == AuthType.WEB_IDENTITY
        ? OMTokenProto.STSAuthType.WEB_IDENTITY
        : OMTokenProto.STSAuthType.ASSUME_ROLE;
  }

  private static AuthType fromProtoAuthType(OMTokenProto.STSAuthType value) {
    return value == OMTokenProto.STSAuthType.WEB_IDENTITY
        ? AuthType.WEB_IDENTITY : AuthType.ASSUME_ROLE;
  }

  private static Set<String> immutableSet(Iterable<String> values) {
    if (values == null) {
      return Collections.emptySet();
    }
    LinkedHashSet<String> set = new LinkedHashSet<>();
    for (String value : values) {
      if (value != null && !value.isEmpty()) {
        set.add(value);
      }
    }
    return Collections.unmodifiableSet(set);
  }

  private static void setIfNotEmpty(StringSetter setter, String value) {
    if (value != null && !value.isEmpty()) {
      setter.set(value);
    }
  }

  private static String emptyToNull(String value) {
    return value == null || value.isEmpty() ? null : value;
  }

  private interface StringSetter {
    void set(String value);
  }
}
