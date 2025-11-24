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
import java.util.Objects;
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

  // STS-specific fields
  private String roleArn;
  private String originalAccessKeyId;
  private String secretAccessKey;
  private String sessionPolicy;

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
    this.originalAccessKeyId = originalAccessKeyId;
    this.roleArn = roleArn;
    this.secretAccessKey = secretAccessKey;
    this.sessionPolicy = sessionPolicy;
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
        .setOriginalAccessKeyId(originalAccessKeyId != null ? originalAccessKeyId : "")
        .setRoleArn(roleArn != null ? roleArn : "")
        .setSecretAccessKey(secretAccessKey != null ? encryptSensitiveField(secretAccessKey) : "")
        .setSessionPolicy(sessionPolicy != null ? sessionPolicy : "");

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

    if (token.hasOriginalAccessKeyId()) {
      this.originalAccessKeyId = token.getOriginalAccessKeyId();
    }
    if (token.hasRoleArn()) {
      this.roleArn = token.getRoleArn();
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
    }
  }

  /**
   * Encrypt a sensitive field using the configured encryption key.
   */
  private String encryptSensitiveField(String value) {
    if (value == null || value.isEmpty()) {
      return value != null ? value : "";
    }
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
    if (encryptedValue == null || encryptedValue.isEmpty()) {
      return encryptedValue != null ? encryptedValue : "";
    }
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
    final String aad = "v1|S3_STS_TOKEN|" + getOwnerId() + "|" + getExpiry().toEpochMilli() + "|" +
        getSecretKeyId().toString();
    return aad.getBytes(StandardCharsets.UTF_8);
  }

  public String getRoleArn() {
    return roleArn;
  }

  public String getSecretAccessKey() {
    return secretAccessKey;
  }

  public String getOriginalAccessKeyId() {
    return originalAccessKeyId;
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
    return Objects.equals(roleArn, that.roleArn) && Objects.equals(secretAccessKey, that.secretAccessKey) &&
        Objects.equals(originalAccessKeyId, that.originalAccessKeyId) &&
        Objects.equals(sessionPolicy, that.sessionPolicy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), roleArn, secretAccessKey, originalAccessKeyId, sessionPolicy);
  }

  @Override
  public String toString() {
    // Intentionally left off secretAccessKey
    return "STSTokenIdentifier{" + "tempAccessKeyId='" + getOwnerId() + "'" +
        ", originalAccessKeyId='" + originalAccessKeyId + "', roleArn='" + roleArn + "'" +
        ", expiry='" + getExpiry() + "', secretKeyId='" + getSecretKeyId() + "'" +
        ", sessionPolicy='" + sessionPolicy + "'}";
  }
}
