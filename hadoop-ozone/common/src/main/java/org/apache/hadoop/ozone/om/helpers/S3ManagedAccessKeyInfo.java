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

package org.apache.hadoop.ozone.om.helpers;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3ManagedAccessKeyInfoProto;

/**
 * Managed local S3 access key information stored in OM metadata.
 */
@Immutable
public final class S3ManagedAccessKeyInfo {
  private static final Codec<S3ManagedAccessKeyInfo> CODEC =
      new DelegatedCodec<>(
          Proto2Codec.get(S3ManagedAccessKeyInfoProto.getDefaultInstance()),
          S3ManagedAccessKeyInfo::fromProtobuf,
          S3ManagedAccessKeyInfo::getProtobuf,
          S3ManagedAccessKeyInfo.class,
          DelegatedCodec.CopyType.SHALLOW);

  private final String accessKeyId;
  private final ByteString encryptedSecretKey;
  private final String secretKeyId;
  private final String effectiveUser;
  private final ImmutableList<String> groups;
  private final String description;
  private final long createdAt;
  private final long expiresAt;
  private final boolean disabled;
  private final String createdBy;
  private final String policyDocument;

  private S3ManagedAccessKeyInfo(Builder builder) {
    this.accessKeyId = Objects.requireNonNull(builder.accessKeyId);
    this.encryptedSecretKey = Objects.requireNonNull(builder.encryptedSecretKey);
    this.secretKeyId = Objects.requireNonNull(builder.secretKeyId);
    this.effectiveUser = Objects.requireNonNull(builder.effectiveUser);
    this.groups = ImmutableList.copyOf(Objects.requireNonNull(builder.groups));
    this.description = Objects.requireNonNull(builder.description);
    this.createdAt = builder.createdAt;
    this.expiresAt = builder.expiresAt;
    this.disabled = builder.disabled;
    this.createdBy = Objects.requireNonNull(builder.createdBy);
    this.policyDocument = Objects.requireNonNull(builder.policyDocument);
  }

  public static Codec<S3ManagedAccessKeyInfo> getCodec() {
    return CODEC;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return newBuilder()
        .setAccessKeyId(accessKeyId)
        .setEncryptedSecretKey(encryptedSecretKey)
        .setSecretKeyId(secretKeyId)
        .setEffectiveUser(effectiveUser)
        .setGroups(groups)
        .setDescription(description)
        .setCreatedAt(createdAt)
        .setExpiresAt(expiresAt)
        .setDisabled(disabled)
        .setCreatedBy(createdBy)
        .setPolicyDocument(policyDocument);
  }

  public String getAccessKeyId() {
    return accessKeyId;
  }

  public ByteString getEncryptedSecretKey() {
    return encryptedSecretKey;
  }

  public String getSecretKeyId() {
    return secretKeyId;
  }

  public String getEffectiveUser() {
    return effectiveUser;
  }

  public List<String> getGroups() {
    return groups;
  }

  public String getDescription() {
    return description;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public long getExpiresAt() {
    return expiresAt;
  }

  public boolean isDisabled() {
    return disabled;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public String getPolicyDocument() {
    return policyDocument;
  }

  public static S3ManagedAccessKeyInfo fromProtobuf(
      S3ManagedAccessKeyInfoProto proto) {
    return newBuilder()
        .setAccessKeyId(proto.getAccessKeyId())
        .setEncryptedSecretKey(proto.getEncryptedSecretKey())
        .setSecretKeyId(proto.getSecretKeyId())
        .setEffectiveUser(proto.getEffectiveUser())
        .setGroups(proto.getGroupsList())
        .setDescription(proto.getDescription())
        .setCreatedAt(proto.getCreatedAt())
        .setExpiresAt(proto.getExpiresAt())
        .setDisabled(proto.getDisabled())
        .setCreatedBy(proto.getCreatedBy())
        .setPolicyDocument(proto.getPolicyDocument())
        .build();
  }

  public S3ManagedAccessKeyInfoProto getProtobuf() {
    S3ManagedAccessKeyInfoProto.Builder builder =
        S3ManagedAccessKeyInfoProto.newBuilder()
            .setAccessKeyId(accessKeyId)
            .setEncryptedSecretKey(encryptedSecretKey)
            .setSecretKeyId(secretKeyId)
            .setEffectiveUser(effectiveUser)
            .addAllGroups(groups)
            .setDescription(description)
            .setCreatedAt(createdAt)
            .setExpiresAt(expiresAt)
            .setCreatedBy(createdBy)
            .setPolicyDocument(policyDocument);
    if (disabled) {
      builder.setDisabled(true);
    }
    return builder.build();
  }

  @Override
  public String toString() {
    return "S3ManagedAccessKeyInfo{" +
        "accessKeyId='" + accessKeyId + '\'' +
        ", encryptedSecretKey=<redacted>" +
        ", secretKeyId='" + secretKeyId + '\'' +
        ", effectiveUser='" + effectiveUser + '\'' +
        ", groups=" + groups +
        ", description='" + description + '\'' +
        ", createdAt=" + createdAt +
        ", expiresAt=" + expiresAt +
        ", disabled=" + disabled +
        ", createdBy='" + createdBy + '\'' +
        ", policyDocument=<redacted>" +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    S3ManagedAccessKeyInfo that = (S3ManagedAccessKeyInfo) o;
    return createdAt == that.createdAt &&
        expiresAt == that.expiresAt &&
        disabled == that.disabled &&
        accessKeyId.equals(that.accessKeyId) &&
        encryptedSecretKey.equals(that.encryptedSecretKey) &&
        secretKeyId.equals(that.secretKeyId) &&
        effectiveUser.equals(that.effectiveUser) &&
        groups.equals(that.groups) &&
        description.equals(that.description) &&
        createdBy.equals(that.createdBy) &&
        policyDocument.equals(that.policyDocument);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accessKeyId, encryptedSecretKey, secretKeyId,
        effectiveUser, groups, description, createdAt, expiresAt, disabled,
        createdBy, policyDocument);
  }

  /**
   * Builder for {@link S3ManagedAccessKeyInfo}.
   */
  public static final class Builder {
    private String accessKeyId = "";
    private ByteString encryptedSecretKey = ByteString.EMPTY;
    private String secretKeyId = "";
    private String effectiveUser = "";
    private List<String> groups = ImmutableList.of();
    private String description = "";
    private long createdAt;
    private long expiresAt;
    private boolean disabled;
    private String createdBy = "";
    private String policyDocument = "";

    public Builder setAccessKeyId(String accessKeyId) {
      this.accessKeyId = Objects.requireNonNull(accessKeyId);
      return this;
    }

    public Builder setEncryptedSecretKey(ByteString encryptedSecretKey) {
      this.encryptedSecretKey = Objects.requireNonNull(encryptedSecretKey);
      return this;
    }

    public Builder setSecretKeyId(String secretKeyId) {
      this.secretKeyId = Objects.requireNonNull(secretKeyId);
      return this;
    }

    public Builder setEffectiveUser(String effectiveUser) {
      this.effectiveUser = Objects.requireNonNull(effectiveUser);
      return this;
    }

    public Builder setGroups(Iterable<String> groups) {
      this.groups = ImmutableList.copyOf(groups);
      return this;
    }

    public Builder addGroup(String group) {
      this.groups = ImmutableList.<String>builder()
          .addAll(groups)
          .add(Objects.requireNonNull(group))
          .build();
      return this;
    }

    public Builder setDescription(String description) {
      this.description = Objects.requireNonNull(description);
      return this;
    }

    public Builder setCreatedAt(long createdAt) {
      this.createdAt = createdAt;
      return this;
    }

    public Builder setExpiresAt(long expiresAt) {
      this.expiresAt = expiresAt;
      return this;
    }

    public Builder setDisabled(boolean disabled) {
      this.disabled = disabled;
      return this;
    }

    public Builder setCreatedBy(String createdBy) {
      this.createdBy = Objects.requireNonNull(createdBy);
      return this;
    }

    public Builder setPolicyDocument(String policyDocument) {
      this.policyDocument = Objects.requireNonNull(policyDocument);
      return this;
    }

    public S3ManagedAccessKeyInfo build() {
      return new S3ManagedAccessKeyInfo(this);
    }
  }
}
