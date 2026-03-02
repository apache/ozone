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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.protocol.OzoneStoragePolicy;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketArgs;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;

/**
 * A class that encapsulates Bucket Arguments.
 */
public final class OmBucketArgs extends WithMetadata implements Auditable {
  /**
   * Name of the volume in which the bucket belongs to.
   */
  private final String volumeName;
  /**
   * Name of the bucket.
   */
  private final String bucketName;
  /**
   * Bucket Version flag.
   */
  private final Boolean isVersionEnabled;
  /**
   * Type of storage to be used for this bucket.
   * [RAM_DISK, SSD, DISK, ARCHIVE]
   */
  private final StorageType storageType;

  /**
   * Bucket encryption key info if encryption is enabled.
   */
  private final BucketEncryptionKeyInfo bekInfo;
  private final long quotaInBytes;
  private final long quotaInNamespace;
  private final boolean quotaInBytesSet;
  private final boolean quotaInNamespaceSet;
  private final DefaultReplicationConfig defaultReplicationConfig;
  /**
   * Bucket Owner Name.
   */
  private final String ownerName;

  private final OzoneStoragePolicy storagePolicy;

  private OmBucketArgs(Builder b) {
    super(b);
    this.volumeName = b.volumeName;
    this.bucketName = b.bucketName;
    this.isVersionEnabled = b.isVersionEnabled;
    this.storageType = b.storageType;
    this.ownerName = b.ownerName;
    this.defaultReplicationConfig = b.defaultReplicationConfig;
    this.quotaInBytesSet = b.quotaInBytesSet;
    this.quotaInBytes = quotaInBytesSet ? b.quotaInBytes : OzoneConsts.QUOTA_RESET;
    this.quotaInNamespaceSet = b.quotaInNamespaceSet;
    this.quotaInNamespace = quotaInNamespaceSet ? b.quotaInNamespace : OzoneConsts.QUOTA_RESET;
    this.bekInfo = b.bekInfo;
    this.storagePolicy = b.storagePolicy;
  }

  /**
   * Returns the Volume Name.
   * @return String.
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Returns the Bucket Name.
   * @return String
   */
  public String getBucketName() {
    return bucketName;
  }

  /**
   * Returns true if bucket version is enabled, else false.
   * @return isVersionEnabled
   */
  public Boolean getIsVersionEnabled() {
    return isVersionEnabled;
  }

  /**
   * Returns the type of storage to be used.
   * @return StorageType
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Returns Bucket Quota in bytes.
   * @return quotaInBytes.
   */
  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  /**
   * Returns true if quotaInBytes has been set to a non default value.
   */
  public boolean hasQuotaInBytes() {
    return quotaInBytesSet;
  }

  /**
   * Returns Bucket Quota in key counts.
   * @return quotaInNamespace.
   */
  public long getQuotaInNamespace() {
    return quotaInNamespace;
  }

  /**
   * Returns true if quotaInNamespace has been set to a non default value.
   */
  public boolean hasQuotaInNamespace() {
    return quotaInNamespaceSet;
  }

  /**
   * Returns Bucket default replication config.
   */
  public DefaultReplicationConfig getDefaultReplicationConfig() {
    return defaultReplicationConfig;
  }

  public BucketEncryptionKeyInfo getBucketEncryptionKeyInfo() {
    return bekInfo;
  }

  /**
   * Returns Bucket Owner Name.
   *
   * @return ownerName.
   */
  public String getOwnerName() {
    return ownerName;
  }

  public OzoneStoragePolicy getStoragePolicy() {
    return storagePolicy;
  }

  /**
   * Returns new builder class that builds a OmBucketArgs.
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, this.volumeName);
    auditMap.put(OzoneConsts.BUCKET, this.bucketName);
    auditMap.put(OzoneConsts.GDPR_FLAG,
        getMetadata().get(OzoneConsts.GDPR_FLAG));
    auditMap.put(OzoneConsts.IS_VERSION_ENABLED,
                String.valueOf(this.isVersionEnabled));
    if (this.storageType != null) {
      auditMap.put(OzoneConsts.STORAGE_TYPE, this.storageType.name());
    }
    if (this.ownerName != null) {
      auditMap.put(OzoneConsts.OWNER, this.ownerName);
    }
    if (this.storagePolicy != null) {
      auditMap.put(OzoneConsts.STORAGE_POLICY, this.storagePolicy.name());
    }
    if (this.quotaInBytesSet && quotaInBytes > 0 ||
        (this.quotaInBytes != OzoneConsts.QUOTA_RESET)) {
      auditMap.put(OzoneConsts.QUOTA_IN_BYTES,
          String.valueOf(this.quotaInBytes));
    }
    if (this.quotaInNamespaceSet && quotaInNamespace > 0 ||
        (this.quotaInNamespace != OzoneConsts.QUOTA_RESET)) {
      auditMap.put(OzoneConsts.QUOTA_IN_NAMESPACE,
          String.valueOf(this.quotaInNamespace));
    }
    if (this.bekInfo != null) {
      auditMap.put(OzoneConsts.BUCKET_ENCRYPTION_KEY,
          this.bekInfo.getKeyName());
    }
    if (this.defaultReplicationConfig != null) {
      auditMap.put(OzoneConsts.REPLICATION_TYPE, String.valueOf(
          this.defaultReplicationConfig.getType()));
      auditMap.put(OzoneConsts.REPLICATION_CONFIG,
          this.defaultReplicationConfig.getReplicationConfig()
              .getReplication());
    }
    return auditMap;
  }

  /**
   * Builder for OmBucketArgs.
   */
  public static class Builder extends WithMetadata.Builder {
    private String volumeName;
    private String bucketName;
    private Boolean isVersionEnabled;
    private StorageType storageType;
    private boolean quotaInBytesSet = false;
    private long quotaInBytes;
    private boolean quotaInNamespaceSet = false;
    private long quotaInNamespace;
    private BucketEncryptionKeyInfo bekInfo;
    private DefaultReplicationConfig defaultReplicationConfig;
    private String ownerName;
    private OzoneStoragePolicy storagePolicy;

    /**
     * Constructs a builder.
     */
    public Builder() {
      quotaInBytes = OzoneConsts.QUOTA_RESET;
      quotaInNamespace = OzoneConsts.QUOTA_RESET;
    }

    public Builder setVolumeName(String volume) {
      this.volumeName = volume;
      return this;
    }

    public Builder setBucketName(String bucket) {
      this.bucketName = bucket;
      return this;
    }

    public Builder setIsVersionEnabled(Boolean versionFlag) {
      this.isVersionEnabled = versionFlag;
      return this;
    }

    @Deprecated
    public Builder setBucketEncryptionKey(BucketEncryptionKeyInfo info) {
      if (info == null || info.getKeyName() != null) {
        this.bekInfo = info;
      }
      return this;
    }

    @Override
    public Builder addAllMetadata(Map<String, String> map) {
      super.addAllMetadata(map);
      return this;
    }

    public Builder setStorageType(StorageType storage) {
      this.storageType = storage;
      return this;
    }

    public Builder setQuotaInBytes(long quota) {
      quotaInBytesSet = true;
      quotaInBytes = quota;
      return this;
    }

    public Builder setQuotaInNamespace(long quota) {
      quotaInNamespaceSet = true;
      quotaInNamespace = quota;
      return this;
    }

    public Builder setDefaultReplicationConfig(
        DefaultReplicationConfig defaultRepConfig) {
      this.defaultReplicationConfig = defaultRepConfig;
      return this;
    }

    public Builder setOwnerName(String owner) {
      ownerName = owner;
      return this;
    }

    public Builder setStoragePolicy(OzoneStoragePolicy policy) {
      this.storagePolicy = policy;
      return this;
    }

    /**
     * Constructs the OmBucketArgs.
     * @return instance of OmBucketArgs.
     */
    public OmBucketArgs build() {
      Objects.requireNonNull(volumeName, "volumeName == null");
      Objects.requireNonNull(bucketName, "bucketName == null");
      return new OmBucketArgs(this);
    }
  }

  /**
   * Creates BucketArgs protobuf from OmBucketArgs.
   */
  public BucketArgs getProtobuf() {
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setVolumeName(volumeName)
        .setBucketName(bucketName);
    if (isVersionEnabled != null) {
      builder.setIsVersionEnabled(isVersionEnabled);
    }
    if (storageType != null) {
      builder.setStorageType(storageType.toProto());
    }
    if (quotaInBytesSet && (
        quotaInBytes > 0 || quotaInBytes == OzoneConsts.QUOTA_RESET)) {
      builder.setQuotaInBytes(quotaInBytes);
    }
    if (quotaInNamespaceSet && (
        quotaInNamespace > 0 || quotaInNamespace == OzoneConsts.QUOTA_RESET)) {
      builder.setQuotaInNamespace(quotaInNamespace);
    }
    if (defaultReplicationConfig != null) {
      builder.setDefaultReplicationConfig(defaultReplicationConfig.toProto());
    }
    if (ownerName != null) {
      builder.setOwnerName(ownerName);
    }
    if (storagePolicy != null) {
      builder.setStoragePolicy(storagePolicy.toProto());
    }

    if (bekInfo != null) {
      builder.setBekInfo(OMPBHelper.convert(bekInfo));
    }

    return builder.build();
  }

  /**
   * Parses BucketInfo protobuf and creates OmBucketArgs Builder.
   * @return Builder instance
   */
  public static Builder builderFromProtobuf(BucketArgs bucketArgs) {
    final OmBucketArgs.Builder builder = newBuilder()
        .setVolumeName(bucketArgs.getVolumeName())
        .setBucketName(bucketArgs.getBucketName())
        .addAllMetadata(KeyValueUtil.getFromProtobuf(bucketArgs.getMetadataList()));

    if (bucketArgs.hasIsVersionEnabled()) {
      builder.setIsVersionEnabled(bucketArgs.getIsVersionEnabled());
    }
    if (bucketArgs.hasStorageType()) {
      builder.setStorageType(StorageType.valueOf(bucketArgs.getStorageType()));
    }
    if (bucketArgs.hasOwnerName()) {
      builder.setOwnerName(bucketArgs.getOwnerName());
    }
    if (bucketArgs.hasStoragePolicy()) {
      builder.setStoragePolicy(
          OzoneStoragePolicy.fromProto(bucketArgs.getStoragePolicy()));
    }

    if (bucketArgs.hasDefaultReplicationConfig()) {
      builder.setDefaultReplicationConfig(
          DefaultReplicationConfig.fromProto(
              bucketArgs.getDefaultReplicationConfig()));
    }

    if (bucketArgs.hasQuotaInBytes()) {
      builder.setQuotaInBytes(bucketArgs.getQuotaInBytes());
    }
    if (bucketArgs.hasQuotaInNamespace()) {
      builder.setQuotaInNamespace(bucketArgs.getQuotaInNamespace());
    }

    if (bucketArgs.hasBekInfo()) {
      builder.setBucketEncryptionKey(
          OMPBHelper.convert(bucketArgs.getBekInfo()));
    }

    return builder;
  }

  /**
   * Parses BucketInfo protobuf and creates OmBucketArgs.
   * @return instance of OmBucketArgs
   */
  public static OmBucketArgs getFromProtobuf(BucketArgs bucketArgs) {
    return builderFromProtobuf(bucketArgs).build();
  }
}
