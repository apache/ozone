/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.helpers;


import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;

import com.google.common.base.Preconditions;

/**
 * A class that encapsulates Bucket Info.
 */
public final class OmBucketInfo extends WithObjectID implements Auditable {
  private static final Codec<OmBucketInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(BucketInfo.getDefaultInstance()),
      OmBucketInfo::getFromProtobuf,
      OmBucketInfo::getProtobuf);

  public static Codec<OmBucketInfo> getCodec() {
    return CODEC;
  }

  /**
   * Name of the volume in which the bucket belongs to.
   */
  private final String volumeName;
  /**
   * Name of the bucket.
   */
  private final String bucketName;
  /**
   * ACL Information (mutable).
   */
  private final List<OzoneAcl> acls;
  /**
   * Bucket Version flag.
   */
  private final boolean isVersionEnabled;
  /**
   * Type of storage to be used for this bucket.
   * [RAM_DISK, SSD, DISK, ARCHIVE]
   */
  private final StorageType storageType;
  /**
   * Creation time of bucket.
   */
  private final long creationTime;
  /**
   * modification time of bucket.
   */
  private long modificationTime;

  /**
   * Bucket encryption key info if encryption is enabled.
   */
  private final BucketEncryptionKeyInfo bekInfo;

  /**
   * Optional default replication for bucket.
   */
  private final DefaultReplicationConfig defaultReplicationConfig;

  private final String sourceVolume;

  private final String sourceBucket;

  private long usedBytes;
  private long usedNamespace;
  private final long quotaInBytes;
  private final long quotaInNamespace;

  /**
   * Bucket Layout.
   */
  private BucketLayout bucketLayout;

  private String owner;

  /**
   * Private constructor, constructed via builder.
   * @param volumeName - Volume name.
   * @param bucketName - Bucket name.
   * @param acls - list of ACLs.
   * @param isVersionEnabled - Bucket version flag.
   * @param storageType - Storage type to be used.
   * @param creationTime - Bucket creation time.
   * @param modificationTime - Bucket modification time.
   * @param metadata - metadata.
   * @param bekInfo - bucket encryption key info.
   * @param sourceVolume - source volume for bucket links, null otherwise
   * @param sourceBucket - source bucket for bucket links, null otherwise
   * @param usedBytes - Bucket Quota Usage in bytes.
   * @param quotaInBytes Bucket quota in bytes.
   * @param quotaInNamespace Bucket quota in counts.
   * @param bucketLayout bucket layout.
   * @param owner owner of the bucket.
   * @param defaultReplicationConfig default replication config.
   * @param bucketLayout Bucket Layout.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private OmBucketInfo(String volumeName,
      String bucketName,
      List<OzoneAcl> acls,
      boolean isVersionEnabled,
      StorageType storageType,
      long creationTime,
      long modificationTime,
      long objectID,
      long updateID,
      Map<String, String> metadata,
      BucketEncryptionKeyInfo bekInfo,
      String sourceVolume,
      String sourceBucket,
      long usedBytes,
      long usedNamespace,
      long quotaInBytes,
      long quotaInNamespace,
      BucketLayout bucketLayout,
      String owner,
      DefaultReplicationConfig defaultReplicationConfig) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.acls = acls;
    this.isVersionEnabled = isVersionEnabled;
    this.storageType = storageType;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
    this.objectID = objectID;
    this.updateID = updateID;
    this.metadata = metadata;
    this.bekInfo = bekInfo;
    this.sourceVolume = sourceVolume;
    this.sourceBucket = sourceBucket;
    this.usedBytes = usedBytes;
    this.usedNamespace = usedNamespace;
    this.quotaInBytes = quotaInBytes;
    this.quotaInNamespace = quotaInNamespace;
    this.bucketLayout = bucketLayout;
    this.owner = owner;
    this.defaultReplicationConfig = defaultReplicationConfig;
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
   * Returns the ACL's associated with this bucket.
   * @return {@literal List<OzoneAcl>}
   */
  public List<OzoneAcl> getAcls() {
    return acls;
  }

  /**
   * Add an ozoneAcl to list of existing Acl set.
   * @param ozoneAcl
   * @return true - if successfully added, false if not added or acl is
   * already existing in the acl list.
   */
  public boolean addAcl(OzoneAcl ozoneAcl) {
    return OzoneAclUtil.addAcl(acls, ozoneAcl);
  }

  /**
   * Remove acl from existing acl list.
   * @param ozoneAcl
   * @return true - if successfully removed, false if not able to remove due
   * to that acl is not in the existing acl list.
   */
  public boolean removeAcl(OzoneAcl ozoneAcl) {
    return OzoneAclUtil.removeAcl(acls, ozoneAcl);
  }

  /**
   * Reset the existing acl list.
   * @param ozoneAcls
   * @return true - if successfully able to reset.
   */
  public boolean setAcls(List<OzoneAcl> ozoneAcls) {
    return OzoneAclUtil.setAcl(acls, ozoneAcls);
  }

  /**
   * Returns true if bucket version is enabled, else false.
   * @return isVersionEnabled
   */
  public boolean getIsVersionEnabled() {
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
   * Returns creation time.
   *
   * @return long
   */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * Returns modification time.
   * @return long
   */
  public long getModificationTime() {
    return modificationTime;
  }


  /**
   * Returns bucket encryption key info.
   * @return bucket encryption key info
   */
  public BucketEncryptionKeyInfo getEncryptionKeyInfo() {
    return bekInfo;
  }

  /**
   * Returns the Bucket Layout.
   *
   * @return BucketLayout.
   */
  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }

  /**
   * Returns bucket EC replication config.
   *
   * @return EC replication config.
   */
  public DefaultReplicationConfig getDefaultReplicationConfig() {
    return defaultReplicationConfig;
  }

  public String getSourceVolume() {
    return sourceVolume;
  }

  public String getSourceBucket() {
    return sourceBucket;
  }


  public long getUsedBytes() {
    return usedBytes;
  }

  public long getUsedNamespace() {
    return usedNamespace;
  }

  public void incrUsedBytes(long bytes) {
    this.usedBytes += bytes;
  }

  public void incrUsedNamespace(long namespaceToUse) {
    this.usedNamespace += namespaceToUse;
  }

  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  public long getQuotaInNamespace() {
    return quotaInNamespace;
  }

  public boolean isLink() {
    return sourceVolume != null && sourceBucket != null;
  }

  public String getOwner() {
    return owner;
  }

  public void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }

  public void setOwner(String ownerName) {
    this.owner = ownerName;
  }

  /**
   * Returns new builder class that builds a OmBucketInfo.
   *
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
    auditMap.put(OzoneConsts.BUCKET_LAYOUT, String.valueOf(this.bucketLayout));
    auditMap.put(OzoneConsts.GDPR_FLAG,
        this.metadata.get(OzoneConsts.GDPR_FLAG));
    auditMap.put(OzoneConsts.ACLS,
        (this.acls != null) ? this.acls.toString() : null);
    auditMap.put(OzoneConsts.IS_VERSION_ENABLED,
        String.valueOf(this.isVersionEnabled));
    auditMap.put(OzoneConsts.STORAGE_TYPE,
        (this.storageType != null) ? this.storageType.name() : null);
    auditMap.put(OzoneConsts.CREATION_TIME, String.valueOf(this.creationTime));
    auditMap.put(OzoneConsts.BUCKET_ENCRYPTION_KEY,
        (bekInfo != null) ? bekInfo.getKeyName() : null);
    auditMap.put(OzoneConsts.MODIFICATION_TIME,
        String.valueOf(this.modificationTime));
    if (isLink()) {
      auditMap.put(OzoneConsts.SOURCE_VOLUME, sourceVolume);
      auditMap.put(OzoneConsts.SOURCE_BUCKET, sourceBucket);
    }
    auditMap.put(OzoneConsts.USED_BYTES, String.valueOf(this.usedBytes));
    auditMap.put(OzoneConsts.USED_NAMESPACE,
        String.valueOf(this.usedNamespace));
    auditMap.put(OzoneConsts.OWNER, this.owner);
    auditMap.put(OzoneConsts.REPLICATION_TYPE,
        (this.defaultReplicationConfig != null) ?
            String.valueOf(this.defaultReplicationConfig.getType()) : null);
    auditMap.put(OzoneConsts.REPLICATION_CONFIG,
        (this.defaultReplicationConfig != null) ?
            this.defaultReplicationConfig.getReplicationConfig()
                .getReplication() : null);
    auditMap.put(OzoneConsts.QUOTA_IN_BYTES, String.valueOf(this.quotaInBytes));
    auditMap.put(OzoneConsts.QUOTA_IN_NAMESPACE,
        String.valueOf(this.quotaInNamespace));
    return auditMap;
  }

  /**
   * Return a new copy of the object.
   */
  public OmBucketInfo copyObject() {
    Builder builder = toBuilder();

    if (bekInfo != null) {
      builder.setBucketEncryptionKey(bekInfo.copy());
    }

    builder.acls.clear();
    acls.forEach(acl -> builder.addAcl(new OzoneAcl(acl.getType(),
        acl.getName(), (BitSet) acl.getAclBitSet().clone(),
        acl.getAclScope())));

    if (defaultReplicationConfig != null) {
      builder.setDefaultReplicationConfig(defaultReplicationConfig.copy());
    }

    return builder.build();
  }

  public Builder toBuilder() {
    return new Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setStorageType(storageType)
        .setIsVersionEnabled(isVersionEnabled)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setObjectID(objectID)
        .setUpdateID(updateID)
        .setBucketEncryptionKey(bekInfo)
        .setSourceVolume(sourceVolume)
        .setSourceBucket(sourceBucket)
        .setAcls(acls)
        .addAllMetadata(metadata)
        .setUsedBytes(usedBytes)
        .setUsedNamespace(usedNamespace)
        .setQuotaInBytes(quotaInBytes)
        .setQuotaInNamespace(quotaInNamespace)
        .setBucketLayout(bucketLayout)
        .setOwner(owner)
        .setDefaultReplicationConfig(defaultReplicationConfig);
  }

  /**
   * Builder for OmBucketInfo.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private List<OzoneAcl> acls;
    private Boolean isVersionEnabled;
    private StorageType storageType;
    private long creationTime;
    private long modificationTime;
    private long objectID;
    private long updateID;
    private Map<String, String> metadata;
    private BucketEncryptionKeyInfo bekInfo;
    private String sourceVolume;
    private String sourceBucket;
    private long usedBytes;
    private long usedNamespace;
    private long quotaInBytes;
    private long quotaInNamespace;
    private BucketLayout bucketLayout;
    private String owner;
    private DefaultReplicationConfig defaultReplicationConfig;

    public Builder() {
      //Default values
      this.acls = new ArrayList<>();
      this.isVersionEnabled = false;
      this.storageType = StorageType.DISK;
      this.metadata = new HashMap<>();
      this.quotaInBytes = OzoneConsts.QUOTA_RESET;
      this.quotaInNamespace = OzoneConsts.QUOTA_RESET;
      this.bucketLayout = BucketLayout.DEFAULT;
    }

    public Builder setVolumeName(String volume) {
      this.volumeName = volume;
      return this;
    }

    public Builder setBucketName(String bucket) {
      this.bucketName = bucket;
      return this;
    }

    public Builder setAcls(List<OzoneAcl> listOfAcls) {
      if (listOfAcls != null) {
        this.acls.addAll(listOfAcls);
      }
      return this;
    }

    public List<OzoneAcl> getAcls() {
      return acls;
    }

    public Builder addAcl(OzoneAcl ozoneAcl) {
      if (ozoneAcl != null) {
        this.acls.add(ozoneAcl);
      }
      return this;
    }

    public Builder setIsVersionEnabled(Boolean versionFlag) {
      this.isVersionEnabled = versionFlag;
      return this;
    }

    public Builder setStorageType(StorageType storage) {
      this.storageType = storage;
      return this;
    }

    public Builder setCreationTime(long createdOn) {
      this.creationTime = createdOn;
      return this;
    }

    public Builder setModificationTime(long modifiedOn) {
      this.modificationTime = modifiedOn;
      return this;
    }

    public Builder setObjectID(long obId) {
      this.objectID = obId;
      return this;
    }

    public Builder setUpdateID(long id) {
      this.updateID = id;
      return this;
    }

    public Builder addMetadata(String key, String value) {
      metadata.put(key, value);
      return this;
    }

    public Builder addAllMetadata(Map<String, String> additionalMetadata) {
      if (additionalMetadata != null) {
        metadata.putAll(additionalMetadata);
      }
      return this;
    }

    public Builder setBucketEncryptionKey(
        BucketEncryptionKeyInfo info) {
      this.bekInfo = info;
      return this;
    }

    public Builder setSourceVolume(String volume) {
      this.sourceVolume = volume;
      return this;
    }

    public Builder setSourceBucket(String bucket) {
      this.sourceBucket = bucket;
      return this;
    }

    public Builder setUsedBytes(long quotaUsage) {
      this.usedBytes = quotaUsage;
      return this;
    }

    public Builder setUsedNamespace(long quotaUsage) {
      this.usedNamespace = quotaUsage;
      return this;
    }

    public Builder setQuotaInBytes(long quota) {
      this.quotaInBytes = quota;
      return this;
    }

    public Builder setQuotaInNamespace(long quota) {
      this.quotaInNamespace = quota;
      return this;
    }

    public Builder setBucketLayout(BucketLayout type) {
      this.bucketLayout = type;
      return this;
    }

    public Builder setOwner(String ownerName) {
      this.owner = ownerName;
      return this;
    }

    public Builder setDefaultReplicationConfig(
        DefaultReplicationConfig defaultReplConfig) {
      this.defaultReplicationConfig = defaultReplConfig;
      return this;
    }

    /**
     * Constructs the OmBucketInfo.
     * @return instance of OmBucketInfo.
     */
    public OmBucketInfo build() {
      Preconditions.checkNotNull(volumeName);
      Preconditions.checkNotNull(bucketName);
      Preconditions.checkNotNull(acls);
      Preconditions.checkNotNull(isVersionEnabled);
      Preconditions.checkNotNull(storageType);
      return new OmBucketInfo(volumeName, bucketName, acls, isVersionEnabled,
          storageType, creationTime, modificationTime, objectID, updateID,
          metadata, bekInfo, sourceVolume, sourceBucket, usedBytes,
          usedNamespace, quotaInBytes, quotaInNamespace, bucketLayout, owner,
          defaultReplicationConfig);
    }
  }

  /**
   * Creates BucketInfo protobuf from OmBucketInfo.
   */
  public BucketInfo getProtobuf() {
    BucketInfo.Builder bib =  BucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .addAllAcls(OzoneAclUtil.toProtobuf(acls))
        .setIsVersionEnabled(isVersionEnabled)
        .setStorageType(storageType.toProto())
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setObjectID(objectID)
        .setUpdateID(updateID)
        .setUsedBytes(usedBytes)
        .setUsedNamespace(usedNamespace)
        .addAllMetadata(KeyValueUtil.toProtobuf(metadata))
        .setQuotaInBytes(quotaInBytes)
        .setQuotaInNamespace(quotaInNamespace);
    if (bucketLayout != null) {
      bib.setBucketLayout(bucketLayout.toProto());
    }
    if (bekInfo != null && bekInfo.getKeyName() != null) {
      bib.setBeinfo(OMPBHelper.convert(bekInfo));
    }
    if (defaultReplicationConfig != null) {
      bib.setDefaultReplicationConfig(defaultReplicationConfig.toProto());
    }
    if (sourceVolume != null) {
      bib.setSourceVolume(sourceVolume);
    }
    if (sourceBucket != null) {
      bib.setSourceBucket(sourceBucket);
    }
    if (owner != null) {
      bib.setOwner(owner);
    }
    return bib.build();
  }


  /**
   * Parses BucketInfo protobuf and creates OmBucketInfo.
   * @param bucketInfo
   * @return instance of OmBucketInfo
   */
  public static OmBucketInfo getFromProtobuf(BucketInfo bucketInfo) {
    return getFromProtobuf(bucketInfo, null);
  }

  /**
   * Parses BucketInfo protobuf and creates OmBucketInfo.
   * @param bucketInfo
   * @return instance of OmBucketInfo
   */
  public static OmBucketInfo getFromProtobuf(BucketInfo bucketInfo,
      BucketLayout buckLayout) {
    Builder obib = OmBucketInfo.newBuilder()
        .setVolumeName(bucketInfo.getVolumeName())
        .setBucketName(bucketInfo.getBucketName())
        .setAcls(bucketInfo.getAclsList().stream().map(
            OzoneAcl::fromProtobuf).collect(Collectors.toList()))
        .setIsVersionEnabled(bucketInfo.getIsVersionEnabled())
        .setStorageType(StorageType.valueOf(bucketInfo.getStorageType()))
        .setCreationTime(bucketInfo.getCreationTime())
        .setUsedBytes(bucketInfo.getUsedBytes())
        .setModificationTime(bucketInfo.getModificationTime())
        .setQuotaInBytes(bucketInfo.getQuotaInBytes())
        .setUsedNamespace(bucketInfo.getUsedNamespace())
        .setQuotaInNamespace(bucketInfo.getQuotaInNamespace());
    if (buckLayout != null) {
      obib.setBucketLayout(buckLayout);
    } else if (bucketInfo.getBucketLayout() != null) {
      obib.setBucketLayout(
          BucketLayout.fromProto(bucketInfo.getBucketLayout()));
    }
    if (bucketInfo.hasDefaultReplicationConfig()) {
      obib.setDefaultReplicationConfig(
          DefaultReplicationConfig.fromProto(
              bucketInfo.getDefaultReplicationConfig()));
    }
    if (bucketInfo.hasObjectID()) {
      obib.setObjectID(bucketInfo.getObjectID());
    }
    if (bucketInfo.hasUpdateID()) {
      obib.setUpdateID(bucketInfo.getUpdateID());
    }
    if (bucketInfo.getMetadataList() != null) {
      obib.addAllMetadata(KeyValueUtil
          .getFromProtobuf(bucketInfo.getMetadataList()));
    }
    if (bucketInfo.hasBeinfo()) {
      obib.setBucketEncryptionKey(OMPBHelper.convert(bucketInfo.getBeinfo()));
    }
    if (bucketInfo.hasSourceVolume()) {
      obib.setSourceVolume(bucketInfo.getSourceVolume());
    }
    if (bucketInfo.hasSourceBucket()) {
      obib.setSourceBucket(bucketInfo.getSourceBucket());
    }
    if (bucketInfo.hasOwner()) {
      obib.setOwner(bucketInfo.getOwner());
    }
    return obib.build();
  }

  @Override
  public String getObjectInfo() {
    String sourceInfo = sourceVolume != null && sourceBucket != null
        ? ", source='" + sourceVolume + "/" + sourceBucket + "'"
        : "";

    return "OMBucketInfo{" +
        "volume='" + volumeName + "'" +
        ", bucket='" + bucketName + "'" +
        ", isVersionEnabled='" + isVersionEnabled + "'" +
        ", storageType='" + storageType + "'" +
        ", creationTime='" + creationTime + "'" +
        ", usedBytes='" + usedBytes + "'" +
        ", usedNamespace='" + usedNamespace + "'" +
        ", quotaInBytes='" + quotaInBytes + "'" +
        ", quotaInNamespace='" + quotaInNamespace + "'" +
        ", bucketLayout='" + bucketLayout + '\'' +
        ", defaultReplicationConfig='" + defaultReplicationConfig + '\'' +
        sourceInfo +
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
    OmBucketInfo that = (OmBucketInfo) o;
    return creationTime == that.creationTime &&
        modificationTime == that.modificationTime &&
        volumeName.equals(that.volumeName) &&
        bucketName.equals(that.bucketName) &&
        Objects.equals(acls, that.acls) &&
        Objects.equals(isVersionEnabled, that.isVersionEnabled) &&
        storageType == that.storageType &&
        objectID == that.objectID &&
        updateID == that.updateID &&
        usedBytes == that.usedBytes &&
        usedNamespace == that.usedNamespace &&
        Objects.equals(sourceVolume, that.sourceVolume) &&
        Objects.equals(sourceBucket, that.sourceBucket) &&
        Objects.equals(metadata, that.metadata) &&
        Objects.equals(bekInfo, that.bekInfo) &&
        Objects.equals(owner, that.owner) &&
        Objects.equals(defaultReplicationConfig, that.defaultReplicationConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(volumeName, bucketName);
  }

  @Override
  public String toString() {
    return "OmBucketInfo{" +
        "volumeName='" + volumeName + "'" +
        ", bucketName='" + bucketName + "'" +
        ", acls=" + acls +
        ", isVersionEnabled=" + isVersionEnabled +
        ", storageType=" + storageType +
        ", creationTime=" + creationTime +
        ", bekInfo=" + bekInfo +
        ", sourceVolume='" + sourceVolume + "'" +
        ", sourceBucket='" + sourceBucket + "'" +
        ", objectID=" + objectID +
        ", updateID=" + updateID +
        ", metadata=" + metadata +
        ", usedBytes=" + usedBytes +
        ", usedNamespace=" + usedNamespace +
        ", quotaInBytes=" + quotaInBytes +
        ", quotaInNamespace=" + quotaInNamespace +
        ", bucketLayout=" + bucketLayout +
        ", owner=" + owner +
        ", defaultReplicationConfig=" + defaultReplicationConfig +
        '}';
  }
}
