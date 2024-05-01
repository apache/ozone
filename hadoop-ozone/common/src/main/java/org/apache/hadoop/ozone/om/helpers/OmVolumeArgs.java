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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;

import com.google.common.base.Preconditions;


/**
 * A class that encapsulates the OmVolumeArgs Args.
 */
public final class OmVolumeArgs extends WithObjectID
    implements CopyObject<OmVolumeArgs>, Auditable {
  private static final Codec<OmVolumeArgs> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(VolumeInfo.getDefaultInstance()),
      OmVolumeArgs::getFromProtobuf,
      OmVolumeArgs::getProtobuf);

  public static Codec<OmVolumeArgs> getCodec() {
    return CODEC;
  }

  private final String adminName;
  private String ownerName;
  private final String volume;
  private long creationTime;
  private long modificationTime;
  private long quotaInBytes;
  private long quotaInNamespace;
  private long usedNamespace;
  private final CopyOnWriteArrayList<OzoneAcl> acls;
  /**
   * Reference count on this Ozone volume.
   *
   * When reference count is larger than zero, it indicates that at least one
   * "lock" is held on the volume by some Ozone feature (e.g. multi-tenancy).
   * Volume delete operation will be denied in this case, and user should be
   * prompted to release the lock first via the interface provided by that
   * feature.
   *
   * Volumes created using CLI, ObjectStore API or upgraded from older OM DB
   * will have reference count set to zero by default.
   */
  private long refCount;

  private OmVolumeArgs(Builder b) {
    super(b);
    this.adminName = b.adminName;
    this.ownerName = b.ownerName;
    this.volume = b.volume;
    this.quotaInBytes = b.quotaInBytes;
    this.quotaInNamespace = b.quotaInNamespace;
    this.usedNamespace = b.usedNamespace;
    this.acls = new CopyOnWriteArrayList<>(b.acls);
    this.creationTime = b.creationTime;
    this.modificationTime = b.modificationTime;
    this.refCount = b.refCount;
  }

  public long getRefCount() {
    Preconditions.checkState(refCount >= 0L, "refCount should not be negative");
    return refCount;
  }

  /**
   * Increase refCount by 1.
   */
  public void incRefCount() {
    this.refCount++;
  }

  /**
   * Decrease refCount by 1.
   */
  public void decRefCount() {
    Preconditions.checkState(this.refCount > 0L,
        "refCount should not become negative");
    this.refCount--;
  }

  public void setOwnerName(String newOwner) {
    this.ownerName = newOwner;
  }

  public void setQuotaInBytes(long quotaInBytes) {
    this.quotaInBytes = quotaInBytes;
  }

  public void setQuotaInNamespace(long quotaInNamespace) {
    this.quotaInNamespace = quotaInNamespace;
  }

  public void setCreationTime(long time) {
    this.creationTime = time;
  }

  public void setModificationTime(long time) {
    this.modificationTime = time;
  }

  public boolean addAcl(OzoneAcl ozoneAcl) {
    return OzoneAclUtil.addAcl(acls, ozoneAcl);
  }

  public boolean setAcls(List<OzoneAcl> ozoneAcls) {
    return OzoneAclUtil.setAcl(acls, ozoneAcls);
  }

  public boolean removeAcl(OzoneAcl ozoneAcl) {
    return OzoneAclUtil.removeAcl(acls, ozoneAcl);
  }

  /**
   * Returns the Admin Name.
   * @return String.
   */
  public String getAdminName() {
    return adminName;
  }

  /**
   * Returns the owner Name.
   * @return String
   */
  public String getOwnerName() {
    return ownerName;
  }

  /**
   * Returns the volume Name.
   * @return String
   */
  public String getVolume() {
    return volume;
  }

  /**
   * Returns creation time.
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
   * Returns Quota in Bytes.
   * @return long, Quota in bytes.
   */
  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  /**
   * Returns Quota in counts.
   * @return long, Quota in counts.
   */
  public long getQuotaInNamespace() {
    return quotaInNamespace;
  }

  public List<OzoneAcl> getAcls() {
    return ImmutableList.copyOf(acls);
  }

  public List<OzoneAcl> getDefaultAcls() {
    List<OzoneAcl> defaultAcls = new ArrayList<>();
    for (OzoneAcl acl : acls) {
      if (acl.getAclScope() == OzoneAcl.AclScope.DEFAULT) {
        defaultAcls.add(acl);
      }
    }
    return defaultAcls;
  }

  /**
   * increase used bucket namespace by n.
   */
  public void incrUsedNamespace(long n) {
    usedNamespace += n;
  }

  /**
   * Returns used bucket namespace.
   * @return usedNamespace
   */
  public long getUsedNamespace() {
    return usedNamespace;
  }

  /**
   * Returns new builder class that builds a OmVolumeArgs.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.ADMIN, this.adminName);
    auditMap.put(OzoneConsts.OWNER, this.ownerName);
    auditMap.put(OzoneConsts.VOLUME, this.volume);
    auditMap.put(OzoneConsts.CREATION_TIME, String.valueOf(this.creationTime));
    auditMap.put(OzoneConsts.MODIFICATION_TIME,
        String.valueOf(this.modificationTime));
    auditMap.put(OzoneConsts.QUOTA_IN_BYTES, String.valueOf(this.quotaInBytes));
    auditMap.put(OzoneConsts.QUOTA_IN_NAMESPACE,
        String.valueOf(this.quotaInNamespace));
    auditMap.put(OzoneConsts.USED_NAMESPACE,
        String.valueOf(this.usedNamespace));
    auditMap.put(OzoneConsts.OBJECT_ID, String.valueOf(this.getObjectID()));
    auditMap.put(OzoneConsts.UPDATE_ID, String.valueOf(this.getUpdateID()));
    return auditMap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OmVolumeArgs that = (OmVolumeArgs) o;
    return Objects.equals(this.getObjectID(), that.getObjectID());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getObjectID());
  }

  /**
   * Builder for OmVolumeArgs.
   */
  public static class Builder extends WithObjectID.Builder {
    private String adminName;
    private String ownerName;
    private String volume;
    private long creationTime;
    private long modificationTime;
    private long quotaInBytes;
    private long quotaInNamespace;
    private long usedNamespace;
    private final List<OzoneAcl> acls;
    private long refCount;

    @Override
    public Builder setObjectID(long id) {
      super.setObjectID(id);
      return this;
    }

    @Override
    public Builder setUpdateID(long id) {
      super.setUpdateID(id);
      return this;
    }

    /**
     * Constructs a builder.
     */
    public Builder() {
      this(new ArrayList<>());
    }

    private Builder(List<OzoneAcl> acls) {
      this.acls = acls;
      quotaInBytes = OzoneConsts.QUOTA_RESET;
      quotaInNamespace = OzoneConsts.QUOTA_RESET;
    }

    public Builder setAdminName(String admin) {
      this.adminName = admin;
      return this;
    }

    public Builder setOwnerName(String owner) {
      this.ownerName = owner;
      return this;
    }

    public Builder setVolume(String volumeName) {
      this.volume = volumeName;
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

    public Builder setQuotaInBytes(long quotaBytes) {
      this.quotaInBytes = quotaBytes;
      return this;
    }

    public Builder setQuotaInNamespace(long quotaNamespace) {
      this.quotaInNamespace = quotaNamespace;
      return this;
    }

    public Builder setUsedNamespace(long namespaceUsage) {
      this.usedNamespace = namespaceUsage;
      return this;
    }

    @Override
    public Builder addMetadata(String key, String value) {
      super.addMetadata(key, value);
      return this;
    }

    @Override
    public Builder addAllMetadata(Map<String, String> additionalMetaData) {
      super.addAllMetadata(additionalMetaData);
      return this;
    }

    public Builder addOzoneAcls(OzoneAcl acl) {
      OzoneAclUtil.addAcl(acls, acl);
      return this;
    }

    public Builder setRefCount(long refCount) {
      Preconditions.checkState(refCount >= 0L,
          "refCount should not be negative");
      this.refCount = refCount;
      return this;
    }

    public OmVolumeArgs build() {
      Preconditions.checkNotNull(adminName);
      Preconditions.checkNotNull(ownerName);
      Preconditions.checkNotNull(volume);
      return new OmVolumeArgs(this);
    }

  }

  public VolumeInfo getProtobuf() {
    List<OzoneAclInfo> aclList = OzoneAclUtil.toProtobuf(acls);
    return VolumeInfo.newBuilder()
        .setAdminName(adminName)
        .setOwnerName(ownerName)
        .setVolume(volume)
        .setQuotaInBytes(quotaInBytes)
        .setQuotaInNamespace(quotaInNamespace)
        .setUsedNamespace(usedNamespace)
        .addAllMetadata(KeyValueUtil.toProtobuf(getMetadata()))
        .addAllVolumeAcls(aclList)
        .setCreationTime(
            creationTime == 0 ? System.currentTimeMillis() : creationTime)
        .setModificationTime(modificationTime)
        .setObjectID(getObjectID())
        .setUpdateID(getUpdateID())
        .setRefCount(refCount)
        .build();
  }

  public static OmVolumeArgs getFromProtobuf(VolumeInfo volInfo) {
    return new Builder(OzoneAclUtil.fromProtobuf(volInfo.getVolumeAclsList()))
        .setAdminName(volInfo.getAdminName())
        .setOwnerName(volInfo.getOwnerName())
        .setVolume(volInfo.getVolume())
        .setQuotaInBytes(volInfo.getQuotaInBytes())
        .setQuotaInNamespace(volInfo.getQuotaInNamespace())
        .setUsedNamespace(volInfo.getUsedNamespace())
        .addAllMetadata(KeyValueUtil.getFromProtobuf(volInfo.getMetadataList()))
        .setCreationTime(volInfo.getCreationTime())
        .setModificationTime(volInfo.getModificationTime())
        .setObjectID(volInfo.getObjectID())
        .setUpdateID(volInfo.getUpdateID())
        .setRefCount(volInfo.getRefCount())
        .build();
  }

  @Override
  public String getObjectInfo() {
    return "OMVolumeArgs{" +
        "volume='" + volume + '\'' +
        ", admin='" + adminName + '\'' +
        ", owner='" + ownerName + '\'' +
        ", creationTime='" + creationTime + '\'' +
        ", quotaInBytes='" + quotaInBytes + '\'' +
        ", usedNamespace='" + usedNamespace + '\'' +
        ", refCount='" + refCount + '\'' +
        '}';
  }

  @Override
  public OmVolumeArgs copyObject() {
    return new Builder(acls)
        .setAdminName(adminName)
        .setOwnerName(ownerName)
        .setVolume(volume)
        .setQuotaInBytes(quotaInBytes)
        .setQuotaInNamespace(quotaInNamespace)
        .setUsedNamespace(usedNamespace)
        .addAllMetadata(getMetadata())
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setObjectID(getObjectID())
        .setUpdateID(getUpdateID())
        .setRefCount(refCount)
        .build();
  }
}
