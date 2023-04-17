/*
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

package org.apache.hadoop.ozone.recon.api.types;

import com.google.common.base.Preconditions;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import java.time.Instant;
import java.util.List;

/**
 * Metadata object represents one bucket in the object store.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public final class BucketMetadata {
  @XmlElement(name = "volumeName")
  private String volumeName;
  @XmlElement(name = "bucketName")
  private String bucketName;
  @XmlElement(name = "acls")
  private List<AclMetadata> acls;
  @XmlElement(name = "isVersionEnabled")
  private boolean isVersionEnabled;
  @XmlElement(name = "storageType")
  private String storageType;
  @XmlElement(name = "creationTime")
  private Instant creationTime;
  @XmlElement(name = "modificationTime")
  private Instant modificationTime;
  @XmlElement(name = "sourceVolume")
  private String sourceVolume;
  @XmlElement(name = "sourceBucket")
  private String sourceBucket;
  @XmlElement(name = "usedBytes")
  private long usedBytes;
  @XmlElement(name = "usedNamespace")
  private long usedNamespace;
  @XmlElement(name = "quotaInBytes")
  private long quotaInBytes;
  @XmlElement(name = "quotaInNamespace")
  private long quotaInNamespace;
  @XmlElement(name = "bucketLayout")
  private String bucketLayout;
  @XmlElement(name = "owner")
  private String owner;

  private BucketMetadata(Builder builder) {
    this.volumeName = builder.volumeName;
    this.bucketName = builder.bucketName;
    this.acls = builder.acls;
    this.isVersionEnabled = builder.isVersionEnabled;
    this.storageType = builder.storageType;
    this.creationTime = builder.creationTime;
    this.modificationTime = builder.modificationTime;
    this.sourceVolume = builder.sourceVolume;
    this.sourceBucket = builder.sourceBucket;
    this.usedBytes = builder.usedBytes;
    this.usedNamespace = builder.usedNamespace;
    this.quotaInBytes = builder.quotaInBytes;
    this.quotaInNamespace = builder.quotaInNamespace;
    this.bucketLayout = builder.bucketLayout;
    this.owner = builder.owner;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public List<AclMetadata> getAcls() {
    return acls;
  }

  public boolean isVersionEnabled() {
    return isVersionEnabled;
  }

  public String getStorageType() {
    return storageType;
  }

  public Instant getCreationTime() {
    return creationTime;
  }

  public Instant getModificationTime() {
    return modificationTime;
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

  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  public long getQuotaInNamespace() {
    return quotaInNamespace;
  }

  public String getBucketLayout() {
    return bucketLayout;
  }

  public String getOwner() {
    return owner;
  }

  /**
   * Returns new builder class that builds a BucketMetadata.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for BucketMetadata.
   */
  @SuppressWarnings("checkstyle:HiddenField")
  public static final class Builder {
    private String volumeName;
    private String bucketName;
    private List<AclMetadata> acls;
    private boolean isVersionEnabled;
    private String storageType;
    private Instant creationTime;
    private Instant modificationTime;
    private String sourceVolume;
    private String sourceBucket;
    private long usedBytes;
    private long usedNamespace;
    private long quotaInBytes;
    private long quotaInNamespace;
    private String bucketLayout;
    private String owner;

    public Builder() {

    }

    public Builder withVolumeName(String volumeName) {
      this.volumeName = volumeName;
      return this;
    }

    public Builder withBucketName(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }

    public Builder withAcls(List<AclMetadata> acls) {
      this.acls = acls;
      return this;
    }

    public Builder withVersionEnabled(boolean isVersionEnabled) {
      this.isVersionEnabled = isVersionEnabled;
      return this;
    }

    public Builder withStorageType(String storageType) {
      this.storageType = storageType;
      return this;
    }

    public Builder withCreationTime(Instant creationTime) {
      this.creationTime = creationTime;
      return this;
    }

    public Builder withModificationTime(Instant modificationTime) {
      this.modificationTime = modificationTime;
      return this;
    }

    public Builder withSourceVolume(String sourceVolume) {
      this.sourceVolume = sourceVolume;
      return this;
    }

    public Builder withSourceBucket(String sourceBucket) {
      this.sourceBucket = sourceBucket;
      return this;
    }

    public Builder withUsedBytes(long usedBytes) {
      this.usedBytes = usedBytes;
      return this;
    }

    public Builder withUsedNamespace(long usedNamespace) {
      this.usedNamespace = usedNamespace;
      return this;
    }

    public Builder withQuotaInBytes(long quotaInBytes) {
      this.quotaInBytes = quotaInBytes;
      return this;
    }

    public Builder withQuotaInNamespace(long quotaInNamespace) {
      this.quotaInNamespace = quotaInNamespace;
      return this;
    }

    public Builder withBucketLayout(String bucketLayout) {
      this.bucketLayout = bucketLayout;
      return this;
    }

    public Builder withOwner(String owner) {
      this.owner = owner;
      return this;
    }

    /**
     * Constructs BucketMetadata.
     *
     * @return instance of BucketMetadata
     */
    public BucketMetadata build() {
      Preconditions.checkNotNull(volumeName);
      Preconditions.checkNotNull(bucketName);

      return new BucketMetadata(this);
    }

  }
}
