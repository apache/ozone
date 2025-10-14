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

package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;

/**
 * Encapsulates the low level bucket info.
 */
public class BucketObjectDBInfo extends ObjectDBInfo {
  @JsonProperty("volumeName")
  private String volumeName;

  @JsonProperty("storageType")
  private StorageType storageType;

  @JsonProperty("versioning")
  private boolean isVersioningEnabled;

  @JsonProperty("usedBytes")
  private long usedBytes;

  @JsonProperty("snapshotUsedBytes")
  private long snapshotUsedBytes;

  @JsonProperty("encryptionInfo")
  private BucketEncryptionKeyInfo bekInfo;

  @JsonProperty("replicationConfigInfo")
  private DefaultReplicationConfig defaultReplicationConfig;

  @JsonProperty("sourceVolume")
  private String sourceVolume;

  @JsonProperty("sourceBucket")
  private String sourceBucket;

  @JsonProperty("bucketLayout")
  private BucketLayout bucketLayout;

  @JsonProperty("owner")
  private String owner;

  public BucketObjectDBInfo() {

  }

  public BucketObjectDBInfo(OmBucketInfo omBucketInfo) {
    super.setMetadata(omBucketInfo.getMetadata());
    super.setName(omBucketInfo.getBucketName());
    super.setQuotaInBytes(omBucketInfo.getQuotaInBytes());
    super.setQuotaInNamespace(omBucketInfo.getQuotaInNamespace());
    super.setUsedNamespace(omBucketInfo.getUsedNamespace());
    super.setCreationTime(omBucketInfo.getCreationTime());
    super.setModificationTime(omBucketInfo.getModificationTime());
    super.setAcls(AclMetadata.fromOzoneAcls(omBucketInfo.getAcls()));
    this.volumeName = omBucketInfo.getVolumeName();
    this.sourceBucket = omBucketInfo.getSourceBucket();
    this.sourceVolume = omBucketInfo.getSourceVolume();
    this.isVersioningEnabled = omBucketInfo.getIsVersionEnabled();
    this.storageType = omBucketInfo.getStorageType();
    this.defaultReplicationConfig = omBucketInfo.getDefaultReplicationConfig();
    this.bucketLayout = omBucketInfo.getBucketLayout();
    this.owner = omBucketInfo.getOwner();
    this.bekInfo = omBucketInfo.getEncryptionKeyInfo();
    this.usedBytes = omBucketInfo.getUsedBytes();
    this.snapshotUsedBytes = omBucketInfo.getSnapshotUsedBytes();
  }

  public String getVolumeName() {
    return volumeName;
  }

  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  public void setStorageType(StorageType storageType) {
    this.storageType = storageType;
  }

  public long getUsedBytes() {
    return usedBytes;
  }

  public long getSnapshotUsedBytes() {
    return snapshotUsedBytes;
  }

  public void setUsedBytes(long usedBytes) {
    this.usedBytes = usedBytes;
  }

  public BucketEncryptionKeyInfo getBekInfo() {
    return bekInfo;
  }

  public void setBekInfo(BucketEncryptionKeyInfo bekInfo) {
    this.bekInfo = bekInfo;
  }

  public DefaultReplicationConfig getDefaultReplicationConfig() {
    return defaultReplicationConfig;
  }

  public void setDefaultReplicationConfig(
      DefaultReplicationConfig defaultReplicationConfig) {
    this.defaultReplicationConfig = defaultReplicationConfig;
  }

  public String getSourceVolume() {
    return sourceVolume;
  }

  public void setSourceVolume(String sourceVolume) {
    this.sourceVolume = sourceVolume;
  }

  public String getSourceBucket() {
    return sourceBucket;
  }

  public void setSourceBucket(String sourceBucket) {
    this.sourceBucket = sourceBucket;
  }

  public boolean isVersioningEnabled() {
    return isVersioningEnabled;
  }

  public void setVersioningEnabled(boolean versioningEnabled) {
    isVersioningEnabled = versioningEnabled;
  }

  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }

  public void setBucketLayout(BucketLayout bucketLayout) {
    this.bucketLayout = bucketLayout;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

}
