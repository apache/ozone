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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;

/**
 * Encapsulates the low level bucket info.
 */
public class BucketObjectDBInfo extends ObjectDBInfo {
  /** volume name from om db. */
  @JsonProperty("volumeName")
  private String volumeName;

  /** storage type for the object. */
  @JsonProperty("storageType")
  private StorageType storageType;

  /** is versioning enabled for the object. */
  @JsonProperty("versioning")
  private boolean isVersioningEnabled;

  /** used bytes for the object. */
  @JsonProperty("usedBytes")
  private String usedBytes;

  /**
   * Bucket encryption key info if encryption is enabled.
   */
  @JsonProperty("encryptionInfo")
  private BucketEncryptionKeyInfo bekInfo;

  /**
   * Optional default replication for bucket.
   */
  @JsonProperty("replicationConfigInfo")
  private DefaultReplicationConfig defaultReplicationConfig;

  /** source volume. */
  @JsonProperty("sourceVolume")
  private String sourceVolume;

  /** source bucket. */
  @JsonProperty("sourceBucket")
  private String sourceBucket;

  /**
   * Bucket Layout.
   */
  @JsonProperty("bucketLayout")
  private BucketLayout bucketLayout;

  /**
   * bucket owner.
   */
  @JsonProperty("owner")
  private String owner;

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

  public String getUsedBytes() {
    return usedBytes;
  }

  public void setUsedBytes(String usedBytes) {
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
