/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client;

import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class encapsulates the arguments that are
 * required for creating a bucket.
 */
public final class BucketArgs {

  /**
   * ACL Information.
   */
  private List<OzoneAcl> acls;
  /**
   * Bucket Version flag.
   */
  private Boolean versioning;
  /**
   * Type of storage to be used for this bucket.
   * [RAM_DISK, SSD, DISK, ARCHIVE]
   */
  private StorageType storageType;

  /**
   * Custom key/value metadata.
   */
  private Map<String, String> metadata;

  /**
   * Bucket encryption key name.
   */
  private String bucketEncryptionKey;
  private DefaultReplicationConfig defaultReplicationConfig;
  private final String sourceVolume;
  private final String sourceBucket;

  private long quotaInBytes;
  private long quotaInNamespace;

  private String owner;

  /**
   * Bucket Layout.
   */
  private BucketLayout bucketLayout = BucketLayout.DEFAULT;

  /**
   * Private constructor, constructed via builder.
   * @param versioning Bucket version flag.
   * @param storageType Storage type to be used.
   * @param acls list of ACLs.
   * @param metadata map of bucket metadata
   * @param bucketEncryptionKey bucket encryption key name
   * @param sourceVolume
   * @param sourceBucket
   * @param quotaInBytes Bucket quota in bytes.
   * @param quotaInNamespace Bucket quota in counts.
   * @param bucketLayout bucket layout.
   * @param owner owner of the bucket.
   * @param defaultReplicationConfig default replication config.
   */
  @SuppressWarnings("parameternumber")
  private BucketArgs(Boolean versioning, StorageType storageType,
      List<OzoneAcl> acls, Map<String, String> metadata,
      String bucketEncryptionKey, String sourceVolume, String sourceBucket,
      long quotaInBytes, long quotaInNamespace, BucketLayout bucketLayout,
      String owner, DefaultReplicationConfig defaultReplicationConfig) {
    this.acls = acls;
    this.versioning = versioning;
    this.storageType = storageType;
    this.metadata = metadata;
    this.bucketEncryptionKey = bucketEncryptionKey;
    this.sourceVolume = sourceVolume;
    this.sourceBucket = sourceBucket;
    this.quotaInBytes = quotaInBytes;
    this.quotaInNamespace = quotaInNamespace;
    this.bucketLayout = bucketLayout;
    this.owner = owner;
    this.defaultReplicationConfig = defaultReplicationConfig;
  }

  /**
   * Returns true if bucket version is enabled, else false.
   * @return isVersionEnabled
   */
  public Boolean getVersioning() {
    return versioning;
  }

  /**
   * Returns the type of storage to be used.
   * @return StorageType
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Returns the ACL's associated with this bucket.
   * @return {@literal List<OzoneAcl>}
   */
  public List<OzoneAcl> getAcls() {
    return acls;
  }

  /**
   * Custom metadata for the buckets.
   *
   * @return key value map
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  /**
   * Returns the bucket encryption key name.
   * @return bucket encryption key
   */
  public String getEncryptionKey() {
    return bucketEncryptionKey;
  }

  /**
   * Returns the bucket default replication config.
   * @return bucket's default Replication Config.
   */
  public DefaultReplicationConfig getDefaultReplicationConfig() {
    return this.defaultReplicationConfig;
  }

  /**
   * Returns new builder class that builds a OmBucketInfo.
   *
   * @return Builder
   */
  public static BucketArgs.Builder newBuilder() {
    return new BucketArgs.Builder();
  }

  public String getSourceVolume() {
    return sourceVolume;
  }

  public String getSourceBucket() {
    return sourceBucket;
  }

  /**
   * Returns Bucket Quota in bytes.
   * @return quotaInBytes.
   */
  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  /**
   * Returns Bucket Quota in key counts.
   * @return quotaInNamespace.
   */
  public long getQuotaInNamespace() {
    return quotaInNamespace;
  }

  /**
   * Returns the Bucket Layout.
   */
  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }

  /**
   * Returns the Owner Name.
   */
  public String getOwner() {
    return owner;
  }

  /**
   * Builder for OmBucketInfo.
   */
  public static class Builder {
    private Boolean versioning;
    private StorageType storageType;
    private List<OzoneAcl> acls;
    private Map<String, String> metadata;
    private String bucketEncryptionKey;
    private String sourceVolume;
    private String sourceBucket;
    private long quotaInBytes;
    private long quotaInNamespace;
    private BucketLayout bucketLayout;
    private String owner;
    private DefaultReplicationConfig defaultReplicationConfig;

    public Builder() {
      metadata = new HashMap<>();
      quotaInBytes = OzoneConsts.QUOTA_RESET;
      quotaInNamespace = OzoneConsts.QUOTA_RESET;
    }

    public BucketArgs.Builder setVersioning(Boolean versionFlag) {
      this.versioning = versionFlag;
      return this;
    }

    public BucketArgs.Builder setStorageType(StorageType storage) {
      this.storageType = storage;
      return this;
    }

    public BucketArgs.Builder setAcls(List<OzoneAcl> listOfAcls) {
      this.acls = listOfAcls;
      return this;
    }

    public BucketArgs.Builder addMetadata(String key, String value) {
      this.metadata.put(key, value);
      return this;
    }

    public BucketArgs.Builder setBucketEncryptionKey(String bek) {
      this.bucketEncryptionKey = bek;
      return this;
    }

    public BucketArgs.Builder setSourceVolume(String volume) {
      sourceVolume = volume;
      return this;
    }

    public BucketArgs.Builder setSourceBucket(String bucket) {
      sourceBucket = bucket;
      return this;
    }

    public BucketArgs.Builder setQuotaInBytes(long quota) {
      quotaInBytes = quota;
      return this;
    }

    public BucketArgs.Builder setQuotaInNamespace(long quota) {
      quotaInNamespace = quota;
      return this;
    }

    public BucketArgs.Builder setBucketLayout(BucketLayout buckLayout) {
      bucketLayout = buckLayout;
      return this;
    }

    public BucketArgs.Builder setOwner(String ownerName) {
      owner = ownerName;
      return this;
    }

    public BucketArgs.Builder setDefaultReplicationConfig(
        DefaultReplicationConfig defaultReplConfig) {
      defaultReplicationConfig = defaultReplConfig;
      return this;
    }

    /**
     * Constructs the BucketArgs.
     * @return instance of BucketArgs.
     */
    public BucketArgs build() {
      return new BucketArgs(versioning, storageType, acls, metadata,
          bucketEncryptionKey, sourceVolume, sourceBucket, quotaInBytes,
          quotaInNamespace, bucketLayout, owner, defaultReplicationConfig);
    }
  }
}
