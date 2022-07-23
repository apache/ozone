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

package org.apache.hadoop.ozone.om.helpers;

import java.time.Instant;

import org.apache.hadoop.hdds.client.ReplicationConfig;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Information about one initialized upload.
 */
public class OmMultipartUpload {

  private String volumeName;

  private String bucketName;

  private String keyName;

  private String uploadId;

  private Instant creationTime;

  private ReplicationConfig replicationConfig;

  public OmMultipartUpload(Builder objectBuild) {
    this.volumeName = objectBuild.volumeName;
    this.bucketName = objectBuild.bucketName;
    this.keyName = objectBuild.keyName;
    this.uploadId = objectBuild.uploadId;
    this.creationTime = objectBuild.creationTime;
    this.replicationConfig = objectBuild.replicationConfig;
  }

  public static OmMultipartUpload from(String key) {
    String[] split = key.split(OM_KEY_PREFIX);
    if (split.length < 5) {
      throw new IllegalArgumentException("Key " + key
          + " doesn't have enough segments to be a valid multipart upload key");
    }
    String uploadId = split[split.length - 1];
    String volume = split[1];
    String bucket = split[2];
    return new OmMultipartUpload.Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(key.substring(volume.length() + bucket.length() + 3,
            key.length() - uploadId.length() - 1))
        .setUploadID(uploadId).build();
  }

  public String getDbKey() {
    return OmMultipartUpload
        .getDbKey(volumeName, bucketName, keyName, uploadId);
  }

  public static String getDbKey(String volume, String bucket, String key,
      String uploadId) {
    return getDbKey(volume, bucket, key) + OM_KEY_PREFIX + uploadId;

  }

  public static String getDbKey(String volume, String bucket, String key) {
    return OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket +
        OM_KEY_PREFIX + key;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getKeyName() {
    return keyName;
  }

  public String getUploadId() {
    return uploadId;
  }

  public Instant getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(Instant creationTime) {
    this.creationTime = creationTime;
  }

  public void setReplicationConfig(ReplicationConfig replicationConfig) {
    this.replicationConfig = replicationConfig;
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  /**
   * Builder class of OmMultipartUpload.
   */
  public static class Builder {
    private String volumeName;

    private String bucketName;

    private String keyName;

    private String uploadId;

    private Instant creationTime;

    private ReplicationConfig replicationConfig;

    public Builder setVolumeName(String volumename) {
      this.volumeName = volumename;
      return this;
    }

    public Builder setBucketName(String bucketname) {
      this.bucketName = bucketname;
      return this;
    }

    public Builder setKeyName(String keyname) {
      this.keyName = keyname;
      return this;
    }

    public Builder setUploadID(String uploadid) {
      this.uploadId = uploadid;
      return this;
    }

    public Builder setCreationTime(Instant creationtime) {
      this.creationTime = creationtime;
      return this;
    }

    public Builder setReplicationConfig(ReplicationConfig replicationconfig) {
      this.replicationConfig = replicationconfig;
      return this;
    }

    //Return the final constructed builder object
    public OmMultipartUpload build() {
      return new OmMultipartUpload(this);
    }
  }

}
