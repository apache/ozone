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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;

import java.time.Instant;
import java.util.Map;
import java.util.HashMap;

/**
 * A class that encapsulates OzoneKey.
 */
public class OzoneKey {

  public static final long DEFAULT_CREATION_TIME_VALUE = Long.MIN_VALUE;
  public static final long DEFAULT_MODIFICATION_TIME_VALUE = Long.MIN_VALUE;

  /**
   * Name of the Volume the Key belongs to.
   */
  private final String volumeName;
  /**
   * Name of the Bucket the Key belongs to.
   */
  private final String bucketName;
  /**
   * Name of the Key.
   */
  private final String name;
  /**
   * Size of the data.
   */
  private final long dataSize;
  /**
   * Creation time of the key.
   */
  private Instant creationTime;
  /**
   * Modification time of the key.
   */
  private Instant modificationTime;

  private ReplicationConfig replicationConfig;

  private Map<String, String> metadata = new HashMap<>();

  /**
   * Indicator if key is a file.
   */
  private final boolean isFile;

  /**
   * Constructs OzoneKey from OmKeyInfo.
   *
   */
  @SuppressWarnings("parameternumber")
  public OzoneKey(String volumeName, String bucketName,
      String keyName, long size, long creationTime,
      long modificationTime, ReplicationConfig replicationConfig,
      boolean isFile) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.name = keyName;
    this.dataSize = size;
    this.creationTime = Instant.ofEpochMilli(creationTime);
    this.modificationTime = Instant.ofEpochMilli(modificationTime);
    this.replicationConfig = replicationConfig;
    this.isFile = isFile;
  }

  @SuppressWarnings("parameternumber")
  public OzoneKey(String volumeName, String bucketName,
                  String keyName, long size, long creationTime,
                  long modificationTime, ReplicationConfig replicationConfig,
                  Map<String, String> metadata, boolean isFile) {
    this(volumeName, bucketName, keyName, size, creationTime,
        modificationTime, replicationConfig, isFile);
    this.metadata.putAll(metadata);
  }

  /**
   * Returns Volume Name associated with the Key.
   *
   * @return volumeName
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Returns Bucket Name associated with the Key.
   *
   * @return bucketName
   */
  public String getBucketName() {
    return bucketName;
  }

  /**
   * Returns the Key Name.
   *
   * @return keyName
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the size of the data.
   *
   * @return dataSize
   */
  public long getDataSize() {
    return dataSize;
  }

  /**
   * Returns the creation time of the key.
   *
   * @return creation time
   */
  public Instant getCreationTime() {
    return creationTime;
  }

  public Instant getCreationTime(Instant bucketCreationTime) {
    if (creationTime == Instant.ofEpochMilli(DEFAULT_CREATION_TIME_VALUE)) {
      return bucketCreationTime;
    }
    return creationTime;
  }

  /**
   * Returns the modification time of the key.
   *
   * @return modification time
   */
  public Instant getModificationTime() {
    return modificationTime;
  }

  public Instant getModificationTime(Instant bucketModificationTime) {
    if (modificationTime ==
        Instant.ofEpochMilli(DEFAULT_MODIFICATION_TIME_VALUE)) {
      return bucketModificationTime;
    }
    return modificationTime;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata.putAll(metadata);
  }

  @Deprecated
  @JsonIgnore
  public ReplicationType getReplicationType(
      ReplicationConfig bucketReplicationConfig) {
    if (replicationConfig == null) {
      return ReplicationType
          .fromProto(bucketReplicationConfig.getReplicationType());
    }
    return ReplicationType
        .fromProto(replicationConfig.getReplicationType());
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  public ReplicationConfig getReplicationConfig(
      ReplicationConfig bucketReplicationConfig) {
    if (replicationConfig == null) {
      return bucketReplicationConfig;
    }
    return replicationConfig;
  }

  /**
   * Returns indicator if key is a file.
   * @return file
   */
  public boolean isFile() {
    return isFile;
  }

  public static OzoneKey fromKeyInfo(OmKeyInfo keyInfo) {
    return new OzoneKey(keyInfo.getVolumeName(), keyInfo.getBucketName(),
        keyInfo.getKeyName(), keyInfo.getDataSize(), keyInfo.getCreationTime(),
        keyInfo.getModificationTime(), keyInfo.getReplicationConfig(),
        keyInfo.getMetadata(), keyInfo.isFile());
  }

}
