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

package org.apache.hadoop.ozone.client;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;

/**
 * A class that encapsulates OzoneKey.
 */
public class OzoneKey {

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
   * Name of the Key owner.
   */
  private final String owner;
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

  private final Map<String, String> metadata = new HashMap<>();

  private final Map<String, String> tags = new HashMap<>();

  /**
   * Indicator if key is a file.
   */
  private final boolean isFile;

  @SuppressWarnings("parameternumber")
  public OzoneKey(String volumeName, String bucketName,
      String keyName, long size, long creationTime,
      long modificationTime, ReplicationConfig replicationConfig,
      boolean isFile, String owner) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.name = keyName;
    this.dataSize = size;
    this.creationTime = Instant.ofEpochMilli(creationTime);
    this.modificationTime = Instant.ofEpochMilli(modificationTime);
    this.replicationConfig = replicationConfig;
    this.isFile = isFile;
    this.owner = owner;
  }

  @SuppressWarnings("parameternumber")
  public OzoneKey(String volumeName, String bucketName,
                  String keyName, long size, long creationTime,
                  long modificationTime, ReplicationConfig replicationConfig,
                  Map<String, String> metadata, boolean isFile, String owner,
                  Map<String, String> tags) {
    this(volumeName, bucketName, keyName, size, creationTime,
        modificationTime, replicationConfig, isFile, owner);
    this.metadata.putAll(metadata);
    this.tags.putAll(tags);
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
   * Returns the Owner Name.
   *
   * @return keyName
   */
  public String getOwner() {
    return owner;
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

  /**
   * Returns the modification time of the key.
   *
   * @return modification time
   */
  public Instant getModificationTime() {
    return modificationTime;
  }

  /**
   * Returns the metadata of the key.
   *
   * @return key metadata.
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  /**
   * Returns the tags of the key.
   *
   * @return key tags.
   */
  public Map<String, String> getTags() {
    return tags;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata.putAll(metadata);
  }

  /**
   * Returns the replication type of the key.
   *
   * @return replicationType
   */
  @Deprecated
  @JsonIgnore
  public ReplicationType getReplicationType() {
    return ReplicationType
            .fromProto(replicationConfig.getReplicationType());
  }

  @Deprecated
  @JsonIgnore
  public int getReplicationFactor() {
    return replicationConfig.getRequiredNodes();
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  /**
   * Returns indicator if key is a file.
   * @return file
   */
  public boolean isFile() {
    return isFile;
  }

  /**
   * Constructs OzoneKey from OmKeyInfo.
   *
   */
  public static OzoneKey fromKeyInfo(OmKeyInfo keyInfo) {
    return new OzoneKey(keyInfo.getVolumeName(), keyInfo.getBucketName(),
        keyInfo.getKeyName(), keyInfo.getDataSize(), keyInfo.getCreationTime(),
        keyInfo.getModificationTime(), keyInfo.getReplicationConfig(),
        keyInfo.getMetadata(), keyInfo.isFile(), keyInfo.getOwnerName(),
        keyInfo.getTags());
  }

}
