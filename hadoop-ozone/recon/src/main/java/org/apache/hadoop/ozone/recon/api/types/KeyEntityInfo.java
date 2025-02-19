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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import org.apache.hadoop.hdds.client.ReplicationConfig;

/**
 * POJO object wrapper for metadata of a given key/file.
 */
public class KeyEntityInfo {

  /** This is key table key of rocksDB and will help UI to implement pagination
   * where UI will use the last record key to send in API as preKeyPrefix. */
  @JsonProperty("key")
  private String key;

  /** Path of a key/file. */
  @JsonProperty("path")
  private String path;

  @JsonProperty("inStateSince")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long inStateSince;

  @JsonProperty("size")
  private long size;

  @JsonProperty("replicatedSize")
  private long replicatedSize;

  @JsonProperty("replicationInfo")
  private ReplicationConfig replicationConfig;

  /** key creation time. */
  @JsonProperty("creationTime")
  private long creationTime;

  /** key modification time. */
  @JsonProperty("modificationTime")
  private long modificationTime;

  /** Indicate if the path is a key for Web UI. */
  @JsonProperty("isKey")
  private boolean isKey;

  public KeyEntityInfo() {
    key = "";
    path = "";
    size = 0L;
    replicatedSize = 0L;
    replicationConfig = null;
    creationTime = Instant.now().toEpochMilli();
    modificationTime = Instant.now().toEpochMilli();
    isKey = true;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public long getInStateSince() {
    return inStateSince;
  }

  public void setInStateSince(long inStateSince) {
    this.inStateSince = inStateSince;
  }

  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public long getReplicatedSize() {
    return replicatedSize;
  }

  public void setReplicatedSize(long replicatedSize) {
    this.replicatedSize = replicatedSize;
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  public void setReplicationConfig(
      ReplicationConfig replicationConfig) {
    this.replicationConfig = replicationConfig;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(long creationTime) {
    this.creationTime = creationTime;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }

  public boolean isKey() {
    return isKey;
  }

  public void setIsKey(boolean isKey) {
    this.isKey = isKey;
  }
}
