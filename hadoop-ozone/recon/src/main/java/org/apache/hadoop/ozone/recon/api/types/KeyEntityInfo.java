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
import org.apache.hadoop.hdds.client.ReplicationConfig;

import java.time.Instant;

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
  private long inStateSince;

  @JsonProperty("size")
  private long size;

  @JsonProperty("replicatedSize")
  private long replicatedSize;

  @JsonProperty("replicationInfo")
  private ReplicationConfig replicationConfig;

  public KeyEntityInfo() {
    key = "";
    path = "";
    inStateSince = Instant.now().toEpochMilli();
    size = 0L;
    replicatedSize = 0L;
    replicationConfig = null;
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
}
