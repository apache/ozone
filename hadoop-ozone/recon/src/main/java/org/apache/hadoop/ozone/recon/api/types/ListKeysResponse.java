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
import java.util.ArrayList;
import java.util.List;

/**
 * HTTP Response wrapped for listKeys requests.
 */
public class ListKeysResponse {
  /** Path status. */
  @JsonProperty("status")
  private ResponseStatus status;

  /** The current path request. */
  @JsonProperty("path")
  private String path;

  /** Amount of data mapped to all keys and files in a cluster across all DNs. */
  @JsonProperty("replicatedDataSize")
  private long replicatedDataSize;

  /** Amount of data mapped to all keys and files on a single DN. */
  @JsonProperty("unReplicatedDataSize")
  private long unReplicatedDataSize;

  /** last key sent. */
  @JsonProperty("lastKey")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private String lastKey;

  /** list of keys. */
  @JsonProperty("keys")
  private List<ReconBasicOmKeyInfo> keys;

  public ListKeysResponse() {
    this.status = ResponseStatus.OK;
    this.path = "";
    this.keys = new ArrayList<>();
    this.replicatedDataSize = -1L;
    this.unReplicatedDataSize = -1L;
    this.lastKey = "";
  }

  public ResponseStatus getStatus() {
    return this.status;
  }

  public void setStatus(ResponseStatus status) {
    this.status = status;
  }

  public long getReplicatedDataSize() {
    return replicatedDataSize;
  }

  public void setReplicatedDataSize(long replicatedDataSize) {
    this.replicatedDataSize = replicatedDataSize;
  }

  public long getUnReplicatedDataSize() {
    return unReplicatedDataSize;
  }

  public void setUnReplicatedDataSize(long unReplicatedDataSize) {
    this.unReplicatedDataSize = unReplicatedDataSize;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public List<ReconBasicOmKeyInfo> getKeys() {
    return keys;
  }

  public void setKeys(List<ReconBasicOmKeyInfo> keys) {
    this.keys = keys;
  }

  public String getLastKey() {
    return lastKey;
  }

  public void setLastKey(String lastKey) {
    this.lastKey = lastKey;
  }

}
