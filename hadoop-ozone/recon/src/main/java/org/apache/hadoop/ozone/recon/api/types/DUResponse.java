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
import java.util.ArrayList;
import java.util.List;

/**
 * HTTP Response wrapped for Disk Usage requests.
 */
public class DUResponse {
  /** Path status. */
  @JsonProperty("status")
  private ResponseStatus status;

  /** The current path request. */
  @JsonProperty("path")
  private String path;

  /** Total size under current path.*/
  @JsonProperty("size")
  private long size;

  /** Total size with replicas counted.*/
  @JsonProperty("sizeWithReplica")
  private long sizeWithReplica;

  /** The number of subpaths under the request path. */
  @JsonProperty("subPathCount")
  private int count;

  /** Encapsulates a DU instance for a subpath. */
  @JsonProperty("subPaths")
  private List<DiskUsage> duData;

  @JsonProperty("sizeDirectKey")
  private long keySize;

  public DUResponse() {
    this.status = ResponseStatus.OK;
    this.duData = new ArrayList<>();
    // by default, the replication feature is disabled
    this.sizeWithReplica = -1L;
    this.keySize = -1L;
  }

  public ResponseStatus getStatus() {
    return this.status;
  }

  public void setStatus(ResponseStatus status) {
    this.status = status;
  }

  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public long getSizeWithReplica() {
    return sizeWithReplica;
  }

  public void setSizeWithReplica(long sizeWithReplica) {
    this.sizeWithReplica = sizeWithReplica;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public int getCount() {
    return count;
  }

  public long getKeySize() {
    return keySize;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public List<DiskUsage> getDuData() {
    return duData;
  }

  public void setDuData(List<DiskUsage> duData) {
    this.duData = duData;
  }

  public void setKeySize(long keySize) {
    this.keySize = keySize;
  }

  /**
   * DU info for a path (path name, data size).
   */
  public static class DiskUsage {
    /** The subpath name. */
    @JsonProperty("path")
    private String subpath;

    /** Disk usage without replication under the subpath. */
    @JsonProperty("size")
    private long size;

    /** Disk usage with replication under the subpath. */
    @JsonProperty("sizeWithReplica")
    private long sizeWithReplica;

    /** Indicate if the path is a key for Web UI. */
    @JsonProperty("isKey")
    private boolean isKey;

    public DiskUsage() {
      this.sizeWithReplica = -1L;
      this.isKey = false;
    }

    public long getSize() {
      return size;
    }

    public String getSubpath() {
      return subpath;
    }

    public long getSizeWithReplica() {
      return sizeWithReplica;
    }

    public void setSize(long size) {
      this.size = size;
    }

    public void setSubpath(String subpath) {
      this.subpath = subpath;
    }

    public void setSizeWithReplica(long sizeWithReplica) {
      this.sizeWithReplica = sizeWithReplica;
    }

    public void setKey(boolean key) {
      isKey = key;
    }

    public boolean isKey() {
      return isKey;
    }
  }
}
