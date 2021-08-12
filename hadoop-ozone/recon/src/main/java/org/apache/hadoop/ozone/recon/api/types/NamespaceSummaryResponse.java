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

/**
 * HTTP Response wrapped for a 'summary' request.
 */
public class NamespaceSummaryResponse {
  /** The namespace the request path is on. */
  @JsonProperty("type")
  private EntityType entityType;

  /** Total number of volumes under root, -1 for other types. */
  @JsonProperty("numVolume")
  private int numVolume;

  /** Total number of buckets for root/volume, -1 for other types. */
  @JsonProperty("numBucket")
  private int numBucket;

  /** Total number of directories for all types except key, -1 for key. */
  @JsonProperty("numDir")
  private int numTotalDir;

  /** Total number of keys. */
  @JsonProperty("numKey")
  private long numTotalKey;

  /** Path Status. */
  @JsonProperty("status")
  private ResponseStatus status;

  public NamespaceSummaryResponse(EntityType entityType) {
    this.entityType = entityType;
    this.numVolume = -1;
    this.numBucket = -1;
    this.numTotalDir = -1;
    this.numTotalKey = 0;
    this.status = ResponseStatus.OK;
  }

  public EntityType getEntityType() {
    return this.entityType;
  }

  public int getNumVolume() {
    return this.numVolume;
  }

  public int getNumBucket() {
    return this.numBucket;
  }

  public int getNumTotalDir() {
    return this.numTotalDir;
  }

  public long getNumTotalKey() {
    return this.numTotalKey;
  }

  public ResponseStatus getStatus() {
    return this.status;
  }

  public void setEntityType(EntityType entityType) {
    this.entityType = entityType;
  }

  public void setNumVolume(int numVolume) {
    this.numVolume = numVolume;
  }

  public void setNumBucket(int numBucket) {
    this.numBucket = numBucket;
  }

  public void setNumTotalDir(int numTotalDir) {
    this.numTotalDir = numTotalDir;
  }

  public void setNumTotalKey(long numTotalKey) {
    this.numTotalKey = numTotalKey;
  }

  public void setStatus(ResponseStatus status) {
    this.status = status;
  }
}
