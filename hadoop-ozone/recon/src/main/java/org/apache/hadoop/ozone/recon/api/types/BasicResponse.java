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
 * HTTP Response wrapped for a 'basic' request.
 */
public class BasicResponse {
  /** The namespace the request path is on. */
  @JsonProperty("type")
  private EntityType entityType;

  /** Total number of buckets for volume, 0 for other types. */
  @JsonProperty("bucket")
  private int numTotalBucket;

  /** Total number of directories for a bucket or directory, 0 for others. */
  @JsonProperty("dir")
  private int numTotalDir;

  /** Total number of keys for a bucket or directory, 0 for others. */
  @JsonProperty("key")
  private int numTotalKey;

  /** Path Status. */
  @JsonProperty("status")
  private NamespaceResponseCode status;

  public BasicResponse(EntityType entityType) {
    this.entityType = entityType;
    this.numTotalBucket = 0;
    this.numTotalDir = 0;
    this.numTotalKey = 0;
    this.status = NamespaceResponseCode.OK;
  }

  public EntityType getEntityType() {
    return this.entityType;
  }

  public int getNumTotalBucket() {
    return this.numTotalBucket;
  }

  public int getNumTotalDir() {
    return this.numTotalDir;
  }

  public int getNumTotalKey() {
    return this.numTotalKey;
  }

  public NamespaceResponseCode getStatus() {
    return this.status;
  }

  public void setEntityType(EntityType entityType) {
    this.entityType = entityType;
  }

  public void setNumTotalBucket(int numTotalBucket) {
    this.numTotalBucket = numTotalBucket;
  }

  public void setNumTotalDir(int numTotalDir) {
    this.numTotalDir = numTotalDir;
  }

  public void setNumTotalKey(int numTotalKey) {
    this.numTotalKey = numTotalKey;
  }

  public void setStatus(NamespaceResponseCode status) {
    this.status = status;
  }
}
