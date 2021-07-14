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
 * 'type': the namespace the request path is on.
 * 'bucket': Total number of buckets for volume, 0 for other types.
 * 'dir': Total number of directories for a bucket or directory, 0 for others.
 * 'key': Total number of keys for a bucket or directory, 0 for others.
 * 'pathNotFound': set to true if request path is valid
 */
public class BasicResponse {
  @JsonProperty("type")
  private EntityType entityType;

  @JsonProperty("bucket")
  private int totalBucket;

  @JsonProperty("dir")
  private int totalDir;

  @JsonProperty("key")
  private int totalKey;

  @JsonProperty("pathNotFound")
  private boolean pathNotFound;

  public BasicResponse(EntityType entityType) {
    this.entityType = entityType;
    this.totalBucket = 0;
    this.totalDir = 0;
    this.totalKey = 0;
    this.pathNotFound = false;
  }

  public EntityType getEntityType() {
    return this.entityType;
  }

  public int getTotalBucket() {
    return this.totalBucket;
  }

  public int getTotalDir() {
    return this.totalDir;
  }

  public int getTotalKey() {
    return this.totalKey;
  }

  public boolean isPathNotFound() {
    return this.pathNotFound;
  }

  public void setEntityType(EntityType entityType) {
    this.entityType = entityType;
  }

  public void setTotalBucket(int totalBucket) {
    this.totalBucket = totalBucket;
  }

  public void setTotalDir(int totalDir) {
    this.totalDir = totalDir;
  }

  public void setTotalKey(int totalKey) {
    this.totalKey = totalKey;
  }

  public void setPathNotFound(boolean pathNotFound) {
    this.pathNotFound = pathNotFound;
  }
}
