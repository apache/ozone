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
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

/**
 * HTTP Response wrapped for a 'summary' request.
 */
public class NamespaceSummaryResponse {
  /** Path for metadata summary. */
  @JsonProperty("path")
  private String path;

  /** The namespace the request path is on. */
  @JsonProperty("type")
  private EntityType entityType;

  /** Count stats which tells the number of volumes/buckets/dir/files etc. */
  @JsonProperty("countStats")
  private CountStats countStats;

  @JsonProperty("objectInfo")
  private ObjectDBInfo objectDBInfo;

  /** Path Status. */
  @JsonProperty("status")
  private ResponseStatus status;

  /**
   * Returns new builder class that builds a NamespaceSummaryResponse.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  public NamespaceSummaryResponse(Builder b) {
    this.path = b.path;
    this.entityType = b.entityType;
    this.countStats = b.countStats;
    this.objectDBInfo = b.objectDBInfo;
    this.status = b.status;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public CountStats getCountStats() {
    return countStats;
  }

  public void setCountStats(CountStats countStats) {
    this.countStats = countStats;
  }

  public EntityType getEntityType() {
    return this.entityType;
  }

  public ResponseStatus getStatus() {
    return this.status;
  }

  public void setEntityType(EntityType entityType) {
    this.entityType = entityType;
  }

  public void setStatus(ResponseStatus status) {
    this.status = status;
  }

  public ObjectDBInfo getObjectDBInfo() {
    return objectDBInfo;
  }

  public void setObjectDBInfo(ObjectDBInfo objectDBInfo) {
    this.objectDBInfo = objectDBInfo;
  }

  /**
   * Builder for NamespaceSummaryResponse.
   */
  public static final class Builder {
    private String path;
    private EntityType entityType;
    private CountStats countStats;
    private ObjectDBInfo objectDBInfo;
    private ResponseStatus status;

    public Builder() {
      // Default values
      this.path = StringUtils.EMPTY;
      this.entityType = EntityType.ROOT;
    }

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public Builder setEntityType(
        EntityType entityType) {
      this.entityType = entityType;
      return this;
    }

    public Builder setCountStats(
        CountStats countStats) {
      this.countStats = countStats;
      return this;
    }

    public Builder setObjectDBInfo(
        ObjectDBInfo objectDBInfo) {
      this.objectDBInfo = objectDBInfo;
      return this;
    }

    public Builder setStatus(
        ResponseStatus status) {
      this.status = status;
      return this;
    }

    public NamespaceSummaryResponse build() {
      Objects.requireNonNull(path, "path == null");
      Objects.requireNonNull(entityType, "entityType == null");
      Objects.requireNonNull(status, "status == null");

      return new NamespaceSummaryResponse(this);
    }
  }

}
