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

/**
 * Class to encapsulate volume level metrics from OM.
 */

public final class VolumeMetrics {
  private final long objectID;
  private final long bucketCount;
  private final long accessCount;
  private final long size;
  private final long creationTimeStamp;
  private final long lastUpdatedTimeStamp;

  private VolumeMetrics() {
    this(0L, 0L, 0L, 0L,
         0L, 0L);
  }

  private VolumeMetrics(long objectID,
                       long bucketCount,
                       long accessCount,
                       long size,
                       long creationTimeStamp,
                       long lastUpdatedTimeStamp) {
    this.objectID = objectID;
    this.bucketCount = bucketCount;
    this.accessCount = accessCount;
    this.size = size;
    this.creationTimeStamp = creationTimeStamp;
    this.lastUpdatedTimeStamp = lastUpdatedTimeStamp;
  }

  /**
   * Builder of VolumeMetrics.
   */
  public static class Builder {

    private long objectID;
    private long bucketCount;
    private long accessCount;
    private long size;
    private long creationTimeStamp;
    private long lastUpdatedTimeStamp;

    public VolumeMetrics build() {
      return new VolumeMetrics(objectID, bucketCount,
          accessCount, size, creationTimeStamp, lastUpdatedTimeStamp);
    }

    public Builder setObjectID(long objectID) {
      this.objectID = objectID;
      return this;
    }

    public Builder setBucketCount(long bucketCount) {
      this.bucketCount = bucketCount;
      return this;
    }

    public Builder setAccessCount(long accessCount) {
      this.accessCount = accessCount;
      return this;
    }

    public Builder setSize(long size) {
      this.size = size;
      return this;
    }

    public Builder setCreationTimeStamp(long creationTimeStamp) {
      this.creationTimeStamp = creationTimeStamp;
      return this;
    }

    public Builder setLastUpdatedTimeStamp(long lastUpdatedTimeStamp) {
      this.lastUpdatedTimeStamp = lastUpdatedTimeStamp;
      return this;
    }
  }
}
