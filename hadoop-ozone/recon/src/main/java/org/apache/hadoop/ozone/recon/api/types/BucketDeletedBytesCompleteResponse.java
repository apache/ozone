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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.ozone.recon.api.BucketDeletedBytesMetricsService;

/**
 * Response returned when bucket deleted-bytes collection is complete.
 * Includes the aggregated purgeable vs snapshot-trapped totals, purgeable key/directory counts, and the purgeable
 * size split between deleted keys and files under deleted directories (-1 if any OM response lacked the split).
 */
public class BucketDeletedBytesCompleteResponse {

  @JsonProperty("status")
  private final BucketDeletedBytesMetricsService.MetricCollectionStatus status;

  @JsonProperty("totalBucketsQueried")
  private final int totalBucketsQueried;

  @JsonProperty("totalBucketQueriesFailed")
  private final long totalBucketQueriesFailed;

  @JsonProperty("totalPurgeableSize")
  private final long totalPurgeableSize;

  @JsonProperty("totalSnapshotTrappedSize")
  private final long totalSnapshotTrappedSize;

  @JsonProperty("totalPurgeableKeys")
  private final long totalPurgeableKeys;

  @JsonProperty("totalPurgeableDirs")
  private final long totalPurgeableDirs;

  @JsonProperty("totalPurgeableKeySize")
  private final long totalPurgeableKeySize;

  @JsonProperty("totalPurgeableDirSize")
  private final long totalPurgeableDirSize;

  @JsonCreator
  @SuppressWarnings("checkstyle:ParameterNumber")
  public BucketDeletedBytesCompleteResponse(
      @JsonProperty("status") BucketDeletedBytesMetricsService.MetricCollectionStatus status,
      @JsonProperty("totalBucketsQueried") int totalBucketsQueried,
      @JsonProperty("totalBucketQueriesFailed") long totalBucketQueriesFailed,
      @JsonProperty("totalPurgeableSize") long totalPurgeableSize,
      @JsonProperty("totalSnapshotTrappedSize") long totalSnapshotTrappedSize,
      @JsonProperty("totalPurgeableKeys") long totalPurgeableKeys,
      @JsonProperty("totalPurgeableDirs") long totalPurgeableDirs,
      @JsonProperty("totalPurgeableKeySize") long totalPurgeableKeySize,
      @JsonProperty("totalPurgeableDirSize") long totalPurgeableDirSize) {
    this.status = status;
    this.totalBucketsQueried = totalBucketsQueried;
    this.totalBucketQueriesFailed = totalBucketQueriesFailed;
    this.totalPurgeableSize = totalPurgeableSize;
    this.totalSnapshotTrappedSize = totalSnapshotTrappedSize;
    this.totalPurgeableKeys = totalPurgeableKeys;
    this.totalPurgeableDirs = totalPurgeableDirs;
    this.totalPurgeableKeySize = totalPurgeableKeySize;
    this.totalPurgeableDirSize = totalPurgeableDirSize;
  }

  public BucketDeletedBytesMetricsService.MetricCollectionStatus getStatus() {
    return status;
  }

  public int getTotalBucketsQueried() {
    return totalBucketsQueried;
  }

  public long getTotalBucketQueriesFailed() {
    return totalBucketQueriesFailed;
  }

  public long getTotalPurgeableSize() {
    return totalPurgeableSize;
  }

  public long getTotalSnapshotTrappedSize() {
    return totalSnapshotTrappedSize;
  }

  public long getTotalPurgeableKeys() {
    return totalPurgeableKeys;
  }

  public long getTotalPurgeableDirs() {
    return totalPurgeableDirs;
  }

  public long getTotalPurgeableKeySize() {
    return totalPurgeableKeySize;
  }

  public long getTotalPurgeableDirSize() {
    return totalPurgeableDirSize;
  }
}
