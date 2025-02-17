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
import java.util.Collection;

/**
 * Class that represents the API response structure of Buckets.
 */
public class BucketsResponse {
  /**
   * Total count of buckets.
   */
  @JsonProperty("totalCount")
  private long totalCount;

  /**
   * An array of buckets.
   */
  @JsonProperty("buckets")
  private Collection<BucketObjectDBInfo> buckets;

  public BucketsResponse(long totalCount,
      Collection<BucketObjectDBInfo> buckets) {
    this.totalCount = totalCount;
    this.buckets = buckets;
  }

  public long getTotalCount() {
    return totalCount;
  }

  public Collection<BucketObjectDBInfo> getBuckets() {
    return buckets;
  }
}
