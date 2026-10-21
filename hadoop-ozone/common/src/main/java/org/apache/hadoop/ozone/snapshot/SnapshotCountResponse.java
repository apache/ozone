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

package org.apache.hadoop.ozone.snapshot;

import java.util.List;

/**
 * POJO for snapshot count API.
 */
public final class SnapshotCountResponse {
  private final long active;
  private final long deleted;
  private final long total;
  private final List<SnapshotBucketCount> buckets;

  public SnapshotCountResponse(long active, long deleted, long total, List<SnapshotBucketCount> buckets) {
    this.active = active;
    this.deleted = deleted;
    this.total = total;
    this.buckets = buckets;
  }

  public long getActive() {
    return active;
  }

  public long getDeleted() {
    return deleted;
  }

  public long getTotal() {
    return total;
  }

  public List<SnapshotBucketCount> getBuckets() {
    return buckets;
  }
}
