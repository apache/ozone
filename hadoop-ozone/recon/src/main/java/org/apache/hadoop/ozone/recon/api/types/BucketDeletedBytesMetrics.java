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
import org.apache.hadoop.ozone.om.helpers.BucketDeletedBytes;

/**
 * Represents the snapshot-trapped vs purgeable deleted-data split for a bucket.
 * A value of -1 indicates the bucket has not yet been queried or the query failed.
 */
public class BucketDeletedBytesMetrics {

  @JsonProperty("volumeName")
  private final String volumeName;

  @JsonProperty("bucketName")
  private final String bucketName;

  private final BucketDeletedBytes deletedBytes;

  public BucketDeletedBytesMetrics(String volumeName, String bucketName, BucketDeletedBytes deletedBytes) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.deletedBytes = deletedBytes;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  @JsonProperty("snapshotTrappedBytes")
  public long getSnapshotTrappedBytes() {
    return deletedBytes.getSnapshotTrappedBytes();
  }

  @JsonProperty("purgeableBytes")
  public long getPurgeableBytes() {
    return deletedBytes.getPurgeableBytes();
  }

  @JsonProperty("snapshotTrappedKeys")
  public long getSnapshotTrappedKeys() {
    return deletedBytes.getSnapshotTrappedKeys();
  }

  @JsonProperty("purgeableKeys")
  public long getPurgeableKeys() {
    return deletedBytes.getPurgeableKeys();
  }

  @JsonProperty("snapshotTrappedDirs")
  public long getSnapshotTrappedDirs() {
    return deletedBytes.getSnapshotTrappedDirs();
  }

  @JsonProperty("purgeableDirs")
  public long getPurgeableDirs() {
    return deletedBytes.getPurgeableDirs();
  }

  @JsonProperty("purgeableKeyBytes")
  public long getPurgeableKeyBytes() {
    return deletedBytes.getPurgeableKeyBytes();
  }

  @JsonProperty("purgeableDirBytes")
  public long getPurgeableDirBytes() {
    return deletedBytes.getPurgeableDirBytes();
  }
}
