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

/**
 * Pending deletion data of a single bucket, as seen in the active OM DB copy held by Recon.
 * Keys come from the deletedTable and directories from the deletedDirTable.
 * All byte values are replicated sizes.
 */
public class BucketPendingDeletion {
  @JsonProperty("volumeName")
  private final String volumeName;
  @JsonProperty("bucketName")
  private final String bucketName;
  @JsonProperty("pendingDeletionKeys")
  private long pendingDeletionKeys;
  @JsonProperty("pendingDeletionKeyBytes")
  private long pendingDeletionKeyBytes;
  @JsonProperty("pendingDeletionDirs")
  private long pendingDeletionDirs;
  @JsonProperty("pendingDeletionDirBytes")
  private long pendingDeletionDirBytes;

  public BucketPendingDeletion(String volumeName, String bucketName) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
  }

  public void addKeys(long count, long replicatedBytes) {
    pendingDeletionKeys += count;
    pendingDeletionKeyBytes += replicatedBytes;
  }

  public void addDir(long replicatedBytes) {
    pendingDeletionDirs++;
    pendingDeletionDirBytes += replicatedBytes;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public long getPendingDeletionKeys() {
    return pendingDeletionKeys;
  }

  public long getPendingDeletionKeyBytes() {
    return pendingDeletionKeyBytes;
  }

  public long getPendingDeletionDirs() {
    return pendingDeletionDirs;
  }

  public long getPendingDeletionDirBytes() {
    return pendingDeletionDirBytes;
  }

  @JsonProperty("totalPendingDeletionBytes")
  public long getTotalPendingDeletionBytes() {
    return pendingDeletionKeyBytes + pendingDeletionDirBytes;
  }
}
