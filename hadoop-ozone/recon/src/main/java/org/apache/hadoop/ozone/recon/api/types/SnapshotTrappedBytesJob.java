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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.ozone.om.helpers.BucketDeletedBytes;

/**
 * An asynchronous request to OM for the snapshot-trapped vs purgeable deleted bytes of a bucket.
 * The calculation runs on OM and is expensive, so Recon runs it in the background and the client polls.
 */
public class SnapshotTrappedBytesJob {

  @JsonProperty("jobId")
  private final String jobId;
  @JsonProperty("volumeName")
  private final String volumeName;
  @JsonProperty("bucketName")
  private final String bucketName;
  @JsonProperty("status")
  private volatile JobStatus status;
  @JsonProperty("submittedAt")
  private final long submittedAt;
  @JsonProperty("startedAt")
  private volatile long startedAt;
  @JsonProperty("completedAt")
  private volatile long completedAt;
  @JsonProperty("errorMessage")
  private volatile String errorMessage;
  @JsonProperty("result")
  private volatile Result result;

  public SnapshotTrappedBytesJob(String jobId, String volumeName, String bucketName) {
    this.jobId = jobId;
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.status = JobStatus.QUEUED;
    this.submittedAt = System.currentTimeMillis();
  }

  public String getJobId() {
    return jobId;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public JobStatus getStatus() {
    return status;
  }

  public long getSubmittedAt() {
    return submittedAt;
  }

  public long getStartedAt() {
    return startedAt;
  }

  public long getCompletedAt() {
    return completedAt;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public Result getResult() {
    return result;
  }

  @JsonIgnore
  public boolean isActive() {
    return status == JobStatus.QUEUED || status == JobStatus.RUNNING;
  }

  public void markRunning() {
    startedAt = System.currentTimeMillis();
    status = JobStatus.RUNNING;
  }

  public void markCompleted(BucketDeletedBytes deletedBytes) {
    result = new Result(deletedBytes);
    completedAt = System.currentTimeMillis();
    status = JobStatus.COMPLETED;
  }

  public void markFailed(String message) {
    errorMessage = message;
    completedAt = System.currentTimeMillis();
    status = JobStatus.FAILED;
  }

  /**
   * Status of a snapshot-trapped bytes job.
   */
  public enum JobStatus {
    QUEUED,
    RUNNING,
    COMPLETED,
    FAILED
  }

  /**
   * Snapshot-trapped vs purgeable split reported by OM. Byte values are replicated sizes.
   */
  public static final class Result {
    @JsonProperty("snapshotTrappedBytes")
    private final long snapshotTrappedBytes;
    @JsonProperty("purgeableBytes")
    private final long purgeableBytes;
    @JsonProperty("snapshotTrappedKeys")
    private final long snapshotTrappedKeys;
    @JsonProperty("purgeableKeys")
    private final long purgeableKeys;
    @JsonProperty("snapshotTrappedDirs")
    private final long snapshotTrappedDirs;
    @JsonProperty("purgeableDirs")
    private final long purgeableDirs;

    Result(BucketDeletedBytes deletedBytes) {
      this.snapshotTrappedBytes = deletedBytes.getSnapshotTrappedBytes();
      this.purgeableBytes = deletedBytes.getPurgeableBytes();
      this.snapshotTrappedKeys = deletedBytes.getSnapshotTrappedKeys();
      this.purgeableKeys = deletedBytes.getPurgeableKeys();
      this.snapshotTrappedDirs = deletedBytes.getSnapshotTrappedDirs();
      this.purgeableDirs = deletedBytes.getPurgeableDirs();
    }

    public long getSnapshotTrappedBytes() {
      return snapshotTrappedBytes;
    }

    public long getPurgeableBytes() {
      return purgeableBytes;
    }

    public long getSnapshotTrappedKeys() {
      return snapshotTrappedKeys;
    }

    public long getPurgeableKeys() {
      return purgeableKeys;
    }

    public long getSnapshotTrappedDirs() {
      return snapshotTrappedDirs;
    }

    public long getPurgeableDirs() {
      return purgeableDirs;
    }
  }
}
