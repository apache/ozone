/**
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
package org.apache.hadoop.ozone.om.snapshot;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;

/**
 * POJO for Snapshot diff job.
 */
public class SnapshotDiffJob {

  private static final Codec<SnapshotDiffJob> CODEC =
      new SnapshotDiffJobCodec();

  public static Codec<SnapshotDiffJob> getCodec() {
    return CODEC;
  }

  private long creationTime;
  private String jobId;
  private JobStatus status;
  private String volume;
  private String bucket;
  private String fromSnapshot;
  private String toSnapshot;
  private boolean forceFullDiff;
  private long totalDiffEntries;

  // Default constructor for Jackson Serializer.
  public SnapshotDiffJob() {

  }

  @SuppressWarnings("parameternumber")
  public SnapshotDiffJob(long creationTime,
                         String jobId,
                         JobStatus jobStatus,
                         String volume,
                         String bucket,
                         String fromSnapshot,
                         String toSnapshot,
                         boolean forceFullDiff,
                         long totalDiffEntries) {
    this.creationTime = creationTime;
    this.jobId = jobId;
    this.status = jobStatus;
    this.volume = volume;
    this.bucket = bucket;
    this.fromSnapshot = fromSnapshot;
    this.toSnapshot = toSnapshot;
    this.forceFullDiff = forceFullDiff;
    this.totalDiffEntries = totalDiffEntries;
  }

  public String getJobId() {
    return jobId;
  }

  public JobStatus getStatus() {
    return status;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public void setStatus(JobStatus status) {
    this.status = status;
  }

  public String getVolume() {
    return volume;
  }

  public void setVolume(String volume) {
    this.volume = volume;
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public String getFromSnapshot() {
    return fromSnapshot;
  }

  public void setFromSnapshot(String fromSnapshot) {
    this.fromSnapshot = fromSnapshot;
  }

  public String getToSnapshot() {
    return toSnapshot;
  }

  public void setToSnapshot(String toSnapshot) {
    this.toSnapshot = toSnapshot;
  }

  public boolean isForceFullDiff() {
    return forceFullDiff;
  }

  public void setForceFullDiff(boolean forceFullDiff) {
    this.forceFullDiff = forceFullDiff;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(long creationTime) {
    this.creationTime = creationTime;
  }

  public long getTotalDiffEntries() {
    return totalDiffEntries;
  }

  public void setTotalDiffEntries(long totalDiffEntries) {
    this.totalDiffEntries = totalDiffEntries;
  }

  @Override
  public String toString() {
    return "creationTime : " + creationTime +
        ", jobId: " + jobId +
        ", status: " + status +
        ", volume: " + volume +
        ", bucket: " + bucket +
        ", fromSnapshot: " + fromSnapshot +
        ", toSnapshot: " + toSnapshot +
        ", forceFullDiff: " + forceFullDiff +
        ", totalDiffEntries: " + totalDiffEntries;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other instanceof SnapshotDiffJob) {
      SnapshotDiffJob otherJob = (SnapshotDiffJob) other;
      return Objects.equals(this.creationTime, otherJob.creationTime) &&
          Objects.equals(this.jobId, otherJob.jobId) &&
          Objects.equals(this.status, otherJob.status) &&
          Objects.equals(this.volume, otherJob.volume) &&
          Objects.equals(this.bucket, otherJob.bucket) &&
          Objects.equals(this.fromSnapshot, otherJob.fromSnapshot) &&
          Objects.equals(this.toSnapshot, otherJob.toSnapshot) &&
          Objects.equals(this.forceFullDiff, otherJob.forceFullDiff) &&
          Objects.equals(this.totalDiffEntries, otherJob.totalDiffEntries);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(creationTime, jobId, status, volume, bucket,
        fromSnapshot, toSnapshot, forceFullDiff, totalDiffEntries);
  }

  /**
   * Codec to encode SnapshotDiffJob as byte array.
   */
  private static final class SnapshotDiffJobCodec
      implements Codec<SnapshotDiffJob> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public byte[] toPersistedFormat(SnapshotDiffJob object)
        throws IOException {
      return MAPPER.writeValueAsBytes(object);
    }

    @Override
    public SnapshotDiffJob fromPersistedFormat(byte[] rawData)
        throws IOException {
      return MAPPER.readValue(rawData, SnapshotDiffJob.class);
    }

    @Override
    public SnapshotDiffJob copyObject(SnapshotDiffJob object) {
      // Note: Not really a "copy". from OmDBDiffReportEntryCodec
      return object;
    }
  }
}
