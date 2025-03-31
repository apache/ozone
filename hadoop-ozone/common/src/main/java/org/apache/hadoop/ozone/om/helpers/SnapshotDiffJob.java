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

package org.apache.hadoop.ozone.om.helpers;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotDiffJobProto;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus;

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
  private boolean disableNativeDiff;
  private long totalDiffEntries;

  // Reason tells why the job was FAILED. It should be set only if job status
  // is FAILED.
  private String reason;

  // Represents current sub-status of the job if the job is in IN_PROGRESS state.
  private SubStatus subStatus;
  // percentage of keys processed in the Object-ID generation phase.
  // This is the most time-consuming phase as it loads both snapshots
  // and reads from it populating the ObjectID-key map.
  private double keysProcessedPct;

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
                         boolean disableNativeDiff,
                         long totalDiffEntries,
                         SubStatus subStatus,
                         double keysProcessedPct) {
    this.creationTime = creationTime;
    this.jobId = jobId;
    this.status = jobStatus;
    this.volume = volume;
    this.bucket = bucket;
    this.fromSnapshot = fromSnapshot;
    this.toSnapshot = toSnapshot;
    this.forceFullDiff = forceFullDiff;
    this.disableNativeDiff = disableNativeDiff;
    this.totalDiffEntries = totalDiffEntries;
    this.reason = StringUtils.EMPTY;
    this.subStatus = subStatus;
    this.keysProcessedPct = keysProcessedPct;
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

  public String getReason() {
    return reason;
  }

  public void setReason(String reason) {
    this.reason = reason;
  }

  public boolean isNativeDiffDisabled() {
    return disableNativeDiff;
  }

  public void disableNativeDiff(boolean disableNativeDiffVal) {
    this.disableNativeDiff = disableNativeDiffVal;
  }

  public SubStatus getSubStatus() {
    return subStatus;
  }

  public void setSubStatus(SubStatus subStatus) {
    this.subStatus = subStatus;
  }

  public double getKeysProcessedPct() {
    return keysProcessedPct;
  }

  public void setKeysProcessedPct(double keysProcessedPct) {
    this.keysProcessedPct = keysProcessedPct;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("creationTime : ").append(creationTime)
        .append(", jobId: ").append(jobId)
        .append(", status: ").append(status)
        .append(", volume: ").append(volume)
        .append(", bucket: ").append(bucket)
        .append(", fromSnapshot: ").append(fromSnapshot)
        .append(", toSnapshot: ").append(toSnapshot)
        .append(", forceFullDiff: ").append(forceFullDiff)
        .append(", disableNativeDiff: ").append(disableNativeDiff)
        .append(", totalDiffEntries: ").append(totalDiffEntries);

    if (StringUtils.isNotEmpty(reason)) {
      sb.append(", reason: ").append(reason);
    }
    if (status.equals(JobStatus.IN_PROGRESS) && subStatus != null) {
      sb.append(", subStatus: ").append(status);
      if (subStatus.equals(SubStatus.OBJECT_ID_MAP_GEN_FSO) ||
          subStatus.equals(SubStatus.OBJECT_ID_MAP_GEN_OBS)) {
        sb.append(String.format(", keysProcessedPercent: %.2f", keysProcessedPct));
      }
    }
    return sb.toString();
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
          Objects.equals(this.totalDiffEntries, otherJob.totalDiffEntries) &&
          Objects.equals(this.disableNativeDiff, otherJob.disableNativeDiff)
          && Objects.equals(this.reason, otherJob.reason) &&
          Objects.equals(this.subStatus, otherJob.subStatus) &&
          Objects.equals(this.keysProcessedPct, otherJob.keysProcessedPct);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(creationTime, jobId, status, volume, bucket,
        fromSnapshot, toSnapshot, forceFullDiff, disableNativeDiff,
        totalDiffEntries, reason, subStatus, keysProcessedPct);
  }

  public SnapshotDiffJobProto toProtoBuf() {
    return SnapshotDiffJobProto.newBuilder()
        .setCreationTime(creationTime)
        .setJobId(jobId)
        .setStatus(status.toProtobuf())
        .setVolume(volume)
        .setBucket(bucket)
        .setFromSnapshot(fromSnapshot)
        .setToSnapshot(toSnapshot)
        .setForceFullDiff(forceFullDiff)
        .setDisableNativeDiff(disableNativeDiff)
        .setTotalDiffEntries(totalDiffEntries)
        .setSubStatus(subStatus.toProtoBuf())
        .setKeysProcessedPct(keysProcessedPct)
        .build();
  }

  public static SnapshotDiffJob getFromProtoBuf(
      SnapshotDiffJobProto diffJobProto) {
    return new SnapshotDiffJob(
        diffJobProto.getCreationTime(),
        diffJobProto.getJobId(),
        JobStatus.fromProtobuf(diffJobProto.getStatus()),
        diffJobProto.getVolume(),
        diffJobProto.getBucket(),
        diffJobProto.getFromSnapshot(),
        diffJobProto.getToSnapshot(),
        diffJobProto.getForceFullDiff(),
        diffJobProto.getDisableNativeDiff(),
        diffJobProto.getTotalDiffEntries(),
        SubStatus.fromProtoBuf(diffJobProto.getSubStatus()),
        diffJobProto.getKeysProcessedPct());
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
    public Class<SnapshotDiffJob> getTypeClass() {
      return SnapshotDiffJob.class;
    }

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
