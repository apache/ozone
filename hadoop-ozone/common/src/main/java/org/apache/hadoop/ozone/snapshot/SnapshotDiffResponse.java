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
package org.apache.hadoop.ozone.snapshot;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotDiffResponse.JobStatusProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotDiffResponse.CancelStatusProto;

/**
 * POJO for Snapshot Diff Response.
 */
public class SnapshotDiffResponse {

  /**
   * Snapshot diff job status enum.
   */
  public enum JobStatus {
    QUEUED,
    IN_PROGRESS,
    DONE,
    REJECTED,
    FAILED,
    CANCELED;

    public JobStatusProto toProtobuf() {
      return JobStatusProto.valueOf(this.name());
    }

    public static JobStatus fromProtobuf(JobStatusProto jobStatusProto) {
      return JobStatus.valueOf(jobStatusProto.name());
    }
  }

  /**
   * Snapshot diff cancel status enum.
   */
  public enum CancelStatus {
    JOB_NOT_CANCELED("Job is not canceled"),
    NEW_JOB("Cannot cancel a newly submitted job"),
    JOB_DONE("Job is DONE, cancel failed"),
    INVALID_STATUS_TRANSITION("Job is not IN_PROGRESS, cancel failed"),
    JOB_ALREADY_CANCELED("Job has already been canceled"),
    CANCEL_SUCCESS("Job has successfully been canceled");

    private final String description;

    CancelStatus(String description) {
      this.description = description;
    }

    public String getDescription() {
      return description;
    }

    public CancelStatusProto toProtobuf() {
      return CancelStatusProto.valueOf(this.name());
    }

    public static CancelStatus fromProtobuf(
        CancelStatusProto cancelStatusProto) {
      return CancelStatus.valueOf(cancelStatusProto.name());
    }
  }

  private final SnapshotDiffReportOzone snapshotDiffReport;
  private final JobStatus jobStatus;
  private final long waitTimeInMs;
  private final CancelStatus cancelStatus;

  public SnapshotDiffResponse(final SnapshotDiffReportOzone snapshotDiffReport,
                              final JobStatus jobStatus,
                              final long waitTimeInMs) {
    this.snapshotDiffReport = snapshotDiffReport;
    this.jobStatus = jobStatus;
    this.waitTimeInMs = waitTimeInMs;
    this.cancelStatus = CancelStatus.JOB_NOT_CANCELED;
  }

  public SnapshotDiffResponse(final SnapshotDiffReportOzone snapshotDiffReport,
                              final JobStatus jobStatus,
                              final long waitTimeInMs,
                              final CancelStatus cancelStatus) {
    this.snapshotDiffReport = snapshotDiffReport;
    this.jobStatus = jobStatus;
    this.waitTimeInMs = waitTimeInMs;
    this.cancelStatus = cancelStatus;
  }

  public SnapshotDiffReportOzone getSnapshotDiffReport() {
    return snapshotDiffReport;
  }

  public JobStatus getJobStatus() {
    return jobStatus;
  }

  public long getWaitTimeInMs() {
    return waitTimeInMs;
  }

  public CancelStatus getCancelStatus() {
    return cancelStatus;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    if (cancelStatus == CancelStatus.JOB_NOT_CANCELED ||
        cancelStatus == CancelStatus.CANCEL_SUCCESS) {
      if (jobStatus == JobStatus.DONE) {
        str.append(snapshotDiffReport.toString());
      } else {
        str.append("Snapshot diff job is ");
        str.append(jobStatus);
        str.append("\n");
        str.append("Please retry after ");
        str.append(waitTimeInMs);
        str.append(" ms.\n");
      }
    } else {
      str.append(cancelStatus.getDescription());
      str.append("\n");
    }
    return str.toString();
  }
}
