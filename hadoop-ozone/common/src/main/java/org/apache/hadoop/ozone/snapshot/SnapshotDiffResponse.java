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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotDiffResponse.JobCancelResultProto;

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
    CANCELLED;

    public JobStatusProto toProtobuf() {
      return JobStatusProto.valueOf(this.name());
    }

    public static JobStatus fromProtobuf(JobStatusProto jobStatusProto) {
      return JobStatus.valueOf(jobStatusProto.name());
    }
  }

  /**
   * Snapshot diff cancel result enum.
   */
  public enum JobCancelResult {
    JOB_NOT_CANCELLED("Job hasn't been cancelled"),
    NEW_JOB("Cannot cancel a newly submitted job"),
    JOB_DONE("Job is already DONE"),
    INVALID_STATUS_TRANSITION("Job is not IN_PROGRESS, cancel failed"),
    JOB_ALREADY_CANCELLED("Job has already been cancelled"),
    CANCELLATION_SUCCESS("Job has successfully been cancelled");

    private final String description;

    JobCancelResult(String description) {
      this.description = description;
    }

    public String getDescription() {
      return description;
    }

    public JobCancelResultProto toProtobuf() {
      return JobCancelResultProto.valueOf(this.name());
    }

    public static JobCancelResult fromProtobuf(
        JobCancelResultProto jobCancelResultProto) {
      return JobCancelResult.valueOf(jobCancelResultProto.name());
    }
  }

  private final SnapshotDiffReportOzone snapshotDiffReport;
  private final JobStatus jobStatus;
  private final long waitTimeInMs;
  private final JobCancelResult jobCancelResult;

  public SnapshotDiffResponse(final SnapshotDiffReportOzone snapshotDiffReport,
                              final JobStatus jobStatus,
                              final long waitTimeInMs) {
    this.snapshotDiffReport = snapshotDiffReport;
    this.jobStatus = jobStatus;
    this.waitTimeInMs = waitTimeInMs;
    this.jobCancelResult = JobCancelResult.JOB_NOT_CANCELLED;
  }

  public SnapshotDiffResponse(final SnapshotDiffReportOzone snapshotDiffReport,
                              final JobStatus jobStatus,
                              final long waitTimeInMs,
                              final JobCancelResult jobCancelResult) {
    this.snapshotDiffReport = snapshotDiffReport;
    this.jobStatus = jobStatus;
    this.waitTimeInMs = waitTimeInMs;
    this.jobCancelResult = jobCancelResult;
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

  public JobCancelResult getJobCancelResult() {
    return jobCancelResult;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    if (jobCancelResult == JobCancelResult.JOB_NOT_CANCELLED ||
        jobCancelResult == JobCancelResult.CANCELLATION_SUCCESS) {
      if (jobStatus == JobStatus.DONE) {
        str.append(snapshotDiffReport.toString());
      } else {
        str.append("Snapshot diff job is ");
        str.append(jobStatus);
        str.append(". Please retry after ");
        str.append(waitTimeInMs);
        str.append(" ms.\n");
      }
    } else {
      str.append(jobCancelResult.getDescription());
      str.append("\n");
    }
    return str.toString();
  }
}
