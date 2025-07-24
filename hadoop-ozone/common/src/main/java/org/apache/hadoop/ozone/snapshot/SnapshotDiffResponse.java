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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotDiffResponse.JobStatusProto;

/**
 * POJO for Snapshot Diff Response.
 */
public class SnapshotDiffResponse {

  private final SnapshotDiffReportOzone snapshotDiffReport;
  private final JobStatus jobStatus;
  private final long waitTimeInMs;
  private final String reason;
  private SubStatus subStatus;
  private double progressPercent = 0.0;

  public SnapshotDiffResponse(final SnapshotDiffReportOzone snapshotDiffReport,
                              final JobStatus jobStatus,
                              final long waitTimeInMs) {
    this.snapshotDiffReport = snapshotDiffReport;
    this.jobStatus = jobStatus;
    this.waitTimeInMs = waitTimeInMs;
    this.reason = StringUtils.EMPTY;
  }

  public SnapshotDiffResponse(final SnapshotDiffReportOzone snapshotDiffReport,
                              final JobStatus jobStatus,
                              final long waitTimeInMs,
                              final String reason) {
    this.snapshotDiffReport = snapshotDiffReport;
    this.jobStatus = jobStatus;
    this.waitTimeInMs = waitTimeInMs;
    this.reason = reason;
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

  public String getReason() {
    return reason;
  }

  public void setSubStatus(SubStatus subStatus) {
    this.subStatus = subStatus;
  }

  public void setProgressPercent(double progressPercent) {
    this.progressPercent = progressPercent;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    switch (jobStatus) {
    case DONE:
      str.append(snapshotDiffReport.toString());
      break;
    case FAILED:
      str.append("Snapshot diff job is FAILED due to '");
      if (StringUtils.isNotEmpty(reason)) {
        str.append(reason);
      } else {
        str.append("Unknown reason.");
      }
      str.append("'. Please retry after ")
          .append(waitTimeInMs)
          .append(" ms.\n");
      break;
    case CANCELLED:
      str.append("Snapshot diff job has been CANCELLED.");
      break;
    default:
      str.append("Snapshot diff job is ")
          .append(jobStatus)
          .append(". Please retry after ")
          .append(waitTimeInMs)
          .append(" ms.\n");
      if (subStatus != null) {
        str.append("SubStatus : ")
            .append(subStatus);
        if (subStatus.equals(SubStatus.OBJECT_ID_MAP_GEN_OBS) ||
            subStatus.equals(SubStatus.OBJECT_ID_MAP_GEN_FSO)) {
          str.append("Keys Processed Estimated Percentage : ")
              .append(progressPercent);
        }
      }
    }
    return str.toString();
  }

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
   * Snapshot diff job sub-status enum.
   */
  public enum SubStatus {
    SST_FILE_DELTA_DAG_WALK,
    SST_FILE_DELTA_FULL_DIFF,
    OBJECT_ID_MAP_GEN_OBS,
    OBJECT_ID_MAP_GEN_FSO,
    DIFF_REPORT_GEN;

    public static SubStatus fromProtoBuf(OzoneManagerProtocolProtos.SnapshotDiffResponse.SubStatus subStatusProto) {
      return SubStatus.valueOf(subStatusProto.name());
    }

    public OzoneManagerProtocolProtos.SnapshotDiffResponse.SubStatus toProtoBuf() {
      return OzoneManagerProtocolProtos.SnapshotDiffResponse.SubStatus.valueOf(this.name());
    }
  }
}
