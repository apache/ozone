package org.apache.hadoop.ozone.snapshot;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotDiffResponse.JobStatusProto;

public class SnapshotDiffResponse {

  public enum JobStatus {
    QUEUED,
    IN_PROGRESS,
    DONE,
    REJECTED,
    FAILED;

    public JobStatusProto toProtobuf() {
      return JobStatusProto.valueOf(this.name());
    }

    public static JobStatus fromProtobuf(JobStatusProto jobStatusProto) {
      return JobStatus.valueOf(jobStatusProto.name());
    }
  }

  private final SnapshotDiffReport snapshotDiffReport;
  private final JobStatus jobStatus;
  private final long waitTimeInMs;

  public SnapshotDiffResponse(final SnapshotDiffReport snapshotDiffReport,
                              final JobStatus jobStatus,
                              final long waitTimeInMs) {
    this.snapshotDiffReport = snapshotDiffReport;
    this.jobStatus = jobStatus;
    this.waitTimeInMs = waitTimeInMs;
  }

  public SnapshotDiffReport getSnapshotDiffReport() {
    return snapshotDiffReport;
  }

  public JobStatus getJobStatus() {
    return jobStatus;
  }

  public long getWaitTimeInMs() {
    return waitTimeInMs;
  }
}
