package org.apache.hadoop.ozone.om.snapshot;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;

public class SnapshotDiffJob {
  public String jobId;
  public SnapshotDiffResponse.JobStatus status;

  // Default constructor for Jackson Serializer.
  public SnapshotDiffJob() {

  }

  public SnapshotDiffJob(String jobId, SnapshotDiffResponse.JobStatus jobStatus) {
    this.jobId = jobId;
    this.status = jobStatus;
  }

  public static class SnapDiffJobCodec implements Codec<SnapshotDiffJob> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    @Override
    public byte[] toPersistedFormat(SnapshotDiffJob object) throws IOException {
      return MAPPER.writeValueAsBytes(object);
    }

    @Override
    public SnapshotDiffJob fromPersistedFormat(byte[] rawData) throws IOException {
      return MAPPER.readValue(rawData, SnapshotDiffJob.class);
    }

    @Override
    public SnapshotDiffJob copyObject(SnapshotDiffJob object) {
      // Note: Not really a "copy". from OmDBDiffReportEntryCodec
      return object;
    }
  }
}
