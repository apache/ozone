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
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;

/**
 * POJO for Snapshot diff job.
 */
public class SnapshotDiffJob {
  private String jobId;
  private JobStatus status;

  // Default constructor for Jackson Serializer.
  public SnapshotDiffJob() {

  }

  public SnapshotDiffJob(String jobId, JobStatus jobStatus) {
    this.jobId = jobId;
    this.status = jobStatus;
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

  /**
   * Codec to encode SnapshotDiffJob as byte array.
   */
  public static class SnapshotDiffJobCodec implements Codec<SnapshotDiffJob> {

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
