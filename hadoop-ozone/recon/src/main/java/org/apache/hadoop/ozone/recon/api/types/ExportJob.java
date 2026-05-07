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
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents an asynchronous CSV export job.
 */
public class ExportJob {
  @JsonProperty("jobId")
  private String jobId;
  
  @JsonProperty("state")
  private String state;

  @JsonProperty("status")
  private JobStatus status;
  
  @JsonProperty("submittedAt")
  private long submittedAt;
  
  @JsonProperty("startedAt")
  private long startedAt;
  
  @JsonProperty("completedAt")
  private long completedAt;
  
  @JsonProperty("totalRecords")
  private long totalRecords;
  
  @JsonProperty("estimatedTotal")
  private long estimatedTotal;
  
  // Full path is kept internally for file I/O; only the filename is exposed via JSON
  private String filePath;

  @JsonProperty("fileName")
  private String fileName;
  
  @JsonProperty("errorMessage")
  private String errorMessage;
  
  @JsonProperty("progressPercent")
  private int progressPercent;
  
  @JsonProperty("queuePosition")
  private int queuePosition;

  // Internal — not serialized
  private int maxDownloads;

  @JsonIgnore
  private final AtomicInteger downloadCount = new AtomicInteger(0);

  public ExportJob(String jobId, String state, int maxDownloads) {
    this.jobId = jobId;
    this.state = state;
    this.status = JobStatus.QUEUED;
    this.submittedAt = System.currentTimeMillis();
    this.totalRecords = 0;
    this.estimatedTotal = -1;
    this.maxDownloads = maxDownloads;
  }

  public String getJobId() {
    return jobId;
  }

  public String getState() {
    return state;
  }

  public JobStatus getStatus() {
    return status;
  }

  public void setStatus(JobStatus status) {
    this.status = status;
    if (status == JobStatus.RUNNING && startedAt == 0) {
      startedAt = System.currentTimeMillis();
    } else if ((status == JobStatus.COMPLETED || status == JobStatus.FAILED) && completedAt == 0) {
      completedAt = System.currentTimeMillis();
    }
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

  public long getTotalRecords() {
    return totalRecords;
  }

  public void setTotalRecords(long totalRecords) {
    this.totalRecords = totalRecords;
  }

  public long getEstimatedTotal() {
    return estimatedTotal;
  }

  public void setEstimatedTotal(long estimatedTotal) {
    this.estimatedTotal = estimatedTotal;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
    if (filePath == null) {
      this.fileName = null;
      return;
    }
    java.nio.file.Path path = Paths.get(filePath).getFileName();
    this.fileName = path != null ? path.toString() : filePath;
  }

  public String getFileName() {
    return fileName;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public int getProgressPercent() {
    if (estimatedTotal > 0 && totalRecords > 0) {
      return (int) ((totalRecords * 100) / estimatedTotal);
    }
    return 0;
  }
  
  public int getQueuePosition() {
    return queuePosition;
  }
  
  public void setQueuePosition(int queuePosition) {
    this.queuePosition = queuePosition;
  }

  @JsonProperty("downloadCount")
  public int getDownloadCount() {
    return downloadCount.get();
  }

  public int getMaxDownloads() {
    return maxDownloads;
  }

  @JsonProperty("downloadsRemaining")
  public int getDownloadsRemaining() {
    return Math.max(0, maxDownloads - downloadCount.get());
  }

  /**
   * Best-effort hint for UI; may be briefly stale vs {@link #tryReserveDownload()}.
   */
  public boolean isDownloadAllowed() {
    return downloadCount.get() < maxDownloads;
  }

  /**
   * Atomically consumes one download slot if any remain. Use this from the
   * download endpoint so concurrent requests cannot bypass {@code maxDownloads}.
   *
   * @return true if a slot was reserved, false if the limit was already reached
   */
  public boolean tryReserveDownload() {
    while (true) {
      int current = downloadCount.get();
      if (current >= maxDownloads) {
        return false;
      }
      if (downloadCount.compareAndSet(current, current + 1)) {
        return true;
      }
    }
  }

  /**
   * Current execution state of the export job.
   */
  public enum JobStatus {
    QUEUED,      // Waiting for worker thread
    RUNNING,     // Actively exporting
    COMPLETED,   // File ready for download
    FAILED       // Error occurred
  }
}
