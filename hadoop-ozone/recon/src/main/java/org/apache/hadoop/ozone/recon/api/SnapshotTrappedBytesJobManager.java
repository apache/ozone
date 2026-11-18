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

package org.apache.hadoop.ozone.recon.api;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import org.apache.hadoop.ozone.om.helpers.BucketDeletedBytes;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.recon.api.types.SnapshotTrappedBytesJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs on-demand snapshot-trapped bytes calculations on OM in the background.
 * <p>
 * The calculation walks the deleted tables of the active OM DB and of every snapshot of the bucket, so it is
 * expensive for OM. Jobs therefore run one at a time, a running or queued job for the same bucket is reused
 * instead of starting another one, and the number of outstanding jobs is bounded.
 * Finished jobs are kept for {@link #JOB_RETENTION_MS} so the result can be shown again without recomputing it.
 */
@Singleton
public class SnapshotTrappedBytesJobManager {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotTrappedBytesJobManager.class);

  static final int MAX_ACTIVE_JOBS = 10;
  static final int MAX_RETAINED_JOBS = 100;
  static final long JOB_RETENTION_MS = TimeUnit.HOURS.toMillis(1);

  private final OzoneManagerProtocol omClient;
  private final ExecutorService executor;
  // Insertion ordered, so the oldest job is evicted first. Guarded by this.
  private final Map<String, SnapshotTrappedBytesJob> jobs = new LinkedHashMap<>();

  @Inject
  public SnapshotTrappedBytesJobManager(OzoneManagerProtocol omClient) {
    this(omClient, Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("ReconSnapshotTrappedBytes-%d").setDaemon(true).build()));
  }

  @VisibleForTesting
  SnapshotTrappedBytesJobManager(OzoneManagerProtocol omClient, ExecutorService executor) {
    this.omClient = omClient;
    this.executor = executor;
  }

  /**
   * Submits a calculation for the bucket, or returns the queued or running job for the same bucket.
   *
   * @throws IllegalStateException if too many jobs are already queued or running.
   */
  public synchronized SnapshotTrappedBytesJob submitJob(String volumeName, String bucketName) {
    evictFinishedJobs();
    int activeJobs = 0;
    for (SnapshotTrappedBytesJob job : jobs.values()) {
      if (job.isActive()) {
        if (job.getVolumeName().equals(volumeName) && job.getBucketName().equals(bucketName)) {
          return job;
        }
        activeJobs++;
      }
    }
    if (activeJobs >= MAX_ACTIVE_JOBS) {
      throw new IllegalStateException("Too many snapshot-trapped bytes calculations are in progress (max "
          + MAX_ACTIVE_JOBS + "). Please try again later.");
    }

    SnapshotTrappedBytesJob job = new SnapshotTrappedBytesJob(UUID.randomUUID().toString(), volumeName, bucketName);
    jobs.put(job.getJobId(), job);
    executor.submit(() -> runJob(job));
    LOG.info("Submitted snapshot-trapped bytes job {} for bucket {}/{}", job.getJobId(), volumeName, bucketName);
    return job;
  }

  public synchronized SnapshotTrappedBytesJob getJob(String jobId) {
    evictFinishedJobs();
    return jobs.get(jobId);
  }

  public synchronized List<SnapshotTrappedBytesJob> getJobs() {
    evictFinishedJobs();
    return new ArrayList<>(jobs.values());
  }

  private void runJob(SnapshotTrappedBytesJob job) {
    job.markRunning();
    String bucketPath = job.getVolumeName() + OM_KEY_PREFIX + job.getBucketName();
    try {
      if (omClient == null) {
        throw new IOException("OM client is not available in Recon");
      }
      BucketDeletedBytes deletedBytes = omClient.getBucketDeletedBytes(bucketPath);
      job.markCompleted(deletedBytes);
      LOG.info("Snapshot-trapped bytes job {} for bucket {} completed in {} ms", job.getJobId(), bucketPath,
          job.getCompletedAt() - job.getStartedAt());
    } catch (Exception e) {
      LOG.warn("Snapshot-trapped bytes job {} for bucket {} failed", job.getJobId(), bucketPath, e);
      job.markFailed(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
    }
  }

  /**
   * Drops finished jobs older than the retention period, then the oldest finished jobs over the retained limit.
   */
  private void evictFinishedJobs() {
    long cutoff = System.currentTimeMillis() - JOB_RETENTION_MS;
    int excess = jobs.size() - MAX_RETAINED_JOBS;
    for (Iterator<SnapshotTrappedBytesJob> it = jobs.values().iterator(); it.hasNext();) {
      SnapshotTrappedBytesJob job = it.next();
      if (!job.isActive() && (job.getCompletedAt() < cutoff || excess > 0)) {
        it.remove();
        excess--;
      }
    }
  }

  @PreDestroy
  public void shutdown() {
    executor.shutdownNow();
    if (omClient != null) {
      try {
        omClient.close();
      } catch (IOException e) {
        LOG.warn("Failed to close OM client", e);
      }
    }
  }
}
