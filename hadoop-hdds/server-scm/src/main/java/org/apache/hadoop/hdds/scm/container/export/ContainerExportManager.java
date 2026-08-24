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

package org.apache.hadoop.hdds.scm.container.export;

import java.io.BufferedWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages asynchronous container ID export jobs on the SCM leader.
 *
 * <p>Health filters read {@link org.apache.hadoop.hdds.scm.container.ContainerInfo#getHealthState()}
 * as last written by Replication Manager; they are not recomputed during export and may be stale
 * if RM has not yet evaluated a container.
 *
 * <p>Job status is kept in memory only. On SCM restart or leader failover, in-flight jobs are lost
 * and the operator must re-submit on the new leader. {@link ExportFileManager} owns on-disk layout,
 * locking, part files, and completed archives; this class tracks {@link ExportJob} state and
 * schedules work.
 */
public class ContainerExportManager {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerExportManager.class);

  private static final int DEFAULT_BATCH_SIZE = 100_000;
  private static final int DEFAULT_PART_SIZE = 500_000;
  private static final long SHUTDOWN_TIMEOUT_MS = 5_000;

  private final Map<ExportJob.Id, ExportJob> jobMap = new ConcurrentHashMap<>();
  private final AtomicReference<ExportJob.Id> runningJobId = new AtomicReference<>();
  private final ExecutorService workerPool;
  private final ContainerManager containerManager;
  private final ExportFileManager fileManager;
  private final BooleanSupplier isLeaderReady;
  private final int partSize;
  private final int batchSize;

  public ContainerExportManager(String scmId, ContainerManager containerManager, BooleanSupplier isLeaderReady,
      OzoneConfiguration conf) {
    this(scmId, containerManager, isLeaderReady,
        ExportFileManager.resolveExportDirectory(conf), DEFAULT_PART_SIZE, DEFAULT_BATCH_SIZE);
  }

  ContainerExportManager(String scmId, ContainerManager containerManager, BooleanSupplier isLeaderReady,
      String exportDirectory, int partSize, int batchSize) {
    this.containerManager = Objects.requireNonNull(containerManager, "containerManager == null");
    this.isLeaderReady = Objects.requireNonNull(isLeaderReady, "isLeaderReady == null");
    this.fileManager = new ExportFileManager(exportDirectory);
    this.partSize = partSize;
    this.batchSize = batchSize;
    this.workerPool = newWorkerPool(scmId);
  }

  private static ExecutorService newWorkerPool(String scmId) {
    return Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, scmId + "-ContainerExportWorker");
      t.setDaemon(true);
      return t;
    });
  }

  /**
   * Initializes the export directory. Must be called once before submitting jobs.
   */
  public void start() throws IOException {
    fileManager.start();
    LOG.info("ContainerExportManager started (dir={}, partSize={}, batchSize={})",
        fileManager.getExportDirectory(), partSize, batchSize);
  }

  /**
   * Submit a container ID export job on the SCM leader.
   *
   * @return job id, or {@code null} if not leader or another export is already running
   */
  public ExportJob.Id submitJob(ContainerID start, LifeCycleState lifeCycleState,
      ContainerHealthState healthState) {
    final ExportScope scope = ExportScope.of(lifeCycleState, healthState);

    if (!isLeaderReady.getAsBoolean()) {
      return null;
    }

    ExportJob.Id jobId = ExportJob.Id.newId();
    if (!runningJobId.compareAndSet(null, jobId)) {
      return null;
    }

    Instant now = Instant.now();
    String jobStartTime = ExportFileManager.formatJobStartTime(now);
    String plannedArchivePath = fileManager.resolveArchiveFile(scope, jobStartTime, jobId).getAbsolutePath();

    ExportJob job = new ExportJob(jobId, scope, jobStartTime, plannedArchivePath, start, batchSize, partSize);
    jobMap.put(jobId, job);

    workerPool.submit(() -> executeExport(job));
    LOG.info("Submitted container ID export job {} (scope={}, start={}, batchSize={}, partSize={})",
        jobId, scope, start, batchSize, partSize);
    return jobId;
  }

  public ExportJob.Status getExportStatus(ExportJob.Id jobId) {
    ExportJob job = jobMap.get(jobId);
    return job != null ? job.toStatus() : null;
  }

  public void shutdown() {
    LOG.info("Shutting down ContainerExportManager");
    workerPool.shutdownNow();
    try {
      if (!workerPool.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        LOG.warn("Timed out waiting for export worker shutdown");
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted waiting for export worker shutdown");
      Thread.currentThread().interrupt();
    }
    try {
      fileManager.unlock();
    } catch (IOException e) {
      LOG.warn("Failed to unlock container export directory", e);
    }
  }

  private void executeExport(ExportJob job) {
    String jobIdValue = job.getId().getValue();
    String plannedArchivePath = job.getPlannedArchivePath();

    try {
      fileManager.createJobDirectory(job.getId());

      ContainerID cursor = job.getStartContainerId();
      int partIndex = 1;
      long totalRows = 0;
      long recordsInCurrentPart = 0;
      BufferedWriter writer = null;
      // Pre-allocated buffer: ~12 chars per ID (up to 20 digits + newline) per batch.
      StringBuilder buf = new StringBuilder(job.getBatchSize() * 12);

      try {
        while (true) {
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Export job " + jobIdValue + " cancelled");
          }
          if (!isLeaderReady.getAsBoolean()) {
            throw new IOException("SCM lost leadership during export job " + jobIdValue);
          }

          List<ContainerID> batch = containerManager.getContainerIDs(
              cursor, job.getBatchSize(), job.getLifeCycleState(), job.getHealthState());
          if (batch.isEmpty()) {
            break;
          }

          for (ContainerID containerId : batch) {
            if (recordsInCurrentPart == 0) {
              writer = closeWriter(writer);
              writer = fileManager.newPartWriter(job.getId(), job.partFileName(partIndex));
              job.writeMetadataHeader(writer, partIndex, containerId.getProtobuf().getId());
              LOG.info("Export job {} created part{}", jobIdValue, partIndex);
            }

            buf.append(containerId.getProtobuf().getId()).append('\n');
            totalRows++;
            recordsInCurrentPart++;
            job.updateTotalRows(totalRows);

            if (recordsInCurrentPart >= job.getPartSize()) {
              writer.write(buf.toString());
              buf.setLength(0);
              writer = closeWriter(writer);
              recordsInCurrentPart = 0;
              partIndex++;
            }
          }

          if (buf.length() > 0 && writer != null) {
            writer.write(buf.toString());
            buf.setLength(0);
          }

          cursor = ContainerID.valueOf(
              batch.get(batch.size() - 1).getProtobuf().getId() + 1);
        }

        writer = closeWriter(writer);
      } finally {
        closeWriter(writer);
      }

      if (totalRows == 0) {
        job.completeWithNoMatches();
        LOG.info("Export job {} completed with zero matching containers", jobIdValue);
      } else {
        fileManager.writeArchive(job.getId(), plannedArchivePath);
        job.completeSucceeded();
        LOG.info("Export job {} completed ({} rows, archive={}).",
            jobIdValue, totalRows, plannedArchivePath);
      }
      fileManager.deleteJobDirectory(job.getId());
    } catch (InterruptedException e) {
      fileManager.cleanupFailedJob(job.getId(), plannedArchivePath);
      job.fail(e.getMessage());
      LOG.info("Export job {} was cancelled", jobIdValue);
      Thread.currentThread().interrupt();
    } catch (IOException | RuntimeException e) {
      fileManager.cleanupFailedJob(job.getId(), plannedArchivePath);
      job.fail(e.getMessage() != null ? e.getMessage() : e.toString());
      LOG.error("Export job {} failed", jobIdValue, e);
    } finally {
      runningJobId.compareAndSet(job.getId(), null);
    }
  }

  private static BufferedWriter closeWriter(BufferedWriter writer) throws IOException {
    if (writer != null) {
      writer.flush();
      writer.close();
    }
    return null;
  }
}
