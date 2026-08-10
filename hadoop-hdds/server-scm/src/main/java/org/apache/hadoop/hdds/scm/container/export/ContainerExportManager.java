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

import static org.apache.hadoop.hdds.scm.container.export.ExportLimits.DEFAULT_PAGE_SIZE;
import static org.apache.hadoop.hdds.scm.container.export.ExportLimits.DEFAULT_SHARD_SIZE;

import java.io.BufferedWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
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
 * locking, shard files, and completed archives; this class tracks {@link ExportJob} state,
 * schedules work, and applies {@code maxTerminalJobs} eviction.
 */
public class ContainerExportManager {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerExportManager.class);

  //TODO: make ozone.scm.container.export.max.terminal.jobs configurable.
  private static final int DEFAULT_MAX_TERMINAL_JOBS = 10;
  private static final long SHUTDOWN_TIMEOUT_MS = 5_000;

  private final Map<ExportJob.Id, ExportJob> jobMap = new ConcurrentHashMap<>();
  private final Deque<String> completedArchivePaths = new ArrayDeque<>();
  private final AtomicReference<ExportJob.Id> runningJobId = new AtomicReference<>();
  private final ExecutorService workerPool;
  private final ContainerManager containerManager;
  private final ExportFileManager fileManager;
  private final ExportMetrics metrics;
  private final BooleanSupplier isLeaderReady;
  private final int defaultShardSize;
  private final int defaultPageSize;
  private final int maxTerminalJobs;
  /** SCM node id, used in the export worker thread name. */
  private final String scmId;

  public ContainerExportManager(ContainerManager containerManager, BooleanSupplier isLeaderReady,
      OzoneConfiguration conf, String scmId) {
    this.containerManager = Objects.requireNonNull(containerManager, "containerManager == null");
    this.isLeaderReady = Objects.requireNonNull(isLeaderReady, "isLeaderReady == null");
    this.fileManager = new ExportFileManager(
        Objects.requireNonNull(ExportFileManager.resolveExportDirectory(conf), "exportDirectory == null"));
    this.scmId = Objects.requireNonNull(scmId, "scmId == null");
    this.defaultShardSize = DEFAULT_SHARD_SIZE;
    this.defaultPageSize = DEFAULT_PAGE_SIZE;
    this.maxTerminalJobs = DEFAULT_MAX_TERMINAL_JOBS;
    this.metrics = ExportMetrics.create();
    this.workerPool = newWorkerPool(this.scmId);
  }

  ContainerExportManager(ContainerManager containerManager, BooleanSupplier isLeaderReady,
      String exportDirectory, int defaultShardSize, int defaultPageSize, int maxTerminalJobs,
      String scmId) {
    this.containerManager = Objects.requireNonNull(containerManager, "containerManager == null");
    this.isLeaderReady = Objects.requireNonNull(isLeaderReady, "isLeaderReady == null");
    this.fileManager = new ExportFileManager(Objects.requireNonNull(exportDirectory, "exportDirectory == null"));
    this.scmId = Objects.requireNonNull(scmId, "scmId == null");
    this.defaultShardSize = defaultShardSize;
    this.defaultPageSize = defaultPageSize;
    this.maxTerminalJobs = maxTerminalJobs;
    this.metrics = null;
    this.workerPool = newWorkerPool(this.scmId);
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
    reloadCompletedArchives();
    LOG.info("ContainerExportManager started (dir={}, defaultShardSize={}, defaultPageSize={}, maxTerminalJobs={})",
        fileManager.getExportDirectory(), defaultShardSize, defaultPageSize, maxTerminalJobs);
  }

  /**
   * Submit a container ID export job on the SCM leader.
   * Batch sizing is described by {@link ExportSizing}.
   *
   * @return job id, or {@code null} if not leader or another export is already running
   */
  public ExportJob.Id submitJob(ContainerID start, LifeCycleState lifeCycleState,
      ContainerHealthState healthState, long maxRows, int pageSize, int shardSize) {
    if (!isLeaderReady.getAsBoolean()) {
      return null;
    }
    if (lifeCycleState == null && healthState == null) {
      throw new IllegalArgumentException("At least one of healthState or lifecycleState filter is required.");
    }
    validateRequest(start);
    ExportSizing.validate(maxRows, pageSize, shardSize);
    ExportSizing sizing = ExportSizing.resolve(maxRows, pageSize, shardSize, defaultPageSize, defaultShardSize);

    ExportJob.Id jobId = ExportJob.Id.newId();
    if (!runningJobId.compareAndSet(null, jobId)) {
      return null;
    }

    ExportScope scope = ExportScope.of(lifeCycleState, healthState);
    Instant now = Instant.now();
    String metadataTimestamp = ExportFileManager.formatMetadataTimestamp(now);
    String tarPath = fileManager.resolveArchivePath(scope, now, jobId);

    ExportJob job = new ExportJob(jobId, scope, metadataTimestamp, tarPath, start, sizing);

    evictOldTerminalJobs();
    jobMap.put(jobId, job);

    if (metrics != null) {
      metrics.incrExportJobsSubmitted();
    }

    workerPool.submit(() -> executeExport(job));
    LOG.info("Submitted container ID export job {} (scope={}, start={}, maxRows={}, pageSize={}, shardSize={})",
        jobId, scope, start, sizing.getMaxRows(), sizing.getPageSize(), sizing.getShardSize());
    return jobId;
  }

  private static void validateRequest(ContainerID start) {
    if (start != null && start.getProtobuf().getId() < 0) {
      throw new IllegalArgumentException(
          "start container ID must be non-negative, got: " + start.getProtobuf().getId());
    }
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
    if (metrics != null) {
      metrics.unRegister();
    }
  }

  Map<ExportJob.Id, ExportJob> getJobMap() {
    return jobMap;
  }

  private void evictOldTerminalJobs() {
    List<Map.Entry<ExportJob.Id, ExportJob>> terminalJobs = jobMap.entrySet().stream()
        .filter(e -> e.getValue().getExecutionState().isTerminal())
        .sorted(Comparator.comparingLong(e -> e.getValue().getEndTimeNs()))
        .collect(Collectors.toList());
    int excess = terminalJobs.size() - maxTerminalJobs;
    for (int i = 0; i < excess; i++) {
      ExportJob evicted = terminalJobs.get(i).getValue();
      removeCompletedArchive(evicted.getTarPath());
      jobMap.remove(evicted.getId());
    }
    trimCompletedArchives();
  }

  private void reloadCompletedArchives() {
    completedArchivePaths.clear();
    completedArchivePaths.addAll(fileManager.listCompletedArchivePaths());
    trimCompletedArchives();
  }

  private void trimCompletedArchives() {
    while (completedArchivePaths.size() > maxTerminalJobs) {
      removeCompletedArchive(completedArchivePaths.removeFirst());
    }
  }

  private void removeCompletedArchive(String tarPath) {
    if (tarPath == null) {
      return;
    }
    fileManager.deleteExportTar(tarPath);
    completedArchivePaths.remove(tarPath);
    jobMap.entrySet().removeIf(e -> tarPath.equals(e.getValue().getTarPath()));
  }

  private void executeExport(ExportJob job) {
    String jobIdValue = job.getId().getValue();
    String archivePath = job.getTarPath();
    job.startExecution();

    try {
      fileManager.createJobDirectory(job.getId());

      ContainerID cursor = job.getStartContainerId();
      int fileIndex = 1;
      long totalRows = 0;
      long recordsInCurrentFile = 0;
      BufferedWriter writer = null;
      // Pre-allocated buffer: ~12 chars per ID (up to 20 digits + newline) per page.
      StringBuilder buf = new StringBuilder(job.getPageSize() * 12);

      try {
        while (true) {
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Export job " + jobIdValue + " cancelled");
          }
          if (!isLeaderReady.getAsBoolean()) {
            throw new IOException("SCM lost leadership during export job " + jobIdValue);
          }

          int fetchCount = job.getPageSize();
          if (job.getMaxRows() > 0) {
            long remaining = job.getMaxRows() - totalRows;
            if (remaining <= 0) {
              break;
            }
            fetchCount = (int) Math.min(fetchCount, remaining);
          }

          List<ContainerID> page = containerManager.getContainerIDs(
              cursor, fetchCount, job.getLifeCycleState(), job.getHealthState());
          if (page.isEmpty()) {
            break;
          }

          for (ContainerID containerId : page) {
            if (recordsInCurrentFile == 0) {
              writer = closeWriter(writer);
              writer = fileManager.newShardWriter(job.getId(), job.shardFileName(fileIndex));
              job.writeMetadataHeader(writer, fileIndex, containerId);
              LOG.info("Export job {} created shard part{}", jobIdValue, fileIndex);
            }

            buf.append(containerId.getProtobuf().getId()).append('\n');
            totalRows++;
            recordsInCurrentFile++;
            job.updateTotalRows(totalRows);

            if (recordsInCurrentFile >= job.getShardSize()) {
              writer.write(buf.toString());
              buf.setLength(0);
              writer = closeWriter(writer);
              recordsInCurrentFile = 0;
              fileIndex++;
            }
          }

          // Flush the batch buffer at the end of each page.
          if (buf.length() > 0 && writer != null) {
            writer.write(buf.toString());
            buf.setLength(0);
          }

          cursor = ContainerID.valueOf(
              page.get(page.size() - 1).getProtobuf().getId() + 1);
        }

        writer = closeWriter(writer);
      } finally {
        closeWriter(writer);
      }

      if (totalRows == 0) {
        job.completeWithNoMatches();
        LOG.info("Export job {} completed with zero matching containers", jobIdValue);
      } else {
        fileManager.writeArchive(job.getId(), archivePath);
        job.completeWithArchive(archivePath);
        completedArchivePaths.addLast(archivePath);
        LOG.info("Export job {} completed ({} rows, archive={}).",
            jobIdValue, totalRows, archivePath);
      }
      fileManager.deleteJobDirectory(job.getId());
    } catch (InterruptedException e) {
      job.fail(e.getMessage());
      fileManager.cleanupFailedJob(job.getId(), archivePath);
      LOG.info("Export job {} was cancelled", jobIdValue);
      Thread.currentThread().interrupt();
    } catch (IOException | RuntimeException e) {
      job.fail(e.getMessage() != null ? e.getMessage() : e.toString());
      fileManager.cleanupFailedJob(job.getId(), archivePath);
      LOG.error("Export job {} failed", jobIdValue, e);
    } finally {
      runningJobId.compareAndSet(job.getId(), null);
      if (metrics != null) {
        ExportJob.ExecutionState state = job.getExecutionState();
        if (state == ExportJob.ExecutionState.SUCCEEDED) {
          metrics.incrExportJobsSucceeded();
          metrics.recordLastSuccessfulExport(job.getTotalRows(), fileManager.getArchiveLength(archivePath));
        } else if (state == ExportJob.ExecutionState.FAILED) {
          metrics.incrExportJobsFailed();
        }
      }
      if (job.getExecutionState().isTerminal()) {
        evictOldTerminalJobs();
      }
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
