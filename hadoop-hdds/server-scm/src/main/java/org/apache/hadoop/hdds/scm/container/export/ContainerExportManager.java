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

import static org.apache.hadoop.hdds.scm.container.export.ExportLimits.DEFAULT_BATCH_SIZE;
import static org.apache.hadoop.hdds.scm.container.export.ExportLimits.DEFAULT_PART_SIZE;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.ratis.util.Preconditions;
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

  private static final long SHUTDOWN_TIMEOUT_MS = 5_000;

  private final Map<ExportJob.Id, ExportJob> jobMap = new ConcurrentHashMap<>();
  private final AtomicReference<ExportJob> runningJob = new AtomicReference<>();
  private final ExecutorService workerPool;
  private final ContainerManager containerManager;
  private final ExportFileManager fileManager;
  private final BooleanSupplier isLeaderReady;
  private final ExportSizing defaultSizing;

  public ContainerExportManager(String scmId, ContainerManager containerManager, BooleanSupplier isLeaderReady,
      OzoneConfiguration conf) {
    this(scmId, containerManager, isLeaderReady,
        ExportFileManager.resolveExportDirectory(conf), DEFAULT_PART_SIZE, DEFAULT_BATCH_SIZE);
  }

  ContainerExportManager(String scmId, ContainerManager containerManager, BooleanSupplier isLeaderReady,
      String exportDirectory, int partSize, int batchSize) {
    this.defaultSizing = new ExportSizing(0, 0, 0, batchSize, partSize);
    this.containerManager = Objects.requireNonNull(containerManager, "containerManager == null");
    this.isLeaderReady = Objects.requireNonNull(isLeaderReady, "isLeaderReady == null");
    this.fileManager = new ExportFileManager(exportDirectory);
    this.workerPool = newWorkerPool(scmId);
  }

  private ExecutorService newWorkerPool(String scmId) {
    return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> {
      Thread t = new Thread(r, scmId + "-ContainerExportWorker");
      t.setDaemon(true);
      return t;
    }) {
      @Override
      protected void terminated() {
        onWorkerTerminated();
      }
    };
  }

  /**
   * Initializes the export directory. Must be called once before submitting jobs.
   */
  public void start() throws IOException {
    fileManager.start();
    LOG.info("ContainerExportManager started (dir={}, partSize={}, batchSize={})",
        fileManager.getExportDirectory(), defaultSizing.getPartSize(), defaultSizing.getBatchSize());
  }

  /**
   * Submit a container ID export job on the SCM leader.
   *
   * @param maxRows upper bound on exported container IDs; {@code 0} means no limit
   * @param requestBatchSize container IDs per listing query; {@code 0} means the manager default
   * @param requestPartSize container IDs per part file; {@code 0} means the manager default
   * @return the submitted job
   * @throws IllegalArgumentException if a size is negative or above its {@link ExportLimits} maximum
   * @throws SCMException with {@link SCMException.ResultCodes#SCM_NOT_LEADER} if this SCM is not the ready leader
   * @throws IOException if another export is already running or the export worker is shut down
   */
  public ExportJob submitJob(ContainerID start, LifeCycleState lifeCycleState, ContainerHealthState healthState,
      long maxRows, int requestBatchSize, int requestPartSize) throws IOException {
    final ExportSizing sizing = new ExportSizing(maxRows, requestBatchSize, requestPartSize,
        defaultSizing.getBatchSize(), defaultSizing.getPartSize());

    if (!isLeaderReady.getAsBoolean()) {
      throw new SCMException("Container ID export can only be submitted on the SCM leader",
          SCMException.ResultCodes.SCM_NOT_LEADER);
    }

    final ExportScope scope = ExportScope.of(lifeCycleState, healthState);
    final ExportJob job = new ExportJob(scope, start, sizing);
    if (!runningJob.compareAndSet(null, job)) {
      throw new IOException("Another container ID export is already running");
    }

    jobMap.put(job.getId(), job);
    try {
      workerPool.submit(() -> executeExport(job));
    } catch (RejectedExecutionException e) {
      jobMap.remove(job.getId());
      runningJob.compareAndSet(job, null);
      throw new IOException("Container ID export worker is shut down", e);
    }
    LOG.info("Submitted container ID export job {} (scope={}, start={}, maxRows={}, batchSize={}, partSize={})",
        job, scope, start, job.getMaxRows(), job.getBatchSize(), job.getPartSize());
    return job;
  }

  public ExportJob.Status getExportStatus(ExportJob.Id jobId) {
    ExportJob job = jobMap.get(jobId);
    return job != null ? job.getStatus() : null;
  }

  public void shutdown() {
    LOG.info("Shutting down ContainerExportManager");
    workerPool.shutdownNow();
    try {
      if (!workerPool.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        LOG.warn("Export worker did not stop within {} ms; the container export directory lock is "
            + "released once it stops", SHUTDOWN_TIMEOUT_MS);
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted waiting for export worker shutdown; the container export directory lock is "
          + "released once it stops");
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Runs once the worker has stopped, so the export directory is no longer in use: the lock is kept
   * until then so that a new manager cannot start and delete files the worker is still writing.
   */
  private void onWorkerTerminated() {
    // A job accepted but drained from the executor queue by shutdownNow() never ran; fail it.
    final ExportJob notStarted = runningJob.getAndSet(null);
    if (notStarted != null) {
      notStarted.getFuture().completeExceptionally(new InterruptedException("Export cancelled: SCM shutting down"));
    }
    try {
      fileManager.unlock();
    } catch (IOException e) {
      LOG.warn("Failed to unlock container export directory", e);
    }
  }

  private void executeExport(ExportJob job) {
    Long idsWritten = null;
    Throwable failure = new IllegalStateException(job + " terminated unexpectedly");
    try {
      fileManager.createJobDirectory(job.getId());

      idsWritten = writeContainerIDs(job);
      if (idsWritten == null) {
        failure = new InterruptedException("Export cancelled");
        LOG.info("Export {} was cancelled", job);
      } else {
        if (idsWritten > 0) {
          fileManager.writeArchive(job, isLeaderReady);
          LOG.info("{} completed: {} ids, archive={}", job, idsWritten, job.getFileName());
        } else {
          LOG.info("{} completed with {} matching containers", job, idsWritten);
        }
        fileManager.deleteJobDirectory(job.getId());
        failure = null;
      }
    } catch (Exception e) {
      failure = e;
      LOG.error("Export {} failed", job, e);
    } finally {
      if (failure != null) {
        fileManager.cleanupFailedJob(job);
      }
      final boolean cleared = runningJob.compareAndSet(job, null);
      if (failure == null) {
        job.getFuture().complete(idsWritten);
      } else {
        job.getFuture().completeExceptionally(failure);
      }
      Preconditions.assertTrue(cleared);
    }
  }

  private Long writeContainerIDs(ExportJob job) throws IOException {
    final byte[] buffer = new byte[job.getBatchSize() * 12];
    int partNumber = 1;
    long idsWritten = 0;
    ContainerID cursor = job.getStartContainerId();
    List<ContainerID> batch = Collections.emptyList();
    int batchIndex = 0;

    while (cursor != null || batchIndex < batch.size()) {
      if (Thread.interrupted()) {
        return null;
      }
      if (!isLeaderReady.getAsBoolean()) {
        throw new IOException("SCM lost leadership during " + job);
      }

      if (batchIndex >= batch.size()) {
        batch = fetchContainerIds(job, cursor, idsWritten);
        batchIndex = 0;
        if (batch.isEmpty()) {
          break;
        }
        cursor = nextCursor(batch);
      }

      final ContainerID partStartContainerId = batch.get(batchIndex);
      try (OutputStream out = fileManager.newPartOutputStream(job.getId(), job.partFileName(partNumber))) {
        job.writeMetadataHeader(out, partNumber, partStartContainerId);
        LOG.info("{} created part{}", job, partNumber);

        long idsInCurrentPart = 0;
        int offset = 0;
        while (idsInCurrentPart < job.getPartSize()
            && (batchIndex < batch.size() || cursor != null)) {
          if (batchIndex >= batch.size()) {
            batch = fetchContainerIds(job, cursor, idsWritten);
            batchIndex = 0;
            if (batch.isEmpty()) {
              cursor = null;
              break;
            }
            cursor = nextCursor(batch);
          }

          final ContainerID containerId = batch.get(batchIndex++);
          final byte[] idBytes = (containerId.getProtobuf().getId() + "\n").getBytes(StandardCharsets.UTF_8);
          if (offset + idBytes.length > buffer.length) {
            out.write(buffer, 0, offset);
            offset = 0;
          }
          System.arraycopy(idBytes, 0, buffer, offset, idBytes.length);
          offset += idBytes.length;
          idsWritten++;
          idsInCurrentPart++;
        }

        if (offset > 0) {
          out.write(buffer, 0, offset);
        }
      }
      partNumber++;
    }
    return idsWritten;
  }

  private static ContainerID nextCursor(List<ContainerID> batch) {
    return ContainerID.valueOf(batch.get(batch.size() - 1).getProtobuf().getId() + 1);
  }

  private List<ContainerID> fetchContainerIds(ExportJob job, ContainerID cursor, long idsWritten) throws IOException {
    if (!isLeaderReady.getAsBoolean()) {
      throw new IOException("SCM lost leadership during " + job);
    }
    int count = job.getBatchSize();
    if (job.getMaxRows() > 0) {
      final long remaining = job.getMaxRows() - idsWritten;
      if (remaining <= 0) {
        return Collections.emptyList();
      }
      count = (int) Math.min(count, remaining);
    }
    return containerManager.getContainerIDs(cursor, count, job.getLifeCycleState(), job.getHealthState());
  }
}
