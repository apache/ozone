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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
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

  private static final int DEFAULT_BATCH_SIZE = 100_000;
  private static final int DEFAULT_PART_SIZE = 500_000;
  private static final long SHUTDOWN_TIMEOUT_MS = 5_000;

  private final Map<ExportJob.Id, ExportJob> jobMap = new ConcurrentHashMap<>();
  private final AtomicReference<ExportJob> runningJob = new AtomicReference<>();
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
   * @return the submitted job, or {@code null} if not leader or another export is already running
   */
  public ExportJob submitJob(ContainerID start, LifeCycleState lifeCycleState, ContainerHealthState healthState) {
    if (!isLeaderReady.getAsBoolean()) {
      return null;
    }

    final ExportScope scope = ExportScope.of(lifeCycleState, healthState);
    final ExportJob job = new ExportJob(scope, start, batchSize, partSize);
    if (!runningJob.compareAndSet(null, job)) {
      return null;
    }

    jobMap.put(job.getId(), job);
    workerPool.submit(() -> executeExport(job));
    LOG.info("Submitted container ID export job {} (scope={}, start={}, batchSize={}, partSize={})",
        job, scope, start, batchSize, partSize);
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
    try {
      fileManager.createJobDirectory(job.getId());

      final Long idsWritten = writeContainerIDs(job);
      if (idsWritten == null) {
        fileManager.cleanupFailedJob(job);
        job.getFuture().completeExceptionally(new InterruptedException("Export cancelled"));
        LOG.info("Export {} was cancelled", job);
      } else {
        if (idsWritten > 0) {
          fileManager.writeArchive(job);
          LOG.info("{} completed: {} ids, archive={}", job, idsWritten, job.getFileName());
        } else {
          LOG.info("{} completed with {} matching containers", job, idsWritten);
        }
        job.getFuture().complete(idsWritten);
      }

      fileManager.deleteJobDirectory(job.getId());
    } catch (Exception e) {
      fileManager.cleanupFailedJob(job);
      job.getFuture().completeExceptionally(e);
      LOG.error("Export {} failed", job, e);
    } finally {
      final boolean cleared = runningJob.compareAndSet(job, null);
      Preconditions.assertTrue(cleared);
    }
  }

  private Long writeContainerIDs(ExportJob job) throws IOException {
    // Pre-allocated buffer: ~12 chars per ID (up to 20 digits + newline) per batch.
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
        batch = fetchContainerIds(job, cursor);
        batchIndex = 0;
        if (batch.isEmpty()) {
          break;
        }
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
            cursor = ContainerID.valueOf(batch.get(batch.size() - 1).getProtobuf().getId() + 1);
            batch = fetchContainerIds(job, cursor);
            batchIndex = 0;
            if (batch.isEmpty()) {
              cursor = null;
              break;
            }
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

  private List<ContainerID> fetchContainerIds(ExportJob job, ContainerID cursor) throws IOException {
    if (!isLeaderReady.getAsBoolean()) {
      throw new IOException("SCM lost leadership during " + job);
    }
    return containerManager.getContainerIDs(
        cursor, job.getBatchSize(), job.getLifeCycleState(), job.getHealthState());
  }
}
