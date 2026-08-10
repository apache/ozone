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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.Archiver;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.ExportJob;
import org.apache.hadoop.ozone.recon.api.types.ExportJob.JobStatus;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.apache.ozone.recon.schema.generated.tables.records.UnhealthyContainersRecord;
import org.jooq.Cursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages asynchronous CSV export jobs.
 */
@Singleton
public class ExportJobManager {
  private static final Logger LOG = LoggerFactory.getLogger(ExportJobManager.class);

  private final Map<String, ExportJob> jobTracker = new ConcurrentHashMap<>();
  private final LinkedHashMap<String, ExportJob> jobQueue = new LinkedHashMap<>();
  private final Map<String, Future<?>> runningTasks = new ConcurrentHashMap<>();
  private final ExecutorService workerPool;
  private final ContainerHealthSchemaManager containerHealthSchemaManager;
  private final String exportDirectory;
  private final int maxDownloads;
  private final int maxQueueSize;

  @Inject
  public ExportJobManager(ContainerHealthSchemaManager containerHealthSchemaManager,
                          OzoneConfiguration conf) {
    this.containerHealthSchemaManager = containerHealthSchemaManager;
    
    // Use single thread executor for sequential processing (no concurrent DB access)
    this.workerPool = Executors.newSingleThreadExecutor();

    // Resolve export directory: use configured value if set, otherwise fall back to
    // {ozone.recon.db.dir}/exports so exports survive OS restarts alongside Recon data
    String configuredDir = conf.get(ReconServerConfigKeys.OZONE_RECON_EXPORT_DIRECTORY,
        ReconServerConfigKeys.OZONE_RECON_EXPORT_DIRECTORY_DEFAULT);
    if (configuredDir == null || configuredDir.isEmpty()) {
      File reconDbDir = new ReconUtils().getReconDbDir(
          conf, ReconServerConfigKeys.OZONE_RECON_DB_DIR);
      configuredDir = new File(reconDbDir, "exports").getAbsolutePath();
    }
    this.exportDirectory = configuredDir;
    this.maxDownloads = conf.getInt(
        ReconServerConfigKeys.OZONE_RECON_EXPORT_MAX_DOWNLOADS,
        ReconServerConfigKeys.OZONE_RECON_EXPORT_MAX_DOWNLOADS_DEFAULT);
    this.maxQueueSize = conf.getInt(
        ReconServerConfigKeys.OZONE_RECON_EXPORT_MAX_JOBS_TOTAL,
        ReconServerConfigKeys.OZONE_RECON_EXPORT_MAX_JOBS_TOTAL_DEFAULT);

    // Create export directory if it doesn't exist
    try {
      Files.createDirectories(Paths.get(exportDirectory));
    } catch (IOException e) {
      LOG.error("Failed to create export directory: {}", exportDirectory, e);
    }

    // Clean any leftover TARs / working dirs from a previous run so disk
    // is bounded by what was started in the current Recon process.
    File dir = new File(exportDirectory);
    File[] entries = dir.listFiles();
    int removed = 0;
    if (entries != null) {
      for (File entry : entries) {
        if (entry.isDirectory()) {
          FileUtils.deleteQuietly(entry);
        } else if (entry.getName().endsWith(".tar")) {
          FileUtils.deleteQuietly(entry);
        } else {
          continue;
        }
        removed++;
      }
    }
    if (removed > 0) {
      LOG.info("Startup cleanup: removed {} leftover export artifact(s) from {}",
          removed, exportDirectory);
    }

    LOG.info("ExportJobManager initialized with single-threaded queue (max {} jobs)", maxQueueSize);
  }

  public String submitJob(String state) {
    String jobId = UUID.randomUUID().toString();
    ExportJob job = new ExportJob(jobId, state, maxDownloads);
    String filePath = exportDirectory + "/export_" + state.toLowerCase()
        + "_" + System.currentTimeMillis() + ".tar";
    job.setFilePath(filePath);

    int queuePosition;
    // Single lock for all queue-related checks and mutations to avoid nested.
    synchronized (jobQueue) {
      // Reject if a job for this state is already queued, running, or completed
      boolean stateAlreadyExists = jobTracker.values().stream().anyMatch(
          j -> j.getState().equals(state)
              && (j.getStatus() == JobStatus.QUEUED
                  || j.getStatus() == JobStatus.RUNNING
                  || j.getStatus() == JobStatus.COMPLETED));
      if (stateAlreadyExists) {
        throw new IllegalStateException(
            "An export for state " + state + " already exists. Please delete the existing export "
                + "from the Completed Exports table before starting a new one.");
      }

      if (jobQueue.size() >= maxQueueSize) {
        throw new IllegalStateException(
            "Export queue is full (max " + maxQueueSize + " jobs). Please try again later.");
      }

      jobTracker.put(jobId, job);
      jobQueue.put(jobId, job);
      queuePosition = jobQueue.size();
    }

    // Submit outside the lock — workerPool.submit is thread-safe on its own
    Future<?> future = workerPool.submit(() -> executeExport(job));
    runningTasks.put(jobId, future);

    LOG.info("Submitted export job {} (state={}, queue position={})", jobId, state, queuePosition);

    return jobId;
  }

  public ExportJob getJob(String jobId) {
    return jobTracker.get(jobId);
  }

  /**
   * Returns all tracked export jobs (any status).
   */
  public List<ExportJob> getAllJobs() {
    return new ArrayList<>(jobTracker.values());
  }
  
  /**
   * Get the queue position for a job (1-indexed).
   * Returns 0 if job is not in queue (running, completed, or not found).
   */
  public int getQueuePosition(String jobId) {
    synchronized (jobQueue) {
      if (!jobQueue.containsKey(jobId)) {
        return 0;
      }
      
      int position = 1;
      for (String id : jobQueue.keySet()) {
        if (id.equals(jobId)) {
          return position;
        }
        position++;
      }
      return 0;
    }
  }

  /**
   * cancelJob is a unified cleanup method
   * Cancel a QUEUED or RUNNING job, or delete a COMPLETED/FAILED job and its TAR file.
   * Removes the job from the tracker in all cases.
   */
  public void cancelJob(String jobId) {
    ExportJob job = jobTracker.get(jobId);
    if (job == null) {
      throw new IllegalStateException("Job not found: " + jobId);
    }

    if (job.getStatus() == JobStatus.QUEUED || job.getStatus() == JobStatus.RUNNING) {
      // Remove from queue if still waiting
      synchronized (jobQueue) {
        jobQueue.remove(jobId);
      }
      Future<?> future = runningTasks.remove(jobId);
      if (future != null) {
        future.cancel(true);
      }
      job.setStatus(JobStatus.FAILED);
      job.setErrorMessage("Cancelled by user");
      // Clean up any partial temp directory
      FileUtils.deleteQuietly(new File(exportDirectory + "/" + jobId));
    }

    // Delete the TAR file outside the lock — file I/O does not need synchronization
    if (job.getFilePath() != null) {
      FileUtils.deleteQuietly(new File(job.getFilePath()));
    }

    // Remove from both maps atomically so submitJob's duplicate-state check
    // (which also runs inside synchronized(jobQueue)) never sees a half-removed job
    synchronized (jobQueue) {
      jobQueue.remove(jobId);   // no-op for COMPLETED/FAILED jobs already off the queue
      jobTracker.remove(jobId);
    }

    LOG.info("Deleted export job {} file={} (was {})", jobId, job.getFileName(), job.getStatus());
  }

  private void executeExport(ExportJob job) {
    String jobDirectory = exportDirectory + "/" + job.getJobId();
    Path jobDir = Paths.get(jobDirectory);
    String tarFilePath = job.getFilePath();  // Use the filename set in submitJob
    
    try {
      // Create job-specific directory for CSV files
      Files.createDirectories(jobDir);
      
      // Remove from queue and mark as running
      synchronized (jobQueue) {
        jobQueue.remove(job.getJobId());
      }
      job.setStatus(JobStatus.RUNNING);
      LOG.info("Starting export job {}", job.getJobId());
      
      ContainerSchemaDefinition.UnHealthyContainerStates internalState =
          ContainerSchemaDefinition.UnHealthyContainerStates.valueOf(job.getState());
      
      // Get total count first for progress tracking
      long estimatedTotal = containerHealthSchemaManager.getUnhealthyContainersCount(internalState, -1, 0);
      job.setEstimatedTotal(estimatedTotal);
      LOG.info("Export job {} will process approximately {} records", job.getJobId(), estimatedTotal);

      // Open database cursor (-1 = unlimited, 0 = no prevKey offset)
      try (Cursor<UnhealthyContainersRecord> cursor =
               containerHealthSchemaManager.getUnhealthyContainersCursor(internalState, -1, 0)) {
        int fileIndex = 1;
        long totalRecords = 0;
        long recordsInCurrentFile = 0;
        final int recordsPerFile = 500_000;
        
        BufferedWriter writer = null;
        OutputStream fos = null;
        try {
          while (cursor.hasNext()) {
            // Check for cancellation
            if (Thread.currentThread().isInterrupted()) {
              throw new InterruptedException("Job cancelled");
            }
            
            // Start new CSV file if needed
            if (recordsInCurrentFile == 0) {
              // Close previous file if exists
              if (writer != null) {
                writer.flush();
                writer.close();
              }
              
              String csvFileName = String.format("%s/unhealthy_containers_%s_part%03d.csv",
                  jobDirectory, job.getState().toLowerCase(), fileIndex);
              fos = Files.newOutputStream(Paths.get(csvFileName));
              try {
                writer = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8));
              } finally {
                if (writer == null) {
                  fos.close();
                }
              }
              
              // Write CSV header
              writer.write("container_id,container_state,in_state_since," +
                  "expected_replica_count,actual_replica_count,replica_delta\n");
              
              LOG.info("Created CSV file: part{}", fileIndex);
            }
            
            // Fetch and write record
            UnhealthyContainersRecord rec = cursor.fetchNext();
            StringBuilder sb = new StringBuilder(128);
            sb.append(rec.getContainerId()).append(',')
                .append(rec.getContainerState()).append(',')
                .append(rec.getInStateSince()).append(',')
                .append(rec.getExpectedReplicaCount()).append(',')
                .append(rec.getActualReplicaCount()).append(',')
                .append(rec.getReplicaDelta()).append('\n');
            writer.write(sb.toString());
            
            totalRecords++;
            recordsInCurrentFile++;
            job.setTotalRecords(totalRecords);
            
            // Move to next file if per-file record limit reached
            if (recordsInCurrentFile >= recordsPerFile) {
              writer.flush();
              writer.close();
              writer = null;
              recordsInCurrentFile = 0;
              fileIndex++;
            }
            
            // Flush every 10K rows
            if (recordsInCurrentFile > 0 && recordsInCurrentFile % 10000 == 0) {
              writer.flush();
            }
          }
          
          // Close last file
          if (writer != null) {
            writer.flush();
            writer.close();
          }
          
        } finally {
          if (writer != null) {
            try {
              writer.close();
            } catch (IOException e) {
              LOG.warn("Error closing writer", e);
            }
          }
        }
        
        LOG.info("Export job {} wrote {} records across {} files",
            job.getJobId(), totalRecords, fileIndex);
        
        // Create TAR archive
        File tarFile = new File(tarFilePath);
        Archiver.create(tarFile, jobDir);
        LOG.info("Created TAR archive: {}", tarFilePath);
        
        // Delete CSV files and job directory
        FileUtils.deleteDirectory(jobDir.toFile());
        LOG.info("Deleted temporary CSV files for job {}", job.getJobId());
        
        // Update job with TAR file path
        job.setFilePath(tarFilePath);
        job.setStatus(JobStatus.COMPLETED);
        LOG.info("Completed export job {} ({} records)", job.getJobId(), totalRecords);
        
      } catch (InterruptedException e) {
        job.setStatus(JobStatus.FAILED);
        job.setErrorMessage("Job was cancelled");
        FileUtils.deleteQuietly(jobDir.toFile());
        FileUtils.deleteQuietly(new File(tarFilePath));
        LOG.info("Export job {} was cancelled", job.getJobId());
        Thread.currentThread().interrupt();
      }
      
    } catch (IOException | RuntimeException e) {
      job.setStatus(JobStatus.FAILED);
      job.setErrorMessage(e.getMessage());
      FileUtils.deleteQuietly(new File(exportDirectory + "/" + job.getJobId()));
      FileUtils.deleteQuietly(new File(tarFilePath));
      LOG.error("Export job {} failed", job.getJobId(), e);
    } finally {
      runningTasks.remove(job.getJobId());
    }
  }
  
  @PreDestroy
  public void shutdown() {
    LOG.info("Shutting down ExportJobManager");
    workerPool.shutdownNow();
    try {
      workerPool.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Timeout waiting for executor shutdown", e);
      Thread.currentThread().interrupt();
    }
  }
}
