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

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task to query data from OMDB and write into Recon RocksDB.
 * Reprocess() will take a snapshots on OMDB, and iterate the keyTable,
 * the fileTable and the dirTable to write all information to RocksDB.
 *
 * For FSO-enabled keyTable (fileTable), we need to fetch the parent object
 * (bucket or directory), increment its numOfKeys by 1, increase its sizeOfKeys
 * by the file data size, and update the file size distribution bin accordingly.
 *
 * For dirTable, we need to fetch the parent object (bucket or directory),
 * add the current directory's objectID to the parent object's childDir field.
 *
 * For keyTable, the parent object is not available. Get the parent object,
 * add it to the current object and reuse the existing methods for FSO.
 * Only processing entries that belong to Legacy buckets. If the entry
 * refers to a directory then build directory info object from it.
 *
 * Process() will write all OMDB updates to RocksDB.
 * Write logic is the same as above. For update action, we will treat it as
 * delete old value first, and write updated value then.
 */
public class NSSummaryTask implements ReconOmTask {
  private static final Logger LOG =
      LoggerFactory.getLogger(NSSummaryTask.class);

  // Unified control for all NSS tree rebuild operations
  private static final AtomicReference<RebuildState> REBUILD_STATE =
      new AtomicReference<>(RebuildState.IDLE);

  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private final ReconOMMetadataManager reconOMMetadataManager;
  private final OzoneConfiguration ozoneConfiguration;
  private final NSSummaryTaskWithFSO nsSummaryTaskWithFSO;
  private final NSSummaryTaskWithLegacy nsSummaryTaskWithLegacy;
  private final NSSummaryTaskWithOBS nsSummaryTaskWithOBS;

  /**
   * Rebuild state enum to track NSSummary tree rebuild status.
   */
  public enum RebuildState {
    IDLE,     // No rebuild in progress
    RUNNING,  // Rebuild currently in progress
    FAILED    // Last rebuild failed
  }

  @Inject
  public NSSummaryTask(ReconNamespaceSummaryManager
                       reconNamespaceSummaryManager,
                       ReconOMMetadataManager
                       reconOMMetadataManager,
                       OzoneConfiguration
                       ozoneConfiguration) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.reconOMMetadataManager = reconOMMetadataManager;
    this.ozoneConfiguration = ozoneConfiguration;
    
    long nsSummaryFlushToDBMaxThreshold = ozoneConfiguration.getLong(
        OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD,
        OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT);
    
    int maxKeysInMemory = ozoneConfiguration.getInt(
        ReconServerConfigKeys.OZONE_RECON_TASK_REPROCESS_MAX_KEYS_IN_MEMORY,
        ReconServerConfigKeys.OZONE_RECON_TASK_REPROCESS_MAX_KEYS_IN_MEMORY_DEFAULT);
    int maxIterators = ozoneConfiguration.getInt(
        ReconServerConfigKeys.OZONE_RECON_TASK_REPROCESS_MAX_ITERATORS,
        ReconServerConfigKeys.OZONE_RECON_TASK_REPROCESS_MAX_ITERATORS_DEFAULT);
    int maxWorkers = ozoneConfiguration.getInt(
        ReconServerConfigKeys.OZONE_RECON_TASK_REPROCESS_MAX_WORKERS,
        ReconServerConfigKeys.OZONE_RECON_TASK_REPROCESS_MAX_WORKERS_DEFAULT);

    this.nsSummaryTaskWithFSO =
        new NSSummaryTaskWithFSO(reconNamespaceSummaryManager, reconOMMetadataManager, nsSummaryFlushToDBMaxThreshold,
            maxIterators, maxWorkers, maxKeysInMemory);
    this.nsSummaryTaskWithLegacy =
        new NSSummaryTaskWithLegacy(reconNamespaceSummaryManager, reconOMMetadataManager, ozoneConfiguration,
            nsSummaryFlushToDBMaxThreshold);
    this.nsSummaryTaskWithOBS =
        new NSSummaryTaskWithOBS(reconNamespaceSummaryManager, reconOMMetadataManager, nsSummaryFlushToDBMaxThreshold,
            maxIterators, maxWorkers, maxKeysInMemory);
  }

  @Override
  public NSSummaryTask getStagedTask(ReconOMMetadataManager stagedOmMetadataManager, DBStore stagedReconDbStore)
      throws IOException {
    ReconNamespaceSummaryManager stagedNsSummaryManager =
        reconNamespaceSummaryManager.getStagedNsSummaryManager(stagedReconDbStore);
    return new NSSummaryTask(stagedNsSummaryManager, stagedOmMetadataManager, ozoneConfiguration);
  }

  @Override
  public String getTaskName() {
    return "NSSummaryTask";
  }

  /**
   * Get the current rebuild state of NSSummary tree.
   * 
   * @return current RebuildState
   */
  public static RebuildState getRebuildState() {
    return REBUILD_STATE.get();
  }

  /**
   * Bucket Type Enum which mimic subtasks for their data processing.
   */
  public enum BucketType {
    FSO("File System Optimized Bucket"),
    OBS("Object Store Bucket"),
    LEGACY("Legacy Bucket");

    private final String description;

    BucketType(String description) {
      this.description = description;
    }

    public String getDescription() {
      return description;
    }
  }

  @Override
  public TaskResult process(
      OMUpdateEventBatch events, Map<String, Integer> subTaskSeekPosMap) {
    boolean anyFailure = false; // Track if any bucket fails
    Map<String, Integer> updatedSeekPositions = new HashMap<>();

    // Process FSO bucket
    Integer bucketSeek = subTaskSeekPosMap.getOrDefault(BucketType.FSO.name(), 0);
    Pair<Integer, Boolean> bucketResult = nsSummaryTaskWithFSO.processWithFSO(events, bucketSeek);
    updatedSeekPositions.put(BucketType.FSO.name(), bucketResult.getLeft());
    if (!bucketResult.getRight()) {
      LOG.error("processWithFSO failed.");
      anyFailure = true;
    }

    // Process Legacy bucket
    bucketSeek = subTaskSeekPosMap.getOrDefault(BucketType.LEGACY.name(), 0);
    bucketResult = nsSummaryTaskWithLegacy.processWithLegacy(events, bucketSeek);
    updatedSeekPositions.put(BucketType.LEGACY.name(), bucketResult.getLeft());
    if (!bucketResult.getRight()) {
      LOG.error("processWithLegacy failed.");
      anyFailure = true;
    }

    // Process OBS bucket
    bucketSeek = subTaskSeekPosMap.getOrDefault(BucketType.OBS.name(), 0);
    bucketResult = nsSummaryTaskWithOBS.processWithOBS(events, bucketSeek);
    updatedSeekPositions.put(BucketType.OBS.name(), bucketResult.getLeft());
    if (!bucketResult.getRight()) {
      LOG.error("processWithOBS failed.");
      anyFailure = true;
    }

    // Return task failure if any bucket failed, while keeping each bucket's latest seek position
    return new TaskResult.Builder()
        .setTaskName(getTaskName())
        .setSubTaskSeekPositions(updatedSeekPositions)
        .setTaskSuccess(!anyFailure)
        .build();
  }

  @Override
  public TaskResult reprocess(OMMetadataManager omMetadataManager) {
    // Unified control for all NSS tree rebuild operations
    RebuildState currentState = REBUILD_STATE.get();
    if (currentState == RebuildState.RUNNING) {
      LOG.info("NSSummary tree rebuild is already in progress, skipping duplicate request.");
      return buildTaskResult(false);
    }

    if (!REBUILD_STATE.compareAndSet(currentState, RebuildState.RUNNING)) {
      // Check if another thread successfully started the rebuild
      if (REBUILD_STATE.get() == RebuildState.RUNNING) {
        LOG.info("Rebuild already in progress by another thread, returning success");
        return buildTaskResult(true);
      }
      LOG.info("Failed to acquire rebuild lock, unknown state");
      return buildTaskResult(false);
    }

    LOG.info("Starting NSSummary tree reprocess with unified control...");
    long startTime = System.nanoTime(); // Record start time
    
    try {
      return executeReprocess(omMetadataManager, startTime);
    } catch (Exception e) {
      LOG.error("NSSummary reprocess failed with exception.", e);
      REBUILD_STATE.set(RebuildState.FAILED);
      return buildTaskResult(false);
    }
  }

  /**
   * Execute the actual reprocess operation with proper state management.
   */
  protected TaskResult executeReprocess(OMMetadataManager omMetadataManager, long startTime) {
    // Initialize a list of tasks to run in parallel
    Collection<Callable<Boolean>> tasks = new ArrayList<>();

    try {
      // reinit Recon RocksDB's namespace CF.
      reconNamespaceSummaryManager.clearNSSummaryTable();
    } catch (IOException ioEx) {
      LOG.error("Unable to clear NSSummary table in Recon DB. ", ioEx);
      REBUILD_STATE.set(RebuildState.FAILED);
      return buildTaskResult(false);
    }

    tasks.add(() -> nsSummaryTaskWithFSO
        .reprocessWithFSO(omMetadataManager));
    tasks.add(() -> nsSummaryTaskWithLegacy
        .reprocessWithLegacy(reconOMMetadataManager));
    tasks.add(() -> nsSummaryTaskWithOBS
        .reprocessWithOBS(reconOMMetadataManager));

    List<Future<Boolean>> results;
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("Recon-NSSummaryTask-%d")
        .build();
    ExecutorService executorService = Executors.newFixedThreadPool(3,
        threadFactory);
    boolean success = false;
    try {
      results = executorService.invokeAll(tasks);
      for (Future<Boolean> result : results) {
        if (result.get().equals(false)) {
          LOG.error("NSSummary reprocess failed for one of the sub-tasks.");
          REBUILD_STATE.set(RebuildState.FAILED);
          return buildTaskResult(false);
        }
      }
      success = true;

    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      LOG.error("NSSummaryTask was interrupted.", ex);
      REBUILD_STATE.set(RebuildState.FAILED);
      return buildTaskResult(false);
    } catch (ExecutionException ex) {
      LOG.error("Error while reprocessing NSSummary table in Recon DB.", ex.getCause());
      REBUILD_STATE.set(RebuildState.FAILED);
      return buildTaskResult(false);
    } finally {
      executorService.shutdown();
      // Deterministic resource cleanup with timeout
      try {
        // get() ensures the work is done. awaitTermination ensures the workers are also verifiably gone.
        // It turns an asynchronous shutdown into a synchronous, deterministic one
        if (!executorService.awaitTermination(5, TimeUnit.MINUTES)) {
          LOG.warn("Executor service for NSSummaryTask did not terminate in the specified time.");
          executorService.shutdownNow();
        }
      } catch (InterruptedException ex) {
        LOG.error("NSSummaryTask executor service termination was interrupted.", ex);
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
      }

      long endTime = System.nanoTime();
      // Convert to milliseconds
      long durationInMillis =
          TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

      // Log performance metrics
      LOG.info("NSSummary reprocess execution time: {} milliseconds", durationInMillis);
      
      // Reset state to IDLE on successful completion
      if (success) {
        REBUILD_STATE.set(RebuildState.IDLE);
        LOG.info("NSSummary tree reprocess completed successfully with unified control.");
      }
    }

    return buildTaskResult(true);
  }

  /**
   * Build a TaskResult with the given success status.
   */
  @Override
  public TaskResult buildTaskResult(boolean success) {
    return new TaskResult.Builder()
        .setTaskName(getTaskName())
        .setTaskSuccess(success)
        .build();
  }

  /**
   * Reset rebuild state to IDLE. This is primarily for testing purposes.
   */
  @VisibleForTesting
  public static void resetRebuildState() {
    REBUILD_STATE.set(RebuildState.IDLE);
  }

  /**
   * Set rebuild state to FAILED. This is primarily for testing purposes.
   */
  @VisibleForTesting
  public static void setRebuildStateToFailed() {
    REBUILD_STATE.set(RebuildState.FAILED);
  }

  public ReconNamespaceSummaryManager getReconNamespaceSummaryManager() {
    return reconNamespaceSummaryManager;
  }
}
