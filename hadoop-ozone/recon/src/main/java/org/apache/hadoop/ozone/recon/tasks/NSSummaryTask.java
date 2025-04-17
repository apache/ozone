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
import javax.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
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

  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private final ReconOMMetadataManager reconOMMetadataManager;
  private final NSSummaryTaskWithFSO nsSummaryTaskWithFSO;
  private final NSSummaryTaskWithLegacy nsSummaryTaskWithLegacy;
  private final NSSummaryTaskWithOBS nsSummaryTaskWithOBS;

  @Inject
  public NSSummaryTask(ReconNamespaceSummaryManager
                       reconNamespaceSummaryManager,
                       ReconOMMetadataManager
                       reconOMMetadataManager,
                       OzoneConfiguration
                       ozoneConfiguration) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.reconOMMetadataManager = reconOMMetadataManager;
    long nsSummaryFlushToDBMaxThreshold = ozoneConfiguration.getLong(
        OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD,
        OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT);

    this.nsSummaryTaskWithFSO = new NSSummaryTaskWithFSO(
        reconNamespaceSummaryManager, reconOMMetadataManager,
        nsSummaryFlushToDBMaxThreshold);
    this.nsSummaryTaskWithLegacy = new NSSummaryTaskWithLegacy(
        reconNamespaceSummaryManager, reconOMMetadataManager,
        ozoneConfiguration, nsSummaryFlushToDBMaxThreshold);
    this.nsSummaryTaskWithOBS = new NSSummaryTaskWithOBS(
        reconNamespaceSummaryManager, reconOMMetadataManager, nsSummaryFlushToDBMaxThreshold);
  }

  @Override
  public String getTaskName() {
    return "NSSummaryTask";
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
    // Initialize a list of tasks to run in parallel
    Collection<Callable<Boolean>> tasks = new ArrayList<>();

    long startTime = System.nanoTime(); // Record start time

    try {
      // reinit Recon RocksDB's namespace CF.
      reconNamespaceSummaryManager.clearNSSummaryTable();
    } catch (IOException ioEx) {
      LOG.error("Unable to clear NSSummary table in Recon DB. ",
          ioEx);
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
    ExecutorService executorService = Executors.newFixedThreadPool(2,
        threadFactory);
    try {
      results = executorService.invokeAll(tasks);
      for (Future<Boolean> result : results) {
        if (result.get().equals(false)) {
          return buildTaskResult(false);
        }
      }
    } catch (InterruptedException | ExecutionException ex) {
      LOG.error("Error while reprocessing NSSummary table in Recon DB.", ex);
      return buildTaskResult(false);
    } finally {
      executorService.shutdown();

      long endTime = System.nanoTime();
      // Convert to milliseconds
      long durationInMillis =
          TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

      // Log performance metrics
      LOG.debug("Task execution time: {} milliseconds", durationInMillis);
    }

    return buildTaskResult(true);
  }

}
