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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.spi.ReconFileMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.util.ParallelTableIteratorOperation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that encapsulates the common code for file size count tasks.
 */
public abstract class FileSizeCountTaskHelper {
  protected static final Logger LOG = LoggerFactory.getLogger(FileSizeCountTaskHelper.class);
  
  // Static lock object for table truncation synchronization
  private static final Object TRUNCATE_LOCK = new Object();

  /**
   * Increments the count for a given key on a PUT event.
   */
  public static void handlePutKeyEvent(OmKeyInfo omKeyInfo,
                                       Map<FileSizeCountKey, Long> fileSizeCountMap) {
    fileSizeCountMap.compute(getFileSizeCountKey(omKeyInfo),
        (k, v) -> (v == null ? 0L : v) + 1L);
  }

  /**
   * Decrements the count for a given key on a DELETE event.
   */
  public static void handleDeleteKeyEvent(String key, OmKeyInfo omKeyInfo,
                                          Map<FileSizeCountKey, Long> fileSizeCountMap) {
    if (omKeyInfo == null) {
      LOG.warn("Deleting a key not found while handling DELETE key event. Key not found in Recon OM DB: {}", key);
    } else {
      fileSizeCountMap.compute(getFileSizeCountKey(omKeyInfo),
          (k, v) -> (v == null ? 0L : v) - 1L);
    }
  }

  /**
   * Returns a FileSizeCountKey for the given OmKeyInfo.
   */
  public static FileSizeCountKey getFileSizeCountKey(OmKeyInfo omKeyInfo) {
    return new FileSizeCountKey(omKeyInfo.getVolumeName(),
        omKeyInfo.getBucketName(),
        ReconUtils.getFileSizeUpperBound(omKeyInfo.getDataSize()));
  }

  /**
   * Truncates the file count table if needed during reprocess.
   * Uses a flag to ensure the table is truncated only once across all tasks.
   */
  public static void truncateFileCountTableIfNeeded(ReconFileMetadataManager reconFileMetadataManager,
                                                    String taskName) {
    synchronized (TRUNCATE_LOCK) {
      if (ReconConstants.FILE_SIZE_COUNT_TABLE_TRUNCATED.compareAndSet(false, true)) {
        try {
          reconFileMetadataManager.clearFileCountTable();
          LOG.info("Successfully truncated file count table for reprocess by task: {}", taskName);
        } catch (Exception e) {
          LOG.error("Failed to truncate file count table for task: {}", taskName, e);
          // Reset flag on failure so another task can try
          ReconConstants.FILE_SIZE_COUNT_TABLE_TRUNCATED.set(false);
          throw new RuntimeException("Failed to truncate file count table", e);
        }
      } else {
        LOG.debug("File count table already truncated by another task, skipping for task: {}", taskName);
      }
    }
  }

  /**
   * Executes the reprocess method using RocksDB for the given task.
   */
  public static ReconOmTask.TaskResult reprocess(OMMetadataManager omMetadataManager,
                                                 ReconFileMetadataManager reconFileMetadataManager,
                                                 BucketLayout bucketLayout,
                                                 String taskName,
                                                 int maxIterators,
                                                 int maxWorkers,
                                                 int maxKeysInMemory) {
    LOG.info("{}: Starting parallel RocksDB reprocess with {} iterators, {} workers for bucket layout {}",
        taskName, maxIterators, maxWorkers, bucketLayout);
    Map<FileSizeCountKey, Long> fileSizeCountMap = new ConcurrentHashMap<>();
    long overallStartTime = Time.monotonicNow();
    
    // Ensure the file count table is truncated only once during reprocess
    truncateFileCountTableIfNeeded(reconFileMetadataManager, taskName);
    
    long iterationStartTime = Time.monotonicNow();
    boolean status = reprocessBucketLayout(
        bucketLayout, omMetadataManager, fileSizeCountMap, reconFileMetadataManager, taskName,
        maxIterators, maxWorkers, maxKeysInMemory);
    if (!status) {
      return buildTaskResult(taskName, false);
    }
    long iterationEndTime = Time.monotonicNow();
    
    long writeStartTime = Time.monotonicNow();
    // Write remaining counts to DB (no global lock needed - FSO and OBS are mutually exclusive)
    writeCountsToDB(fileSizeCountMap, reconFileMetadataManager);
    long writeEndTime = Time.monotonicNow();
    
    long overallEndTime = Time.monotonicNow();
    long totalDurationMs = overallEndTime - overallStartTime;
    long iterationDurationMs = iterationEndTime - iterationStartTime;
    long writeDurationMs = writeEndTime - writeStartTime;
    
    LOG.info("{}: Parallel RocksDB reprocess completed - Total: {} ms (Iteration: {} ms, Write: {} ms) - " +
        "File count entries: {}", taskName, totalDurationMs, iterationDurationMs, writeDurationMs, 
        fileSizeCountMap.size());
    
    return buildTaskResult(taskName, true);
  }

  /**
   * Iterates over the OM DB keys for the given bucket layout using lockless per-worker maps.
   * Each worker maintains its own map to eliminate read lock contention.
   */
  public static boolean reprocessBucketLayout(BucketLayout bucketLayout,
                                              OMMetadataManager omMetadataManager,
                                              Map<FileSizeCountKey, Long> fileSizeCountMap,
                                              ReconFileMetadataManager reconFileMetadataManager,
                                              String taskName,
                                              int maxIterators,
                                              int maxWorkers,
                                              int maxKeysInMemory) {
    LOG.info("{}: Starting lockless parallel iteration with {} iterators, {} workers for bucket layout {}",
        taskName, maxIterators, maxWorkers, bucketLayout);
    Table<String, OmKeyInfo> omKeyInfoTable = omMetadataManager.getKeyTable(bucketLayout);
    long startTime = Time.monotonicNow();

    // Per-worker threshold: divide total by worker count
    final int PER_WORKER_THRESHOLD = Math.max(1, 200000 / maxWorkers);  // 200k / 20 = 10k
    
    // Each worker thread gets its own map (lockless updates!)
    //ThreadLocal<Map<FileSizeCountKey, Long>> workerLocalMap =
    //    ThreadLocal.withInitial(ConcurrentHashMap::new);

    Map<Long, Map<FileSizeCountKey, Long>> allMap = new ConcurrentHashMap<>();
    // Track all worker maps for final flush
    //List<Map<FileSizeCountKey, Long>> allWorkerMaps =
    //    Collections.synchronizedList(new ArrayList<>());
    
    // Single lock for DB flush operations only
    Object flushLock = new Object();
    
    // Worker operation - lockless map updates!
    Function<Table.KeyValue<String, OmKeyInfo>, Void> kvOperation = kv -> {
        // Get this worker thread's private map (no lock needed!)
        //Map<FileSizeCountKey, Long> myMap = workerLocalMap.get();
      Map<FileSizeCountKey, Long> myMap = allMap.computeIfAbsent(Thread.currentThread().getId(), (k) -> new HashMap<>());
      //Map<FileSizeCountKey, Long> myMap = allMap.get(Thread.currentThread().getId());
        
        // Register this worker's map on first use (thread-safe registration)
        /*if (!allWorkerMaps.contains(myMap)) {
            synchronized (allWorkerMaps) {
                if (!allWorkerMaps.contains(myMap)) {
                    allWorkerMaps.add(myMap);
                }
            }
        }*/
        
        // Update map without any locks - each worker owns its map!
        handlePutKeyEvent(kv.getValue(), myMap);
        
        // Check if this worker's map needs flushing
        if (myMap.size() >= PER_WORKER_THRESHOLD) {
            synchronized (flushLock) {
                // Double-check size after acquiring lock
                //if (myMap.size() >= PER_WORKER_THRESHOLD) {
                    LOG.info("{}: Worker flushing {} entries to RocksDB",
                        taskName, myMap.size());
                    writeCountsToDB(myMap, reconFileMetadataManager);
                    myMap.clear();
               // }
            }
        }
        return null;
    };
    
    try (ParallelTableIteratorOperation<String, OmKeyInfo> keyIter =
             new ParallelTableIteratorOperation<>(omMetadataManager, omKeyInfoTable,
                 StringCodec.get(), maxIterators, maxWorkers, maxKeysInMemory, PER_WORKER_THRESHOLD)) {
        keyIter.performTaskOnTableVals(taskName, null, null, kvOperation);
    } catch (Exception ex) {
        LOG.error("Unable to populate File Size Count for {} in RocksDB.", taskName, ex);
        return false;
    }
    
    // Final flush: Ensure all worker maps are written to DB
    // Since its a single thread flushing the maps one by one hence no need for the lock.
    LOG.info("{}: Final flush of {} worker maps", taskName, allMap.size());
    //synchronized (flushLock) {
        for (Map<FileSizeCountKey, Long> workerMap : allMap.values()) {
            if (!workerMap.isEmpty()) {
                LOG.info("{}: Flushing remaining {} entries from worker map",
                    taskName, workerMap.size());
                writeCountsToDB(workerMap, reconFileMetadataManager);
                workerMap.clear();
            }
        }
    //}
    
    // Clean up ThreadLocal to prevent memory leaks
    //workerLocalMap.remove();
    
    long endTime = Time.monotonicNow();
    long durationMs = endTime - startTime;
    double durationSec = durationMs / 1000.0;
    
    LOG.info("{}: Lockless parallel reprocess completed for {} in {} ms ({} sec) - Worker threshold: {} entries",
        taskName, bucketLayout, durationMs, String.format("%.2f", durationSec), PER_WORKER_THRESHOLD);
    
    return true;
  }

  /**
   * Processes a batch of OM update events using RocksDB.
   */
  public static ReconOmTask.TaskResult processEvents(OMUpdateEventBatch events,
                                                     String tableName,
                                                     ReconFileMetadataManager reconFileMetadataManager,
                                                     String taskName) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    Map<FileSizeCountKey, Long> fileSizeCountMap = new HashMap<>();
    long startTime = Time.monotonicNow();
    
    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, Object> omdbUpdateEvent = eventIterator.next();
      if (!tableName.equals(omdbUpdateEvent.getTable())) {
        continue;
      }
      
      String updatedKey = omdbUpdateEvent.getKey();
      Object value = omdbUpdateEvent.getValue();
      Object oldValue = omdbUpdateEvent.getOldValue();
      
      if (value instanceof OmKeyInfo) {
        OmKeyInfo omKeyInfo = (OmKeyInfo) value;
        OmKeyInfo omKeyInfoOld = (OmKeyInfo) oldValue;
        
        try {
          switch (omdbUpdateEvent.getAction()) {
          case PUT:
            handlePutKeyEvent(omKeyInfo, fileSizeCountMap);
            break;
          case DELETE:
            handleDeleteKeyEvent(updatedKey, omKeyInfo, fileSizeCountMap);
            break;
          case UPDATE:
            if (omKeyInfoOld != null) {
              handleDeleteKeyEvent(updatedKey, omKeyInfoOld, fileSizeCountMap);
              handlePutKeyEvent(omKeyInfo, fileSizeCountMap);
            } else {
              LOG.warn("Update event does not have the old keyInfo for {}.", updatedKey);
            }
            break;
          default:
            LOG.trace("Skipping DB update event: {}", omdbUpdateEvent.getAction());
          }
        } catch (Exception e) {
          LOG.error("Unexpected exception while processing key {}.", updatedKey, e);
          return buildTaskResult(taskName, false);
        }
      } else {
        LOG.warn("Unexpected value type {} for key {}. Skipping processing.",
            value.getClass().getName(), updatedKey);
      }
    }
    
    // Write remaining counts to DB (no lock needed for incremental processing)
    writeCountsToDB(fileSizeCountMap, reconFileMetadataManager);
    
    LOG.debug("{} successfully processed using RocksDB in {} milliseconds", taskName,
        (Time.monotonicNow() - startTime));
    return buildTaskResult(taskName, true);
  }

  /**
   * Writes the accumulated file size counts to RocksDB using ReconFileMetadataManager.
   * 
   * Thread Safety: FSO and OBS tasks write to different bucket keys (mutually exclusive),
   * so no global lock is needed. RocksDB handles concurrent writes to different keys safely.
   */
  public static void writeCountsToDB(Map<FileSizeCountKey, Long> fileSizeCountMap,
                                     ReconFileMetadataManager reconFileMetadataManager) {
    if (fileSizeCountMap.isEmpty()) {
      return;
    }

    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      for (Map.Entry<FileSizeCountKey, Long> entry : fileSizeCountMap.entrySet()) {
        FileSizeCountKey key = entry.getKey();
        Long deltaCount = entry.getValue();

        // Read-modify-write: Read current count, add delta, write back
        Long existingCount = reconFileMetadataManager.getFileSizeCount(key);
        Long newCount = (existingCount != null ? existingCount : 0L) + deltaCount;

        if (newCount > 0L) {
          reconFileMetadataManager.batchStoreFileSizeCount(rdbBatchOperation, key, newCount);
        } else if (existingCount != null) {
          // Delete key if count becomes 0 or negative
          reconFileMetadataManager.batchDeleteFileSizeCount(rdbBatchOperation, key);
        }
      }

      reconFileMetadataManager.commitBatchOperation(rdbBatchOperation);
    } catch (Exception e) {
      LOG.error("Error writing file size counts to RocksDB", e);
      throw new RuntimeException("Failed to write to RocksDB", e);
    }
  }

  public static ReconOmTask.TaskResult buildTaskResult(String taskName, boolean success) {
    return new ReconOmTask.TaskResult.Builder()
        .setTaskName(taskName)
        .setTaskSuccess(success)
        .build();
  }
}
