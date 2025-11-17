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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
   * GLOBAL lock for cross-task synchronization during DB writes.
   * 
   * Scope: Shared across ALL tasks (FSO, OBS, Legacy)
   * Protects: RocksDB read-modify-write operations
   * Purpose: Ensures atomic updates when multiple tasks flush concurrently to the same bins
   * 
   * IMPORTANT: Callers MUST acquire this lock before calling writeCountsToDB().
   * This lock should NOT be acquired inside writeCountsToDB() to avoid nested locking.
   */
  public static final ReentrantReadWriteLock FILE_COUNT_WRITE_LOCK = 
      new ReentrantReadWriteLock();

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
    // Acquire GLOBAL lock (cross-task) before writing to DB
    FILE_COUNT_WRITE_LOCK.writeLock().lock();
    try {
      writeCountsToDB(fileSizeCountMap, reconFileMetadataManager);
    } finally {
      FILE_COUNT_WRITE_LOCK.writeLock().unlock();
    }
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
   * Iterates over the OM DB keys for the given bucket layout and updates the fileSizeCountMap (RocksDB version).
   */
  public static boolean reprocessBucketLayout(BucketLayout bucketLayout,
                                              OMMetadataManager omMetadataManager,
                                              Map<FileSizeCountKey, Long> fileSizeCountMap,
                                              ReconFileMetadataManager reconFileMetadataManager,
                                              String taskName,
                                              int maxIterators,
                                              int maxWorkers,
                                              int maxKeysInMemory) {
    LOG.info("{}: Starting parallel iteration with {} iterators, {} workers for bucket layout {}",
        taskName, maxIterators, maxWorkers, bucketLayout);
    Table<String, OmKeyInfo> omKeyInfoTable = omMetadataManager.getKeyTable(bucketLayout);
    long startTime = Time.monotonicNow();
    
    // LOCAL lock (task-specific) - coordinates worker threads within this task only
    // Protects: in-memory fileSizeCountMap from concurrent access by workers in THIS task
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    // Flag to coordinate flush attempts - prevents all threads from queuing for write lock
    AtomicBoolean isFlushingInProgress = new AtomicBoolean(false);
    final int FLUSH_THRESHOLD = 200000;
    
    // Use parallel table iteration
    Function<Table.KeyValue<String, OmKeyInfo>, Void> kvOperation = kv -> {
      try {
        lock.readLock().lock();
        handlePutKeyEvent(kv.getValue(), fileSizeCountMap);
      } finally {
        lock.readLock().unlock();
      }
      
      // Only one thread should attempt flush to avoid blocking all workers
      if (fileSizeCountMap.size() >= FLUSH_THRESHOLD &&
          isFlushingInProgress.compareAndSet(false, true)) {
        try {
          // Step 1: Acquire LOCAL lock (task-specific) to stop worker threads in THIS task
          lock.writeLock().lock();
          try {
            // Double-check after acquiring write lock
            if (fileSizeCountMap.size() >= FLUSH_THRESHOLD) {
              LOG.debug("Flushing {} accumulated counts to RocksDB for {}", fileSizeCountMap.size(), taskName);
              // Step 2: Acquire GLOBAL lock (cross-task) to protect DB from concurrent FSO/OBS/Legacy writes
              FILE_COUNT_WRITE_LOCK.writeLock().lock();
              try {
                writeCountsToDB(fileSizeCountMap, reconFileMetadataManager);
                fileSizeCountMap.clear();
              } finally {
                FILE_COUNT_WRITE_LOCK.writeLock().unlock();  // Release GLOBAL lock
              }
            }
          } finally {
            isFlushingInProgress.set(false);  // Reset flag after flush completes
            lock.writeLock().unlock();  // Release LOCAL lock
          }
        } catch (Exception e) {
          throw new RuntimeException("Failed to flush file size counts", e);
        }
      }
      return null;
    };
    
    try (ParallelTableIteratorOperation<String, OmKeyInfo> keyIter =
             new ParallelTableIteratorOperation<>(omMetadataManager, omKeyInfoTable,
                 StringCodec.get(), maxIterators, maxWorkers, maxKeysInMemory, FLUSH_THRESHOLD)) {
      keyIter.performTaskOnTableVals(taskName, null, null, kvOperation);
    } catch (Exception ex) {
      LOG.error("Unable to populate File Size Count for {} in RocksDB.", taskName, ex);
      return false;
    }
    
    long endTime = Time.monotonicNow();
    long durationMs = endTime - startTime;
    double durationSec = durationMs / 1000.0;
    int totalKeys = fileSizeCountMap.values().stream().mapToInt(Long::intValue).sum();
    double throughput = totalKeys / Math.max(durationSec, 0.001);
    
    LOG.info("{}: Reprocessed {} keys for bucket layout {} in {} ms ({} sec) - Throughput: {}/sec",
        taskName, totalKeys, bucketLayout, durationMs, String.format("%.2f", durationSec), 
        String.format("%.2f", throughput));
    
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
    
    // Acquire GLOBAL lock (cross-task) before writing to DB
    FILE_COUNT_WRITE_LOCK.writeLock().lock();
    try {
      writeCountsToDB(fileSizeCountMap, reconFileMetadataManager);
    } finally {
      FILE_COUNT_WRITE_LOCK.writeLock().unlock();
    }
    
    LOG.debug("{} successfully processed using RocksDB in {} milliseconds", taskName,
        (Time.monotonicNow() - startTime));
    return buildTaskResult(taskName, true);
  }

  /**
   * Writes the accumulated file size counts to RocksDB using ReconFileMetadataManager.
   * 
   * IMPORTANT: Caller MUST hold FILE_COUNT_WRITE_LOCK.writeLock() before calling this method.
   * This ensures atomic read-modify-write operations across concurrent FSO/OBS task flushes.
   */
  public static void writeCountsToDB(Map<FileSizeCountKey, Long> fileSizeCountMap,
                                     ReconFileMetadataManager reconFileMetadataManager) {
    if (fileSizeCountMap.isEmpty()) {
      return;
    }

    // Note: Caller must already hold FILE_COUNT_WRITE_LOCK.writeLock()
    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      for (Map.Entry<FileSizeCountKey, Long> entry : fileSizeCountMap.entrySet()) {
        FileSizeCountKey key = entry.getKey();
        Long deltaCount = entry.getValue();

        // Always do read-modify-write to handle concurrent task flushes correctly
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
