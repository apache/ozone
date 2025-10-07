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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.spi.ReconFileMetadataManager;
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
    FileSizeCountKey key = getFileSizeCountKey(omKeyInfo);
    Long count = fileSizeCountMap.containsKey(key) ? fileSizeCountMap.get(key) + 1L : 1L;
    fileSizeCountMap.put(key, count);
  }

  /**
   * Decrements the count for a given key on a DELETE event.
   */
  public static void handleDeleteKeyEvent(String key, OmKeyInfo omKeyInfo,
                                          Map<FileSizeCountKey, Long> fileSizeCountMap) {
    if (omKeyInfo == null) {
      LOG.warn("Deleting a key not found while handling DELETE key event. Key not found in Recon OM DB: {}", key);
    } else {
      FileSizeCountKey countKey = getFileSizeCountKey(omKeyInfo);
      Long count = fileSizeCountMap.containsKey(countKey) ? fileSizeCountMap.get(countKey) - 1L : -1L;
      fileSizeCountMap.put(countKey, count);
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
                                                 String taskName) {
    LOG.info("Starting RocksDB Reprocess for {}", taskName);
    Map<FileSizeCountKey, Long> fileSizeCountMap = new HashMap<>();
    long startTime = Time.monotonicNow();
    
    // Ensure the file count table is truncated only once during reprocess
    truncateFileCountTableIfNeeded(reconFileMetadataManager, taskName);
    
    boolean status = reprocessBucketLayout(
        bucketLayout, omMetadataManager, fileSizeCountMap, reconFileMetadataManager, taskName);
    if (!status) {
      return buildTaskResult(taskName, false);
    }
    
    writeCountsToDB(fileSizeCountMap, reconFileMetadataManager);
    
    long endTime = Time.monotonicNow();
    LOG.info("{} completed RocksDB Reprocess in {} ms.", taskName, (endTime - startTime));
    
    return buildTaskResult(taskName, true);
  }

  /**
   * Iterates over the OM DB keys for the given bucket layout and updates the fileSizeCountMap (RocksDB version).
   */
  public static boolean reprocessBucketLayout(BucketLayout bucketLayout,
                                              OMMetadataManager omMetadataManager,
                                              Map<FileSizeCountKey, Long> fileSizeCountMap,
                                              ReconFileMetadataManager reconFileMetadataManager,
                                              String taskName) {
    Table<String, OmKeyInfo> omKeyInfoTable = omMetadataManager.getKeyTable(bucketLayout);
    int totalKeysProcessed = 0;
    
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> keyIter =
             omKeyInfoTable.iterator()) {
      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
        handlePutKeyEvent(kv.getValue(), fileSizeCountMap);
        totalKeysProcessed++;

        // Flush to RocksDB periodically.
        if (fileSizeCountMap.size() >= 100000) {
          // For reprocess, we don't need to check existing values since table was truncated
          LOG.debug("Flushing {} accumulated counts to RocksDB for {}", fileSizeCountMap.size(), taskName);
          writeCountsToDB(fileSizeCountMap, reconFileMetadataManager);
          fileSizeCountMap.clear();
        }
      }
    } catch (IOException ioEx) {
      LOG.error("Unable to populate File Size Count for {} in RocksDB.", taskName, ioEx);
      return false;
    }
    
    LOG.info("Reprocessed {} keys for bucket layout {} using RocksDB.", totalKeysProcessed, bucketLayout);
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
    
    writeCountsToDB(fileSizeCountMap, reconFileMetadataManager);
    
    LOG.debug("{} successfully processed using RocksDB in {} milliseconds", taskName,
        (Time.monotonicNow() - startTime));
    return buildTaskResult(taskName, true);
  }

  /**
   * Writes the accumulated file size counts to RocksDB using ReconFileMetadataManager.
   */
  /**
   * Checks if the file count table is empty by trying to get the first entry.
   * This mimics the SQL Derby behavior of isFileCountBySizeTableEmpty().
   */
  private static boolean isFileCountTableEmpty(ReconFileMetadataManager reconFileMetadataManager) {
    try (TableIterator<FileSizeCountKey, ? extends Table.KeyValue<FileSizeCountKey, Long>> iterator = 
         reconFileMetadataManager.getFileCountTable().iterator()) {
      return !iterator.hasNext();
    } catch (Exception e) {
      LOG.warn("Error checking if file count table is empty, assuming not empty", e);
      return false;
    }
  }

  public static void writeCountsToDB(Map<FileSizeCountKey, Long> fileSizeCountMap,
                                     ReconFileMetadataManager reconFileMetadataManager) {
    if (fileSizeCountMap.isEmpty()) {
      return;
    }
    
    boolean isTableEmpty = isFileCountTableEmpty(reconFileMetadataManager);
    
    LOG.debug("writeCountsToDB: processing {} entries, isTableEmpty={}", 
        fileSizeCountMap.size(), isTableEmpty);

    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      for (Map.Entry<FileSizeCountKey, Long> entry : fileSizeCountMap.entrySet()) {
        FileSizeCountKey key = entry.getKey();
        Long deltaCount = entry.getValue();
        
        LOG.debug("Processing key: {}, deltaCount: {}", key, deltaCount);
        
        if (isTableEmpty) {
          // Direct insert when table is empty (like SQL Derby reprocess behavior)
          LOG.debug("Direct insert (table empty): key={}, deltaCount={}", key, deltaCount);
          if (deltaCount > 0L) {
            reconFileMetadataManager.batchStoreFileSizeCount(rdbBatchOperation, key, deltaCount);
            LOG.debug("Storing key={} with deltaCount={}", key, deltaCount);
          }
        } else {
          // Incremental update when table has data (like SQL Derby incremental behavior)
          Long existingCount = reconFileMetadataManager.getFileSizeCount(key);
          Long newCount = (existingCount != null ? existingCount : 0L) + deltaCount;
          
          LOG.debug("Incremental update: key={}, existingCount={}, deltaCount={}, newCount={}", 
              key, existingCount, deltaCount, newCount);
          
          if (newCount > 0L) {
            reconFileMetadataManager.batchStoreFileSizeCount(rdbBatchOperation, key, newCount);
            LOG.debug("Storing key={} with newCount={}", key, newCount);
          } else if (existingCount != null) {
            // Delete key if count becomes 0 or negative
            reconFileMetadataManager.batchDeleteFileSizeCount(rdbBatchOperation, key);
            LOG.debug("Deleting key={} as newCount={} <= 0", key, newCount);
          }
        }
      }
      
      LOG.debug("Committing batch operation with {} operations", fileSizeCountMap.size());
      reconFileMetadataManager.commitBatchOperation(rdbBatchOperation);
      LOG.debug("Batch operation committed successfully");
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
