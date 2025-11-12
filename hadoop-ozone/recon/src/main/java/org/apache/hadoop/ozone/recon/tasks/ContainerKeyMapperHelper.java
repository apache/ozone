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
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.api.types.KeyPrefixContainer;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.util.ParallelTableIteratorOperation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that encapsulates the common logic for ContainerKeyMapperTaskFSO and ContainerKeyMapperTaskOBS.
 */
public abstract class ContainerKeyMapperHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerKeyMapperHelper.class);

  // Static lock to guard table truncation.
  private static final Object TRUNCATE_LOCK = new Object();

  /**
   * SHARED across all tasks (FSO + OBS) for cross-task synchronization.
   * Maps: ContainerId -> AtomicLong (key count in that container)
   * Purpose: Prevents data corruption when FSO and OBS tasks run concurrently
   * and both write to the same container IDs. Both tasks accumulate into this
   * single shared map, ensuring final DB write contains complete totals.
   */
  private static final Map<Long, AtomicLong> SHARED_CONTAINER_KEY_COUNT_MAP = new ConcurrentHashMap<>();

  // Lock to guard shared map initialization/clearing
  private static final Object SHARED_MAP_LOCK = new Object();

  /**
   * Ensures that the container key tables are truncated only once before reprocessing.
   * Uses an AtomicBoolean to track if truncation has already been performed.
   *
   * @param reconContainerMetadataManager The metadata manager instance responsible for DB operations.
   */
  public static void truncateTablesIfNeeded(ReconContainerMetadataManager reconContainerMetadataManager,
                                            String taskName) {
    synchronized (TRUNCATE_LOCK) {
      if (ReconConstants.CONTAINER_KEY_TABLES_TRUNCATED.compareAndSet(false, true)) {
        try {
          // Perform table truncation
          reconContainerMetadataManager.reinitWithNewContainerDataFromOm(Collections.emptyMap());
          LOG.debug("Successfully truncated container key tables.");
        } catch (Exception e) {
          // Reset the flag so truncation can be retried
          ReconConstants.CONTAINER_KEY_TABLES_TRUNCATED.set(false);
          LOG.error("Error while truncating container key tables for task {}. Resetting flag.", taskName, e);
          throw new RuntimeException("Table truncation failed", e);
        }
      } else {
        LOG.debug("Container key tables already truncated by another task.");
      }
    }
  }

  /**
   * Ensures the shared container count map is cleared once per reprocess cycle.
   * This must be called by the first task that starts reprocessing to prevent
   * cross-task data corruption where FSO and OBS tasks overwrite each other's counts.
   */
  private static void initializeSharedContainerCountMapIfNeeded(String taskName) {
    synchronized (SHARED_MAP_LOCK) {
      if (ReconConstants.CONTAINER_KEY_COUNT_MAP_INITIALIZED.compareAndSet(false, true)) {
        SHARED_CONTAINER_KEY_COUNT_MAP.clear();
        LOG.info("{}: Initialized shared container count map for cross-task synchronization", taskName);
      } else {
        LOG.debug("{}: Shared container count map already initialized by another task", taskName);
      }
    }
  }

  /**
   * Clears the shared container count map and resets its initialization flag.
   * This method should be called by tests to ensure clean state between test runs.
   */
  public static void clearSharedContainerCountMap() {
    synchronized (SHARED_MAP_LOCK) {
      SHARED_CONTAINER_KEY_COUNT_MAP.clear();
      ReconConstants.CONTAINER_KEY_COUNT_MAP_INITIALIZED.set(false);
    }
  }

  public static boolean reprocess(OMMetadataManager omMetadataManager,
                                                ReconContainerMetadataManager reconContainerMetadataManager,
                                                BucketLayout bucketLayout,
                                                String taskName,
                                                long containerKeyFlushToDBMaxThreshold,
                                                int maxIterators,
                                                int maxWorkers,
                                                int maxKeysInMemory) {
    AtomicLong omKeyCount = new AtomicLong(0);
    // Local map: per-task ContainerKeyPrefix mappings (cleared on flush)
    Map<ContainerKeyPrefix, Integer> localContainerKeyMap = new ConcurrentHashMap<>();
    // Shared map: cross-task container counts (FSO + OBS accumulate here)

    try {
      LOG.info("{}: Starting parallel reprocess with {} iterators, {} workers, max {} keys in memory for bucket layout {}",
          taskName, maxIterators, maxWorkers, maxKeysInMemory, bucketLayout);
      Instant start = Instant.now();

      // Ensure the tables are truncated only once
      truncateTablesIfNeeded(reconContainerMetadataManager, taskName);

      // Initialize shared container count map once per cycle
      initializeSharedContainerCountMapIfNeeded(taskName);

      // Get the appropriate table based on BucketLayout
      Table<String, OmKeyInfo> omKeyInfoTable = omMetadataManager.getKeyTable(bucketLayout);

      // Use fair lock to prevent write lock starvation when flushing
      ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
      // Flag to coordinate flush attempts - prevents all threads from queuing for write lock
      AtomicBoolean isFlushingInProgress = new AtomicBoolean(false);
      
      // Use parallel table iteration
      Function<Table.KeyValue<String, OmKeyInfo>, Void> kvOperation = kv -> {
        try {
          try {
            lock.readLock().lock();
            handleKeyReprocess(kv.getKey(), kv.getValue(), localContainerKeyMap, SHARED_CONTAINER_KEY_COUNT_MAP,
                reconContainerMetadataManager);
          } finally {
            lock.readLock().unlock();
          }
          omKeyCount.incrementAndGet();
          
          // Only one thread should attempt flush to avoid blocking all workers
          if (localContainerKeyMap.size() >= containerKeyFlushToDBMaxThreshold &&
              isFlushingInProgress.compareAndSet(false, true)) {
            try {
              lock.writeLock().lock();
              try {
                if (!checkAndCallFlushToDB(localContainerKeyMap, containerKeyFlushToDBMaxThreshold,
                    reconContainerMetadataManager)) {
                  throw new UncheckedIOException(new IOException("Unable to flush containerKey information to the DB"));
                }
              } finally {
                isFlushingInProgress.set(false);  // Reset flag after flush completes
              }
            } finally {
              lock.writeLock().unlock();
            }
          }
          return null;
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      };
      try (ParallelTableIteratorOperation<String, OmKeyInfo> keyIter =
               new ParallelTableIteratorOperation<>(omMetadataManager, omKeyInfoTable,
                   StringCodec.get(), maxIterators, maxWorkers, maxKeysInMemory, containerKeyFlushToDBMaxThreshold)) {
        keyIter.performTaskOnTableVals(taskName, null, null, kvOperation);
      }

      // Final flush and commit: local map cleared, shared map kept for other tasks
      if (!flushAndCommitContainerKeyInfoToDB(localContainerKeyMap, SHARED_CONTAINER_KEY_COUNT_MAP, reconContainerMetadataManager)) {
        LOG.error("Failed to flush Container Key data to DB for {}", taskName);
        return false;
      }

      Instant end = Instant.now();
      long durationMillis = Duration.between(start, end).toMillis();
      double durationSeconds = (double) durationMillis / 1000.0;
      long keysProcessed = omKeyCount.get();
      double throughput = keysProcessed / Math.max(durationSeconds, 0.001);
      
      LOG.info("{}: Parallel reprocess completed. Processed {} keys in {} ms ({} sec) - " +
          "Throughput: {} keys/sec - Containers (shared): {}, Container-Key mappings (local): {}",
          taskName, keysProcessed, durationMillis, String.format("%.2f", durationSeconds),
          String.format("%.2f", throughput), SHARED_CONTAINER_KEY_COUNT_MAP.size(), localContainerKeyMap.size());
    } catch (Exception ex) {
      LOG.error("Error populating Container Key data for {} in Recon DB.", taskName, ex);
      return false;
    }
    return true;
  }

  private static synchronized boolean checkAndCallFlushToDB(Map<ContainerKeyPrefix, Integer> localContainerKeyMap,
                                               long containerKeyFlushToDBMaxThreshold,
                                               ReconContainerMetadataManager reconContainerMetadataManager) {
    if (localContainerKeyMap.size() >= containerKeyFlushToDBMaxThreshold) {
      return flushAndCommitContainerKeyInfoToDB(localContainerKeyMap, Collections.emptyMap(), reconContainerMetadataManager);
    }
    return true;
  }

  public static boolean process(OMUpdateEventBatch events,
                                               String tableName,
                                               ReconContainerMetadataManager reconContainerMetadataManager,
                                               String taskName) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    int eventCount = 0;

    // In-memory maps for fast look up and batch write
    // (HDDS-8580) localContainerKeyMap map is allowed to be used
    // in "process" without batching since the maximum number of keys
    // is bounded by delta limit configurations

    // Local map: (container, key) -> count (per event batch)
    Map<ContainerKeyPrefix, Integer> localContainerKeyMap = new HashMap<>();
    // Local map: containerId -> key count (per event batch)
    Map<Long, Long> localContainerKeyCountMap = new HashMap<>();
    // List of the deleted (container, key) pair's
    List<ContainerKeyPrefix> deletedKeyCountList = new ArrayList<>();
    long startTime = Time.monotonicNow();

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, OmKeyInfo> omdbUpdateEvent = eventIterator.next();
      // Filter event inside process method to avoid duping
      if (!tableName.equals(omdbUpdateEvent.getTable())) {
        continue;
      }
      String updatedKey = omdbUpdateEvent.getKey();
      OmKeyInfo updatedKeyValue = omdbUpdateEvent.getValue();
      try {
        switch (omdbUpdateEvent.getAction()) {
        case PUT:
          handlePutOMKeyEvent(updatedKey, updatedKeyValue, localContainerKeyMap,
              localContainerKeyCountMap, deletedKeyCountList, reconContainerMetadataManager);
          break;

        case DELETE:
          handleDeleteOMKeyEvent(updatedKey, localContainerKeyMap,
              localContainerKeyCountMap, deletedKeyCountList, reconContainerMetadataManager);
          break;

        case UPDATE:
          if (omdbUpdateEvent.getOldValue() != null) {
            handleDeleteOMKeyEvent(
                omdbUpdateEvent.getOldValue().getKeyName(), localContainerKeyMap,
                localContainerKeyCountMap, deletedKeyCountList, reconContainerMetadataManager);
          } else {
            LOG.warn("Update event does not have the old Key Info for {}.", updatedKey);
          }
          handlePutOMKeyEvent(updatedKey, updatedKeyValue, localContainerKeyMap,
              localContainerKeyCountMap, deletedKeyCountList, reconContainerMetadataManager);
          break;

        default:
          LOG.info("Skipping DB update event: {}", omdbUpdateEvent.getAction());
        }
        eventCount++;
      } catch (IOException e) {
        LOG.error("Unexpected exception while updating key data: {} ", updatedKey, e);
        return false;
      }
    }
    try {
      // Convert local Long map to AtomicLong map for writeToTheDB compatibility
      Map<Long, AtomicLong> localContainerKeyCountMapAtomic = new ConcurrentHashMap<>();
      localContainerKeyCountMap.forEach((k, v) -> localContainerKeyCountMapAtomic.put(k, new AtomicLong(v)));
      writeToTheDB(localContainerKeyMap, localContainerKeyCountMapAtomic, deletedKeyCountList, reconContainerMetadataManager);
    } catch (IOException e) {
      LOG.error("Unable to write Container Key Prefix data in Recon DB.", e);
      return false;
    }
    LOG.debug("{} successfully processed {} OM DB update event(s) in {} milliseconds.",
        taskName, eventCount, (Time.monotonicNow() - startTime));
    return true;
  }

  /**
   * Note to add an OM key and update containerID -&gt; no. of keys count.
   *
   * @param key key String
   * @param omKeyInfo omKeyInfo value
   * @param containerKeyMap we keep the added containerKeys in this map
   *                        (in this batch)
   * @param containerKeyCountMap we keep the containerKey counts in this map
   * @param deletedContainerKeyList list of the deleted containerKeys
   * @throws IOException if unable to write to recon DB.
   */
  private static void handlePutOMKeyEvent(String key, OmKeyInfo omKeyInfo,
                                          Map<ContainerKeyPrefix, Integer> containerKeyMap,
                                          Map<Long, Long> containerKeyCountMap,
                                          List<ContainerKeyPrefix> deletedContainerKeyList,
                                          ReconContainerMetadataManager reconContainerMetadataManager)
      throws IOException {
    long containerCountToIncrement = 0;
    for (OmKeyLocationInfoGroup omKeyLocationInfoGroup : omKeyInfo.getKeyLocationVersions()) {
      long keyVersion = omKeyLocationInfoGroup.getVersion();
      for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfoGroup.getLocationList()) {
        long containerId = omKeyLocationInfo.getContainerID();
        ContainerKeyPrefix containerKeyPrefix = ContainerKeyPrefix.get(containerId, key, keyVersion);
        if (reconContainerMetadataManager.getCountForContainerKeyPrefix(containerKeyPrefix) == 0 &&
            !containerKeyMap.containsKey(containerKeyPrefix)) {
          // Save on writes. No need to save same container-key prefix
          // mapping again.
          containerKeyMap.put(containerKeyPrefix, 1);
          // Remove the container-key prefix from the deleted list if we
          // previously deleted it in this batch (and now we add it again)
          deletedContainerKeyList.remove(containerKeyPrefix);

          // check if container already exists and
          // increment the count of containers if it does not exist
          if (!reconContainerMetadataManager.doesContainerExists(containerId) &&
              !containerKeyCountMap.containsKey(containerId)) {
            containerCountToIncrement++;
          }

          // update the count of keys for the given containerID
          long keyCount;
          if (containerKeyCountMap.containsKey(containerId)) {
            keyCount = containerKeyCountMap.get(containerId);
          } else {
            keyCount = reconContainerMetadataManager.getKeyCountForContainer(containerId);
          }

          // increment the count and update containerKeyCount.
          // keyCount will be 0 if containerID is not found. So, there is no
          // need to initialize keyCount for the first time.
          containerKeyCountMap.put(containerId, ++keyCount);
        }
      }
    }

    if (containerCountToIncrement > 0) {
      reconContainerMetadataManager.incrementContainerCountBy(containerCountToIncrement);
    }
  }

  /**
   * Note to delete an OM Key and update the containerID -&gt; no. of keys counts
   * (we are preparing for batch deletion in these data structures).
   *
   * @param key key String.
   * @param containerKeyMap we keep the added containerKeys in this map
   *                        (in this batch)
   * @param containerKeyCountMap we keep the containerKey counts in this map
   * @param deletedContainerKeyList list of the deleted containerKeys
   * @throws IOException If Unable to write to container DB.
   */
  private static void handleDeleteOMKeyEvent(String key,
                                             Map<ContainerKeyPrefix, Integer> containerKeyMap,
                                             Map<Long, Long> containerKeyCountMap,
                                             List<ContainerKeyPrefix> deletedContainerKeyList,
                                             ReconContainerMetadataManager reconContainerMetadataManager)
      throws IOException {

    Set<ContainerKeyPrefix> keysToBeDeleted = new HashSet<>();
    try (TableIterator<KeyPrefixContainer, ? extends Table.KeyValue<KeyPrefixContainer, Integer>>
             keyContainerIterator = reconContainerMetadataManager.getKeyContainerTableIterator()) {

      // Check if we have keys in this container in the DB
      keyContainerIterator.seek(KeyPrefixContainer.get(key));
      while (keyContainerIterator.hasNext()) {
        Table.KeyValue<KeyPrefixContainer, Integer> keyValue = keyContainerIterator.next();
        String keyPrefix = keyValue.getKey().getKeyPrefix();
        if (keyPrefix.equals(key)) {
          if (keyValue.getKey().getContainerId() != -1) {
            keysToBeDeleted.add(keyValue.getKey().toContainerKeyPrefix());
          }
        } else {
          break;
        }
      }
    }

    // Check if we have keys in this container in our containerKeyMap
    containerKeyMap.keySet().forEach((ContainerKeyPrefix containerKeyPrefix) -> {
      String keyPrefix = containerKeyPrefix.getKeyPrefix();
      if (keyPrefix.equals(key)) {
        keysToBeDeleted.add(containerKeyPrefix);
      }
    });

    for (ContainerKeyPrefix containerKeyPrefix : keysToBeDeleted) {
      deletedContainerKeyList.add(containerKeyPrefix);
      // Remove the container-key prefix from the map if we previously added
      // it in this batch (and now we delete it)
      containerKeyMap.remove(containerKeyPrefix);

      // Decrement count and update containerKeyCount.
      Long containerID = containerKeyPrefix.getContainerId();
      long keyCount;
      if (containerKeyCountMap.containsKey(containerID)) {
        keyCount = containerKeyCountMap.get(containerID);
      } else {
        keyCount = reconContainerMetadataManager.getKeyCountForContainer(containerID);
      }
      if (keyCount > 0) {
        containerKeyCountMap.put(containerID, --keyCount);
      }
    }
  }

  private static void writeToTheDB(Map<ContainerKeyPrefix, Integer> localContainerKeyMap,
                                   Map<Long, AtomicLong> containerKeyCountMap,
                                   List<ContainerKeyPrefix> deletedContainerKeyList,
                                   ReconContainerMetadataManager reconContainerMetadataManager)
      throws IOException {

    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {

      // Write container key mappings (local per-task data)
      localContainerKeyMap.keySet().forEach((ContainerKeyPrefix key) -> {
        try {
          reconContainerMetadataManager.batchStoreContainerKeyMapping(
              rdbBatchOperation, key, localContainerKeyMap.get(key));
        } catch (IOException e) {
          LOG.error("Unable to write Container Key Prefix data in Recon DB.", e);
        }
      });

      // Write container key count mappings (can be local or shared depending on caller)
      containerKeyCountMap.keySet().forEach((Long key) -> {
        try {
          long count = containerKeyCountMap.get(key).get();  // Get value from AtomicLong
          reconContainerMetadataManager.batchStoreContainerKeyCounts(
              rdbBatchOperation, key, count);
        } catch (IOException e) {
          LOG.error("Unable to write Container Key Count data in Recon DB.", e);
        }
      });

      // Delete container key mappings
      deletedContainerKeyList.forEach((ContainerKeyPrefix key) -> {
        try {
          reconContainerMetadataManager.batchDeleteContainerMapping(
              rdbBatchOperation, key);
        } catch (IOException e) {
          LOG.error("Unable to delete Container Key Prefix data in Recon DB.", e);
        }
      });

      // Commit batch operation
      reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);
    }
  }

  /**
   * Write an OM key to container DB and update containerID -&gt; no. of keys
   * count to the Global Stats table.
   *
   * @param key key String
   * @param omKeyInfo omKeyInfo value
   * @param localContainerKeyMap Local per-task map for ContainerKeyPrefix mappings
   *                             (cleared on flush, not shared between tasks)
   * @param sharedContainerKeyCountMap Shared cross-task map for container counts
   *                                   (FSO + OBS both update this, uses AtomicLong for thread safety)
   * @param reconContainerMetadataManager Recon metadata manager instance
   * @throws IOException if unable to write to recon DB.
   */
  public static void handleKeyReprocess(String key,
                                        OmKeyInfo omKeyInfo,
                                        Map<ContainerKeyPrefix, Integer> localContainerKeyMap,
                                        Map<Long, AtomicLong> sharedContainerKeyCountMap,
                                        ReconContainerMetadataManager reconContainerMetadataManager)
      throws IOException {

    AtomicLong containerCountToIncrement = new AtomicLong(0);

    for (OmKeyLocationInfoGroup omKeyLocationInfoGroup : omKeyInfo.getKeyLocationVersions()) {
      long keyVersion = omKeyLocationInfoGroup.getVersion();
      for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfoGroup.getLocationList()) {
        long containerId = omKeyLocationInfo.getContainerID();
        ContainerKeyPrefix containerKeyPrefix = ContainerKeyPrefix.get(containerId, key, keyVersion);

        if (reconContainerMetadataManager.getCountForContainerKeyPrefix(containerKeyPrefix) == 0
            && !localContainerKeyMap.containsKey(containerKeyPrefix)) {
          // Save on writes. No need to save same container-key prefix mapping again.
          localContainerKeyMap.put(containerKeyPrefix, 1);

          // Thread-safe increment using shared AtomicLong map (cross-task safe: FSO + OBS)
          AtomicLong count = sharedContainerKeyCountMap.computeIfAbsent(containerId, 
              k -> new AtomicLong(0));
          
          long newCount = count.incrementAndGet();
          
          // Check if this is the first key for this container (across all tasks)
          if (newCount == 1) {
            containerCountToIncrement.incrementAndGet();
          }
        }
      }
    }

    if (containerCountToIncrement.get() > 0) {
      reconContainerMetadataManager.incrementContainerCountBy(containerCountToIncrement.get());
    }
  }

  public static boolean flushAndCommitContainerKeyInfoToDB(
      Map<ContainerKeyPrefix, Integer> localContainerKeyMap,
      Map<Long, AtomicLong> sharedContainerKeyCountMap,
      ReconContainerMetadataManager reconContainerMetadataManager) {

    try {
      // No deleted container list needed since "reprocess" only has put operations
      writeToTheDB(localContainerKeyMap, sharedContainerKeyCountMap, Collections.emptyList(), reconContainerMetadataManager);

      // Only clear localContainerKeyMap (per-task), keep sharedContainerKeyCountMap for other tasks
      localContainerKeyMap.clear();

    } catch (IOException e) {
      LOG.error("Unable to write Container Key and Container Key Count data in Recon DB.", e);
      return false;
    }
    return true;
  }

}
