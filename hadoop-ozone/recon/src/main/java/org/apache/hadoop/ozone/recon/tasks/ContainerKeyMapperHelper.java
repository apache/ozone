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

import com.google.common.annotations.VisibleForTesting;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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

  // Single lock to guard all initialization operations (table truncation + map clearing)
  private static final Object INITIALIZATION_LOCK = new Object();

  /**
   * Reference counter to track how many tasks are actively using the shared map.
   * Initialized to 2 (FSO + OBS tasks) during initialization.
   * Each task decrements on completion. Last task (count reaches 0) clears the shared map.
   */
  private static final AtomicInteger ACTIVE_TASK_COUNT = new AtomicInteger(0);

  /**
   * SHARED across all tasks (FSO + OBS) for cross-task synchronization.
   * Maps: ContainerId -> AtomicLong (key count in that container)
   * Purpose: Prevents data corruption when FSO and OBS tasks run concurrently
   * and both write to the same container IDs. Both tasks accumulate into this
   * single shared map, ensuring final DB write contains complete totals.
   */
  private static final Map<Long, AtomicLong> SHARED_CONTAINER_KEY_COUNT_MAP = new ConcurrentHashMap<>();

  /**
   * Performs one-time initialization for Container Key Mapper tasks.
   * This includes:
   * 1. Truncating container key tables in DB
   * 2. Clearing the shared container count map
   * 
   * This method is called by both FSO and OBS tasks at the start of reprocess.
   * Only the first task to call this will perform initialization.
   * 
   * @param reconContainerMetadataManager The metadata manager for DB operations
   * @param taskName Name of the task calling this method (for logging)
   * @throws RuntimeException if initialization fails
   */
  private static void initializeContainerKeyMapperIfNeeded(
      ReconContainerMetadataManager reconContainerMetadataManager,
      String taskName) {
    
    synchronized (INITIALIZATION_LOCK) {
      ACTIVE_TASK_COUNT.incrementAndGet();
      // Check if already initialized by another task
      if (ReconConstants.CONTAINER_KEY_MAPPER_INITIALIZED.compareAndSet(false, true)) {
        try {
          // Step 1: Truncate tables
          reconContainerMetadataManager.reinitWithNewContainerDataFromOm(Collections.emptyMap());
          
          // Step 2: Clear shared map
          SHARED_CONTAINER_KEY_COUNT_MAP.clear();

        } catch (Exception e) {
          // CRITICAL: Decrement counter and reset flag so another task can retry
          ACTIVE_TASK_COUNT.decrementAndGet();
          ReconConstants.CONTAINER_KEY_MAPPER_INITIALIZED.set(false);
          LOG.error("{}: Container Key Mapper initialization failed. Resetting flag for retry.", taskName, e);
          throw new RuntimeException("Container Key Mapper initialization failed", e);
        }
      } else {
        LOG.debug("{}: Container Key Mapper already initialized by another task", taskName);
      }
    }
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public static boolean reprocess(OMMetadataManager omMetadataManager,
                                                ReconContainerMetadataManager reconContainerMetadataManager,
                                                BucketLayout bucketLayout,
                                                String taskName,
                                                long containerKeyFlushToDBMaxThreshold,
                                                int maxIterators,
                                                int maxWorkers,
                                                int maxKeysInMemory) {
    try {
      LOG.info("{}: Starting reprocess for bucket layout {}", taskName, bucketLayout);
      Instant start = Instant.now();

      // Perform one-time initialization (truncate tables + clear shared map)
      initializeContainerKeyMapperIfNeeded(reconContainerMetadataManager, taskName);

      Table<String, OmKeyInfo> omKeyInfoTable = omMetadataManager.getKeyTable(bucketLayout);

      // Divide threshold by worker count so each worker flushes independently
      final long perWorkerThreshold = Math.max(1, containerKeyFlushToDBMaxThreshold / maxWorkers);
      
      // Map thread IDs to worker-specific local maps for lockless updates
      Map<Long, Map<ContainerKeyPrefix, Integer>> allLocalMaps = new ConcurrentHashMap<>();
      
      Function<Table.KeyValue<String, OmKeyInfo>, Void> kvOperation = kv -> {
        try {
          // Get or create this worker's private local map using thread ID
          Map<ContainerKeyPrefix, Integer> containerKeyPrefixMap = allLocalMaps.computeIfAbsent(
              Thread.currentThread().getId(), k -> new ConcurrentHashMap<>());
          
          handleKeyReprocess(kv.getKey(), kv.getValue(), containerKeyPrefixMap, SHARED_CONTAINER_KEY_COUNT_MAP,
              reconContainerMetadataManager);

          // Flush this worker's map when it reaches threshold
          if (containerKeyPrefixMap.size() >= perWorkerThreshold) {
            if (!flushAndCommitContainerKeyInfoToDB(containerKeyPrefixMap, Collections.emptyMap(),
                reconContainerMetadataManager)) {
              throw new UncheckedIOException(new IOException("Unable to flush containerKey information to the DB"));
            }
          }
          return null;
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      };
      
      try (ParallelTableIteratorOperation<String, OmKeyInfo> keyIter =
               new ParallelTableIteratorOperation<>(omMetadataManager, omKeyInfoTable,
                   StringCodec.get(), maxIterators, perWorkerThreshold)) {
        keyIter.performTaskOnTableVals(taskName, null, null, kvOperation);
      }

      // Final flush: Write remaining entries from all worker local maps to DB
      for (Map<ContainerKeyPrefix, Integer> containerKeyPrefixMap : allLocalMaps.values()) {
        if (!containerKeyPrefixMap.isEmpty()) {
          if (!flushAndCommitContainerKeyInfoToDB(containerKeyPrefixMap, Collections.emptyMap(),
              reconContainerMetadataManager)) {
            LOG.error("Failed to flush worker local map for {}", taskName);
            return false;
          }
        }
      }

      // Decrement active task counter
      int remainingTasks = ACTIVE_TASK_COUNT.decrementAndGet();
      LOG.info("{}: Task completed. Remaining active tasks: {}", taskName, remainingTasks);

      // Only last task flushes shared map and writes container count
      if (remainingTasks == 0) {
        synchronized (INITIALIZATION_LOCK) {
          // Capture total container count from shared map
          long totalContainers = SHARED_CONTAINER_KEY_COUNT_MAP.size();

          // Flush shared container count map
          if (!flushAndCommitContainerKeyInfoToDB(Collections.emptyMap(), SHARED_CONTAINER_KEY_COUNT_MAP, 
              reconContainerMetadataManager)) {
            LOG.error("Failed to flush shared container count map for {}", taskName);
            return false;
          }

          // Write total container count once at the end
          if (totalContainers > 0) {
            reconContainerMetadataManager.incrementContainerCountBy(totalContainers);
          }

          // Clean up shared resources
          SHARED_CONTAINER_KEY_COUNT_MAP.clear();
          ReconConstants.CONTAINER_KEY_MAPPER_INITIALIZED.set(false);
          LOG.info("{}: Last task completed. Cleared shared map and reset initialization flag.", taskName);
        }
      }

      Instant end = Instant.now();
      long durationMillis = Duration.between(start, end).toMillis();
      double durationSeconds = (double) durationMillis / 1000.0;

      LOG.info("{}: Reprocess completed in {} sec", taskName, durationSeconds);
    } catch (Exception ex) {
      LOG.error("Error populating Container Key data for {} in Recon DB.", taskName, ex);
      return false;
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
      writeToTheDB(localContainerKeyMap, localContainerKeyCountMapAtomic, deletedKeyCountList,
          reconContainerMetadataManager);
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

    for (OmKeyLocationInfoGroup omKeyLocationInfoGroup : omKeyInfo.getKeyLocationVersions()) {
      long keyVersion = omKeyLocationInfoGroup.getVersion();
      for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfoGroup.getLocationList()) {
        long containerId = omKeyLocationInfo.getContainerID();
        ContainerKeyPrefix containerKeyPrefix = ContainerKeyPrefix.get(containerId, key, keyVersion);

        // During reprocess, tables are empty so skip DB lookup - just check in-memory map
        if (!localContainerKeyMap.containsKey(containerKeyPrefix)) {
          // Save on writes. No need to save same container-key prefix mapping again.
          localContainerKeyMap.put(containerKeyPrefix, 1);

          // Thread-safe increment using computeIfAbsent (cross-task safe: FSO + OBS)
          sharedContainerKeyCountMap.computeIfAbsent(containerId, k -> new AtomicLong(0))
              .incrementAndGet();
        }
      }
    }
    // Container count will be written once at the end of reprocess, not here (Derby optimization)
  }

  public static boolean flushAndCommitContainerKeyInfoToDB(
      Map<ContainerKeyPrefix, Integer> localContainerKeyMap,
      Map<Long, AtomicLong> sharedContainerKeyCountMap,
      ReconContainerMetadataManager reconContainerMetadataManager) {

    try {
      // No deleted container list needed since "reprocess" only has put operations
      writeToTheDB(localContainerKeyMap, sharedContainerKeyCountMap, Collections.emptyList(),
          reconContainerMetadataManager);

      // Only clear localContainerKeyMap (per-task), keep sharedContainerKeyCountMap for other tasks
      localContainerKeyMap.clear();

    } catch (IOException e) {
      LOG.error("Unable to write Container Key and Container Key Count data in Recon DB.", e);
      return false;
    }
    return true;
  }

  /**
   * Clears the shared container count map and resets initialization flag.
   * This method should be called by tests to ensure clean state between test runs.
   */
  @VisibleForTesting
  public static void clearSharedContainerCountMap() {
    synchronized (INITIALIZATION_LOCK) {
      SHARED_CONTAINER_KEY_COUNT_MAP.clear();
      ReconConstants.CONTAINER_KEY_MAPPER_INITIALIZED.set(false);
      ACTIVE_TASK_COUNT.set(0);
      LOG.debug("Cleared shared container count map, reset initialization flag, and reset task counter for tests");
    }
  }

}
