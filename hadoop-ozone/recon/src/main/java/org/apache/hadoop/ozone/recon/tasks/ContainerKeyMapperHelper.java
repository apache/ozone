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
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
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

  public static boolean reprocess(OMMetadataManager omMetadataManager,
                                                ReconContainerMetadataManager reconContainerMetadataManager,
                                                BucketLayout bucketLayout,
                                                String taskName,
                                                long containerKeyFlushToDBMaxThreshold) {
    long omKeyCount = 0;
    Map<ContainerKeyPrefix, Integer> containerKeyMap = new HashMap<>();
    Map<Long, Long> containerKeyCountMap = new HashMap<>();

    try {
      LOG.debug("Starting a 'reprocess' run for {}.", taskName);
      Instant start = Instant.now();

      // Ensure the tables are truncated only once
      truncateTablesIfNeeded(reconContainerMetadataManager, taskName);

      // Get the appropriate table based on BucketLayout
      Table<String, OmKeyInfo> omKeyInfoTable = omMetadataManager.getKeyTable(bucketLayout);

      // Iterate through the table and process keys
      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> keyIter = omKeyInfoTable.iterator()) {
        while (keyIter.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
          handleKeyReprocess(kv.getKey(), kv.getValue(), containerKeyMap, containerKeyCountMap,
              reconContainerMetadataManager);
          omKeyCount++;

          // Check and flush data if it reaches the batch threshold
          if (!checkAndCallFlushToDB(containerKeyMap, containerKeyFlushToDBMaxThreshold,
              reconContainerMetadataManager)) {
            LOG.error("Failed to flush container key data for {}", taskName);
            return false;
          }
        }
      }

      // Final flush and commit
      if (!flushAndCommitContainerKeyInfoToDB(containerKeyMap, containerKeyCountMap, reconContainerMetadataManager)) {
        LOG.error("Failed to flush Container Key data to DB for {}", taskName);
        return false;
      }

      Instant end = Instant.now();
      long durationMillis = Duration.between(start, end).toMillis();
      double durationSeconds = (double) durationMillis / 1000.0;
      LOG.debug("Completed 'reprocess' for {}. Processed {} keys in {} ms ({} seconds).",
          taskName, omKeyCount, durationMillis, durationSeconds);

    } catch (IOException ioEx) {
      LOG.error("Error populating Container Key data for {} in Recon DB.", taskName, ioEx);
      return false;
    }
    return true;
  }

  private static boolean checkAndCallFlushToDB(Map<ContainerKeyPrefix, Integer> containerKeyMap,
                                               long containerKeyFlushToDBMaxThreshold,
                                               ReconContainerMetadataManager reconContainerMetadataManager) {
    if (containerKeyMap.size() >= containerKeyFlushToDBMaxThreshold) {
      return flushAndCommitContainerKeyInfoToDB(containerKeyMap, Collections.emptyMap(), reconContainerMetadataManager);
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
    // (HDDS-8580) containerKeyMap map is allowed to be used
    // in "process" without batching since the maximum number of keys
    // is bounded by delta limit configurations

    // (container, key) -> count
    Map<ContainerKeyPrefix, Integer> containerKeyMap = new HashMap<>();
    // containerId -> key count
    Map<Long, Long> containerKeyCountMap = new HashMap<>();
    // List of the deleted (container, key) pair's
    List<ContainerKeyPrefix> deletedKeyCountList = new ArrayList<>();
    long startTime = System.currentTimeMillis();

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
          handlePutOMKeyEvent(updatedKey, updatedKeyValue, containerKeyMap,
              containerKeyCountMap, deletedKeyCountList, reconContainerMetadataManager);
          break;

        case DELETE:
          handleDeleteOMKeyEvent(updatedKey, containerKeyMap,
              containerKeyCountMap, deletedKeyCountList, reconContainerMetadataManager);
          break;

        case UPDATE:
          if (omdbUpdateEvent.getOldValue() != null) {
            handleDeleteOMKeyEvent(
                omdbUpdateEvent.getOldValue().getKeyName(), containerKeyMap,
                containerKeyCountMap, deletedKeyCountList, reconContainerMetadataManager);
          } else {
            LOG.warn("Update event does not have the old Key Info for {}.", updatedKey);
          }
          handlePutOMKeyEvent(updatedKey, updatedKeyValue, containerKeyMap,
              containerKeyCountMap, deletedKeyCountList, reconContainerMetadataManager);
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
      writeToTheDB(containerKeyMap, containerKeyCountMap, deletedKeyCountList, reconContainerMetadataManager);
    } catch (IOException e) {
      LOG.error("Unable to write Container Key Prefix data in Recon DB.", e);
      return false;
    }
    LOG.debug("{} successfully processed {} OM DB update event(s) in {} milliseconds.",
        taskName, eventCount, (System.currentTimeMillis() - startTime));
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

  private static void writeToTheDB(Map<ContainerKeyPrefix, Integer> containerKeyMap,
                                   Map<Long, Long> containerKeyCountMap,
                                   List<ContainerKeyPrefix> deletedContainerKeyList,
                                   ReconContainerMetadataManager reconContainerMetadataManager)
      throws IOException {

    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {

      // Write container key mappings
      containerKeyMap.keySet().forEach((ContainerKeyPrefix key) -> {
        try {
          reconContainerMetadataManager.batchStoreContainerKeyMapping(
              rdbBatchOperation, key, containerKeyMap.get(key));
        } catch (IOException e) {
          LOG.error("Unable to write Container Key Prefix data in Recon DB.", e);
        }
      });

      // Write container key count mappings
      containerKeyCountMap.keySet().forEach((Long key) -> {
        try {
          reconContainerMetadataManager.batchStoreContainerKeyCounts(
              rdbBatchOperation, key, containerKeyCountMap.get(key));
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
   * @param containerKeyMap we keep the added containerKeys in this map
   *                        to allow incremental batching to containerKeyTable
   * @param containerKeyCountMap we keep the containerKey counts in this map
   *                             to allow batching to containerKeyCountTable
   *                             after reprocessing is done
   * @param reconContainerMetadataManager Recon metadata manager instance
   * @throws IOException if unable to write to recon DB.
   */
  public static void handleKeyReprocess(String key,
                                        OmKeyInfo omKeyInfo,
                                        Map<ContainerKeyPrefix, Integer> containerKeyMap,
                                        Map<Long, Long> containerKeyCountMap,
                                        ReconContainerMetadataManager reconContainerMetadataManager)
      throws IOException {

    long containerCountToIncrement = 0;

    for (OmKeyLocationInfoGroup omKeyLocationInfoGroup : omKeyInfo.getKeyLocationVersions()) {
      long keyVersion = omKeyLocationInfoGroup.getVersion();
      for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfoGroup.getLocationList()) {
        long containerId = omKeyLocationInfo.getContainerID();
        ContainerKeyPrefix containerKeyPrefix = ContainerKeyPrefix.get(containerId, key, keyVersion);

        if (reconContainerMetadataManager.getCountForContainerKeyPrefix(containerKeyPrefix) == 0
            && !containerKeyMap.containsKey(containerKeyPrefix)) {
          // Save on writes. No need to save same container-key prefix mapping again.
          containerKeyMap.put(containerKeyPrefix, 1);

          // Check if container already exists; if not, increment the count
          if (!reconContainerMetadataManager.doesContainerExists(containerId)
              && !containerKeyCountMap.containsKey(containerId)) {
            containerCountToIncrement++;
          }

          // Update the count of keys for the given containerID
          long keyCount = containerKeyCountMap.getOrDefault(containerId,
              reconContainerMetadataManager.getKeyCountForContainer(containerId));

          containerKeyCountMap.put(containerId, keyCount + 1);
        }
      }
    }

    if (containerCountToIncrement > 0) {
      reconContainerMetadataManager.incrementContainerCountBy(containerCountToIncrement);
    }
  }

  public static boolean flushAndCommitContainerKeyInfoToDB(
      Map<ContainerKeyPrefix, Integer> containerKeyMap,
      Map<Long, Long> containerKeyCountMap,
      ReconContainerMetadataManager reconContainerMetadataManager) {

    try {
      // No deleted container list needed since "reprocess" only has put operations
      writeToTheDB(containerKeyMap, containerKeyCountMap, Collections.emptyList(), reconContainerMetadataManager);

      // Clear in-memory maps after successful commit
      containerKeyMap.clear();
      containerKeyCountMap.clear();

    } catch (IOException e) {
      LOG.error("Unable to write Container Key and Container Key Count data in Recon DB.", e);
      return false;
    }
    return true;
  }

}
