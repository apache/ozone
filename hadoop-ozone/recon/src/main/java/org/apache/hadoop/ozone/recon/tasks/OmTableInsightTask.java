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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hdds.utils.db.ByteArrayCodec;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.TableCache;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.apache.hadoop.ozone.recon.tasks.util.ParallelTableIteratorOperation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to iterate over the OM DB and store the total counts of volumes,
 * buckets, keys, open keys, deleted keys, etc.
 */
public class OmTableInsightTask implements ReconOmTask {
  private static final Logger LOG =
      LoggerFactory.getLogger(OmTableInsightTask.class);

  private ReconGlobalStatsManager reconGlobalStatsManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private Map<String, OmTableHandler> tableHandlers;
  private Collection<String> tables;
  private Map<String, Long> objectCountMap;
  private Map<String, Long> unReplicatedSizeMap;
  private Map<String, Long> replicatedSizeMap;
  private final int maxKeysInMemory;
  private final int maxIterators;

  @Inject
  public OmTableInsightTask(ReconGlobalStatsManager reconGlobalStatsManager,
                             ReconOMMetadataManager reconOMMetadataManager) throws IOException {
    this.reconGlobalStatsManager = reconGlobalStatsManager;
    this.reconOMMetadataManager = reconOMMetadataManager;

    // Initialize table handlers
    tableHandlers = new HashMap<>();
    tableHandlers.put(OPEN_KEY_TABLE, new OpenKeysInsightHandler());
    tableHandlers.put(OPEN_FILE_TABLE, new OpenKeysInsightHandler());
    tableHandlers.put(DELETED_TABLE, new DeletedKeysInsightHandler());
    this.maxKeysInMemory = reconOMMetadataManager.getOzoneConfiguration().getInt(
        ReconServerConfigKeys.OZONE_RECON_TASK_REPROCESS_MAX_KEYS_IN_MEMORY,
        ReconServerConfigKeys.OZONE_RECON_TASK_REPROCESS_MAX_KEYS_IN_MEMORY_DEFAULT);
    this.maxIterators = reconOMMetadataManager.getOzoneConfiguration().getInt(
        ReconServerConfigKeys.OZONE_RECON_TASK_REPROCESS_MAX_ITERATORS,
        ReconServerConfigKeys.OZONE_RECON_TASK_REPROCESS_MAX_ITERATORS_DEFAULT);
  }

  @Override
  public ReconOmTask getStagedTask(ReconOMMetadataManager stagedOmMetadataManager, DBStore stagedReconDbStore)
      throws IOException {
    ReconGlobalStatsManager stagedGlobalStatsManager =
        reconGlobalStatsManager.getStagedReconGlobalStatsManager(stagedReconDbStore);
    return new OmTableInsightTask(stagedGlobalStatsManager, stagedOmMetadataManager);
  }

  /**
   * Initialize the OM table insight task with first time initialization of resources.
   */
  @Override
  public void init() {
    ReconOmTask.super.init();
    tables = getTaskTables();

    // Initialize maps to store count and size information
    objectCountMap = initializeCountMap();
    unReplicatedSizeMap = initializeSizeMap(false);
    replicatedSizeMap = initializeSizeMap(true);
  }

  /**
   * Reprocess all OM tables to calculate counts and sizes.
   * Handler tables (with size calculation) use sequential iteration.
   * Simple tables (count only) use parallel iteration with String keys,
   * or sequential for non-String key tables.
   *
   * @param omMetadataManager OM Metadata instance
   * @return TaskResult indicating success or failure
   */
  @Override
  public TaskResult reprocess(OMMetadataManager omMetadataManager) {
    LOG.info("Starting RocksDB Reprocess for {}", getTaskName());
    long startTime = Time.monotonicNow();

    init();
    for (String tableName : tables) {
      Table<?, ?> table = omMetadataManager.getTable(tableName);

      try {
        if (tableHandlers.containsKey(tableName)) {
          Table<String, ?> stringTable = (Table<String, ?>) table;
          try (TableIterator<String, ? extends Table.KeyValue<String, ?>> iterator = stringTable.iterator()) {
            Triple<Long, Long, Long> details = tableHandlers.get(tableName).getTableSizeAndCount(iterator);
            objectCountMap.put(getTableCountKeyFromTable(tableName), details.getLeft());
            unReplicatedSizeMap.put(getUnReplicatedSizeKeyFromTable(tableName), details.getMiddle());
            replicatedSizeMap.put(getReplicatedSizeKeyFromTable(tableName), details.getRight());
          }
        } else {
          if (usesNonStringKeys(tableName)) {
          } else {
            processTableInParallel(tableName, table, omMetadataManager);
          }
        }
      } catch (Exception ioEx) {
        LOG.error("Unable to populate Table Count in Recon DB.", ioEx);
        return buildTaskResult(false);
      }
    }
    // Write the data to the DB
    if (!objectCountMap.isEmpty()) {
      writeDataToDB(objectCountMap);
    }
    if (!unReplicatedSizeMap.isEmpty()) {
      writeDataToDB(unReplicatedSizeMap);
    }
    if (!replicatedSizeMap.isEmpty()) {
      writeDataToDB(replicatedSizeMap);
    }
    long endTime = Time.monotonicNow();
    long durationMs = endTime - startTime;
    double durationSec = durationMs / 1000.0;
    int handlerTables = tableHandlers.size();  // 3 handler tables (with size calculation)
    int simpleTables = tables.size() - handlerTables;  // Simple count-only tables

    LOG.info("{}: RocksDB reprocess completed in {} ms ({} sec) - " +
        "Total tables: {}, Handler tables (with size): {}, Simple tables (count only): {}",
        getTaskName(), durationMs, String.format("%.2f", durationSec), 
        tables.size(), handlerTables, simpleTables);
    return buildTaskResult(true);
  }

  /**
   * Check if table uses non-String keys (e.g., OzoneTokenIdentifier).
   * These tables cannot use StringCodec and must be processed sequentially.
   */
  private boolean usesNonStringKeys(String tableName) {
    return tableName.equals("dTokenTable") || tableName.equals("s3SecretTable");
  }

  /**
   * Process table sequentially using raw iterator (no type assumptions).
   * Used for tables with non-String keys or as fallback.
   */
  private void processTableSequentially(String tableName, Table<?, ?> table) throws IOException {
    LOG.info("{}: Processing table {} sequentially (non-String keys)", getTaskName(), tableName);
    
    Table<String, ?> stringTable = (Table<String, ?>) table;
    try (TableIterator<String, ? extends Table.KeyValue<String, ?>> iterator = stringTable.iterator()) {
      long count = Iterators.size(iterator);
      objectCountMap.put(getTableCountKeyFromTable(tableName), count);
    }
  }

  /**
   * Process table in parallel using multiple iterators and workers.
   * Only for tables with String keys.
   */
  private void processTableInParallel(String tableName, Table<?, ?> table, 
                                      OMMetadataManager omMetadataManager) throws Exception {
    int workerCount = 2;  // Only 2 workers needed for simple counting
    long loggingThreshold = calculateLoggingThreshold(table);
    
    LOG.info("{}: Processing simple table {} with parallel iteration ({} iterators, {} workers)",
        getTaskName(), tableName, maxIterators, workerCount);
    
    AtomicLong count = new AtomicLong(0);
    
    // Cast to String keys for parallel processing
    Table<String, Object> genericTable = (Table<String, Object>) table;

    try (ParallelTableIteratorOperation<String, byte[]> parallelIter = new ParallelTableIteratorOperation<>(
        omMetadataManager, omMetadataManager.getStore()
        .getTable(tableName, StringCodec.get(), ByteArrayCodec.get(), TableCache.CacheType.NO_CACHE), StringCodec.get(),
        maxIterators, workerCount, maxKeysInMemory, loggingThreshold)) {
      
      parallelIter.performTaskOnTableVals(getTaskName(), null, null, kv -> {
        if (kv != null) {
          count.incrementAndGet();
        }
        return null;
      });
    }
    
    long finalCount = count.get();
    LOG.info("{}: Table {} counted {} entries", getTaskName(), tableName, finalCount);
    objectCountMap.put(getTableCountKeyFromTable(tableName), finalCount);
  }

  /**
   * Calculate logging threshold based on table size.
   * Logs progress every 1% of total keys, minimum 1.
   */
  private long calculateLoggingThreshold(Table<?, ?> table) {
    try {
      long estimatedKeys = table.getEstimatedKeyCount();
      return Math.max(estimatedKeys / 100, 1);
    } catch (IOException e) {
      LOG.debug("Could not estimate key count, using default logging threshold");
      return 100000;  // Default: log every 100K keys
    }
  }

  @Override
  public String getTaskName() {
    return "OmTableInsightTask";
  }

  public Collection<String> getTaskTables() {
    return new ArrayList<>(reconOMMetadataManager.listTableNames());
  }

  /**
   * Read the update events and update the count and sizes of respective object
   * (volume, bucket, key etc.) based on the action (put or delete).
   *
   * @param events            Update events - PUT, DELETE and UPDATE.
   * @param subTaskSeekPosMap
   * @return Pair
   */
  @Override
  public TaskResult process(OMUpdateEventBatch events,
                            Map<String, Integer> subTaskSeekPosMap) {
    // Initialize tables if not already initialized
    if (tables == null || tables.isEmpty()) {
      init();
    }

    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();

    String tableName;
    OMDBUpdateEvent<String, Object> omdbUpdateEvent;
    // Process each update event
    long startTime = Time.monotonicNow();
    while (eventIterator.hasNext()) {
      omdbUpdateEvent = eventIterator.next();
      tableName = omdbUpdateEvent.getTable();
      if (!tables.contains(tableName)) {
        continue;
      }
      try {
        switch (omdbUpdateEvent.getAction()) {
        case PUT:
          handlePutEvent(omdbUpdateEvent, tableName);
          break;

        case DELETE:
          handleDeleteEvent(omdbUpdateEvent, tableName);
          break;

        case UPDATE:
          handleUpdateEvent(omdbUpdateEvent, tableName);
          break;

        default:
          LOG.trace("Skipping DB update event : Table: {}, Action: {}",
              tableName, omdbUpdateEvent.getAction());
        }
      } catch (Exception e) {
        LOG.error(
            "Unexpected exception while processing the table {}, Action: {}",
            tableName, omdbUpdateEvent.getAction(), e);
        return buildTaskResult(false);
      }
    }
    // Write the updated count and size information to the database
    if (!objectCountMap.isEmpty()) {
      writeDataToDB(objectCountMap);
    }
    if (!unReplicatedSizeMap.isEmpty()) {
      writeDataToDB(unReplicatedSizeMap);
    }
    if (!replicatedSizeMap.isEmpty()) {
      writeDataToDB(replicatedSizeMap);
    }
    LOG.debug("{} successfully processed in {} milliseconds",
        getTaskName(), (Time.monotonicNow() - startTime));
    return buildTaskResult(true);
  }

  private void handlePutEvent(OMDBUpdateEvent<String, Object> event,
                              String tableName) {
    OmTableHandler tableHandler = tableHandlers.get(tableName);
    if (event.getValue() != null) {
      if (tableHandler != null) {
        tableHandler.handlePutEvent(event, tableName, objectCountMap,
            unReplicatedSizeMap, replicatedSizeMap);
      } else {
        String countKey = getTableCountKeyFromTable(tableName);
        objectCountMap.computeIfPresent(countKey, (k, count) -> count + 1L);
      }
    }
  }

  private void handleDeleteEvent(OMDBUpdateEvent<String, Object> event,
                                 String tableName) {
    OmTableHandler tableHandler = tableHandlers.get(tableName);
    if (event.getValue() != null) {
      if (tableHandler != null) {
        tableHandler.handleDeleteEvent(event, tableName, objectCountMap,
            unReplicatedSizeMap, replicatedSizeMap);
      } else {
        objectCountMap.computeIfPresent(getTableCountKeyFromTable(tableName),
            (k, count) -> count > 0 ? count - 1L : 0L);
      }
    }
  }

  private void handleUpdateEvent(OMDBUpdateEvent<String, Object> event,
                                 String tableName) {

    OmTableHandler tableHandler = tableHandlers.get(tableName);
    if (event.getValue() != null) {
      if (tableHandler != null) {
        // Handle update for only size related tables
        tableHandler.handleUpdateEvent(event, tableName, objectCountMap,
            unReplicatedSizeMap, replicatedSizeMap);
      }
    }
  }

  /**
   * Write the updated count and size information to the database.
   *
   * @param dataMap Map containing the updated count and size information.
   */
  private void writeDataToDB(Map<String, Long> dataMap) {
    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      for (Entry<String, Long> entry : dataMap.entrySet()) {
        String key = entry.getKey();
        Long value = entry.getValue();
        GlobalStatsValue globalStatsValue = new GlobalStatsValue(value);
        reconGlobalStatsManager.batchStoreGlobalStats(rdbBatchOperation, key, globalStatsValue);
      }
      reconGlobalStatsManager.commitBatchOperation(rdbBatchOperation);
    } catch (IOException e) {
      LOG.error("Failed to write data to RocksDB GlobalStats table", e);
    }
  }

  /**
   * Initializes and returns a count map with the counts for the tables.
   *
   * @return The count map containing the counts for each table.
   */
  public HashMap<String, Long> initializeCountMap() {
    HashMap<String, Long> objCountMap = new HashMap<>(tables.size());
    for (String tableName : tables) {
      String key = getTableCountKeyFromTable(tableName);
      objCountMap.put(key, getValueForKey(key));
    }
    return objCountMap;
  }

  /**
   * Initializes a size map with the replicated or unreplicated sizes for the
   * tables to calculate size.
   *
   * @return The size map containing the size counts for each table.
   */
  public HashMap<String, Long> initializeSizeMap(boolean replicated) {
    String tableName;
    OmTableHandler tableHandler;
    HashMap<String, Long> sizeCountMap = new HashMap<>();
    for (Map.Entry<String, OmTableHandler> entry : tableHandlers.entrySet()) {
      tableName = entry.getKey();
      tableHandler = entry.getValue();
      String key =
          replicated ? tableHandler.getReplicatedSizeKeyFromTable(tableName) :
          tableHandler.getUnReplicatedSizeKeyFromTable(tableName);
      sizeCountMap.put(key, getValueForKey(key));
    }
    return sizeCountMap;
  }

  public static String getTableCountKeyFromTable(String tableName) {
    return tableName + "Count";
  }

  public static String getReplicatedSizeKeyFromTable(String tableName) {
    return tableName + "ReplicatedDataSize";
  }

  public static String getUnReplicatedSizeKeyFromTable(String tableName) {
    return tableName + "UnReplicatedDataSize";
  }

  /**
   * Get the value stored for the given key from the Global Stats table.
   * Return 0 if the record is not found.
   *
   * @param key Key in the Global Stats table
   * @return The value associated with the key
   */
  private long getValueForKey(String key) {
    try {
      GlobalStatsValue globalStatsValue = reconGlobalStatsManager.getGlobalStatsValue(key);
      return (globalStatsValue == null) ? 0L : globalStatsValue.getValue();
    } catch (IOException e) {
      LOG.error("Failed to get value for key {} from RocksDB GlobalStats table", key, e);
      return 0L;
    }
  }

  @VisibleForTesting
  public void setTables(Collection<String> tables) {
    this.tables = tables;
  }

  @VisibleForTesting
  public void setObjectCountMap(HashMap<String, Long> objectCountMap) {
    this.objectCountMap = objectCountMap;
  }

  @VisibleForTesting
  public void setUnReplicatedSizeMap(HashMap<String, Long> unReplicatedSizeMap) {
    this.unReplicatedSizeMap = unReplicatedSizeMap;
  }

  @VisibleForTesting
  public void setReplicatedSizeMap(HashMap<String, Long> replicatedSizeMap) {
    this.replicatedSizeMap = replicatedSizeMap;
  }
}


