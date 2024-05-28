/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.tasks;

import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.hadoop.ozone.recon.schema.tables.pojos.GlobalStats;
import org.jooq.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.using;
import static org.jooq.impl.DSL.currentTimestamp;

/**
 * Class to iterate over the OM DB and store the total counts of volumes,
 * buckets, keys, open keys, deleted keys, etc.
 */
public class OmTableInsightTask implements ReconOmTask {
  private static final Logger LOG =
      LoggerFactory.getLogger(OmTableInsightTask.class);

  private GlobalStatsDao globalStatsDao;
  private Configuration sqlConfiguration;
  private ReconOMMetadataManager reconOMMetadataManager;
  private Map<String, OmTableHandler> tableHandlers;

  @Inject
  public OmTableInsightTask(GlobalStatsDao globalStatsDao,
                             Configuration sqlConfiguration,
                             ReconOMMetadataManager reconOMMetadataManager) {
    this.globalStatsDao = globalStatsDao;
    this.sqlConfiguration = sqlConfiguration;
    this.reconOMMetadataManager = reconOMMetadataManager;

    // Initialize table handlers
    tableHandlers = new HashMap<>();
    tableHandlers.put(OPEN_KEY_TABLE, new OpenKeysInsightHandler());
    tableHandlers.put(OPEN_FILE_TABLE, new OpenKeysInsightHandler());
    tableHandlers.put(DELETED_TABLE, new DeletedKeysInsightHandler());
  }

  /**
   * Iterates the rows of each table in the OM snapshot DB and calculates the
   * counts and sizes for table data.
   *
   * For tables that require data size calculation
   * (as returned by getTablesToCalculateSize), both the number of
   * records (count) and total data size of the records are calculated.
   * For all other tables, only the count of records is calculated.
   *
   * @param omMetadataManager OM Metadata instance.
   * @return Pair
   */
  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    HashMap<String, Long> objectCountMap = initializeCountMap();
    HashMap<String, Long> unReplicatedSizeMap = initializeSizeMap(false);
    HashMap<String, Long> replicatedSizeMap = initializeSizeMap(true);

    for (String tableName : getTaskTables()) {
      Table table = omMetadataManager.getTable(tableName);
      if (table == null) {
        LOG.error("Table " + tableName + " not found in OM Metadata.");
        return new ImmutablePair<>(getTaskName(), false);
      }

      try (TableIterator<String, ? extends Table.KeyValue<String, ?>> iterator
               = table.iterator()) {
        if (tableHandlers.containsKey(tableName)) {
          Triple<Long, Long, Long> details =
              tableHandlers.get(tableName).getTableSizeAndCount(iterator);
          objectCountMap.put(getTableCountKeyFromTable(tableName),
              details.getLeft());
          unReplicatedSizeMap.put(
              getUnReplicatedSizeKeyFromTable(tableName), details.getMiddle());
          replicatedSizeMap.put(getReplicatedSizeKeyFromTable(tableName),
              details.getRight());
        } else {
          long count = Iterators.size(iterator);
          objectCountMap.put(getTableCountKeyFromTable(tableName), count);
        }
      } catch (IOException ioEx) {
        LOG.error("Unable to populate Table Count in Recon DB.", ioEx);
        return new ImmutablePair<>(getTaskName(), false);
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

    LOG.debug("Completed a 'reprocess' run of OmTableInsightTask.");
    return new ImmutablePair<>(getTaskName(), true);
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
   * @param events Update events - PUT, DELETE and UPDATE.
   * @return Pair
   */
  @Override
  public Pair<String, Boolean> process(OMUpdateEventBatch events) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    // Initialize maps to store count and size information
    HashMap<String, Long> objectCountMap = initializeCountMap();
    HashMap<String, Long> unReplicatedSizeMap = initializeSizeMap(false);
    HashMap<String, Long> replicatedSizeMap = initializeSizeMap(true);
    final Collection<String> taskTables = getTaskTables();

    // Process each update event
    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, Object> omdbUpdateEvent = eventIterator.next();
      String tableName = omdbUpdateEvent.getTable();
      if (!taskTables.contains(tableName)) {
        continue;
      }
      try {
        switch (omdbUpdateEvent.getAction()) {
        case PUT:
          handlePutEvent(omdbUpdateEvent, tableName, objectCountMap,
              unReplicatedSizeMap, replicatedSizeMap);
          break;

        case DELETE:
          handleDeleteEvent(omdbUpdateEvent, tableName, objectCountMap,
              unReplicatedSizeMap, replicatedSizeMap);
          break;

        case UPDATE:
          handleUpdateEvent(omdbUpdateEvent, tableName, objectCountMap,
              unReplicatedSizeMap, replicatedSizeMap);
          break;

        default:
          LOG.trace("Skipping DB update event : Table: {}, Action: {}",
              tableName, omdbUpdateEvent.getAction());
        }
      } catch (Exception e) {
        LOG.error(
            "Unexpected exception while processing the table {}, Action: {}",
            tableName, omdbUpdateEvent.getAction(), e);
        return new ImmutablePair<>(getTaskName(), false);
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
    LOG.debug("Completed a 'process' run of OmTableInsightTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  private void handlePutEvent(OMDBUpdateEvent<String, Object> event,
                              String tableName,
                              HashMap<String, Long> objectCountMap,
                              HashMap<String, Long> unReplicatedSizeMap,
                              HashMap<String, Long> replicatedSizeMap)
      throws IOException {
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
                                 String tableName,
                                 HashMap<String, Long> objectCountMap,
                                 HashMap<String, Long> unReplicatedSizeMap,
                                 HashMap<String, Long> replicatedSizeMap)
      throws IOException {
    OmTableHandler tableHandler = tableHandlers.get(tableName);
    if (event.getValue() != null) {
      if (tableHandler != null) {
        tableHandler.handleDeleteEvent(event, tableName, objectCountMap,
            unReplicatedSizeMap, replicatedSizeMap);
      } else {
        String countKey = getTableCountKeyFromTable(tableName);
        objectCountMap.computeIfPresent(countKey,
            (k, count) -> count > 0 ? count - 1L : 0L);
      }
    }
  }


  private void handleUpdateEvent(OMDBUpdateEvent<String, Object> event,
                                 String tableName,
                                 HashMap<String, Long> objectCountMap,
                                 HashMap<String, Long> unReplicatedSizeMap,
                                 HashMap<String, Long> replicatedSizeMap) {

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
    List<GlobalStats> insertGlobalStats = new ArrayList<>();
    List<GlobalStats> updateGlobalStats = new ArrayList<>();

    for (Entry<String, Long> entry : dataMap.entrySet()) {
      Timestamp now =
          using(sqlConfiguration).fetchValue(select(currentTimestamp()));
      GlobalStats record = globalStatsDao.fetchOneByKey(entry.getKey());
      GlobalStats newRecord
          = new GlobalStats(entry.getKey(), entry.getValue(), now);

      // Insert a new record for key if it does not exist
      if (record == null) {
        insertGlobalStats.add(newRecord);
      } else {
        updateGlobalStats.add(newRecord);
      }
    }

    globalStatsDao.insert(insertGlobalStats);
    globalStatsDao.update(updateGlobalStats);
  }

  /**
   * Initializes and returns a count map with the counts for the tables.
   *
   * @return The count map containing the counts for each table.
   */
  private HashMap<String, Long> initializeCountMap() {
    Collection<String> tables = getTaskTables();
    HashMap<String, Long> objectCountMap = new HashMap<>(tables.size());
    for (String tableName : tables) {
      String key = getTableCountKeyFromTable(tableName);
      objectCountMap.put(key, getValueForKey(key));
    }
    return objectCountMap;
  }

  /**
   * Initializes a size map with the replicated or unreplicated sizes for the
   * tables to calculate size.
   *
   * @return The size map containing the size counts for each table.
   */
  private HashMap<String, Long> initializeSizeMap(boolean replicated) {
    HashMap<String, Long> sizeCountMap = new HashMap<>();
    for (Map.Entry<String, OmTableHandler> entry : tableHandlers.entrySet()) {
      String tableName = entry.getKey();
      OmTableHandler tableHandler = entry.getValue();
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
    GlobalStats record = globalStatsDao.fetchOneByKey(key);

    return (record == null) ? 0L : record.getValue();
  }

}


