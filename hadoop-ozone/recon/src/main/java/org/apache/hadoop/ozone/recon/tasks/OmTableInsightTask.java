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
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.hadoop.ozone.recon.schema.tables.pojos.GlobalStats;
import org.jooq.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import java.util.Map.Entry;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.jooq.impl.DSL.currentTimestamp;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.using;

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

  @Inject
  public OmTableInsightTask(GlobalStatsDao globalStatsDao,
                            Configuration sqlConfiguration,
                            ReconOMMetadataManager reconOMMetadataManager) {
    this.globalStatsDao = globalStatsDao;
    this.sqlConfiguration = sqlConfiguration;
    this.reconOMMetadataManager = reconOMMetadataManager;
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
    HashMap<String, Long> unReplicatedSizeCountMap = initializeSizeMap(false);
    HashMap<String, Long> replicatedSizeCountMap = initializeSizeMap(true);

    for (String tableName : getTaskTables()) {
      Table table = omMetadataManager.getTable(tableName);
      if (table == null) {
        LOG.error("Table " + tableName + " not found in OM Metadata.");
        return new ImmutablePair<>(getTaskName(), false);
      }

      try (
          TableIterator<String, ? extends Table.KeyValue<String, ?>> iterator
              = table.iterator()) {
        if (getTablesToCalculateSize().contains(tableName)) {
          Triple<Long, Long, Long> details = getTableSizeAndCount(iterator);
          objectCountMap.put(getTableCountKeyFromTable(tableName),
              details.getLeft());
          unReplicatedSizeCountMap.put(
              getUnReplicatedSizeKeyFromTable(tableName), details.getMiddle());
          replicatedSizeCountMap.put(getReplicatedSizeKeyFromTable(tableName),
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
    if (!unReplicatedSizeCountMap.isEmpty()) {
      writeDataToDB(unReplicatedSizeCountMap);
    }
    if (!replicatedSizeCountMap.isEmpty()) {
      writeDataToDB(replicatedSizeCountMap);
    }

    LOG.info("Completed a 'reprocess' run of OmTableInsightTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  /**
   * Returns a triple with the total count of records (left), total unreplicated
   * size (middle), and total replicated size (right) in the given iterator.
   * Increments count for each record and adds the dataSize if a record's value
   * is an instance of OmKeyInfo. If the iterator is null, returns (0, 0, 0).
   *
   * @param iterator The iterator over the table to be iterated.
   * @return A Triple with three Long values representing the count,
   *         unreplicated size and replicated size.
   * @throws IOException If an I/O error occurs during the iterator traversal.
   */
  private Triple<Long, Long, Long> getTableSizeAndCount(
      TableIterator<String, ? extends Table.KeyValue<String, ?>> iterator)
      throws IOException {
    long count = 0;
    long unReplicatedSize = 0;
    long replicatedSize = 0;

    if (iterator != null) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, ?> kv = iterator.next();
        if (kv != null && kv.getValue() != null) {
          if (kv.getValue() instanceof OmKeyInfo) {
            OmKeyInfo omKeyInfo = (OmKeyInfo) kv.getValue();
            unReplicatedSize += omKeyInfo.getDataSize();
            replicatedSize += omKeyInfo.getReplicatedSize();
            count++;
          }
          if (kv.getValue() instanceof RepeatedOmKeyInfo) {
            RepeatedOmKeyInfo repeatedOmKeyInfo = (RepeatedOmKeyInfo) kv
                .getValue();
            Pair<Long, Long> result = repeatedOmKeyInfo.getTotalSize();
            unReplicatedSize += result.getRight();
            replicatedSize += result.getLeft();
            // Since we can have multiple deleted keys of same name
            count += repeatedOmKeyInfo.getOmKeyInfoList().size();
          }
        }
      }
    }

    return Triple.of(count, unReplicatedSize, replicatedSize);
  }

  /**
   * Returns a collection of table names that require data size calculation.
   */
  public Collection<String> getTablesToCalculateSize() {
    List<String> taskTables = new ArrayList<>();
    taskTables.add(OPEN_KEY_TABLE);
    taskTables.add(OPEN_FILE_TABLE);
    taskTables.add(DELETED_TABLE);
    return taskTables;
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
    HashMap<String, Long> unreplicatedSizeCountMap = initializeSizeMap(false);
    HashMap<String, Long> replicatedSizeCountMap = initializeSizeMap(true);
    final Collection<String> taskTables = getTaskTables();
    final Collection<String> sizeRelatedTables = getTablesToCalculateSize();

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
          handlePutEvent(omdbUpdateEvent, tableName, sizeRelatedTables,
              objectCountMap, unreplicatedSizeCountMap, replicatedSizeCountMap);
          break;

        case DELETE:
          handleDeleteEvent(omdbUpdateEvent, tableName, sizeRelatedTables,
              objectCountMap, unreplicatedSizeCountMap, replicatedSizeCountMap);
          break;

        case UPDATE:
          handleUpdateEvent(omdbUpdateEvent, tableName, sizeRelatedTables,
              objectCountMap, unreplicatedSizeCountMap, replicatedSizeCountMap);
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
    if (!unreplicatedSizeCountMap.isEmpty()) {
      writeDataToDB(unreplicatedSizeCountMap);
    }
    if (!replicatedSizeCountMap.isEmpty()) {
      writeDataToDB(replicatedSizeCountMap);
    }
    LOG.info("Completed a 'process' run of OmTableInsightTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  private void handlePutEvent(OMDBUpdateEvent<String, Object> event,
                              String tableName,
                              Collection<String> sizeRelatedTables,
                              HashMap<String, Long> objectCountMap,
                              HashMap<String, Long> unreplicatedSizeCountMap,
                              HashMap<String, Long> replicatedSizeCountMap) {

    if (sizeRelatedTables.contains(tableName)) {
      handleSizeRelatedTablePutEvent(event, tableName, objectCountMap,
          unreplicatedSizeCountMap, replicatedSizeCountMap);
    } else {
      String countKey = getTableCountKeyFromTable(tableName);
      objectCountMap.computeIfPresent(countKey, (k, count) -> count + 1L);
    }
  }

  private void handleSizeRelatedTablePutEvent(
      OMDBUpdateEvent<String, Object> event,
      String tableName,
      HashMap<String, Long> objectCountMap,
      HashMap<String, Long> unreplicatedSizeCountMap,
      HashMap<String, Long> replicatedSizeCountMap) {

    String countKey = getTableCountKeyFromTable(tableName);
    String unReplicatedSizeKey = getUnReplicatedSizeKeyFromTable(tableName);
    String replicatedSizeKey = getReplicatedSizeKeyFromTable(tableName);

    if (event.getValue() instanceof OmKeyInfo) {
      // Handle PUT for OpenKeyTable & OpenFileTable
      OmKeyInfo omKeyInfo = (OmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(countKey, (k, count) -> count + 1L);
      unreplicatedSizeCountMap.computeIfPresent(unReplicatedSizeKey,
          (k, size) -> size + omKeyInfo.getDataSize());
      replicatedSizeCountMap.computeIfPresent(replicatedSizeKey,
          (k, size) -> size + omKeyInfo.getReplicatedSize());
    } else if (event.getValue() instanceof RepeatedOmKeyInfo) {
      // Handle PUT for DeletedTable
      RepeatedOmKeyInfo repeatedOmKeyInfo =
          (RepeatedOmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(countKey,
          (k, count) -> count + repeatedOmKeyInfo.getOmKeyInfoList().size());
      Pair<Long, Long> result = repeatedOmKeyInfo.getTotalSize();
      unreplicatedSizeCountMap.computeIfPresent(unReplicatedSizeKey,
          (k, size) -> size + result.getLeft());
      replicatedSizeCountMap.computeIfPresent(replicatedSizeKey,
          (k, size) -> size + result.getRight());
    }
  }


  private void handleDeleteEvent(OMDBUpdateEvent<String, Object> event,
                                 String tableName,
                                 Collection<String> sizeRelatedTables,
                                 HashMap<String, Long> objectCountMap,
                                 HashMap<String, Long> unreplicatedSizeCountMap,
                                 HashMap<String, Long> replicatedSizeCountMap) {

    if (event.getValue() != null) {
      if (sizeRelatedTables.contains(tableName)) {
        handleSizeRelatedTableDeleteEvent(event, tableName, objectCountMap,
            unreplicatedSizeCountMap, replicatedSizeCountMap);
      } else {
        String countKey = getTableCountKeyFromTable(tableName);
        objectCountMap.computeIfPresent(countKey,
            (k, count) -> count > 0 ? count - 1L : 0L);
      }
    }
  }

  private void handleSizeRelatedTableDeleteEvent(
      OMDBUpdateEvent<String, Object> event,
      String tableName,
      HashMap<String, Long> objectCountMap,
      HashMap<String, Long> unreplicatedSizeCountMap,
      HashMap<String, Long> replicatedSizeCountMap) {

    String countKey = getTableCountKeyFromTable(tableName);
    String unReplicatedSizeKey = getUnReplicatedSizeKeyFromTable(tableName);
    String replicatedSizeKey = getReplicatedSizeKeyFromTable(tableName);

    if (event.getValue() instanceof OmKeyInfo) {
      // Handle DELETE for OpenKeyTable & OpenFileTable
      OmKeyInfo omKeyInfo = (OmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(countKey,
          (k, count) -> count > 0 ? count - 1L : 0L);
      unreplicatedSizeCountMap.computeIfPresent(unReplicatedSizeKey,
          (k, size) -> size > omKeyInfo.getDataSize() ?
              size - omKeyInfo.getDataSize() : 0L);
      replicatedSizeCountMap.computeIfPresent(replicatedSizeKey,
          (k, size) -> size > omKeyInfo.getReplicatedSize() ?
              size - omKeyInfo.getReplicatedSize() : 0L);
    } else if (event.getValue() instanceof RepeatedOmKeyInfo) {
      // Handle DELETE for DeletedTable
      RepeatedOmKeyInfo repeatedOmKeyInfo =
          (RepeatedOmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(countKey, (k, count) -> count > 0 ?
          count - repeatedOmKeyInfo.getOmKeyInfoList().size() : 0L);
      Pair<Long, Long> result = repeatedOmKeyInfo.getTotalSize();
      unreplicatedSizeCountMap.computeIfPresent(unReplicatedSizeKey,
          (k, size) -> size > result.getLeft() ? size - result.getLeft() : 0L);
      replicatedSizeCountMap.computeIfPresent(replicatedSizeKey,
          (k, size) -> size > result.getRight() ? size - result.getRight() :
              0L);
    }
  }

  private void handleUpdateEvent(OMDBUpdateEvent<String, Object> event,
                                 String tableName,
                                 Collection<String> sizeRelatedTables,
                                 HashMap<String, Long> objectCountMap,
                                 HashMap<String, Long> unreplicatedSizeCountMap,
                                 HashMap<String, Long> replicatedSizeCountMap) {

    if (event.getValue() != null) {
      if (sizeRelatedTables.contains(tableName)) {
        // Handle update for only size related tables
        handleSizeRelatedTableUpdateEvent(event, tableName, objectCountMap,
            unreplicatedSizeCountMap, replicatedSizeCountMap);
      }
    }
  }


  private void handleSizeRelatedTableUpdateEvent(
      OMDBUpdateEvent<String, Object> event,
      String tableName,
      HashMap<String, Long> objectCountMap,
      HashMap<String, Long> unreplicatedSizeCountMap,
      HashMap<String, Long> replicatedSizeCountMap) {

    if (event.getOldValue() == null) {
      LOG.warn("Update event does not have the old Key Info for {}.",
          event.getKey());
      return;
    }
    String countKey = getTableCountKeyFromTable(tableName);
    String unReplicatedSizeKey = getUnReplicatedSizeKeyFromTable(tableName);
    String replicatedSizeKey = getReplicatedSizeKeyFromTable(tableName);

    // In Update event the count for the open table will not change. So we don't
    // need to update the count. Except for RepeatedOmKeyInfo, for which the
    // size of omKeyInfoList can change
    if (event.getValue() instanceof OmKeyInfo) {
      // Handle UPDATE for OpenKeyTable & OpenFileTable
      OmKeyInfo oldKeyInfo = (OmKeyInfo) event.getOldValue();
      OmKeyInfo newKeyInfo = (OmKeyInfo) event.getValue();
      unreplicatedSizeCountMap.computeIfPresent(unReplicatedSizeKey,
          (k, size) -> size - oldKeyInfo.getDataSize() +
              newKeyInfo.getDataSize());
      replicatedSizeCountMap.computeIfPresent(replicatedSizeKey,
          (k, size) -> size - oldKeyInfo.getReplicatedSize() +
              newKeyInfo.getReplicatedSize());
    } else if (event.getValue() instanceof RepeatedOmKeyInfo) {
      // Handle UPDATE for DeletedTable
      RepeatedOmKeyInfo oldRepeatedOmKeyInfo =
          (RepeatedOmKeyInfo) event.getOldValue();
      RepeatedOmKeyInfo newRepeatedOmKeyInfo =
          (RepeatedOmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(countKey,
          (k, count) -> count > 0 ?
              count - oldRepeatedOmKeyInfo.getOmKeyInfoList().size() +
                  newRepeatedOmKeyInfo.getOmKeyInfoList().size() : 0L);
      Pair<Long, Long> oldSize = oldRepeatedOmKeyInfo.getTotalSize();
      Pair<Long, Long> newSize = newRepeatedOmKeyInfo.getTotalSize();
      unreplicatedSizeCountMap.computeIfPresent(unReplicatedSizeKey,
          (k, size) -> size - oldSize.getLeft() + newSize.getLeft());
      replicatedSizeCountMap.computeIfPresent(replicatedSizeKey,
          (k, size) -> size - oldSize.getRight() + newSize.getRight());
    }
  }


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
    Collection<String> tables = getTablesToCalculateSize();
    HashMap<String, Long> sizeCountMap = new HashMap<>(tables.size());
    for (String tableName : tables) {
      String key = replicated ? getReplicatedSizeKeyFromTable(tableName) :
          getUnReplicatedSizeKeyFromTable(tableName);
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


