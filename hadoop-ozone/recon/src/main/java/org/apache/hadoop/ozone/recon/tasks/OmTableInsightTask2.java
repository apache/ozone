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

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconNamespaceSummaryManagerImpl;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.hadoop.ozone.recon.schema.tables.pojos.GlobalStats;
import org.jooq.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.Map.Entry;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.*;
import static org.jooq.impl.DSL.*;

/**
 * Class to iterate over the OM DB and store the total counts of volumes,
 * buckets, keys, open keys, deleted keys, etc.
 */
public class OmTableInsightTask2 implements ReconOmTask {
  private static final Logger LOG =
      LoggerFactory.getLogger(OmTableInsightTask2.class);

  private GlobalStatsDao globalStatsDao;
  private Configuration sqlConfiguration;
  private ReconOMMetadataManager reconOMMetadataManager;
  private ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager;
  private Table<Long, NSSummary> nsSummaryTable;
  private Map<String, OmTableHandler> tableHandlers;

  @Inject
  public OmTableInsightTask2(GlobalStatsDao globalStatsDao,
                             Configuration sqlConfiguration,
                             ReconOMMetadataManager reconOMMetadataManager,
                             ReconNamespaceSummaryManagerImpl
                                  reconNamespaceSummaryManager) {
    this.globalStatsDao = globalStatsDao;
    this.sqlConfiguration = sqlConfiguration;
    this.reconOMMetadataManager = reconOMMetadataManager;
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.nsSummaryTable = reconNamespaceSummaryManager.getNSSummaryTable();

    // Initialize table handlers
    tableHandlers = new HashMap<>();
    tableHandlers.put(OPEN_KEY_TABLE, new OpenKeyTableHandler());
    tableHandlers.put(OPEN_FILE_TABLE, new OpenFileTableHandler());
    tableHandlers.put(DELETED_TABLE, new DeletedTableHandler());
    tableHandlers.put(DELETED_DIR_TABLE, new DeletedDirectoryTableHandler(reconNamespaceSummaryManager));
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
          Triple<Long, Long, Long> details = getTableSizeAndCount(iterator,
              tableName);
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
   * is an instance of OmKeyInfo,RepeatedOmKeyInfo.
   * If the iterator is null, returns (0, 0, 0).
   *
   * @param iterator The iterator over the table to be iterated.
   * @param tableName The name of the table being iterated.
   * @return A Triple with three Long values representing the count,
   *         unreplicated size and replicated size.
   * @throws IOException If an I/O error occurs during the iterator traversal.
   */
  private Triple<Long, Long, Long> getTableSizeAndCount(
      TableIterator<String, ? extends Table.KeyValue<String, ?>> iterator,
      String tableName)
      throws IOException {
    long count = 0;
    long unReplicatedSize = 0;
    long replicatedSize = 0;

    boolean isFileTable = tableName.equals(OPEN_FILE_TABLE);
    boolean isKeyTable = tableName.equals(OPEN_KEY_TABLE);
    boolean isDeletedDirTable = tableName.equals(DELETED_DIR_TABLE);

    if (iterator != null) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, ?> kv = iterator.next();
        if (kv != null && kv.getValue() != null) {
          if (kv.getValue() instanceof OmKeyInfo) {
            OmKeyInfo omKeyInfo = (OmKeyInfo) kv.getValue();
            if (isFileTable || isKeyTable) {
              unReplicatedSize += omKeyInfo.getDataSize();
              replicatedSize += omKeyInfo.getReplicatedSize();
            } else if (isDeletedDirTable) {
              unReplicatedSize +=
                  fetchSizeForDeletedDirectory(omKeyInfo.getObjectID());
            }
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
   * Given an object ID, return total data size (no replication)
   * under this object. Note:- This method is RECURSIVE.
   *
   * @param objectId the object's ID
   * @return total used data size in bytes
   * @throws IOException ioEx
   */
  protected long fetchSizeForDeletedDirectory(long objectId)
      throws IOException {
    NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    if (nsSummary == null) {
      return 0L;
    }
    long totalSize = nsSummary.getSizeOfFiles();
    for (long childId : nsSummary.getChildDir()) {
      totalSize += fetchSizeForDeletedDirectory(childId);
    }
    return totalSize;
  }

  /**
   * Returns a collection of table names that require data size calculation.
   */
  public Collection<String> getTablesToCalculateSize() {
    List<String> taskTables = new ArrayList<>();
    taskTables.add(OPEN_KEY_TABLE);
    taskTables.add(OPEN_FILE_TABLE);
    taskTables.add(DELETED_TABLE);
    taskTables.add(DELETED_DIR_TABLE);
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
                              HashMap<String, Long> replicatedSizeCountMap)
      throws IOException {
    OmTableHandler tableHandler = tableHandlers.get(tableName);

    if (sizeRelatedTables.contains(tableName) && tableHandler != null) {
      tableHandler.handlePutEvent(event, tableName, sizeRelatedTables,
          objectCountMap, unreplicatedSizeCountMap, replicatedSizeCountMap);
    } else {
      String countKey = getTableCountKeyFromTable(tableName);
      objectCountMap.computeIfPresent(countKey, (k, count) -> count + 1L);
    }
  }

  private void handleDeleteEvent(OMDBUpdateEvent<String, Object> event,
                                 String tableName,
                                 Collection<String> sizeRelatedTables,
                                 HashMap<String, Long> objectCountMap,
                                 HashMap<String, Long> unreplicatedSizeCountMap,
                                 HashMap<String, Long> replicatedSizeCountMap)
      throws IOException {
    OmTableHandler tableHandler = tableHandlers.get(tableName);

    if (event.getValue() != null) {
      if (sizeRelatedTables.contains(tableName)) {
        tableHandler.handleDeleteEvent(event, tableName, sizeRelatedTables,
            objectCountMap, unreplicatedSizeCountMap, replicatedSizeCountMap);
      } else {
        String countKey = getTableCountKeyFromTable(tableName);
        objectCountMap.computeIfPresent(countKey,
            (k, count) -> count > 0 ? count - 1L : 0L);
      }
    }
  }


  private void handleUpdateEvent(OMDBUpdateEvent<String, Object> event,
                                 String tableName,
                                 Collection<String> sizeRelatedTables,
                                 HashMap<String, Long> objectCountMap,
                                 HashMap<String, Long> unreplicatedSizeCountMap,
                                 HashMap<String, Long> replicatedSizeCountMap) {

    OmTableHandler tableHandler = tableHandlers.get(tableName);

    if (event.getValue() != null) {
      if (sizeRelatedTables.contains(tableName)) {
        // Handle update for only size related tables
        tableHandler.handleUpdateEvent(event, tableName, sizeRelatedTables,
            objectCountMap, unreplicatedSizeCountMap, replicatedSizeCountMap);
      }
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

  @VisibleForTesting
  public void setNsSummaryTable(Table<Long, NSSummary> nsSummaryTable) {
    this.nsSummaryTable = nsSummaryTable;
  }

}


