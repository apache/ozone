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

import com.google.inject.Inject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
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

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.*;
import static org.jooq.impl.DSL.currentTimestamp;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.using;

/**
 * Class to iterate over the OM DB and store the total counts of volumes,
 * buckets, keys, open keys, deleted keys, etc.
 */
public class TableInsightTask implements ReconOmTask {
  private static final Logger LOG =
      LoggerFactory.getLogger(TableInsightTask.class);

  private GlobalStatsDao globalStatsDao;
  private Configuration sqlConfiguration;
  private ReconOMMetadataManager reconOMMetadataManager;

  @Inject
  public TableInsightTask(GlobalStatsDao globalStatsDao,
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
   * (as returned by getTablesRequiringSizeCalculation), both the number of
   * records (count) and total data size of the records are calculated.
   * For all other tables, only the count of records is calculated.
   *
   * @param omMetadataManager OM Metadata instance.
   * @return Pair
   */
  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    HashMap<String, Long> objectCountMap = initializeCountMap();
    HashMap<String, Long> sizeCountMap = initializeSizeMap();

    for (String tableName : getTaskTables()) {
      Table table = omMetadataManager.getTable(tableName);
      if (table == null) {
        LOG.error("Table " + tableName + " not found in OM Metadata.");
        return new ImmutablePair<>(getTaskName(), false);
      }

      try {
        if (getTablesRequiringSizeCalculation().contains(tableName)) {
          Pair<Long, Long> details = getTableSizeAndCount(table);
          objectCountMap.put(getTableCountKeyFromTable(tableName),
              details.getLeft());
          sizeCountMap.put(getTableSizeKeyFromTable(tableName),
              details.getRight());
        } else {
          long count = getCount(table.iterator());
          objectCountMap.put(getTableCountKeyFromTable(tableName), count);
        }
      } catch (IOException ioEx) {
        LOG.error("Unable to populate Table Count in Recon DB.", ioEx);
        return new ImmutablePair<>(getTaskName(), false);
      }
    }

    writeDataToDB(objectCountMap);
    writeDataToDB(sizeCountMap);

    LOG.info("Completed a 'reprocess' run of TableInsightTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  /**
   *  Returns a pair with the total count of records (left) and total data size
   *  (right) in the given table. Increments count for each record and adds the
   *  dataSize if a record's value is an instance of OmKeyInfo. If the table is
   *  null, returns (0,0).
   *
   * @param table The table from which to get the count and data size.
   * @return A pair of Long values of count & data size
   * @throws IOException If an I/O error occurs during the table iteration.
   */
  public Pair<Long, Long> getTableSizeAndCount(Table table) throws IOException {
    long size = 0;
    long count = 0;
    if (table == null) {
      return Pair.of(count, size);
    }

    TableIterator<String, ? extends Table.KeyValue<String, ?>> iterator =
        table.iterator();
    while (iterator.hasNext()) {
      Table.KeyValue<String, ?> kv = iterator.next();

      if (kv != null && kv.getValue() != null) {
        if (kv.getValue() instanceof OmKeyInfo) {
          size += ((OmKeyInfo) kv.getValue()).getDataSize();
        }
      }
      count++;  // Increment count for each row
    }
    return Pair.of(count, size);
  }

  /**
   * Returns a collection of table names that require data size calculation.
   */
  public Collection<String> getTablesRequiringSizeCalculation() {
    List<String> taskTables = new ArrayList<>();
    taskTables.add(OPEN_KEY_TABLE);
    taskTables.add(OPEN_FILE_TABLE);
    return taskTables;
  }

  private long getCount(Iterator iterator) {
    long count = 0L;
    while (iterator.hasNext()) {
      count++;
      iterator.next();
    }
    return count;
  }

  @Override
  public String getTaskName() {
    return "TableInsightTask";
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
    HashMap<String, Long> objectCountMap = initializeCountMap();
    HashMap<String, Long> objectSizeMap =
        initializeSizeMap();  // Initialize a new map for sizes
    final Collection<String> taskTables = getTaskTables();
    final Collection<String> sizeRelatedTables =
        getTablesRequiringSizeCalculation();

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, Object> omdbUpdateEvent = eventIterator.next();

      if (!taskTables.contains(omdbUpdateEvent.getTable())) {
        continue;
      }

      String countKey = getTableCountKeyFromTable(omdbUpdateEvent.getTable());
      String sizeKey = getTableSizeKeyFromTable(omdbUpdateEvent.getTable());

      try {
        switch (omdbUpdateEvent.getAction()) {
        case PUT:
          objectCountMap.computeIfPresent(countKey, (k, count) -> count + 1L);

          // Compute size if the table is size-related
          if (sizeRelatedTables.contains(omdbUpdateEvent.getTable()) &&
              omdbUpdateEvent.getValue() instanceof OmKeyInfo) {
            objectSizeMap.computeIfPresent(sizeKey, (k, size) -> size +
                ((OmKeyInfo) omdbUpdateEvent.getValue()).getDataSize());
          }
          break;

        case DELETE:
          if (omdbUpdateEvent.getValue() != null) {
            String key = getTableCountKeyFromTable(omdbUpdateEvent.getTable());
            objectCountMap.computeIfPresent(key,
                (k, count) -> count > 0 ? count - 1L : 0L);

            // Compute size if the table is size-related
            if (sizeRelatedTables.contains(omdbUpdateEvent.getTable()) &&
                omdbUpdateEvent.getValue() instanceof OmKeyInfo) {
              // The code ensures the resulting size isn't negative by setting
              // values smaller than the OmKeyInfo object's data size to 0L.
              objectSizeMap.computeIfPresent(sizeKey, (k, size) -> size >
                  ((OmKeyInfo) omdbUpdateEvent.getValue()).getDataSize() ?
                  size -
                      ((OmKeyInfo) omdbUpdateEvent.getValue()).getDataSize() :
                  0L);
            }
          }
          break;

        default:
          LOG.trace("Skipping DB update event : Table: {}, Action: {}",
              omdbUpdateEvent.getTable(), omdbUpdateEvent.getAction());
        }
      } catch (Exception e) {
        LOG.error(
            "Unexpected exception while processing the table {}, Action: {}",
            omdbUpdateEvent.getTable(), omdbUpdateEvent.getAction(), e);
        return new ImmutablePair<>(getTaskName(), false);
      }
    }

    writeDataToDB(objectCountMap); // Write count data to DB
    writeDataToDB(objectSizeMap);  // Write size data to DB

    LOG.info("Completed a 'process' run of TableInsightTask.");
    return new ImmutablePair<>(getTaskName(), true);
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
      objectCountMap.put(key, getCountForKey(key));
    }
    return objectCountMap;
  }

  private HashMap<String, Long> initializeSizeMap() {
    Collection<String> tables = getTablesRequiringSizeCalculation();
    HashMap<String, Long> sizeCountMap = new HashMap<>(tables.size());
    for (String tableName : tables) {
      String key = getTableSizeKeyFromTable(tableName);
      sizeCountMap.put(key, getSizeForKey(key));
    }
    return sizeCountMap;
  }

  public static String getTableCountKeyFromTable(String tableName) {
    return tableName + "TableCount";
  }

  public static String getTableSizeKeyFromTable(String tableName) {
    return tableName + "DataSize";
  }

  /**
   * Get the count stored for the given key from Global Stats table.
   * Return 0 if record not found.
   *
   * @param key Key in the global stats table
   * @return count
   */
  private long getCountForKey(String key) {
    GlobalStats record = globalStatsDao.fetchOneByKey(key);

    return (record == null) ? 0L : record.getValue();
  }

  /**
   * Get the size stored for the given key from Global Stats table.
   * Return 0 if record not found.
   *
   * @param key Key in the global stats table
   * @return size
   */
  private long getSizeForKey(String key) {
    GlobalStats record = globalStatsDao.fetchOneByKey(key);

    return (record == null) ? 0L : record.getValue();
  }
}
