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
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.hadoop.ozone.recon.schema.tables.pojos.GlobalStats;
import org.jooq.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * Class to iterate over the OM DB and store the total counts of volumes,
 * buckets, keys, open keys, deleted keys, etc.
 */
public class TableCountTask implements ReconOmTask {
  private static final Logger LOG =
      LoggerFactory.getLogger(TableCountTask.class);

  private GlobalStatsDao globalStatsDao;
  private Configuration sqlConfiguration;
  private ReconOMMetadataManager reconOMMetadataManager;

  @Inject
  public TableCountTask(GlobalStatsDao globalStatsDao,
                        Configuration sqlConfiguration,
                        ReconOMMetadataManager reconOMMetadataManager) {
    this.globalStatsDao = globalStatsDao;
    this.sqlConfiguration = sqlConfiguration;
    this.reconOMMetadataManager = reconOMMetadataManager;
  }

  /**
   * Iterate the rows of each table in OM snapshot DB and calculate the
   * counts for each table.
   *
   * @param omMetadataManager OM Metadata instance.
   * @return Pair
   */
  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    for (String tableName : getTaskTables()) {
      Table table = omMetadataManager.getTable(tableName);
      try (TableIterator keyIter = table.iterator()) {
        long count = getCount(keyIter);
        ReconUtils.upsertGlobalStatsTable(sqlConfiguration, globalStatsDao,
            getRowKeyFromTable(tableName),
            count);
      } catch (IOException ioEx) {
        LOG.error("Unable to populate Table Count in Recon DB.", ioEx);
        return new ImmutablePair<>(getTaskName(), false);
      }
    }
    LOG.info("Completed a 'reprocess' run of TableCountTask.");
    return new ImmutablePair<>(getTaskName(), true);
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
    return "TableCountTask";
  }

  public Collection<String> getTaskTables() {
    return new ArrayList<>(reconOMMetadataManager.listTableNames());
  }

  /**
   * Read the update events and update the count of respective object
   * (volume, bucket, key etc.) based on the action (put or delete).
   *
   * @param events Update events - PUT, DELETE and UPDATE.
   * @return Pair
   */
  @Override
  public Pair<String, Boolean> process(OMUpdateEventBatch events) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    HashMap<String, Long> objectCountMap = initializeCountMap();
    final Collection<String> taskTables = getTaskTables();

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, Object> omdbUpdateEvent = eventIterator.next();
      // Filter event inside process method to avoid duping
      if (!taskTables.contains(omdbUpdateEvent.getTable())) {
        continue;
      }
      String rowKey = getRowKeyFromTable(omdbUpdateEvent.getTable());
      try{
        switch (omdbUpdateEvent.getAction()) {
        case PUT:
          objectCountMap.computeIfPresent(rowKey, (k, count) -> count + 1L);
          break;

        case DELETE:
          // if value is null, it means that the volume / bucket / key
          // is already deleted and does not exist in the OM database anymore.
          if (omdbUpdateEvent.getValue() != null) {
            String key = getRowKeyFromTable(omdbUpdateEvent.getTable());
            objectCountMap.computeIfPresent(key,
                (k, count) -> count > 0 ? count - 1L : 0L);
          }
          break;

        default: LOG.trace("Skipping DB update event : Table: {}, Action: {}",
            omdbUpdateEvent.getTable(), omdbUpdateEvent.getAction());
        }
      } catch (Exception e) {
        LOG.error("Unexpected exception while processing the table {}, " +
                "Action: {}", omdbUpdateEvent.getTable(),
            omdbUpdateEvent.getAction(), e);
        return new ImmutablePair<>(getTaskName(), false);
      }
    }
    for (Entry<String, Long> entry: objectCountMap.entrySet()) {
      ReconUtils.upsertGlobalStatsTable(sqlConfiguration, globalStatsDao,
          entry.getKey(),
          entry.getValue());
    }

    LOG.info("Completed a 'process' run of TableCountTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  private HashMap<String, Long> initializeCountMap() {
    Collection<String> tables = getTaskTables();
    HashMap<String, Long> objectCountMap = new HashMap<>(tables.size());
    for (String tableName: tables) {
      String key = getRowKeyFromTable(tableName);
      objectCountMap.put(key, getCountForKey(key));
    }
    return objectCountMap;
  }

  public static String getRowKeyFromTable(String tableName) {
    return tableName + "Count";
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
}
