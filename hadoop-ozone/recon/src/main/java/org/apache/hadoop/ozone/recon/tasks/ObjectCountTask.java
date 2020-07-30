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
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.hadoop.ozone.recon.schema.tables.pojos.GlobalStats;
import org.jooq.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.VOLUME_TABLE;
import static org.apache.hadoop.ozone.recon.ReconConstants.BUCKET_COUNT_KEY;
import static org.apache.hadoop.ozone.recon.ReconConstants.DELETED_KEY_COUNT_KEY;
import static org.apache.hadoop.ozone.recon.ReconConstants.KEY_COUNT_KEY;
import static org.apache.hadoop.ozone.recon.ReconConstants.OPEN_KEY_COUNT_KEY;
import static org.apache.hadoop.ozone.recon.ReconConstants.VOLUME_COUNT_KEY;
import static org.jooq.impl.DSL.currentTimestamp;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.using;

/**
 * Class to iterate over the OM DB and store the total counts of volumes,
 * buckets, keys, open keys, deleted keys, etc.
 */
public class ObjectCountTask implements ReconOmTask {
  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectCountTask.class);

  private GlobalStatsDao globalStatsDao;
  private Configuration sqlConfiguration;
  private HashMap<String, String> tableNameToRowKey;
  private HashMap<String, Long> objectCountMap;

  @Inject
  public ObjectCountTask(GlobalStatsDao globalStatsDao,
                         Configuration sqlConfiguration) {
    this.globalStatsDao = globalStatsDao;
    this.sqlConfiguration = sqlConfiguration;
    tableNameToRowKey = new HashMap<>();
    tableNameToRowKey.put(VOLUME_TABLE, VOLUME_COUNT_KEY);
    tableNameToRowKey.put(BUCKET_TABLE, BUCKET_COUNT_KEY);
    tableNameToRowKey.put(KEY_TABLE, KEY_COUNT_KEY);
    tableNameToRowKey.put(OPEN_KEY_TABLE, OPEN_KEY_COUNT_KEY);
    tableNameToRowKey.put(DELETED_TABLE, DELETED_KEY_COUNT_KEY);
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
    Table<String, OmKeyInfo> omKeyInfoTable = omMetadataManager.getKeyTable();
    Table<String, OmBucketInfo> omBucketInfoTable =
        omMetadataManager.getBucketTable();
    Table<String, OmVolumeArgs> omVolumeInfoTable =
        omMetadataManager.getVolumeTable();
    Table<String, OmKeyInfo> omOpenKeyTable =
        omMetadataManager.getOpenKeyTable();
    Table<String, RepeatedOmKeyInfo> omDeletedKeyTable =
        omMetadataManager.getDeletedTable();
    long keyCount = 0L;
    long bucketCount = 0L;
    long volumeCount = 0L;
    long openKeyCount = 0L;
    long deletedKeyCount = 0L;

    // iterate key table and calculate the total count
    try (TableIterator keyIter = omKeyInfoTable.iterator()) {
      keyCount = getCount(keyIter);
    } catch (IOException ioEx) {
      LOG.error("Unable to populate Key Count in Recon DB.", ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }

    // iterate bucket table and calculate the total count
    try (TableIterator bucketIter = omBucketInfoTable.iterator()) {
      bucketCount = getCount(bucketIter);
    } catch (IOException ioEx) {
      LOG.error("Unable to populate Bucket Count in Recon DB.", ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }

    // iterate volume table and calculate the total count
    try (TableIterator volumeIter = omVolumeInfoTable.iterator()) {
      volumeCount = getCount(volumeIter);
    } catch (IOException ioEx) {
      LOG.error("Unable to populate Volume Count in Recon DB.", ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }

    // iterate openKey table and calculate the total count
    try (TableIterator openKeyIter = omOpenKeyTable.iterator()) {
      openKeyCount = getCount(openKeyIter);
    } catch (IOException ioEx) {
      LOG.error("Unable to populate OpenKey Count in Recon DB.", ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }

    // iterate deletedKey table and calculate the total count
    try (TableIterator deletedKeyIter = omDeletedKeyTable.iterator()) {
      deletedKeyCount = getCount(deletedKeyIter);
    } catch (IOException ioEx) {
      LOG.error("Unable to populate DeletedKey Count in Recon DB.", ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }

    // insert or update counts in global stats table
    upsertGlobalStatsTable(VOLUME_COUNT_KEY, volumeCount);
    upsertGlobalStatsTable(BUCKET_COUNT_KEY, bucketCount);
    upsertGlobalStatsTable(KEY_COUNT_KEY, keyCount);
    upsertGlobalStatsTable(OPEN_KEY_COUNT_KEY, openKeyCount);
    upsertGlobalStatsTable(DELETED_KEY_COUNT_KEY, deletedKeyCount);

    LOG.info("Completed a 'reprocess' run of ObjectCountTask.");
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

  private void upsertGlobalStatsTable(String key, Long count) {
    // Get the current timestamp
    Timestamp now =
        using(sqlConfiguration).fetchValue(select(currentTimestamp()));
    GlobalStats record = globalStatsDao.fetchOneByKey(key);
    GlobalStats newRecord = new GlobalStats(key, count, now);

    // Insert a new record for key if it does not exist
    if (record == null) {
      globalStatsDao.insert(newRecord);
    } else {
      globalStatsDao.update(newRecord);
    }
  }

  @Override
  public String getTaskName() {
    return "ObjectCountTask";
  }

  @Override
  public Collection<String> getTaskTables() {
    return new ArrayList<>(
        Arrays.asList(KEY_TABLE, VOLUME_TABLE, BUCKET_TABLE,
            OPEN_KEY_TABLE, DELETED_TABLE));
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

    initializeCountMap();

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, Object> omdbUpdateEvent = eventIterator.next();

      try{
        switch (omdbUpdateEvent.getAction()) {
        case PUT:
          handlePutEvent(tableNameToRowKey.get(omdbUpdateEvent.getTable()));
          break;

        case DELETE:
          handleDeleteEvent(tableNameToRowKey.get(omdbUpdateEvent.getTable()));
          break;

        default: LOG.trace("Skipping DB update event : Table: {}, Action: {}",
            omdbUpdateEvent.getTable(), omdbUpdateEvent.getAction());
        }
      } catch (Exception e) {
        LOG.error("Unexpected exception while processing object : Table {}, " +
                "Action: {}", omdbUpdateEvent.getTable(),
            omdbUpdateEvent.getAction(), e);
        return new ImmutablePair<>(getTaskName(), false);
      }
    }
    writeCountsToDB();
    LOG.info("Completed a 'process' run of ObjectCountTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  private void writeCountsToDB() {
    for (Entry<String, Long> entry: objectCountMap.entrySet()) {
      upsertGlobalStatsTable(entry.getKey(), entry.getValue());
    }
  }

  private void handleDeleteEvent(String key) throws RuntimeException {
    if (objectCountMap.containsKey(key)) {
      long count = objectCountMap.get(key);
      if (count > 0) {
        objectCountMap.put(key, count - 1L);
      }
    } else {
      throw new RuntimeException(
          String.format("objectCountMap does not contain key: %s", key));
    }
  }

  private void handlePutEvent(String key) throws RuntimeException {
    if (objectCountMap.containsKey(key)) {
      long count = objectCountMap.get(key);
      objectCountMap.put(key, count + 1L);
    } else {
      throw new RuntimeException(
          String.format("objectCountMap does not contain key: %s", key));
    }
  }

  private void initializeCountMap() {
    objectCountMap = new HashMap<>();
    objectCountMap.put(VOLUME_COUNT_KEY, getCountForKey(VOLUME_COUNT_KEY));
    objectCountMap.put(BUCKET_COUNT_KEY, getCountForKey(BUCKET_COUNT_KEY));
    objectCountMap.put(KEY_COUNT_KEY, getCountForKey(KEY_COUNT_KEY));
    objectCountMap.put(OPEN_KEY_COUNT_KEY, getCountForKey(OPEN_KEY_COUNT_KEY));
    objectCountMap.put(DELETED_KEY_COUNT_KEY,
        getCountForKey(DELETED_KEY_COUNT_KEY));
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
