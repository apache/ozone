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
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.WithObjectID;
import org.hadoop.ozone.recon.schema.MetricsSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.VolumeMetricsDao;
import org.hadoop.ozone.recon.schema.tables.pojos.VolumeMetrics;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.VOLUME_TABLE;
import static org.hadoop.ozone.recon.schema.tables.VolumeMetricsTable.VOLUME_METRICS;

/**
 * Class to iterate over the OM DB and populate the Recon metrics DB with
 * the metrics at volume, bucket, key, file and directory level.
 */
public class EntityMetricsGeneratorTask implements ReconOmTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(EntityMetricsGeneratorTask.class);

  private VolumeMetricsDao volumeMetricsDao;
  private DSLContext dslContext;

  @Inject
  public EntityMetricsGeneratorTask(VolumeMetricsDao volumeMetricsDao,
                                    MetricsSchemaDefinition
                               metricsSchemaDefinition) {
    this.volumeMetricsDao = volumeMetricsDao;
    this.dslContext = metricsSchemaDefinition.getDSLContext();
  }

  /**
   * Read the Keys from OM snapshot DB and calculate the upper bound of
   * File Size it belongs to.
   *
   * @param omMetadataManager OM Metadata instance.
   * @return Pair
   */
  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    Map<Long, VolumeMetrics> volumeMetricsMap = new HashMap<>();
    Table<String, OmVolumeArgs> omVolumeArgsTable =
        omMetadataManager.getVolumeTable();
    try (TableIterator<String, ? extends Table.KeyValue<String, OmVolumeArgs>>
             keyIter = omVolumeArgsTable.iterator()) {
      while (keyIter.hasNext()) {
        VolumeMetrics volumeMetrics = new VolumeMetrics();
        Table.KeyValue<String, OmVolumeArgs> kv = keyIter.next();
        OmVolumeArgs value = kv.getValue();
        volumeMetrics.setVolumeObjectid(value.getObjectID());
        List<OmBucketInfo> buckets = listBucketsUnderVolume(omMetadataManager,
            value.getVolume());
        volumeMetrics.setBucketCount(buckets.size());
        volumeMetrics.setSize(getTotalSizesUnderVolume(
            omMetadataManager, value.getVolume()));
        volumeMetrics.setCreationTimestamp(new Timestamp(
            value.getCreationTime()));
        volumeMetrics.setLastUpdatedTimestamp(new Timestamp(
            value.getModificationTime()));
        volumeMetricsMap.put(value.getObjectID(), volumeMetrics);
      }
    } catch (IOException ioEx) {
      LOG.error("Unable to populate File Size Count in Recon DB. ", ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }
    // Truncate table before inserting new rows
    int execute = dslContext.delete(VOLUME_METRICS).execute();
    LOG.info("Deleted {} records from {}", execute, VOLUME_METRICS);

    updateVolumeMetricsToDB(true, volumeMetricsMap);

    LOG.info("Completed a 'reprocess' run of FileSizeCountTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  @Override
  public String getTaskName() {
    return "EntityMetricsGeneratorTask";
  }

  public Collection<String> getTaskTables() {
    return Arrays.asList(VOLUME_TABLE, BUCKET_TABLE, KEY_TABLE,
        FILE_TABLE, DIRECTORY_TABLE);
  }

  /**
   * Read the Keys from update events and update the count of files
   * pertaining to a certain upper bound.
   *
   * @param events Update events - PUT/DELETE.
   * @return Pair
   */
  @Override
  public Pair<String, Boolean> process(OMUpdateEventBatch events) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    Map<Long, VolumeMetrics> volumeMetricsMap = new HashMap<>();
    final Collection<String> taskTables = getTaskTables();

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, ? extends
          WithObjectID> omdbUpdateEvent = eventIterator.next();
      OMDBUpdateEvent.OMDBUpdateAction action = omdbUpdateEvent.getAction();
      // Filter event inside process method to avoid duping
      String table = omdbUpdateEvent.getTable();
      if (!taskTables.contains(table)) {
        continue;
      }
      String updatedKey = omdbUpdateEvent.getKey();

      try {
        if (table.equals(VOLUME_TABLE)) {
          OMDBUpdateEvent<String, OmVolumeArgs> volTableUpdateEvent =
              (OMDBUpdateEvent<String, OmVolumeArgs>) omdbUpdateEvent;
          OmVolumeArgs updatedVolInfo = volTableUpdateEvent.getValue();
          OmVolumeArgs oldVolInfo = volTableUpdateEvent.getOldValue();

          switch (action) {
          case PUT:
            handlePutVolEvent(updatedVolInfo, volumeMetricsMap);
            break;

          case DELETE:
            handleDeleteVolEvent(updatedVolInfo, volumeMetricsMap);
            break;

          case UPDATE:
            //handleDeleteKeyEvent(updatedKey, omdbUpdateEvent.getOldValue(),
            //fileSizeCountMap);
            //handlePutKeyEvent(omKeyInfo, fileSizeCountMap);
            break;

          default:
            LOG.trace("Skipping DB update event : {}",
                omdbUpdateEvent.getAction());
          }
        }
      } catch (Exception e) {
        LOG.error("Unexpected exception while processing key {}.",
            updatedKey, e);
        return new ImmutablePair<>(getTaskName(), false);
      }
    }
    updateVolumeMetricsToDB(false, volumeMetricsMap);
    LOG.info("Completed a 'process' run of FileSizeCountTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  private void handleDeleteVolEvent(OmVolumeArgs updatedVolInfo,
                                    Map<Long, VolumeMetrics> volumeMetricsMap) {
    VolumeMetrics volumeMetrics = volumeMetricsMap.get(
        updatedVolInfo.getObjectID());
    if (volumeMetrics == null) {
      // If we don't have it in this batch we try to get it from the DB
      volumeMetrics = volumeMetricsDao.findById(updatedVolInfo.getObjectID());
    }
    if (volumeMetrics == null) {
      return;
    }
    volumeMetricsMap.remove(updatedVolInfo.getObjectID());
  }


  /**
   * Populate DB with the counts of file sizes calculated
   * using the dao.
   *
   */
  private void updateVolumeMetricsToDB(boolean isDbTruncated,
                                       Map<Long, VolumeMetrics>
                                           volumeMetricsMap) {
    List<VolumeMetrics> insertToDb = new ArrayList<>();
    List<VolumeMetrics> updateInDb = new ArrayList<>();

    volumeMetricsMap.keySet().forEach((Long key) -> {
      VolumeMetrics volumeMetrics = volumeMetricsMap.get(key);
      VolumeMetrics newRecord = new VolumeMetrics();
      newRecord.setVolumeObjectid(volumeMetrics.getVolumeObjectid());
      newRecord.setBucketCount(volumeMetrics.getBucketCount());
      newRecord.setAccessCount(volumeMetrics.getAccessCount());
      newRecord.setSize(volumeMetrics.getSize());
      newRecord.setCreationTimestamp(volumeMetrics.getCreationTimestamp());
      newRecord.setLastUpdatedTimestamp(
          volumeMetrics.getLastUpdatedTimestamp());
      if (!isDbTruncated) {
        // Get the current count from database and update
        VolumeMetrics volMetrics = volumeMetricsDao.findById(
            volumeMetrics.getVolumeObjectid());
        if (volMetrics == null &&
            (newRecord.getBucketCount() > 0 ||
            newRecord.getAccessCount() > 0 ||
            newRecord.getSize() > 0)) {
          // insert new row only for non-zero counts.
          insertToDb.add(newRecord);
        } else if (volMetrics != null) {
          newRecord.setBucketCount(volMetrics.getBucketCount() +
              volumeMetrics.getBucketCount());
          newRecord.setAccessCount(volMetrics.getAccessCount() +
              volumeMetrics.getAccessCount());
          newRecord.setSize(volMetrics.getSize() +
              volumeMetrics.getSize());
          newRecord.setLastUpdatedTimestamp(
              volumeMetrics.getLastUpdatedTimestamp());
          updateInDb.add(newRecord);
        }
      } else if (newRecord.getBucketCount() > 0 ||
          newRecord.getAccessCount() > 0 ||
          newRecord.getSize() > 0) {
        // insert new row only for non-zero counts.
        insertToDb.add(newRecord);
      }
    });
    volumeMetricsDao.insert(insertToDb);
    volumeMetricsDao.update(updateInDb);
  }

  private BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }

  private void handlePutVolEvent(OmVolumeArgs updatedVolInfo,
                                 Map<Long, VolumeMetrics> volumeMetricsMap) {
    VolumeMetrics volumeMetrics = volumeMetricsMap.get(
        updatedVolInfo.getObjectID());
    if (volumeMetrics == null) {
      // If we don't have it in this batch we try to get it from the DB
      volumeMetrics = volumeMetricsDao.findById(updatedVolInfo.getObjectID());
    }
    if (volumeMetrics == null) {
      // If we don't have it locally and in the DB we create a new instance
      // as this is a new ID
      volumeMetrics = new VolumeMetrics();
    }
    volumeMetrics.setVolumeObjectid(updatedVolInfo.getObjectID());
    volumeMetrics.setBucketCount(0);
    volumeMetrics.setSize(0);
    volumeMetrics.setCreationTimestamp(new Timestamp(
        updatedVolInfo.getCreationTime()));
    volumeMetrics.setLastUpdatedTimestamp(new Timestamp(
        updatedVolInfo.getModificationTime()));
    volumeMetricsMap.put(updatedVolInfo.getObjectID(), volumeMetrics);
  }

  /**
   * Calculate and update the count of files being tracked by
   * fileSizeCountMap.
   * Used by reprocess() and process().
   *
   * @param omKeyInfo OmKey being updated for count
   */
  /*private void handleDeleteKeyEvent(String key, OmKeyInfo omKeyInfo,
                                    Map<FileSizeCountTask.FileSizeCountKey, Long>
                                        fileSizeCountMap) {
    if (omKeyInfo == null) {
      LOG.warn("Deleting a key not found while handling DELETE key event. Key" +
          " not found in Recon OM DB : {}", key);
    } else {
      FileSizeCountTask.FileSizeCountKey countKey = getFileSizeCountKey(omKeyInfo);
      Long count = fileSizeCountMap.containsKey(countKey) ?
          fileSizeCountMap.get(countKey) - 1L : -1L;
      fileSizeCountMap.put(countKey, count);
    }
  }*/

  /**
   * List all buckets under a volume, if volume name is null, return all
   * buckets under the system.
   *
   * @param omMetadataManager
   * @param volumeName        volume name
   * @return a list of buckets
   * @throws IOException IOE
   */
  private List<OmBucketInfo> listBucketsUnderVolume(
      OMMetadataManager omMetadataManager, final String volumeName)
      throws IOException {
    List<OmBucketInfo> result = new ArrayList<>();
    // if volume name is null, seek prefix is an empty string
    String seekPrefix = "";

    Table<String, OmBucketInfo> bucketTable =
        omMetadataManager.getBucketTable();

    try (TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>>
             iterator = bucketTable.iterator()) {

      if (volumeName != null) {
        if (!volumeExists(omMetadataManager, volumeName)) {
          return result;
        }
        seekPrefix = omMetadataManager.getVolumeKey(volumeName + OM_KEY_PREFIX);
      }

      while (iterator.hasNext()) {
        Table.KeyValue<String, OmBucketInfo> kv = iterator.next();

        String key = kv.getKey();
        OmBucketInfo omBucketInfo = kv.getValue();

        if (omBucketInfo != null) {
          // We should return only the keys, whose keys match with
          // the seek prefix
          if (key.startsWith(seekPrefix)) {
            result.add(omBucketInfo);
          }
        }
      }
    }
    return result;
  }

  private Integer getTotalSizesUnderVolume(
      OMMetadataManager omMetadataManager, final String volumeName)
      throws IOException {
    int totalSize = 0;
    // if volume name is null, seek prefix is an empty string
    String seekPrefix = "";

    Table<String, OmBucketInfo> bucketTable =
        omMetadataManager.getBucketTable();

    try (TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>>
             iterator = bucketTable.iterator()) {

      if (volumeName != null) {
        if (!volumeExists(omMetadataManager, volumeName)) {
          return Integer.valueOf(totalSize);
        }
        seekPrefix = omMetadataManager.getVolumeKey(volumeName + OM_KEY_PREFIX);
      }

      while (iterator.hasNext()) {
        Table.KeyValue<String, OmBucketInfo> kv = iterator.next();

        String key = kv.getKey();
        OmBucketInfo omBucketInfo = kv.getValue();

        if (omBucketInfo != null) {
          // We should return only the keys, whose keys match with
          // the seek prefix
          if (key.startsWith(seekPrefix)) {
            totalSize += omBucketInfo.getUsedBytes();
          }
        }
      }
    }
    return Integer.valueOf(totalSize);
  }

  private boolean volumeExists(OMMetadataManager omMetadataManager,
                              String volName) throws IOException {
    String volDBKey = omMetadataManager.getVolumeKey(volName);
    return omMetadataManager.getVolumeTable().getSkipCache(volDBKey) != null;
  }

}
