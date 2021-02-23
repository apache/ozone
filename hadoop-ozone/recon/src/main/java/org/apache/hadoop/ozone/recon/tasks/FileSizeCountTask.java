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
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.FileCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.pojos.FileCountBySize;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.hadoop.ozone.recon.schema.tables.FileCountBySizeTable.FILE_COUNT_BY_SIZE;

/**
 * Class to iterate over the OM DB and store the counts of existing/new
 * files binned into ranges (1KB, 2Kb..,4MB,.., 1TB,..1PB) to the Recon
 * fileSize DB.
 */
public class FileSizeCountTask implements ReconOmTask {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSizeCountTask.class);

  // 1125899906842624L = 1PB
  private static final long MAX_FILE_SIZE_UPPER_BOUND = 1125899906842624L;
  private FileCountBySizeDao fileCountBySizeDao;
  private DSLContext dslContext;

  @Inject
  public FileSizeCountTask(FileCountBySizeDao fileCountBySizeDao,
                           UtilizationSchemaDefinition
                               utilizationSchemaDefinition) {
    this.fileCountBySizeDao = fileCountBySizeDao;
    this.dslContext = utilizationSchemaDefinition.getDSLContext();
  }

  private static int nextClosestPowerIndexOfTwo(long dataSize) {
    int index = 0;
    while(dataSize != 0) {
      dataSize >>= 1;
      index += 1;
    }
    return index;
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
    Table<String, OmKeyInfo> omKeyInfoTable = omMetadataManager.getKeyTable();
    Map<FileSizeCountKey, Long> fileSizeCountMap = new HashMap<>();
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        keyIter = omKeyInfoTable.iterator()) {
      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
        handlePutKeyEvent(kv.getValue(), fileSizeCountMap);
      }
    } catch (IOException ioEx) {
      LOG.error("Unable to populate File Size Count in Recon DB. ", ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }
    // Truncate table before inserting new rows
    int execute = dslContext.delete(FILE_COUNT_BY_SIZE).execute();
    LOG.info("Deleted {} records from {}", execute, FILE_COUNT_BY_SIZE);

    writeCountsToDB(true, fileSizeCountMap);

    LOG.info("Completed a 'reprocess' run of FileSizeCountTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  @Override
  public String getTaskName() {
    return "FileSizeCountTask";
  }

  public Collection<String> getTaskTables() {
    return Collections.singletonList(KEY_TABLE);
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
    Map<FileSizeCountKey, Long> fileSizeCountMap = new HashMap<>();
    final Collection<String> taskTables = getTaskTables();

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, OmKeyInfo> omdbUpdateEvent = eventIterator.next();
      // Filter event inside process method to avoid duping
      if (!taskTables.contains(omdbUpdateEvent.getTable())) {
        continue;
      }
      String updatedKey = omdbUpdateEvent.getKey();
      OmKeyInfo omKeyInfo = omdbUpdateEvent.getValue();

      try{
        switch (omdbUpdateEvent.getAction()) {
        case PUT:
          handlePutKeyEvent(omKeyInfo, fileSizeCountMap);
          break;

        case DELETE:
          handleDeleteKeyEvent(updatedKey, omKeyInfo, fileSizeCountMap);
          break;

        case UPDATE:
          handleDeleteKeyEvent(updatedKey, omdbUpdateEvent.getOldValue(),
              fileSizeCountMap);
          handlePutKeyEvent(omKeyInfo, fileSizeCountMap);
          break;

        default: LOG.trace("Skipping DB update event : {}",
            omdbUpdateEvent.getAction());
        }
      } catch (Exception e) {
        LOG.error("Unexpected exception while processing key {}.",
                updatedKey, e);
        return new ImmutablePair<>(getTaskName(), false);
      }
    }
    writeCountsToDB(false, fileSizeCountMap);
    LOG.info("Completed a 'process' run of FileSizeCountTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  private long getFileSizeUpperBound(long fileSize) {
    if (fileSize >= MAX_FILE_SIZE_UPPER_BOUND) {
      return Long.MAX_VALUE;
    }
    int index = nextClosestPowerIndexOfTwo(fileSize);
    // The smallest file size being tracked for count
    // is 1 KB i.e. 1024 = 2 ^ 10.
    int binIndex = index < 10 ? 0 : index - 10;
    return (long) Math.pow(2, (10 + binIndex));
  }

  /**
   * Populate DB with the counts of file sizes calculated
   * using the dao.
   *
   */
  private void writeCountsToDB(boolean isDbTruncated,
                               Map<FileSizeCountKey, Long> fileSizeCountMap) {
    fileSizeCountMap.keySet().forEach((FileSizeCountKey key) -> {
      FileCountBySize newRecord = new FileCountBySize();
      newRecord.setVolume(key.volume);
      newRecord.setBucket(key.bucket);
      newRecord.setFileSize(key.fileSizeUpperBound);
      newRecord.setCount(fileSizeCountMap.get(key));
      if (!isDbTruncated) {
        // Get the current count from database and update
        Record3<String, String, Long> recordToFind =
            dslContext.newRecord(
                FILE_COUNT_BY_SIZE.VOLUME,
                FILE_COUNT_BY_SIZE.BUCKET,
                FILE_COUNT_BY_SIZE.FILE_SIZE)
                .value1(key.volume)
                .value2(key.bucket)
                .value3(key.fileSizeUpperBound);
        FileCountBySize fileCountRecord =
            fileCountBySizeDao.findById(recordToFind);
        if (fileCountRecord == null && newRecord.getCount() > 0L) {
          // insert new row only for non-zero counts.
          fileCountBySizeDao.insert(newRecord);
        } else if (fileCountRecord != null) {
          newRecord.setCount(fileCountRecord.getCount() +
              fileSizeCountMap.get(key));
          fileCountBySizeDao.update(newRecord);
        }
      } else if (newRecord.getCount() > 0) {
        // insert new row only for non-zero counts.
        fileCountBySizeDao.insert(newRecord);
      }
    });
  }

  private FileSizeCountKey getFileSizeCountKey(OmKeyInfo omKeyInfo) {
    return new FileSizeCountKey(omKeyInfo.getVolumeName(),
        omKeyInfo.getBucketName(),
        getFileSizeUpperBound(omKeyInfo.getDataSize()));
  }

  /**
   * Calculate and update the count of files being tracked by
   * fileSizeCountMap.
   * Used by reprocess() and process().
   *
   * @param omKeyInfo OmKey being updated for count
   */
  private void handlePutKeyEvent(OmKeyInfo omKeyInfo,
                                 Map<FileSizeCountKey, Long> fileSizeCountMap) {
    FileSizeCountKey key = getFileSizeCountKey(omKeyInfo);
    Long count = fileSizeCountMap.containsKey(key) ?
        fileSizeCountMap.get(key) + 1L : 1L;
    fileSizeCountMap.put(key, count);
  }

  /**
   * Calculate and update the count of files being tracked by
   * fileSizeCountMap.
   * Used by reprocess() and process().
   *
   * @param omKeyInfo OmKey being updated for count
   */
  private void handleDeleteKeyEvent(String key, OmKeyInfo omKeyInfo,
                                    Map<FileSizeCountKey, Long>
                                        fileSizeCountMap) {
    if (omKeyInfo == null) {
      LOG.warn("Deleting a key not found while handling DELETE key event. Key" +
          " not found in Recon OM DB : {}", key);
    } else {
      FileSizeCountKey countKey = getFileSizeCountKey(omKeyInfo);
      Long count = fileSizeCountMap.containsKey(countKey) ?
          fileSizeCountMap.get(countKey) - 1L : -1L;
      fileSizeCountMap.put(countKey, count);
    }
  }

  private static class FileSizeCountKey {
    private String volume;
    private String bucket;
    private Long fileSizeUpperBound;

    FileSizeCountKey(String volume, String bucket,
                     Long fileSizeUpperBound) {
      this.volume = volume;
      this.bucket = bucket;
      this.fileSizeUpperBound = fileSizeUpperBound;
    }

    @Override
    public boolean equals(Object obj) {
      if(obj instanceof FileSizeCountKey) {
        FileSizeCountKey s = (FileSizeCountKey) obj;
        return volume.equals(s.volume) && bucket.equals(s.bucket) &&
            fileSizeUpperBound.equals(s.fileSizeUpperBound);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (volume  + bucket + fileSizeUpperBound).hashCode();
    }
  }
}
