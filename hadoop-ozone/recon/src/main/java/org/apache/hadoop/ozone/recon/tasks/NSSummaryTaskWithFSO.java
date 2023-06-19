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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.WithObjectID;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.OrphanKeyMetaData;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Class for handling FSO specific tasks.
 */
public class NSSummaryTaskWithFSO extends NSSummaryTaskDbEventHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(NSSummaryTaskWithFSO.class);
  private final ReconOMMetadataManager reconOMMetadataManager;

  public NSSummaryTaskWithFSO(ReconNamespaceSummaryManager
                              reconNamespaceSummaryManager,
                              ReconOMMetadataManager
                              reconOMMetadataManager,
                              OzoneConfiguration
                              ozoneConfiguration,
                              ReconDBProvider reconDBProvider)
      throws IOException {
    super(reconNamespaceSummaryManager,
        reconOMMetadataManager, ozoneConfiguration, reconDBProvider);
    this.reconOMMetadataManager = reconOMMetadataManager;
  }

  // We only listen to updates from FSO-enabled KeyTable(FileTable) and DirTable
  public Collection<String> getTaskTables() {
    return Arrays.asList(OmMetadataManagerImpl.FILE_TABLE,
        OmMetadataManagerImpl.DIRECTORY_TABLE,
        OmMetadataManagerImpl.DELETED_DIR_TABLE,
        OmMetadataManagerImpl.BUCKET_TABLE);
  }

  public boolean processWithFSO(OMUpdateEventBatch events) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    final Collection<String> taskTables = getTaskTables();
    Map<Long, NSSummary> nsSummaryMap = new HashMap<>();
    Map<Long, OrphanKeyMetaData> orphanKeysMetaDataMap = new HashMap<>();

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, ? extends
          WithObjectID> omdbUpdateEvent = eventIterator.next();
      OMDBUpdateEvent.OMDBUpdateAction action = omdbUpdateEvent.getAction();

      // we only process updates on OM's FileTable and Dirtable
      String table = omdbUpdateEvent.getTable();
      if (!taskTables.contains(table)) {
        continue;
      }

      String updatedKey = omdbUpdateEvent.getKey();

      try {
        if (table.equals(OmMetadataManagerImpl.FILE_TABLE)) {
          processFileTableEvent(nsSummaryMap, orphanKeysMetaDataMap,
              omdbUpdateEvent,
              action,
              updatedKey);
        } else if (table.equals(OmMetadataManagerImpl.DIRECTORY_TABLE)) {
          processDirectoryTableEvent(nsSummaryMap, orphanKeysMetaDataMap,
              omdbUpdateEvent,
              action,
              updatedKey);
        } else if (table.equals(OmMetadataManagerImpl.DELETED_DIR_TABLE)) {
          processDeletedDirTableEvent(orphanKeysMetaDataMap, omdbUpdateEvent,
              action);
        } else if (table.equals(OmMetadataManagerImpl.BUCKET_TABLE)) {
          processBucketTableEvent(orphanKeysMetaDataMap, omdbUpdateEvent,
              action);
        }
      } catch (IOException ioEx) {
        LOG.error("Unable to process Namespace Summary data in Recon DB. ",
            ioEx);
        return false;
      }
      if (!checkAndCallFlushToDB(nsSummaryMap)) {
        return false;
      }

      if (!checkOrphanDataAndCallWriteFlushToDB(orphanKeysMetaDataMap)) {
        return false;
      }
    }
    // flush and commit left out entries at end
    if (!flushAndCommitNSToDB(nsSummaryMap)) {
      return false;
    }
    // flush and commit left out entries at end
    if (!writeFlushAndCommitOrphanKeysMetaDataToDB(orphanKeysMetaDataMap)) {
      return false;
    }

    LOG.info("Completed a process run of NSSummaryTaskWithFSO");
    return true;
  }

  private void processFileTableEvent(
      Map<Long, NSSummary> nsSummaryMap,
      Map<Long, OrphanKeyMetaData> orphanKeysMetaDataMap,
      OMDBUpdateEvent<String, ? extends WithObjectID> omdbUpdateEvent,
      OMDBUpdateEvent.OMDBUpdateAction action,
      String updatedKey) throws IOException {
    // key update on fileTable
    OMDBUpdateEvent<String, OmKeyInfo> keyTableUpdateEvent =
        (OMDBUpdateEvent<String, OmKeyInfo>) omdbUpdateEvent;
    OmKeyInfo updatedKeyInfo = keyTableUpdateEvent.getValue();
    OmKeyInfo oldKeyInfo = keyTableUpdateEvent.getOldValue();

    switch (action) {
    case PUT:
      handlePutKeyEvent(updatedKeyInfo, nsSummaryMap,
          orphanKeysMetaDataMap,
          NODESTATUS.ORPHAN_PARENT_NODE_UPDATE_STATUS_IN_PROGRESS
              .getValue());
      break;

    case DELETE:
      handleDeleteKeyEvent(updatedKeyInfo, nsSummaryMap,
          orphanKeysMetaDataMap);
      break;

    case UPDATE:
      if (oldKeyInfo != null) {
        // delete first, then put
        handleDeleteKeyEvent(oldKeyInfo, nsSummaryMap,
            orphanKeysMetaDataMap);
      } else {
        LOG.warn("Update event does not have the old keyInfo for {}.",
            updatedKey);
      }
      handlePutKeyEvent(updatedKeyInfo, nsSummaryMap,
          orphanKeysMetaDataMap,
          NODESTATUS.ORPHAN_PARENT_NODE_UPDATE_STATUS_IN_PROGRESS
              .getValue());
      break;

    default:
      LOG.debug("Skipping DB update event : {}",
          omdbUpdateEvent.getAction());
    }
  }

  private void processDirectoryTableEvent(
      Map<Long, NSSummary> nsSummaryMap,
      Map<Long, OrphanKeyMetaData> orphanKeysMetaDataMap,
      OMDBUpdateEvent<String, ? extends WithObjectID> omdbUpdateEvent,
      OMDBUpdateEvent.OMDBUpdateAction action,
      String updatedKey) throws IOException {
    // directory update on DirTable
    OMDBUpdateEvent<String, OmDirectoryInfo> dirTableUpdateEvent =
        (OMDBUpdateEvent<String, OmDirectoryInfo>) omdbUpdateEvent;
    OmDirectoryInfo updatedDirectoryInfo = dirTableUpdateEvent.getValue();
    OmDirectoryInfo oldDirectoryInfo = dirTableUpdateEvent.getOldValue();

    switch (action) {
    case PUT:
      handlePutDirEvent(updatedDirectoryInfo, nsSummaryMap,
          orphanKeysMetaDataMap,
          NODESTATUS.ORPHAN_PARENT_NODE_UPDATE_STATUS_IN_PROGRESS
              .getValue());
      break;

    case DELETE:
      handleDeleteDirEvent(updatedDirectoryInfo, nsSummaryMap,
          orphanKeysMetaDataMap);
      break;

    case UPDATE:
      if (oldDirectoryInfo != null) {
        // delete first, then put
        handleDeleteDirEvent(oldDirectoryInfo, nsSummaryMap,
            orphanKeysMetaDataMap);
      } else {
        LOG.warn("Update event does not have the old dirInfo for {}.",
            updatedKey);
      }
      handlePutDirEvent(updatedDirectoryInfo, nsSummaryMap,
          orphanKeysMetaDataMap,
          NODESTATUS.ORPHAN_PARENT_NODE_UPDATE_STATUS_IN_PROGRESS
              .getValue());
      break;

    default:
      LOG.debug("Skipping DB update event : {}",
          omdbUpdateEvent.getAction());
    }
  }

  private void processDeletedDirTableEvent(
      Map<Long, OrphanKeyMetaData> orphanKeysMetaDataMap,
      OMDBUpdateEvent<String, ? extends WithObjectID> omdbUpdateEvent,
      OMDBUpdateEvent.OMDBUpdateAction action)
      throws IOException {
    // key update on deletedDirectoryTable
    OMDBUpdateEvent<String, OmKeyInfo> deletedDirTableUpdateEvent =
        (OMDBUpdateEvent<String, OmKeyInfo>) omdbUpdateEvent;
    OmKeyInfo updatedKeyInfo = deletedDirTableUpdateEvent.getValue();

    switch (action) {
    case PUT:
      handlePutDeleteDirEvent(updatedKeyInfo, orphanKeysMetaDataMap,
          NODESTATUS.ORPHAN_PARENT_NODE_UPDATE_STATUS_IN_PROGRESS
              .getValue());
      break;
    case DELETE:
      handleDeleteEvent(updatedKeyInfo.getObjectID(),
          updatedKeyInfo.getParentObjectID(),
          updatedKeyInfo.getKeyName(), orphanKeysMetaDataMap,
          NODESTATUS.ORPHAN_PARENT_NODE_UPDATE_STATUS_IN_PROGRESS
              .getValue());
      break;
    case UPDATE:
      break;
    default:
      LOG.debug("Skipping DB update event : {}",
          omdbUpdateEvent.getAction());
    }
  }

  private void processBucketTableEvent(
      Map<Long, OrphanKeyMetaData> orphanKeysMetaDataMap,
      OMDBUpdateEvent<String, ? extends WithObjectID> omdbUpdateEvent,
      OMDBUpdateEvent.OMDBUpdateAction action)
      throws IOException {
    // key update on Bucket Table
    OMDBUpdateEvent<String, OmBucketInfo> bucketTableUpdateEvent =
        (OMDBUpdateEvent<String, OmBucketInfo>) omdbUpdateEvent;
    OmBucketInfo updatedBucketInfo = bucketTableUpdateEvent.getValue();

    switch (action) {
    case PUT:
    case UPDATE:
      break;
    case DELETE:
      String dbVolumeKey = reconOMMetadataManager.getVolumeKey(
          updatedBucketInfo.getVolumeName());
      OmVolumeArgs volumeArgs =
          reconOMMetadataManager.getVolumeTable().get(dbVolumeKey);
      String keyName =
          StringUtils.join(OzoneConsts.OM_KEY_PREFIX,
              volumeArgs.getObjectID(),
              OzoneConsts.OM_KEY_PREFIX, updatedBucketInfo.getObjectID());
      handleDeleteEvent(updatedBucketInfo.getObjectID(),
          volumeArgs.getObjectID(), keyName, orphanKeysMetaDataMap,
          NODESTATUS.ORPHAN_PARENT_NODE_UPDATE_STATUS_IN_PROGRESS
              .getValue());
      break;
    default:
      LOG.debug("Skipping DB update event : {}",
          omdbUpdateEvent.getAction());
    }
  }

  public boolean reprocessWithFSO(OMMetadataManager omMetadataManager) {
    Map<Long, NSSummary> nsSummaryMap = new HashMap<>();
    Map<Long, OrphanKeyMetaData> orphanKeyMetaDataMap = new HashMap<>();

    try {
      if (!handleDirectoryTableEvents(omMetadataManager, nsSummaryMap,
          orphanKeyMetaDataMap)) {
        return false;
      }

      if (!handleFileTableEvents(omMetadataManager, nsSummaryMap,
          orphanKeyMetaDataMap)) {
        return false;
      }

      if (!writeFlushAndCommitOrphanKeysMetaDataToDB(orphanKeyMetaDataMap)) {
        return false;
      }

      Set<Long> bucketObjIdSet = new HashSet<>();
      buildBucketObjIdSet(omMetadataManager, bucketObjIdSet);

      List<Long> orphanMetaDataKeyList = new ArrayList<>();
      LOG.info("Starting to verify orphan key candidates...");
      // If any deleted directory is present as parent key in
      // orphanKeysMetaDataTable, then remove that parent directory entry
      // from orphanKeysMetaDataTable, because child keys for deleted directory
      // will not be treated as orphans.
      Instant start = Instant.now();
      if (!removeDeletedDirEntries(omMetadataManager, orphanMetaDataKeyList)) {
        return false;
      }
      if (!batchDeleteAndCommitOrphanKeysMetaDataToDB(orphanMetaDataKeyList)) {
        return false;
      }
      // Verify if child keys for left out parents are really orphans or
      // their parents are bucket objects.
      if (!verifyOrphanParentsForBucket(bucketObjIdSet,
          orphanMetaDataKeyList)) {
        return false;
      }
      if (!batchDeleteAndCommitOrphanKeysMetaDataToDB(orphanMetaDataKeyList)) {
        return false;
      }
      LOG.info("Completed verification of orphan key candidates.");
      Instant end = Instant.now();
      long duration = Duration.between(start, end).toMillis();
      LOG.info("It took me {} seconds to complete verification of orphan keys.",
          (double) duration / 1000.0);

    } catch (IOException ioEx) {
      LOG.error("Unable to reprocess Namespace Summary data in Recon DB. ",
              ioEx);
      return false;
    }
    // flush and commit left out keys at end
    if (!flushAndCommitNSToDB(nsSummaryMap)) {
      return false;
    }
    LOG.info("Completed a reprocess run of NSSummaryTaskWithFSO");
    return true;
  }

  private static void buildBucketObjIdSet(OMMetadataManager omMetadataManager,
                                Set<Long> bucketObjIdSet) throws IOException {
    Table<String, OmBucketInfo> bucketTable =
        omMetadataManager.getBucketTable();
    try (
        TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>>
            bucketTableIter = bucketTable.iterator()) {
      while (bucketTableIter.hasNext()) {
        Table.KeyValue<String, OmBucketInfo> bucketInfoKeyValue
            = bucketTableIter.next();
        OmBucketInfo bucketInfo = bucketInfoKeyValue.getValue();
        bucketObjIdSet.add(bucketInfo.getObjectID());
      }
    }
  }

  private boolean removeDeletedDirEntries(OMMetadataManager omMetadataManager,
                            List<Long> orphanMetaDataKeyList)
      throws IOException {
    Table<String, OmKeyInfo> omKeyInfoTable =
        omMetadataManager.getDeletedDirTable();
    try (
        TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
            keyIter = omKeyInfoTable.iterator()) {
      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
        OmKeyInfo omKeyInfo = kv.getValue();
        orphanMetaDataKeyList.add(omKeyInfo.getObjectID());
        if (!checkOrphanDataThresholdAndAddToDeleteBatch(
            orphanMetaDataKeyList)) {
          return false;
        }
      }
      return true;
    }
  }

  private boolean handleDirectoryTableEvents(
      OMMetadataManager omMetadataManager,
      Map<Long, NSSummary> nsSummaryMap,
      Map<Long, OrphanKeyMetaData> orphanKeysMetaDataMap)
      throws IOException {
    Table<String, OmDirectoryInfo> dirTable =
        omMetadataManager.getDirectoryTable();
    try (TableIterator<String,
        ? extends Table.KeyValue<String, OmDirectoryInfo>>
             dirTableIter = dirTable.iterator()) {
      while (dirTableIter.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> kv = dirTableIter.next();
        OmDirectoryInfo directoryInfo = kv.getValue();
        handlePutDirEvent(directoryInfo, nsSummaryMap, orphanKeysMetaDataMap,
            NODESTATUS.ORPHAN_PARENT_NODE_UPDATE_STATUS_IN_PROGRESS.getValue());
        if (!checkAndCallFlushToDB(nsSummaryMap)) {
          return false;
        }
        if (!checkOrphanDataAndCallWriteFlushToDB(orphanKeysMetaDataMap)) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean handleFileTableEvents(
      OMMetadataManager omMetadataManager,
      Map<Long, NSSummary> nsSummaryMap,
      Map<Long, OrphanKeyMetaData> orphanKeysMetaDataSetMap)
      throws IOException {
    // Get fileTable used by FSO
    Table<String, OmKeyInfo> keyTable =
        omMetadataManager.getFileTable();

    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
             keyTableIter = keyTable.iterator()) {
      while (keyTableIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyTableIter.next();
        OmKeyInfo keyInfo = kv.getValue();
        handlePutKeyEvent(keyInfo, nsSummaryMap, orphanKeysMetaDataSetMap,
            NODESTATUS.ORPHAN_PARENT_NODE_UPDATE_STATUS_IN_PROGRESS.getValue());
        if (!checkAndCallFlushToDB(nsSummaryMap)) {
          return false;
        }
        if (!checkOrphanDataAndCallWriteFlushToDB(orphanKeysMetaDataSetMap)) {
          return false;
        }
      }
    }
    return true;
  }

}
