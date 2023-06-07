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
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithObjectID;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.OrphanKeyMetaData;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition.ORPHAN_KEYS_METADATA;

/**
 * Task class to iterate over the OM DB and management of orphan_keys_metadata
 * table data.
 */
public class OrphanMetaDataManagementTask implements ReconOmTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(OrphanMetaDataManagementTask.class);
  private final DBStore reconDbStore;
  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private final ReconOMMetadataManager reconOMMetadataManager;
  private final Table<Long, OrphanKeyMetaData> orphanKeysMetaDataTable;

  @Inject
  public OrphanMetaDataManagementTask(
      ReconDBProvider reconDBProvider,
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager reconOMMetadataManager)
      throws IOException {
    this.reconDbStore = reconDBProvider.getDbStore();
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.reconOMMetadataManager = reconOMMetadataManager;
    this.orphanKeysMetaDataTable =
        ORPHAN_KEYS_METADATA.getTable(reconDbStore);
  }

  @Override
  public String getTaskName() {
    return "OrphanMetaDataManagementTask";
  }

  public Collection<String> getTaskTables() {
    List<String> taskTables = new ArrayList<>();
    taskTables.add(DELETED_DIR_TABLE);
    taskTables.add(BUCKET_TABLE);
    return taskTables;
  }

  /**
   * Process a set of OM events on tables that the task is listening on.
   *
   * @param events Set of events to be processed by the task.
   * @return Pair of task name -> task success.
   */
  @Override
  public Pair<String, Boolean> process(OMUpdateEventBatch events) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    final Collection<String> taskTables = getTaskTables();
    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, ? extends
          WithObjectID> omdbUpdateEvent = eventIterator.next();
      OMDBUpdateEvent.OMDBUpdateAction action = omdbUpdateEvent.getAction();
      // we only process updates on OM's deletedDirectoryTable
      String table = omdbUpdateEvent.getTable();
      if (!taskTables.contains(table)) {
        continue;
      }

      try {
        if (table.equals(DELETED_DIR_TABLE)) {
          // key update on deletedDirectoryTable
          OMDBUpdateEvent<String, OmKeyInfo> deletedDirTableUpdateEvent =
              (OMDBUpdateEvent<String, OmKeyInfo>) omdbUpdateEvent;
          OmKeyInfo updatedKeyInfo = deletedDirTableUpdateEvent.getValue();

          switch (action) {
          case PUT:
            handlePutDeleteDirEvent(updatedKeyInfo);
            break;
          case DELETE:
            handleDeleteDirEvent(updatedKeyInfo);
          case UPDATE:
            break;
          default:
            LOG.debug("Skipping DB update event : {}",
                omdbUpdateEvent.getAction());
          }
        }
        if (table.equals(BUCKET_TABLE)) {
          // key update on Bucket Table
          OMDBUpdateEvent<String, OmBucketInfo> bucketTableUpdateEvent =
              (OMDBUpdateEvent<String, OmBucketInfo>) omdbUpdateEvent;
          OmBucketInfo updatedBucketInfo = bucketTableUpdateEvent.getValue();

          switch (action) {
          case PUT:
          case UPDATE:
            break;
          case DELETE:
            handleBucketDeleteEvent(updatedBucketInfo);
            break;
          default:
            LOG.debug("Skipping DB update event : {}",
                omdbUpdateEvent.getAction());
          }
        }
      } catch (Exception ex) {
        LOG.error("Unable to process Namespace Summary data in Recon DB. ", ex);
        return new ImmutablePair<>(getTaskName(), false);
      }
    }
    return new ImmutablePair<>(getTaskName(), true);
  }

  private void handleBucketDeleteEvent(OmBucketInfo updatedBucketInfo) {
    long objectID = updatedBucketInfo.getObjectID();
    try {
      NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectID);
      if (null != nsSummary) {
        if (nsSummary.getChildDir().size() == 0 &&
            nsSummary.getNumOfFiles() == 0) {
          removeOrphanMetaData(objectID);
        }
      }
    } catch (IOException e) {
      // Logging as Info as we don't want to log as error when any dir not
      // found in orphan candidate metadata set. This is done to avoid 2
      // rocks DB operations - check if present and then delete operation.
      LOG.info("ObjectId {} may not be found in orphan metadata table.",
          objectID);
    }
  }

  private void removeOrphanMetaData(long objectID) {
    try {
      OrphanKeyMetaData orphanKeyMetaData =
          orphanKeysMetaDataTable.get(objectID);
      if (null != orphanKeyMetaData) {
        orphanKeysMetaDataTable.delete(objectID);
      }
    } catch (IOException e) {
      // Logging as Info as we don't want to log as error when any dir not
      // found in orphan candidate metadata set. This is done to avoid 2
      // rocks DB operations - check if present and then delete operation.
      LOG.info("ObjectId {} may not be found in orphan metadata table.",
          objectID);
    }
  }

  private void handlePutDeleteDirEvent(OmKeyInfo updatedKeyInfo) {
    long objectID = updatedKeyInfo.getObjectID();
    removeOrphanMetaData(objectID);
  }

  private void handleDeleteDirEvent(OmKeyInfo updatedKeyInfo) {
    long objectID = updatedKeyInfo.getObjectID();
    try {
      NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectID);
      if (null != nsSummary) {
        if (nsSummary.getChildDir().size() != 0 ||
            nsSummary.getNumOfFiles() != 0) {
          addToOrphanMetaData(updatedKeyInfo, nsSummary);
        }
      }
    } catch (IOException e) {
      // Logging as Info as we don't want to log as error when any dir not
      // found in orphan candidate metadata set. This is done to avoid 2
      // rocks DB operations - check if present and then delete operation.
      LOG.info("ObjectId {} may not be found in orphan metadata table or " +
          "namespaceSummaryTable.", objectID);
    }
  }

  private void addToOrphanMetaData(OmKeyInfo updatedKeyInfo,
                                   NSSummary nsSummary) {
    long objectID = updatedKeyInfo.getObjectID();
    long parentObjectID = updatedKeyInfo.getParentObjectID();
    String delDirName = updatedKeyInfo.getKeyName();
    try {
      OrphanKeyMetaData orphanKeyMetaData =
          reconNamespaceSummaryManager.getOrphanKeyMetaData(parentObjectID);
      if (null != orphanKeyMetaData) {
        Set<Long> objectIds = orphanKeyMetaData.getObjectIds();
        objectIds.remove(objectID);
      } else {
        final String[] keys = delDirName.split(OM_KEY_PREFIX);
        final long volumeId = Long.parseLong(keys[1]);
        final long bucketId = Long.parseLong(keys[2]);
        List<OmKeyInfo> pendingDeletionSubFiles =
            getPendingDeletionSubFiles(volumeId, bucketId, updatedKeyInfo);
        Set<Long> deletedFileObjectIds = pendingDeletionSubFiles.stream()
            .map(deletedFiles -> deletedFiles.getObjectID()).collect(
                Collectors.toSet());
        Set<Long> objectIds = new HashSet<>();
        objectIds.addAll(nsSummary.getChildDir());
        objectIds.addAll(deletedFileObjectIds);
        orphanKeyMetaData =
            new OrphanKeyMetaData(objectIds, 1L);
        orphanKeysMetaDataTable.put(objectID, orphanKeyMetaData);
      }
    } catch (IOException e) {
      // Logging as Info as we don't want to log as error when any dir not
      // found in orphan candidate metadata set. This is done to avoid 2
      // rocks DB operations - check if present and then delete operation.
      LOG.info("ObjectId {} may not be found in orphan metadata table.",
          objectID);
    }
  }

  /**
   * Process events on tables that the task is listening on.
   *
   * @param omMetadataManager OM Metadata manager instance.
   * @return Pair of task name -> task success.
   */
  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    return new ImmutablePair<>(getTaskName(), true);
  }

  private List<OmKeyInfo> getPendingDeletionSubFiles(
      long volumeId, long bucketId, OmKeyInfo parentInfo)
      throws IOException {
    List<OmKeyInfo> files = new ArrayList<>();
    String seekFileInDB =
        reconOMMetadataManager.getOzonePathKey(volumeId, bucketId,
            parentInfo.getObjectID(), "");

    Table fileTable = reconOMMetadataManager.getFileTable();
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
             iterator = fileTable.iterator()) {

      iterator.seek(seekFileInDB);

      while (iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> entry = iterator.next();
        OmKeyInfo fileInfo = entry.getValue();
        if (!OMFileRequest.isImmediateChild(fileInfo.getParentObjectID(),
            parentInfo.getObjectID())) {
          break;
        }
        fileInfo.setFileName(fileInfo.getKeyName());
        String fullKeyPath = OMFileRequest.getAbsolutePath(
            parentInfo.getKeyName(), fileInfo.getKeyName());
        fileInfo.setKeyName(fullKeyPath);

        files.add(fileInfo);
      }
    }

    return files;
  }
}
