/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_DIR_TABLE;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for handling FSO specific tasks.
 */
public class NSSummaryTaskWithFSO extends NSSummaryTaskDbEventHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(NSSummaryTaskWithFSO.class);

  private final long nsSummaryFlushToDBMaxThreshold;

  public NSSummaryTaskWithFSO(ReconNamespaceSummaryManager
                              reconNamespaceSummaryManager,
                              ReconOMMetadataManager
                              reconOMMetadataManager,
                              long nsSummaryFlushToDBMaxThreshold) {
    super(reconNamespaceSummaryManager,
        reconOMMetadataManager);
    this.nsSummaryFlushToDBMaxThreshold = nsSummaryFlushToDBMaxThreshold;
  }

  // We only listen to updates from FSO-enabled KeyTable(FileTable), DirTable, DeletedTable, and DeletedDirTable
  public Collection<String> getTaskTables() {
    return Arrays.asList(FILE_TABLE, DIRECTORY_TABLE, DELETED_TABLE, DELETED_DIR_TABLE);
  }

  public Pair<Integer, Boolean> processWithFSO(OMUpdateEventBatch events,
                                               int seekPos) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    int itrPos = 0;
    while (eventIterator.hasNext() && itrPos < seekPos) {
      eventIterator.next();
      itrPos++;
    }
    final Collection<String> taskTables = getTaskTables();
    Map<Long, NSSummary> nsSummaryMap = new HashMap<>();
    int eventCounter = 0;

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, ? extends
              WithParentObjectId> omdbUpdateEvent = eventIterator.next();
      OMDBUpdateEvent.OMDBUpdateAction action = omdbUpdateEvent.getAction();
      eventCounter++;

      // we only process updates on OM's FileTable, DirTable, DeletedTable, and DeletedDirTable
      String table = omdbUpdateEvent.getTable();
      boolean updateOnFileTable = table.equals(FILE_TABLE);
      boolean updateOnDeletedTable = table.equals(DELETED_TABLE);
      boolean updateOnDeletedDirTable = table.equals(DELETED_DIR_TABLE);
      if (!taskTables.contains(table)) {
        continue;
      }

      String updatedKey = omdbUpdateEvent.getKey();

      try {
        if (updateOnFileTable) {
          // key update on fileTable
          OMDBUpdateEvent<String, OmKeyInfo> keyTableUpdateEvent =
                  (OMDBUpdateEvent<String, OmKeyInfo>) omdbUpdateEvent;
          OmKeyInfo updatedKeyInfo = keyTableUpdateEvent.getValue();
          OmKeyInfo oldKeyInfo = keyTableUpdateEvent.getOldValue();

          switch (action) {
          case PUT:
            handlePutKeyEvent(updatedKeyInfo, nsSummaryMap);
            break;

          case DELETE:
            handleDeleteKeyEvent(updatedKeyInfo, nsSummaryMap);
            break;

          case UPDATE:
            if (oldKeyInfo != null) {
              // delete first, then put
              handleDeleteKeyEvent(oldKeyInfo, nsSummaryMap);
            } else {
              LOG.warn("Update event does not have the old keyInfo for {}.",
                      updatedKey);
            }
            handlePutKeyEvent(updatedKeyInfo, nsSummaryMap);
            break;

          default:
            LOG.debug("Skipping DB update event : {}",
                    omdbUpdateEvent.getAction());
          }

        } else if (updateOnDeletedTable) {
          // deleted table update
          Object value = omdbUpdateEvent.getValue();
          if (!(value instanceof RepeatedOmKeyInfo)) {
            LOG.warn("Unexpected value type {} for key {}. Skipping processing.",
                value.getClass().getName(), updatedKey);
            continue;
          }
          RepeatedOmKeyInfo repeatedOmKeyInfo = (RepeatedOmKeyInfo) value;
          Object oldValue = omdbUpdateEvent.getOldValue();
          RepeatedOmKeyInfo oldRepeatedOmKeyInfo = oldValue instanceof RepeatedOmKeyInfo ? 
              (RepeatedOmKeyInfo) oldValue : null;

          switch (action) {
          case PUT:
            handlePutDeletedKeyEvent(repeatedOmKeyInfo, nsSummaryMap);
            break;

          case DELETE:
            handleDeleteDeletedKeyEvent(repeatedOmKeyInfo, nsSummaryMap);
            break;

          case UPDATE:
            // For deleted table, treat UPDATE as DELETE + PUT
            if (oldRepeatedOmKeyInfo != null) {
              handleDeleteDeletedKeyEvent(oldRepeatedOmKeyInfo, nsSummaryMap);
            }
            handlePutDeletedKeyEvent(repeatedOmKeyInfo, nsSummaryMap);
            break;

          default:
            LOG.debug("Skipping DB update event : {}",
                    omdbUpdateEvent.getAction());
          }

        } else if (updateOnDeletedDirTable) {
          // deleted directory table update
          Object value = omdbUpdateEvent.getValue();
          if (!(value instanceof OmKeyInfo)) {
            LOG.warn("Unexpected value type {} for key {}. Skipping processing.",
                value.getClass().getName(), updatedKey);
            continue;
          }
          OmKeyInfo deletedDirInfo = (OmKeyInfo) value;
          Object oldValue = omdbUpdateEvent.getOldValue();
          OmKeyInfo oldDeletedDirInfo = oldValue instanceof OmKeyInfo ? 
              (OmKeyInfo) oldValue : null;

          switch (action) {
          case PUT:
            handlePutDeletedDirEvent(deletedDirInfo, nsSummaryMap);
            break;

          case DELETE:
            handleDeleteDeletedDirEvent(deletedDirInfo, nsSummaryMap);
            break;

          case UPDATE:
            if (oldDeletedDirInfo != null) {
              // delete first, then put
              handleDeleteDeletedDirEvent(oldDeletedDirInfo, nsSummaryMap);
            } else {
              LOG.warn("Update event does not have the old deleted dir info for {}.",
                      updatedKey);
            }
            handlePutDeletedDirEvent(deletedDirInfo, nsSummaryMap);
            break;

          default:
            LOG.debug("Skipping DB update event : {}",
                    omdbUpdateEvent.getAction());
          }

        } else {
          // directory update on DirTable
          OMDBUpdateEvent<String, OmDirectoryInfo> dirTableUpdateEvent =
                  (OMDBUpdateEvent<String, OmDirectoryInfo>) omdbUpdateEvent;
          OmDirectoryInfo updatedDirectoryInfo = dirTableUpdateEvent.getValue();
          OmDirectoryInfo oldDirectoryInfo = dirTableUpdateEvent.getOldValue();

          switch (action) {
          case PUT:
            handlePutDirEvent(updatedDirectoryInfo, nsSummaryMap);
            break;

          case DELETE:
            handleDeleteDirEvent(updatedDirectoryInfo, nsSummaryMap);
            break;

          case UPDATE:
            if (oldDirectoryInfo != null) {
              // delete first, then put
              handleDeleteDirEvent(oldDirectoryInfo, nsSummaryMap);
            } else {
              LOG.warn("Update event does not have the old dirInfo for {}.",
                      updatedKey);
            }
            handlePutDirEvent(updatedDirectoryInfo, nsSummaryMap);
            break;

          default:
            LOG.debug("Skipping DB update event : {}",
                    omdbUpdateEvent.getAction());
          }
        }
      } catch (IOException ioEx) {
        LOG.error("Unable to process Namespace Summary data in Recon DB. ",
                ioEx);
        nsSummaryMap.clear();
        return new ImmutablePair<>(seekPos, false);
      }
      if (nsSummaryMap.size() >= nsSummaryFlushToDBMaxThreshold) {
        if (!flushAndCommitNSToDB(nsSummaryMap)) {
          return new ImmutablePair<>(seekPos, false);
        }
        seekPos = eventCounter + 1;
      }
    }

    // flush and commit left out entries at end
    if (!flushAndCommitNSToDB(nsSummaryMap)) {
      return new ImmutablePair<>(seekPos, false);
    }
    LOG.debug("Completed a process run of NSSummaryTaskWithFSO");
    return new ImmutablePair<>(seekPos, true);
  }

  public boolean reprocessWithFSO(OMMetadataManager omMetadataManager) {
    Map<Long, NSSummary> nsSummaryMap = new HashMap<>();

    try {
      // First process directory table to establish the directory structure
      Table<String, OmDirectoryInfo> dirTable =
          omMetadataManager.getDirectoryTable();
      try (TableIterator<String,
              ? extends Table.KeyValue<String, OmDirectoryInfo>>
                dirTableIter = dirTable.iterator()) {
        while (dirTableIter.hasNext()) {
          Table.KeyValue<String, OmDirectoryInfo> kv = dirTableIter.next();
          OmDirectoryInfo directoryInfo = kv.getValue();
          handlePutDirEvent(directoryInfo, nsSummaryMap);
          if (nsSummaryMap.size() >= nsSummaryFlushToDBMaxThreshold) {
            if (!flushAndCommitNSToDB(nsSummaryMap)) {
              return false;
            }
          }
        }
      }

      // Process fileTable used by FSO
      Table<String, OmKeyInfo> keyTable =
          omMetadataManager.getFileTable();

      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
              keyTableIter = keyTable.iterator()) {
        while (keyTableIter.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> kv = keyTableIter.next();
          OmKeyInfo keyInfo = kv.getValue();
          handlePutKeyEvent(keyInfo, nsSummaryMap);
          if (nsSummaryMap.size() >= nsSummaryFlushToDBMaxThreshold) {
            if (!flushAndCommitNSToDB(nsSummaryMap)) {
              return false;
            }
          }
        }
      }

      // Process deleted table
      Table<String, RepeatedOmKeyInfo> deletedTable =
          omMetadataManager.getDeletedTable();

      try (TableIterator<String, ? extends Table.KeyValue<String, RepeatedOmKeyInfo>>
              deletedTableIter = deletedTable.iterator()) {
        while (deletedTableIter.hasNext()) {
          Table.KeyValue<String, RepeatedOmKeyInfo> kv = deletedTableIter.next();
          RepeatedOmKeyInfo repeatedOmKeyInfo = kv.getValue();
          handlePutDeletedKeyEvent(repeatedOmKeyInfo, nsSummaryMap);
          if (nsSummaryMap.size() >= nsSummaryFlushToDBMaxThreshold) {
            if (!flushAndCommitNSToDB(nsSummaryMap)) {
              return false;
            }
          }
        }
      }

      // Process deleted directory table
      Table<String, OmKeyInfo> deletedDirTable =
          omMetadataManager.getDeletedDirTable();

      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
              deletedDirTableIter = deletedDirTable.iterator()) {
        while (deletedDirTableIter.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> kv = deletedDirTableIter.next();
          OmKeyInfo deletedDirInfo = kv.getValue();
          handlePutDeletedDirEvent(deletedDirInfo, nsSummaryMap);
          if (nsSummaryMap.size() >= nsSummaryFlushToDBMaxThreshold) {
            if (!flushAndCommitNSToDB(nsSummaryMap)) {
              return false;
            }
          }
        }
      }

    } catch (IOException ioEx) {
      LOG.error("Unable to reprocess Namespace Summary data in Recon DB. ",
              ioEx);
      return false;
    }
    // flush and commit left out keys at end
    if (!flushAndCommitNSToDB(nsSummaryMap)) {
      return false;
    }
    LOG.debug("Completed a reprocess run of NSSummaryTaskWithFSO");
    return true;
  }
}
