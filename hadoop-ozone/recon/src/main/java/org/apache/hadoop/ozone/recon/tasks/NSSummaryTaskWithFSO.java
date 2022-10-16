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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;

/**
 * Class for handling FSO specific tasks.
 */
public class NSSummaryTaskWithFSO extends NSSummaryTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(NSSummaryTaskWithFSO.class);

  @Inject
  public NSSummaryTaskWithFSO(ReconNamespaceSummaryManager
                            reconNamespaceSummaryManager) {
    super(reconNamespaceSummaryManager);
  }

  @Override
  public String getTaskName() {
    return "NSSummaryTaskWithFSO";
  }

  // We only listen to updates from FSO-enabled KeyTable(FileTable) and DirTable
  public Collection<String> getTaskTables() {
    return Arrays.asList(FILE_TABLE, DIRECTORY_TABLE);
  }

  @Override
  public Pair<String, Boolean> process(OMUpdateEventBatch events) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    final Collection<String> taskTables = getTaskTables();
    Map<Long, NSSummary> nsSummaryMap = new HashMap<>();

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, ? extends
              WithParentObjectId> omdbUpdateEvent = eventIterator.next();
      OMDBUpdateEvent.OMDBUpdateAction action = omdbUpdateEvent.getAction();

      // we only process updates on OM's FileTable and Dirtable
      String table = omdbUpdateEvent.getTable();
      boolean updateOnFileTable = table.equals(FILE_TABLE);
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
        return new ImmutablePair<>(getTaskName(), false);
      }
    }

    try {
      writeNSSummariesToDB(nsSummaryMap);
    } catch (IOException e) {
      LOG.error("Unable to write Namespace Summary data in Recon DB.", e);
      return new ImmutablePair<>(getTaskName(), false);
    }

    LOG.info("Completed a process run of NSSummaryTaskWithFSO");
    return new ImmutablePair<>(getTaskName(), true);
  }

  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    Map<Long, NSSummary> nsSummaryMap = new HashMap<>();

    try {
      // reinit Recon RocksDB's namespace CF.
      getReconNamespaceSummaryManager().clearNSSummaryTable();

      Table<String, OmDirectoryInfo> dirTable =
          omMetadataManager.getDirectoryTable();
      try (TableIterator<String,
              ? extends Table.KeyValue<String, OmDirectoryInfo>>
                dirTableIter = dirTable.iterator()) {
        while (dirTableIter.hasNext()) {
          Table.KeyValue<String, OmDirectoryInfo> kv = dirTableIter.next();
          OmDirectoryInfo directoryInfo = kv.getValue();
          handlePutDirEvent(directoryInfo, nsSummaryMap);
        }
      }

      // Get fileTable used by FSO
      Table<String, OmKeyInfo> keyTable = omMetadataManager.getFileTable();

      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
              keyTableIter = keyTable.iterator()) {
        while (keyTableIter.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> kv = keyTableIter.next();
          OmKeyInfo keyInfo = kv.getValue();
          handlePutKeyEvent(keyInfo, nsSummaryMap);
        }
      }

    } catch (IOException ioEx) {
      LOG.error("Unable to reprocess Namespace Summary data in Recon DB. ",
              ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }

    try {
      writeNSSummariesToDB(nsSummaryMap);
    } catch (IOException e) {
      LOG.error("Unable to write Namespace Summary data in Recon DB.", e);
      return new ImmutablePair<>(getTaskName(), false);
    }
    LOG.info("Completed a reprocess run of NSSummaryTaskWithFSO");
    return new ImmutablePair<>(getTaskName(), true);
  }
}
