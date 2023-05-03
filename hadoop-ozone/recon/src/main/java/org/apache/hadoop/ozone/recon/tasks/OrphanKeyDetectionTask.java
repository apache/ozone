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
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.recon.api.types.OrphanKeysMetaDataSet;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition.ORPHAN_KEYS_METADATA;

/**
 * Task class to iterate over the OM DB and detect the orphan keys metadata.
 */
public class OrphanKeyDetectionTask implements ReconOmTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(OrphanKeyDetectionTask.class);
  private DBStore reconDbStore;
  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private final Table<Long, OrphanKeysMetaDataSet> orphanKeysMetaDataTable;

  @Inject
  public OrphanKeyDetectionTask(
      ReconDBProvider reconDBProvider,
      ReconNamespaceSummaryManager reconNamespaceSummaryManager)
      throws IOException {
    this.reconDbStore = reconDBProvider.getDbStore();
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.orphanKeysMetaDataTable =
        ORPHAN_KEYS_METADATA.getTable(reconDbStore);
  }

  @Override
  public String getTaskName() {
    return "OrphanKeyDetectionTask";
  }

  public Collection<String> getTaskTables() {
    List<String> taskTables = new ArrayList<>();
    taskTables.add(DELETED_DIR_TABLE);
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
          WithParentObjectId> omdbUpdateEvent = eventIterator.next();
      OMDBUpdateEvent.OMDBUpdateAction action = omdbUpdateEvent.getAction();
      // we only process updates on OM's deletedDirectoryTable
      String table = omdbUpdateEvent.getTable();
      if (!taskTables.contains(table)) {
        continue;
      }

      try {
        // key update on deletedDirectoryTable
        OMDBUpdateEvent<String, OmKeyInfo> keyTableUpdateEvent =
            (OMDBUpdateEvent<String, OmKeyInfo>) omdbUpdateEvent;
        OmKeyInfo updatedKeyInfo = keyTableUpdateEvent.getValue();

        switch (action) {
        case PUT:
          handlePutDeleteDirEvent(updatedKeyInfo);
          break;

        case DELETE:
          break;

        case UPDATE:
          break;

        default:
          LOG.debug("Skipping DB update event : {}",
              omdbUpdateEvent.getAction());
        }
      } catch (Exception ex) {
        LOG.error("Unable to process Namespace Summary data in Recon DB. ", ex);
        return new ImmutablePair<>(getTaskName(), false);
      }
    }
    return new ImmutablePair<>(getTaskName(), true);
  }

  private void handlePutDeleteDirEvent(OmKeyInfo updatedKeyInfo) {
    long objectID = updatedKeyInfo.getObjectID();
    try {
      OrphanKeysMetaDataSet orphanKeysMetaDataSet =
          orphanKeysMetaDataTable.get(objectID);
      if (null != orphanKeysMetaDataSet) {
        orphanKeysMetaDataTable.delete(objectID);
      }
    } catch (IOException e) {
      // Logging as Info as we don't want to log as error when any dir not
      // found in orphan candidate metadata set. This is done to avoid 2
      // rocks DB operations - check if present and then delete operation.
      LOG.info("Deleted Dir objectId {} may not be found in orphan map.",
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

}
