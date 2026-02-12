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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.tasks.util.ParallelTableIteratorOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for handling FSO specific tasks.
 */
public class NSSummaryTaskWithFSO extends NSSummaryTaskDbEventHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(NSSummaryTaskWithFSO.class);

  private final long nsSummaryFlushToDBMaxThreshold;
  private final int maxIterators;
  private final int maxWorkers;
  private final int maxKeysInMemory;

  public NSSummaryTaskWithFSO(ReconNamespaceSummaryManager
                              reconNamespaceSummaryManager,
                              ReconOMMetadataManager
                              reconOMMetadataManager,
                              long nsSummaryFlushToDBMaxThreshold,
                              int maxIterators,
                              int maxWorkers,
                              int maxKeysInMemory) {
    super(reconNamespaceSummaryManager,
        reconOMMetadataManager);
    this.nsSummaryFlushToDBMaxThreshold = nsSummaryFlushToDBMaxThreshold;
    this.maxIterators = maxIterators;
    this.maxWorkers = maxWorkers;
    this.maxKeysInMemory = maxKeysInMemory;
  }

  // We listen to updates from FSO-enabled FileTable, DirTable, DeletedTable and DeletedDirTable
  public Collection<String> getTaskTables() {
    return Arrays.asList(FILE_TABLE, DIRECTORY_TABLE, DELETED_DIR_TABLE);
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
    final Collection<Long> objectIdsToBeDeleted = Collections.synchronizedList(new ArrayList<>());
    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, ? extends
          WithParentObjectId> omdbUpdateEvent = eventIterator.next();
      OMDBUpdateEvent.OMDBUpdateAction action = omdbUpdateEvent.getAction();
      eventCounter++;

      // we process updates on OM's FileTable, DirTable, DeletedTable and DeletedDirTable
      String table = omdbUpdateEvent.getTable();
      if (!taskTables.contains(table)) {
        continue;
      }

      try {
        if (table.equals(FILE_TABLE)) {
          handleUpdateOnFileTable(omdbUpdateEvent, action, nsSummaryMap);

        } else if (table.equals(DELETED_DIR_TABLE)) {
          // Hard delete from deletedDirectoryTable - cleanup memory leak for directories
          handleUpdateOnDeletedDirTable(omdbUpdateEvent, action, nsSummaryMap, objectIdsToBeDeleted);
        } else {
          // directory update on DirTable
          handleUpdateOnDirTable(omdbUpdateEvent, action, nsSummaryMap);
        }
      } catch (IOException ioEx) {
        LOG.error("Unable to process Namespace Summary data in Recon DB. ",
            ioEx);
        nsSummaryMap.clear();
        return new ImmutablePair<>(seekPos, false);
      }
      if (nsSummaryMap.size() >= nsSummaryFlushToDBMaxThreshold) {
        // Deleting hard deleted directories also along with this flush operation from NSSummary table
        // Same list of objectIdsToBeDeleted is used for follow up flush operation as well and done intentionally
        // to make sure that after final flush all objectIds are deleted from NSSummary table.
        if (!flushAndCommitUpdatedNSToDB(nsSummaryMap, objectIdsToBeDeleted)) {
          return new ImmutablePair<>(seekPos, false);
        }
        seekPos = eventCounter + 1;
      }
    }
    // flush and commit left out entries at end.
    // Deleting hard deleted directories also along with this flush operation from NSSummary table
    // Same list of objectIdsToBeDeleted is used this final flush operation as well and done intentionally
    // to make sure that after final flush all objectIds are deleted from NSSummary table.
    if (!flushAndCommitUpdatedNSToDB(nsSummaryMap, objectIdsToBeDeleted)) {
      return new ImmutablePair<>(seekPos, false);
    }

    LOG.debug("Completed a process run of NSSummaryTaskWithFSO");
    return new ImmutablePair<>(seekPos, true);
  }

  private void handleUpdateOnDirTable(OMDBUpdateEvent<String, ? extends WithParentObjectId> omdbUpdateEvent,
                         OMDBUpdateEvent.OMDBUpdateAction action, Map<Long, NSSummary> nsSummaryMap)
      throws IOException {
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
        LOG.warn("Update event does not have the old dirInfo for {}.", dirTableUpdateEvent.getKey());
      }
      handlePutDirEvent(updatedDirectoryInfo, nsSummaryMap);
      break;

    default:
      LOG.debug("Skipping DB update event : {}",
              omdbUpdateEvent.getAction());
    }
  }

  private void handleUpdateOnDeletedDirTable(OMDBUpdateEvent<String, ? extends WithParentObjectId>  omdbUpdateEvent,
                                             OMDBUpdateEvent.OMDBUpdateAction action, Map<Long, NSSummary> nsSummaryMap,
                                             Collection<Long> objectIdsToBeDeleted) {
    OMDBUpdateEvent<String, OmKeyInfo> deletedDirTableUpdateEvent =
        (OMDBUpdateEvent<String, OmKeyInfo>) omdbUpdateEvent;
    OmKeyInfo deletedKeyInfo = deletedDirTableUpdateEvent.getValue();

    switch (action) {
    case DELETE:
      // When entry is removed from deletedDirTable, remove from nsSummaryMap to prevent memory leak
      if (deletedKeyInfo != null) {
        long objectId = deletedKeyInfo.getObjectID();
        nsSummaryMap.remove(objectId);
        LOG.debug("Removed hard deleted directory with objectId {} from nsSummaryMap", objectId);
        objectIdsToBeDeleted.add(objectId);
      }
      break;

    default:
      LOG.debug("Skipping DB update event on deletedDirTable: {}", action);
    }
  }

  private void handleUpdateOnFileTable(OMDBUpdateEvent<String, ? extends WithParentObjectId> omdbUpdateEvent,
                         OMDBUpdateEvent.OMDBUpdateAction action, Map<Long, NSSummary> nsSummaryMap)
      throws IOException {
    // key update on fileTable
    OMDBUpdateEvent<String, OmKeyInfo> keyTableUpdateEvent =
            (OMDBUpdateEvent<String, OmKeyInfo>) omdbUpdateEvent;
    OmKeyInfo updatedKeyInfo = keyTableUpdateEvent.getValue();
    OmKeyInfo oldKeyInfo = keyTableUpdateEvent.getOldValue();

    switch (action) {
    case PUT:
      handlePutKeyEvent(updatedKeyInfo, nsSummaryMap, updatedKeyInfo.getParentObjectID());
      break;

    case DELETE:
      handleDeleteKeyEvent(updatedKeyInfo, nsSummaryMap, updatedKeyInfo.getParentObjectID());
      break;

    case UPDATE:
      if (oldKeyInfo != null) {
        // delete first, then put
        handleDeleteKeyEvent(oldKeyInfo, nsSummaryMap, oldKeyInfo.getParentObjectID());
      } else {
        LOG.warn("Update event does not have the old keyInfo for {}.", omdbUpdateEvent.getKey());
      }
      handlePutKeyEvent(updatedKeyInfo, nsSummaryMap, updatedKeyInfo.getParentObjectID());
      break;

    default:
      LOG.debug("Skipping DB update event : {}",
              omdbUpdateEvent.getAction());
    }
  }

  public boolean reprocessWithFSO(OMMetadataManager omMetadataManager) {
    // We run reprocess in two phases with separate flushers so that directory
    // skeletons are fully persisted before file updates rely on them.
    final int queueCapacity = maxWorkers * 10;

    try (NSSummaryAsyncFlusher dirFlusher =
             NSSummaryAsyncFlusher.create(getReconNamespaceSummaryManager(),
                 "NSSummaryTaskWithFSO-dir", queueCapacity)) {
      if (!processDirTableInParallel(omMetadataManager, dirFlusher)) {
        return false;
      }
    } catch (Exception ex) {
      LOG.error("Unable to reprocess Namespace Summary data in Recon DB (dir phase).", ex);
      return false;
    }

    try (NSSummaryAsyncFlusher fileFlusher =
             NSSummaryAsyncFlusher.create(getReconNamespaceSummaryManager(),
                 "NSSummaryTaskWithFSO-file", queueCapacity)) {
      if (!processFileTableInParallel(omMetadataManager, fileFlusher)) {
        return false;
      }
    } catch (Exception ex) {
      LOG.error("Unable to reprocess Namespace Summary data in Recon DB (file phase).", ex);
      return false;
    }

    LOG.info("Completed a reprocess run of NSSummaryTaskWithFSO");
    return true;
  }

  /**
   * Process dirTable in parallel using per-worker maps with async flushing.
   */
  private boolean processDirTableInParallel(OMMetadataManager omMetadataManager,
                                            NSSummaryAsyncFlusher asyncFlusher) {
    Table<String, OmDirectoryInfo> dirTable = omMetadataManager.getDirectoryTable();

    // Per-worker maps for lockless updates
    Map<Long, Map<Long, NSSummary>> allWorkerMaps = new ConcurrentHashMap<>();

    // Divide threshold by worker count
    final long perWorkerThreshold = Math.max(1, nsSummaryFlushToDBMaxThreshold / maxWorkers);

    Function<Table.KeyValue<String, OmDirectoryInfo>, Void> kvOperation = kv -> {
      // Get this worker's private map
      long threadId = Thread.currentThread().getId();
      Map<Long, NSSummary> workerMap = allWorkerMaps.computeIfAbsent(threadId, k -> new HashMap<>());

      try {
        // Check if async flusher has failed - stop immediately if so
        asyncFlusher.checkForFailures();

        // Update immediate parent only, NO DB reads during reprocess
        handlePutDirEventReprocess(kv.getValue(), workerMap);

        // Submit to async queue when threshold reached
        if (workerMap.size() >= perWorkerThreshold) {
          asyncFlusher.submitForFlush(workerMap);
          // Get fresh map for this worker
          allWorkerMaps.put(threadId, new HashMap<>());
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return null;
    };

    LOG.debug("Starting dirTable parallel iteration");
    long dirStartTime = System.currentTimeMillis();

    try (ParallelTableIteratorOperation<String, OmDirectoryInfo> parallelIter =
        new ParallelTableIteratorOperation<>(omMetadataManager, dirTable, StringCodec.get(),
            maxIterators, maxWorkers, maxKeysInMemory, nsSummaryFlushToDBMaxThreshold)) {
      parallelIter.performTaskOnTableVals("NSSummaryTaskWithFSO-dirTable", null, null, kvOperation);
    } catch (Exception ex) {
      LOG.error("Unable to process dirTable in parallel", ex);
      return false;
    }

    long dirEndTime = System.currentTimeMillis();
    LOG.debug("Completed dirTable parallel iteration in {} ms", (dirEndTime - dirStartTime));

    // Submit any remaining worker maps
    for (Map<Long, NSSummary> remainingMap : allWorkerMaps.values()) {
      if (!remainingMap.isEmpty()) {
        try {
          asyncFlusher.submitForFlush(remainingMap);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        } catch (IOException e) {
          LOG.error("Failed to submit remaining map for flush", e);
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Process fileTable in parallel using per-worker maps with async flushing.
   */
  private boolean processFileTableInParallel(OMMetadataManager omMetadataManager,
                                             NSSummaryAsyncFlusher asyncFlusher) {
    Table<String, OmKeyInfo> fileTable = omMetadataManager.getFileTable();

    // Per-worker maps for lockless updates
    Map<Long, Map<Long, NSSummary>> allWorkerMaps = new ConcurrentHashMap<>();

    // Divide threshold by worker count
    final long perWorkerThreshold = Math.max(1, nsSummaryFlushToDBMaxThreshold / maxWorkers);

    Function<Table.KeyValue<String, OmKeyInfo>, Void> kvOperation = kv -> {
      // Get this worker's private map
      long threadId = Thread.currentThread().getId();
      Map<Long, NSSummary> workerMap = allWorkerMaps.computeIfAbsent(threadId, k -> new HashMap<>());

      try {
        // Check if async flusher has failed - stop immediately if so
        asyncFlusher.checkForFailures();

        // Update immediate parent only, NO DB reads during reprocess
        OmKeyInfo keyInfo = kv.getValue();
        handlePutKeyEventReprocess(keyInfo, workerMap, keyInfo.getParentObjectID());

        // Submit to async queue when threshold reached
        if (workerMap.size() >= perWorkerThreshold) {
          asyncFlusher.submitForFlush(workerMap);
          // Get fresh map for this worker
          allWorkerMaps.put(threadId, new HashMap<>());
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return null;
    };

    LOG.debug("Starting fileTable parallel iteration");
    long fileStartTime = System.currentTimeMillis();

    try (ParallelTableIteratorOperation<String, OmKeyInfo> parallelIter =
             new ParallelTableIteratorOperation<>(omMetadataManager, fileTable, StringCodec.get(),
                 maxIterators, maxWorkers, maxKeysInMemory, nsSummaryFlushToDBMaxThreshold)) {
      parallelIter.performTaskOnTableVals("NSSummaryTaskWithFSO-fileTable", null, null, kvOperation);
    } catch (Exception ex) {
      LOG.error("Unable to process fileTable in parallel", ex);
      return false;
    }

    long fileEndTime = System.currentTimeMillis();
    LOG.debug("Completed fileTable parallel iteration in {} ms", (fileEndTime - fileStartTime));

    // Submit any remaining worker maps
    for (Map<Long, NSSummary> remainingMap : allWorkerMaps.values()) {
      if (!remainingMap.isEmpty()) {
        try {
          asyncFlusher.submitForFlush(remainingMap);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        } catch (IOException e) {
          LOG.error("Failed to submit remaining map for flush", e);
          return false;
        }
      }
    }
    return true;
  }

}
