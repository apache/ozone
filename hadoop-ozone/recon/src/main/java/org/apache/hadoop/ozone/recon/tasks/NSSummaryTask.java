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
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.recon.ReconUtils;
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
 * Task to query data from OMDB and write into Recon RocksDB.
 * Reprocess() will take a snapshots on OMDB, and iterate the keyTable and
 * dirTable to write all information to RocksDB.
 *
 * For FSO-enabled keyTable (fileTable), we need to fetch the parent object
 * (bucket or directory), increment its numOfKeys by 1, increase its sizeOfKeys
 * by the file data size, and update the file size distribution bin accordingly.
 *
 * For dirTable, we need to fetch the parent object (bucket or directory),
 * add the current directory's objectID to the parent object's childDir field.
 *
 * Process() will write all OMDB updates to RocksDB.
 * The write logic is the same as above. For update action, we will treat it as
 * delete old value first, and write updated value then.
 */
public class NSSummaryTask implements ReconOmTask {
  private static final Logger LOG =
          LoggerFactory.getLogger(NSSummaryTask.class);
  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;

  @Inject
  public NSSummaryTask(ReconNamespaceSummaryManager
                                 reconNamespaceSummaryManager) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
  }

  @Override
  public String getTaskName() {
    return "NSSummaryTask";
  }

  // We only listen to updates from FSO-enabled KeyTable(FileTable) and DirTable
  public Collection<String> getTaskTables() {
    return Arrays.asList(new String[]{FILE_TABLE, DIRECTORY_TABLE});
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

      // we only process updates on OM's KeyTable and Dirtable
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

    LOG.info("Completed a process run of NSSummaryTask");
    return new ImmutablePair<>(getTaskName(), true);
  }

  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    Map<Long, NSSummary> nsSummaryMap = new HashMap<>();

    try {
      // reinit Recon RocksDB's namespace CF.
      reconNamespaceSummaryManager.clearNSSummaryTable();

      Table dirTable = omMetadataManager.getDirectoryTable();
      TableIterator<String, ? extends Table.KeyValue<String, OmDirectoryInfo>>
              dirTableIter = dirTable.iterator();

      while (dirTableIter.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> kv = dirTableIter.next();
        OmDirectoryInfo directoryInfo = kv.getValue();
        handlePutDirEvent(directoryInfo, nsSummaryMap);
      }

      // Get fileTable used by FSO
      Table keyTable = omMetadataManager.getFileTable();

      TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
              keyTableIter = keyTable.iterator();

      while (keyTableIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyTableIter.next();
        OmKeyInfo keyInfo = kv.getValue();
        handlePutKeyEvent(keyInfo, nsSummaryMap);
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
    LOG.info("Completed a reprocess run of NSSummaryTask");
    return new ImmutablePair<>(getTaskName(), true);
  }

  private void writeNSSummariesToDB(Map<Long, NSSummary> nsSummaryMap)
      throws IOException {
    RDBBatchOperation rdbBatchOperation = new RDBBatchOperation();
    nsSummaryMap.keySet().forEach((Long key) -> {
      try {
        reconNamespaceSummaryManager.batchStoreNSSummaries(rdbBatchOperation,
            key, nsSummaryMap.get(key));
      } catch (IOException e) {
        LOG.error("Unable to write Namespace Summary data in Recon DB.",
            e);
      }
    });
    reconNamespaceSummaryManager.commitBatchOperation(rdbBatchOperation);
  }

  private void handlePutKeyEvent(OmKeyInfo keyInfo, Map<Long,
      NSSummary> nsSummaryMap) throws IOException {
    long parentObjectId = keyInfo.getParentObjectID();
    // Try to get the NSSummary from our local map that maps NSSummaries to IDs
    NSSummary nsSummary = nsSummaryMap.get(parentObjectId);
    if (nsSummary == null) {
      // If we don't have it in this batch we try to get it from the DB
      nsSummary = reconNamespaceSummaryManager.getNSSummary(parentObjectId);
    }
    if (nsSummary == null) {
      // If we don't have it locally and in the DB we create a new instance
      // as this is a new ID
      nsSummary = new NSSummary();
    }
    int numOfFile = nsSummary.getNumOfFiles();
    long sizeOfFile = nsSummary.getSizeOfFiles();
    int[] fileBucket = nsSummary.getFileSizeBucket();
    nsSummary.setNumOfFiles(numOfFile + 1);
    long dataSize = keyInfo.getDataSize();
    nsSummary.setSizeOfFiles(sizeOfFile + dataSize);
    int binIndex = ReconUtils.getBinIndex(dataSize);

    ++fileBucket[binIndex];
    nsSummary.setFileSizeBucket(fileBucket);
    nsSummaryMap.put(parentObjectId, nsSummary);
  }

  private void handlePutDirEvent(OmDirectoryInfo directoryInfo,
                                 Map<Long, NSSummary> nsSummaryMap)
          throws IOException {
    long parentObjectId = directoryInfo.getParentObjectID();
    long objectId = directoryInfo.getObjectID();
    // write the dir name to the current directory
    String dirName = directoryInfo.getName();
    // Try to get the NSSummary from our local map that maps NSSummaries to IDs
    NSSummary curNSSummary = nsSummaryMap.get(objectId);
    if (curNSSummary == null) {
      // If we don't have it in this batch we try to get it from the DB
      curNSSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    }
    if (curNSSummary == null) {
      // If we don't have it locally and in the DB we create a new instance
      // as this is a new ID
      curNSSummary = new NSSummary();
    }
    curNSSummary.setDirName(dirName);
    nsSummaryMap.put(objectId, curNSSummary);

    // Write the child dir list to the parent directory
    // Try to get the NSSummary from our local map that maps NSSummaries to IDs
    NSSummary nsSummary = nsSummaryMap.get(parentObjectId);
    if (nsSummary == null) {
      // If we don't have it in this batch we try to get it from the DB
      nsSummary = reconNamespaceSummaryManager.getNSSummary(parentObjectId);
    }
    if (nsSummary == null) {
      // If we don't have it locally and in the DB we create a new instance
      // as this is a new ID
      nsSummary = new NSSummary();
    }
    nsSummary.addChildDir(objectId);
    nsSummaryMap.put(parentObjectId, nsSummary);
  }

  private void handleDeleteKeyEvent(OmKeyInfo keyInfo,
                                    Map<Long, NSSummary> nsSummaryMap)
          throws IOException {
    long parentObjectId = keyInfo.getParentObjectID();
    // Try to get the NSSummary from our local map that maps NSSummaries to IDs
    NSSummary nsSummary = nsSummaryMap.get(parentObjectId);
    if (nsSummary == null) {
      // If we don't have it in this batch we try to get it from the DB
      nsSummary = reconNamespaceSummaryManager.getNSSummary(parentObjectId);
    }

    // Just in case the OmKeyInfo isn't correctly written.
    if (nsSummary == null) {
      LOG.error("The namespace table is not correctly populated.");
      return;
    }
    int numOfFile = nsSummary.getNumOfFiles();
    long sizeOfFile = nsSummary.getSizeOfFiles();
    int[] fileBucket = nsSummary.getFileSizeBucket();

    long dataSize = keyInfo.getDataSize();
    int binIndex = ReconUtils.getBinIndex(dataSize);

    // decrement count, data size, and bucket count
    // even if there's no direct key, we still keep the entry because
    // we still need children dir IDs info
    nsSummary.setNumOfFiles(numOfFile - 1);
    nsSummary.setSizeOfFiles(sizeOfFile - dataSize);
    --fileBucket[binIndex];
    nsSummary.setFileSizeBucket(fileBucket);
    nsSummaryMap.put(parentObjectId, nsSummary);
  }

  private void handleDeleteDirEvent(OmDirectoryInfo directoryInfo,
                                    Map<Long, NSSummary> nsSummaryMap)
          throws IOException {
    long parentObjectId = directoryInfo.getParentObjectID();
    long objectId = directoryInfo.getObjectID();
    // Try to get the NSSummary from our local map that maps NSSummaries to IDs
    NSSummary nsSummary = nsSummaryMap.get(parentObjectId);
    if (nsSummary == null) {
      // If we don't have it in this batch we try to get it from the DB
      nsSummary = reconNamespaceSummaryManager.getNSSummary(parentObjectId);
    }

    // Just in case the OmDirectoryInfo isn't correctly written.
    if (nsSummary == null) {
      LOG.error("The namespace table is not correctly populated.");
      return;
    }

    nsSummary.removeChildDir(objectId);
    nsSummaryMap.put(parentObjectId, nsSummary);
  }
}

