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
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

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
            writeOmKeyInfoOnNamespaceDB(updatedKeyInfo);
            break;

          case DELETE:
            deleteOmKeyInfoOnNamespaceDB(updatedKeyInfo);
            break;

          case UPDATE:
            if (oldKeyInfo != null) {
              // delete first, then put
              deleteOmKeyInfoOnNamespaceDB(oldKeyInfo);
            } else {
              LOG.warn("Update event does not have the old keyInfo for {}.",
                      updatedKey);
            }
            writeOmKeyInfoOnNamespaceDB(updatedKeyInfo);
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
            writeOmDirectoryInfoOnNamespaceDB(updatedDirectoryInfo);
            break;

          case DELETE:
            deleteOmDirectoryInfoOnNamespaceDB(updatedDirectoryInfo);
            break;

          case UPDATE:
            if (oldDirectoryInfo != null) {
              // delete first, then put
              deleteOmDirectoryInfoOnNamespaceDB(oldDirectoryInfo);
            } else {
              LOG.warn("Update event does not have the old dirInfo for {}.",
                      updatedKey);
            }
            writeOmDirectoryInfoOnNamespaceDB(updatedDirectoryInfo);
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
    LOG.info("Completed a process run of NSSummaryTask");
    return new ImmutablePair<>(getTaskName(), true);
  }

  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {

    try {
      // reinit Recon RocksDB's namespace CF.
      reconNamespaceSummaryManager.clearNSSummaryTable();

      Table dirTable = omMetadataManager.getDirectoryTable();
      TableIterator<String, ? extends Table.KeyValue<String, OmDirectoryInfo>>
              dirTableIter = dirTable.iterator();

      while (dirTableIter.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> kv = dirTableIter.next();
        OmDirectoryInfo directoryInfo = kv.getValue();
        writeOmDirectoryInfoOnNamespaceDB(directoryInfo);
      }

      // Get fileTable used by FSO
      Table keyTable = omMetadataManager.getFileTable();

      TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
              keyTableIter = keyTable.iterator();

      while (keyTableIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyTableIter.next();
        OmKeyInfo keyInfo = kv.getValue();
        writeOmKeyInfoOnNamespaceDB(keyInfo);
      }

    } catch (IOException ioEx) {
      LOG.error("Unable to reprocess Namespace Summary data in Recon DB. ",
              ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }

    LOG.info("Completed a reprocess run of NSSummaryTask");
    return new ImmutablePair<>(getTaskName(), true);
  }

  private void writeOmKeyInfoOnNamespaceDB(OmKeyInfo keyInfo)
          throws IOException {
    long parentObjectId = keyInfo.getParentObjectID();
    NSSummary nsSummary = reconNamespaceSummaryManager
            .getNSSummary(parentObjectId);
    if (nsSummary == null) {
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
    reconNamespaceSummaryManager.storeNSSummary(parentObjectId, nsSummary);
  }

  private void writeOmDirectoryInfoOnNamespaceDB(OmDirectoryInfo directoryInfo)
          throws IOException {
    long parentObjectId = directoryInfo.getParentObjectID();
    long objectId = directoryInfo.getObjectID();
    // write the dir name to the current directory
    String dirName = directoryInfo.getName();
    NSSummary curNSSummary =
            reconNamespaceSummaryManager.getNSSummary(objectId);
    if (curNSSummary == null) {
      curNSSummary = new NSSummary();
    }
    curNSSummary.setDirName(dirName);
    reconNamespaceSummaryManager.storeNSSummary(objectId, curNSSummary);

    // write the child dir list to the parent directory
    NSSummary nsSummary = reconNamespaceSummaryManager
            .getNSSummary(parentObjectId);
    if (nsSummary == null) {
      nsSummary = new NSSummary();
    }
    nsSummary.addChildDir(objectId);
    reconNamespaceSummaryManager.storeNSSummary(parentObjectId, nsSummary);
  }

  private void deleteOmKeyInfoOnNamespaceDB(OmKeyInfo keyInfo)
          throws IOException {
    long parentObjectId = keyInfo.getParentObjectID();
    NSSummary nsSummary = reconNamespaceSummaryManager
            .getNSSummary(parentObjectId);

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
    reconNamespaceSummaryManager.storeNSSummary(parentObjectId, nsSummary);
  }

  private void deleteOmDirectoryInfoOnNamespaceDB(OmDirectoryInfo directoryInfo)
          throws IOException {
    long parentObjectId = directoryInfo.getParentObjectID();
    long objectId = directoryInfo.getObjectID();
    NSSummary nsSummary = reconNamespaceSummaryManager
            .getNSSummary(parentObjectId);

    // Just in case the OmDirectoryInfo isn't correctly written.
    if (nsSummary == null) {
      LOG.error("The namespace table is not correctly populated.");
      return;
    }

    nsSummary.removeChildDir(objectId);
    reconNamespaceSummaryManager.storeNSSummary(parentObjectId, nsSummary);
  }
}

