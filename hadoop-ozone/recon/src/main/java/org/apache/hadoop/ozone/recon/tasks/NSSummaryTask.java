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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Map;

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
public abstract class NSSummaryTask implements ReconOmTask {
  private static final Logger LOG =
          LoggerFactory.getLogger(NSSummaryTask.class);

  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;

  @Inject
  public NSSummaryTask(ReconNamespaceSummaryManager
                                 reconNamespaceSummaryManager) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
  }

  public ReconNamespaceSummaryManager getReconNamespaceSummaryManager() {
    return reconNamespaceSummaryManager;
  }

  public abstract String getTaskName();

  public abstract Pair<String, Boolean> process(OMUpdateEventBatch events);

  public abstract Pair<String, Boolean> reprocess(
      OMMetadataManager omMetadataManager);

  protected void writeNSSummariesToDB(Map<Long, NSSummary> nsSummaryMap)
      throws IOException {
    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
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
  }

  protected void handlePutKeyEvent(OmKeyInfo keyInfo, Map<Long,
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

  protected void handlePutDirEvent(OmDirectoryInfo directoryInfo,
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

  protected void handleDeleteKeyEvent(OmKeyInfo keyInfo,
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

  protected void handleDeleteDirEvent(OmDirectoryInfo directoryInfo,
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

