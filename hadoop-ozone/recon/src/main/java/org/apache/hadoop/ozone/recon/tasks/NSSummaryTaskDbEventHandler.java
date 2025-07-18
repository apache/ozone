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

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for holding all NSSummaryTask methods
 * related to DB operations so that they can commonly be
 * used in NSSummaryTaskWithFSO and NSSummaryTaskWithLegacy.
 */
public class NSSummaryTaskDbEventHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(NSSummaryTaskDbEventHandler.class);
  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private ReconOMMetadataManager reconOMMetadataManager;

  public NSSummaryTaskDbEventHandler(ReconNamespaceSummaryManager
                                     reconNamespaceSummaryManager,
                                     ReconOMMetadataManager
                                     reconOMMetadataManager) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.reconOMMetadataManager = reconOMMetadataManager;
  }

  public ReconNamespaceSummaryManager getReconNamespaceSummaryManager() {
    return reconNamespaceSummaryManager;
  }

  public ReconOMMetadataManager getReconOMMetadataManager() {
    return reconOMMetadataManager;
  }

  protected void writeNSSummariesToDB(Map<Long, NSSummary> nsSummaryMap)
      throws IOException {
    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      for (Map.Entry<Long, NSSummary> entry : nsSummaryMap.entrySet()) {
        try {
          reconNamespaceSummaryManager.batchStoreNSSummaries(rdbBatchOperation,
              entry.getKey(), entry.getValue());
        } catch (IOException e) {
          LOG.error("Unable to write Namespace Summary data in Recon DB.",
              e);
          throw e;
        }
      }
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
    int[] fileBucket = nsSummary.getFileSizeBucket();
    nsSummary.setNumOfFiles(nsSummary.getNumOfFiles() + 1);
    nsSummary.setSizeOfFiles(nsSummary.getSizeOfFiles() + keyInfo.getDataSize());
    int binIndex = ReconUtils.getFileSizeBinIndex(keyInfo.getDataSize());

    ++fileBucket[binIndex];
    nsSummary.setFileSizeBucket(fileBucket);
    nsSummaryMap.put(parentObjectId, nsSummary);

    // Propagate totalSize and totalCount changes upwards (only during live processing, not reprocessing)
    propagateSizeUpwards(parentObjectId, keyInfo.getDataSize(), 1, nsSummaryMap);
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
    // Set the parent directory ID
    curNSSummary.setParentId(parentObjectId);
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
    int[] fileBucket = nsSummary.getFileSizeBucket();

    int binIndex = ReconUtils.getFileSizeBinIndex(keyInfo.getDataSize());

    // decrement count, data size, and bucket count
    // even if there's no direct key, we still keep the entry because
    // we still need children dir IDs info
    nsSummary.setNumOfFiles(nsSummary.getNumOfFiles() - 1);
    nsSummary.setSizeOfFiles(nsSummary.getSizeOfFiles() - keyInfo.getDataSize());
    --fileBucket[binIndex];
    nsSummary.setFileSizeBucket(fileBucket);
    nsSummaryMap.put(parentObjectId, nsSummary);

    // Propagate totalSize and totalCount decreases upwards (only during live processing, not reprocessing)
    propagateSizeUpwards(parentObjectId, -keyInfo.getDataSize(), -1, nsSummaryMap);
  }

  protected void handleDeleteDirEvent(OmDirectoryInfo directoryInfo,
                                      Map<Long, NSSummary> nsSummaryMap)
      throws IOException {
    // Fetch the current directory's nssummary.
    long currentObjectId = directoryInfo.getObjectID();
    NSSummary curNSSummary = nsSummaryMap.get(currentObjectId);
    if (curNSSummary == null) {
      // If we don't have it in this batch we try to get it from the DB
      curNSSummary = reconNamespaceSummaryManager.getNSSummary(currentObjectId);
    }

    // Fetch the parent directory's nssummary.
    long parentObjectId = directoryInfo.getParentObjectID();
    // Try to get the NSSummary from our local map that maps NSSummaries to IDs
    NSSummary parentNsSummary = nsSummaryMap.get(parentObjectId);
    if (parentNsSummary == null) {
      // If we don't have it in this batch we try to get it from the DB
      parentNsSummary = reconNamespaceSummaryManager.getNSSummary(parentObjectId);
    }

    // Just in case the OmDirectoryInfo isn't correctly written.
    if (parentNsSummary == null) {
      LOG.error("The namespace table is not correctly populated.");
      return;
    }

    // Set the parentID of the current directory to 0
    curNSSummary.setParentId(0);
    nsSummaryMap.put(currentObjectId, curNSSummary);

    // Remove the refrence of the current directory from the parent directory's NSSummary
    parentNsSummary.removeChildDir(directoryInfo.getObjectID());
    parentNsSummary.setTotalSize(parentNsSummary.getTotalSize() - curNSSummary.getSizeOfFiles());
    nsSummaryMap.put(parentObjectId, parentNsSummary);
  }

  protected void propagateSizeUpwards(long objectId, long sizeChange, 
                                       long countChange, Map<Long, NSSummary> nsSummaryMap) 
                                       throws IOException {
    // Get the current directory's NSSummary
    NSSummary nsSummary = nsSummaryMap.get(objectId);
    if (nsSummary == null) {
      nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    }
    if (nsSummary == null) {
      return; // No more parents to update
    }

    // Update totalSize and totalCount for this directory
    long currentTotalSize = nsSummary.getTotalSize();
    long currentTotalCount = nsSummary.getTotalCount();
    
    // Initialize if unset (-1)
    if (currentTotalSize == -1) {
      currentTotalSize = nsSummary.getSizeOfFiles();
    }
    if (currentTotalCount == -1) {
      currentTotalCount = nsSummary.getNumOfFiles();
    }
    
    nsSummary.setTotalSize(currentTotalSize + sizeChange);
    nsSummary.setTotalCount(currentTotalCount + countChange);
    nsSummaryMap.put(objectId, nsSummary);

    // Continue propagating to parent
    long parentId = nsSummary.getParentId();
    if (parentId != 0) {
      propagateSizeUpwards(parentId, sizeChange, countChange, nsSummaryMap);
    }
  }

  protected boolean flushAndCommitNSToDB(Map<Long, NSSummary> nsSummaryMap) {
    try {
      writeNSSummariesToDB(nsSummaryMap);
    } catch (IOException e) {
      LOG.error("Unable to write Namespace Summary data in Recon DB.", e);
      return false;
    } finally {
      nsSummaryMap.clear();
    }
    return true;
  }
}
