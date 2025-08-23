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
import java.util.Collection;
import java.util.Collections;
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

  private void updateNSSummariesToDB(Map<Long, NSSummary> nsSummaryMap, Collection<Long> objectIdsToBeDeleted)
      throws IOException {
    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      for (Map.Entry<Long, NSSummary> entry : nsSummaryMap.entrySet()) {
        try {
          reconNamespaceSummaryManager.batchStoreNSSummaries(rdbBatchOperation, entry.getKey(), entry.getValue());
        } catch (IOException e) {
          LOG.error("Unable to write Namespace Summary data in Recon DB.", e);
          throw e;
        }
      }
      for (Long objectId : objectIdsToBeDeleted) {
        try {
          reconNamespaceSummaryManager.batchDeleteNSSummaries(rdbBatchOperation, objectId);
        } catch (IOException e) {
          LOG.error("Unable to delete Namespace Summary data from Recon DB.", e);
          throw e;
        }
      }
      reconNamespaceSummaryManager.commitBatchOperation(rdbBatchOperation);
    }
    LOG.debug("Successfully updated Namespace Summary data in Recon DB.");
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
    
    // Update immediate parent's totals (these fields now represent totals)
    nsSummary.setNumOfFiles(nsSummary.getNumOfFiles() + 1);
    nsSummary.setSizeOfFiles(nsSummary.getSizeOfFiles() + keyInfo.getDataSize());
    int binIndex = ReconUtils.getFileSizeBinIndex(keyInfo.getDataSize());

    ++fileBucket[binIndex];
    nsSummary.setFileSizeBucket(fileBucket);
    nsSummaryMap.put(parentObjectId, nsSummary);

    // Propagate upwards to all parents in the parent chain
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

    // Check if this directory already has content (files/subdirs) that need propagation
    boolean directoryAlreadyExists = (curNSSummary != null);
    long existingSizeOfFiles = directoryAlreadyExists ? curNSSummary.getSizeOfFiles() : 0;
    int existingNumOfFiles = directoryAlreadyExists ? curNSSummary.getNumOfFiles() : 0;

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

    // If the directory already existed with content, propagate its totals upward
    if (directoryAlreadyExists && (existingSizeOfFiles > 0 || existingNumOfFiles > 0)) {
      propagateSizeUpwards(parentObjectId, existingSizeOfFiles, existingNumOfFiles, nsSummaryMap);
    }
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

    // Decrement immediate parent's totals (these fields now represent totals)
    nsSummary.setNumOfFiles(nsSummary.getNumOfFiles() - 1);
    nsSummary.setSizeOfFiles(nsSummary.getSizeOfFiles() - keyInfo.getDataSize());
    --fileBucket[binIndex];
    nsSummary.setFileSizeBucket(fileBucket);
    nsSummaryMap.put(parentObjectId, nsSummary);

    // Propagate upwards to all parents in the parent chain
    propagateSizeUpwards(parentObjectId, -keyInfo.getDataSize(), -1, nsSummaryMap);
  }

  protected void handleDeleteDirEvent(OmDirectoryInfo directoryInfo,
                                      Map<Long, NSSummary> nsSummaryMap)
      throws IOException {
    long deletedDirObjectId = directoryInfo.getObjectID();
    long parentObjectId = directoryInfo.getParentObjectID();
    
    // Get the deleted directory's NSSummary to extract its totals
    NSSummary deletedDirSummary = nsSummaryMap.get(deletedDirObjectId);
    if (deletedDirSummary == null) {
      deletedDirSummary = reconNamespaceSummaryManager.getNSSummary(deletedDirObjectId);
    }
    
    // Get the parent directory's NSSummary
    NSSummary parentNsSummary = nsSummaryMap.get(parentObjectId);
    if (parentNsSummary == null) {
      parentNsSummary = reconNamespaceSummaryManager.getNSSummary(parentObjectId);
    }

    // Just in case the OmDirectoryInfo isn't correctly written.
    if (parentNsSummary == null) {
      LOG.error("The namespace table is not correctly populated.");
      return;
    }

    // If deleted directory exists, decrement its totals from parent and propagate
    if (deletedDirSummary != null) {
      // Decrement parent's totals by the deleted directory's totals
      parentNsSummary.setNumOfFiles(parentNsSummary.getNumOfFiles() - deletedDirSummary.getNumOfFiles());
      parentNsSummary.setSizeOfFiles(parentNsSummary.getSizeOfFiles() - deletedDirSummary.getSizeOfFiles());
      
      // Propagate the decrements upwards to all ancestors
      propagateSizeUpwards(parentObjectId, -deletedDirSummary.getSizeOfFiles(), 
                          -deletedDirSummary.getNumOfFiles(), nsSummaryMap);
      
      // Set the deleted directory's parentId to 0 (unlink it)
      deletedDirSummary.setParentId(0);
      nsSummaryMap.put(deletedDirObjectId, deletedDirSummary);
    }

    // Remove the deleted directory ID from parent's childDir set
    parentNsSummary.removeChildDir(deletedDirObjectId);
    nsSummaryMap.put(parentObjectId, parentNsSummary);
  }

  protected boolean flushAndCommitNSToDB(Map<Long, NSSummary> nsSummaryMap) {
    try {
      updateNSSummariesToDB(nsSummaryMap, Collections.emptyList());
    } catch (IOException e) {
      LOG.error("Unable to write Namespace Summary data in Recon DB.", e);
      return false;
    } finally {
      nsSummaryMap.clear();
    }
    return true;
  }

  /**
   * Flush and commit updated NSSummary to DB. This includes deleted objects of OM metadata also.
   *
   * @param nsSummaryMap Map of objectId to NSSummary
   * @param objectIdsToBeDeleted list of objectids to be deleted
   * @return true if successful, false otherwise
   */
  protected boolean flushAndCommitUpdatedNSToDB(Map<Long, NSSummary> nsSummaryMap,
                                                Collection<Long> objectIdsToBeDeleted) {
    try {
      updateNSSummariesToDB(nsSummaryMap, objectIdsToBeDeleted);
    } catch (IOException e) {
      LOG.error("Unable to write Namespace Summary data in Recon DB.", e);
      return false;
    } finally {
      nsSummaryMap.clear();
    }
    return true;
  }

  /**
   * Propagates size and count changes upwards through the parent chain.
   * This ensures that when files are added/deleted, all ancestor directories
   * reflect the total changes in their sizeOfFiles and numOfFiles fields.
   */
  protected void propagateSizeUpwards(long objectId, long sizeChange,
                                       int countChange, Map<Long, NSSummary> nsSummaryMap) 
                                       throws IOException {
    // Get the current directory's NSSummary
    NSSummary nsSummary = nsSummaryMap.get(objectId);
    if (nsSummary == null) {
      nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    }
    if (nsSummary == null) {
      return; // No more parents to update
    }

    // Continue propagating to parent
    long parentId = nsSummary.getParentId();
    if (parentId != 0) {
      // Get parent's NSSummary
      NSSummary parentSummary = nsSummaryMap.get(parentId);
      if (parentSummary == null) {
        parentSummary = reconNamespaceSummaryManager.getNSSummary(parentId);
      }
      if (parentSummary != null) {
        // Update parent's totals
        parentSummary.setSizeOfFiles(parentSummary.getSizeOfFiles() + sizeChange);
        parentSummary.setNumOfFiles(parentSummary.getNumOfFiles() + countChange);
        nsSummaryMap.put(parentId, parentSummary);
        
        // Recursively propagate to grandparents
        propagateSizeUpwards(parentId, sizeChange, countChange, nsSummaryMap);
      }
    }
  }

}
