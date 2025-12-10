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
      // Read-Modify-Write for each entry to prevent race conditions
      for (Map.Entry<Long, NSSummary> entry : nsSummaryMap.entrySet()) {
        try {
          Long parentId = entry.getKey();
          NSSummary deltaSummary = entry.getValue();
          
          // Step 1: READ existing value from DB
          NSSummary existingSummary = reconNamespaceSummaryManager.getNSSummary(parentId);
          
          if (existingSummary == null) {
            // First time - just write the delta as-is
            existingSummary = deltaSummary;
          } else {
            // Step 2: MODIFY - merge delta into existing
            existingSummary.setNumOfFiles(existingSummary.getNumOfFiles() + deltaSummary.getNumOfFiles());
            existingSummary.setSizeOfFiles(existingSummary.getSizeOfFiles() + deltaSummary.getSizeOfFiles());
            existingSummary.setReplicatedSizeOfFiles(
                existingSummary.getReplicatedSizeOfFiles() + deltaSummary.getReplicatedSizeOfFiles());
            
            // Merge file size buckets
            int[] existingBucket = existingSummary.getFileSizeBucket();
            int[] deltaBucket = deltaSummary.getFileSizeBucket();
            for (int i = 0; i < existingBucket.length; i++) {
              existingBucket[i] += deltaBucket[i];
            }
            existingSummary.setFileSizeBucket(existingBucket);
            
            // Merge child directory sets
            existingSummary.getChildDir().addAll(deltaSummary.getChildDir());
            
            // Preserve metadata if not set in existing
            if ((existingSummary.getDirName() == null || existingSummary.getDirName().isEmpty()) &&
                (deltaSummary.getDirName() != null && !deltaSummary.getDirName().isEmpty())) {
              existingSummary.setDirName(deltaSummary.getDirName());
            }
            if (existingSummary.getParentId() == 0 && deltaSummary.getParentId() != 0) {
              existingSummary.setParentId(deltaSummary.getParentId());
            }
          }
          
          // Step 3: WRITE merged result back to DB
          reconNamespaceSummaryManager.batchStoreNSSummaries(rdbBatchOperation, parentId, existingSummary);
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
    handlePutKeyEvent(keyInfo, nsSummaryMap, true);
  }

  protected void handlePutKeyEvent(OmKeyInfo keyInfo, Map<Long,
      NSSummary> nsSummaryMap, boolean allowDbRead) throws IOException {
    long parentObjectId = keyInfo.getParentObjectID();
    // Try to get from map
    NSSummary nsSummary = nsSummaryMap.get(parentObjectId);
    if (nsSummary == null && allowDbRead) {
      // Only read from DB during delta updates (process method)
      nsSummary = reconNamespaceSummaryManager.getNSSummary(parentObjectId);
    }
    if (nsSummary == null) {
      // Create new instance if not found
      nsSummary = new NSSummary();
    }
    int[] fileBucket = nsSummary.getFileSizeBucket();

    // Update immediate parent's totals (includes all descendant files)
    nsSummary.setNumOfFiles(nsSummary.getNumOfFiles() + 1);
    nsSummary.setSizeOfFiles(nsSummary.getSizeOfFiles() + keyInfo.getDataSize());
    // Before arithmetic operations, check for sentinel value
    long currentReplSize = nsSummary.getReplicatedSizeOfFiles();
    if (currentReplSize < 0) {
      // Old data, initialize to 0 before first use
      currentReplSize = 0;
      nsSummary.setReplicatedSizeOfFiles(0);
    }
    nsSummary.setReplicatedSizeOfFiles(currentReplSize + keyInfo.getReplicatedSize());
    int binIndex = ReconUtils.getFileSizeBinIndex(keyInfo.getDataSize());

    ++fileBucket[binIndex];
    nsSummary.setFileSizeBucket(fileBucket);
    nsSummaryMap.put(parentObjectId, nsSummary);

    // Propagate upwards to all parents in the parent chain
    propagateSizeUpwards(parentObjectId, keyInfo.getDataSize(), keyInfo.getReplicatedSize(), 1, nsSummaryMap, allowDbRead);
  }

  protected void handlePutDirEvent(OmDirectoryInfo directoryInfo,
                                   Map<Long, NSSummary> nsSummaryMap)
      throws IOException {
    handlePutDirEvent(directoryInfo, nsSummaryMap, true);
  }

  protected void handlePutDirEvent(OmDirectoryInfo directoryInfo,
                                   Map<Long, NSSummary> nsSummaryMap,
                                   boolean allowDbRead)
      throws IOException {
    long parentObjectId = directoryInfo.getParentObjectID();
    long objectId = directoryInfo.getObjectID();
    // write the dir name to the current directory
    String dirName = directoryInfo.getName();

    // Get or create the directory's NSSummary
    NSSummary curNSSummary = nsSummaryMap.get(objectId);
    if (curNSSummary == null && allowDbRead) {
      // Read from DB during delta updates (process method)
      curNSSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    }
    
    // Check if this directory already has content (files/subdirs) that need propagation
    boolean directoryAlreadyExists = (curNSSummary != null);
    long existingSizeOfFiles = directoryAlreadyExists ? curNSSummary.getSizeOfFiles() : 0;
    int existingNumOfFiles = directoryAlreadyExists ? curNSSummary.getNumOfFiles() : 0;
    long existingReplicatedSizeOfFiles = directoryAlreadyExists ? curNSSummary.getReplicatedSizeOfFiles() : 0;

    if (!directoryAlreadyExists) {
      curNSSummary = new NSSummary();
    }
    curNSSummary.setDirName(dirName);
    curNSSummary.setParentId(parentObjectId);
    nsSummaryMap.put(objectId, curNSSummary);

    // Get or create the parent's NSSummary
    NSSummary parentNSSummary = nsSummaryMap.get(parentObjectId);
    if (parentNSSummary == null && allowDbRead) {
      // Read from DB during delta updates (process method)
      parentNSSummary = reconNamespaceSummaryManager.getNSSummary(parentObjectId);
    }
    if (parentNSSummary == null) {
      // Create new instance if not found
      parentNSSummary = new NSSummary();
    }

    // Add child directory to parent
    parentNSSummary.addChildDir(objectId);
    
    // If the directory already existed with content, update immediate parent's stats
    if (directoryAlreadyExists && (existingSizeOfFiles > 0 || existingNumOfFiles > 0)) {
      parentNSSummary.setNumOfFiles(parentNSSummary.getNumOfFiles() + existingNumOfFiles);
      parentNSSummary.setSizeOfFiles(parentNSSummary.getSizeOfFiles() + existingSizeOfFiles);
      
      long parentReplSize = parentNSSummary.getReplicatedSizeOfFiles();
      if (parentReplSize < 0) {
        parentReplSize = 0;
      }
      parentNSSummary.setReplicatedSizeOfFiles(parentReplSize + existingReplicatedSizeOfFiles);
      nsSummaryMap.put(parentObjectId, parentNSSummary);
      
      // Propagate to grandparents and beyond
      propagateSizeUpwards(parentObjectId, existingSizeOfFiles,
          existingReplicatedSizeOfFiles, existingNumOfFiles, nsSummaryMap, allowDbRead);
    } else {
      nsSummaryMap.put(parentObjectId, parentNSSummary);
    }
  }

  protected void handleDeleteKeyEvent(OmKeyInfo keyInfo,
                                      Map<Long, NSSummary> nsSummaryMap)
      throws IOException {
    handleDeleteKeyEvent(keyInfo, nsSummaryMap, true);
  }

  protected void handleDeleteKeyEvent(OmKeyInfo keyInfo,
                                      Map<Long, NSSummary> nsSummaryMap,
                                      boolean allowDbRead)
      throws IOException {
    long parentObjectId = keyInfo.getParentObjectID();
    // Try to get from map
    NSSummary nsSummary = nsSummaryMap.get(parentObjectId);
    if (nsSummary == null && allowDbRead) {
      // Read from DB during delta updates (process method)
      nsSummary = reconNamespaceSummaryManager.getNSSummary(parentObjectId);
    }

    // Just in case the OmKeyInfo isn't correctly written
    if (nsSummary == null) {
      LOG.error("The namespace table is not correctly populated.");
      return;
    }
    int[] fileBucket = nsSummary.getFileSizeBucket();

    int binIndex = ReconUtils.getFileSizeBinIndex(keyInfo.getDataSize());

    // Decrement immediate parent's totals (these fields now represent totals)
    nsSummary.setNumOfFiles(nsSummary.getNumOfFiles() - 1);
    nsSummary.setSizeOfFiles(nsSummary.getSizeOfFiles() - keyInfo.getDataSize());
    long currentReplSize = nsSummary.getReplicatedSizeOfFiles();
    long keyReplSize = keyInfo.getReplicatedSize();
    if (currentReplSize >= 0 && keyReplSize >= 0) {
      nsSummary.setReplicatedSizeOfFiles(currentReplSize - keyReplSize);
    }
    --fileBucket[binIndex];
    nsSummary.setFileSizeBucket(fileBucket);
    nsSummaryMap.put(parentObjectId, nsSummary);

    // Propagate upwards to all parents in the parent chain
    propagateSizeUpwards(parentObjectId, -keyInfo.getDataSize(),
        -keyInfo.getReplicatedSize(), -1, nsSummaryMap, allowDbRead);
  }

  protected void handleDeleteDirEvent(OmDirectoryInfo directoryInfo,
                                      Map<Long, NSSummary> nsSummaryMap)
      throws IOException {
    handleDeleteDirEvent(directoryInfo, nsSummaryMap, true);
  }

  protected void handleDeleteDirEvent(OmDirectoryInfo directoryInfo,
                                      Map<Long, NSSummary> nsSummaryMap,
                                      boolean allowDbRead)
      throws IOException {
    long deletedDirObjectId = directoryInfo.getObjectID();
    long parentObjectId = directoryInfo.getParentObjectID();
    
    // Get the deleted directory's NSSummary to extract its totals
    NSSummary deletedDirSummary = nsSummaryMap.get(deletedDirObjectId);
    if (deletedDirSummary == null && allowDbRead) {
      deletedDirSummary = reconNamespaceSummaryManager.getNSSummary(deletedDirObjectId);
    }
    
    // Get the parent directory's NSSummary
    NSSummary parentNsSummary = nsSummaryMap.get(parentObjectId);
    if (parentNsSummary == null && allowDbRead) {
      parentNsSummary = reconNamespaceSummaryManager.getNSSummary(parentObjectId);
    }

    // Just in case the OmDirectoryInfo isn't correctly written.
    if (parentNsSummary == null) {
      LOG.error("The namespace table is not correctly populated.");
      return;
    }

    // Remove the deleted directory ID from parent's childDir set
    parentNsSummary.removeChildDir(deletedDirObjectId);
    
    // If deleted directory exists with content, update immediate parent's stats
    if (deletedDirSummary != null) {
      long deletedSize = deletedDirSummary.getSizeOfFiles();
      int deletedNumFiles = deletedDirSummary.getNumOfFiles();
      long deletedReplSize = deletedDirSummary.getReplicatedSizeOfFiles();
      if (deletedReplSize < 0) {
        deletedReplSize = 0;
      }
      
      // Decrement immediate parent's totals
      parentNsSummary.setNumOfFiles(parentNsSummary.getNumOfFiles() - deletedNumFiles);
      parentNsSummary.setSizeOfFiles(parentNsSummary.getSizeOfFiles() - deletedSize);
      long parentReplSize = parentNsSummary.getReplicatedSizeOfFiles();
      if (parentReplSize >= 0) {
        parentNsSummary.setReplicatedSizeOfFiles(parentReplSize - deletedReplSize);
      }
      nsSummaryMap.put(parentObjectId, parentNsSummary);
      
      // Propagate to grandparents and beyond
      propagateSizeUpwards(parentObjectId, -deletedSize, -deletedReplSize, -deletedNumFiles, nsSummaryMap, allowDbRead);
      
      // Set the deleted directory's parentId to 0 (unlink it)
      deletedDirSummary.setParentId(0);
      nsSummaryMap.put(deletedDirObjectId, deletedDirSummary);
    } else {
      nsSummaryMap.put(parentObjectId, parentNsSummary);
    }
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
      LOG.error("Unable to write Namespace Summary data in Recon DB. batchSize={}", nsSummaryMap.size(), e);
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
  protected void propagateSizeUpwards(long objectId, long sizeChange, long replicatedSizeChange,
                                       int countChange, Map<Long, NSSummary> nsSummaryMap,
                                       boolean allowDbRead) 
                                       throws IOException {
    // Get the current directory's NSSummary
    NSSummary nsSummary = nsSummaryMap.get(objectId);
    if (nsSummary == null && allowDbRead) {
      // Read from DB during delta updates
      nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    }
    if (nsSummary == null) {
      return; // Not found, stop propagation
    }

    // Continue propagating to parent
    long parentId = nsSummary.getParentId();
    if (parentId != 0) {
      // Get parent's NSSummary
      NSSummary parentSummary = nsSummaryMap.get(parentId);
      if (parentSummary == null && allowDbRead) {
        // Read from DB during delta updates
        parentSummary = reconNamespaceSummaryManager.getNSSummary(parentId);
      }
      if (parentSummary == null) {
        // Create new parent entry if not found
        parentSummary = new NSSummary();
      }
      
      // Update parent's totals
      parentSummary.setSizeOfFiles(parentSummary.getSizeOfFiles() + sizeChange);
      long parentReplSize = parentSummary.getReplicatedSizeOfFiles();
      if (parentReplSize < 0) {
        parentReplSize = 0;
      }
      parentSummary.setReplicatedSizeOfFiles(parentReplSize + replicatedSizeChange);
      parentSummary.setNumOfFiles(parentSummary.getNumOfFiles() + countChange);
      nsSummaryMap.put(parentId, parentSummary);
      
      // Recursively propagate to grandparents
      propagateSizeUpwards(parentId, sizeChange, replicatedSizeChange, countChange, nsSummaryMap, allowDbRead);
    }
  }

}
