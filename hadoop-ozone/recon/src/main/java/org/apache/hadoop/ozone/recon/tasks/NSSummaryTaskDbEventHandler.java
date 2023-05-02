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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.OrphanKeyMetaData;
import org.apache.hadoop.ozone.recon.api.types.OrphanKeysMetaDataSet;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_ORPHANKEYS_METADATA_FLUSH_TO_DB_MAX_THRESHOLD;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_ORPHANKEYS_METADATA_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT;

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

  private final long nsSummaryFlushToDBMaxThreshold;

  private final long orphanKeysFlushToDBMaxThreshold;

  public NSSummaryTaskDbEventHandler(ReconNamespaceSummaryManager
                                     reconNamespaceSummaryManager,
                                     ReconOMMetadataManager
                                     reconOMMetadataManager,
                                     OzoneConfiguration
                                     ozoneConfiguration) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.reconOMMetadataManager = reconOMMetadataManager;
    nsSummaryFlushToDBMaxThreshold = ozoneConfiguration.getLong(
        OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD,
        OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT);
    orphanKeysFlushToDBMaxThreshold = ozoneConfiguration.getLong(
        OZONE_RECON_ORPHANKEYS_METADATA_FLUSH_TO_DB_MAX_THRESHOLD,
        OZONE_RECON_ORPHANKEYS_METADATA_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT);
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

  protected void writeOrphanKeysMetaDataToDB(
      Map<Long, OrphanKeysMetaDataSet> orphanKeysMetaDataSetMap)
      throws IOException {
    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      orphanKeysMetaDataSetMap.keySet().forEach((Long key) -> {
        try {
          reconNamespaceSummaryManager.batchStoreOrphanKeysMetaData(
              rdbBatchOperation,
              key, orphanKeysMetaDataSetMap.get(key));
        } catch (IOException e) {
          LOG.error("Unable to write orphan keys meta data in Recon DB.",
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

  protected boolean flushAndCommitNSToDB(Map<Long, NSSummary> nsSummaryMap) {
    try {
      writeNSSummariesToDB(nsSummaryMap);
      nsSummaryMap.clear();
    } catch (IOException e) {
      LOG.error("Unable to write Namespace Summary data in Recon DB.", e);
      return false;
    }
    return true;
  }

  protected boolean checkAndCallFlushToDB(
      Map<Long, NSSummary> nsSummaryMap) {
    // if map contains more than entries, flush to DB and clear the map
    if (null != nsSummaryMap && nsSummaryMap.size() >=
        nsSummaryFlushToDBMaxThreshold) {
      return flushAndCommitNSToDB(nsSummaryMap);
    }
    return true;
  }

  protected boolean flushAndCommitOrphanKeysMetaDataToDB(
      Map<Long, OrphanKeysMetaDataSet> orphanKeysMetaDataSetMap) {
    try {
      writeOrphanKeysMetaDataToDB(orphanKeysMetaDataSetMap);
      orphanKeysMetaDataSetMap.clear();
    } catch (IOException e) {
      LOG.error("Unable to write orphan keys meta data in Recon DB.", e);
      return false;
    }
    return true;
  }

  protected boolean checkOrphanDataAndCallFlushToDB(
      Map<Long, OrphanKeysMetaDataSet> orphanKeysMetaDataSetMap) {
    // if map contains more than entries, flush to DB and clear the map
    if (null != orphanKeysMetaDataSetMap && orphanKeysMetaDataSetMap.size() >=
        orphanKeysFlushToDBMaxThreshold) {
      return flushAndCommitOrphanKeysMetaDataToDB(orphanKeysMetaDataSetMap);
    }
    return true;
  }

  protected void buildOrphanCandidateSet(
      WithParentObjectId fileDirInfo,
      Map<Long, NSSummary> nsSummaryMap,
      Map<Long, OrphanKeysMetaDataSet> orphanKeysMetaDataSetMap)
      throws IOException {
    long objectID = fileDirInfo.getObjectID();
    long parentObjectID = fileDirInfo.getParentObjectID();
    NSSummary nsSummary = nsSummaryMap.get(parentObjectID);
    if (nsSummary == null) {
      // If we don't have it in this batch we try to get it from the DB
      nsSummary = reconNamespaceSummaryManager.getNSSummary(parentObjectID);
    }
    if (null == nsSummary) {
      OrphanKeysMetaDataSet orphanKeysMetaDataSet =
          orphanKeysMetaDataSetMap.get(parentObjectID);
      if (null == orphanKeysMetaDataSet) {
        orphanKeysMetaDataSet =
            reconNamespaceSummaryManager.getOrphanKeysMetaDataSet(
                parentObjectID);
      }
      Set<OrphanKeyMetaData> orphanKeyMetaDataSet;
      if (null == orphanKeysMetaDataSet) {
        orphanKeyMetaDataSet = new HashSet<>();
        orphanKeysMetaDataSet = new OrphanKeysMetaDataSet(orphanKeyMetaDataSet);
      }
      orphanKeyMetaDataSet = orphanKeysMetaDataSet.getSet();
      OrphanKeyMetaData orphanKeyMetaData = new OrphanKeyMetaData(objectID);
      orphanKeyMetaDataSet.add(orphanKeyMetaData);
      orphanKeysMetaDataSetMap.put(parentObjectID, orphanKeysMetaDataSet);
    } else {
      orphanKeysMetaDataSetMap.remove(parentObjectID);
    }
  }
}
