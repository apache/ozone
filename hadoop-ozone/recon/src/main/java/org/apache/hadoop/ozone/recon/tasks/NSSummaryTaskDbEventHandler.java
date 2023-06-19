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
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.OrphanKeyMetaData;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_ORPHANKEYS_METADATA_FLUSH_TO_DB_MAX_THRESHOLD;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_ORPHANKEYS_METADATA_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT;
import static org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition.ORPHAN_KEYS_METADATA;

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
  private final DBStore reconDbStore;
  private final Table<Long, OrphanKeyMetaData> orphanKeysMetaDataTable;

  private final long nsSummaryFlushToDBMaxThreshold;

  private final long orphanKeysFlushToDBMaxThreshold;

  public NSSummaryTaskDbEventHandler(ReconNamespaceSummaryManager
                                     reconNamespaceSummaryManager,
                                     ReconOMMetadataManager
                                     reconOMMetadataManager,
                                     OzoneConfiguration
                                     ozoneConfiguration,
                                     ReconDBProvider reconDBProvider)
      throws IOException {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.reconOMMetadataManager = reconOMMetadataManager;
    this.reconDbStore = reconDBProvider.getDbStore();
    this.orphanKeysMetaDataTable =
        ORPHAN_KEYS_METADATA.getTable(reconDbStore);
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
      try {
        reconNamespaceSummaryManager.commitBatchOperation(rdbBatchOperation);
      } catch (IOException e) {
        LOG.error("Failed to commit batch operation for writing NSSummary " +
            "data in Recon DB.", e);
        throw e;
      }
    }
  }

  protected void writeOrphanKeysMetaDataToDB(
      Map<Long, OrphanKeyMetaData> orphanKeyMetaDataMap)
      throws IOException {
    List<Long> orphanKeysParentIdListToBeDeleted =
        new ArrayList<>(orphanKeyMetaDataMap.keySet().size());
    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      for (Map.Entry<Long, OrphanKeyMetaData> entry :
          orphanKeyMetaDataMap.entrySet()) {
        try {
          Long key = entry.getKey();
          OrphanKeyMetaData orphanKeyMetaData =
              orphanKeyMetaDataMap.get(key);
          if (orphanKeyMetaData.getObjectIds().size() > 0) {
            orphanKeyMetaData.setStatus(
                NODESTATUS.ORPHAN_PARENT_NODE_UPDATE_STATUS_COMPLETE
                    .getValue());
            reconNamespaceSummaryManager.batchStoreOrphanKeyMetaData(
                rdbBatchOperation, key, orphanKeyMetaData);
          } else {
            orphanKeysParentIdListToBeDeleted.add(key);
          }
        } catch (IOException e) {
          LOG.error("Unable to write orphan keys meta data in Recon DB.",
              e);
          throw e;
        }
      }
      if (orphanKeysParentIdListToBeDeleted.size() > 0) {
        deleteOrphanKeysMetaDataFromDB(orphanKeysParentIdListToBeDeleted);
        orphanKeyMetaDataMap.keySet()
            .removeAll(orphanKeysParentIdListToBeDeleted);
      }
      try {
        reconNamespaceSummaryManager.commitBatchOperation(rdbBatchOperation);
      } catch (IOException e) {
        LOG.error("Failed to commit batch operation for writing orphan keys " +
            "meta data in Recon DB.", e);
        throw e;
      }
    }
  }

  protected void handlePutKeyEvent(
      OmKeyInfo keyInfo, Map<Long, NSSummary> nsSummaryMap,
      Map<Long, OrphanKeyMetaData> orphanKeyMetaDataMap,
      long status) throws IOException {
    long parentObjectId = keyInfo.getParentObjectID();
    // Try to get the NSSummary from our local map that maps NSSummaries to IDs
    NSSummary nsSummary = nsSummaryMap.get(parentObjectId);
    if (nsSummary == null) {
      // If we don't have it in this batch we try to get it from the DB
      nsSummary = reconNamespaceSummaryManager.getNSSummary(parentObjectId);
    }
    removeFromOrphanIfExists(keyInfo, orphanKeyMetaDataMap, status);
    if (nsSummary == null) {
      // If we don't have it locally and in the DB we create a new instance
      // as this is a new ID
      nsSummary = new NSSummary();
      // file might probably be present in bucket or directory,
      // however add as probable orphan.
      addOrphanCandidate(keyInfo, orphanKeyMetaDataMap,
          status, false);
    } else {
      addOrphanCandidate(keyInfo, orphanKeyMetaDataMap,
          status, true);
    }
    int numOfFile = nsSummary.getNumOfFiles();
    long sizeOfFile = nsSummary.getSizeOfFiles();
    int[] fileBucket = nsSummary.getFileSizeBucket();
    nsSummary.setNumOfFiles(numOfFile + 1);
    long dataSize = keyInfo.getDataSize();
    nsSummary.setSizeOfFiles(sizeOfFile + dataSize);
    int binIndex = ReconUtils.getFileSizeBinIndex(dataSize);

    ++fileBucket[binIndex];
    nsSummary.setFileSizeBucket(fileBucket);
    nsSummaryMap.put(parentObjectId, nsSummary);
  }

  protected void handlePutDirEvent(OmDirectoryInfo omDirectoryInfo,
                                   Map<Long, NSSummary> nsSummaryMap,
                                   Map<Long, OrphanKeyMetaData>
                                       orphanKeyMetaDataMap,
                                   long status)
      throws IOException {
    long parentObjectId = omDirectoryInfo.getParentObjectID();
    long objectId = omDirectoryInfo.getObjectID();
    // write the dir name to the current directory
    String dirName = omDirectoryInfo.getName();
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
    removeFromOrphanIfExists(omDirectoryInfo, orphanKeyMetaDataMap, status);
    // Write the child dir list to the parent directory
    // Try to get the NSSummary from our local map that maps NSSummaries to IDs
    NSSummary nsSummary = nsSummaryMap.get(parentObjectId);
    if (nsSummary == null) {
      // If we don't have it in this batch we try to get it from the DB
      nsSummary = reconNamespaceSummaryManager.getNSSummary(parentObjectId);
      if (nsSummary == null) {
        // If we don't have it locally and in the DB we create a new instance
        // as this is a new ID
        nsSummary = new NSSummary();
        addOrphanCandidate(omDirectoryInfo, orphanKeyMetaDataMap,
            status, false);
      } else {
        addOrphanCandidate(omDirectoryInfo, orphanKeyMetaDataMap,
            status, true);
      }
    }
    nsSummary.addChildDir(objectId);
    nsSummaryMap.put(parentObjectId, nsSummary);
  }

  private <T extends WithParentObjectId> void removeFromOrphanIfExists(
      T fileDirInfo,
      Map<Long, OrphanKeyMetaData> orphanKeyMetaDataMap, long status)
      throws IOException {
    // If object as parent has come, then its child are not orphan and can
    // remove parent from orphan map.
    if (null != orphanKeyMetaDataMap) {
      long objectID = fileDirInfo.getObjectID();
      OrphanKeyMetaData orphanKeyMetaData = orphanKeyMetaDataMap.get(objectID);
      if (null == orphanKeyMetaData) {
        orphanKeyMetaData =
            reconNamespaceSummaryManager.getOrphanKeyMetaData(objectID);
      }
      if (null != orphanKeyMetaData) {
        orphanKeyMetaData.setStatus(status);
        orphanKeyMetaData.getObjectIds().clear();
      }
    }
  }

  protected void handleDeleteKeyEvent(
      OmKeyInfo keyInfo,
      Map<Long, NSSummary> nsSummaryMap,
      Map<Long, OrphanKeyMetaData> orphanKeyMetaDataMap)
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
    int binIndex = ReconUtils.getFileSizeBinIndex(dataSize);

    // decrement count, data size, and bucket count
    // even if there's no direct key, we still keep the entry because
    // we still need children dir IDs info
    nsSummary.setNumOfFiles(numOfFile - 1);
    nsSummary.setSizeOfFiles(sizeOfFile - dataSize);
    --fileBucket[binIndex];
    nsSummary.setFileSizeBucket(fileBucket);
    nsSummaryMap.put(parentObjectId, nsSummary);
    removeOrphanChild(keyInfo.getObjectID(), keyInfo.getParentObjectID(),
        orphanKeyMetaDataMap);
  }

  private <T extends WithParentObjectId> void removeOrphanChild(
      long objectID,
      long parentObjectID,
      Map<Long, OrphanKeyMetaData> orphanKeyMetaDataMap) throws IOException {
    if (null != orphanKeyMetaDataMap) {
      OrphanKeyMetaData orphanKeyMetaData =
          orphanKeyMetaDataMap.get(parentObjectID);
      if (null == orphanKeyMetaData) {
        orphanKeyMetaData =
            reconNamespaceSummaryManager.getOrphanKeyMetaData(
                parentObjectID);
      }
      if (null != orphanKeyMetaData) {
        Set<Long> objectIds = orphanKeyMetaData.getObjectIds();
        objectIds.remove(objectID);
        orphanKeyMetaDataMap.put(parentObjectID, orphanKeyMetaData);
      }
    }
  }

  protected void handleDeleteDirEvent(
      OmDirectoryInfo directoryInfo,
      Map<Long, NSSummary> nsSummaryMap,
      Map<Long, OrphanKeyMetaData> orphanKeyMetaDataMap)
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
    removeOrphanChild(directoryInfo.getObjectID(),
        directoryInfo.getParentObjectID(), orphanKeyMetaDataMap);
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

  protected boolean writeFlushAndCommitOrphanKeysMetaDataToDB(
      Map<Long, OrphanKeyMetaData> orphanKeyMetaDataMap) {
    try {
      writeOrphanKeysMetaDataToDB(orphanKeyMetaDataMap);
      orphanKeyMetaDataMap.clear();
    } catch (IOException e) {
      LOG.error("Unable to write orphan keys meta data in Recon DB.", e);
      return false;
    }
    return true;
  }

  protected boolean checkOrphanDataAndCallWriteFlushToDB(
      Map<Long, OrphanKeyMetaData> orphanKeyMetaDataMap) {
    // if map contains more than entries, flush to DB and clear the map
    if (null != orphanKeyMetaDataMap && orphanKeyMetaDataMap.size() >=
        orphanKeysFlushToDBMaxThreshold) {
      return writeFlushAndCommitOrphanKeysMetaDataToDB(orphanKeyMetaDataMap);
    }
    return true;
  }

  protected void deleteOrphanKeysMetaDataFromDB(
      List<Long> orphanKeysParentIdList) throws IOException {
    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      for (Long parentId : orphanKeysParentIdList) {
        try {
          reconNamespaceSummaryManager.batchDeleteOrphanKeyMetaData(
              rdbBatchOperation, parentId);
        } catch (IOException e) {
          LOG.error(
              "Unable to delete orphan keys from orphanKeysMetaDataTable " +
                  "in Recon DB.", e);
          throw e;
        }
      }
      try {
        reconNamespaceSummaryManager.commitBatchOperation(rdbBatchOperation);
      } catch (IOException e) {
        LOG.error("Failed to commit batch operation for deleting orphan keys " +
            "meta data in Recon DB.", e);
        throw e;
      }
    }
  }

  protected boolean batchDeleteAndCommitOrphanKeysMetaDataToDB(
      List<Long> orphanKeysParentIdList) {
    try {
      deleteOrphanKeysMetaDataFromDB(orphanKeysParentIdList);
      orphanKeysParentIdList.clear();
    } catch (IOException e) {
      LOG.error("Unable to delete orphan keys meta data from Recon DB.", e);
      return false;
    }
    return true;
  }

  protected boolean checkOrphanDataThresholdAndAddToDeleteBatch(
      List<Long> orphanKeysParentIdList) {
    // if map contains more than entries, flush to DB and clear the map
    if (null != orphanKeysParentIdList && orphanKeysParentIdList.size() >=
        orphanKeysFlushToDBMaxThreshold) {
      return batchDeleteAndCommitOrphanKeysMetaDataToDB(orphanKeysParentIdList);
    }
    return true;
  }

  private <T extends WithParentObjectId> void addOrphanCandidate(
      T fileDirObjInfo,
      Map<Long, OrphanKeyMetaData> orphanKeyMetaDataMap,
      long status,
      boolean parentExist)
      throws IOException {
    if (null != orphanKeyMetaDataMap) {
      long objectID = fileDirObjInfo.getObjectID();
      long parentObjectID = fileDirObjInfo.getParentObjectID();
      // if parent exist in NSSummaryMap, need to check if parent is already an
      // orphan or not using orphanKeyMetaData. If already orphan, then add
      // this child also in orphan list.
      if (parentExist) {
        OrphanKeyMetaData orphanKeyMetaData =
            orphanKeyMetaDataMap.get(parentObjectID);
        if (null == orphanKeyMetaData) {
          orphanKeyMetaData =
              reconNamespaceSummaryManager.getOrphanKeyMetaData(
                  parentObjectID);
        }
        if (null != orphanKeyMetaData) {
          Set<Long> objectIds = orphanKeyMetaData.getObjectIds();
          objectIds.add(objectID);
          orphanKeyMetaDataMap.put(parentObjectID, orphanKeyMetaData);
        }
      // If parent does not exist in NSSummaryMap, then the child can be orphan.
      } else {
        Set<Long> objectIds = new HashSet<>();
        objectIds.add(objectID);
        OrphanKeyMetaData orphanKeyMetaData =
            new OrphanKeyMetaData(objectIds, status);
        orphanKeyMetaDataMap.put(parentObjectID, orphanKeyMetaData);
      }
    }
  }

  protected boolean verifyOrphanParentsForBucket(
      Set<Long> bucketObjectIdsSet,
      List<Long> toBeDeletedBucketObjectIdsFromOrphanMap)
      throws IOException {
    // if orphan parentId matches bucket, and bucket exist, then its not orphan
    // (as bucket is not present in key/file table as parent) and remove from
    // orphan map.
    try (TableIterator<Long, ? extends Table.KeyValue<Long,
        OrphanKeyMetaData>> orphanKeysMetaDataIter =
             orphanKeysMetaDataTable.iterator()) {
      while (orphanKeysMetaDataIter.hasNext()) {
        Table.KeyValue<Long, OrphanKeyMetaData> keyValue =
            orphanKeysMetaDataIter.next();
        Long parentId = keyValue.getKey();
        if (bucketObjectIdsSet.contains(parentId)) {
          toBeDeletedBucketObjectIdsFromOrphanMap.add(parentId);
          if (!checkOrphanDataThresholdAndAddToDeleteBatch(
              toBeDeletedBucketObjectIdsFromOrphanMap)) {
            return false;
          }
        }
      }
      return true;
    }
  }

  /**
   * States that represent if orphan's parent node metadata
   * update is in progress or completed to avoid dirty read.
   */
  public enum NODESTATUS {
    ORPHAN_PARENT_NODE_UPDATE_STATUS_IN_PROGRESS(1),
    ORPHAN_PARENT_NODE_UPDATE_STATUS_COMPLETE(2);

    private final long value;

    /**
     * Constructs states.
     *
     * @param value  Enum Value
     */
    NODESTATUS(long value) {
      this.value = value;
    }

    /**
     * Returns the in progress status.
     *
     * @return progress status.
     */
    public static NODESTATUS getOrphanParentNodeUpdateStatusInProgress() {
      return ORPHAN_PARENT_NODE_UPDATE_STATUS_IN_PROGRESS;
    }

    /**
     * Returns the completed status.
     *
     * @return completed status.
     */
    public static NODESTATUS getOrphanParentNodeUpdateStatusComplete() {
      return ORPHAN_PARENT_NODE_UPDATE_STATUS_COMPLETE;
    }

    /**
     * returns the numeric value associated with the endPoint.
     *
     * @return int.
     */
    public long getValue() {
      return value;
    }
  }

  protected void handlePutDeleteDirEvent(
      OmKeyInfo updatedKeyInfo,
      Map<Long, OrphanKeyMetaData> orphanKeyMetaDataMap,
      long status)
      throws IOException {
    // for cases where parent is not present and moved to deleted table,
    // still sub-files and sub-directory can not be marked as orphans,
    // so its removed from orphan map.
    removeFromOrphanIfExists(updatedKeyInfo, orphanKeyMetaDataMap, status);
  }

  protected void handleDeleteEvent(
      long objectID,
      long parentObjectID,
      String volBucketId,
      Map<Long, OrphanKeyMetaData> orphanKeyMetaDataMap,
      long status) throws IOException {
    try {
      NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectID);
      if (null != nsSummary) {
        if (nsSummary.getChildDir().size() != 0 ||
            nsSummary.getNumOfFiles() != 0) {
          addToOrphanMetaData(objectID, volBucketId, nsSummary,
              orphanKeyMetaDataMap, status);
        }
      }
      removeOrphanChild(objectID, parentObjectID, orphanKeyMetaDataMap);
    } catch (IOException e) {
      LOG.info("ObjectId {} may not be found in orphan metadata table or " +
          "namespaceSummaryTable.", objectID);
      throw e;
    }
  }

  protected void addToOrphanMetaData(
      long objectID, String volBucketId,
      NSSummary nsSummary,
      Map<Long, OrphanKeyMetaData> orphanKeyMetaDataMap,
      long status) throws IOException {
    try {
      final String[] keys = volBucketId.split(OM_KEY_PREFIX);
      final long volumeId = Long.parseLong(keys[1]);
      final long bucketId = Long.parseLong(keys[2]);
      List<Long> deletedFileObjectIds =
          getPendingDeletionSubFilesObjectIds(volumeId, bucketId, objectID);
      Set<Long> objectIds = new HashSet<>();
      objectIds.addAll(nsSummary.getChildDir());
      objectIds.addAll(deletedFileObjectIds);
      OrphanKeyMetaData orphanKeyMetaData =
          new OrphanKeyMetaData(objectIds, status);
      orphanKeyMetaDataMap.put(objectID, orphanKeyMetaData);
    } catch (IOException e) {
      LOG.error(
          "Error while fetching pending for delete sub files object ids : {}",
          e);
      throw e;
    }
  }

  private List<Long> getPendingDeletionSubFilesObjectIds(
      long volumeId, long bucketId, long objectID)
      throws IOException {
    List<Long> fileObjectIds = new ArrayList<>();
    String seekFileInDB =
        reconOMMetadataManager.getOzonePathKey(volumeId, bucketId, objectID,
            "");

    Table fileTable = reconOMMetadataManager.getFileTable();
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
             iterator = fileTable.iterator()) {

      iterator.seek(seekFileInDB);

      while (iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> entry = iterator.next();
        OmKeyInfo fileInfo = entry.getValue();
        if (!OMFileRequest.isImmediateChild(fileInfo.getParentObjectID(),
            objectID)) {
          break;
        }
        fileObjectIds.add(fileInfo.getObjectID());
      }
    }
    return fileObjectIds;
  }
}
