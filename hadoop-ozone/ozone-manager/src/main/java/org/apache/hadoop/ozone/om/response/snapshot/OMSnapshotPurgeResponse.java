/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.response.snapshot;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.SNAPSHOT_INFO_TABLE;

/**
 * Response for OMSnapshotPurgeRequest.
 */
@CleanupTableInfo(cleanupTables = {SNAPSHOT_INFO_TABLE})
public class OMSnapshotPurgeResponse extends OMClientResponse {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMSnapshotPurgeResponse.class);
  private final List<String> snapshotDbKeys;
  private final Map<String, SnapshotInfo> updatedSnapInfos;

  public OMSnapshotPurgeResponse(@Nonnull OMResponse omResponse,
      @Nonnull List<String> snapshotDbKeys,
      Map<String, SnapshotInfo> updatedSnapInfos) {
    super(omResponse);
    this.snapshotDbKeys = snapshotDbKeys;
    this.updatedSnapInfos = updatedSnapInfos;
  }

  /**
   * Constructor for failed request.
   * It should not be used for successful request.
   */
  public OMSnapshotPurgeResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
    this.snapshotDbKeys = null;
    this.updatedSnapInfos = null;
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    OmMetadataManagerImpl metadataManager = (OmMetadataManagerImpl)
        omMetadataManager;
    updateSnapInfo(metadataManager, batchOperation);
    for (String dbKey: snapshotDbKeys) {
      SnapshotInfo snapshotInfo = omMetadataManager
          .getSnapshotInfoTable().get(dbKey);
      // Even though snapshot existed when SnapshotDeletingService
      // was running. It might be deleted in the previous run and
      // the DB might not have been updated yet. So snapshotInfo
      // can be null.
      if (snapshotInfo == null) {
        continue;
      }
      cleanupSnapshotChain(metadataManager, snapshotInfo, batchOperation);
      // Delete Snapshot checkpoint directory.
      deleteCheckpointDirectory(omMetadataManager, snapshotInfo);
      omMetadataManager.getSnapshotInfoTable().deleteWithBatch(batchOperation,
          dbKey);
    }
  }

  private void updateSnapInfo(OmMetadataManagerImpl metadataManager,
                              BatchOperation batchOp)
      throws IOException {
    for (Map.Entry<String, SnapshotInfo> entry : updatedSnapInfos.entrySet()) {
      metadataManager.getSnapshotInfoTable().putWithBatch(batchOp,
          entry.getKey(), entry.getValue());
    }
  }

  /**
   * Cleans up the snapshot chain and updates next snapshot's
   * previousPath and previousGlobal IDs.
   */
  private void cleanupSnapshotChain(OmMetadataManagerImpl metadataManager,
                                    SnapshotInfo snapInfo,
                                    BatchOperation batchOperation)
      throws IOException {
    SnapshotChainManager snapshotChainManager = metadataManager
        .getSnapshotChainManager();
    SnapshotInfo nextPathSnapInfo = null;
    SnapshotInfo nextGlobalSnapInfo;

    // If the snapshot is deleted in the previous run, then the in-memory
    // SnapshotChainManager might throw NoSuchElementException as the snapshot
    // is removed in-memory but OMDoubleBuffer has not flushed yet.
    boolean hasNextPathSnapshot;
    boolean hasNextGlobalSnapshot;
    try {
      hasNextPathSnapshot = snapshotChainManager.hasNextPathSnapshot(
          snapInfo.getSnapshotPath(), snapInfo.getSnapshotId());
      hasNextGlobalSnapshot = snapshotChainManager.hasNextGlobalSnapshot(
          snapInfo.getSnapshotId());
    } catch (NoSuchElementException ex) {
      LOG.warn("The Snapshot {} could have been deleted in the previous run.",
          snapInfo.getSnapshotId(), ex);
      return;
    }

    // Updates next path snapshot's previous snapshot ID
    if (hasNextPathSnapshot) {
      UUID nextPathSnapshotId = snapshotChainManager.nextPathSnapshot(
          snapInfo.getSnapshotPath(), snapInfo.getSnapshotId());

      String snapshotTableKey = snapshotChainManager
          .getTableKey(nextPathSnapshotId);
      nextPathSnapInfo = metadataManager.getSnapshotInfoTable()
          .get(snapshotTableKey);
      if (nextPathSnapInfo != null) {
        nextPathSnapInfo.setPathPreviousSnapshotId(
            snapInfo.getPathPreviousSnapshotId());
        metadataManager.getSnapshotInfoTable().putWithBatch(batchOperation,
            nextPathSnapInfo.getTableKey(), nextPathSnapInfo);
      }
    }

    // Updates next global snapshot's previous snapshot ID
    if (hasNextGlobalSnapshot) {
      UUID nextGlobalSnapshotId =
          snapshotChainManager.nextGlobalSnapshot(snapInfo.getSnapshotId());

      String snapshotTableKey = snapshotChainManager
          .getTableKey(nextGlobalSnapshotId);
      nextGlobalSnapInfo = metadataManager.getSnapshotInfoTable()
          .get(snapshotTableKey);
      // If both next global and path snapshot are same, it may overwrite
      // nextPathSnapInfo.setPathPreviousSnapshotID(), adding this check
      // will prevent it.
      if (nextGlobalSnapInfo != null && nextPathSnapInfo != null &&
          nextGlobalSnapInfo.getSnapshotId().equals(
              nextPathSnapInfo.getSnapshotId())) {
        nextPathSnapInfo.setGlobalPreviousSnapshotId(
            snapInfo.getPathPreviousSnapshotId());
        metadataManager.getSnapshotInfoTable().putWithBatch(batchOperation,
            nextPathSnapInfo.getTableKey(), nextPathSnapInfo);
      } else if (nextGlobalSnapInfo != null) {
        nextGlobalSnapInfo.setGlobalPreviousSnapshotId(
            snapInfo.getPathPreviousSnapshotId());
        metadataManager.getSnapshotInfoTable().putWithBatch(batchOperation,
            nextGlobalSnapInfo.getTableKey(), nextGlobalSnapInfo);
      }
    }

    // Removes current snapshot from the snapshot chain.
    snapshotChainManager.deleteSnapshot(snapInfo);
  }

  /**
   * Deletes the checkpoint directory for a snapshot.
   */
  private void deleteCheckpointDirectory(OMMetadataManager omMetadataManager,
                                         SnapshotInfo snapshotInfo) {
    RDBStore store = (RDBStore) omMetadataManager.getStore();
    String checkpointPrefix = store.getDbLocation().getName();
    Path snapshotDirPath = Paths.get(store.getSnapshotsParentDir(),
        checkpointPrefix + snapshotInfo.getCheckpointDir());
    try {
      FileUtils.deleteDirectory(snapshotDirPath.toFile());
    } catch (IOException ex) {
      LOG.error("Failed to delete snapshot directory {} for snapshot {}",
          snapshotDirPath, snapshotInfo.getTableKey(), ex);
    }
  }
}
