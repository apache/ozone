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

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.SNAPSHOT_INFO_TABLE;

/**
 * Response for OMSnapshotPurgeRequest.
 */
@CleanupTableInfo(cleanupTables = {SNAPSHOT_INFO_TABLE})
public class OMSnapshotPurgeResponse extends OMClientResponse {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMSnapshotPurgeResponse.class);
  private List<String> snapshotDbKeys;

  public OMSnapshotPurgeResponse(@Nonnull OMResponse omResponse,
      @Nonnull List<String> snapshotDbKeys) {
    super(omResponse);
    this.snapshotDbKeys = snapshotDbKeys;
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    OmMetadataManagerImpl metadataManager = (OmMetadataManagerImpl)
        omMetadataManager;
    for (String dbKey: snapshotDbKeys) {
      SnapshotInfo snapshotInfo = omMetadataManager
          .getSnapshotInfoTable().get(dbKey);
      cleanupSnapshotChain(metadataManager, snapshotInfo, batchOperation);
      // Delete Snapshot checkpoint directory.
      deleteCheckpointDirectory(omMetadataManager, snapshotInfo);
      omMetadataManager.getSnapshotInfoTable().deleteWithBatch(batchOperation,
          dbKey);
    }
  }

  /**
   * Cleans up the snapshot chain and updates next snapshot's
   * previousPath and previousGlobal IDs.
   * @param metadataManager
   * @param snapInfo
   * @param batchOperation
   */
  private void cleanupSnapshotChain(OmMetadataManagerImpl metadataManager,
      SnapshotInfo snapInfo, BatchOperation batchOperation) throws IOException {
    SnapshotChainManager snapshotChainManager = metadataManager
        .getSnapshotChainManager();

    // Updates next path snapshot's previous snapshot ID
    if (snapshotChainManager.hasNextPathSnapshot(
        snapInfo.getSnapshotPath(), snapInfo.getSnapshotID())) {
      String nextPathSnapshotId =
          snapshotChainManager.nextPathSnapshot(
              snapInfo.getSnapshotPath(), snapInfo.getSnapshotID());

      String snapshotTableKey = snapshotChainManager
          .getTableKey(nextPathSnapshotId);
      SnapshotInfo nextPathSnapInfo =
          metadataManager.getSnapshotInfoTable().get(snapshotTableKey);
      if (nextPathSnapInfo != null) {
        nextPathSnapInfo.setPathPreviousSnapshotID(
            snapInfo.getPathPreviousSnapshotID());
        metadataManager.getSnapshotInfoTable().putWithBatch(batchOperation,
            nextPathSnapInfo.getTableKey(), nextPathSnapInfo);
      }
    }

    // Updates next global snapshot's previous snapshot ID
    if (snapshotChainManager.hasNextGlobalSnapshot(
        snapInfo.getSnapshotID())) {
      String nextGlobalSnapshotId =
          snapshotChainManager.nextGlobalSnapshot(snapInfo.getSnapshotID());

      String snapshotTableKey = snapshotChainManager
          .getTableKey(nextGlobalSnapshotId);
      SnapshotInfo nextGlobalSnapInfo =
          metadataManager.getSnapshotInfoTable().get(snapshotTableKey);
      if (nextGlobalSnapInfo != null) {
        nextGlobalSnapInfo.setGlobalPreviousSnapshotID(
            snapInfo.getPathPreviousSnapshotID());
        metadataManager.getSnapshotInfoTable().putWithBatch(batchOperation,
            nextGlobalSnapInfo.getTableKey(), nextGlobalSnapInfo);
      }
    }

    // Removes current snapshot from the snapshot chain.
    snapshotChainManager.deleteSnapshot(snapInfo);
  }

  /**
   * Deletes the checkpoint directory for a snapshot.
   * @param omMetadataManager
   * @param snapshotInfo
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
