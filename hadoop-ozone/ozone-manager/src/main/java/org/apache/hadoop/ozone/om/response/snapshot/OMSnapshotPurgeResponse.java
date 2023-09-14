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
  private final Map<String, SnapshotInfo> updatedPreviousAndGlobalSnapInfos;

  public OMSnapshotPurgeResponse(
      @Nonnull OMResponse omResponse,
      @Nonnull List<String> snapshotDbKeys,
      Map<String, SnapshotInfo> updatedSnapInfos,
      Map<String, SnapshotInfo> updatedPreviousAndGlobalSnapInfos
  ) {
    super(omResponse);
    this.snapshotDbKeys = snapshotDbKeys;
    this.updatedSnapInfos = updatedSnapInfos;
    this.updatedPreviousAndGlobalSnapInfos = updatedPreviousAndGlobalSnapInfos;
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
    this.updatedPreviousAndGlobalSnapInfos = null;
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    OmMetadataManagerImpl metadataManager = (OmMetadataManagerImpl)
        omMetadataManager;
    updateSnapInfo(metadataManager, batchOperation, updatedSnapInfos);
    updateSnapInfo(metadataManager, batchOperation,
        updatedPreviousAndGlobalSnapInfos);
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

      // Delete Snapshot checkpoint directory.
      deleteCheckpointDirectory(omMetadataManager, snapshotInfo);
      omMetadataManager.getSnapshotInfoTable().deleteWithBatch(batchOperation,
          dbKey);
    }
  }

  private void updateSnapInfo(OmMetadataManagerImpl metadataManager,
                              BatchOperation batchOp,
                              Map<String, SnapshotInfo> snapshotInfos)
      throws IOException {
    for (Map.Entry<String, SnapshotInfo> entry : snapshotInfos.entrySet()) {
      metadataManager.getSnapshotInfoTable().putWithBatch(batchOp,
          entry.getKey(), entry.getValue());
    }
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
