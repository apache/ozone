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
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.SNAPSHOT_INFO_TABLE;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.SNAPSHOT_LOCK;

/**
 * Response for OMSnapshotPurgeRequest.
 */
@CleanupTableInfo(cleanupTables = {SNAPSHOT_INFO_TABLE})
public class OMSnapshotPurgeResponse extends OMClientResponse {
  private static final Logger LOG = LoggerFactory.getLogger(OMSnapshotPurgeResponse.class);

  private final OmPurgeResponse omPurgeResponse;

  /**
   * This is for the backward compatibility when OMSnapshotPurgeRequest has list of snapshots to purge.
   */
  @Deprecated
  private final List<OmPurgeResponse> omPurgeResponses;

  public OMSnapshotPurgeResponse(@Nonnull OMResponse omResponse, OmPurgeResponse omPurgeResponse) {
    super(omResponse);
    this.omPurgeResponse = omPurgeResponse;
    this.omPurgeResponses = null;
  }

  @Deprecated
  public OMSnapshotPurgeResponse(@Nonnull OMResponse omResponse, List<OmPurgeResponse> omPurgeResponses) {
    super(omResponse);
    this.omPurgeResponse = null;
    this.omPurgeResponses = omPurgeResponses;
  }

  /**
   * Constructor for failed request.
   * It should not be used for successful request.
   */
  public OMSnapshotPurgeResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
    this.omPurgeResponse = null;
    this.omPurgeResponses = null;
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager, BatchOperation batchOperation) throws IOException {
    if (omPurgeResponse != null) {
      addToDbBatch(omPurgeResponse, omMetadataManager, batchOperation);
    } else if (omPurgeResponses != null) {
      for (OmPurgeResponse purgeResponse : omPurgeResponses) {
        addToDbBatch(purgeResponse, omMetadataManager, batchOperation);
      }
    } else {
      throw new IllegalStateException("One of snapshotPurgeResponse or snapshotPurgeResponses should be present");
    }
  }

  private void addToDbBatch(OmPurgeResponse purgeResponse,
                            OMMetadataManager omMetadataManager,
                            BatchOperation batchOperation) throws IOException {

    OmMetadataManagerImpl metadataManager = (OmMetadataManagerImpl) omMetadataManager;
    // Order of transactions is flush next path level snapshot updates followed by next global snapshot and
    // next active snapshot. This order should not be changed unless the original order of the operations
    // is changed in OmSnapshotPurgeRequest.
    updateSnapInfo(metadataManager, batchOperation, purgeResponse.nextPathSnapshotInfo);
    updateSnapInfo(metadataManager, batchOperation, purgeResponse.nextGlobalSnapshotInfo);
    updateSnapInfo(metadataManager, batchOperation, purgeResponse.nextActiveSnapshotInfo);

    // Skip the cache here because snapshot is purged from cache in OMSnapshotPurgeRequest.
    SnapshotInfo snapshotInfo = omMetadataManager.getSnapshotInfoTable()
        .getSkipCache(purgeResponse.snapshotTableKey);

    // Even though snapshot existed when SnapshotDeletingService was running.
    // It might be deleted in the previous run and the DB might not have been updated yet.
    if (snapshotInfo != null) {
      // Delete Snapshot checkpoint directory.
      deleteCheckpointDirectory(omMetadataManager, snapshotInfo);
      // Finally, delete the snapshot.
      omMetadataManager.getSnapshotInfoTable().deleteWithBatch(batchOperation, purgeResponse.snapshotTableKey);
    } else {
      LOG.warn("Snapshot: '{}' is no longer exist in snapshot table. Might be removed in previous run.",
          purgeResponse.snapshotTableKey);
    }
  }

  private void updateSnapInfo(OmMetadataManagerImpl metadataManager,
                              BatchOperation batchOp,
                              SnapshotInfo snapshotInfo)
      throws IOException {
    if (snapshotInfo != null) {
      metadataManager.getSnapshotInfoTable().putWithBatch(batchOp, snapshotInfo.getTableKey(), snapshotInfo);
    }
  }

  /**
   * Deletes the checkpoint directory for a snapshot.
   */
  private void deleteCheckpointDirectory(OMMetadataManager omMetadataManager,
                                         SnapshotInfo snapshotInfo) {
    // Acquiring write lock to avoid race condition with sst filtering service which creates a sst filtered file
    // inside the snapshot directory. Any operation apart which doesn't create/delete files under this snapshot
    // directory can run in parallel along with this operation.
    OMLockDetails omLockDetails = omMetadataManager.getLock()
        .acquireWriteLock(SNAPSHOT_LOCK, snapshotInfo.getVolumeName(), snapshotInfo.getBucketName(),
            snapshotInfo.getName());
    boolean acquiredSnapshotLock = omLockDetails.isLockAcquired();
    if (acquiredSnapshotLock) {
      Path snapshotDirPath = OmSnapshotManager.getSnapshotPath(omMetadataManager, snapshotInfo);
      try {
        FileUtils.deleteDirectory(snapshotDirPath.toFile());
      } catch (IOException ex) {
        LOG.error("Failed to delete snapshot directory {} for snapshot {}",
            snapshotDirPath, snapshotInfo.getTableKey(), ex);
      } finally {
        omMetadataManager.getLock().releaseWriteLock(SNAPSHOT_LOCK, snapshotInfo.getVolumeName(),
            snapshotInfo.getBucketName(), snapshotInfo.getName());
      }
    }
  }

  /**
   * POJO to maintain the order of transactions when purge API is called with batch.
   */
  public static final class OmPurgeResponse {
    private final String snapshotTableKey;
    private final SnapshotInfo nextPathSnapshotInfo;
    private final SnapshotInfo nextGlobalSnapshotInfo;
    private final SnapshotInfo nextActiveSnapshotInfo;

    public OmPurgeResponse(@Nonnull String snapshotTableKey,
                           SnapshotInfo nextPathSnapshotInfo,
                           SnapshotInfo nextGlobalSnapshotInfo,
                           SnapshotInfo nextActiveSnapshotInfo) {
      this.snapshotTableKey = snapshotTableKey;
      this.nextPathSnapshotInfo = nextPathSnapshotInfo;
      this.nextGlobalSnapshotInfo = nextGlobalSnapshotInfo;
      this.nextActiveSnapshotInfo = nextActiveSnapshotInfo;
    }

    @Override
    public String toString() {
      return "{snapshotTableKey: '" + snapshotTableKey + '\'' +
          ", nextPathSnapshotInfo: '" +
          (nextPathSnapshotInfo != null ? nextPathSnapshotInfo.getName() : null) + '\'' +
          ", nextGlobalSnapshotInfo: '" +
          (nextGlobalSnapshotInfo != null ? nextGlobalSnapshotInfo.getName() : null) + '\'' +
          ", nextActiveSnapshotInfo: '" +
          (nextActiveSnapshotInfo != null ? nextActiveSnapshotInfo.getName() : null) + '\'' +
          '}';
    }
  }
}
