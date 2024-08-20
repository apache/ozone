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

import jakarta.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Stat;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.SNAPSHOT_INFO_TABLE;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.SNAPSHOT_LOCK;

/**
 * Response for OMSnapshotMoveDeletedKeysRequest.
 */
@CleanupTableInfo(cleanupTables = {SNAPSHOT_INFO_TABLE})
public class OMSnapshotPurgeAndMoveResponse extends OMClientResponse {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMSnapshotPurgeAndMoveResponse.class);

  private SnapshotInfo purgedSnapshot;
  private SnapshotInfo nextSnapshot;
  Map<String, SnapshotInfo> updatedSnapInfos;

  Path purgedSnapshotPath;
  private File statePath;

  enum State {
    STARTING, RENAME_TABLE_WRITE, RENAME_TABLE_LOAD, DELETED_TABLE_WRITE, DELETED_TABLE_LOAD, DELETED_DIR_TABLE_WRITE,
    DELETED_DIR_TABLE_LOAD
  }


  public OMSnapshotPurgeAndMoveResponse(@Nonnull OMResponse omResponse,
                                        @Nonnull SnapshotInfo purgedSnapshot,
                                        @Nonnull SnapshotInfo nextSnapshot,
                                        Map<String, SnapshotInfo> updatedSnapInfos) {
    super(omResponse);
    this.purgedSnapshot = purgedSnapshot;
    this.nextSnapshot = nextSnapshot;
    this.updatedSnapInfos = updatedSnapInfos;
  }

  private void writeState(State currentState) throws IOException {
    try (FileWriter writer = new FileWriter(statePath)) {
      writer.write(currentState.toString());
    }
  }

  private State readState() throws IOException {
    return State.valueOf(StringUtils.bytes2String(Files.readAllBytes(statePath.toPath())));
  }

  private State writeAndLoadTable(OmSnapshotManager sourceSnapshotManager, OMMetadataManager targetMetadataManager,
                                  State currentState, State writeState, State loadState,
                                  String tableName, String prefix) throws IOException {

    File sstFile = new File(purgedSnapshotPath + "_" + tableName + ".sst");
    if (currentState != writeState) {
      if (sstFile.exists()) {
        sstFile.delete();
      }
      try (ReferenceCounted<OmSnapshot> snapshotReference = sourceSnapshotManager.getSnapshot(
          purgedSnapshot.getVolumeName(), purgedSnapshot.getBucketName(), purgedSnapshot.getName())) {
        OmSnapshot omSnapshot = snapshotReference.get();
        (omSnapshot.getMetadataManager().getTable(tableName)).dumpToFileWithPrefix(sstFile, prefix);
      }
      currentState = writeState;
      writeState(currentState);
    }
    if (!sstFile.exists()) {
      throw new IOException("Sst file created: "+ sstFile.getAbsolutePath() +" doesn't exist");
    }
    targetMetadataManager.getTable(tableName).loadFromFile(sstFile);
    currentState = loadState;
    writeState(currentState);
    sstFile.delete();
    return currentState;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMSnapshotPurgeAndMoveResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager, BatchOperation batchOperation) throws IOException {
    //Loading Deleted Table, RenameTable & DeletedDirectory table by creating a temporary sst file and ingesting it
    // in the rocksDB directly to avoid memory pressure on rocksdb which would have been the case if a batch would
    // have been happened. Here the entries would be directly written to a file and rocksdb would just add the file
    // in its meta.
    purgedSnapshotPath = OmSnapshotManager.getSnapshotPath(omMetadataManager, purgedSnapshot);
    statePath = new File( purgedSnapshotPath + "_state");
    OmSnapshotManager omSnapshotManager = ((OmMetadataManagerImpl) omMetadataManager)
        .getOzoneManager().getOmSnapshotManager();
    String dbBucketKey = omMetadataManager.getBucketKey(purgedSnapshot.getVolumeName(),
        purgedSnapshot.getBucketName());
    long volumeId = omMetadataManager.getVolumeId(purgedSnapshot.getVolumeName());
    long bucketId = omMetadataManager.getBucketTable().get(dbBucketKey).getObjectID();
    String dbBucketKeyForDir = omMetadataManager
        .getBucketKey(Long.toString(volumeId), Long.toString(bucketId)) + OM_KEY_PREFIX;
    String snapshotBucketKey = dbBucketKey + OzoneConsts.OM_KEY_PREFIX;

    State currentState = null;
    if (!statePath.exists() && purgedSnapshotPath.toFile().exists()) {
      currentState = State.STARTING;
    } else if (statePath.exists()) {
      currentState = readState();
    }
    ReferenceCounted<OmSnapshot> rcOmNextSnapshot = null;
    OMMetadataManager nextTableMetadataManager = omMetadataManager;
    if (nextSnapshot != null) {
      rcOmNextSnapshot = omSnapshotManager.getSnapshot(nextSnapshot.getVolumeName(), nextSnapshot.getBucketName(),
          nextSnapshot.getName());
      nextTableMetadataManager = rcOmNextSnapshot.get().getMetadataManager();
    }
    try {
      switch (currentState) {
      case STARTING:
      case RENAME_TABLE_WRITE:
        currentState = writeAndLoadTable(omSnapshotManager, nextTableMetadataManager, currentState,
            State.RENAME_TABLE_WRITE, State.RENAME_TABLE_LOAD, OmMetadataManagerImpl.SNAPSHOT_RENAMED_TABLE,
            snapshotBucketKey);
      case RENAME_TABLE_LOAD:
      case DELETED_TABLE_WRITE:
        currentState = writeAndLoadTable(omSnapshotManager, nextTableMetadataManager, currentState,
            State.DELETED_TABLE_WRITE, State.DELETED_TABLE_LOAD, OmMetadataManagerImpl.DELETED_TABLE,
            snapshotBucketKey);
      case DELETED_TABLE_LOAD:
      case DELETED_DIR_TABLE_WRITE:
        currentState = writeAndLoadTable(omSnapshotManager, nextTableMetadataManager, currentState,
            State.DELETED_DIR_TABLE_WRITE, State.DELETED_DIR_TABLE_LOAD, OmMetadataManagerImpl.DELETED_DIR_TABLE,
            dbBucketKeyForDir);
      }
      deleteCheckpointDirectory(omMetadataManager, purgedSnapshot);
      statePath.delete();
    } finally {
      IOUtils.closeQuietly(rcOmNextSnapshot);
    }
    for (Map.Entry<String, SnapshotInfo> updatedSnapshotInfo : updatedSnapInfos.entrySet()) {
      omMetadataManager.getSnapshotInfoTable().putWithBatch(batchOperation, updatedSnapshotInfo.getKey(),
          updatedSnapshotInfo.getValue());
    }
    omMetadataManager.getSnapshotInfoTable().deleteWithBatch(batchOperation, purgedSnapshot.getTableKey());
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


}

