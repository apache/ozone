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

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.SNAPSHOT_RENAMED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.SNAPSHOT_INFO_TABLE;

/**
 * Response for OMSnapshotCreateRequest.
 */
@CleanupTableInfo(cleanupTables = {
    DELETED_TABLE, SNAPSHOT_RENAMED_TABLE, SNAPSHOT_INFO_TABLE})
public class OMSnapshotCreateResponse extends OMClientResponse {

  private SnapshotInfo snapshotInfo;

  public OMSnapshotCreateResponse(@Nonnull OMResponse omResponse,
      @Nonnull SnapshotInfo snapshotInfo) {
    super(omResponse);
    this.snapshotInfo = snapshotInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMSnapshotCreateResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // Note: It is intentional that we write SnapshotInfo to DB _before_
    // creating snapshot DB checkpoint. Otherwise there could be a
    // small chance that the DB listeners in RocksDBCheckpointDiffer could skip
    // the compaction tracking when the checkpoint is _just_ created and another
    // compaction happens. Even though the worst case if that happens it
    // would just negatively impact SnapDiff performance because of the missing
    // compaction DAG nodes. SnapDiff correctness will not be impacted by this.

    // Add to db
    String key = snapshotInfo.getTableKey();
    omMetadataManager.getSnapshotInfoTable().putWithBatch(batchOperation,
        key, snapshotInfo);

    // Create the snapshot checkpoint. Also cleans up some tables.
    OmSnapshotManager.createOmSnapshotCheckpoint(omMetadataManager,
        snapshotInfo);

    // TODO: [SNAPSHOT] Move to createOmSnapshotCheckpoint and add table lock
    // Remove all entries from snapshotRenamedTable
    try (TableIterator<String, ? extends Table.KeyValue<String, String>>
        iterator = omMetadataManager.getSnapshotRenamedTable().iterator()) {

      String dbSnapshotBucketKey = omMetadataManager.getBucketKey(
          snapshotInfo.getVolumeName(), snapshotInfo.getBucketName())
          + OM_KEY_PREFIX;
      iterator.seek(dbSnapshotBucketKey);

      while (iterator.hasNext()) {
        String renameDbKey = iterator.next().getKey();
        if (!renameDbKey.startsWith(dbSnapshotBucketKey)) {
          break;
        }
        omMetadataManager.getSnapshotRenamedTable()
            .deleteWithBatch(batchOperation, renameDbKey);
      }
    }
  }
}
