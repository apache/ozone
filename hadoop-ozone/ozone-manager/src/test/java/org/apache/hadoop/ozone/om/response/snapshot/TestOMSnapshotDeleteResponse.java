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

package org.apache.hadoop.ozone.om.response.snapshot;

import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateSnapshotResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteSnapshotResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * This class tests OMSnapshotDeleteResponse.
 */
public class TestOMSnapshotDeleteResponse {

  @TempDir
  private Path folder;
  
  private OMMetadataManager omMetadataManager;
  private BatchOperation batchOperation;
  private OzoneConfiguration ozoneConfiguration;

  @BeforeEach
  public void setup() throws Exception {
    ozoneConfiguration = new OzoneConfiguration();
    String fsPath = folder.toAbsolutePath().toString();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        fsPath);
    OzoneManager ozoneManager = mock(OzoneManager.class);
    OmSnapshotManager omSnapshotManager = mock(OmSnapshotManager.class);
    OmSnapshotLocalDataManager omSnapshotLocalDataManager = mock(OmSnapshotLocalDataManager.class);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    when(ozoneManager.getOmSnapshotManager()).thenReturn(omSnapshotManager);
    when(omSnapshotManager.getSnapshotLocalDataManager()).thenReturn(omSnapshotLocalDataManager);
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, ozoneManager);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
  }

  @AfterEach
  public void tearDown() {
    if (batchOperation != null) {
      batchOperation.close();
    }
  }

  @Test
  public void testAddToDBBatch() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String snapshotName = UUID.randomUUID().toString();
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance(volumeName,
        bucketName,
        snapshotName,
        snapshotId,
        Time.now());

    // confirm table is empty
    assertEquals(0,
        omMetadataManager
        .countRowsInTable(omMetadataManager.getSnapshotInfoTable()));

    // Prepare the table, write an entry with SnapshotCreate
    OMSnapshotResponseTestUtil.addVolumeBucketInfoToTable(
        omMetadataManager, volumeName, bucketName);
    OMSnapshotCreateResponse omSnapshotCreateResponse =
        new OMSnapshotCreateResponse(OMResponse.newBuilder()
            .setCmdType(Type.CreateSnapshot)
            .setStatus(Status.OK)
            .setCreateSnapshotResponse(
                CreateSnapshotResponse.newBuilder()
                    .setSnapshotInfo(snapshotInfo.getProtobuf())
                    .build()
            ).build(), snapshotInfo);
    omSnapshotCreateResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // Confirm snapshot directory was created
    String snapshotDir = OmSnapshotManager.getSnapshotPath(ozoneConfiguration,
        snapshotInfo, 0);
    assertTrue((new File(snapshotDir)).exists());

    // Confirm table has 1 entry
    assertEquals(1, omMetadataManager
        .countRowsInTable(omMetadataManager.getSnapshotInfoTable()));

    try (Table.KeyValueIterator<String, SnapshotInfo> iter =
             omMetadataManager.getSnapshotInfoTable().iterator()) {
      // Check snapshotInfo entry content
      Table.KeyValue<String, SnapshotInfo> keyValue = iter.next();
      SnapshotInfo storedInfo = keyValue.getValue();
      assertEquals(snapshotInfo.getTableKey(), keyValue.getKey());
      assertEquals(snapshotInfo, storedInfo);
      assertEquals(SNAPSHOT_ACTIVE,
          snapshotInfo.getSnapshotStatus());
    }

    // Update snapshot status to DELETED
    snapshotInfo.setSnapshotStatus(SNAPSHOT_DELETED);

    // Trigger OMSnapshotDeleteResponse#addToDBBatch
    OMSnapshotDeleteResponse omSnapshotDeleteResponse =
        new OMSnapshotDeleteResponse(OMResponse.newBuilder()
            .setCmdType(Type.DeleteSnapshot)
            .setStatus(Status.OK)
            .setDeleteSnapshotResponse(
                DeleteSnapshotResponse.newBuilder().build()
            ).build(), snapshotInfo.getTableKey(), snapshotInfo);
    omSnapshotDeleteResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // Confirm addToDBBatch result
    // 1. The table still has 1 entry
    assertEquals(1, omMetadataManager
        .countRowsInTable(omMetadataManager.getSnapshotInfoTable()));

    try (Table.KeyValueIterator<String, SnapshotInfo> iter =
             omMetadataManager.getSnapshotInfoTable().iterator()) {
      // 2. snapshot status should now be DELETED
      Table.KeyValue<String, SnapshotInfo> keyValue = iter.next();
      SnapshotInfo storedInfo = keyValue.getValue();
      assertEquals(snapshotInfo.getTableKey(), keyValue.getKey());
      assertEquals(snapshotInfo, storedInfo);
      assertEquals(SNAPSHOT_DELETED,
          snapshotInfo.getSnapshotStatus());
    }
  }

}
