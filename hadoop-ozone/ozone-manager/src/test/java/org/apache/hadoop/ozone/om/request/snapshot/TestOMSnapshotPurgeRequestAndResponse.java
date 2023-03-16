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

package org.apache.hadoop.ozone.om.request.snapshot;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.TestOMKeyRequest;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotCreateResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotPurgeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotPurgeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests OMSnapshotPurgeRequest class.
 */
public class TestOMSnapshotPurgeRequestAndResponse extends TestOMKeyRequest {

  private BatchOperation batchOperation;
  private List<Path> checkpointPaths = new ArrayList<>();

  /**
   * Creates volume, bucket and snapshot entries.
   */
  private List<String> createSnapshots(int numSnapshotKeys)
      throws Exception {

    // Add volume, bucket and key entries to OM DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    // Create Snapshot and CheckpointDir
    List<String> purgeSnapshots = new ArrayList<>(numSnapshotKeys);
    for (int i = 1; i <= numSnapshotKeys; i++) {
      String snapshotName = keyName + "-" + i;
      createSnapshotCheckpoint(snapshotName);
      purgeSnapshots.add(SnapshotInfo.getTableKey(volumeName,
          bucketName, snapshotName));
    }

    return purgeSnapshots;
  }

  /**
   * Create OMRequest which encapsulates SnapshotPurgeRequest.
   *
   * @return OMRequest
   */
  private OMRequest createPurgeKeysRequest(List<String> purgeSnapshotKeys) {
    SnapshotPurgeRequest snapshotPurgeRequest = SnapshotPurgeRequest
        .newBuilder()
        .addAllSnapshotDBKeys(purgeSnapshotKeys)
        .build();

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(Type.SnapshotPurge)
        .setSnapshotPurgeRequest(snapshotPurgeRequest)
        .setClientId(UUID.randomUUID().toString())
        .build();

    return omRequest;
  }

  /**
   * Create snapshot and checkpoint directory.
   */
  private void createSnapshotCheckpoint(String snapshotName) throws Exception {
    when(ozoneManager.isAdmin(any())).thenReturn(true);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
    OMRequest omRequest = OMRequestTestUtils
        .createSnapshotRequest(volumeName, bucketName, snapshotName);
    // Pre-Execute OMSnapshotCreateRequest.
    OMSnapshotCreateRequest omSnapshotCreateRequest =
        TestOMSnapshotCreateRequest.doPreExecute(omRequest, ozoneManager);

    // validateAndUpdateCache OMSnapshotCreateResponse.
    OMSnapshotCreateResponse omClientResponse = (OMSnapshotCreateResponse)
        omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);
    // Add to batch and commit to DB.
    omClientResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    batchOperation.close();

    String key = SnapshotInfo.getTableKey(volumeName,
        bucketName, snapshotName);
    SnapshotInfo snapshotInfo =
        omMetadataManager.getSnapshotInfoTable().get(key);
    Assertions.assertNotNull(snapshotInfo);

    RDBStore store = (RDBStore) omMetadataManager.getStore();
    String checkpointPrefix = store.getDbLocation().getName();
    Path snapshotDirPath = Paths.get(store.getSnapshotsParentDir(),
        checkpointPrefix + snapshotInfo.getCheckpointDir());
    //Check the DB is still there
    Assertions.assertTrue(Files.exists(snapshotDirPath));
    checkpointPaths.add(snapshotDirPath);
  }

  private OMSnapshotPurgeRequest preExecute(OMRequest originalOmRequest)
      throws IOException {
    OMSnapshotPurgeRequest omSnapshotPurgeRequest =
        new OMSnapshotPurgeRequest(originalOmRequest);
    OMRequest modifiedOmRequest = omSnapshotPurgeRequest
        .preExecute(ozoneManager);
    return new OMSnapshotPurgeRequest(modifiedOmRequest);
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {

    List<String> snapshotDbKeysToPurge = createSnapshots(10);
    OMRequest snapshotPurgeRequest = createPurgeKeysRequest(
        snapshotDbKeysToPurge);
    // Pre-Execute OMSnapshotPurgeRequest
    OMSnapshotPurgeRequest omSnapshotPurgeRequest =
        preExecute(snapshotPurgeRequest);

    // validateAndUpdateCache for OMSnapshotPurgeRequest.
    OMSnapshotPurgeResponse omSnapshotPurgeResponse = (OMSnapshotPurgeResponse)
        omSnapshotPurgeRequest.validateAndUpdateCache(ozoneManager, 200L,
        ozoneManagerDoubleBufferHelper);

    // Commit to DB.
    batchOperation = omMetadataManager.getStore().initBatchOperation();
    omSnapshotPurgeResponse.checkAndUpdateDB(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // Check if the entries are deleted.
    Assertions.assertTrue(omMetadataManager.getSnapshotInfoTable().isEmpty());

    // Check if all the checkpoints are cleared.
    for (Path checkpoint : checkpointPaths) {
      Assertions.assertFalse(Files.exists(checkpoint));
    }
  }

}
