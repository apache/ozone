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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmMetadataReader;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotCreateResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotPurgeResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotPurgeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests OMSnapshotPurgeRequest class.
 */
public class TestOMSnapshotPurgeRequestAndResponse {

  private BatchOperation batchOperation;
  private List<Path> checkpointPaths = new ArrayList<>();

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;
  private AuditLogger auditLogger;

  private String volumeName;
  private String bucketName;
  private String keyName;


  // Just setting ozoneManagerDoubleBuffer which does nothing.
  private static OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper =
      ((response, transactionIndex) -> {
        return null;
      });

  @BeforeEach
  public void setup() throws Exception {
    File testDir = GenericTestUtils.getRandomizedTestDir();
    ozoneManager = Mockito.mock(OzoneManager.class);
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(0);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
    when(ozoneManager.isRatisEnabled()).thenReturn(true);
    auditLogger = Mockito.mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        testDir.getAbsolutePath());
    ozoneConfiguration.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        testDir.getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    when(ozoneManager.isAdmin(any(UserGroupInformation.class)))
        .thenReturn(true);

    OmMetadataReader omMetadataReader = Mockito.mock(OmMetadataReader.class);
    when(ozoneManager.getOmMetadataReader()).thenReturn(omMetadataReader);
    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    keyName = UUID.randomUUID().toString();
  }

  /**
   * Creates volume, bucket and snapshot entries.
   */
  private List<String> createSnapshots(int numSnapshotKeys)
      throws Exception {

    Random random = new Random();
    // Add volume, bucket and key entries to OM DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    // Create Snapshot and CheckpointDir
    List<String> purgeSnapshots = new ArrayList<>(numSnapshotKeys);
    for (int i = 1; i <= numSnapshotKeys; i++) {
      String snapshotName = keyName + "-" + random.nextLong();
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

  private void purgeSnapshots(OMRequest snapshotPurgeRequest)
      throws IOException {
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
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {

    List<String> snapshotDbKeysToPurge = createSnapshots(10);
    Assertions.assertFalse(omMetadataManager.getSnapshotInfoTable().isEmpty());
    OMRequest snapshotPurgeRequest = createPurgeKeysRequest(
        snapshotDbKeysToPurge);
    purgeSnapshots(snapshotPurgeRequest);

    // Check if the entries are deleted.
    Assertions.assertTrue(omMetadataManager.getSnapshotInfoTable().isEmpty());

    // Check if all the checkpoints are cleared.
    for (Path checkpoint : checkpointPaths) {
      Assertions.assertFalse(Files.exists(checkpoint));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 4})
  public void testSnapshotChainCleanup(int index) throws Exception {
    List<String> snapshots = createSnapshots(5);
    String snapShotToPurge = snapshots.get(index);

    // Before purge, check snapshot chain
    OmMetadataManagerImpl metadataManager =
        (OmMetadataManagerImpl) omMetadataManager;
    SnapshotChainManager chainManager = metadataManager
        .getSnapshotChainManager();
    SnapshotInfo snapInfo = metadataManager.getSnapshotInfoTable()
        .get(snapShotToPurge);

    // Get previous and next snapshotInfos to verify if the SnapInfo
    // is changed.
    String prevPathSnapId = null;
    String prevGlobalSnapId = null;
    String nextPathSnapId = null;
    String nextGlobalSnapId = null;

    if (chainManager.hasPreviousPathSnapshot(snapInfo.getSnapshotPath(),
        snapInfo.getSnapshotID())) {
      prevPathSnapId = chainManager.previousPathSnapshot(
          snapInfo.getSnapshotPath(), snapInfo.getSnapshotID());
    }
    if (chainManager.hasPreviousGlobalSnapshot(snapInfo.getSnapshotID())) {
      prevGlobalSnapId = chainManager.previousGlobalSnapshot(
          snapInfo.getSnapshotID());
    }
    if (chainManager.hasNextPathSnapshot(snapInfo.getSnapshotPath(),
        snapInfo.getSnapshotID())) {
      nextPathSnapId = chainManager.nextPathSnapshot(
          snapInfo.getSnapshotPath(), snapInfo.getSnapshotID());
    }
    if (chainManager.hasNextGlobalSnapshot(snapInfo.getSnapshotID())) {
      nextGlobalSnapId = chainManager.nextGlobalSnapshot(
          snapInfo.getSnapshotID());
    }

    long rowsInTableBeforePurge = omMetadataManager
        .countRowsInTable(omMetadataManager.getSnapshotInfoTable());
    // Purge Snapshot of the given index.
    List<String> toPurgeList = Collections.singletonList(snapShotToPurge);
    OMRequest snapshotPurgeRequest = createPurgeKeysRequest(
        toPurgeList);
    purgeSnapshots(snapshotPurgeRequest);

    // After purge, check snapshot chain.
    if (nextPathSnapId != null) {
      SnapshotInfo nextPathSnapshotInfoAfterPurge = metadataManager
          .getSnapshotInfoTable().get(chainManager.getTableKey(nextPathSnapId));
      Assertions.assertEquals(nextPathSnapshotInfoAfterPurge
          .getGlobalPreviousSnapshotID(), prevPathSnapId);
    }

    if (nextGlobalSnapId != null) {
      SnapshotInfo nextGlobalSnapshotInfoAfterPurge = metadataManager
          .getSnapshotInfoTable().get(chainManager
              .getTableKey(nextGlobalSnapId));
      Assertions.assertEquals(nextGlobalSnapshotInfoAfterPurge
          .getGlobalPreviousSnapshotID(), prevGlobalSnapId);
    }

    Assertions.assertNotEquals(rowsInTableBeforePurge, omMetadataManager
        .countRowsInTable(omMetadataManager.getSnapshotInfoTable()));
  }
}
