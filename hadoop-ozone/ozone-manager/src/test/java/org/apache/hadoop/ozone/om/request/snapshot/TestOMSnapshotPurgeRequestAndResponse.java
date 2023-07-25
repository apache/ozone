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
import org.apache.hadoop.ozone.om.IOmMetadataReader;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotCreateResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotPurgeResponse;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.om.snapshot.SnapshotCache;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotPurgeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
  private OmSnapshotManager omSnapshotManager;
  private AuditLogger auditLogger;

  private String volumeName;
  private String bucketName;
  private String keyName;

  // Just setting ozoneManagerDoubleBuffer which does nothing.
  private static final OzoneManagerDoubleBufferHelper
      DOUBLE_BUFFER_HELPER = ((response, transactionIndex) -> null);

  @BeforeEach
  public void setup() throws Exception {
    File testDir = GenericTestUtils.getRandomizedTestDir();
    ozoneManager = Mockito.mock(OzoneManager.class);
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.isAllowed(anyString())).thenReturn(true);
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
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    when(ozoneManager.isAdmin(any())).thenReturn(true);
    when(ozoneManager.isFilesystemSnapshotEnabled()).thenReturn(true);

    ReferenceCounted<IOmMetadataReader, SnapshotCache> rcOmMetadataReader =
        Mockito.mock(ReferenceCounted.class);
    when(ozoneManager.getOmMetadataReader()).thenReturn(rcOmMetadataReader);
    omSnapshotManager = new OmSnapshotManager(ozoneManager);
    when(ozoneManager.getOmSnapshotManager()).thenReturn(omSnapshotManager);
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
    createSnapshotCheckpoint(volumeName, bucketName, snapshotName);
  }

  private void createSnapshotCheckpoint(String volume,
                                        String bucket,
                                        String snapshotName) throws Exception {
    batchOperation = omMetadataManager.getStore().initBatchOperation();
    OMRequest omRequest = OMRequestTestUtils
        .createSnapshotRequest(volume, bucket, snapshotName);
    // Pre-Execute OMSnapshotCreateRequest.
    OMSnapshotCreateRequest omSnapshotCreateRequest =
        TestOMSnapshotCreateRequest.doPreExecute(omRequest, ozoneManager);

    // validateAndUpdateCache OMSnapshotCreateResponse.
    OMSnapshotCreateResponse omClientResponse = (OMSnapshotCreateResponse)
        omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 1,
            DOUBLE_BUFFER_HELPER);
    // Add to batch and commit to DB.
    omClientResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    batchOperation.close();

    String key = SnapshotInfo.getTableKey(volume, bucket, snapshotName);
    SnapshotInfo snapshotInfo =
        omMetadataManager.getSnapshotInfoTable().get(key);
    assertNotNull(snapshotInfo);

    RDBStore store = (RDBStore) omMetadataManager.getStore();
    String checkpointPrefix = store.getDbLocation().getName();
    Path snapshotDirPath = Paths.get(store.getSnapshotsParentDir(),
        checkpointPrefix + snapshotInfo.getCheckpointDir());
    // Check the DB is still there
    assertTrue(Files.exists(snapshotDirPath));
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
            DOUBLE_BUFFER_HELPER);

    // Commit to DB.
    batchOperation = omMetadataManager.getStore().initBatchOperation();
    omSnapshotPurgeResponse.checkAndUpdateDB(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {

    List<String> snapshotDbKeysToPurge = createSnapshots(10);
    assertFalse(omMetadataManager.getSnapshotInfoTable().isEmpty());
    OMRequest snapshotPurgeRequest = createPurgeKeysRequest(
        snapshotDbKeysToPurge);
    purgeSnapshots(snapshotPurgeRequest);

    // Check if the entries are deleted.
    assertTrue(omMetadataManager.getSnapshotInfoTable().isEmpty());

    // Check if all the checkpoints are cleared.
    for (Path checkpoint : checkpointPaths) {
      assertFalse(Files.exists(checkpoint));
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
    // Get previous and next snapshotInfos to verify if the SnapInfo
    // is changed.
    UUID prevPathSnapId = null;
    UUID prevGlobalSnapId = null;
    UUID nextPathSnapId = null;
    UUID nextGlobalSnapId = null;

    if (chainManager.hasPreviousPathSnapshot(snapInfo.getSnapshotPath(),
        snapInfo.getSnapshotId())) {
      prevPathSnapId = chainManager.previousPathSnapshot(
          snapInfo.getSnapshotPath(), snapInfo.getSnapshotId());
    }
    if (chainManager.hasPreviousGlobalSnapshot(snapInfo.getSnapshotId())) {
      prevGlobalSnapId = chainManager.previousGlobalSnapshot(
          snapInfo.getSnapshotId());
    }
    if (chainManager.hasNextPathSnapshot(snapInfo.getSnapshotPath(),
        snapInfo.getSnapshotId())) {
      nextPathSnapId = chainManager.nextPathSnapshot(
          snapInfo.getSnapshotPath(), snapInfo.getSnapshotId());
    }
    if (chainManager.hasNextGlobalSnapshot(snapInfo.getSnapshotId())) {
      nextGlobalSnapId = chainManager.nextGlobalSnapshot(
          snapInfo.getSnapshotId());
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
      assertEquals(nextPathSnapshotInfoAfterPurge
          .getGlobalPreviousSnapshotId(), prevPathSnapId);
    }

    if (nextGlobalSnapId != null) {
      SnapshotInfo nextGlobalSnapshotInfoAfterPurge = metadataManager
          .getSnapshotInfoTable().get(chainManager
              .getTableKey(nextGlobalSnapId));
      assertEquals(nextGlobalSnapshotInfoAfterPurge
          .getGlobalPreviousSnapshotId(), prevGlobalSnapId);
    }

    assertNotEquals(rowsInTableBeforePurge, omMetadataManager
        .countRowsInTable(omMetadataManager.getSnapshotInfoTable()));
  }

  private static Stream<Arguments> snapshotPurgeCases() {
    return Stream.of(
        Arguments.of(0, true),
        Arguments.of(1,  true),
        Arguments.of(2,  true),
        Arguments.of(3,  true),
        Arguments.of(4,  true),
        Arguments.of(5,  true),
        Arguments.of(6,  true),
        Arguments.of(7,  true),
        Arguments.of(8,  true),
        Arguments.of(0,  false),
        Arguments.of(1,  false),
        Arguments.of(2,  false),
        Arguments.of(3,  false),
        Arguments.of(4,  false),
        Arguments.of(5,  false),
        Arguments.of(6,  false),
        Arguments.of(7,  false),
        Arguments.of(8,  false)
    );
  }

  @ParameterizedTest
  @MethodSource("snapshotPurgeCases")
  public void testSnapshotChainInSnapshotInfoTableAfterSnapshotPurge(
      int purgeIndex,
      boolean createInBucketOrder) throws Exception {
    List<String> buckets = Arrays.asList(
        "buck-1-" + UUID.randomUUID(),
        "buck-2-" + UUID.randomUUID(),
        "buck-3-" + UUID.randomUUID()
    );

    for (String bucket : buckets) {
      OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucket,
          omMetadataManager);
    }

    List<SnapshotInfo> snapshotInfoList = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        int bucketIndex = createInBucketOrder ? i : j;
        String bucket = buckets.get(bucketIndex % 3);
        String snapshotName = UUID.randomUUID().toString();
        createSnapshotCheckpoint(volumeName, bucket, snapshotName);
        String snapshotTableKey =
            SnapshotInfo.getTableKey(volumeName, bucket, snapshotName);
        SnapshotInfo snapshotInfo =
            omMetadataManager.getSnapshotInfoTable().get(snapshotTableKey);
        snapshotInfoList.add(snapshotInfo);
      }
    }

    validateSnapshotOrderInSnapshotInfoTableAndSnapshotChain(snapshotInfoList);

    SnapshotInfo purgeSnapshotInfo = snapshotInfoList.get(purgeIndex);

    String purgeSnapshotKey = SnapshotInfo.getTableKey(volumeName,
        purgeSnapshotInfo.getBucketName(),
        purgeSnapshotInfo.getName());

    OMRequest snapshotPurgeRequest = createPurgeKeysRequest(
        Collections.singletonList(purgeSnapshotKey));
    purgeSnapshots(snapshotPurgeRequest);

    List<SnapshotInfo> snapshotInfoListAfterPurge = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      if (i == purgeIndex) {
        // Ignoring purgeIndex because snapshot at purgeIndex has been purged.
        continue;
      }

      SnapshotInfo info = snapshotInfoList.get(i);
      String snapshotKey = SnapshotInfo.getTableKey(volumeName,
          info.getBucketName(),
          info.getName());

      snapshotInfoListAfterPurge.add(
          omMetadataManager.getSnapshotInfoTable().get(snapshotKey));
    }
    validateSnapshotOrderInSnapshotInfoTableAndSnapshotChain(
        snapshotInfoListAfterPurge);
  }

  private void validateSnapshotOrderInSnapshotInfoTableAndSnapshotChain(
      List<SnapshotInfo> snapshotInfoList
  ) {
    if (snapshotInfoList.isEmpty()) {
      return;
    }

    OmMetadataManagerImpl metadataManager =
        (OmMetadataManagerImpl) omMetadataManager;
    SnapshotChainManager chainManager = metadataManager
        .getSnapshotChainManager();

    SnapshotInfo previousSnapshotInfo = snapshotInfoList.get(0);
    for (int i = 1; i < snapshotInfoList.size(); i++) {
      SnapshotInfo currentSnapshotInfo = snapshotInfoList.get(i);
      assertEquals(previousSnapshotInfo.getSnapshotId(),
          currentSnapshotInfo.getGlobalPreviousSnapshotId());

      // Also validate in global chain of SnapshotChainManager.
      assertEquals(previousSnapshotInfo.getSnapshotId(),
          chainManager.previousGlobalSnapshot(
              currentSnapshotInfo.getSnapshotId()));
      assertEquals(currentSnapshotInfo.getSnapshotId(),
          chainManager.nextGlobalSnapshot(
              previousSnapshotInfo.getSnapshotId()));

      previousSnapshotInfo = currentSnapshotInfo;
    }

    Map<String, List<SnapshotInfo>> collect = snapshotInfoList.stream()
        .collect(Collectors.groupingBy(SnapshotInfo::getBucketName));

    for (List<SnapshotInfo> pathSnapshotInfoList : collect.values()) {
      if (pathSnapshotInfoList.isEmpty()) {
        continue;
      }

      SnapshotInfo previousPathSnapshotInfo = pathSnapshotInfoList.get(0);

      for (int i = 1; i < pathSnapshotInfoList.size(); i++) {
        SnapshotInfo currentPathSnapshotInfo = pathSnapshotInfoList.get(i);
        assertEquals(previousPathSnapshotInfo.getSnapshotId(),
            currentPathSnapshotInfo.getPathPreviousSnapshotId());

        // Also validate in path chain of SnapshotChainManager.
        assertEquals(previousPathSnapshotInfo.getSnapshotId(),
            chainManager.previousPathSnapshot(
                currentPathSnapshotInfo.getSnapshotPath(),
                currentPathSnapshotInfo.getSnapshotId()));
        assertEquals(currentPathSnapshotInfo.getSnapshotId(),
            chainManager.nextPathSnapshot(
                currentPathSnapshotInfo.getSnapshotPath(),
                previousPathSnapshotInfo.getSnapshotId()));

        previousPathSnapshotInfo = currentPathSnapshotInfo;
      }
    }
  }
}
