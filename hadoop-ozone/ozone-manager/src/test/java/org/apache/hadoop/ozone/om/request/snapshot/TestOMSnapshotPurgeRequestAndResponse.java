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

package org.apache.hadoop.ozone.om.request.snapshot;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.INTERNAL_ERROR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotPurgeResponse;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataProvider;
import org.apache.hadoop.ozone.om.snapshot.TestSnapshotRequestAndResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotPurgeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests OMSnapshotPurgeRequest class.
 */
public class TestOMSnapshotPurgeRequestAndResponse extends TestSnapshotRequestAndResponse {
  private final List<Path> checkpointPaths = new ArrayList<>();
  private String keyName;

  public TestOMSnapshotPurgeRequestAndResponse() {
    super(true);
  }

  @BeforeEach
  public void setup() throws Exception {
    keyName = UUID.randomUUID().toString();
  }

  /**
   * Creates volume, bucket and snapshot entries.
   */
  private List<String> createSnapshots(int numSnapshotKeys)
      throws Exception {

    Random random = new Random();

    // Create Snapshot and CheckpointDir
    List<String> purgeSnapshots = new ArrayList<>(numSnapshotKeys);
    for (int i = 1; i <= numSnapshotKeys; i++) {
      String snapshotName = keyName + "-" + random.nextLong();
      createSnapshotCheckpoint(snapshotName);
      purgeSnapshots.add(SnapshotInfo.getTableKey(getVolumeName(),
          getBucketName(), snapshotName));
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
    checkpointPaths.add(createSnapshotCheckpoint(getVolumeName(), getBucketName(), snapshotName));
  }

  private OMSnapshotPurgeRequest preExecute(OMRequest originalOmRequest)
      throws IOException {
    OMSnapshotPurgeRequest omSnapshotPurgeRequest =
        new OMSnapshotPurgeRequest(originalOmRequest);
    OMRequest modifiedOmRequest = omSnapshotPurgeRequest
        .preExecute(getOzoneManager());
    return new OMSnapshotPurgeRequest(modifiedOmRequest);
  }

  private void purgeSnapshots(OMRequest snapshotPurgeRequest)
      throws IOException {
    // Pre-Execute OMSnapshotPurgeRequest
    OMSnapshotPurgeRequest omSnapshotPurgeRequest =
        preExecute(snapshotPurgeRequest);

    // validateAndUpdateCache for OMSnapshotPurgeRequest.
    OMSnapshotPurgeResponse omSnapshotPurgeResponse = (OMSnapshotPurgeResponse)
        omSnapshotPurgeRequest.validateAndUpdateCache(getOzoneManager(), 200L);

    // Commit to DB.
    try (BatchOperation batchOperation = getOmMetadataManager().getStore().initBatchOperation()) {
      omSnapshotPurgeResponse.checkAndUpdateDB(getOmMetadataManager(), batchOperation);
      getOmMetadataManager().getStore().commitBatchOperation(batchOperation);
    }
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    long initialSnapshotPurgeCount = getOmSnapshotIntMetrics().getNumSnapshotPurges();
    long initialSnapshotPurgeFailCount = getOmSnapshotIntMetrics().getNumSnapshotPurgeFails();

    List<String> snapshotDbKeysToPurge = createSnapshots(10);
    assertFalse(getOmMetadataManager().getSnapshotInfoTable().isEmpty());
    List<SnapshotInfo> snapshotInfos = new ArrayList<>();
    for (String snapshotKey : snapshotDbKeysToPurge) {
      snapshotInfos.add(getOmMetadataManager().getSnapshotInfoTable().get(snapshotKey));
    }

    // Check if all the checkpoints are created.
    for (Path checkpoint : checkpointPaths) {
      assertTrue(Files.exists(checkpoint));
      assertTrue(Files.exists(Paths.get(
          OmSnapshotLocalDataManager.getSnapshotLocalPropertyYamlPath(checkpoint))));
    }

    OMRequest snapshotPurgeRequest = createPurgeKeysRequest(
        snapshotDbKeysToPurge);

    OMSnapshotPurgeRequest omSnapshotPurgeRequest = preExecute(snapshotPurgeRequest);
    TransactionInfo transactionInfo = TransactionInfo.valueOf(TransactionInfo.getTermIndex(200L));
    OMSnapshotPurgeResponse omSnapshotPurgeResponse = (OMSnapshotPurgeResponse)
        omSnapshotPurgeRequest.validateAndUpdateCache(getOzoneManager(), transactionInfo.getTransactionIndex());

    for (String snapshotTableKey: snapshotDbKeysToPurge) {
      assertNull(getOmMetadataManager().getSnapshotInfoTable().get(snapshotTableKey));
    }

    try (BatchOperation batchOperation = getOmMetadataManager().getStore().initBatchOperation()) {
      omSnapshotPurgeResponse.checkAndUpdateDB(getOmMetadataManager(), batchOperation);
      getOmMetadataManager().getStore().commitBatchOperation(batchOperation);
    }

    // Check if the entries are deleted.
    assertTrue(getOmMetadataManager().getSnapshotInfoTable().isEmpty());

    // Check if all the checkpoints are cleared.
    for (Path checkpoint : checkpointPaths) {
      assertFalse(Files.exists(checkpoint));
    }
    OmSnapshotLocalDataManager snapshotLocalDataManager =
        getOzoneManager().getOmSnapshotManager().getSnapshotLocalDataManager();
    for (SnapshotInfo snapshotInfo : snapshotInfos) {
      try (ReadableOmSnapshotLocalDataProvider snapProvider =
               snapshotLocalDataManager.getOmSnapshotLocalData(snapshotInfo)) {
        assertEquals(transactionInfo, snapProvider.getSnapshotLocalData().getTransactionInfo());
      }
    }

    assertEquals(initialSnapshotPurgeCount + 1, getOmSnapshotIntMetrics().getNumSnapshotPurges());
    assertEquals(initialSnapshotPurgeFailCount, getOmSnapshotIntMetrics().getNumSnapshotPurgeFails());
  }

  @Test
  public void testDuplicateSnapshotPurge() throws Exception {
    List<String> snapshotDbKeysToPurge = createSnapshots(1);
    assertFalse(getOmMetadataManager().getSnapshotInfoTable().isEmpty());
    OMRequest snapshotPurgeRequest = createPurgeKeysRequest(
        snapshotDbKeysToPurge);

    OMSnapshotPurgeRequest omSnapshotPurgeRequest = preExecute(snapshotPurgeRequest);

    OMSnapshotPurgeResponse omSnapshotPurgeResponse = (OMSnapshotPurgeResponse)
        omSnapshotPurgeRequest.validateAndUpdateCache(getOzoneManager(), 200L);

    try (BatchOperation batchOperation = getOmMetadataManager().getStore().initBatchOperation()) {
      omSnapshotPurgeResponse.checkAndUpdateDB(getOmMetadataManager(), batchOperation);
      getOmMetadataManager().getStore().commitBatchOperation(batchOperation);
    }

    // Check if the entries are deleted.
    assertTrue(getOmMetadataManager().getSnapshotInfoTable().isEmpty());

    OMSnapshotPurgeResponse omSnapshotPurgeResponse1 = (OMSnapshotPurgeResponse)
        omSnapshotPurgeRequest.validateAndUpdateCache(getOzoneManager(), 201L);

    for (Map.Entry<String, SnapshotInfo> purgedSnapshot : omSnapshotPurgeResponse1.getUpdatedSnapInfos().entrySet()) {
      assertNotNull(purgedSnapshot.getValue());
    }
    for (String snapshotTableKey: snapshotDbKeysToPurge) {
      assertNull(getOmMetadataManager().getSnapshotInfoTable().get(snapshotTableKey));
    }

    try (BatchOperation batchOperation = getOmMetadataManager().getStore().initBatchOperation()) {
      omSnapshotPurgeResponse1.checkAndUpdateDB(getOmMetadataManager(), batchOperation);
      getOmMetadataManager().getStore().commitBatchOperation(batchOperation);
    }
  }

  /**
   * This test is mainly to validate metrics and error code.
   */
  @Test
  public void testValidateAndUpdateCacheFailure() throws Exception {
    long initialSnapshotPurgeCount = getOmSnapshotIntMetrics().getNumSnapshotPurges();
    long initialSnapshotPurgeFailCount = getOmSnapshotIntMetrics().getNumSnapshotPurgeFails();

    List<String> snapshotDbKeysToPurge = createSnapshots(10);

    OmMetadataManagerImpl mockedMetadataManager = mock(OmMetadataManagerImpl.class);
    Table<String, SnapshotInfo> mockedSnapshotInfoTable = mock(Table.class);

    when(mockedSnapshotInfoTable.get(anyString())).thenThrow(new CodecException("Injected fault error."));
    when(mockedMetadataManager.getSnapshotInfoTable()).thenReturn(mockedSnapshotInfoTable);
    when(getOzoneManager().getMetadataManager()).thenReturn(mockedMetadataManager);

    OMRequest snapshotPurgeRequest = createPurgeKeysRequest(snapshotDbKeysToPurge);
    OMSnapshotPurgeRequest omSnapshotPurgeRequest = preExecute(snapshotPurgeRequest);

    OMSnapshotPurgeResponse omSnapshotPurgeResponse = (OMSnapshotPurgeResponse)
        omSnapshotPurgeRequest.validateAndUpdateCache(getOzoneManager(), 200L);

    assertEquals(INTERNAL_ERROR, omSnapshotPurgeResponse.getOMResponse().getStatus());
    assertEquals(initialSnapshotPurgeCount, getOmSnapshotIntMetrics().getNumSnapshotPurges());
    assertEquals(initialSnapshotPurgeFailCount + 1, getOmSnapshotIntMetrics().getNumSnapshotPurgeFails());
  }

  // TODO: clean up: Do we this test after
  //  testSnapshotChainInSnapshotInfoTableAfterSnapshotPurge?
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 4})
  public void testSnapshotChainCleanup(int index) throws Exception {
    List<String> snapshots = createSnapshots(5);
    String snapShotToPurge = snapshots.get(index);

    // Before purge, check snapshot chain
    OmMetadataManagerImpl metadataManager =
        (OmMetadataManagerImpl) getOmMetadataManager();
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

    long rowsInTableBeforePurge = getOmMetadataManager()
        .countRowsInTable(getOmMetadataManager().getSnapshotInfoTable());
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

    assertNotEquals(rowsInTableBeforePurge, getOmMetadataManager()
        .countRowsInTable(getOmMetadataManager().getSnapshotInfoTable()));
  }

  private static Stream<Arguments> snapshotPurgeCases() {
    return Stream.of(
        Arguments.of("Single bucket: purge first snapshot.",
            1, 5, 0, 0, true),
        Arguments.of("Single bucket: purge snapshot at index 2.",
            1, 5, 2, 2, true),
        Arguments.of("Single bucket: purge snapshots from index 1 to 3.",
            1, 5, 1, 3, true),
        Arguments.of("Single bucket: purge last snapshot.",
            1, 5, 4, 4, true),
        Arguments.of("Multiple buckets (keys are created in bucket order): " +
            "purge first snapshot.", 3, 5, 0, 0, true),
        Arguments.of("Multiple buckets (keys are created in bucket order): " +
            "purge first 5 snapshots.", 3, 5, 0, 4, true),
        Arguments.of("Multiple buckets (keys are created in bucket order): " +
            "purge snapshot at index 7.", 3, 5, 7, 7, true),
        Arguments.of("Multiple buckets (keys are created in bucket order): " +
            "purge snapshots from index 5 to 9.", 3, 5, 5, 9, true),
        Arguments.of("Multiple buckets (keys are created in bucket order): " +
            "purge snapshots from index 3 to 12.", 3, 5, 3, 12, true),
        Arguments.of("Multiple buckets (keys are created in bucket order): " +
            "purge last 5 snapshots.", 3, 5, 10, 14, true),
        Arguments.of("Multiple buckets (keys are created in bucket order): " +
            "purge last snapshot.", 3, 5, 14, 14, true),
        Arguments.of("Multiple buckets (keys are not created in bucket " +
            "order): purge first snapshot.", 3, 5, 0, 0, false),
        Arguments.of("Multiple buckets (keys are not created in bucket " +
            "order): purge first 5 snapshots.", 3, 5, 0, 5, false),
        Arguments.of("Multiple buckets (keys are not created in bucket " +
            "order): purge snapshot at index 7.", 3, 5, 7, 7, false),
        Arguments.of("Multiple buckets (keys are not created in bucket " +
            "order): purge snapshots from index 5 to 9.", 3, 5, 5, 9, false),
        Arguments.of("Multiple buckets (keys are not created in bucket " +
            "order): purge snapshots from index 3 to 12.", 3, 5, 3, 12, false),
        Arguments.of("Multiple buckets (keys are not created in bucket " +
            "order): purge last 5 snapshots.", 3, 5, 10, 14, false),
        Arguments.of("Multiple buckets (keys are not created in bucket " +
            "order): purge last snapshot.", 3, 5, 14, 14, false)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("snapshotPurgeCases")
  public void testSnapshotChainInSnapshotInfoTableAfterSnapshotPurge(
      String description,
      int numberOfBuckets,
      int numberOfKeysPerBucket,
      int fromIndex,
      int toIndex,
      boolean createInBucketOrder) throws Exception {
    SnapshotChainManager chainManager =
        ((OmMetadataManagerImpl) getOmMetadataManager()).getSnapshotChainManager();
    int totalKeys = numberOfBuckets * numberOfKeysPerBucket;

    List<String> buckets = new ArrayList<>();
    for (int i = 0; i < numberOfBuckets; i++) {
      String bucketNameLocal = "bucket-" + UUID.randomUUID();
      OMRequestTestUtils.addVolumeAndBucketToDB(getVolumeName(), bucketNameLocal,
          getOmMetadataManager());
      buckets.add(bucketNameLocal);
    }

    List<SnapshotInfo> snapshotInfoList = new ArrayList<>();

    for (int i = 0; i < numberOfBuckets; i++) {
      for (int j = 0; j < numberOfKeysPerBucket; j++) {
        int bucketIndex = createInBucketOrder ? i : j;
        String bucket = buckets.get(bucketIndex % numberOfBuckets);
        String snapshotName = UUID.randomUUID().toString();
        createSnapshotCheckpoint(getVolumeName(), bucket, snapshotName);
        String snapshotTableKey =
            SnapshotInfo.getTableKey(getVolumeName(), bucket, snapshotName);
        SnapshotInfo snapshotInfo =
            getOmMetadataManager().getSnapshotInfoTable().get(snapshotTableKey);
        snapshotInfo.setDeepClean(true);
        snapshotInfo.setDeepCleanedDeletedDir(true);
        getOmMetadataManager().getSnapshotInfoTable().addCacheEntry(snapshotTableKey, snapshotInfo,
            System.currentTimeMillis());
        snapshotInfoList.add(getOmMetadataManager().getSnapshotInfoTable().get(snapshotTableKey));
      }
    }

    long numberOfSnapshotBeforePurge = getOmMetadataManager()
        .countRowsInTable(getOmMetadataManager().getSnapshotInfoTable());
    assertEquals(totalKeys, numberOfSnapshotBeforePurge);
    assertEquals(totalKeys, chainManager.getGlobalSnapshotChain().size());
    Map<UUID, ByteString> expectedTransactionInfos = new HashMap<>();
    Map<UUID, Boolean> expectedDeepCleanFlags = new HashMap<>();
    // Ratis transaction uses term index 1 while creating snapshot.
    ByteString expectedLastTransactionVal = TransactionInfo.valueOf(TransactionInfo.getTermIndex(1L))
        .toByteString();
    for (SnapshotInfo snapshotInfo : snapshotInfoList) {
      expectedTransactionInfos.put(snapshotInfo.getSnapshotId(), expectedLastTransactionVal);
      expectedDeepCleanFlags.put(snapshotInfo.getSnapshotId(), true);
    }
    validateSnapshotOrderInSnapshotInfoTableAndSnapshotChain(snapshotInfoList, expectedTransactionInfos,
        expectedDeepCleanFlags);
    // Ratis transaction uses term index 200 while purging snapshot.
    expectedLastTransactionVal = TransactionInfo.valueOf(TransactionInfo.getTermIndex(200L))
        .toByteString();
    List<String> purgeSnapshotKeys = new ArrayList<>();
    for (int i = fromIndex; i <= toIndex; i++) {
      SnapshotInfo purgeSnapshotInfo = snapshotInfoList.get(i);
      UUID snapId = purgeSnapshotInfo.getSnapshotId();
      // expecting nextPathSnapshot & nextGlobalSnapshot in chain gets updated.
      if (chainManager.hasNextGlobalSnapshot(snapId)) {
        expectedTransactionInfos.put(chainManager.nextGlobalSnapshot(snapId), expectedLastTransactionVal);
      }
      if (chainManager.hasNextPathSnapshot(purgeSnapshotInfo.getSnapshotPath(), snapId)) {
        UUID nextPathSnapshot = chainManager.nextPathSnapshot(purgeSnapshotInfo.getSnapshotPath(), snapId);
        expectedTransactionInfos.put(nextPathSnapshot, expectedLastTransactionVal);
        expectedDeepCleanFlags.put(nextPathSnapshot, false);
        if (chainManager.hasNextPathSnapshot(purgeSnapshotInfo.getSnapshotPath(), nextPathSnapshot)) {
          UUID nextToNextPathSnapshot = chainManager.nextPathSnapshot(purgeSnapshotInfo.getSnapshotPath(),
              nextPathSnapshot);
          expectedTransactionInfos.put(nextToNextPathSnapshot, expectedLastTransactionVal);
          expectedDeepCleanFlags.put(nextToNextPathSnapshot, false);
        }
      }
      String purgeSnapshotKey = SnapshotInfo.getTableKey(getVolumeName(),
          purgeSnapshotInfo.getBucketName(),
          purgeSnapshotInfo.getName());
      purgeSnapshotKeys.add(purgeSnapshotKey);
    }

    OMRequest snapshotPurgeRequest = createPurgeKeysRequest(purgeSnapshotKeys);
    purgeSnapshots(snapshotPurgeRequest);

    List<SnapshotInfo> snapshotInfoListAfterPurge = new ArrayList<>();
    for (int i = 0; i < totalKeys; i++) {
      if (i < fromIndex || i > toIndex) {
        SnapshotInfo info = snapshotInfoList.get(i);
        String snapshotKey = SnapshotInfo.getTableKey(getVolumeName(),
            info.getBucketName(), info.getName());
        snapshotInfoListAfterPurge.add(
            getOmMetadataManager().getSnapshotInfoTable().get(snapshotKey));
      }
    }

    long expectNumberOfSnapshotAfterPurge = totalKeys -
        (toIndex - fromIndex + 1);
    long actualNumberOfSnapshotAfterPurge = getOmMetadataManager()
        .countRowsInTable(getOmMetadataManager().getSnapshotInfoTable());
    assertEquals(expectNumberOfSnapshotAfterPurge,
        actualNumberOfSnapshotAfterPurge);
    assertEquals(expectNumberOfSnapshotAfterPurge, chainManager
        .getGlobalSnapshotChain().size());
    validateSnapshotOrderInSnapshotInfoTableAndSnapshotChain(snapshotInfoListAfterPurge, expectedTransactionInfos,
        expectedDeepCleanFlags);
  }

  private void validateSnapshotOrderInSnapshotInfoTableAndSnapshotChain(
      List<SnapshotInfo> snapshotInfoList, Map<UUID, ByteString> expectedTransactionInfos,
      Map<UUID, Boolean> expectedDeepCleanFlags) throws IOException {
    if (snapshotInfoList.isEmpty()) {
      return;
    }
    for (SnapshotInfo snapshotInfo : snapshotInfoList) {
      assertEquals(snapshotInfo.getLastTransactionInfo(), expectedTransactionInfos.get(snapshotInfo.getSnapshotId()));
      assertEquals(snapshotInfo.isDeepCleaned(), expectedDeepCleanFlags.get(snapshotInfo.getSnapshotId()));
      assertEquals(snapshotInfo.isDeepCleanedDeletedDir(), expectedDeepCleanFlags.get(snapshotInfo.getSnapshotId()));
    }
    OmMetadataManagerImpl metadataManager =
        (OmMetadataManagerImpl) getOmMetadataManager();
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
