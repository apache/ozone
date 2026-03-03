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

package org.apache.hadoop.ozone.om.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Class to unit test SnapshotDeletingService.
 */
@ExtendWith(MockitoExtension.class)
public class TestSnapshotDeletingService {
  @Mock
  private OzoneManager ozoneManager;
  @Mock
  private KeyManagerImpl keyManager;
  @Mock
  private OmSnapshotManager omSnapshotManager;
  @Mock
  private SnapshotChainManager chainManager;
  @Mock
  private OmMetadataManagerImpl omMetadataManager;
  private final OzoneConfiguration conf = new OzoneConfiguration();
  private final long sdsRunInterval = Duration.ofMillis(1000).toMillis();
  private final  long sdsServiceTimeout = Duration.ofSeconds(10).toMillis();

  private static Stream<Arguments> testCasesForIgnoreSnapshotGc() throws IOException {
    SnapshotInfo flushedSnapshot = SnapshotInfo.newBuilder().setSstFiltered(true)
        .setLastTransactionInfo(TransactionInfo.valueOf(1, 1).toByteString())
        .setName("snap1").build();
    SnapshotInfo unFlushedSnapshot = SnapshotInfo.newBuilder().setSstFiltered(false).setName("snap1")
        .setLastTransactionInfo(TransactionInfo.valueOf(0, 0).toByteString()).build();
    return Stream.of(
        Arguments.of(flushedSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED, false),
        Arguments.of(flushedSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, true),
        Arguments.of(unFlushedSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED, false),
        Arguments.of(unFlushedSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, true),
        Arguments.of(flushedSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED, false),
        Arguments.of(unFlushedSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED, false),
        Arguments.of(unFlushedSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, true),
        Arguments.of(flushedSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, true));
  }

  @ParameterizedTest
  @MethodSource("testCasesForIgnoreSnapshotGc")
  public void testProcessSnapshotLogicInSDS(SnapshotInfo snapshotInfo,
                                            SnapshotInfo.SnapshotStatus status,
                                            boolean expectedOutcome)
      throws IOException {
    Mockito.when(omMetadataManager.getSnapshotChainManager()).thenReturn(chainManager);
    Mockito.when(ozoneManager.getOmSnapshotManager()).thenReturn(omSnapshotManager);
    Mockito.when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    Mockito.when(ozoneManager.getConfiguration()).thenReturn(conf);
    if (status == SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED) {
      Table<String, TransactionInfo> transactionInfoTable = Mockito.mock(Table.class);
      Mockito.when(omMetadataManager.getTransactionInfoTable()).thenReturn(transactionInfoTable);
      Mockito.when(transactionInfoTable.getSkipCache(Mockito.anyString()))
          .thenReturn(TransactionInfo.valueOf(1, 1));
    }

    SnapshotDeletingService snapshotDeletingService =
        new SnapshotDeletingService(sdsRunInterval, sdsServiceTimeout, ozoneManager);

    snapshotInfo.setSnapshotStatus(status);
    assertEquals(expectedOutcome, snapshotDeletingService.shouldIgnoreSnapshot(snapshotInfo));
  }

  /**
   * Test that verifies the Ratis buffer limit is respected during batching and all entries are processed.
   * This test creates entries that would exceed the buffer limit and verifies:
   * 1. Multiple batches are created when needed
   * 2. No single batch exceeds the buffer limit
   * 3. All entries (deletedKeys, renamedKeys, deletedDirs) are eventually processed without orphans
   */
  @Test
  public void testSnapshotMoveKeysRequestBatching() throws Exception {
    final int ratisBufferLimit = 50 * 1024; // 50 KB
    OzoneConfiguration testConf = new OzoneConfiguration();
    testConf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        ratisBufferLimit, StorageUnit.BYTES);

    Mockito.when(omMetadataManager.getSnapshotChainManager()).thenReturn(chainManager);
    Mockito.when(ozoneManager.getOmSnapshotManager()).thenReturn(omSnapshotManager);
    Mockito.when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    Mockito.when(ozoneManager.getConfiguration()).thenReturn(testConf);

    SnapshotDeletingService service = new SnapshotDeletingService(sdsRunInterval, sdsServiceTimeout, ozoneManager);
    SnapshotDeletingService spyService = Mockito.spy(service);

    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapInfo = SnapshotInfo.newBuilder()
        .setSnapshotId(snapshotId)
        .setVolumeName("vol1")
        .setBucketName("bucket1")
        .setName("snap1")
        .setSstFiltered(true)
        .setLastTransactionInfo(TransactionInfo.valueOf(1, 1).toByteString())
        .build();

    int numDeletedKeys = 15;
    int numRenamedKeys = 10;
    int numDeletedDirs = 10;

    List<SnapshotMoveKeyInfos> deletedKeys = createLargeDeletedKeys(numDeletedKeys);
    List<HddsProtos.KeyValue> renamedKeys = createLargeRenamedKeys(numRenamedKeys);
    List<SnapshotMoveKeyInfos> deletedDirs = createLargeDeletedDirs(numDeletedDirs);

    List<OMRequest> capturedRequests = new ArrayList<>();

    Mockito.doAnswer(invocation -> {
      OMRequest request = invocation.getArgument(0);
      capturedRequests.add(request);
      return OzoneManagerProtocolProtos.OMResponse.newBuilder()
          .setCmdType(OzoneManagerProtocolProtos.Type.SnapshotMoveTableKeys)
          .setStatus(OzoneManagerProtocolProtos.Status.OK)
          .setSuccess(true)
          .build();
    }).when(spyService).submitRequest(Mockito.any(OMRequest.class));

    // Create task instance directly from the service
    SnapshotDeletingService.SnapshotDeletingTask task = spyService.new SnapshotDeletingTask();

    // Invoke the batching method directly (now public)
    int submitted = task.submitSnapshotMoveDeletedKeysWithBatching(
        snapInfo, deletedKeys, renamedKeys, deletedDirs);

    // Verify results
    int totalExpected = numDeletedKeys + numRenamedKeys + numDeletedDirs;
    assertEquals(totalExpected, submitted,
        "All entries should be submitted");

    // Verify multiple batches were created (since data should exceed buffer)
    assertTrue(capturedRequests.size() > 1);

    for (OMRequest omRequest : capturedRequests) {
      assertEquals(OzoneManagerProtocolProtos.Type.SnapshotMoveTableKeys, omRequest.getCmdType());

      int requestSize = omRequest.getSerializedSize();
      assertTrue(requestSize <= ratisBufferLimit);
    }

    int totalDeletedKeysProcessed = capturedRequests.stream()
        .mapToInt(req -> req.getSnapshotMoveTableKeysRequest().getDeletedKeysCount())
        .sum();
    int totalRenamedKeysProcessed = capturedRequests.stream()
        .mapToInt(req -> req.getSnapshotMoveTableKeysRequest().getRenamedKeysCount())
        .sum();
    int totalDeletedDirsProcessed = capturedRequests.stream()
        .mapToInt(req -> req.getSnapshotMoveTableKeysRequest().getDeletedDirsCount())
        .sum();

    assertEquals(numDeletedKeys, totalDeletedKeysProcessed);
    assertEquals(numRenamedKeys, totalRenamedKeysProcessed);
    assertEquals(numDeletedDirs, totalDeletedDirsProcessed);

    // Verify no orphan entries (all items accounted for)
    assertEquals(totalExpected, totalDeletedKeysProcessed + totalRenamedKeysProcessed + totalDeletedDirsProcessed);
  }

  /**
   * Helper method to create large deleted keys that will contribute to buffer size.
   */
  private List<SnapshotMoveKeyInfos> createLargeDeletedKeys(int count) {
    List<SnapshotMoveKeyInfos> deletedKeys = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      // Create keys with large names and multiple key infos to increase size
      String largeKeyName = "/vol1/bucket1/" + generateLargeString(500) + "_key_" + i;
      List<OmKeyInfo> keyInfos = new ArrayList<>();
      for (int j = 0; j < 3; j++) {
        OmKeyInfo keyInfo = createOmKeyInfo(largeKeyName + "_part_" + j);
        keyInfos.add(keyInfo);
      }

      SnapshotMoveKeyInfos moveKeyInfo = SnapshotMoveKeyInfos.newBuilder()
          .setKey(largeKeyName)
          .addAllKeyInfos(keyInfos.stream()
              .map(k -> k.getProtobuf(ClientVersion.CURRENT.serialize()))
              .collect(Collectors.toList()))
          .build();
      deletedKeys.add(moveKeyInfo);
    }
    return deletedKeys;
  }

  /**
   * Helper method to create large renamed keys.
   */
  private List<HddsProtos.KeyValue> createLargeRenamedKeys(int count) {
    List<HddsProtos.KeyValue> renamedKeys = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String largeOldName = "/vol1/bucket1/" + generateLargeString(500) + "_old_" + i;
      String largeNewName = "/vol1/bucket1/" + generateLargeString(500) + "_new_" + i;
      renamedKeys.add(HddsProtos.KeyValue.newBuilder()
          .setKey(largeOldName)
          .setValue(largeNewName)
          .build());
    }
    return renamedKeys;
  }

  /**
   * Helper method to create large deleted directories.
   */
  private List<SnapshotMoveKeyInfos> createLargeDeletedDirs(int count) {
    List<SnapshotMoveKeyInfos> deletedDirs = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String largeDirName = "/vol1/bucket1/" + generateLargeString(500) + "_dir_" + i;
      OmKeyInfo dirInfo = createOmKeyInfo(largeDirName);
      
      SnapshotMoveKeyInfos moveDirInfo = SnapshotMoveKeyInfos.newBuilder()
          .setKey(largeDirName)
          .addKeyInfos(dirInfo.getProtobuf(ClientVersion.CURRENT.serialize()))
          .build();
      deletedDirs.add(moveDirInfo);
    }
    return deletedDirs;
  }

  /**
   * Helper method to create an OmKeyInfo object for testing.
   */
  private OmKeyInfo createOmKeyInfo(String keyName) {
    return new OmKeyInfo.Builder()
        .setVolumeName("vol1")
        .setBucketName("bucket1")
        .setKeyName(keyName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE))
        .setObjectID(System.currentTimeMillis())
        .setUpdateID(System.currentTimeMillis())
        .setDataSize(1024 * 1024) // 1 MB
        .setCreationTime(System.currentTimeMillis())
        .setModificationTime(System.currentTimeMillis())
        .setOmKeyLocationInfos(new ArrayList<>())
        .build();
  }

  /**
   * Helper method to generate a large string of specified length.
   */
  private String generateLargeString(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append((char) ('a' + (i % 26)));
    }
    return sb.toString();
  }
}
