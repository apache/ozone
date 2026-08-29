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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartKey;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUpload;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequestTests;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadAbortRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadAbortRequestWithFSO;
import org.apache.hadoop.ozone.om.request.volume.OMQuotaRepairRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.volume.OMQuotaRepairResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test class for quota repair.
 */
@Timeout(120)
public class TestQuotaRepairTask extends OMKeyRequestTests {

  /** Seconds; must match {@link Timeout} on this class. */
  private static final int REPAIR_TEST_TIMEOUT_SECONDS = 120;

  private static Boolean awaitRepair(CompletableFuture<Boolean> repair) throws Exception {
    return repair.get(REPAIR_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  @Test
  public void testQuotaRepair() throws Exception {
    OzoneManagerProtocolProtos.OMResponse respMock = mock(OzoneManagerProtocolProtos.OMResponse.class);
    when(respMock.getSuccess()).thenReturn(true);
    OzoneManagerRatisServer ratisServerMock = mock(OzoneManagerRatisServer.class);
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> ref = new AtomicReference<>();
    doAnswer(invocation -> {
      ref.set(invocation.getArgument(0, OzoneManagerProtocolProtos.OMRequest.class));
      return respMock;
    }).when(ratisServerMock).submitRequest(any(), any(), anyLong());
    when(ozoneManager.getOmRatisServer()).thenReturn(ratisServerMock);
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.OBJECT_STORE);

    int count = 10;
    String parentDir = "/user";
    for (int i = 0; i < count; i++) {
      OMRequestTestUtils.addKeyToTableAndCache(volumeName, bucketName,
          parentDir.concat("/key" + i), -1, RatisReplicationConfig.getInstance(THREE), 150 + i, omMetadataManager);
    }

    String fsoBucketName = "fso" + bucketName;
    OMRequestTestUtils.addBucketToDB(volumeName, fsoBucketName,
        omMetadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    long parentId = OMRequestTestUtils.addParentsToDirTable(volumeName,
        fsoBucketName, "c/d/e", omMetadataManager);
    for (int i = 0; i < count; i++) {
      String fileName = "file1" + i;
      OmKeyInfo omKeyInfo =
          OMRequestTestUtils.createOmKeyInfo(volumeName, fsoBucketName, fileName,
                  RatisReplicationConfig.getInstance(ONE))
              .setObjectID(parentId + 1 + i)
              .setParentObjectID(parentId)
              .setUpdateID(100L + i)
              .build();
      omKeyInfo.setKeyName(fileName);
      OMRequestTestUtils.addFileToKeyTable(false, false,
          fileName, omKeyInfo, -1, 50 + i, omMetadataManager);
    }

    // Intentionally zero out buckets' used bytes first
    zeroOutBucketUsedBytes(volumeName, bucketName, 1L);
    zeroOutBucketUsedBytes(volumeName, fsoBucketName, 2L);

    // all count is 0 as above is adding directly to key / file table
    // and directory table
    OmBucketInfo obsBucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName));
    assertEquals(0, obsBucketInfo.getUsedNamespace());
    assertEquals(0, obsBucketInfo.getUsedBytes());
    OmBucketInfo fsoBucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, fsoBucketName));
    assertEquals(0, fsoBucketInfo.getUsedNamespace());
    assertEquals(0, fsoBucketInfo.getUsedBytes());
    
    QuotaRepairTask quotaRepairTask = new QuotaRepairTask(ozoneManager);
    CompletableFuture<Boolean> repair = quotaRepairTask.repair();
    Boolean repairStatus = awaitRepair(repair);
    assertTrue(repairStatus);

    OMQuotaRepairRequest omQuotaRepairRequest = new OMQuotaRepairRequest(ref.get());
    OMClientResponse omClientResponse = omQuotaRepairRequest.validateAndUpdateCache(ozoneManager, 1);
    BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation();
    ((OMQuotaRepairResponse)omClientResponse).addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    // 10 files of each type, obs have replication of three and
    // fso have replication of one
    OmBucketInfo obsUpdateBucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName));
    OmBucketInfo fsoUpdateBucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, fsoBucketName));
    assertEquals(10, obsUpdateBucketInfo.getUsedNamespace());
    assertEquals(30000, obsUpdateBucketInfo.getUsedBytes());
    assertEquals(13, fsoUpdateBucketInfo.getUsedNamespace());
    assertEquals(10000, fsoUpdateBucketInfo.getUsedBytes());
  }

  @Test
  public void testQuotaRepairForOldVersionVolumeBucket() throws Exception {
    OzoneManagerProtocolProtos.OMResponse respMock = mock(OzoneManagerProtocolProtos.OMResponse.class);
    when(respMock.getSuccess()).thenReturn(true);
    OzoneManagerRatisServer ratisServerMock = mock(OzoneManagerRatisServer.class);
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> ref = new AtomicReference<>();
    doAnswer(invocation -> {
      ref.set(invocation.getArgument(0, OzoneManagerProtocolProtos.OMRequest.class));
      return respMock;
    }).when(ratisServerMock).submitRequest(any(), any(), anyLong());
    when(ozoneManager.getOmRatisServer()).thenReturn(ratisServerMock);
    // add volume with -2 value
    OmVolumeArgs omVolumeArgs =
        OmVolumeArgs.newBuilder().setCreationTime(Time.now())
            .setVolume(volumeName).setAdminName(volumeName)
            .setOwnerName(volumeName).setQuotaInBytes(-2)
            .setQuotaInNamespace(-2).build();
    omMetadataManager.getVolumeTable().put(
        omMetadataManager.getVolumeKey(volumeName), omVolumeArgs);
    omMetadataManager.getVolumeTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getVolumeKey(volumeName)),
        CacheValue.get(1L, omVolumeArgs));
    
    // add bucket with -2 value and add to db
    OMRequestTestUtils.addBucketToDB(volumeName, bucketName,
        omMetadataManager, -2);
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    omMetadataManager.getBucketTable().put(bucketKey, omMetadataManager.getBucketTable().get(bucketKey));

    // pre check for quota flag
    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(bucketKey);
    assertEquals(-2, bucketInfo.getQuotaInBytes());
    
    omVolumeArgs = omMetadataManager.getVolumeTable().get(
        omMetadataManager.getVolumeKey(volumeName));
    assertEquals(-2, omVolumeArgs.getQuotaInBytes());
    assertEquals(-2, omVolumeArgs.getQuotaInNamespace());

    QuotaRepairTask quotaRepairTask = new QuotaRepairTask(ozoneManager);
    CompletableFuture<Boolean> repair = quotaRepairTask.repair();
    Boolean repairStatus = awaitRepair(repair);
    assertTrue(repairStatus);

    OMQuotaRepairRequest omQuotaRepairRequest = new OMQuotaRepairRequest(ref.get());
    OMClientResponse omClientResponse = omQuotaRepairRequest.validateAndUpdateCache(ozoneManager, 1);
    BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation();
    ((OMQuotaRepairResponse)omClientResponse).addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    bucketInfo = omMetadataManager.getBucketTable().get(
        bucketKey);
    assertEquals(-1, bucketInfo.getQuotaInBytes());
    OmVolumeArgs volArgsVerify = omMetadataManager.getVolumeTable()
        .get(omMetadataManager.getVolumeKey(volumeName));
    assertEquals(-1, volArgsVerify.getQuotaInBytes());
    assertEquals(-1, volArgsVerify.getQuotaInNamespace());
  }

  @Test
  public void testQuotaRepairDeletedTableSnapshotQuota() throws Exception {
    OzoneManagerProtocolProtos.OMResponse respMock = mock(OzoneManagerProtocolProtos.OMResponse.class);
    when(respMock.getSuccess()).thenReturn(true);
    OzoneManagerRatisServer ratisServerMock = mock(OzoneManagerRatisServer.class);
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> ref = new AtomicReference<>();
    doAnswer(invocation -> {
      ref.set(invocation.getArgument(0, OzoneManagerProtocolProtos.OMRequest.class));
      return respMock;
    }).when(ratisServerMock).submitRequest(any(), any(), anyLong());
    when(ozoneManager.getOmRatisServer()).thenReturn(ratisServerMock);

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.OBJECT_STORE);

    String keyName = "/user/snapKey";
    OMRequestTestUtils.addKeyToTableAndCache(volumeName, bucketName,
        keyName, -1, RatisReplicationConfig.getInstance(THREE), 1L, omMetadataManager);

    String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName));
    long bucketObjId = bucketInfo.getObjectID();

    OMRequestTestUtils.deleteKey(ozoneKey, bucketObjId, omMetadataManager, 2L);

    RepeatedOmKeyInfo deletedEntry = omMetadataManager.getDeletedTable().get(ozoneKey);
    long expectedSnapNs = deletedEntry.getOmKeyInfoList().size();

    bucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName));
    OmBucketInfo corruptedSnapshot = bucketInfo.toBuilder()
        .setSnapshotUsedBytes(7L)
        .setSnapshotUsedNamespace(99L)
        .build();
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    omMetadataManager.getBucketTable().put(bucketKey, corruptedSnapshot);
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(bucketKey), CacheValue.get(3L, corruptedSnapshot));

    QuotaRepairTask quotaRepairTask = new QuotaRepairTask(ozoneManager);
    CompletableFuture<Boolean> repair = quotaRepairTask.repair();
    assertTrue(awaitRepair(repair));

    OMQuotaRepairRequest omQuotaRepairRequest = new OMQuotaRepairRequest(ref.get());
    OMClientResponse omClientResponse = omQuotaRepairRequest.validateAndUpdateCache(ozoneManager, 1);
    BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation();
    ((OMQuotaRepairResponse) omClientResponse).addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    OmBucketInfo repaired = omMetadataManager.getBucketTable().get(bucketKey);
    assertEquals(0, repaired.getUsedBytes());
    assertEquals(0, repaired.getUsedNamespace());
    assertEquals(expectedSnapNs, repaired.getSnapshotUsedNamespace());
    assertTrue(repaired.getSnapshotUsedBytes() > 0,
        "Snapshot pending-delete bytes must be recomputed from deletedTable");
  }

  @Test
  public void testQuotaRepairSnapshotDbDeletedTableQuota() throws Exception {
    OzoneManagerProtocolProtos.OMResponse respMock = mock(OzoneManagerProtocolProtos.OMResponse.class);
    when(respMock.getSuccess()).thenReturn(true);
    OzoneManagerRatisServer ratisServerMock = mock(OzoneManagerRatisServer.class);
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> ref = new AtomicReference<>();
    doAnswer(invocation -> {
      ref.set(invocation.getArgument(0, OzoneManagerProtocolProtos.OMRequest.class));
      return respMock;
    }).when(ratisServerMock).submitRequest(any(), any(), anyLong());
    when(ozoneManager.getOmRatisServer()).thenReturn(ratisServerMock);

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.OBJECT_STORE);

    String keyName = "/user/snapKey";
    OMRequestTestUtils.addKeyToTableAndCache(volumeName, bucketName,
        keyName, -1, RatisReplicationConfig.getInstance(THREE), 1L, omMetadataManager);

    String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(BucketLayout.OBJECT_STORE).get(ozoneKey);
    long keyBytes = omKeyInfo.getReplicatedSize();

    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName));
    OMRequestTestUtils.deleteKey(ozoneKey, bucketInfo.getObjectID(), omMetadataManager, 2L);

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo afterDelete = bucketInfo.toBuilder()
        .setUsedBytes(0)
        .setUsedNamespace(0)
        .setSnapshotUsedBytes(keyBytes)
        .setSnapshotUsedNamespace(1)
        .build();
    omMetadataManager.getBucketTable().put(bucketKey, afterDelete);

    when(ozoneManager.getDefaultReplicationConfig())
        .thenReturn(RatisReplicationConfig.getInstance(THREE));
    createSnapshot("snap1");

    assertNull(omMetadataManager.getDeletedTable().get(ozoneKey),
        "Deleted key should move out of active deletedTable after snapshot");
    assertEquals(0, omMetadataManager.countRowsInTable(omMetadataManager.getDeletedTable()));

    OmBucketInfo corrupted = afterDelete.toBuilder()
        .setSnapshotUsedBytes(7L)
        .build();
    omMetadataManager.getBucketTable().put(bucketKey, corrupted);
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(bucketKey), CacheValue.get(3L, corrupted));

    QuotaRepairTask quotaRepairTask = new QuotaRepairTask(ozoneManager);
    CompletableFuture<Boolean> repair = quotaRepairTask.repair();
    assertTrue(awaitRepair(repair));

    OMQuotaRepairRequest omQuotaRepairRequest = new OMQuotaRepairRequest(ref.get());
    OMClientResponse omClientResponse = omQuotaRepairRequest.validateAndUpdateCache(ozoneManager, 1);
    BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation();
    ((OMQuotaRepairResponse) omClientResponse).addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    OmBucketInfo repaired = omMetadataManager.getBucketTable().get(bucketKey);
    assertEquals(0, repaired.getUsedBytes());
    assertEquals(0, repaired.getUsedNamespace());
    assertEquals(keyBytes, repaired.getSnapshotUsedBytes());
    assertEquals(1, repaired.getSnapshotUsedNamespace());
  }

  private void zeroOutBucketUsedBytes(String volumeName, String bucketName,
                                      long trxnLogIndex)
      throws IOException {
    String dbKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(dbKey);
    bucketInfo.decrUsedBytes(bucketInfo.getUsedBytes(), false);
    omMetadataManager.getBucketTable()
        .addCacheEntry(new CacheKey<>(dbKey),
            CacheValue.get(trxnLogIndex, bucketInfo));
    omMetadataManager.getBucketTable().put(dbKey, bucketInfo);
  }

  @Test
  public void testScanTableInBatchesFailsFastOnWorkerFailure() throws Exception {
    int totalEntries = QuotaRepairTask.BATCH_SIZE * 8;
    AtomicInteger remaining = new AtomicInteger(totalEntries);
    @SuppressWarnings("unchecked")
    Table.KeyValueIterator<String, OmKeyInfo> keyIter = mock(Table.KeyValueIterator.class);
    when(keyIter.hasNext()).thenAnswer(inv -> remaining.get() > 0);
    when(keyIter.next()).thenAnswer(inv -> {
      remaining.decrementAndGet();
      return Table.newKeyValue("/vol/bucket/key", null);
    });
    ExecutorService executor = Executors.newFixedThreadPool(3);
    try {
      UncheckedExecutionException ex = assertThrows(UncheckedExecutionException.class,
          () -> QuotaRepairTask.scanTableInBatches(executor, keyIter, "worker failure test", kv -> {
            throw new UncheckedIOException(new IOException("injected worker failure"));
          }));
      assertInstanceOf(UncheckedIOException.class, ex.getCause().getCause());
      // no worker may outlive the scan
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testScanTableInBatchesFailsOnProducerInterrupt() throws Exception {
    @SuppressWarnings("unchecked")
    Table.KeyValueIterator<String, OmKeyInfo> keyIter = mock(Table.KeyValueIterator.class);
    when(keyIter.hasNext()).thenReturn(true);
    when(keyIter.next()).thenAnswer(inv -> Table.newKeyValue("/vol/bucket/key", null));
    ExecutorService executor = Executors.newFixedThreadPool(3);
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    Thread producer = new Thread(() -> {
      try {
        QuotaRepairTask.scanTableInBatches(executor, keyIter, "interrupt test", kv -> { });
      } catch (Throwable t) {
        thrown.set(t);
      }
    });
    try {
      producer.start();
      producer.interrupt();
      producer.join(TimeUnit.SECONDS.toMillis(60));
      assertFalse(producer.isAlive());
      UncheckedExecutionException ex = assertInstanceOf(UncheckedExecutionException.class, thrown.get());
      assertInstanceOf(InterruptedException.class, ex.getCause());
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testInterruptedScanStillAwaitsWorkers() throws Exception {
    AtomicInteger remaining = new AtomicInteger(QuotaRepairTask.BATCH_SIZE);
    @SuppressWarnings("unchecked")
    Table.KeyValueIterator<String, OmKeyInfo> keyIter = mock(Table.KeyValueIterator.class);
    when(keyIter.hasNext()).thenAnswer(inv -> remaining.get() > 0);
    when(keyIter.next()).thenAnswer(inv -> {
      remaining.decrementAndGet();
      return Table.newKeyValue("/vol/bucket/key", null);
    });
    CountDownLatch workerStarted = new CountDownLatch(1);
    CountDownLatch releaseWorker = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(3);
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    Thread producer = new Thread(() -> {
      try {
        QuotaRepairTask.scanTableInBatches(executor, keyIter, "await workers test", kv -> {
          workerStarted.countDown();
          try {
            releaseWorker.await();
          } catch (InterruptedException ex) {
            throw new IllegalStateException(ex);
          }
        });
      } catch (Throwable t) {
        thrown.set(t);
      }
    });
    try {
      producer.start();
      assertTrue(workerStarted.await(30, TimeUnit.SECONDS));
      producer.interrupt();
      // the interrupted producer must keep waiting for the blocked worker instead of exiting
      producer.join(TimeUnit.SECONDS.toMillis(1));
      assertTrue(producer.isAlive());
      releaseWorker.countDown();
      producer.join(TimeUnit.SECONDS.toMillis(60));
      assertFalse(producer.isAlive());
      UncheckedExecutionException ex = assertInstanceOf(UncheckedExecutionException.class, thrown.get());
      assertInstanceOf(InterruptedException.class, ex.getCause());
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    } finally {
      releaseWorker.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testQuotaRepairCountsMPUParts() throws Exception {
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> request = mockQuotaRepairRequest();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.OBJECT_STORE);
    String otherBucketName = "other" + bucketName;
    OMRequestTestUtils.addBucketToDB(volumeName, otherBucketName,
        omMetadataManager, BucketLayout.OBJECT_STORE);

    // committed keys stay charged after the MPU parts are released
    int keyCount = 3;
    for (int i = 0; i < keyCount; i++) {
      OMRequestTestUtils.addKeyToTableAndCache(volumeName, bucketName, "key" + i, -1,
          RatisReplicationConfig.getInstance(ONE), 10L + i, omMetadataManager);
    }
    long keyBytes = keyCount * 1000L;

    // legacy schema: part 1 is committed twice (100 then 300), only the current 300 counts
    String legacyKey = "legacyMpuKey";
    String legacyUploadId = UUID.randomUUID().toString();
    OmMultipartKeyInfo legacyInfo = newMultipartInfo(legacyUploadId, 1001L, false);
    legacyInfo.addPartKeyInfo(createPart(bucketName, legacyKey, legacyUploadId, 1, 100L));
    legacyInfo.addPartKeyInfo(createPart(bucketName, legacyKey, legacyUploadId, 1, 300L));
    legacyInfo.addPartKeyInfo(createPart(bucketName, legacyKey, legacyUploadId, 2, 200L));
    addMultipartInfo(bucketName, legacyKey, legacyInfo, 1L);
    long legacyBytes = 300L + 200L;

    // split-parts schema: part 1 is rewritten (111 then 400), only the current 400 counts
    String splitKey = "splitMpuKey";
    String splitUploadId = UUID.randomUUID().toString();
    addMultipartInfo(bucketName, splitKey, newMultipartInfo(splitUploadId, 1002L, true), 2L);
    putSplitPart(bucketName, splitKey, splitUploadId, 1, 111L);
    putSplitPart(bucketName, splitKey, splitUploadId, 1, 400L);
    putSplitPart(bucketName, splitKey, splitUploadId, 2, 500L);
    long splitBytes = 400L + 500L;

    // uploads initiated but with no part committed yet must contribute nothing
    String emptyLegacyUploadId = UUID.randomUUID().toString();
    addMultipartInfo(bucketName, "emptyLegacyMpuKey",
        newMultipartInfo(emptyLegacyUploadId, 1003L, false), 3L);
    String emptySplitUploadId = UUID.randomUUID().toString();
    addMultipartInfo(bucketName, "emptySplitMpuKey",
        newMultipartInfo(emptySplitUploadId, 1004L, true), 4L);

    // a second bucket proves parts are charged to the bucket that owns them.
    // Its upload replicates three ways, so the part size is charged three times over.
    String otherKey = "otherMpuKey";
    String otherUploadId = UUID.randomUUID().toString();
    OmMultipartKeyInfo otherInfo = newMultipartInfo(otherUploadId, 2001L, false).toBuilder()
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE)).build();
    otherInfo.addPartKeyInfo(createPart(otherBucketName, otherKey, otherUploadId, 1, 700L));
    addMultipartInfo(otherBucketName, otherKey, otherInfo, 5L);
    long otherBytes = 700L * 3;

    String bucketKey = corruptBucketUsage(bucketName, 12345L, 99L, 6L);
    String otherBucketKey = corruptBucketUsage(otherBucketName, 54321L, 77L, 7L);
    applyQuotaRepair(request, 8L);

    OmBucketInfo repaired = omMetadataManager.getBucketTable().get(bucketKey);
    assertEquals(keyBytes + legacyBytes + splitBytes, repaired.getUsedBytes());
    // MPU parts consume no namespace, only the committed keys do
    assertEquals(keyCount, repaired.getUsedNamespace());
    OmBucketInfo otherRepaired = omMetadataManager.getBucketTable().get(otherBucketKey);
    assertEquals(otherBytes, otherRepaired.getUsedBytes());
    assertEquals(0L, otherRepaired.getUsedNamespace());

    // aborting through the real request path must release exactly what repair counted.
    // No open key is staged, so abort takes its orphan-parts path.
    abortMpu(bucketName, legacyKey, legacyUploadId, BucketLayout.OBJECT_STORE, 5001L);
    abortMpu(bucketName, splitKey, splitUploadId, BucketLayout.OBJECT_STORE, 5002L);
    assertEquals(keyBytes, omMetadataManager.getBucketTable().get(bucketKey).getUsedBytes());
    abortMpu(otherBucketName, otherKey, otherUploadId, BucketLayout.OBJECT_STORE, 5003L);
    assertEquals(0L, omMetadataManager.getBucketTable().get(otherBucketKey).getUsedBytes());
  }

  @Test
  public void testQuotaRepairCountsMPUPartsWithFSO() throws Exception {
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> request = mockQuotaRepairRequest();
    String fsoBucketName = "fsomup" + bucketName;
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, fsoBucketName,
        omMetadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    String keyName = "fsoMpuFile";
    String uploadId = UUID.randomUUID().toString();
    OmMultipartKeyInfo mpuInfo = newMultipartInfo(uploadId, 3001L, true).toBuilder()
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE)).build();
    addMultipartInfo(fsoBucketName, keyName, mpuInfo, 1L);
    putSplitPart(fsoBucketName, keyName, uploadId, 1, 100L);
    long ratisBytes = 100L * 3;

    // EC parity overhead is charged too: one full rs-3-2-1024k stripe of data
    // costs the 3 MiB of data plus 2 MiB of parity
    String ecKeyName = "ecMpuFile";
    String ecUploadId = UUID.randomUUID().toString();
    OmMultipartKeyInfo ecInfo = newMultipartInfo(ecUploadId, 3002L, true).toBuilder()
        .setReplicationConfig(new ECReplicationConfig("rs-3-2-1024k")).build();
    addMultipartInfo(fsoBucketName, ecKeyName, ecInfo, 2L);
    putSplitPart(fsoBucketName, ecKeyName, ecUploadId, 1, 3 * 1024 * 1024L);
    long ecBytes = 5 * 1024 * 1024L;

    String bucketKey = corruptBucketUsage(fsoBucketName, 777L, 88L, 3L);
    applyQuotaRepair(request, 4L);

    OmBucketInfo repaired = omMetadataManager.getBucketTable().get(bucketKey);
    assertEquals(ratisBytes + ecBytes, repaired.getUsedBytes());
    assertEquals(0L, repaired.getUsedNamespace());

    abortMpu(fsoBucketName, keyName, uploadId, BucketLayout.FILE_SYSTEM_OPTIMIZED, 5001L);
    abortMpu(fsoBucketName, ecKeyName, ecUploadId, BucketLayout.FILE_SYSTEM_OPTIMIZED, 5002L);
    assertEquals(0L, omMetadataManager.getBucketTable().get(bucketKey).getUsedBytes());
  }

  @Test
  public void testQuotaRepairSkipsMPUOutsideRequestedBuckets() throws Exception {
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> request = mockQuotaRepairRequest();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.OBJECT_STORE);
    String skippedBucketName = "skipped" + bucketName;
    OMRequestTestUtils.addBucketToDB(volumeName, skippedBucketName,
        omMetadataManager, BucketLayout.OBJECT_STORE);

    String repairedKey = "repairedMpuKey";
    String repairedUploadId = UUID.randomUUID().toString();
    OmMultipartKeyInfo repairedInfo = newMultipartInfo(repairedUploadId, 4001L, false);
    repairedInfo.addPartKeyInfo(createPart(bucketName, repairedKey, repairedUploadId, 1, 500L));
    addMultipartInfo(bucketName, repairedKey, repairedInfo, 1L);

    String skippedKey = "skippedMpuKey";
    String skippedUploadId = UUID.randomUUID().toString();
    OmMultipartKeyInfo skippedInfo = newMultipartInfo(skippedUploadId, 4002L, false);
    skippedInfo.addPartKeyInfo(createPart(skippedBucketName, skippedKey, skippedUploadId, 1, 900L));
    addMultipartInfo(skippedBucketName, skippedKey, skippedInfo, 2L);

    String bucketKey = corruptBucketUsage(bucketName, 12345L, 99L, 3L);
    String skippedBucketKey = corruptBucketUsage(skippedBucketName, 54321L, 77L, 4L);
    applyQuotaRepair(request, 5L, bucketKey);

    assertEquals(500L, omMetadataManager.getBucketTable().get(bucketKey).getUsedBytes());
    // the unrequested bucket keeps its stored usage, its parts are not folded in anywhere
    OmBucketInfo skipped = omMetadataManager.getBucketTable().get(skippedBucketKey);
    assertEquals(54321L, skipped.getUsedBytes());
    assertEquals(77L, skipped.getUsedNamespace());
  }

  private AtomicReference<OzoneManagerProtocolProtos.OMRequest> mockQuotaRepairRequest() throws Exception {
    OzoneManagerProtocolProtos.OMResponse response = mock(OzoneManagerProtocolProtos.OMResponse.class);
    when(response.getSuccess()).thenReturn(true);
    OzoneManagerRatisServer ratisServer = mock(OzoneManagerRatisServer.class);
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> request = new AtomicReference<>();
    doAnswer(invocation -> {
      request.set(invocation.getArgument(0, OzoneManagerProtocolProtos.OMRequest.class));
      return response;
    }).when(ratisServer).submitRequest(any(), any(), anyLong());
    when(ozoneManager.getOmRatisServer()).thenReturn(ratisServer);
    return request;
  }

  private OmMultipartKeyInfo newMultipartInfo(String uploadId, long objectId, boolean splitParts) {
    OmMultipartKeyInfo info = OMRequestTestUtils.createOmMultipartKeyInfo(
        uploadId, Time.now(), HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, objectId);
    if (!splitParts) {
      return info;
    }
    return info.toBuilder()
        .setSchemaVersion(OmMultipartKeyInfo.SPLIT_PARTS_TABLE_SCHEMA_VERSION).build();
  }

  private void addMultipartInfo(String bucket, String keyName, OmMultipartKeyInfo multipartInfo,
      long transactionIndex) throws IOException {
    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName, bucket,
        keyName, multipartInfo.getReplicationConfig()).build();
    OMRequestTestUtils.addMultipartInfoToTable(false, omKeyInfo, multipartInfo, transactionIndex,
        omMetadataManager);
  }

  // repair scans a RocksDB checkpoint, so parts must be written to the table, not the cache
  private void putSplitPart(String bucket, String keyName, String uploadId, int partNumber,
      long dataSize) throws IOException {
    omMetadataManager.getMultipartPartsTable().put(OmMultipartPartKey.of(uploadId, partNumber),
        createSplitPart(bucket, keyName, uploadId, partNumber, dataSize));
  }

  private String corruptBucketUsage(String bucket, long usedBytes, long usedNamespace, long transactionIndex)
      throws IOException {
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucket);
    OmBucketInfo corrupted = omMetadataManager.getBucketTable().get(bucketKey).toBuilder()
        .setUsedBytes(usedBytes).setUsedNamespace(usedNamespace).build();
    omMetadataManager.getBucketTable().put(bucketKey, corrupted);
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(bucketKey), CacheValue.get(transactionIndex, corrupted));
    return bucketKey;
  }

  private void applyQuotaRepair(AtomicReference<OzoneManagerProtocolProtos.OMRequest> request,
      long transactionIndex, String... buckets) throws Exception {
    QuotaRepairTask quotaRepairTask = new QuotaRepairTask(ozoneManager);
    assertTrue(awaitRepair(quotaRepairTask.repair(Arrays.asList(buckets))));
    OMClientResponse response = new OMQuotaRepairRequest(request.get())
        .validateAndUpdateCache(ozoneManager, transactionIndex);
    BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation();
    ((OMQuotaRepairResponse) response).addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
  }

  private void abortMpu(String bucket, String keyName, String uploadId, BucketLayout layout,
      long transactionIndex) throws Exception {
    OzoneManagerProtocolProtos.OMRequest abortRequest = OMRequestTestUtils.createAbortMPURequest(
        volumeName, bucket, keyName, uploadId);
    OzoneManagerProtocolProtos.OMRequest preExecuted;
    OMClientResponse response;
    if (layout == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      preExecuted = new S3MultipartUploadAbortRequestWithFSO(abortRequest, layout).preExecute(ozoneManager);
      response = new S3MultipartUploadAbortRequestWithFSO(preExecuted, layout)
          .validateAndUpdateCache(ozoneManager, transactionIndex);
    } else {
      preExecuted = new S3MultipartUploadAbortRequest(abortRequest, layout).preExecute(ozoneManager);
      response = new S3MultipartUploadAbortRequest(preExecuted, layout)
          .validateAndUpdateCache(ozoneManager, transactionIndex);
    }
    assertTrue(response.getOMResponse().getSuccess());
  }

  private PartKeyInfo createPart(String bucket, String keyName, String uploadId, int partNumber,
      long dataSize) {
    return PartKeyInfo.newBuilder().setPartNumber(partNumber)
        .setPartName(OmMultipartUpload.getDbKey(volumeName, bucket, keyName, uploadId) + "/" + partNumber)
        .setPartKeyInfo(KeyInfo.newBuilder().setVolumeName(volumeName).setBucketName(bucket)
            .setKeyName(keyName).setDataSize(dataSize).setCreationTime(Time.now()).setModificationTime(Time.now())
            .setType(HddsProtos.ReplicationType.RATIS).setFactor(ONE).build())
        .build();
  }

  private OmMultipartPartInfo createSplitPart(String bucket, String keyName, String uploadId,
      int partNumber, long dataSize) {
    OmKeyInfo partKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName, bucket, keyName,
        RatisReplicationConfig.getInstance(ONE)).setDataSize(dataSize)
        .setObjectID(2000L + partNumber).setUpdateID(2000L + partNumber)
        .addMetadata(OzoneConsts.ETAG, "etag-" + partNumber).build();
    return OmMultipartPartInfo.from(
        OmMultipartUpload.getDbKey(volumeName, bucket, keyName, uploadId) + "/" + partNumber,
        partNumber, partKeyInfo);
  }
}
