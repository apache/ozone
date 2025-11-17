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

package org.apache.hadoop.ozone.om.ratis;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.newBucketInfoBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketCreateRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketDeleteRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeCreateRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketCreateResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketDeleteResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * This class tests OzoneManagerDouble Buffer with actual OMResponse classes.
 */
public class TestOzoneManagerDoubleBufferWithOMResponse {

  private static final int MAX_VOLUMES = 1000;

  private OzoneManager ozoneManager;
  private OMMetadataManager omMetadataManager;
  private OzoneManagerDoubleBuffer doubleBuffer;
  private final AtomicLong trxId = new AtomicLong(0);
  private long term = 1L;

  @TempDir
  private Path folder;

  @BeforeEach
  public void setup() throws IOException {
    ozoneManager = mock(OzoneManager.class, withSettings().stubOnly());
    OMMetrics omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getMaxUserVolumeCount()).thenReturn(10L);
    AuditLogger auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    when(ozoneManager.getConfig()).thenReturn(ozoneConfiguration.getObject(OmConfig.class));
    doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
    doubleBuffer = OzoneManagerDoubleBuffer.newBuilder()
        .setOmMetadataManager(omMetadataManager)
        .setMaxUnFlushedTransactionCount(100000)
        .build()
        .start();
  }

  @AfterEach
  public void stop() {
    doubleBuffer.stop();
  }

  /**
   * This tests OzoneManagerDoubleBuffer implementation. It calls
   * testDoubleBuffer with number of iterations to do transactions and
   * number of buckets to be created in each iteration. It then
   * verifies OM DB entries count is matching with total number of
   * transactions or not.
   */
  @Test
  public void testDoubleBuffer1() throws Exception {
    testDoubleBuffer(1, 10);
  }

  @Test
  public void testDoubleBuffer10() throws Exception {
    testDoubleBuffer(10, 100);
  }

  @Test
  public void testDoubleBuffer100() throws Exception {
    testDoubleBuffer(100, 100);
  }

  @Test
  public void testDoubleBuffer1000() throws Exception {
    testDoubleBuffer(MAX_VOLUMES, 500);
  }

  /**
   * This test first creates a volume, and then does a mix of transactions
   * like create/delete buckets and add them to double buffer. Then it
   * verifies OM DB entries are matching with actual responses added to
   * double buffer or not.
   */
  @Test
  public void testDoubleBufferWithMixOfTransactions() throws Exception {
    // This test checks count, data in table is correct or not.
    Queue< OMBucketCreateResponse > bucketQueue =
        new ConcurrentLinkedQueue<>();
    Queue< OMBucketDeleteResponse > deleteBucketQueue =
        new ConcurrentLinkedQueue<>();

    String volumeName = UUID.randomUUID().toString();
    OMVolumeCreateResponse omVolumeCreateResponse =
        (OMVolumeCreateResponse) createVolume(volumeName,
            trxId.incrementAndGet());

    int bucketCount = 10;

    doMixTransactions(volumeName, bucketCount, deleteBucketQueue, bucketQueue);

    // As for every 2 transactions of create bucket we add deleted bucket.
    final int deleteCount = 5;

    // We are doing +1 for volume transaction.
    GenericTestUtils.waitFor(
        () -> doubleBuffer.getFlushedTransactionCountForTesting() == bucketCount + deleteCount + 1,
        100, 120000);

    assertEquals(1, omMetadataManager.countRowsInTable(
        omMetadataManager.getVolumeTable()));

    assertEquals(5, omMetadataManager.countRowsInTable(
        omMetadataManager.getBucketTable()));

    // Now after this in our DB we should have 5 buckets and one volume

    checkVolume(volumeName, omVolumeCreateResponse);

    checkCreateBuckets(bucketQueue);

    checkDeletedBuckets(deleteBucketQueue);

    // Check lastAppliedIndex is updated correctly or not.
    final long expectedIndex = bucketCount + deleteCount + 1;
    GenericTestUtils.waitFor(() -> assertTransactionInfo(expectedIndex),
        100, 30000);
  }

  private boolean assertTransactionInfo(long lastAppliedIndex) {
    final TransactionInfo info;
    try {
      info = omMetadataManager.getTransactionInfoTable().get(TRANSACTION_INFO_KEY);
    } catch (IOException e) {
      return false;
    }
    return info != null
        && info.getTransactionIndex() == lastAppliedIndex
        && info.getTerm() == term;
  }

  /**
   * This test first creates a volume, and then does a mix of transactions
   * like create/delete buckets in parallel and add to double buffer. Then it
   * verifies OM DB entries are matching with actual responses added to
   * double buffer or not.
   */
  @Test
  public void testDoubleBufferWithMixOfTransactionsParallel() throws Exception {
    // This test checks count, data in table is correct or not.

    Queue< OMBucketCreateResponse > bucketQueue =
        new ConcurrentLinkedQueue<>();
    Queue< OMBucketDeleteResponse > deleteBucketQueue =
        new ConcurrentLinkedQueue<>();

    String volumeName1 = UUID.randomUUID().toString();

    OMVolumeCreateResponse omVolumeCreateResponse1 =
        (OMVolumeCreateResponse) createVolume(volumeName1,
            trxId.incrementAndGet());

    String volumeName2 = UUID.randomUUID().toString();
    OMVolumeCreateResponse omVolumeCreateResponse2 =
        (OMVolumeCreateResponse) createVolume(volumeName2,
            trxId.incrementAndGet());

    int bucketsPerVolume = 10;

    Daemon daemon1 = new Daemon(() -> doMixTransactions(volumeName1,
        bucketsPerVolume, deleteBucketQueue, bucketQueue));
    Daemon daemon2 = new Daemon(() -> doMixTransactions(volumeName2,
        bucketsPerVolume, deleteBucketQueue, bucketQueue));

    daemon1.start();
    daemon2.start();

    int bucketCount = 2 * bucketsPerVolume;

      // As for every 2 transactions of create bucket we add deleted bucket.
    final int deleteCount = 10;

    // We are doing +1 for volume transaction.
    GenericTestUtils.waitFor(
        () -> doubleBuffer.getFlushedTransactionCountForTesting() == bucketCount + deleteCount + 2,
        100, 120000);

    assertEquals(2, omMetadataManager.countRowsInTable(
        omMetadataManager.getVolumeTable()));

    assertEquals(10, omMetadataManager.countRowsInTable(
        omMetadataManager.getBucketTable()));

    // Now after this in our DB we should have 5 buckets and one volume


    checkVolume(volumeName1, omVolumeCreateResponse1);
    checkVolume(volumeName2, omVolumeCreateResponse2);

    checkCreateBuckets(bucketQueue);

    checkDeletedBuckets(deleteBucketQueue);

    // Not checking lastAppliedIndex here, because 2 daemon threads are
    // running in parallel, so lastAppliedIndex cannot be always
    // total transaction count. So, just checking here whether it is less
    // than total transaction count.
    final TransactionInfo info = omMetadataManager.getTransactionInfoTable().get(TRANSACTION_INFO_KEY);
    assertThat(info.getTransactionIndex()).isLessThanOrEqualTo(bucketCount + deleteCount + 2);
  }

  /**
   * This method add's a mix of createBucket/DeleteBucket responses to double
   * buffer. Total number of responses added is specified by bucketCount.
   */
  private void doMixTransactions(String volumeName, int bucketCount,
      Queue<OMBucketDeleteResponse> deleteBucketQueue,
      Queue<OMBucketCreateResponse> bucketQueue) {
    for (int i = 0; i < bucketCount; i++) {
      String bucketName = UUID.randomUUID().toString();
      long transactionID = trxId.incrementAndGet();
      OMBucketCreateResponse omBucketCreateResponse = createBucket(volumeName,
          bucketName, transactionID);
      // For every 2 transactions have a deleted bucket.
      if (i % 2 == 0) {
        OMBucketDeleteResponse omBucketDeleteResponse =
            (OMBucketDeleteResponse) deleteBucket(volumeName, bucketName,
                trxId.incrementAndGet());
        deleteBucketQueue.add(omBucketDeleteResponse);
      } else {
        bucketQueue.add(omBucketCreateResponse);
      }
    }
  }

  private OMClientResponse deleteBucket(String volumeName, String bucketName,
      long transactionID) {
    OzoneManagerProtocolProtos.OMRequest omRequest =
        OMRequestTestUtils.createDeleteBucketRequest(volumeName, bucketName);

    OMBucketDeleteRequest omBucketDeleteRequest =
        new OMBucketDeleteRequest(omRequest);

    final TermIndex termIndex = TermIndex.valueOf(term, transactionID);
    final ExecutionContext context = ExecutionContext.of(termIndex.getIndex(), termIndex);
    OMClientResponse omClientResponse = omBucketDeleteRequest.validateAndUpdateCache(ozoneManager, context);
    doubleBuffer.add(omClientResponse, termIndex);
    return omClientResponse;
  }

  /**
   * Verifies volume table data is matching with actual response added to
   * double buffer.
   */
  private void checkVolume(String volumeName,
      OMVolumeCreateResponse omVolumeCreateResponse) throws Exception {
    OmVolumeArgs tableVolumeArgs = omMetadataManager.getVolumeTable().get(
        omMetadataManager.getVolumeKey(volumeName));
    assertNotNull(tableVolumeArgs);

    OmVolumeArgs omVolumeArgs = omVolumeCreateResponse.getOmVolumeArgs();

    assertEquals(omVolumeArgs.getVolume(), tableVolumeArgs.getVolume());
    assertEquals(omVolumeArgs.getAdminName(), tableVolumeArgs.getAdminName());
    assertEquals(omVolumeArgs.getOwnerName(), tableVolumeArgs.getOwnerName());
    assertEquals(omVolumeArgs.getCreationTime(), tableVolumeArgs.getCreationTime());
  }

  /**
   * Verifies bucket table data is matching with actual response added to
   * double buffer.
   */
  private void checkCreateBuckets(Queue<OMBucketCreateResponse> bucketQueue) {
    bucketQueue.forEach((omBucketCreateResponse) -> {
      OmBucketInfo omBucketInfo = omBucketCreateResponse.getOmBucketInfo();
      String bucket = omBucketInfo.getBucketName();
      OmBucketInfo tableBucketInfo = null;
      try {
        tableBucketInfo =
            omMetadataManager.getBucketTable().get(
                omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
                    bucket));
      } catch (IOException ex) {
        fail("testDoubleBufferWithMixOfTransactions failed");
      }
      assertNotNull(tableBucketInfo);

      assertEquals(omBucketInfo.getVolumeName(), tableBucketInfo.getVolumeName());
      assertEquals(omBucketInfo.getBucketName(), tableBucketInfo.getBucketName());
      assertEquals(omBucketInfo.getCreationTime(), tableBucketInfo.getCreationTime());
    });
  }

  /**
   * Verifies deleted bucket responses added to double buffer are actually
   * removed from the OM DB or not.
   */
  private void checkDeletedBuckets(Queue<OMBucketDeleteResponse>
      deleteBucketQueue) {
    deleteBucketQueue.forEach((omBucketDeleteResponse -> {
      try {
        assertNull(omMetadataManager.getBucketTable().get(
            omMetadataManager.getBucketKey(
                omBucketDeleteResponse.getVolumeName(),
                omBucketDeleteResponse.getBucketName())));
      } catch (IOException ex) {
        fail("testDoubleBufferWithMixOfTransactions failed");
      }
    }));
  }

  /**
   * Create bucketCount number of createBucket responses for each iteration.
   * All these iterations are run in parallel. Then verify OM DB has correct
   * number of entries or not.
   */
  private void testDoubleBuffer(int volumeCount, int bucketsPerVolume)
      throws Exception {
    // Reset transaction id.
    trxId.set(0);
    for (int i = 0; i < volumeCount; i++) {
      Daemon d1 = new Daemon(() -> doTransactions(bucketsPerVolume));
      d1.start();
    }

    // We are doing +1 for volume transaction.
    // Here not checking lastAppliedIndex because transactionIndex is
    // shared across threads, and lastAppliedIndex cannot be always
    // expectedTransactions. So, skipping that check here.
    int expectedBuckets = bucketsPerVolume * volumeCount;
    long expectedTransactions = volumeCount + expectedBuckets;

    GenericTestUtils.waitFor(
        () -> expectedTransactions == doubleBuffer.getFlushedTransactionCountForTesting(),
        100, volumeCount * 500);

    GenericTestUtils.waitFor(() ->
        assertRowCount(volumeCount, omMetadataManager.getVolumeTable()),
        300, volumeCount * 300);


    GenericTestUtils.waitFor(() ->
        assertRowCount(expectedBuckets, omMetadataManager.getBucketTable()),
        300, volumeCount * 300);

    assertThat(doubleBuffer.getFlushIterationsForTesting()).isGreaterThan(0);
  }

  private boolean assertRowCount(int expected, Table<String, ?> table) {
    AtomicLong count = new AtomicLong(0L);
    assertDoesNotThrow(() -> count.set(omMetadataManager.countRowsInTable(table)));
    return count.get() == expected;
  }

  /**
   * This method adds bucketCount number of createBucket responses to double
   * buffer.
   */
  private void doTransactions(int bucketCount) {
    String volumeName = UUID.randomUUID().toString();
    createVolume(volumeName, trxId.incrementAndGet());
    for (int i = 0; i < bucketCount; i++) {
      createBucket(volumeName, UUID.randomUUID().toString(),
          trxId.incrementAndGet());
    }
  }

  /**
   * Create OMVolumeCreateResponse for specified volume.
   * @return OMVolumeCreateResponse
   */
  private OMClientResponse createVolume(String volumeName,
      long transactionId) {

    String admin = OzoneConsts.OZONE;
    String owner = UUID.randomUUID().toString();
    OzoneManagerProtocolProtos.OMRequest omRequest =
        OMRequestTestUtils.createVolumeRequest(volumeName, admin, owner);

    OMVolumeCreateRequest omVolumeCreateRequest =
        new OMVolumeCreateRequest(omRequest);
    try {
      omVolumeCreateRequest.setUGI(UserGroupInformation.getCurrentUser());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final TermIndex termIndex = TransactionInfo.getTermIndex(transactionId);
    final ExecutionContext context = ExecutionContext.of(termIndex.getIndex(), termIndex);
    OMClientResponse omClientResponse = omVolumeCreateRequest.validateAndUpdateCache(ozoneManager, context);
    doubleBuffer.add(omClientResponse, termIndex);
    return omClientResponse;
  }

  /**
   * Create OMBucketCreateResponse for specified volume and bucket.
   * @return OMBucketCreateResponse
   */
  private OMBucketCreateResponse createBucket(String volumeName,
      String bucketName, long transactionID) {

    BucketInfo.Builder bucketInfo =
        newBucketInfoBuilder(bucketName, volumeName)
            .setStorageType(StorageTypeProto.DISK);
    OzoneManagerProtocolProtos.OMRequest omRequest =
        OMRequestTestUtils.newCreateBucketRequest(bucketInfo).build();

    OMBucketCreateRequest omBucketCreateRequest =
        new OMBucketCreateRequest(omRequest);
    try {
      omBucketCreateRequest.setUGI(UserGroupInformation.getCurrentUser());
    } catch (IOException e) {
    }

    final TermIndex termIndex = TermIndex.valueOf(term, transactionID);
    final ExecutionContext context = ExecutionContext.of(termIndex.getIndex(), termIndex);
    OMClientResponse omClientResponse = omBucketCreateRequest.validateAndUpdateCache(ozoneManager, context);
    doubleBuffer.add(omClientResponse, termIndex);
    return (OMBucketCreateResponse) omClientResponse;
  }
}

