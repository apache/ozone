/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.ratis;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.om.ratis.metrics.OzoneManagerDoubleBufferMetrics;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This class tests OzoneManagerDoubleBuffer implementation with
 * dummy response class.
 */
@Timeout(300)
public class TestOzoneManagerDoubleBufferWithDummyResponse {

  private OMMetadataManager omMetadataManager;
  private OzoneManagerDoubleBuffer doubleBuffer;
  private final AtomicLong trxId = new AtomicLong(0);
  private long term = 1L;
  @TempDir
  private Path folder;

  @BeforeEach
  public void setup() throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_METADATA_DIRS,
        folder.toAbsolutePath().toString());
    omMetadataManager =
        new OmMetadataManagerImpl(configuration, null);
    doubleBuffer = new OzoneManagerDoubleBuffer.Builder()
        .setOmMetadataManager(omMetadataManager)
        .setmaxUnFlushedTransactionCount(10000)
        .enableRatis(true)
        .build();
  }

  @AfterEach
  public void stop() {
    doubleBuffer.stop();
  }

  /**
   * This tests add's 100 bucket creation responses to doubleBuffer, and
   * check OM DB bucket table has 100 entries or not. In addition checks
   * flushed transaction count is matching with expected count or not.
   */
  @Test
  public void testDoubleBufferWithDummyResponse() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    int bucketCount = 100;
    OzoneManagerDoubleBufferMetrics metrics =
        doubleBuffer.getOzoneManagerDoubleBufferMetrics();

    // As we have not flushed/added any transactions, all metrics should have
    // value zero.
    assertEquals(0, metrics.getTotalNumOfFlushOperations());
    assertEquals(0, metrics.getTotalNumOfFlushedTransactions());
    assertEquals(0, metrics.getMaxNumberOfTransactionsFlushedInOneIteration());

    for (int i = 0; i < bucketCount; i++) {
      doubleBuffer.add(createDummyBucketResponse(volumeName),
          TermIndex.valueOf(term, trxId.incrementAndGet()));
    }
    waitFor(() -> metrics.getTotalNumOfFlushedTransactions() == bucketCount,
        100, 60000);

    assertTrue(metrics.getTotalNumOfFlushOperations() > 0);
    assertEquals(bucketCount, doubleBuffer.getFlushedTransactionCount());
    assertTrue(metrics.getMaxNumberOfTransactionsFlushedInOneIteration() > 0);
    assertEquals(bucketCount, omMetadataManager.countRowsInTable(
        omMetadataManager.getBucketTable()));
    assertTrue(doubleBuffer.getFlushIterations() > 0);
    assertTrue(metrics.getFlushTime().lastStat().numSamples() > 0);
    assertTrue(metrics.getAvgFlushTransactionsInOneIteration() > 0);
    assertEquals(bucketCount, (long) metrics.getQueueSize().lastStat().total());
    assertTrue(metrics.getQueueSize().lastStat().numSamples() > 0);

    // Assert there is only instance of OM Double Metrics.
    OzoneManagerDoubleBufferMetrics metricsCopy =
        OzoneManagerDoubleBufferMetrics.create();
    assertEquals(metrics, metricsCopy);

    TransactionInfo transactionInfo =
        omMetadataManager.getTransactionInfoTable().get(TRANSACTION_INFO_KEY);
    // Check lastAppliedIndex is updated correctly or not.
    assertNotNull(transactionInfo);
    assertEquals(bucketCount, transactionInfo.getTransactionIndex());
    Assertions.assertEquals(term, transactionInfo.getTerm());
  }

  /**
   * Create DummyBucketCreate response.
   */
  private OMDummyCreateBucketResponse createDummyBucketResponse(
      String volumeName) {
    OmBucketInfo omBucketInfo =
        OmBucketInfo.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(UUID.randomUUID().toString())
            .setCreationTime(Time.now())
            .build();
    return new OMDummyCreateBucketResponse(omBucketInfo,
        OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateBucket)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCreateBucketResponse(CreateBucketResponse.newBuilder().build())
            .build());
  }


  /**
   * DummyCreatedBucket Response class used in testing.
   */
  @CleanupTableInfo(cleanupTables = {BUCKET_TABLE})
  private static class OMDummyCreateBucketResponse extends OMClientResponse {
    private final OmBucketInfo omBucketInfo;

    OMDummyCreateBucketResponse(OmBucketInfo omBucketInfo,
        OMResponse omResponse) {
      super(omResponse);
      this.omBucketInfo = omBucketInfo;
    }

    @Override
    public void addToDBBatch(OMMetadataManager omMetadataManager,
        BatchOperation batchOperation) throws IOException {
      String dbBucketKey =
          omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
              omBucketInfo.getBucketName());
      omMetadataManager.getBucketTable().putWithBatch(batchOperation,
          dbBucketKey, omBucketInfo);
    }
  }
}
