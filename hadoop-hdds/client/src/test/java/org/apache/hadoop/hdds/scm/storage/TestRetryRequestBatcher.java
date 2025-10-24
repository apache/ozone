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

package org.apache.hadoop.hdds.scm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test the RetryRequestBatcher.
 */
public class TestRetryRequestBatcher {

  private RetryRequestBatcher batcher;

  @BeforeEach
  public void setup() {
    batcher = new RetryRequestBatcher();
  }

  @Test
  public void testBasicWriteChunkAndPutBlock() {
    ChunkBuffer buffer1 = ChunkBuffer.allocate(1024);
    buffer1.put(new byte[512]);

    batcher.trackInflightWriteChunkRequest(buffer1, 0);
    batcher.trackInflightPutBlockRequest(512);

    // Should have 2 pending requests: 1 writeChunk + 1 putBlock
    assertEquals(2, batcher.getPendingRequestCount());
    assertEquals(0, batcher.getAcknowledgedOffset());
    assertEquals(512, batcher.getSentOffset());

    RetryRequestBatcher.OptimizedRetryPlan plan = batcher.optimizeForRetry();
    assertEquals(1, plan.getCombinedChunks().size());
    assertTrue(plan.needsPutBlock());
    assertEquals(512, plan.getTotalDataLength());
  }

  @Test
  public void testCombineMultipleWriteChunks() {
    // Simulate: writeChunk1, writeChunk3 (writeChunk2 already acked)
    ChunkBuffer buffer1 = ChunkBuffer.allocate(1024);
    buffer1.put(new byte[512]);

    ChunkBuffer buffer3 = ChunkBuffer.allocate(1024);
    buffer3.put(new byte[512]);

    batcher.trackInflightWriteChunkRequest(buffer1, 0);
    batcher.trackInflightWriteChunkRequest(buffer3, 1024);

    RetryRequestBatcher.OptimizedRetryPlan plan = batcher.optimizeForRetry();

    // Both chunks should be combined
    assertEquals(2, plan.getCombinedChunks().size());
    assertEquals(1024, plan.getTotalDataLength());
    assertFalse(plan.needsPutBlock());
  }

  @Test
  public void testKeepOnlyLatestPutBlock() {
    // Simulate: writeChunk1, putBlock2, writeChunk3, putBlock4
    ChunkBuffer buffer1 = ChunkBuffer.allocate(1024);
    buffer1.put(new byte[512]);

    ChunkBuffer buffer3 = ChunkBuffer.allocate(1024);
    buffer3.put(new byte[512]);

    batcher.trackInflightWriteChunkRequest(buffer1, 0);
    batcher.trackInflightPutBlockRequest(512);
    batcher.trackInflightWriteChunkRequest(buffer3, 512);
    batcher.trackInflightPutBlockRequest(1024);

    RetryRequestBatcher.OptimizedRetryPlan plan = batcher.optimizeForRetry();

    // Both writeChunks should be combined, only latest putBlock needed
    assertEquals(2, plan.getCombinedChunks().size());
    assertTrue(plan.needsPutBlock());
    assertEquals(1024, plan.getTotalDataLength());
  }

  @Test
  public void testAcknowledgeRemovesRequests() {
    ChunkBuffer buffer1 = ChunkBuffer.allocate(1024);
    buffer1.put(new byte[512]);

    ChunkBuffer buffer2 = ChunkBuffer.allocate(1024);
    buffer2.put(new byte[512]);

    batcher.trackInflightWriteChunkRequest(buffer1, 0);
    batcher.trackInflightPutBlockRequest(512);
    batcher.trackInflightWriteChunkRequest(buffer2, 512);

    assertEquals(3, batcher.getPendingRequestCount());

    // Acknowledge first chunk and putBlock
    batcher.acknowledgeUpTo(512);

    assertEquals(1, batcher.getPendingRequestCount());
    assertEquals(512, batcher.getAcknowledgedOffset());

    RetryRequestBatcher.OptimizedRetryPlan plan = batcher.optimizeForRetry();
    assertEquals(1, plan.getCombinedChunks().size());
  }

  @Test
  public void testClearResetsState() {
    ChunkBuffer buffer1 = ChunkBuffer.allocate(1024);
    buffer1.put(new byte[512]);

    batcher.trackInflightWriteChunkRequest(buffer1, 0);
    batcher.trackInflightPutBlockRequest(512);

    batcher.clear();

    assertEquals(0, batcher.getPendingRequestCount());
    assertEquals(0, batcher.getAcknowledgedOffset());
    assertEquals(0, batcher.getSentOffset());
  }

  @Test
  public void testUnacknowledgedDataLength() {
    ChunkBuffer buffer1 = ChunkBuffer.allocate(1024);
    buffer1.put(new byte[512]);

    batcher.trackInflightWriteChunkRequest(buffer1, 0);

    assertEquals(512, batcher.getUnacknowledgedDataLength());

    batcher.acknowledgeUpTo(256);

    assertEquals(256, batcher.getUnacknowledgedDataLength());

    batcher.acknowledgeUpTo(512);

    assertEquals(0, batcher.getUnacknowledgedDataLength());
  }

  @Test
  public void testEmptyOptimization() {
    RetryRequestBatcher.OptimizedRetryPlan plan = batcher.optimizeForRetry();

    assertEquals(0, plan.getCombinedChunks().size());
    assertFalse(plan.needsPutBlock());
    assertEquals(0, plan.getTotalDataLength());
  }

  @Test
  public void testComplexScenario() {
    // Simulate a complex failure scenario:
    // writeChunk1 (0-512) - sent
    // putBlock (512) - sent
    // writeChunk2 (512-1024) - sent and acked
    // writeChunk3 (1024-1536) - sent
    // putBlock (1536) - sent
    // writeChunk4 (1536-2048) - sent
    // putBlock (2048) - sent
    // Then ack up to 1024

    ChunkBuffer buffer1 = ChunkBuffer.allocate(1024);
    buffer1.put(new byte[512]);

    ChunkBuffer buffer2 = ChunkBuffer.allocate(1024);
    buffer2.put(new byte[512]);

    ChunkBuffer buffer3 = ChunkBuffer.allocate(1024);
    buffer3.put(new byte[512]);

    ChunkBuffer buffer4 = ChunkBuffer.allocate(1024);
    buffer4.put(new byte[512]);

    batcher.trackInflightWriteChunkRequest(buffer1, 0);
    batcher.trackInflightPutBlockRequest(512);
    batcher.trackInflightWriteChunkRequest(buffer2, 512);
    batcher.trackInflightWriteChunkRequest(buffer3, 1024);
    batcher.trackInflightPutBlockRequest(1536);
    batcher.trackInflightWriteChunkRequest(buffer4, 1536);
    batcher.trackInflightPutBlockRequest(2048);

    // Acknowledge up to 1024 (buffer2 is acked)
    batcher.acknowledgeUpTo(1024);

    RetryRequestBatcher.OptimizedRetryPlan plan = batcher.optimizeForRetry();

    // Should have buffer3 and buffer4 combined
    assertEquals(2, plan.getCombinedChunks().size());
    // Should need putBlock (the latest one at 2048)
    assertTrue(plan.needsPutBlock());
    // Total data length should be 1024 (buffer3 + buffer4)
    assertEquals(1024, plan.getTotalDataLength());
  }

  @Test
  public void testOnlyPutBlockRequests() {
    batcher.trackInflightPutBlockRequest(512);
    batcher.trackInflightPutBlockRequest(1024);
    batcher.trackInflightPutBlockRequest(1536);

    assertEquals(1, batcher.getPendingRequestCount());

    RetryRequestBatcher.OptimizedRetryPlan plan = batcher.optimizeForRetry();

    // No chunks to combine
    assertEquals(0, plan.getCombinedChunks().size());
    // But we need the latest putBlock
    assertTrue(plan.needsPutBlock());
    assertEquals(0, plan.getTotalDataLength());
  }

  @Test
  public void testPutBlockKeepsOnlyHighestOffset() {
    batcher.trackInflightPutBlockRequest(512);
    batcher.trackInflightPutBlockRequest(256);
    batcher.trackInflightPutBlockRequest(1024);

    assertEquals(1, batcher.getPendingRequestCount());

    RetryRequestBatcher.OptimizedRetryPlan plan = batcher.optimizeForRetry();
    assertTrue(plan.needsPutBlock());
  }

  @Test
  public void testSentOffsetTracking() {
    ChunkBuffer buffer1 = ChunkBuffer.allocate(1024);
    buffer1.put(new byte[512]);

    ChunkBuffer buffer2 = ChunkBuffer.allocate(1024);
    buffer2.put(new byte[256]);

    batcher.trackInflightWriteChunkRequest(buffer1, 0);
    assertEquals(512, batcher.getSentOffset());

    batcher.trackInflightWriteChunkRequest(buffer2, 512);
    assertEquals(768, batcher.getSentOffset());
  }
}
