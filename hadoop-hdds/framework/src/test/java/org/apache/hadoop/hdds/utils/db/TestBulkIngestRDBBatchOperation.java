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

package org.apache.hadoop.hdds.utils.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

/**
 * This class tests the functionality of the {@link BulkIngestRDBBatchOperation} class,
 * ensuring that bulk ingestion operations are executed correctly with proper batch
 * management and flushing behavior.
 *
 * The tests verify multiple aspects of batch behavior:
 * - Flushing when the batch size reaches the threshold during operations.
 * - Flushing of the last non-full batch when the batch operation is closed.
 * - Creating new batches after each flush to avoid reusing old ones.
 *
 * Additionally, the tests validate that resources, such as batch objects, are
 * properly managed and closed after being used.
 *
 * Key features tested include:
 * - Correct detection of batch size thresholds and execution of flushes.
 * - Handling of multiple flushes, ensuring that new batches are created each time.
 * - Flushing of remaining operations upon closing the bulk ingestion process.
 */
public class TestBulkIngestRDBBatchOperation {

  private static final class RecordingBulkIngest extends BulkIngestRDBBatchOperation {
    private final List<RDBBatchOperation> flushed = new ArrayList<>();

    RecordingBulkIngest(int batchSize) {
      super(batchSize);
    }

    @Override
    public void opOnBatchFull(RDBBatchOperation batch) {
      flushed.add(batch);
    }

    List<RDBBatchOperation> getFlushedBatches() {
      return flushed;
    }
  }

  @Test
  public void testFlushWhenBatchReachesThresholdDuringOperations() throws Exception {
    final AtomicInteger instanceIdx = new AtomicInteger();
    try (MockedConstruction<RDBBatchOperation> mocked = Mockito.mockConstruction(RDBBatchOperation.class,
        (batch, context) -> {
          // First batch: 2 ops; size checked after each op.
          if (instanceIdx.getAndIncrement() == 0) {
            when(batch.size()).thenReturn(5L, 10L);
          } else {
            when(batch.size()).thenReturn(0L);
          }
        })) {

      final RecordingBulkIngest bulk = new RecordingBulkIngest(10);
      final ColumnFamily family = mock(ColumnFamily.class);

      bulk.put(family, new byte[] {0x01}, new byte[] {0x11});
      assertEquals(0, bulk.getFlushedBatches().size(), "should not flush before threshold is reached");

      bulk.delete(family, new byte[] {0x02});

      assertEquals(1, bulk.getFlushedBatches().size(), "should flush when threshold is reached");
      assertSame(mocked.constructed().get(0), bulk.getFlushedBatches().get(0));

      // flush should close old batch and construct a new one
      verify(mocked.constructed().get(0), times(1)).close();
      assertEquals(2, mocked.constructed().size(), "should construct a new batch after flushing");

      // close() should flush the final (possibly non-full) batch as well
      bulk.close();
      assertEquals(2, bulk.getFlushedBatches().size(), "close() should flush the remaining batch");
      assertSame(mocked.constructed().get(1), bulk.getFlushedBatches().get(1));
      verify(mocked.constructed().get(1), times(1)).close();
    }
  }

  @Test
  public void testCloseFlushesEvenIfThresholdNotReached() throws Exception {
    try (MockedConstruction<RDBBatchOperation> mocked = Mockito.mockConstruction(RDBBatchOperation.class,
        (batch, context) -> when(batch.size()).thenReturn(1L))) {

      final RecordingBulkIngest bulk = new RecordingBulkIngest(10);
      final ColumnFamily family = mock(ColumnFamily.class);

      bulk.put(family, new byte[] {0x01}, new byte[] {0x11});
      assertEquals(0, bulk.getFlushedBatches().size(), "should not flush during ops if not full");

      bulk.close();

      assertEquals(1, bulk.getFlushedBatches().size(), "close() should flush");
      assertSame(mocked.constructed().get(0), bulk.getFlushedBatches().get(0));
      verify(mocked.constructed().get(0), times(1)).close();
      assertEquals(1, mocked.constructed().size(), "close() should not create a new batch");
    }
  }

  @Test
  public void testMultipleFlushesCreateNewBatchEachTime() throws Exception {
    try (MockedConstruction<RDBBatchOperation> mocked = Mockito.mockConstruction(RDBBatchOperation.class,
        (batch, context) -> when(batch.size()).thenReturn(1L))) {

      final RecordingBulkIngest bulk = new RecordingBulkIngest(1);
      final ColumnFamily family = mock(ColumnFamily.class);

      bulk.put(family, new byte[] {0x01}, new byte[] {0x11}); // flush batch #0
      bulk.delete(family, new byte[] {0x02});                 // flush batch #1
      bulk.deleteRange(family, new byte[] {0x03}, new byte[] {0x04}); // flush batch #2

      // Each op flushes immediately, creating a new batch each time.
      assertEquals(3, bulk.getFlushedBatches().size());
      assertEquals(4, mocked.constructed().size(), "3 flushes should create 3 new batches (plus the initial)");

      // close() flushes the current batch (#3) too
      bulk.close();
      assertEquals(4, bulk.getFlushedBatches().size());

      // Every constructed batch should be closed exactly once.
      for (RDBBatchOperation b : mocked.constructed()) {
        verify(b, times(1)).close();
      }
    }
  }

  @Test
  public void testDelegatesOpsToUnderlyingRdbBatchOperation() throws Exception {
    try (MockedConstruction<RDBBatchOperation> mocked = Mockito.mockConstruction(RDBBatchOperation.class,
        (batch, context) -> when(batch.size()).thenReturn(0L))) {

      final RecordingBulkIngest bulk = new RecordingBulkIngest(10);
      final ColumnFamily family = mock(ColumnFamily.class);

      final byte[] key1 = new byte[] {0x01};
      final byte[] value1 = new byte[] {0x11};
      final byte[] key2 = new byte[] {0x02};
      final byte[] start = new byte[] {0x03};
      final byte[] end = new byte[] {0x04};

      bulk.put(family, key1, value1);
      bulk.delete(family, key2);
      bulk.deleteRange(family, start, end);
      try (CodecBuffer cKey = CodecBuffer.wrap(new byte[] {0x05});
           CodecBuffer cValue = CodecBuffer.wrap(new byte[] {0x55})) {
        bulk.put(family, cKey, cValue);
      }

      assertEquals(1, mocked.constructed().size(), "should use a single batch when not full");
      final RDBBatchOperation batch0 = mocked.constructed().get(0);
      verify(batch0, times(1)).put(family, key1, value1);
      verify(batch0, times(1)).delete(family, key2);
      verify(batch0, times(1)).deleteRange(family, start, end);
      verify(batch0, times(1)).put(Mockito.eq(family), Mockito.any(CodecBuffer.class), Mockito.any(CodecBuffer.class));

      // No flush should have occurred before close().
      assertEquals(0, bulk.getFlushedBatches().size());

      bulk.close();
      assertEquals(1, bulk.getFlushedBatches().size(), "close() should flush the remaining batch");
      verify(batch0, times(1)).close();
    }
  }

  @Test
  public void testDelegatesToNewBatchAfterFlush() throws Exception {
    final AtomicInteger instanceIdx = new AtomicInteger();
    try (MockedConstruction<RDBBatchOperation> mocked = Mockito.mockConstruction(
        RDBBatchOperation.class,
        (batch, context) -> {
          // First batch flushes immediately; second batch does not.
          if (instanceIdx.getAndIncrement() == 0) {
            when(batch.size()).thenReturn(1L);
          } else {
            when(batch.size()).thenReturn(0L);
          }
        })) {

      final RecordingBulkIngest bulk = new RecordingBulkIngest(1);
      final ColumnFamily family = mock(ColumnFamily.class);

      final byte[] key1 = new byte[] {0x01};
      final byte[] value1 = new byte[] {0x11};
      final byte[] key2 = new byte[] {0x02};

      bulk.put(family, key1, value1); // goes to batch #0 and flushes
      assertEquals(2, mocked.constructed().size(), "flush should construct a new batch");

      final RDBBatchOperation batch0 = mocked.constructed().get(0);
      final RDBBatchOperation batch1 = mocked.constructed().get(1);

      verify(batch0, times(1)).put(family, key1, value1);
      verify(batch0, times(1)).close();

      bulk.delete(family, key2); // should go to new batch (#1)
      verify(batch1, times(1)).delete(family, key2);
      try (CodecBuffer cKey = CodecBuffer.wrap(new byte[] {0x03});
           CodecBuffer cValue = CodecBuffer.wrap(new byte[] {0x33})) {
        bulk.put(family, cKey, cValue); // should also go to new batch (#1)
        verify(batch1, times(1)).put(family, cKey, cValue);
      }
      assertEquals(2, mocked.constructed().size(), "op on non-full second batch should not create a new batch");

      bulk.close();
      verify(batch1, times(1)).close();
    }
  }

}


