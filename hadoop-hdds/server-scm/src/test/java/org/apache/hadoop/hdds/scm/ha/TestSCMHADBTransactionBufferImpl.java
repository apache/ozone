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

package org.apache.hadoop.hdds.scm.ha;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_PENDING_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_PENDING_LIMIT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.File;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for the count-based flush of {@link SCMHADBTransactionBufferImpl}
 * ({@link SCMHADBTransactionBufferImpl#flushIfPendingLimitReached()}).
 */
public class TestSCMHADBTransactionBufferImpl {

  private static final TransactionInfo TRX_INFO_T4 = TransactionInfo.valueOf(1, 4);
  private static final TransactionInfo TRX_INFO_T5 = TransactionInfo.valueOf(1, 5);
  private static final ByteString VALUE = ByteString.copyFromUtf8("value");

  @TempDir
  private File testDir;

  private SCMMetadataStore metadataStore;
  private Table<String, ByteString> serviceConfigTable;
  private Table<String, TransactionInfo> transactionInfoTable;
  private DeletedBlockLogImpl deletedBlockLog;
  private final List<SCMHADBTransactionBufferImpl> buffers = new ArrayList<>();

  @BeforeEach
  public void setup() throws Exception {
    metadataStore = new SCMMetadataStoreImpl(SCMTestUtils.getConf(testDir));
    serviceConfigTable = metadataStore.getStatefulServiceConfigTable();
    transactionInfoTable = metadataStore.getTransactionInfoTable();
    deletedBlockLog = mock(DeletedBlockLogImpl.class);
  }

  @AfterEach
  public void cleanup() throws Exception {
    for (SCMHADBTransactionBufferImpl buffer : buffers) {
      buffer.close();
    }
    if (metadataStore != null) {
      metadataStore.stop();
    }
  }

  @Test
  public void testNoFlushBelowLimit() throws Exception {
    SCMHADBTransactionBufferImpl buffer = newBuffer(3);
    buffer.updateLatestTrxInfo(TRX_INFO_T4);
    buffer.flush();
    clearInvocations(deletedBlockLog);

    for (int i = 0; i < 2; i++) {
      put(buffer, "key" + i);
      assertFalse(buffer.flushIfPendingLimitReached());
    }
    assertNotDurable("key0");
    assertNotDurable("key1");
    verify(deletedBlockLog, never()).onFlush();
  }

  @Test
  public void testFlushAtLimitPersistsWholeBatchAndTransactionInfo() throws Exception {
    SCMHADBTransactionBufferImpl buffer = newBuffer(3);
    buffer.updateLatestTrxInfo(TRX_INFO_T4);
    buffer.flush();
    clearInvocations(deletedBlockLog);

    put(buffer, "key0");
    put(buffer, "key1");
    buffer.updateLatestTrxInfo(TRX_INFO_T5);
    put(buffer, "key2");

    assertTrue(buffer.flushIfPendingLimitReached());
    assertDurable("key0");
    assertDurable("key1");
    assertDurable("key2");
    assertEquals(TRX_INFO_T5, transactionInfoTable.get(TRANSACTION_INFO_KEY));
    verify(deletedBlockLog, times(1)).onFlush();
  }

  @Test
  public void testSingleApplyOvershootingLimitFlushesOnce() throws Exception {
    SCMHADBTransactionBufferImpl buffer = newBuffer(3);
    buffer.updateLatestTrxInfo(TRX_INFO_T4);
    buffer.flush();
    clearInvocations(deletedBlockLog);

    for (int i = 0; i < 5; i++) {
      put(buffer, "key" + i);
    }
    buffer.updateLatestTrxInfo(TRX_INFO_T5);

    assertTrue(buffer.flushIfPendingLimitReached());
    for (int i = 0; i < 5; i++) {
      assertDurable("key" + i);
    }
    assertEquals(TRX_INFO_T5, transactionInfoTable.get(TRANSACTION_INFO_KEY));
    assertFalse(buffer.flushIfPendingLimitReached(), "buffer was drained, nothing left to flush");
    verify(deletedBlockLog, times(1)).onFlush();
  }

  @Test
  public void testCounterRestartsAfterFlush() throws Exception {
    SCMHADBTransactionBufferImpl buffer = newBuffer(3);
    buffer.updateLatestTrxInfo(TRX_INFO_T4);
    buffer.flush();

    for (int i = 0; i < 3; i++) {
      put(buffer, "a" + i);
    }
    assertTrue(buffer.flushIfPendingLimitReached());

    put(buffer, "b0");
    put(buffer, "b1");
    assertFalse(buffer.flushIfPendingLimitReached(), "only 2 pending after the previous flush");
    assertNotDurable("b0");

    put(buffer, "b2");
    assertTrue(buffer.flushIfPendingLimitReached());
    assertDurable("b0");
    assertDurable("b1");
    assertDurable("b2");
  }

  @Test
  public void testFlushesWhileApplyingTransactionIsInProgress() throws Exception {
    SCMHADBTransactionBufferImpl buffer = newBuffer(1);
    buffer.updateLatestTrxInfo(TRX_INFO_T4);
    buffer.flush();

    buffer.beginApplyingTransaction();
    try {
      put(buffer, "key");
      buffer.updateLatestTrxInfo(TRX_INFO_T5);
      assertTrue(buffer.flushIfPendingLimitReached());
      assertDurable("key");
      assertEquals(TRX_INFO_T5, transactionInfoTable.get(TRANSACTION_INFO_KEY));
    } finally {
      buffer.endApplyingTransaction();
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {0, -1, Long.MIN_VALUE})
  public void testNonPositiveLimitFallsBackToDefault(long limit) throws Exception {
    SCMHADBTransactionBufferImpl buffer = newBuffer(limit);
    buffer.updateLatestTrxInfo(TRX_INFO_T4);
    buffer.flush();

    final long defaultLimit = OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_PENDING_LIMIT_DEFAULT;
    for (int i = 0; i < defaultLimit - 1; i++) {
      put(buffer, "key" + i);
    }
    assertFalse(buffer.flushIfPendingLimitReached());
    put(buffer, "key" + (defaultLimit - 1));
    assertTrue(buffer.flushIfPendingLimitReached());
  }
  
  @Test
  public void testConcurrentCallersFlushExactlyOnce() throws Exception {
    SCMHADBTransactionBufferImpl buffer = newBuffer(1);
    buffer.updateLatestTrxInfo(TRX_INFO_T4);
    buffer.flush();
    clearInvocations(deletedBlockLog);
    put(buffer, "key");
    buffer.updateLatestTrxInfo(TRX_INFO_T5);

    final int threads = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch start = new CountDownLatch(1);
      AtomicInteger flushed = new AtomicInteger();
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        futures.add(pool.submit(() -> {
          start.await();
          if (buffer.flushIfPendingLimitReached()) {
            flushed.incrementAndGet();
          }
          return null;
        }));
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(10, TimeUnit.SECONDS);
      }
      assertEquals(1, flushed.get());
      verify(deletedBlockLog, times(1)).onFlush();
      assertDurable("key");
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void testFlushWaitsForBufferLock() throws Exception {
    SCMHADBTransactionBufferImpl buffer = newBuffer(1);
    buffer.updateLatestTrxInfo(TRX_INFO_T4);
    buffer.flush();
    put(buffer, "key");
    buffer.updateLatestTrxInfo(TRX_INFO_T5);

    AtomicInteger flushed = new AtomicInteger();
    Thread flusher = new Thread(() -> {
      try {
        if (buffer.flushIfPendingLimitReached()) {
          flushed.incrementAndGet();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    buffer.lock();
    try {
      flusher.start();
      waitFor(() -> flusher.getState() == Thread.State.WAITING, 10, 5_000);
      assertNotDurable("key");
      assertEquals(TRX_INFO_T4, transactionInfoTable.get(TRANSACTION_INFO_KEY));
    } finally {
      buffer.unlock();
    }
    flusher.join(10_000);
    assertEquals(1, flushed.get());
    assertDurable("key");
    assertEquals(TRX_INFO_T5, transactionInfoTable.get(TRANSACTION_INFO_KEY));
  }

  @Test
  public void testFailedFlushPropagatesAndIsRetried() throws Exception {
    SCMHADBTransactionBufferImpl buffer = newBuffer(1);
    buffer.updateLatestTrxInfo(TRX_INFO_T4);
    buffer.flush();
    put(buffer, "key");
    buffer.updateLatestTrxInfo(TRX_INFO_T5);

    doThrow(new IllegalStateException("injected")).when(deletedBlockLog).onFlush();
    assertThrows(IllegalStateException.class, buffer::flushIfPendingLimitReached);

    // Pending count was not reset by the failed flush, and the lock was released.
    doNothing().when(deletedBlockLog).onFlush();
    assertTrue(buffer.flushIfPendingLimitReached());
    assertDurable("key");
    assertEquals(TRX_INFO_T5, transactionInfoTable.get(TRANSACTION_INFO_KEY));
  }

  private SCMHADBTransactionBufferImpl newBuffer(OzoneConfiguration conf) throws Exception {
    StorageContainerManager scm = mock(StorageContainerManager.class);
    BlockManager blockManager = mock(BlockManager.class);
    when(scm.getConfiguration()).thenReturn(conf);
    when(scm.getScmMetadataStore()).thenReturn(metadataStore);
    when(scm.getSystemClock()).thenReturn(Clock.systemUTC());
    when(scm.getScmBlockManager()).thenReturn(blockManager);
    when(blockManager.getDeletedBlockLog()).thenReturn(deletedBlockLog);
    SCMHADBTransactionBufferImpl buffer = new SCMHADBTransactionBufferImpl(scm);
    buffers.add(buffer);
    return buffer;
  }

  private SCMHADBTransactionBufferImpl newBuffer(long limit) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setLong(OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_PENDING_LIMIT, limit);
    return newBuffer(conf);
  }

  private void put(SCMHADBTransactionBufferImpl buffer, String key) throws Exception {
    buffer.addToBuffer(serviceConfigTable, key, VALUE);
  }

  private void assertDurable(String key) throws Exception {
    assertEquals(VALUE, serviceConfigTable.get(key), key + " should be durable");
  }

  private void assertNotDurable(String key) throws Exception {
    assertNull(serviceConfigTable.get(key), key + " should still be buffered only");
  }
}
