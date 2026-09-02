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

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogStateManagerImpl;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
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

/**
 * Tests for {@link SCMHATransactionBufferMonitorTask} and the flush race
 * conditions it can trigger against {@link SCMHADBTransactionBufferImpl}.
 */
public class TestSCMHATransactionBufferMonitorTask {

  private static final long FLUSH_INTERVAL_MS = 1000L;
  private static final TransactionInfo TRX_INFO_T4 =
      TransactionInfo.valueOf(1, 4);
  private static final TransactionInfo TRX_INFO_T5 =
      TransactionInfo.valueOf(1, 5);

  @TempDir
  private File testDir;

  private final AtomicLong clockMillis = new AtomicLong(0);
  private SCMMetadataStore metadataStore;
  private SCMHADBTransactionBufferImpl transactionBuffer;
  private Table<String, ByteString> statefulServiceConfigTable;
  private Table<String, TransactionInfo> transactionInfoTable;
  private DeletedBlockLogImpl deletedBlockLog;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    metadataStore = new SCMMetadataStoreImpl(conf);
    statefulServiceConfigTable = metadataStore.getStatefulServiceConfigTable();
    transactionInfoTable = metadataStore.getTransactionInfoTable();

    StorageContainerManager scm = mock(StorageContainerManager.class);
    BlockManager blockManager = mock(BlockManager.class);
    deletedBlockLog = mock(DeletedBlockLogImpl.class);
    Clock clock = mock(Clock.class);
    when(clock.millis()).thenAnswer(invocation -> clockMillis.get());
    when(scm.getScmMetadataStore()).thenReturn(metadataStore);
    when(scm.getSystemClock()).thenReturn(clock);
    when(scm.getScmBlockManager()).thenReturn(blockManager);
    when(blockManager.getDeletedBlockLog()).thenReturn(deletedBlockLog);

    transactionBuffer = new SCMHADBTransactionBufferImpl(scm);
    clockMillis.set(FLUSH_INTERVAL_MS + 1);
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (transactionBuffer != null) {
      transactionBuffer.close();
    }
    if (metadataStore != null) {
      metadataStore.stop();
    }
  }

  private void advanceClockPastFlushInterval() {
    clockMillis.addAndGet(FLUSH_INTERVAL_MS + 1);
  }

  /**
   * Demonstrates the partial flush race when shouldFlush and flush are called
   * separately: buffered data can be committed with a stale transaction index.
   */
  @Test
  public void testPartialFlushWithSeparateShouldFlushAndFlush() throws Exception {
    transactionBuffer.updateLatestTrxInfo(TRX_INFO_T4);
    transactionBuffer.flush();

    transactionBuffer.addToBuffer(statefulServiceConfigTable, "key",
        ByteString.copyFromUtf8("value"));

    advanceClockPastFlushInterval();
    if (transactionBuffer.shouldFlush(FLUSH_INTERVAL_MS)) {
      transactionBuffer.flush();
    }

    assertEquals(TRX_INFO_T4, transactionInfoTable.get(TRANSACTION_INFO_KEY));
    assertEquals(ByteString.copyFromUtf8("value"),
        statefulServiceConfigTable.get("key"));
  }

  @Test
  public void testFlushIfNeededDoesNotFlushDuringTransactionApply()
      throws Exception {
    transactionBuffer.updateLatestTrxInfo(TRX_INFO_T4);
    transactionBuffer.flush();

    transactionBuffer.beginApplyingTransaction();
    try {
      transactionBuffer.addToBuffer(statefulServiceConfigTable, "key",
          ByteString.copyFromUtf8("value"));
      transactionBuffer.flushIfNeeded(FLUSH_INTERVAL_MS);
      assertNull(statefulServiceConfigTable.get("key"));
    } finally {
      transactionBuffer.endApplyingTransaction();
    }

    transactionBuffer.updateLatestTrxInfo(TRX_INFO_T5);
    advanceClockPastFlushInterval();
    transactionBuffer.flushIfNeeded(FLUSH_INTERVAL_MS);

    assertEquals(TRX_INFO_T5, transactionInfoTable.get(TRANSACTION_INFO_KEY));
    assertEquals(ByteString.copyFromUtf8("value"),
        statefulServiceConfigTable.get("key"));
  }

  /**
   * Demonstrates that calling flush() directly inside an applyTransaction
   * window (the old behaviour of StatefulServiceStateManagerImpl) persists
   * the batch with the stale transaction index that was current before the
   * apply updated it.
   */
  @Test
  public void testDirectFlushDuringApplyWritesStaleTransactionInfo()
      throws Exception {
    transactionBuffer.updateLatestTrxInfo(TRX_INFO_T4);
    transactionBuffer.flush();

    transactionBuffer.beginApplyingTransaction();
    try {
      transactionBuffer.addToBuffer(statefulServiceConfigTable, "key",
          ByteString.copyFromUtf8("value"));
      // Old saveConfiguration behaviour: flush() before updateLatestTrxInfo.
      transactionBuffer.flush();
      // Data is on disk, but the transaction index is still T4 — stale.
      assertEquals(TRX_INFO_T4, transactionInfoTable.get(TRANSACTION_INFO_KEY));
      assertEquals(ByteString.copyFromUtf8("value"),
          statefulServiceConfigTable.get("key"));
    } finally {
      transactionBuffer.updateLatestTrxInfo(TRX_INFO_T5);
      transactionBuffer.endApplyingTransaction();
    }
  }

  /**
   * Verifies that using flushIfNeeded(0) instead of flush() inside an apply
   * window defers the write until after updateLatestTrxInfo(), keeping the
   * on-disk transaction index consistent with the buffered data.
   */
  @Test
  public void testFlushIfNeededZeroWaitDefersDuringApply() throws Exception {
    transactionBuffer.updateLatestTrxInfo(TRX_INFO_T4);
    transactionBuffer.flush();

    transactionBuffer.beginApplyingTransaction();
    try {
      transactionBuffer.addToBuffer(statefulServiceConfigTable, "key",
          ByteString.copyFromUtf8("value"));
      // New saveConfiguration behaviour: skipped because apply is in progress.
      transactionBuffer.flushIfNeeded(0);
      assertNull(statefulServiceConfigTable.get("key"),
          "flushIfNeeded must not flush while a transaction is being applied");
    } finally {
      transactionBuffer.updateLatestTrxInfo(TRX_INFO_T5);
      transactionBuffer.endApplyingTransaction();
    }

    // After the apply window closes, the monitor flushes both data and the
    // correct transaction index atomically.
    advanceClockPastFlushInterval();
    transactionBuffer.flushIfNeeded(FLUSH_INTERVAL_MS);

    assertEquals(TRX_INFO_T5, transactionInfoTable.get(TRANSACTION_INFO_KEY));
    assertEquals(ByteString.copyFromUtf8("value"),
        statefulServiceConfigTable.get("key"));
  }

  @Test
  public void testMonitorTaskDoesNotPartialFlushDuringTransactionApply()
      throws Exception {
    transactionBuffer.updateLatestTrxInfo(TRX_INFO_T4);
    transactionBuffer.flush();

    CountDownLatch addedToBuffer = new CountDownLatch(1);
    CountDownLatch allowFinishApply = new CountDownLatch(1);
    CountDownLatch applyFinished = new CountDownLatch(1);
    SCMHATransactionBufferMonitorTask monitorTask =
        new SCMHATransactionBufferMonitorTask(transactionBuffer, FLUSH_INTERVAL_MS);

    Thread applyThread = new Thread(() -> {
      transactionBuffer.beginApplyingTransaction();
      try {
        try {
          transactionBuffer.addToBuffer(statefulServiceConfigTable, "key",
              ByteString.copyFromUtf8("value"));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        addedToBuffer.countDown();
        try {
          allowFinishApply.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        transactionBuffer.updateLatestTrxInfo(TRX_INFO_T5);
      } finally {
        transactionBuffer.endApplyingTransaction();
        applyFinished.countDown();
      }
    });

    Thread monitorThread = new Thread(() -> {
      try {
        while (!applyFinished.await(10, TimeUnit.MILLISECONDS)) {
          monitorTask.run();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    applyThread.start();
    monitorThread.start();

    assertTrue(addedToBuffer.await(10, TimeUnit.SECONDS),
        "Timed out waiting for applyThread to add data to buffer");
    monitorTask.run();
    assertNull(statefulServiceConfigTable.get("key"),
        "Monitor must not flush before transaction info is updated");

    allowFinishApply.countDown();
    applyThread.join(10_000);
    monitorThread.join(10_000);

    advanceClockPastFlushInterval();
    transactionBuffer.flushIfNeeded(FLUSH_INTERVAL_MS);
    assertEquals(TRX_INFO_T5, transactionInfoTable.get(TRANSACTION_INFO_KEY));
    assertEquals(ByteString.copyFromUtf8("value"),
        statefulServiceConfigTable.get("key"));
  }

  /**
   * Verifies that {@link SCMHADBTransactionBuffer#lock()} establishes mutual exclusion with
   * {@link SCMHADBTransactionBufferImpl#flush()}'s write lock (HDDS-16145).
   */
  @Test
  public void testLockBlocksConcurrentFlush() throws Exception {
    transactionBuffer.updateLatestTrxInfo(TRX_INFO_T4);
    transactionBuffer.flush();

    transactionBuffer.lock();
    Thread flusher = new Thread(() -> {
      transactionBuffer.updateLatestTrxInfo(TRX_INFO_T5);
      try {
        transactionBuffer.flush();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    flusher.start();
    waitFor(() -> flusher.getState() == Thread.State.WAITING, 10, 5_000);

    // flush() must not have committed while the buffer lock is held.
    assertEquals(TRX_INFO_T4, transactionInfoTable.get(TRANSACTION_INFO_KEY));

    transactionBuffer.unlock();
    flusher.join(10_000);
    assertEquals(TRX_INFO_T5, transactionInfoTable.get(TRANSACTION_INFO_KEY));
  }

  /**
   * Verifies that {@link DeletedBlockLogStateManagerImpl#removeTransactionsFromDB} holds the buffer
   * lock across its whole mark-remove-summary sequence, so a concurrent flush() cannot land right
   * after txId is marked hidden but before it is actually removed from the buffer (HDDS-16145).
   * Without the fix, that gap has no lock protecting it: flush() lands there, resets
   * {@code deletingTxIDs}, and re-exposes the still-present row to the deletion scanner.
   */
  @Test
  public void testRemoveTransactionsFromDBBlocksConcurrentFlushUntilDurable() throws Exception {
    long txId = 42L;
    DeletedBlocksTransaction tx = DeletedBlocksTransaction.newBuilder()
        .setTxID(txId)
        .setContainerID(1L)
        .setCount(0)
        .addLocalID(1L)
        .build();

    Table<Long, DeletedBlocksTransaction> deletedTable = metadataStore.getDeletedBlocksTXTable();
    DeletedBlockLogStateManagerImpl stateManager = new DeletedBlockLogStateManagerImpl(
        deletedTable, statefulServiceConfigTable, mock(ContainerManager.class), transactionBuffer);
    doAnswer(invocation -> {
      stateManager.onFlush();
      return null;
    }).when(deletedBlockLog).onFlush();

    // Baseline: the transaction is durably present and deletingTxIDs is empty.
    stateManager.addTransactionsToDB(new ArrayList<>(Collections.singletonList(tx)), null);
    transactionBuffer.updateLatestTrxInfo(TRX_INFO_T4);
    transactionBuffer.flush();
    assertNotNull(deletedTable.get(txId));

    // Replace deletingTxIDs with a spy so the remover can be paused right after it marks txId as
    // hidden but before it removes the row from the buffer - the exact gap the fix's buffer lock
    // must cover.
    Field deletingTxIDsField = DeletedBlockLogStateManagerImpl.class.getDeclaredField("deletingTxIDs");
    deletingTxIDsField.setAccessible(true);
    Set<Long> spyDeletingTxIDs = spy((Set<Long>) deletingTxIDsField.get(stateManager));
    deletingTxIDsField.set(stateManager, spyDeletingTxIDs);

    CountDownLatch marked = new CountDownLatch(1);
    CountDownLatch releaseRemoval = new CountDownLatch(1);
    doAnswer(invocation -> {
      boolean result = (Boolean) invocation.callRealMethod();
      marked.countDown();
      releaseRemoval.await(10, TimeUnit.SECONDS);
      return result;
    }).when(spyDeletingTxIDs).addAll(any());

    ArrayList<Long> txIDs = new ArrayList<>(Collections.singletonList(txId));
    Thread remover = new Thread(() -> {
      try {
        stateManager.removeTransactionsFromDB(txIDs, null);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
    remover.start();
    assertTrue(marked.await(10, TimeUnit.SECONDS),
        "Timed out waiting for remover to mark txId as hidden");

    Thread flusher = new Thread(() -> {
      transactionBuffer.updateLatestTrxInfo(TRX_INFO_T5);
      try {
        transactionBuffer.flush();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    flusher.start();
    waitFor(() -> flusher.getState() == Thread.State.WAITING, 10, 5_000);

    // The remover has marked txId as hidden but not yet removed it from the buffer: flush must
    // still be blocked by the buffer lock, so the row remains durably present and hidden.
    assertEquals(TRX_INFO_T4, transactionInfoTable.get(TRANSACTION_INFO_KEY));
    assertNotNull(deletedTable.get(txId));

    releaseRemoval.countDown();
    remover.join(10_000);
    flusher.join(10_000);

    // Only once removeTransactionsFromDB releases the lock can flush proceed: it durably removes
    // the row and resets deletingTxIDs together, so the row is never re-exposed to the scanner.
    assertNull(deletedTable.get(txId));
    assertEquals(TRX_INFO_T5, transactionInfoTable.get(TRANSACTION_INFO_KEY));
    try (Table.KeyValueIterator<Long, DeletedBlocksTransaction> iterator = stateManager.getReadOnlyIterator()) {
      assertFalse(iterator.hasNext());
    }
  }
}
