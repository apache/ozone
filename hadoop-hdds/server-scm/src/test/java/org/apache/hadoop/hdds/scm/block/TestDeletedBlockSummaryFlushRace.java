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

package org.apache.hadoop.hdds.scm.block;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.File;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBufferImpl;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression test for SCM leader transfer flush can split a delete
 * transaction apply and persist a summary undercount.
 */
public class TestDeletedBlockSummaryFlushRace {

  private static final TransactionInfo TRX_INFO_T1 = TransactionInfo.valueOf(1, 1);
  private static final TransactionInfo TRX_INFO_T2 = TransactionInfo.valueOf(1, 2);
  private static final long TX_ID_1 = 1L;
  private static final long TX_ID_2 = 2L;
  private static final long CONTAINER_ID = 100L;

  @TempDir
  private File testDir;

  private final AtomicLong clockMillis = new AtomicLong(0);
  private SCMMetadataStore metadataStore;
  private ScmBlockDeletingServiceMetrics metrics;
  private BlockManager blockManager;

  @BeforeEach
  public void setUp() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    metadataStore = new SCMMetadataStoreImpl(conf);
    blockManager = mock(BlockManager.class);
    when(blockManager.getDeletedBlockLog()).thenReturn(mock(DeletedBlockLogImpl.class));
    metrics = ScmBlockDeletingServiceMetrics.create(blockManager);
    SCMDeletedBlockTransactionStatusManager.setDisableDataDistributionForTest(false);
  }

  @AfterEach
  public void tearDown() throws Exception {
    SCMDeletedBlockTransactionStatusManager.setDisableDataDistributionForTest(false);
    ScmBlockDeletingServiceMetrics.unRegister();
    if (metadataStore != null) {
      metadataStore.stop();
    }
  }

  @Test
  public void testNotifyLeaderChangedFlushCausesPermanentSummaryUndercount() throws Throwable {
    CountDownLatch afterFirstRowWrite = new CountDownLatch(1);
    CountDownLatch beforeSummaryWrite = new CountDownLatch(1);
    PausingBuffer buffer = new PausingBuffer(buildMockScm(), afterFirstRowWrite, beforeSummaryWrite);

    DeletedBlockLogStateManagerImpl stateManager = new DeletedBlockLogStateManagerImpl(
        metadataStore.getDeletedBlocksTXTable(),
        metadataStore.getStatefulServiceConfigTable(),
        mock(ContainerManager.class),
        buffer);
    SCMDeletedBlockTransactionStatusManager statusManager =
        new SCMDeletedBlockTransactionStatusManager(stateManager,
            metadataStore.getStatefulServiceConfigTable(),
            mock(ContainerManager.class), metrics, Long.MAX_VALUE);

    // Phase 1: commit Tx1 normally so the DB has a valid baseline summary.
    // After this flush: DB = {Tx1 row, summary S0 = {txCount=1, blockCount=5}}
    // In-memory: {txCount=1, blockCount=5}
    statusManager.addTransactions(toList(buildTx(TX_ID_1, 5)));
    buffer.updateLatestTrxInfo(TRX_INFO_T1);
    buffer.flush();
    assertEquals(1, statusManager.getSummary().getTotalTransactionCount(),
        "baseline: in-memory must reflect the one committed transaction");

    // Phase 2: arm the pause so the next addToBuffer call (the Tx2 row) blocks
    // before the summary addToBuffer call, opening the race window.
    buffer.armPause(afterFirstRowWrite, beforeSummaryWrite);

    // The apply thread simulates the Ratis applier applying a delete transaction.
    // Inside addTransactions:
    //   1. incrDeletedBlocksSummary increments in-memory to {txCount=2, blockCount=10}.
    //   2. addTransactionsToDB buffers the Tx2 row — PausingBuffer fires afterFirstRowWrite
    //      and blocks before the summary addToBuffer call.
    AtomicReference<Throwable> applyError = new AtomicReference<>();
    Thread applyThread = new Thread(() -> {
      try {
        statusManager.addTransactions(toList(buildTx(TX_ID_2, 5)));
        buffer.updateLatestTrxInfo(TRX_INFO_T2);
      } catch (Throwable t) {
        applyError.set(t);
      }
    });
    applyThread.start();

    assertTrue(afterFirstRowWrite.await(10, TimeUnit.SECONDS),
        "Timed out: Tx2 row was not buffered by the apply thread");
    AtomicReference<Throwable> flushError = new AtomicReference<>();
    Thread flushThread = new Thread(() -> {
      try {
        buffer.flush();
      } catch (Throwable t) {
        flushError.set(t);
      }
    });
    flushThread.start();

    // Give the background flush a moment to either commit (without fix) or block
    // on the write-lock (with fix), then release the apply thread to complete
    // the summary write so both threads can finish.
    Thread.sleep(200);
    beforeSummaryWrite.countDown();

    flushThread.join(15_000);
    if (flushError.get() != null) {
      throw flushError.get();
    }
    applyThread.join(15_000);
    if (applyError.get() != null) {
      throw applyError.get();
    }

    // After both threads complete:
    //   Without the fix: flush split the batch — Tx2 row is durable but DB still
    //     has S0 (the apply thread wrote S1 to the new batch, not yet flushed).
    //   With the fix: flush committed row + summary atomically; DB has S1.
    // Either way, Tx2 row must be durable.
    assertNotNull(metadataStore.getDeletedBlocksTXTable().get(TX_ID_2),
        "Tx2 row must be durable after the flush");

    // Simulate onBecomeLeader. With the bug, initDataDistributionData reloads
    // the stale S0 (split DB) and resets the in-memory counters from 2 to 1.
    // With the fix, it reads the atomically-committed S1 and keeps in-memory at 2.
    statusManager.onBecomeLeader();

    assertEquals(2, statusManager.getSummary().getTotalTransactionCount(),
        "After onBecomeLeader, in-memory summary must be 2 (both Tx1 and Tx2 applied). "
            + "BUG: initDataDistributionData reloaded the stale S0 (txCount=1) from the "
            + "split-flush DB, discarding the correctly-incremented in-memory value.");

    // Flush the new batch so S1 is now durable.
    buffer.flush();
    ByteString rawSummaryFinal =
        metadataStore.getStatefulServiceConfigTable()
            .get(DeletedBlockLogStateManagerImpl.SERVICE_DEFINITION.getServiceName());
    DeletedBlocksTransactionSummary dbSummaryFinal =
        DeletedBlocksTransactionSummary.parseFrom(rawSummaryFinal);
    assertEquals(2, dbSummaryFinal.getTotalTransactionCount(),
        "DB summary must be S1 (txCount=2) after the apply thread completed and flushed");
    assertEquals(2, statusManager.getSummary().getTotalTransactionCount(),
        "In-memory summary must be 2 after apply completes and DB is consistent. "
            + "BUG: the split flush + onBecomeLeader permanently baked S0 (txCount=1) "
            + "into the in-memory counters; they remain undercounted even after "
            + "the correct S1 is durable.");
  }

  /**
   * Verifies the complementary safe path: {@code flushIfNeeded} with an active
   * apply (applyingTransactions > 0) does NOT flush, so row and summary are
   * always committed together and {@code onBecomeLeader} reloads a consistent
   * summary. This is the behavior {@code SCMHATransactionBufferMonitorTask}
   * relies on; {@code notifyLeaderChanged} must adopt the same guard.
   */
  @Test
  public void testFlushIfNeededDoesNotSplitWritesDuringApply() throws Throwable {
    PausingBuffer buffer = new PausingBuffer(buildMockScm(),
        new CountDownLatch(1), new CountDownLatch(1));

    DeletedBlockLogStateManagerImpl stateManager = new DeletedBlockLogStateManagerImpl(
        metadataStore.getDeletedBlocksTXTable(),
        metadataStore.getStatefulServiceConfigTable(),
        mock(ContainerManager.class),
        buffer);
    SCMDeletedBlockTransactionStatusManager statusManager =
        new SCMDeletedBlockTransactionStatusManager(stateManager,
            metadataStore.getStatefulServiceConfigTable(),
            mock(ContainerManager.class), metrics, Long.MAX_VALUE);

    // Commit Tx1 as baseline.
    statusManager.addTransactions(toList(buildTx(TX_ID_1, 5)));
    buffer.updateLatestTrxInfo(TRX_INFO_T1);
    buffer.flush();

    // Simulate applyTransaction wrapping the addTransactions call.
    buffer.beginApplyingTransaction();
    statusManager.addTransactions(toList(buildTx(TX_ID_2, 5)));

    // flushIfNeeded skips flush because applyingTransactions > 0.
    buffer.flushIfNeeded(0);

    // Nothing new is durable yet — the batch still holds both the row and the
    // updated summary together, which is the safe state.
    assertNull(metadataStore.getDeletedBlocksTXTable().get(TX_ID_2),
        "flushIfNeeded must not flush while an apply is in progress");
    ByteString rawSummary =
        metadataStore.getStatefulServiceConfigTable()
            .get(DeletedBlockLogStateManagerImpl.SERVICE_DEFINITION.getServiceName());
    DeletedBlocksTransactionSummary dbSummary = DeletedBlocksTransactionSummary.parseFrom(rawSummary);
    assertEquals(1, dbSummary.getTotalTransactionCount(),
        "DB summary must still be S0 — flushIfNeeded deferred the write");

    buffer.updateLatestTrxInfo(TRX_INFO_T2);
    buffer.endApplyingTransaction();

    // Now the full batch (row + summary) is committed atomically.
    clockMillis.addAndGet(2000);
    buffer.flushIfNeeded(1000);

    assertNotNull(metadataStore.getDeletedBlocksTXTable().get(TX_ID_2),
        "Tx2 row must be durable after the guarded flush");
    ByteString rawSummaryFinal =
        metadataStore.getStatefulServiceConfigTable()
            .get(DeletedBlockLogStateManagerImpl.SERVICE_DEFINITION.getServiceName());
    DeletedBlocksTransactionSummary dbSummaryFinal =
        DeletedBlocksTransactionSummary.parseFrom(rawSummaryFinal);
    assertEquals(2, dbSummaryFinal.getTotalTransactionCount(),
        "DB summary must be S1 (txCount=2) after the guarded flush committed the full batch");

    // onBecomeLeader now reloads a consistent S1 from DB.
    statusManager.onBecomeLeader();
    assertEquals(2, statusManager.getSummary().getTotalTransactionCount(),
        "After onBecomeLeader with a consistent DB, in-memory must be S1 (txCount=2)");
  }

  // -------------------------------------------------------------------------

  private StorageContainerManager buildMockScm() {
    Clock clock = mock(Clock.class);
    when(clock.millis()).thenAnswer(inv -> clockMillis.get());
    when(blockManager.getDeletedBlockLog()).thenReturn(mock(DeletedBlockLogImpl.class));
    StorageContainerManager scm = mock(StorageContainerManager.class);
    when(scm.getScmMetadataStore()).thenReturn(metadataStore);
    when(scm.getSystemClock()).thenReturn(clock);
    when(scm.getScmBlockManager()).thenReturn(blockManager);
    return scm;
  }

  private static DeletedBlocksTransaction buildTx(long txId, int blockCount) {
    DeletedBlocksTransaction.Builder b = DeletedBlocksTransaction.newBuilder()
        .setTxID(txId)
        .setContainerID(CONTAINER_ID)
        .setCount(0)
        .setTotalBlockSize((long) blockCount * 10);
    for (int i = 0; i < blockCount; i++) {
      b.addLocalID((long) i);
    }
    return b.build();
  }

  private static ArrayList<DeletedBlocksTransaction> toList(DeletedBlocksTransaction... txs) {
    return new ArrayList<>(Arrays.asList(txs));
  }

  /**
   * A {@link SCMHADBTransactionBufferImpl} subclass that, once armed, pauses
   * after the first {@code addToBuffer} call (the transaction-row write) and
   * before the second (the summary write). This replicates the race window in
   * {@code DeletedBlockLogStateManagerImpl.addTransactionsToDB} where
   * {@code notifyLeaderChanged}'s unguarded {@code flush()} can interpose.
   */
  static class PausingBuffer extends SCMHADBTransactionBufferImpl {

    private volatile CountDownLatch afterFirstAdd;
    private volatile CountDownLatch beforeSecondAdd;
    private volatile boolean armed = false;

    PausingBuffer(StorageContainerManager scm,
        CountDownLatch afterFirstAdd, CountDownLatch beforeSecondAdd)
        throws RocksDatabaseException, CodecException {
      super(scm);
      this.afterFirstAdd = afterFirstAdd;
      this.beforeSecondAdd = beforeSecondAdd;
    }

    void armPause(CountDownLatch afterFirst, CountDownLatch beforeSecond) {
      this.afterFirstAdd = afterFirst;
      this.beforeSecondAdd = beforeSecond;
      this.armed = true;
    }

    @Override
    public <KEY, VALUE> void addToBuffer(Table<KEY, VALUE> table, KEY key, VALUE value)
        throws RocksDatabaseException, CodecException {
      super.addToBuffer(table, key, value);
      if (armed) {
        armed = false;
        afterFirstAdd.countDown();
        try {
          beforeSecondAdd.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
