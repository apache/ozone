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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
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

  /**
   * Regression test for the reviewer-flagged gap: {@code addTransactions()} evaluates
   * {@code getSummary()} before entering {@code runWithBufferLock()}, so a concurrent
   * {@code onBecomeLeader()} could reload the old DB summary between the counter update and
   * the snapshot, and that stale snapshot would then be the one persisted. Verifies the
   * summary handed to {@code addTransactionsToDB} (i.e. what actually gets persisted) always
   * reflects the just-applied increment, even when {@code onBecomeLeader} races to interleave
   * exactly in that window.
   */
  @Test
  public void testOnBecomeLeaderCannotInterleaveDuringAddTransactionsSnapshot() throws Throwable {
    DeletedBlockLogStateManager mockStateManager = mock(DeletedBlockLogStateManager.class);
    AtomicReference<DeletedBlocksTransactionSummary> persistedSummary = new AtomicReference<>();
    doAnswer(inv -> {
      persistedSummary.set(inv.getArgument(1));
      return null;
    }).when(mockStateManager).addTransactionsToDB(any(ArrayList.class), any());

    // Seed the DB with a baseline summary (as if Tx1 was already durably committed) without
    // going through the mock, which does not itself write anything.
    DeletedBlocksTransactionSummary baseline = DeletedBlocksTransactionSummary.newBuilder()
        .setTotalTransactionCount(1).setTotalBlockCount(5).setTotalBlockSize(50)
        .setTotalBlockReplicatedSize(0).build();
    metadataStore.getStatefulServiceConfigTable().put(
        DeletedBlockLogStateManagerImpl.SERVICE_DEFINITION.getServiceName(), baseline.toByteString());

    CountDownLatch afterUpdate = new CountDownLatch(1);
    CountDownLatch beforeSnapshot = new CountDownLatch(1);
    PausingStatusManager statusManager = new PausingStatusManager(mockStateManager,
        metadataStore.getStatefulServiceConfigTable(), mock(ContainerManager.class), metrics, Long.MAX_VALUE);
    assertEquals(1, statusManager.getSummary().getTotalTransactionCount(),
        "baseline: in-memory must be initialized from the seeded DB summary");

    statusManager.armPause(afterUpdate, beforeSnapshot);

    AtomicReference<Throwable> applyError = new AtomicReference<>();
    Thread applyThread = new Thread(() -> {
      try {
        statusManager.addTransactions(toList(buildTx(TX_ID_2, 5)));
      } catch (Throwable t) {
        applyError.set(t);
      }
    });
    applyThread.start();

    assertTrue(afterUpdate.await(10, TimeUnit.SECONDS),
        "Timed out waiting for the apply thread to update the in-memory summary");

    // Simulate a concurrent SCM leader change trying to reload the (still stale) DB summary
    // right in the update-to-snapshot window.
    Thread leaderThread = new Thread(statusManager::onBecomeLeader);
    leaderThread.start();

    leaderThread.join(300);
    assertTrue(leaderThread.isAlive(),
        "onBecomeLeader must block until the apply thread finishes update+snapshot as one unit; "
            + "if it proceeded here, the update-to-snapshot race is unguarded.");

    beforeSnapshot.countDown();

    applyThread.join(15_000);
    if (applyError.get() != null) {
      throw applyError.get();
    }
    leaderThread.join(15_000);

    assertNotNull(persistedSummary.get(), "addTransactionsToDB must have been called");
    assertEquals(2, persistedSummary.get().getTotalTransactionCount(),
        "The summary handed to addTransactionsToDB (what gets durably persisted) must reflect "
            + "Tx2's increment. BUG: a concurrent onBecomeLeader reset landed between the "
            + "counter update and getSummary(), so a stale summary would have been persisted.");
  }

  /**
   * Analogous regression test for the remove path, per the reviewer's explicit request to
   * also cover {@code removeTransactions()}.
   */
  @Test
  public void testOnBecomeLeaderCannotInterleaveDuringRemoveTransactionsSnapshot() throws Throwable {
    DeletedBlockLogStateManager mockStateManager = mock(DeletedBlockLogStateManager.class);
    AtomicReference<DeletedBlocksTransactionSummary> persistedSummary = new AtomicReference<>();
    doAnswer(inv -> {
      persistedSummary.set(inv.getArgument(1));
      return null;
    }).when(mockStateManager).removeTransactionsFromDB(any(ArrayList.class), any());

    // Seed the DB with a baseline summary as if Tx1 and Tx2 were both already durable.
    DeletedBlocksTransactionSummary baseline = DeletedBlocksTransactionSummary.newBuilder()
        .setTotalTransactionCount(2).setTotalBlockCount(10).setTotalBlockSize(100)
        .setTotalBlockReplicatedSize(0).build();
    metadataStore.getStatefulServiceConfigTable().put(
        DeletedBlockLogStateManagerImpl.SERVICE_DEFINITION.getServiceName(), baseline.toByteString());

    CountDownLatch afterUpdate = new CountDownLatch(1);
    CountDownLatch beforeSnapshot = new CountDownLatch(1);
    PausingStatusManager statusManager = new PausingStatusManager(mockStateManager,
        metadataStore.getStatefulServiceConfigTable(), mock(ContainerManager.class), metrics, Long.MAX_VALUE);
    assertEquals(2, statusManager.getSummary().getTotalTransactionCount(),
        "baseline: in-memory must be initialized from the seeded DB summary");
    // Tx2 must be tracked in txSizeMap for removeTransactions to decrement the summary for it.
    statusManager.getTxSizeMap().put(TX_ID_2,
        new SCMDeletedBlockTransactionStatusManager.TxBlockInfo(TX_ID_2, CONTAINER_ID, 5, 50, 0));

    statusManager.armPause(afterUpdate, beforeSnapshot);

    AtomicReference<Throwable> applyError = new AtomicReference<>();
    Thread applyThread = new Thread(() -> {
      try {
        statusManager.removeTransactions(toLongList(TX_ID_2));
      } catch (Throwable t) {
        applyError.set(t);
      }
    });
    applyThread.start();

    assertTrue(afterUpdate.await(10, TimeUnit.SECONDS),
        "Timed out waiting for the apply thread to update the in-memory summary");

    Thread leaderThread = new Thread(statusManager::onBecomeLeader);
    leaderThread.start();

    leaderThread.join(300);
    assertTrue(leaderThread.isAlive(),
        "onBecomeLeader must block until the apply thread finishes update+snapshot as one unit; "
            + "if it proceeded here, the update-to-snapshot race is unguarded.");

    beforeSnapshot.countDown();

    applyThread.join(15_000);
    if (applyError.get() != null) {
      throw applyError.get();
    }
    leaderThread.join(15_000);

    assertNotNull(persistedSummary.get(), "removeTransactionsFromDB must have been called");
    assertEquals(1, persistedSummary.get().getTotalTransactionCount(),
        "The summary handed to removeTransactionsFromDB (what gets durably persisted) must "
            + "reflect Tx2's removal. BUG: a concurrent onBecomeLeader reset landed between the "
            + "counter update and getSummary(), so a stale summary would have been persisted.");
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

  private static ArrayList<Long> toLongList(Long... ids) {
    return new ArrayList<>(Arrays.asList(ids));
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

  /**
   * A {@link SCMDeletedBlockTransactionStatusManager} subclass that, once armed, pauses
   * inside the {@code summaryLock}-guarded window: after the summary counters are updated
   * and before they are snapshotted via {@code getSummary()}. This lets a test deterministically
   * race a concurrent {@code onBecomeLeader()} against exactly that window.
   */
  static class PausingStatusManager extends SCMDeletedBlockTransactionStatusManager {

    private volatile CountDownLatch afterUpdate;
    private volatile CountDownLatch beforeSnapshot;
    private volatile boolean armed = false;

    PausingStatusManager(DeletedBlockLogStateManager deletedBlockLogStateManager,
        Table<String, ByteString> statefulServiceConfigTable, ContainerManager containerManager,
        ScmBlockDeletingServiceMetrics metrics, long scmCommandTimeoutMs) throws IOException {
      super(deletedBlockLogStateManager, statefulServiceConfigTable, containerManager, metrics,
          scmCommandTimeoutMs);
    }

    void armPause(CountDownLatch afterUpdateLatch, CountDownLatch beforeSnapshotLatch) {
      this.afterUpdate = afterUpdateLatch;
      this.beforeSnapshot = beforeSnapshotLatch;
      this.armed = true;
    }

    @Override
    protected void onSummaryUpdatedForTest() {
      if (armed) {
        armed = false;
        afterUpdate.countDown();
        try {
          beforeSnapshot.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
