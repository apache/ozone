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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that the transaction index persisted under TRANSACTION_INFO_KEY only ever moves forward.
 * <p>
 * Two writers reach that key: the batch commit inside the double buffer, which publishes a batch
 * of transactions together with the index describing them, and {@code persistIfNewer}, used when
 * the state machine takes a snapshot from an index the buffer has not yet caught up to. If the
 * second can land on top of a commit it did not observe, the DB is left holding transactions its
 * own watermark disclaims and a restart replays them. See HDDS-16092.
 * <p>
 * These tests keep the flush daemon stopped so the test thread is the only flusher, matching
 * production, where {@code flushCurrentBuffer} is reached from the daemon alone. Two threads
 * committing concurrently would write indexes out of order for reasons that have nothing to do
 * with the ordering under test.
 */
class TestOzoneManagerDoubleBufferTransactionInfo {

  private OzoneManagerDoubleBuffer doubleBuffer;
  private OmMetadataManagerImpl omMetadataManager;
  private OMClientResponse keyCreateResponse;

  @TempDir
  private File tempDir;

  @BeforeEach
  public void setup() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, tempDir.getAbsolutePath());

    OzoneManager ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.getMetrics()).thenReturn(OMMetrics.create(conf));
    AuditLogger auditLogger = mock(AuditLogger.class);
    doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);

    omMetadataManager = new OmMetadataManagerImpl(conf, ozoneManager);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);

    // Not started: the test thread drives every flush, so the ordering under test is the only
    // concurrency in play.
    doubleBuffer = OzoneManagerDoubleBuffer.newBuilder()
        .setOmMetadataManager(omMetadataManager)
        .setMaxUnFlushedTransactionCount(1000)
        .build();

    OMResponse omResponse = mock(OMResponse.class);
    when(omResponse.getTraceID()).thenReturn("traceId");
    when(omResponse.getCmdType()).thenReturn(OzoneManagerProtocolProtos.Type.CreateKey);
    keyCreateResponse = mock(OMKeyCreateResponse.class);
    when(keyCreateResponse.getOMResponse()).thenReturn(omResponse);
    doNothing().when(keyCreateResponse).checkAndUpdateDB(any(), any());
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (doubleBuffer != null) {
      doubleBuffer.stop();
    }
    if (omMetadataManager != null) {
      omMetadataManager.stop();
    }
  }

  private Table<String, TransactionInfo> transactionInfoTable() {
    return omMetadataManager.getTransactionInfoTable();
  }

  private TermIndex storedTermIndex() throws IOException {
    final TransactionInfo stored = transactionInfoTable().get(OzoneConsts.TRANSACTION_INFO_KEY);
    return stored == null ? null : stored.getTermIndex();
  }

  private void commit(long index) throws IOException {
    doubleBuffer.add(keyCreateResponse, TransactionInfo.getTermIndex(index));
    doubleBuffer.flushCurrentBuffer();
  }

  /**
   * Drives the exact interleaving the ordering exists to exclude: a snapshot is held between
   * reading the stored index and writing its own, while a real batch commit is attempted. The
   * commit has to be excluded from that window, otherwise the snapshot's older index lands on
   * top of it.
   * <p>
   * This is the only test that covers the locking half of the fix. Removing the lock while
   * leaving the comparison in persistIfNewer fails here and nowhere else; the window it needs
   * is far too narrow to hit by chance.
   */
  @Test
  public void testPersistIfNewerIsOrderedAgainstBatchCommit() throws Exception {
    transactionInfoTable().put(OzoneConsts.TRANSACTION_INFO_KEY,
        TransactionInfo.valueOf(TransactionInfo.getTermIndex(50)));

    final CountDownLatch snapshotRead = new CountDownLatch(1);
    final CountDownLatch commitDone = new CountDownLatch(1);
    final ExecutorService committer = Executors.newSingleThreadExecutor();
    try {
      doubleBuffer.setAfterTransactionInfoRead(() -> {
        snapshotRead.countDown();
        try {
          // Long enough that an unordered commit would have finished inside the window. When it
          // is ordered this simply times out and the snapshot carries on.
          commitDone.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      final Future<?> commit = committer.submit(() -> {
        snapshotRead.await();
        commit(105);
        commitDone.countDown();
        return null;
      });

      doubleBuffer.persistIfNewer(TransactionInfo.valueOf(TransactionInfo.getTermIndex(100)));
      commit.get(30, TimeUnit.SECONDS);

      assertEquals(TransactionInfo.getTermIndex(105), storedTermIndex(),
          "a batch commit must not be overwritten by a snapshot that read before it");
    } finally {
      doubleBuffer.setAfterTransactionInfoRead(() -> { });
      committer.shutdownNow();
    }
  }

  /**
   * The same invariant without a staged interleaving: with commits and snapshots running
   * concurrently, a reader must never observe the stored index go backwards.
   * <p>
   * This covers the comparison in persistIfNewer, which it fails within a handful of rounds when
   * removed. It does not cover the lock -- the read-to-write window is too narrow to lose the
   * race by chance -- so it complements the staged test above rather than repeating it.
   */
  @Test
  public void testPersistedTransactionInfoNeverMovesBackwards() throws Exception {
    final int rounds = 300;
    final AtomicBoolean running = new AtomicBoolean(true);
    final AtomicReference<String> regression = new AtomicReference<>();
    final ExecutorService threads = Executors.newFixedThreadPool(2);
    try {
      final Future<?> snapshots = threads.submit(() -> {
        while (running.get()) {
          final TermIndex before = storedTermIndex();
          if (before != null) {
            doubleBuffer.persistIfNewer(TransactionInfo.valueOf(before));
          }
        }
        return null;
      });

      final Future<?> reader = threads.submit(() -> {
        long highest = -1;
        while (running.get()) {
          final TermIndex seen = storedTermIndex();
          if (seen != null) {
            if (seen.getIndex() < highest) {
              regression.compareAndSet(null,
                  "stored index went backwards: " + highest + " -> " + seen.getIndex());
              return null;
            }
            highest = seen.getIndex();
          }
        }
        return null;
      });

      for (int i = 1; i <= rounds; i++) {
        commit(i);
      }
      running.set(false);
      snapshots.get(30, TimeUnit.SECONDS);
      reader.get(30, TimeUnit.SECONDS);

      assertNull(regression.get(), regression.get());
      assertEquals(TransactionInfo.getTermIndex(rounds), storedTermIndex());
    } finally {
      running.set(false);
      threads.shutdownNow();
    }
  }
}
