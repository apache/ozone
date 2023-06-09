/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.ha;

import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.ratis.util.Preconditions.assertInstanceOf;

/**
 * This is a transaction buffer that buffers SCM DB operations for Pipeline and
 * Container. When flush this buffer to DB, a transaction info will also be
 * written into DB to indicate the term and transaction index for the latest
 * operation in DB.
 */
public class SCMHADBTransactionBufferImpl implements SCMHADBTransactionBuffer {
  private final StorageContainerManager scm;
  private final SCMMetadataStore metadataStore;
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

  @GuardedBy("rwLock")
  private boolean closed;
  @GuardedBy("rwLock")
  @Nullable
  private BatchOperation currentBatchOperation;
  @GuardedBy("rwLock")
  private boolean pendingFlush;

  private TransactionInfo latestTrxInfo;
  private final AtomicReference<SnapshotInfo> latestSnapshot
      = new AtomicReference<>();
  private long lastSnapshotTimeMs = 0;

  public SCMHADBTransactionBufferImpl(StorageContainerManager scm)
      throws IOException {
    this.scm = scm;
    metadataStore = scm.getScmMetadataStore();
    init();
  }

  @Nonnull
  private BatchOperation getCurrentBatchOperation() {
    return Objects.requireNonNull(currentBatchOperation, "currentBatch");
  }

  @Override
  public <KEY, VALUE> void addToBuffer(
      Table<KEY, VALUE> table, KEY key, VALUE value) throws IOException {
    rwLock.readLock().lock();
    try {
      assertNotClosed();
      table.putWithBatch(getCurrentBatchOperation(), key, value);
      pendingFlush = true;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public <KEY, VALUE> void removeFromBuffer(Table<KEY, VALUE> table, KEY key)
      throws IOException {
    rwLock.readLock().lock();
    try {
      assertNotClosed();
      table.deleteWithBatch(getCurrentBatchOperation(), key);
      pendingFlush = true;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public void updateLatestTrxInfo(TransactionInfo info) {
    if (info.compareTo(this.latestTrxInfo) <= 0) {
      throw new IllegalArgumentException(
          "Updating DB buffer transaction info by an older transaction info, "
          + "current: " + this.latestTrxInfo + ", updating to: " + info);
    }
    this.latestTrxInfo = info;
  }

  @Override
  public TransactionInfo getLatestTrxInfo() {
    return this.latestTrxInfo;
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    return latestSnapshot.get();
  }

  @Override
  public void setLatestSnapshot(SnapshotInfo latestSnapshot) {
    this.latestSnapshot.set(latestSnapshot);
  }

  @Override
  public AtomicReference<SnapshotInfo> getLatestSnapshotRef() {
    return latestSnapshot;
  }

  @Override
  public void flush() throws IOException {
    rwLock.writeLock().lock();
    try {
      assertNotClosed();

      final BatchOperation batch = getCurrentBatchOperation();
      putTransactionInfo(batch);
      commitAndClose(batch);

      currentBatchOperation = metadataStore.getStore().initBatchOperation();
      setLatestSnapshot();

      assertInstanceOf(scm.getScmBlockManager().getDeletedBlockLog(),
          DeletedBlockLogImpl.class).onFlush();
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @Override
  public void init() throws IOException {
    rwLock.writeLock().lock();
    try {
      commitAndClose(currentBatchOperation);

      // initialize a batch operation during construction time
      currentBatchOperation = metadataStore.getStore().initBatchOperation();
      pendingFlush = false;

      latestTrxInfo = metadataStore.getTransactionInfoTable()
          .get(TRANSACTION_INFO_KEY);
      if (latestTrxInfo == null) {
        // transaction table is empty
        latestTrxInfo = TransactionInfo.builder()
            .setTransactionIndex(-1)
            .setCurrentTerm(0)
            .build();
      }
      setLatestSnapshot();
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @Override
  public boolean shouldFlush(long snapshotWaitTime) {
    rwLock.readLock().lock();
    try {
      return pendingFlush &&
          scm.getSystemClock().millis() - lastSnapshotTimeMs > snapshotWaitTime;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public String toString() {
    return latestTrxInfo.toString();
  }

  @Override
  public void close() throws IOException {
    rwLock.writeLock().lock();
    try {
      if (!closed) {
        closed = true;

        final BatchOperation batch = currentBatchOperation;
        currentBatchOperation = null;
        commitAndClose(batch);
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  private void putTransactionInfo(BatchOperation batch) throws IOException {
    Table<String, TransactionInfo> transactionInfoTable
        = metadataStore.getTransactionInfoTable();
    transactionInfoTable.putWithBatch(batch,
        TRANSACTION_INFO_KEY, latestTrxInfo);
    pendingFlush = true;
  }

  private void setLatestSnapshot() {
    latestSnapshot.set(latestTrxInfo.toSnapshotInfo());
    lastSnapshotTimeMs = scm.getSystemClock().millis();
  }

  private void assertNotClosed() {
    Preconditions.assertTrue(!closed, "already closed");
  }

  private void commitAndClose(@Nullable BatchOperation batch)
      throws IOException {
    if (batch != null) {
      try {
        if (pendingFlush) {
          metadataStore.getStore().commitBatchOperation(batch);
          pendingFlush = false;
        }
      } finally {
        batch.close();
      }
    }
  }
}
