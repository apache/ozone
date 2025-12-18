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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLog;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a transaction buffer that buffers SCM DB operations for Pipeline and
 * Container. When flush this buffer to DB, a transaction info will also be
 * written into DB to indicate the term and transaction index for the latest
 * operation in DB.
 */
public class SCMHADBTransactionBufferImpl implements SCMHADBTransactionBuffer {

  private static final Logger LOG = LoggerFactory.getLogger(SCMHADBTransactionBufferImpl.class);
  private final StorageContainerManager scm;
  private SCMMetadataStore metadataStore;
  private BatchOperation currentBatchOperation;
  private TransactionInfo latestTrxInfo;
  private final AtomicReference<SnapshotInfo> latestSnapshot
      = new AtomicReference<>();
  private final AtomicLong txFlushPending = new AtomicLong(0);
  private long lastSnapshotTimeMs = 0;
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

  public SCMHADBTransactionBufferImpl(StorageContainerManager scm) throws RocksDatabaseException, CodecException {
    this.scm = scm;
    init();
  }

  private BatchOperation getCurrentBatchOperation() {
    return currentBatchOperation;
  }

  @Override
  public <KEY, VALUE> void addToBuffer(Table<KEY, VALUE> table, KEY key, VALUE value)
      throws RocksDatabaseException, CodecException {
    rwLock.readLock().lock();
    try {
      txFlushPending.getAndIncrement();
      table.putWithBatch(getCurrentBatchOperation(), key, value);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public <KEY, VALUE> void removeFromBuffer(Table<KEY, VALUE> table, KEY key)
      throws CodecException, RocksDatabaseException {
    rwLock.readLock().lock();
    try {
      txFlushPending.getAndIncrement();
      table.deleteWithBatch(getCurrentBatchOperation(), key);
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
    LOG.info("{}: Set latest Snapshot to {}",
        scm.getScmHAManager().getRatisServer().getDivision().getId(), latestSnapshot);
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
      // write latest trx info into trx table in the same batch
      Table<String, TransactionInfo> transactionInfoTable
          = metadataStore.getTransactionInfoTable();
      transactionInfoTable.putWithBatch(currentBatchOperation,
          TRANSACTION_INFO_KEY, latestTrxInfo);

      metadataStore.getStore().commitBatchOperation(currentBatchOperation);
      currentBatchOperation.close();
      this.latestSnapshot.set(latestTrxInfo.toSnapshotInfo());
      // reset batch operation
      currentBatchOperation = metadataStore.getStore().initBatchOperation();

      DeletedBlockLog deletedBlockLog = scm.getScmBlockManager()
          .getDeletedBlockLog();
      Preconditions.checkArgument(
          deletedBlockLog instanceof DeletedBlockLogImpl);
      ((DeletedBlockLogImpl) deletedBlockLog).onFlush();
    } finally {
      txFlushPending.set(0);
      lastSnapshotTimeMs = scm.getSystemClock().millis();
      rwLock.writeLock().unlock();
    }
  }

  @Override
  public void init() throws RocksDatabaseException, CodecException {
    metadataStore = scm.getScmMetadataStore();

    rwLock.writeLock().lock();
    try {
      IOUtils.closeQuietly(currentBatchOperation);

      // initialize a batch operation during construction time
      currentBatchOperation = this.metadataStore.getStore().
          initBatchOperation();
      latestTrxInfo = this.metadataStore.getTransactionInfoTable()
          .get(TRANSACTION_INFO_KEY);
      if (latestTrxInfo == null) {
        // transaction table is empty
        latestTrxInfo = TransactionInfo.DEFAULT_VALUE;
      }
      latestSnapshot.set(latestTrxInfo.toSnapshotInfo());
    } finally {
      txFlushPending.set(0);
      lastSnapshotTimeMs = scm.getSystemClock().millis();
      rwLock.writeLock().unlock();
    }
  }
  
  @Override
  public boolean shouldFlush(long snapshotWaitTime) {
    rwLock.readLock().lock();
    try {
      long timeDiff = scm.getSystemClock().millis() - lastSnapshotTimeMs;
      return txFlushPending.get() > 0 && timeDiff > snapshotWaitTime;
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
    if (currentBatchOperation != null) {
      currentBatchOperation.close();
    }
  }
}
