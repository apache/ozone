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

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.ratis.statemachine.SnapshotInfo;

// TODO: Move this class to test package after fixing Recon
/**
 * SCMHADBTransactionBuffer implementation for Recon and testing.
 */
public class SCMHADBTransactionBufferStub implements SCMHADBTransactionBuffer {
  private DBStore dbStore;
  private BatchOperation currentBatchOperation;
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

  public SCMHADBTransactionBufferStub() {
  }

  public SCMHADBTransactionBufferStub(DBStore store) {
    this.dbStore = store;
  }

  private BatchOperation getCurrentBatchOperation() {
    if (currentBatchOperation == null) {
      if (dbStore != null) {
        currentBatchOperation = dbStore.initBatchOperation();
      } else {
        currentBatchOperation = RDBBatchOperation.newAtomicOperation();
      }
    }
    return currentBatchOperation;
  }

  @Override
  public <KEY, VALUE> void addToBuffer(Table<KEY, VALUE> table, KEY key, VALUE value)
      throws RocksDatabaseException, CodecException {
    rwLock.readLock().lock();
    try {
      table.putWithBatch(getCurrentBatchOperation(), key, value);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public <KEY, VALUE> void removeFromBuffer(Table<KEY, VALUE> table, KEY key) throws CodecException {
    rwLock.readLock().lock();
    try {
      table.deleteWithBatch(getCurrentBatchOperation(), key);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public void updateLatestTrxInfo(TransactionInfo info) {

  }

  @Override
  public TransactionInfo getLatestTrxInfo() {
    return null;
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    return null;
  }

  @Override
  public void setLatestSnapshot(SnapshotInfo latestSnapshot) {

  }

  @Override
  public AtomicReference<SnapshotInfo> getLatestSnapshotRef() {
    return null;
  }

  @Override
  public void flush() throws RocksDatabaseException {
    rwLock.writeLock().lock();
    try {
      if (dbStore != null) {
        dbStore.commitBatchOperation(getCurrentBatchOperation());
      }
      if (currentBatchOperation != null) {
        currentBatchOperation.close();
        currentBatchOperation = null;
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @Override
  public boolean shouldFlush(long snapshotWaitTime) {
    return true;
  }

  @Override
  public void init() {
  }

  @Override
  public void close() throws RocksDatabaseException {
    flush();
  }
}
