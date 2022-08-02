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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLog;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.ratis.statemachine.SnapshotInfo;

import java.io.IOException;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;

/**
 * This is a transaction buffer that buffers SCM DB operations for Pipeline and
 * Container. When flush this buffer to DB, a transaction info will also be
 * written into DB to indicate the term and transaction index for the latest
 * operation in DB.
 */
public class SCMHADBTransactionBufferImpl implements SCMHADBTransactionBuffer {
  private final StorageContainerManager scm;
  private SCMMetadataStore metadataStore;
  private BatchOperation currentBatchOperation;
  private TransactionInfo latestTrxInfo;
  private SnapshotInfo latestSnapshot;

  public SCMHADBTransactionBufferImpl(StorageContainerManager scm)
      throws IOException {
    this.scm = scm;
    init();
  }

  private BatchOperation getCurrentBatchOperation() {
    return currentBatchOperation;
  }

  @Override
  public <KEY, VALUE> void addToBuffer(
      Table<KEY, VALUE> table, KEY key, VALUE value) throws IOException {
    table.putWithBatch(getCurrentBatchOperation(), key, value);
  }

  @Override
  public <KEY, VALUE> void removeFromBuffer(Table<KEY, VALUE> table, KEY key)
      throws IOException {
    table.deleteWithBatch(getCurrentBatchOperation(), key);
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
    return latestSnapshot;
  }

  @Override
  public void setLatestSnapshot(SnapshotInfo latestSnapshot) {
    this.latestSnapshot = latestSnapshot;
  }

  @Override
  public void flush() throws IOException {
    // write latest trx info into trx table in the same batch
    Table<String, TransactionInfo> transactionInfoTable
        = metadataStore.getTransactionInfoTable();
    transactionInfoTable.putWithBatch(currentBatchOperation,
        TRANSACTION_INFO_KEY, latestTrxInfo);

    metadataStore.getStore().commitBatchOperation(currentBatchOperation);
    currentBatchOperation.close();
    this.latestSnapshot = latestTrxInfo.toSnapshotInfo();
    // reset batch operation
    currentBatchOperation = metadataStore.getStore().initBatchOperation();

    DeletedBlockLog deletedBlockLog = scm.getScmBlockManager()
        .getDeletedBlockLog();
    Preconditions.checkArgument(
        deletedBlockLog instanceof DeletedBlockLogImpl);
    ((DeletedBlockLogImpl) deletedBlockLog).onFlush();
  }

  @Override
  public void init() throws IOException {
    metadataStore = scm.getScmMetadataStore();

    // initialize a batch operation during construction time
    currentBatchOperation = this.metadataStore.getStore().initBatchOperation();
    latestTrxInfo = this.metadataStore.getTransactionInfoTable()
        .get(TRANSACTION_INFO_KEY);
    if (latestTrxInfo == null) {
      // transaction table is empty
      latestTrxInfo =
          TransactionInfo
              .builder()
              .setTransactionIndex(-1)
              .setCurrentTerm(0)
              .build();
    }
    latestSnapshot = latestTrxInfo.toSnapshotInfo();
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
