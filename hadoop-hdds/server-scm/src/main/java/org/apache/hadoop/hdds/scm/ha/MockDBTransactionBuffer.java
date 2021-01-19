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

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.ratis.statemachine.SnapshotInfo;

import java.io.IOException;

public class MockDBTransactionBuffer implements DBTransactionBuffer {
  private DBStore dbStore;
  private BatchOperation currentBatchOperation;

  public MockDBTransactionBuffer() {
  }

  public MockDBTransactionBuffer(DBStore store) {
    this.dbStore = store;
  }

  @Override
  public BatchOperation getCurrentBatchOperation() {
    if (currentBatchOperation == null) {
      if (dbStore != null) {
        currentBatchOperation = dbStore.initBatchOperation();
      } else {
        currentBatchOperation = new RDBBatchOperation();
      }
    }
    return currentBatchOperation;
  }

  @Override
  public void updateLatestTrxInfo(SCMTransactionInfo info) {

  }

  @Override
  public SCMTransactionInfo getLatestTrxInfo() {
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
  public void flush() throws IOException {
    if (dbStore != null) {
      dbStore.commitBatchOperation(currentBatchOperation);
      currentBatchOperation.close();
      currentBatchOperation = null;
    }
  }

  @Override
  public void close() throws IOException {
    flush();
  }
}
