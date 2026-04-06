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

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.ha.SCMHandler;
import org.apache.hadoop.hdds.scm.ha.invoker.ScmInvokerCodeGenerator;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.utils.db.Table;

/**
 * DeletedBlockLogStateManager interface to
 * manage deleted blocks and record them in the underlying persist store.
 */
public interface DeletedBlockLogStateManager extends SCMHandler {

  @Override
  default RequestType getType() {
    return RequestType.BLOCK;
  }

  @Replicate
  void addTransactionsToDB(ArrayList<DeletedBlocksTransaction> txs,
      DeletedBlocksTransactionSummary summary) throws IOException;

  @Replicate
  default void addTransactionsToDB(ArrayList<DeletedBlocksTransaction> txs) throws IOException {
    addTransactionsToDB(txs, null);
  }

  @Replicate
  void removeTransactionsFromDB(ArrayList<Long> txIDs, DeletedBlocksTransactionSummary summary)
      throws IOException;

  @Replicate
  default void removeTransactionsFromDB(ArrayList<Long> txIDs) throws IOException {
    removeTransactionsFromDB(txIDs, null);
  }

  @Deprecated
  @Replicate
  default void increaseRetryCountOfTransactionInDB(ArrayList<Long> txIDs) throws IOException {
  }

  @Deprecated
  @Replicate
  default int resetRetryCountOfTransactionInDB(ArrayList<Long> txIDs) throws IOException {
    return 0;
  }

  Table.KeyValueIterator<Long, DeletedBlocksTransaction> getReadOnlyIterator()
      throws IOException;

  void onFlush();

  void reinitialize(Table<Long, DeletedBlocksTransaction> deletedBlocksTXTable,
      Table<String, ByteString> statefulConfigTable);

  static void main(String[] args) {
    ScmInvokerCodeGenerator.generate(DeletedBlockLogStateManager.class, RequestType.BLOCK, true);
  }
}
