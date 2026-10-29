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

import java.util.UUID;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.junit.jupiter.api.Test;

class TestDatanodeDeletedBlockTransactions {

  @Test
  void testNumberOfBlocksForDatanode() {
    DatanodeID first = DatanodeID.of(UUID.randomUUID());
    DatanodeID second = DatanodeID.of(UUID.randomUUID());
    DatanodeDeletedBlockTransactions transactions =
        new DatanodeDeletedBlockTransactions();

    assertEquals(0, transactions.getNumberOfBlocksForDatanode(first));

    transactions.addTransactionToDN(first, transaction(1, 1, 2));
    transactions.addTransactionToDN(first, transaction(2, 3, 4, 5));
    transactions.addTransactionToDN(second, transaction(3, 6));

    assertEquals(5, transactions.getNumberOfBlocksForDatanode(first));
    assertEquals(1, transactions.getNumberOfBlocksForDatanode(second));
  }

  private static DeletedBlocksTransaction transaction(
      long txId, long... localIds) {
    DeletedBlocksTransaction.Builder builder =
        DeletedBlocksTransaction.newBuilder()
            .setTxID(txId)
            .setContainerID(txId);
    for (long localId : localIds) {
      builder.addLocalID(localId);
    }
    return builder.build();
  }
}
