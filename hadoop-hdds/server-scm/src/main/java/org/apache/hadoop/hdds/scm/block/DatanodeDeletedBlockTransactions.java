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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;

/**
 * A wrapper class to hold info about datanode and all deleted block
 * transactions that will be sent to this datanode.
 */
class DatanodeDeletedBlockTransactions {
  // A list of TXs mapped to a certain datanode ID.
  private final Map<DatanodeID, List<DeletedBlocksTransaction>> transactions =
      new HashMap<>();
  // counts blocks deleted across datanodes. Blocks deleted will be counted
  // for all the replicas and may not be unique.
  private int blocksDeleted = 0;

  DatanodeDeletedBlockTransactions() {
  }

  void addTransactionToDN(DatanodeID dnID, DeletedBlocksTransaction tx) {
    transactions.computeIfAbsent(dnID, k -> new LinkedList<>()).add(tx);
    blocksDeleted += tx.getLocalIDCount();
    if (SCMBlockDeletingService.LOG.isDebugEnabled()) {
      SCMBlockDeletingService.LOG
          .debug("Transaction added: {} <- TX({}), DN {} <- blocksDeleted Add {}.",
          dnID, tx.getTxID(), dnID, tx.getLocalIDCount());
    }
  }

  Map<DatanodeID, List<DeletedBlocksTransaction>> getDatanodeTransactionMap() {
    return transactions;
  }

  int getBlocksDeleted() {
    return blocksDeleted;
  }

  List<String> getTransactionIDList(DatanodeID dnId) {
    return Optional.ofNullable(transactions.get(dnId))
        .orElse(new LinkedList<>())
        .stream()
        .map(DeletedBlocksTransaction::getTxID)
        .map(String::valueOf)
        .collect(Collectors.toList());
  }

  public int getNumberOfBlocksForDatanode(DatanodeID dnId) {
    return Optional.ofNullable(transactions.get(dnId))
        .orElse(new LinkedList<>())
        .stream()
        .mapToInt(DeletedBlocksTransaction::getLocalIDCount)
        .sum();
  }

  boolean isEmpty() {
    return transactions.isEmpty();
  }
}
