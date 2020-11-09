/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.block;

import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;

import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A wrapper class to hold info about datanode and all deleted block
 * transactions that will be sent to this datanode.
 */
class DatanodeDeletedBlockTransactions {
  // A list of TXs mapped to a certain datanode ID.
  private final Map<UUID, List<DeletedBlocksTransaction>> transactions =
      new HashMap<>();
  // counts blocks deleted across datanodes. Blocks deleted will be counted
  // for all the replicas and may not be unique.
  private int blocksDeleted = 0;
  private final Map<Long, Long> containerIdToTxnId = new HashMap<>();

  DatanodeDeletedBlockTransactions() {
  }

  void addTransactionToDN(UUID dnID, DeletedBlocksTransaction tx) {
    transactions.computeIfAbsent(dnID, k -> new LinkedList<>()).add(tx);
    containerIdToTxnId.put(tx.getContainerID(), tx.getTxID());
    blocksDeleted += tx.getLocalIDCount();
    if (SCMBlockDeletingService.LOG.isDebugEnabled()) {
      SCMBlockDeletingService.LOG
          .debug("Transaction added: {} <- TX({})", dnID, tx.getTxID());
    }
  }

  Map<UUID, List<DeletedBlocksTransaction>> getDatanodeTransactionMap() {
    return transactions;
  }

  Map<Long, Long> getContainerIdToTxnIdMap() {
    return containerIdToTxnId;
  }

  int getBlocksDeleted() {
    return blocksDeleted;
  }

  List<String> getTransactionIDList(UUID dnId) {
    return Optional.ofNullable(transactions.get(dnId))
        .orElse(new LinkedList<>())
        .stream()
        .map(DeletedBlocksTransaction::getTxID)
        .map(String::valueOf)
        .collect(Collectors.toList());
  }

  boolean isEmpty() {
    return transactions.isEmpty();
  }
}