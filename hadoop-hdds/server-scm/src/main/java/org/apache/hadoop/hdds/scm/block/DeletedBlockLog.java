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
import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.DeletedBlock;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

/**
 * The DeletedBlockLog is a persisted log in SCM to keep tracking
 * container blocks which are under deletion. It maintains info
 * about under-deletion container blocks that notified by OM,
 * and the state how it is processed.
 */
public interface DeletedBlockLog extends Closeable {

  /**
   * Scan entire log once and returns TXs to DatanodeDeletedBlockTransactions.
   * Once DatanodeDeletedBlockTransactions is full, the scan behavior will
   * stop.
   *
   * @param blockDeletionLimit Maximum number of blocks to fetch
   * @param dnList healthy dn list
   * @return Mapping from containerId to latest transactionId for the container.
   * @throws IOException
   */
  DatanodeDeletedBlockTransactions getTransactions(
      int blockDeletionLimit, Set<DatanodeDetails> dnList)
      throws IOException;

  /**
   * Increments count for the given list of transactions by 1.
   * The retry count is maintained only for in-flight transactions,
   * this will be useful in debugging.
   */
  void incrementCount(List<Long> txIDs)
      throws IOException;

  /**
   * Records the creation of a transaction for a DataNode.
   *
   * @param dnId The identifier of the DataNode.
   * @param scmCmdId The ID of the SCM command.
   * @param dnTxSet Set of transaction IDs for the DataNode.
   */
  void recordTransactionCreated(
      DatanodeID dnId, long scmCmdId, Set<Long> dnTxSet);

  /**
   * Handles the cleanup process when a DataNode is reported dead. This method
   * is responsible for updating or cleaning up the transaction records
   * associated with the dead DataNode.
   *
   * @param dnId The identifier of the dead DataNode.
   */
  void onDatanodeDead(DatanodeID dnId);

  /**
   * Records the event of sending a block deletion command to a DataNode. This
   * method is called when a command is successfully dispatched to a DataNode,
   * and it helps in tracking the status of the command.
   *
   * @param dnId Details of the DataNode.
   * @param scmCommand The block deletion command sent.
   */
  void onSent(DatanodeDetails dnId, SCMCommand<?> scmCommand);

  /**
   * Creates block deletion transactions for a set of containers,
   * add into the log and persist them atomically. An object key
   * might be stored in multiple containers and multiple blocks,
   * this API ensures that these updates are done in atomic manner
   * so if any of them fails, the entire operation fails without
   * any updates to the log. Note, this doesn't mean to create only
   * one transaction, it creates multiple transactions (depends on the
   * number of containers) together (on success) or non (on failure).
   *
   * @param containerBlocksMap a map of containerBlocks.
   * @throws IOException
   */
  void addTransactions(Map<Long, List<DeletedBlock>> containerBlocksMap)
      throws IOException;

  /**
   * Returns the total number of valid transactions. A transaction is
   * considered to be valid as long as its count is in range [0, MAX_RETRY].
   *
   * @return number of a valid transactions.
   * @throws IOException
   */
  int getNumOfValidTransactions() throws IOException;

  /**
   * Reinitialize the delete log from the db.
   * @param deletedBlocksTXTable delete transaction table
   * @param statefulConfigTable stateful service config table
   */
  void reinitialize(Table<Long, DeletedBlocksTransaction> deletedBlocksTXTable,
      Table<String, ByteString> statefulConfigTable) throws IOException;

  int getTransactionToDNsCommitMapSize();

  @Nullable
  DeletedBlocksTransactionSummary getTransactionSummary();
}
