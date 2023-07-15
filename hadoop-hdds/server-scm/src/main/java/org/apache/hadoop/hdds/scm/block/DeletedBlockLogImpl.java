/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.block;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.Set;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto.DeleteBlockTransactionResult;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;

import com.google.common.collect.Lists;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT;
import static org.apache.hadoop.hdds.scm.block.SCMDeleteBlocksCommandStatusManager.CmdStatus;
import static org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator.DEL_TXN_ID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A implement class of {@link DeletedBlockLog}, and it uses
 * K/V db to maintain block deletion transactions between scm and datanode.
 * This is a very basic implementation, it simply scans the log and
 * memorize the position that scanned by last time, and uses this to
 * determine where the next scan starts. It has no notion about weight
 * of each transaction so as long as transaction is still valid, they get
 * equally same chance to be retrieved which only depends on the nature
 * order of the transaction ID.
 */
public class DeletedBlockLogImpl implements DeletedBlockLog {

  public static final Logger LOG =
      LoggerFactory.getLogger(DeletedBlockLogImpl.class);

  private final int maxRetry;
  private final ContainerManager containerManager;
  private final Lock lock;
  private final Map<Long, Integer> transactionToRetryCountMap;
  // The access to DeletedBlocksTXTable is protected by
  // DeletedBlockLogStateManager.
  private final DeletedBlockLogStateManager deletedBlockLogStateManager;
  private final SCMContext scmContext;
  private final SequenceIdGenerator sequenceIdGen;
  private final ScmBlockDeletingServiceMetrics metrics;
  private final SCMDeleteBlocksCommandStatusManager
      scmDeleteBlocksCommandStatusManager;
  private long scmCommandTimeoutMs = Duration.ofSeconds(300).toMillis();

  private static final int LIST_ALL_FAILED_TRANSACTIONS = -1;

  @SuppressWarnings("parameternumber")
  public DeletedBlockLogImpl(ConfigurationSource conf,
      ContainerManager containerManager,
      SCMRatisServer ratisServer,
      Table<Long, DeletedBlocksTransaction> deletedBlocksTXTable,
      DBTransactionBuffer dbTxBuffer,
      SCMContext scmContext,
      SequenceIdGenerator sequenceIdGen,
      ScmBlockDeletingServiceMetrics metrics) {
    maxRetry = conf.getInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY,
        OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT);
    this.containerManager = containerManager;
    this.lock = new ReentrantLock();

    transactionToRetryCountMap = new ConcurrentHashMap<>();
    this.deletedBlockLogStateManager = DeletedBlockLogStateManagerImpl
        .newBuilder()
        .setConfiguration(conf)
        .setDeletedBlocksTable(deletedBlocksTXTable)
        .setContainerManager(containerManager)
        .setRatisServer(ratisServer)
        .setSCMDBTransactionBuffer(dbTxBuffer)
        .build();
    this.scmContext = scmContext;
    this.sequenceIdGen = sequenceIdGen;
    this.metrics = metrics;
    this.scmDeleteBlocksCommandStatusManager =
        new SCMDeleteBlocksCommandStatusManager(deletedBlockLogStateManager,
            metrics, containerManager, lock, scmContext, scmCommandTimeoutMs);
  }

  @Override
  public List<DeletedBlocksTransaction> getFailedTransactions(int count,
      long startTxId) throws IOException {
    lock.lock();
    try {
      final List<DeletedBlocksTransaction> failedTXs = Lists.newArrayList();
      try (TableIterator<Long,
          ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iter =
               deletedBlockLogStateManager.getReadOnlyIterator()) {
        if (count == LIST_ALL_FAILED_TRANSACTIONS) {
          while (iter.hasNext()) {
            DeletedBlocksTransaction delTX = iter.next().getValue();
            if (delTX.getCount() == -1) {
              failedTXs.add(delTX);
            }
          }
        } else {
          while (iter.hasNext() && failedTXs.size() < count) {
            DeletedBlocksTransaction delTX = iter.next().getValue();
            if (delTX.getCount() == -1 && delTX.getTxID() >= startTxId) {
              failedTXs.add(delTX);
            }
          }
        }
      }
      return failedTXs;
    } finally {
      lock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param txIDs - transaction ID.
   * @throws IOException
   */
  @Override
  public void incrementCount(List<Long> txIDs)
      throws IOException {
    lock.lock();
    try {
      ArrayList<Long> txIDsToUpdate = new ArrayList<>();
      for (Long txID : txIDs) {
        int currentCount =
            transactionToRetryCountMap.getOrDefault(txID, 0);
        if (currentCount > maxRetry) {
          continue;
        } else {
          currentCount += 1;
          if (currentCount > maxRetry) {
            txIDsToUpdate.add(txID);
          }
          transactionToRetryCountMap.put(txID, currentCount);
        }
      }

      if (!txIDsToUpdate.isEmpty()) {
        deletedBlockLogStateManager
            .increaseRetryCountOfTransactionInDB(txIDsToUpdate);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   *
   */
  @Override
  public int resetCount(List<Long> txIDs) throws IOException {
    lock.lock();
    try {
      if (txIDs == null || txIDs.isEmpty()) {
        txIDs = getFailedTransactions(LIST_ALL_FAILED_TRANSACTIONS, 0).stream()
            .map(DeletedBlocksTransaction::getTxID)
            .collect(Collectors.toList());
      }
      for (Long txID: txIDs) {
        transactionToRetryCountMap.computeIfPresent(txID, (key, value) -> 0);
      }
      return deletedBlockLogStateManager.resetRetryCountOfTransactionInDB(
          new ArrayList<>(new HashSet<>(txIDs)));
    } finally {
      lock.unlock();
    }
  }

  private DeletedBlocksTransaction constructNewTransaction(
      long txID, long containerID, List<Long> blocks) {
    return DeletedBlocksTransaction.newBuilder()
        .setTxID(txID)
        .setContainerID(containerID)
        .addAllLocalID(blocks)
        .setCount(0)
        .build();
  }

  @Override
  public SCMDeleteBlocksCommandStatusManager getScmCommandStatusManager() {
    return scmDeleteBlocksCommandStatusManager;
  }

  private boolean isTransactionFailed(DeleteBlockTransactionResult result) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Got block deletion ACK from datanode, TXIDs={}, " + "success={}",
          result.getTxID(), result.getSuccess());
    }
    if (!result.getSuccess()) {
      LOG.warn("Got failed ACK for TXID={}, prepare to resend the "
          + "TX in next interval", result.getTxID());
      return true;
    }
    return false;
  }

  @Override
  public int getNumOfValidTransactions() throws IOException {
    lock.lock();
    try {
      final AtomicInteger num = new AtomicInteger(0);
      try (TableIterator<Long,
          ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iter =
               deletedBlockLogStateManager.getReadOnlyIterator()) {
        while (iter.hasNext()) {
          DeletedBlocksTransaction delTX = iter.next().getValue();
          if (delTX.getCount() > -1) {
            num.incrementAndGet();
          }
        }
      }
      return num.get();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void reinitialize(
      Table<Long, DeletedBlocksTransaction> deletedTable) {
    // we don't need to handle ScmDeleteBlocksCommandStatusManager and
    // deletedBlockLogStateManager, since they will be cleared
    // when becoming leader.
    deletedBlockLogStateManager.reinitialize(deletedTable);
  }

  /**
   * Called in SCMStateMachine#notifyLeaderChanged when current SCM becomes
   *  leader.
   */
  public void onBecomeLeader() {
    transactionToRetryCountMap.clear();
    scmDeleteBlocksCommandStatusManager.clear();
  }

  /**
   * Called in SCMDBTransactionBuffer#flush when the cached deleting operations
   * are flushed.
   */
  public void onFlush() {
    deletedBlockLogStateManager.onFlush();
  }

  /**
   * {@inheritDoc}
   *
   * @param containerBlocksMap a map of containerBlocks.
   * @throws IOException
   */
  @Override
  public void addTransactions(Map<Long, List<Long>> containerBlocksMap)
      throws IOException {
    lock.lock();
    try {
      ArrayList<DeletedBlocksTransaction> txsToBeAdded = new ArrayList<>();
      for (Map.Entry< Long, List< Long > > entry :
          containerBlocksMap.entrySet()) {
        long nextTXID = sequenceIdGen.getNextId(DEL_TXN_ID);
        DeletedBlocksTransaction tx = constructNewTransaction(nextTXID,
            entry.getKey(), entry.getValue());
        txsToBeAdded.add(tx);
      }

      deletedBlockLogStateManager.addTransactionsToDB(txsToBeAdded);
      metrics.incrBlockDeletionTransactionCreated(txsToBeAdded.size());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
  }

  private void getTransaction(
      DeletedBlocksTransaction tx,
      DatanodeDeletedBlockTransactions transactions,
      Set<DatanodeDetails> dnList,
      Map<UUID, Map<Long, CmdStatus>> commandStatus) {
    try {
      DeletedBlocksTransaction updatedTxn = DeletedBlocksTransaction
          .newBuilder(tx)
          .setCount(transactionToRetryCountMap.getOrDefault(tx.getTxID(), 0))
          .build();
      Set<ContainerReplica> replicas = containerManager
          .getContainerReplicas(
              ContainerID.valueOf(updatedTxn.getContainerID()));
      for (ContainerReplica replica : replicas) {
        DatanodeDetails details = replica.getDatanodeDetails();
        if (shouldAddTransactionToDN(details,
            updatedTxn.getTxID(), dnList, commandStatus)) {
          transactions.addTransactionToDN(details.getUuid(), updatedTxn);
        }
      }
    } catch (IOException e) {
      LOG.warn("Got container info error.", e);
    }
  }

  private boolean shouldAddTransactionToDN(DatanodeDetails dnDetail, long tx,
      Set<DatanodeDetails> dnLists,
      Map<UUID, Map<Long, CmdStatus>> commandStatus) {
    if (!dnLists.contains(dnDetail)) {
      return false;
    }
    if (getScmCommandStatusManager().alreadyExecuted(dnDetail.getUuid(), tx)) {
      return false;
    }
    return !inProcessing(dnDetail.getUuid(), tx, commandStatus);
  }

  private boolean inProcessing(UUID dnId, long deletedBlocksTxId,
      Map<UUID, Map<Long, CmdStatus>> commandStatus) {
    Map<Long, CmdStatus> deletedBlocksTxStatus = commandStatus.get(dnId);
    if (deletedBlocksTxStatus == null ||
        deletedBlocksTxStatus.get(deletedBlocksTxId) == null) {
      return false;
    }
    return deletedBlocksTxStatus.get(deletedBlocksTxId) !=
        CmdStatus.NEED_RESEND;
  }

  @Override
  public DatanodeDeletedBlockTransactions getTransactions(
      int blockDeletionLimit, Set<DatanodeDetails> dnList)
      throws IOException {
    lock.lock();
    try {
      // Here we can clean up the Datanode timeout command that no longer
      // reports heartbeats
      getScmCommandStatusManager().cleanAllTimeoutSCMCommand(
          scmCommandTimeoutMs);
      DatanodeDeletedBlockTransactions transactions =
          new DatanodeDeletedBlockTransactions();
      try (TableIterator<Long,
          ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iter =
               deletedBlockLogStateManager.getReadOnlyIterator()) {
        // Get the CmdStatus status of the aggregation, so that the current
        // status of the specified transaction can be found faster
        Map<UUID, Map<Long, CmdStatus>> commandStatus =
            getScmCommandStatusManager().getCommandStatusByTxId(dnList.stream().
                map(DatanodeDetails::getUuid).collect(Collectors.toSet()));
        ArrayList<Long> txIDs = new ArrayList<>();
        // Here takes block replica count as the threshold to avoid the case
        // that part of replicas committed the TXN and recorded in the
        // ScmDeleteBlocksCommandStatusManager, while they are counted in the
        // threshold.
        while (iter.hasNext() &&
            transactions.getBlocksDeleted() < blockDeletionLimit) {
          Table.KeyValue<Long, DeletedBlocksTransaction> keyValue = iter.next();
          DeletedBlocksTransaction txn = keyValue.getValue();
          final ContainerID id = ContainerID.valueOf(txn.getContainerID());
          try {
            // HDDS-7126. When container is under replicated, it is possible
            // that container is deleted, but transactions are not deleted.
            if (containerManager.getContainer(id).isDeleted()) {
              LOG.warn("Container: " + id + " was deleted for the " +
                  "transaction: " + txn);
              txIDs.add(txn.getTxID());
            } else if (txn.getCount() > -1 && txn.getCount() <= maxRetry
                && !containerManager.getContainer(id).isOpen()) {
              getTransaction(txn, transactions, dnList, commandStatus);
              getScmCommandStatusManager().
                  recordTransactionToDNsCommitMap(txn.getTxID());
            }
          } catch (ContainerNotFoundException ex) {
            LOG.warn("Container: " + id + " was not found for the transaction: "
                + txn);
            txIDs.add(txn.getTxID());
          }
        }
        if (!txIDs.isEmpty()) {
          deletedBlockLogStateManager.removeTransactionsFromDB(txIDs);
          metrics.incrBlockDeletionTransactionCompleted(txIDs.size());
        }
      }
      return transactions;
    } finally {
      lock.unlock();
    }
  }

  public void setScmCommandTimeoutMs(long scmCommandTimeoutMs) {
    this.scmCommandTimeoutMs = scmCommandTimeoutMs;
  }

}
