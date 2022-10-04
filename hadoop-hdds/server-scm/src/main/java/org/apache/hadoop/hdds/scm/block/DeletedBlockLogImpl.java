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
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.Set;
import java.util.Map;
import java.util.LinkedHashSet;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto.DeleteBlockTransactionResult;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.scm.command.CommandStatusReportHandler.DeleteBlockStatus;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;

import com.google.common.collect.Lists;
import static java.lang.Math.min;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT;
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
public class DeletedBlockLogImpl
    implements DeletedBlockLog, EventHandler<DeleteBlockStatus> {

  public static final Logger LOG =
      LoggerFactory.getLogger(DeletedBlockLogImpl.class);

  private final int maxRetry;
  private final ContainerManager containerManager;
  private final Lock lock;
  // Maps txId to set of DNs which are successful in committing the transaction
  private final Map<Long, Set<UUID>> transactionToDNsCommitMap;
  // Maps txId to its retry counts;
  private final Map<Long, Integer> transactionToRetryCountMap;
  // The access to DeletedBlocksTXTable is protected by
  // DeletedBlockLogStateManager.
  private final DeletedBlockLogStateManager deletedBlockLogStateManager;
  private final SCMContext scmContext;
  private final SequenceIdGenerator sequenceIdGen;
  private final ScmBlockDeletingServiceMetrics metrics;

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

    // transactionToDNsCommitMap is updated only when
    // transaction is added to the log and when it is removed.

    // maps transaction to dns which have committed it.
    transactionToDNsCommitMap = new ConcurrentHashMap<>();
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
  }

  @Override
  public List<DeletedBlocksTransaction> getFailedTransactions()
      throws IOException {
    lock.lock();
    try {
      final List<DeletedBlocksTransaction> failedTXs = Lists.newArrayList();
      try (TableIterator<Long,
          ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iter =
               deletedBlockLogStateManager.getReadOnlyIterator()) {
        while (iter.hasNext()) {
          DeletedBlocksTransaction delTX = iter.next().getValue();
          if (delTX.getCount() == -1) {
            failedTXs.add(delTX);
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
      throws IOException, TimeoutException {
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
  public int resetCount(List<Long> txIDs) throws IOException, TimeoutException {
    lock.lock();
    try {
      if (txIDs == null || txIDs.isEmpty()) {
        txIDs = getFailedTransactions().stream()
            .map(DeletedBlocksTransaction::getTxID)
            .collect(Collectors.toList());
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

  /**
   * {@inheritDoc}
   *
   * @param transactionResults - transaction IDs.
   * @param dnID               - Id of Datanode which has acknowledged
   *                           a delete block command.
   * @throws IOException
   */
  @Override
  public void commitTransactions(
      List<DeleteBlockTransactionResult> transactionResults, UUID dnID) {
    lock.lock();
    try {
      ArrayList<Long> txIDsToBeDeleted = new ArrayList<>();
      Set<UUID> dnsWithCommittedTxn;
      for (DeleteBlockTransactionResult transactionResult :
          transactionResults) {
        if (isTransactionFailed(transactionResult)) {
          metrics.incrBlockDeletionTransactionFailure();
          continue;
        }
        try {
          metrics.incrBlockDeletionTransactionSuccess();
          long txID = transactionResult.getTxID();
          // set of dns which have successfully committed transaction txId.
          dnsWithCommittedTxn = transactionToDNsCommitMap.get(txID);
          final ContainerID containerId = ContainerID.valueOf(
              transactionResult.getContainerID());
          if (dnsWithCommittedTxn == null) {
            // Mostly likely it's a retried delete command response.
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "Transaction txId={} commit by dnId={} for containerID={}"
                      + " failed. Corresponding entry not found.", txID, dnID,
                  containerId);
            }
            continue;
          }

          dnsWithCommittedTxn.add(dnID);
          final ContainerInfo container =
              containerManager.getContainer(containerId);
          final Set<ContainerReplica> replicas =
              containerManager.getContainerReplicas(containerId);
          // The delete entry can be safely removed from the log if all the
          // corresponding nodes commit the txn. It is required to check that
          // the nodes returned in the pipeline match the replication factor.
          if (min(replicas.size(), dnsWithCommittedTxn.size())
              >= container.getReplicationConfig().getRequiredNodes()) {
            List<UUID> containerDns = replicas.stream()
                .map(ContainerReplica::getDatanodeDetails)
                .map(DatanodeDetails::getUuid)
                .collect(Collectors.toList());
            if (dnsWithCommittedTxn.containsAll(containerDns)) {
              transactionToDNsCommitMap.remove(txID);
              transactionToRetryCountMap.remove(txID);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Purging txId={} from block deletion log", txID);
              }
              txIDsToBeDeleted.add(txID);
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Datanode txId={} containerId={} committed by dnId={}",
                txID, containerId, dnID);
          }
        } catch (IOException e) {
          LOG.warn("Could not commit delete block transaction: " +
              transactionResult.getTxID(), e);
        }
      }
      try {
        deletedBlockLogStateManager.removeTransactionsFromDB(txIDsToBeDeleted);
        metrics.incrBlockDeletionTransactionCompleted(txIDsToBeDeleted.size());
      } catch (IOException | TimeoutException e) {
        LOG.warn("Could not commit delete block transactions: "
            + txIDsToBeDeleted, e);
      }
    } finally {
      lock.unlock();
    }
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
    // we don't need handle transactionToDNsCommitMap and
    // deletedBlockLogStateManager, since they will be cleared
    // when becoming leader.
    deletedBlockLogStateManager.reinitialize(deletedTable);
  }

  /**
   * Called in SCMStateMachine#notifyLeaderChanged when current SCM becomes
   *  leader.
   */
  public void onBecomeLeader() {
    transactionToDNsCommitMap.clear();
    transactionToRetryCountMap.clear();
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
      throws IOException, TimeoutException {
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

  private void getTransaction(DeletedBlocksTransaction tx,
      DatanodeDeletedBlockTransactions transactions) {
    try {
      Set<ContainerReplica> replicas = containerManager
          .getContainerReplicas(ContainerID.valueOf(tx.getContainerID()));
      for (ContainerReplica replica : replicas) {
        UUID dnID = replica.getDatanodeDetails().getUuid();
        Set<UUID> dnsWithTransactionCommitted =
            transactionToDNsCommitMap.get(tx.getTxID());
        if (dnsWithTransactionCommitted == null || !dnsWithTransactionCommitted
            .contains(dnID)) {
          // Transaction need not be sent to dns which have
          // already committed it
          transactions.addTransactionToDN(dnID, tx);
        }
      }
    } catch (IOException e) {
      LOG.warn("Got container info error.", e);
    }
  }

  @Override
  public DatanodeDeletedBlockTransactions getTransactions(
      int blockDeletionLimit) throws IOException, TimeoutException {
    lock.lock();
    try {
      DatanodeDeletedBlockTransactions transactions =
          new DatanodeDeletedBlockTransactions();
      try (TableIterator<Long,
          ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iter =
               deletedBlockLogStateManager.getReadOnlyIterator()) {
        ArrayList<Long> txIDs = new ArrayList<>();
        // Here takes block replica count as the threshold to avoid the case
        // that part of replicas committed the TXN and recorded in the
        // transactionToDNsCommitMap, while they are counted in the threshold.
        while (iter.hasNext() &&
            transactions.getBlocksDeleted() < blockDeletionLimit) {
          Table.KeyValue<Long, DeletedBlocksTransaction> keyValue = iter.next();
          DeletedBlocksTransaction txn = keyValue.getValue();
          final ContainerID id = ContainerID.valueOf(txn.getContainerID());
          try {
            if (txn.getCount() > -1 && txn.getCount() <= maxRetry
                && !containerManager.getContainer(id).isOpen()) {
              getTransaction(txn, transactions);
              transactionToDNsCommitMap
                  .putIfAbsent(txn.getTxID(), new LinkedHashSet<>());
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

  @Override
  public void onMessage(
      DeleteBlockStatus deleteBlockStatus, EventPublisher publisher) {
    if (!scmContext.isLeader()) {
      LOG.warn("Skip commit transactions since current SCM is not leader.");
      return;
    }

    CommandStatus.Status status = deleteBlockStatus.getCmdStatus().getStatus();
    if (status == CommandStatus.Status.EXECUTED) {
      ContainerBlocksDeletionACKProto ackProto =
          deleteBlockStatus.getCmdStatus().getBlockDeletionAck();
      commitTransactions(ackProto.getResultsList(),
          UUID.fromString(ackProto.getDnId()));
      metrics.incrBlockDeletionCommandSuccess();
    } else if (status == CommandStatus.Status.FAILED) {
      metrics.incrBlockDeletionCommandFailure();
    } else {
      LOG.error("Delete Block Command is not executed yet.");
      return;
    }
  }
}
