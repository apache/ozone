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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto.DeleteBlockTransactionResult;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.command.CommandStatusReportHandler.DeleteBlockStatus;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;

import com.google.common.collect.Lists;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT;
import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.SCMDeleteBlocksCommandStatusManager.CmdStatus;
import static org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator.DEL_TXN_ID;

import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
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
  // The access to DeletedBlocksTXTable is protected by
  // DeletedBlockLogStateManager.
  private final DeletedBlockLogStateManager deletedBlockLogStateManager;
  private final SCMContext scmContext;
  private final SequenceIdGenerator sequenceIdGen;
  private final ScmBlockDeletingServiceMetrics metrics;
  private final SCMDeletedBlockTransactionStatusManager
      transactionStatusManager;
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
    this.transactionStatusManager =
        new SCMDeletedBlockTransactionStatusManager(deletedBlockLogStateManager,
            containerManager, scmContext, metrics, scmCommandTimeoutMs);
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
      transactionStatusManager.incrementRetryCount(txIDs, maxRetry);
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
      transactionStatusManager.resetRetryCount(txIDs);
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
    // we don't need to handle SCMDeletedBlockTransactionStatusManager and
    // deletedBlockLogStateManager, since they will be cleared
    // when becoming leader.
    deletedBlockLogStateManager.reinitialize(deletedTable);
  }

  /**
   * Called in SCMStateMachine#notifyLeaderChanged when current SCM becomes
   *  leader.
   */
  public void onBecomeLeader() {
    transactionStatusManager.clear();
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

  private void getTransaction(DeletedBlocksTransaction tx,
      DatanodeDeletedBlockTransactions transactions,
      Set<DatanodeDetails> dnList, Set<ContainerReplica> replicas,
      Map<UUID, Map<Long, CmdStatus>> commandStatus) {
    DeletedBlocksTransaction updatedTxn =
        DeletedBlocksTransaction.newBuilder(tx)
            .setCount(transactionStatusManager.getOrDefaultRetryCount(
              tx.getTxID(), 0))
            .build();
    for (ContainerReplica replica : replicas) {
      DatanodeDetails details = replica.getDatanodeDetails();
      if (!dnList.contains(details)) {
        continue;
      }
      if (!transactionStatusManager.isDuplication(
          details, updatedTxn.getTxID(), commandStatus)) {
        transactions.addTransactionToDN(details.getUuid(), updatedTxn);
      }
    }
  }

  private Boolean checkInadequateReplica(Set<ContainerReplica> replicas,
      DeletedBlocksTransaction txn) throws ContainerNotFoundException {
    ContainerInfo containerInfo = containerManager
        .getContainer(ContainerID.valueOf(txn.getContainerID()));
    ReplicationManager replicationManager =
        scmContext.getScm().getReplicationManager();
    ContainerHealthResult result = replicationManager
        .getContainerReplicationHealth(containerInfo, replicas);
    return result.getHealthState() != ContainerHealthResult.HealthState.HEALTHY;
  }

  @Override
  public DatanodeDeletedBlockTransactions getTransactions(
      int blockDeletionLimit, Set<DatanodeDetails> dnList)
      throws IOException {
    lock.lock();
    try {
      // Here we can clean up the Datanode timeout command that no longer
      // reports heartbeats
      getSCMDeletedBlockTransactionStatusManager().cleanAllTimeoutSCMCommand(
          scmCommandTimeoutMs);
      DatanodeDeletedBlockTransactions transactions =
          new DatanodeDeletedBlockTransactions();
      try (TableIterator<Long,
          ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iter =
               deletedBlockLogStateManager.getReadOnlyIterator()) {
        // Get the CmdStatus status of the aggregation, so that the current
        // status of the specified transaction can be found faster
        Map<UUID, Map<Long, CmdStatus>> commandStatus =
            getSCMDeletedBlockTransactionStatusManager()
                .getCommandStatusByTxId(dnList.stream().
                map(DatanodeDetails::getUuid).collect(Collectors.toSet()));
        ArrayList<Long> txIDs = new ArrayList<>();
        // Here takes block replica count as the threshold to avoid the case
        // that part of replicas committed the TXN and recorded in the
        // SCMDeletedBlockTransactionStatusManager, while they are counted
        // in the threshold.
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
              Set<ContainerReplica> replicas = containerManager
                  .getContainerReplicas(
                      ContainerID.valueOf(txn.getContainerID()));
              if (checkInadequateReplica(replicas, txn)) {
                continue;
              }
              getTransaction(
                  txn, transactions, dnList, replicas, commandStatus);
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

  @VisibleForTesting
  public SCMDeletedBlockTransactionStatusManager
      getSCMDeletedBlockTransactionStatusManager() {
    return transactionStatusManager;
  }

  @Override
  public void recordTransactionCreated(UUID dnId, long scmCmdId,
      Set<Long> dnTxSet) {
    getSCMDeletedBlockTransactionStatusManager()
        .recordTransactionCreated(dnId, scmCmdId, dnTxSet);
  }

  @Override
  public void onDatanodeDead(UUID dnId) {
    getSCMDeletedBlockTransactionStatusManager().onDatanodeDead(dnId);
  }

  @Override
  public void onSent(DatanodeDetails dnId, SCMCommand<?> scmCommand) {
    getSCMDeletedBlockTransactionStatusManager().onSent(dnId, scmCommand);
  }

  @Override
  public void onMessage(
      DeleteBlockStatus deleteBlockStatus, EventPublisher publisher) {
    if (!scmContext.isLeader()) {
      LOG.info("Skip commit transactions since current SCM is not leader.");
      return;
    }

    DatanodeDetails details = deleteBlockStatus.getDatanodeDetails();
    UUID dnId = details.getUuid();
    for (CommandStatus commandStatus : deleteBlockStatus.getCmdStatus()) {
      CommandStatus.Status status = commandStatus.getStatus();
      lock.lock();
      try {
        if (status == CommandStatus.Status.EXECUTED) {
          ContainerBlocksDeletionACKProto ackProto =
              commandStatus.getBlockDeletionAck();
          getSCMDeletedBlockTransactionStatusManager()
              .commitTransactions(ackProto.getResultsList(), dnId);
          metrics.incrBlockDeletionCommandSuccess();
        } else if (status == CommandStatus.Status.FAILED) {
          metrics.incrBlockDeletionCommandFailure();
        } else {
          LOG.debug("Delete Block Command {} is not executed on the Datanode" +
              " {}.", commandStatus.getCmdId(), dnId);
        }

        getSCMDeletedBlockTransactionStatusManager()
            .commitSCMCommandStatus(deleteBlockStatus.getCmdStatus(), dnId);
      } finally {
        lock.unlock();
      }
    }
  }
}
