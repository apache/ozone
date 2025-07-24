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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT;
import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.SCMDeleteBlocksCommandStatusManager.CmdStatus;
import static org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator.DEL_TXN_ID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.command.CommandStatusReportHandler.DeleteBlockStatus;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.DeletedBlock;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implement class of {@link DeletedBlockLog}, and it uses
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

  private static final Logger LOG =
      LoggerFactory.getLogger(DeletedBlockLogImpl.class);

  private final int maxRetry;
  private final ContainerManager containerManager;
  private final Lock lock;
  // The access to DeletedBlocksTXTable is protected by
  // DeletedBlockLogStateManager.
  private DeletedBlockLogStateManager deletedBlockLogStateManager;
  private final SCMContext scmContext;
  private final SequenceIdGenerator sequenceIdGen;
  private final ScmBlockDeletingServiceMetrics metrics;
  private final SCMDeletedBlockTransactionStatusManager
      transactionStatusManager;
  private long scmCommandTimeoutMs = Duration.ofSeconds(300).toMillis();

  private static final int LIST_ALL_FAILED_TRANSACTIONS = -1;
  private long lastProcessedTransactionId = -1;
  private final int logAppenderQueueByteLimit;
  // an in memory map to cache the size of each transaction sending to DN.
  private final Map<Long, TxBlockInfo> txSizeMap;

  public DeletedBlockLogImpl(ConfigurationSource conf,
      StorageContainerManager scm,
      ContainerManager containerManager,
      DBTransactionBuffer dbTxBuffer,
      ScmBlockDeletingServiceMetrics metrics) throws IOException {
    maxRetry = conf.getInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY,
        OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT);
    this.containerManager = containerManager;
    this.lock = new ReentrantLock();
    this.txSizeMap = new ConcurrentHashMap<>();

    this.deletedBlockLogStateManager = DeletedBlockLogStateManagerImpl
        .newBuilder()
        .setConfiguration(conf)
        .setDeletedBlocksTable(scm.getScmMetadataStore().getDeletedBlocksTXTable())
        .setContainerManager(containerManager)
        .setRatisServer(scm.getScmHAManager().getRatisServer())
        .setSCMDBTransactionBuffer(dbTxBuffer)
        .setStatefulConfigTable(scm.getScmMetadataStore().getStatefulServiceConfigTable())
        .setTxBlockInfoMap(txSizeMap)
        .setMetrics(metrics)
        .build();
    this.scmContext = scm.getScmContext();
    this.sequenceIdGen = scm.getSequenceIdGen();
    this.metrics = metrics;
    this.transactionStatusManager =
        new SCMDeletedBlockTransactionStatusManager(deletedBlockLogStateManager,
            containerManager, metrics, scmCommandTimeoutMs);
    int limit = (int) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    this.logAppenderQueueByteLimit = (int) (limit * 0.9);
  }

  @VisibleForTesting
  public void setDeletedBlockLogStateManager(DeletedBlockLogStateManager manager) {
    this.deletedBlockLogStateManager = manager;
  }

  @VisibleForTesting
  public DeletedBlockLogStateManager getDeletedBlockLogStateManager() {
    return deletedBlockLogStateManager;
  }

  @Override
  public List<DeletedBlocksTransaction> getFailedTransactions(int count,
      long startTxId) throws IOException {
    lock.lock();
    try {
      final List<DeletedBlocksTransaction> failedTXs = Lists.newArrayList();
      try (Table.KeyValueIterator<Long, DeletedBlocksTransaction> iter =
               deletedBlockLogStateManager.getReadOnlyIterator()) {
        if (count == LIST_ALL_FAILED_TRANSACTIONS) {
          while (iter.hasNext()) {
            DeletedBlocksTransaction delTX = iter.next().getValue();
            if (delTX.getCount() == -1) {
              failedTXs.add(delTX);
            }
          }
        } else {
          iter.seek(startTxId);
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
    final int batchSize = 1000;
    int totalProcessed = 0;

    try {
      if (txIDs != null && !txIDs.isEmpty()) {
        return resetRetryCount(txIDs);
      }

      // If txIDs are null or empty, fetch all failed transactions in batches
      long startTxId = 0;
      List<DeletedBlocksTransaction> batch;

      do {
        // Fetch the batch of failed transactions
        batch = getFailedTransactions(batchSize, startTxId);
        if (batch.isEmpty()) {
          break;
        }

        List<Long> batchTxIDs = batch.stream().map(DeletedBlocksTransaction::getTxID).collect(Collectors.toList());
        totalProcessed += resetRetryCount(new ArrayList<>(batchTxIDs));
        // Update startTxId to continue from the last processed transaction
        startTxId = batch.get(batch.size() - 1).getTxID() + 1;
      } while (!batch.isEmpty());

    } catch (Exception e) {
      throw new IOException("Error during transaction reset", e);
    }
    return totalProcessed;
  }

  private int resetRetryCount(List<Long> txIDs) throws IOException {
    int totalProcessed;
    lock.lock();
    try {
      transactionStatusManager.resetRetryCount(txIDs);
      totalProcessed = deletedBlockLogStateManager.resetRetryCountOfTransactionInDB(new ArrayList<>(
          txIDs));
    } finally {
      lock.unlock();
    }
    return totalProcessed;
  }

  private DeletedBlocksTransaction constructNewTransaction(
      long txID, long containerID, List<DeletedBlock> blocks) {
    List<Long> localIdList = blocks.stream().map(b -> b.getBlockID().getLocalID()).collect(Collectors.toList());
    DeletedBlocksTransaction.Builder builder = DeletedBlocksTransaction.newBuilder()
        .setTxID(txID)
        .setContainerID(containerID)
        .addAllLocalID(localIdList)
        .setCount(0);

    if (VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.DATA_DISTRIBUTION)) {
      long replicatedSize = blocks.stream().mapToLong(DeletedBlock::getReplicatedSize).sum();
      // even when HDDSLayoutFeature.DATA_DISTRIBUTION is finalized, old OM can still call the old API
      if (replicatedSize >= 0) {
        builder.setTotalBlockReplicatedSize(replicatedSize);
        builder.setTotalBlockSize(blocks.stream().mapToLong(DeletedBlock::getSize).sum());
      }
    }
    return builder.build();
  }

  @Override
  public int getNumOfValidTransactions() throws IOException {
    lock.lock();
    try {
      final AtomicInteger num = new AtomicInteger(0);
      try (Table.KeyValueIterator<Long, DeletedBlocksTransaction> iter =
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
      Table<Long, DeletedBlocksTransaction> deletedTable, Table<String, ByteString> statefulConfigTable) {
    // we don't need to handle SCMDeletedBlockTransactionStatusManager and
    // deletedBlockLogStateManager, since they will be cleared
    // when becoming leader.
    txSizeMap.clear();
    deletedBlockLogStateManager.reinitialize(deletedTable, statefulConfigTable);
  }

  /**
   * Called in SCMStateMachine#notifyLeaderChanged when current SCM becomes
   *  leader.
   */
  public void onBecomeLeader() {
    transactionStatusManager.clear();
    txSizeMap.clear();
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
  public void addTransactions(Map<Long, List<DeletedBlock>> containerBlocksMap)
      throws IOException {
    lock.lock();
    try {
      ArrayList<DeletedBlocksTransaction> txsToBeAdded = new ArrayList<>();
      long currentBatchSizeBytes = 0;
      for (Map.Entry<Long, List<DeletedBlock>> entry :
          containerBlocksMap.entrySet()) {
        long nextTXID = sequenceIdGen.getNextId(DEL_TXN_ID);
        DeletedBlocksTransaction tx = constructNewTransaction(nextTXID,
            entry.getKey(), entry.getValue());
        txsToBeAdded.add(tx);
        long txSize = tx.getSerializedSize();
        currentBatchSizeBytes += txSize;

        if (currentBatchSizeBytes >= logAppenderQueueByteLimit) {
          deletedBlockLogStateManager.addTransactionsToDB(txsToBeAdded);
          metrics.incrBlockDeletionTransactionCreated(txsToBeAdded.size());
          txsToBeAdded.clear();
          currentBatchSizeBytes = 0;
        }
      }
      if (!txsToBeAdded.isEmpty()) {
        deletedBlockLogStateManager.addTransactionsToDB(txsToBeAdded);
        metrics.incrBlockDeletionTransactionCreated(txsToBeAdded.size());
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() {
  }

  private void getTransaction(DeletedBlocksTransaction tx,
      DatanodeDeletedBlockTransactions transactions,
      Set<ContainerReplica> replicas,
      Map<DatanodeID, Map<Long, CmdStatus>> commandStatus) {
    DeletedBlocksTransaction updatedTxn =
        DeletedBlocksTransaction.newBuilder(tx)
            .setCount(transactionStatusManager.getRetryCount(tx.getTxID()))
            .build();
    boolean flag = false;
    for (ContainerReplica replica : replicas) {
      final DatanodeID datanodeID = replica.getDatanodeDetails().getID();
      if (!transactionStatusManager.isDuplication(
          datanodeID, tx.getTxID(), commandStatus)) {
        transactions.addTransactionToDN(datanodeID, updatedTxn);
        flag = true;
      }
    }
    if (flag) {
      metrics.incrProcessedTransaction();
    }
  }

  private Boolean checkInadequateReplica(Set<ContainerReplica> replicas,
      DeletedBlocksTransaction txn,
      Set<DatanodeDetails> dnList) throws ContainerNotFoundException {
    ContainerInfo containerInfo = containerManager
        .getContainer(ContainerID.valueOf(txn.getContainerID()));
    ReplicationManager replicationManager =
        scmContext.getScm().getReplicationManager();
    ContainerHealthResult result = replicationManager
        .getContainerReplicationHealth(containerInfo, replicas);

    // We have made an improvement here, and we expect that all replicas
    // of the Container being sent will be included in the dnList.
    // This change benefits ACK confirmation and improves deletion speed.
    // The principle behind it is that
    // DN can receive the command to delete a certain Container at the same time and provide
    // feedback to SCM at roughly the same time.
    // This avoids the issue of deletion blocking,
    // where some replicas of a Container are deleted while others do not receive the delete command.
    long containerId = txn.getContainerID();
    for (ContainerReplica replica : replicas) {
      DatanodeDetails datanodeDetails = replica.getDatanodeDetails();
      if (!dnList.contains(datanodeDetails)) {
        DatanodeDetails dnDetail = replica.getDatanodeDetails();
        LOG.debug("Skip Container = {}, because DN = {} is not in dnList.",
            containerId, dnDetail);
        return true;
      }
    }

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
      try (Table.KeyValueIterator<Long, DeletedBlocksTransaction> iter =
               deletedBlockLogStateManager.getReadOnlyIterator()) {
        if (lastProcessedTransactionId != -1) {
          iter.seek(lastProcessedTransactionId);
          /*
           * We should start from (lastProcessedTransactionId + 1) transaction.
           * Now the iterator (iter.next call) is pointing at
           * lastProcessedTransactionId, read the current value to move
           * the cursor.
           */
          if (iter.hasNext()) {
            /*
             * There is a possibility that the lastProcessedTransactionId got
             * deleted from the table, in that case we have to set
             * lastProcessedTransactionId to next available transaction in the table.
             *
             * By doing this there is a chance that we will skip processing the new
             * lastProcessedTransactionId, that should be ok. We can get to it in the
             * next run.
             */
            lastProcessedTransactionId = iter.next().getKey();
          }

          // If we have reached the end, go to beginning.
          if (!iter.hasNext()) {
            iter.seekToFirst();
            lastProcessedTransactionId = -1;
          }
        }

        // Get the CmdStatus status of the aggregation, so that the current
        // status of the specified transaction can be found faster
        final Map<DatanodeID, Map<Long, CmdStatus>> commandStatus =
            getSCMDeletedBlockTransactionStatusManager()
                .getCommandStatusByTxId(dnList.stream().
                map(DatanodeDetails::getID).collect(Collectors.toSet()));
        ArrayList<Long> txIDs = new ArrayList<>();
        metrics.setNumBlockDeletionTransactionDataNodes(dnList.size());
        Table.KeyValue<Long, DeletedBlocksTransaction> keyValue = null;
        // Here takes block replica count as the threshold to avoid the case
        // that part of replicas committed the TXN and recorded in the
        // SCMDeletedBlockTransactionStatusManager, while they are counted
        // in the threshold.
        while (iter.hasNext() &&
            transactions.getBlocksDeleted() < blockDeletionLimit) {
          keyValue = iter.next();
          DeletedBlocksTransaction txn = keyValue.getValue();
          final ContainerID id = ContainerID.valueOf(txn.getContainerID());
          if (VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.DATA_DISTRIBUTION) &&
              txn.hasTotalBlockReplicatedSize()) {
            txSizeMap.put(txn.getTxID(),
                new TxBlockInfo(txn.getLocalIDCount(), txn.getTotalBlockSize(), txn.getTotalBlockReplicatedSize()));
          }
          try {
            // HDDS-7126. When container is under replicated, it is possible
            // that container is deleted, but transactions are not deleted.
            if (containerManager.getContainer(id).isDeleted()) {
              LOG.warn("Container: {} was deleted for the " +
                  "transaction: {}.", id, txn);
              txIDs.add(txn.getTxID());
            } else if (txn.getCount() > -1 && txn.getCount() <= maxRetry
                && !containerManager.getContainer(id).isOpen()) {
              Set<ContainerReplica> replicas = containerManager
                  .getContainerReplicas(
                      ContainerID.valueOf(txn.getContainerID()));
              if (checkInadequateReplica(replicas, txn, dnList)) {
                metrics.incrSkippedTransaction();
                continue;
              }
              getTransaction(txn, transactions, replicas, commandStatus);
            } else if (txn.getCount() >= maxRetry || containerManager.getContainer(id).isOpen()) {
              metrics.incrSkippedTransaction();
            }
          } catch (ContainerNotFoundException ex) {
            LOG.warn("Container: {} was not found for the transaction: {}.", id, txn);
            txIDs.add(txn.getTxID());
          }

          if (lastProcessedTransactionId == keyValue.getKey()) {
            // We have circled back to the last transaction.
            break;
          }

          if (!iter.hasNext() && lastProcessedTransactionId != -1) {
            /*
             * We started from in-between and reached end of the table,
             * now we should go to the start of the table and process
             * the transactions.
             */
            iter.seekToFirst();
          }
        }

        lastProcessedTransactionId = keyValue != null ? keyValue.getKey() : -1;

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
  public void recordTransactionCreated(DatanodeID dnId, long scmCmdId,
      Set<Long> dnTxSet) {
    getSCMDeletedBlockTransactionStatusManager()
        .recordTransactionCreated(dnId, scmCmdId, dnTxSet);
  }

  @Override
  public int getTransactionToDNsCommitMapSize() {
    return getSCMDeletedBlockTransactionStatusManager().getTransactionToDNsCommitMapSize();
  }

  @Override
  public DeletedBlocksTransactionSummary getTransactionSummary() {
    return deletedBlockLogStateManager.getTransactionSummary();
  }

  @Override
  public boolean isTransactionSummarySupported() {
    return VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.DATA_DISTRIBUTION);
  }

  @Override
  public void onDatanodeDead(DatanodeID dnId) {
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
    DatanodeID dnId = details.getID();
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
          metrics.incrDNCommandsSuccess(dnId, 1);
        } else if (status == CommandStatus.Status.FAILED) {
          metrics.incrBlockDeletionCommandFailure();
          metrics.incrDNCommandsFailure(dnId, 1);
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

  /**
   * Block size information of a transaction.
   */
  public static class TxBlockInfo {
    private long totalBlockCount;
    private long totalBlockSize;
    private long totalReplicatedBlockSize;

    public TxBlockInfo(long blockCount, long blockSize, long replicatedSize) {
      this.totalBlockCount = blockCount;
      this.totalBlockSize = blockSize;
      this.totalReplicatedBlockSize = replicatedSize;
    }

    public long getTotalBlockCount() {
      return totalBlockCount;
    }

    public long getTotalBlockSize() {
      return totalBlockSize;
    }

    public long getTotalReplicatedBlockSize() {
      return totalReplicatedBlockSize;
    }
  }
}
