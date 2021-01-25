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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.Set;
import java.util.Map;
import java.util.LinkedHashSet;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto.DeleteBlockTransactionResult;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.command.CommandStatusReportHandler.DeleteBlockStatus;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;

import com.google.common.collect.Lists;
import static java.lang.Math.min;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT;
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
  private static final DeletedBlocksTransaction.Builder DUMMY_TXN_BUILDER =
      DeletedBlocksTransaction.newBuilder().setContainerID(1).setCount(1);

  private final int maxRetry;
  private final ContainerManager containerManager;
  private final SCMMetadataStore scmMetadataStore;
  private final Lock lock;
  // Maps txId to set of DNs which are successful in committing the transaction
  private Map<Long, Set<UUID>> transactionToDNsCommitMap;

  private final AtomicLong largestTxnId;
  // largest transactionId is stored at largestTxnIdHolderKey
  private final long largestTxnIdHolderKey = 0L;


  public DeletedBlockLogImpl(ConfigurationSource conf,
                             ContainerManager containerManager,
                             SCMMetadataStore scmMetadataStore)
      throws IOException {
    maxRetry = conf.getInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY,
        OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT);
    this.containerManager = containerManager;
    this.scmMetadataStore = scmMetadataStore;
    this.lock = new ReentrantLock();

    // transactionToDNsCommitMap is updated only when
    // transaction is added to the log and when it is removed.

    // maps transaction to dns which have committed it.
    transactionToDNsCommitMap = new ConcurrentHashMap<>();

    this.largestTxnId = new AtomicLong(this.getLargestRecordedTXID());
  }

  public Long getNextDeleteBlockTXID() {
    return this.largestTxnId.incrementAndGet();
  }

  public Long getCurrentTXID() {
    return this.largestTxnId.get();
  }

  /**
   * Returns the largest recorded TXID from the DB.
   *
   * @return Long
   * @throws IOException
   */
  private long getLargestRecordedTXID() throws IOException {
    DeletedBlocksTransaction txn =
        scmMetadataStore.getDeletedBlocksTXTable().get(largestTxnIdHolderKey);
    long txnId = txn != null ? txn.getTxID() : 0L;
    if (txn == null) {
      // HDDS-4477 adds largestTxnIdHolderKey to table for storing largest
      // transactionId. In case the key does not exist, fetch largest
      // transactionId from existing transactions and update
      // largestTxnIdHolderKey with same.
      try (TableIterator<Long,
              ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> txIter =
              getIterator()) {
        txIter.seekToLast();
        txnId = txIter.key() != null ? txIter.key() : 0L;
        if (txnId > 0) {
          scmMetadataStore.getDeletedBlocksTXTable().put(largestTxnIdHolderKey,
              DUMMY_TXN_BUILDER.setTxID(txnId).build());
        }
      }
    }
    return txnId;
  }

  @Override
  public List<DeletedBlocksTransaction> getFailedTransactions()
      throws IOException {
    lock.lock();
    try {
      final List<DeletedBlocksTransaction> failedTXs = Lists.newArrayList();
      try (TableIterator<Long,
          ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iter =
               getIterator()) {
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
  public void incrementCount(List<Long> txIDs) throws IOException {
    for (Long txID : txIDs) {
      lock.lock();
      try {
        DeletedBlocksTransaction block =
            scmMetadataStore.getDeletedBlocksTXTable().get(txID);
        if (block == null) {
          if (LOG.isDebugEnabled()) {
            // This can occur due to race condition between retry and old
            // service task where old task removes the transaction and the new
            // task is resending
            LOG.debug("Deleted TXID {} not found.", txID);
          }
          continue;
        }
        DeletedBlocksTransaction.Builder builder = block.toBuilder();
        int currentCount = block.getCount();
        if (currentCount > -1) {
          builder.setCount(++currentCount);
        }
        // if the retry time exceeds the maxRetry value
        // then set the retry value to -1, stop retrying, admins can
        // analyze those blocks and purge them manually by SCMCli.
        if (currentCount > maxRetry) {
          builder.setCount(-1);
        }
        scmMetadataStore.getDeletedBlocksTXTable().put(txID,
            builder.build());
      } catch (IOException ex) {
        LOG.warn("Cannot increase count for txID " + txID, ex);
        // We do not throw error here, since we don't want to abort the loop.
        // Just log and continue processing the rest of txids.
      } finally {
        lock.unlock();
      }
    }
  }


  private DeletedBlocksTransaction constructNewTransaction(long txID,
                                                           long containerID,
                                                           List<Long> blocks) {
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
      Set<UUID> dnsWithCommittedTxn;
      for (DeleteBlockTransactionResult transactionResult :
          transactionResults) {
        if (isTransactionFailed(transactionResult)) {
          continue;
        }
        try {
          long txID = transactionResult.getTxID();
          // set of dns which have successfully committed transaction txId.
          dnsWithCommittedTxn = transactionToDNsCommitMap.get(txID);
          final ContainerID containerId = ContainerID.valueof(
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
              >= container.getReplicationFactor().getNumber()) {
            List<UUID> containerDns = replicas.stream()
                .map(ContainerReplica::getDatanodeDetails)
                .map(DatanodeDetails::getUuid)
                .collect(Collectors.toList());
            if (dnsWithCommittedTxn.containsAll(containerDns)) {
              transactionToDNsCommitMap.remove(txID);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Purging txId={} from block deletion log", txID);
              }
              scmMetadataStore.getDeletedBlocksTXTable().delete(txID);
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

  /**
   * {@inheritDoc}
   *
   * @param containerID - container ID.
   * @param blocks      - blocks that belong to the same container.
   * @throws IOException
   */
  @Override
  public void addTransaction(long containerID, List<Long> blocks)
      throws IOException {
    Map<Long, List<Long>> map = Collections.singletonMap(containerID, blocks);
    addTransactions(map);
  }

  @Override
  public int getNumOfValidTransactions() throws IOException {
    lock.lock();
    try {
      final AtomicInteger num = new AtomicInteger(0);
      try (TableIterator<Long,
          ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iter =
               getIterator()) {
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
    try (BatchOperation batch = scmMetadataStore.getStore()
        .initBatchOperation()) {
      for (Map.Entry<Long, List<Long>> entry : containerBlocksMap.entrySet()) {
        long nextTXID = getNextDeleteBlockTXID();
        scmMetadataStore.getDeletedBlocksTXTable().putWithBatch(batch, nextTXID,
            constructNewTransaction(nextTXID, entry.getKey(),
                entry.getValue()));
      }
      // Add a dummy transaction to store the largestTransactionId at
      // largestTxnIdHolderKey
      scmMetadataStore.getDeletedBlocksTXTable()
          .putWithBatch(batch, largestTxnIdHolderKey,
              DUMMY_TXN_BUILDER.setTxID(getCurrentTXID()).build());
      scmMetadataStore.getStore().commitBatchOperation(batch);
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
          .getContainerReplicas(ContainerID.valueof(tx.getContainerID()));
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
      int blockDeletionLimit) throws IOException {
    lock.lock();
    try {
      DatanodeDeletedBlockTransactions transactions =
          new DatanodeDeletedBlockTransactions();
      try (TableIterator<Long,
          ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iter =
               getIterator()) {
        int numBlocksAdded = 0;
        List<DeletedBlocksTransaction> txnsToBePurged =
            new ArrayList<>();
        while (iter.hasNext() && numBlocksAdded < blockDeletionLimit) {
          Table.KeyValue<Long, DeletedBlocksTransaction> keyValue = iter.next();
          DeletedBlocksTransaction txn = keyValue.getValue();
          final ContainerID id = ContainerID.valueof(txn.getContainerID());
          try {
            if (txn.getCount() > -1 && txn.getCount() <= maxRetry
                && !containerManager.getContainer(id).isOpen()) {
              numBlocksAdded += txn.getLocalIDCount();
              getTransaction(txn, transactions);
              transactionToDNsCommitMap
                  .putIfAbsent(txn.getTxID(), new LinkedHashSet<>());
            }
          } catch (ContainerNotFoundException ex) {
            LOG.warn("Container: " + id + " was not found for the transaction: "
                + txn);
            txnsToBePurged.add(txn);
          }
        }
        purgeTransactions(txnsToBePurged);
      }
      return transactions;
    } finally {
      lock.unlock();
    }
  }

  public void purgeTransactions(List<DeletedBlocksTransaction> txnsToBePurged)
      throws IOException {
    try (BatchOperation batch = scmMetadataStore.getBatchHandler()
        .initBatchOperation()) {
      for (int i = 0; i < txnsToBePurged.size(); i++) {
        scmMetadataStore.getDeletedBlocksTXTable()
            .deleteWithBatch(batch, txnsToBePurged.get(i).getTxID());
      }
      scmMetadataStore.getBatchHandler().commitBatchOperation(batch);
    }
  }

  TableIterator<Long,
      ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> getIterator()
      throws IOException {
    TableIterator<Long,
        ? extends Table.KeyValue<Long, DeletedBlocksTransaction>>
        iter = scmMetadataStore.getDeletedBlocksTXTable().iterator();
    iter.seek(largestTxnIdHolderKey + 1);
    return iter;
  }

  @Override
  public void onMessage(DeleteBlockStatus deleteBlockStatus,
                        EventPublisher publisher) {
    ContainerBlocksDeletionACKProto ackProto =
        deleteBlockStatus.getCmdStatus().getBlockDeletionAck();
    commitTransactions(ackProto.getResultsList(),
        UUID.fromString(ackProto.getDnId()));
  }
}
