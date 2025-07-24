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

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.ha.ReflectionUtil;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link DeletedBlockLogStateManager} implementation
 * based on {@link DeletedBlocksTransaction}.
 */
public class DeletedBlockLogStateManagerImpl
    implements DeletedBlockLogStateManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeletedBlockLogStateManagerImpl.class);
  public static final String SERVICE_NAME = DeletedBlockLogStateManager.class.getSimpleName();
  public static final HddsProtos.DeletedBlocksTransactionSummary EMPTY_SUMMARY =
      HddsProtos.DeletedBlocksTransactionSummary.newBuilder()
          .setFirstTxID(Long.MAX_VALUE)
          .setTotalTransactionCount(0)
          .setTotalBlockCount(0)
          .setTotalBlockSize(0)
          .setTotalBlockReplicatedSize(0)
          .build();

  private Table<Long, DeletedBlocksTransaction> deletedTable;
  private Table<String, ByteString> statefulConfigTable;
  private ContainerManager containerManager;
  private final DBTransactionBuffer transactionBuffer;
  private final Set<Long> deletingTxIDs;
  private final Set<Long> skippingRetryTxIDs;
  private final AtomicLong totalTxCount = new AtomicLong(0);
  private final AtomicLong totalBlockCount = new AtomicLong(0);
  private final AtomicLong totalBlocksSize = new AtomicLong(0);
  private final AtomicLong totalReplicatedBlocksSize = new AtomicLong(0);
  private final Map<Long, DeletedBlockLogImpl.TxBlockInfo> txBlockInfoMap;
  private long firstTxIdForDataDistribution = Long.MAX_VALUE;
  private final ScmBlockDeletingServiceMetrics metrics;
  private boolean isFirstTxIdForDataDistributionSet = false;

  public DeletedBlockLogStateManagerImpl(ConfigurationSource conf,
             Table<Long, DeletedBlocksTransaction> deletedTable,
             Table<String, ByteString> statefulServiceConfigTable,
             ContainerManager containerManager, DBTransactionBuffer txBuffer,
             Map<Long, DeletedBlockLogImpl.TxBlockInfo> txBlockInfoMap,
             ScmBlockDeletingServiceMetrics metrics) throws IOException {
    this.deletedTable = deletedTable;
    this.containerManager = containerManager;
    this.transactionBuffer = txBuffer;
    this.deletingTxIDs = ConcurrentHashMap.newKeySet();
    this.skippingRetryTxIDs = ConcurrentHashMap.newKeySet();
    this.statefulConfigTable = statefulServiceConfigTable;
    this.txBlockInfoMap = txBlockInfoMap;
    this.metrics = metrics;
    this.initDataDistributionData();
  }

  @Override
  public Table.KeyValueIterator<Long, DeletedBlocksTransaction> getReadOnlyIterator()
      throws IOException {
    return new Table.KeyValueIterator<Long, DeletedBlocksTransaction>() {

      private final Table.KeyValueIterator<Long, DeletedBlocksTransaction> iter = deletedTable.iterator();
      private TypedTable.KeyValue<Long, DeletedBlocksTransaction> nextTx;

      {
        findNext();
      }

      private void findNext() {
        while (iter.hasNext()) {
          TypedTable.KeyValue<Long, DeletedBlocksTransaction> next = iter
              .next();
          final long txID = next.getKey();

          if ((deletingTxIDs == null || !deletingTxIDs.contains(txID)) && (
              skippingRetryTxIDs == null || !skippingRetryTxIDs
                  .contains(txID))) {
            nextTx = next;
            if (LOG.isTraceEnabled()) {
              LOG.trace("DeletedBlocksTransaction matching txID:{}",
                  txID);
            }
            return;
          }
        }
        nextTx = null;
      }

      @Override
      public boolean hasNext() {
        return nextTx != null;
      }

      @Override
      public TypedTable.KeyValue<Long, DeletedBlocksTransaction> next() {
        if (nextTx == null) {
          throw new NoSuchElementException("DeletedBlocksTransaction " +
              "Iterator reached end");
        }
        TypedTable.KeyValue<Long, DeletedBlocksTransaction> returnTx = nextTx;
        findNext();
        return returnTx;
      }

      @Override
      public void close() throws RocksDatabaseException {
        iter.close();
      }

      @Override
      public void seekToFirst() {
        iter.seekToFirst();
        findNext();
      }

      @Override
      public void seekToLast() {
        throw new UnsupportedOperationException("seekToLast");
      }

      @Override
      public TypedTable.KeyValue<Long, DeletedBlocksTransaction> seek(Long key)
          throws RocksDatabaseException, CodecException {
        iter.seek(key);
        findNext();
        return nextTx;
      }

      @Override
      public void removeFromDB() {
        throw new UnsupportedOperationException("read-only");
      }
    };
  }

  @Override
  public void addTransactionsToDB(ArrayList<DeletedBlocksTransaction> txs)
      throws IOException {
    Map<ContainerID, Long> containerIdToTxnIdMap = new HashMap<>();
    for (DeletedBlocksTransaction tx : txs) {
      long tid = tx.getTxID();
      containerIdToTxnIdMap.compute(ContainerID.valueOf(tx.getContainerID()),
          (k, v) -> v != null && v > tid ? v : tid);
      transactionBuffer.addToBuffer(deletedTable, tx.getTxID(), tx);
      if (VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.DATA_DISTRIBUTION) &&
          tx.hasTotalBlockReplicatedSize()) {
        if (!isFirstTxIdForDataDistributionSet) {
          // set the first transaction ID for data distribution
          isFirstTxIdForDataDistributionSet = true;
          firstTxIdForDataDistribution = tx.getTxID();
        }
        transactionBuffer.addToBuffer(statefulConfigTable, SERVICE_NAME,
            incrDeletedBlocksSummary(tx).toByteString());
      }
    }

    containerManager.updateDeleteTransactionId(containerIdToTxnIdMap);
  }

  private DeletedBlocksTransactionSummary incrDeletedBlocksSummary(DeletedBlocksTransaction tx) {
    totalTxCount.addAndGet(1);
    totalBlockCount.addAndGet(tx.getLocalIDCount());
    totalBlocksSize.addAndGet(tx.getTotalBlockSize());
    totalReplicatedBlocksSize.addAndGet(tx.getTotalBlockReplicatedSize());
    return DeletedBlocksTransactionSummary.newBuilder()
        .setFirstTxID(firstTxIdForDataDistribution)
        .setTotalTransactionCount(totalTxCount.get())
        .setTotalBlockCount(totalBlockCount.get())
        .setTotalBlockSize(totalBlocksSize.get())
        .setTotalBlockReplicatedSize(totalReplicatedBlocksSize.get())
        .build();
  }

  @Override
  public void removeTransactionsFromDB(ArrayList<Long> txIDs)
      throws IOException {
    if (deletingTxIDs != null) {
      deletingTxIDs.addAll(txIDs);
    }

    for (Long txID : txIDs) {
      transactionBuffer.removeFromBuffer(deletedTable, txID);
      if (VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.DATA_DISTRIBUTION) &&
          txID >= firstTxIdForDataDistribution) {
        DeletedBlockLogImpl.TxBlockInfo txBlockInfo = txBlockInfoMap.remove(txID);
        if (txBlockInfo != null) {
          transactionBuffer.addToBuffer(statefulConfigTable, SERVICE_NAME,
              descDeletedBlocksSummary(txBlockInfo).toByteString());
          metrics.incrBlockDeletionTransactionSizeFromCache();
        } else {
          // Fetch the transaction from DB to get the size. This happens during
          // 1. SCM leader transfer, deletion command send by one SCM,
          //    while the deletion ack received by a different SCM
          // 2. SCM restarts, txBlockInfoMap is empty, while receiving the deletion ack from DN
          DeletedBlocksTransaction tx = deletedTable.get(txID);
          if (tx != null && tx.hasTotalBlockReplicatedSize()) {
            transactionBuffer.addToBuffer(statefulConfigTable, SERVICE_NAME,
                descDeletedBlocksSummary(tx).toByteString());
          }
          metrics.incrBlockDeletionTransactionSizeFromDB();
        }
      }
    }
  }

  private DeletedBlocksTransactionSummary descDeletedBlocksSummary(DeletedBlocksTransaction tx) {
    totalTxCount.addAndGet(-1);
    totalBlockCount.addAndGet(-tx.getLocalIDCount());
    totalBlocksSize.addAndGet(-tx.getTotalBlockSize());
    totalReplicatedBlocksSize.addAndGet(-tx.getTotalBlockReplicatedSize());
    return DeletedBlocksTransactionSummary.newBuilder()
        .setFirstTxID(firstTxIdForDataDistribution)
        .setTotalTransactionCount(totalTxCount.get())
        .setTotalBlockCount(totalBlockCount.get())
        .setTotalBlockSize(totalBlocksSize.get())
        .setTotalBlockReplicatedSize(totalReplicatedBlocksSize.get())
        .build();
  }

  private DeletedBlocksTransactionSummary descDeletedBlocksSummary(DeletedBlockLogImpl.TxBlockInfo txBlockInfo) {
    totalTxCount.addAndGet(-1);
    totalBlockCount.addAndGet(-txBlockInfo.getTotalBlockCount());
    totalBlocksSize.addAndGet(-txBlockInfo.getTotalBlockSize());
    totalReplicatedBlocksSize.addAndGet(-txBlockInfo.getTotalReplicatedBlockSize());
    return DeletedBlocksTransactionSummary.newBuilder()
        .setFirstTxID(firstTxIdForDataDistribution)
        .setTotalTransactionCount(totalTxCount.get())
        .setTotalBlockCount(totalBlockCount.get())
        .setTotalBlockSize(totalBlocksSize.get())
        .setTotalBlockReplicatedSize(totalReplicatedBlocksSize.get())
        .build();
  }

  @Override
  public void increaseRetryCountOfTransactionInDB(
      ArrayList<Long> txIDs) throws IOException {
    for (Long txID : txIDs) {
      DeletedBlocksTransaction block =
          deletedTable.get(txID);
      if (block == null) {
        if (LOG.isDebugEnabled()) {
          // This can occur due to race condition between retry and old
          // service task where old task removes the transaction and the new
          // task is resending
          LOG.debug("Deleted TXID {} not found.", txID);
        }
        continue;
      }
      // if the retry time exceeds the maxRetry value
      // then set the retry value to -1, stop retrying, admins can
      // analyze those blocks and purge them manually by SCMCli.
      DeletedBlocksTransaction.Builder builder = block.toBuilder().setCount(-1);
      transactionBuffer.addToBuffer(deletedTable, txID, builder.build());
      if (skippingRetryTxIDs != null) {
        skippingRetryTxIDs.add(txID);
      }
    }
  }

  @Override
  public int resetRetryCountOfTransactionInDB(ArrayList<Long> txIDs)
      throws IOException {
    Objects.requireNonNull(txIDs, "txIds cannot be null.");
    int resetCount = 0;
    for (long txId: txIDs) {
      try {
        DeletedBlocksTransaction transaction = deletedTable.get(txId);
        if (transaction == null) {
          LOG.warn("txId {} is not found in deletedTable.", txId);
          continue;
        }
        if (transaction.getCount() != -1) {
          LOG.warn("txId {} has already been reset in deletedTable.", txId);
          continue;
        }
        transactionBuffer.addToBuffer(deletedTable, txId,
            transaction.toBuilder().setCount(0).build());
        resetCount += 1;
        if (LOG.isDebugEnabled()) {
          LOG.info("Reset deleted block Txn retry count to 0 in container {}" +
              " with txnId {} ", transaction.getContainerID(), txId);
        }
      } catch (IOException ex) {
        LOG.error("Could not reset deleted block transaction {}.", txId, ex);
        throw ex;
      }
    }
    LOG.info("Reset in total {} deleted block Txn retry count", resetCount);
    return resetCount;
  }

  @Override
  public void onFlush() {
    // onFlush() can be invoked only when ratis is enabled.
    Preconditions.checkNotNull(deletingTxIDs);
    Preconditions.checkNotNull(skippingRetryTxIDs);
    deletingTxIDs.clear();
    skippingRetryTxIDs.clear();
  }

  @Override
  public void reinitialize(
      Table<Long, DeletedBlocksTransaction> deletedBlocksTXTable, Table<String, ByteString> configTable) {
    // Before Reinitialization, flush will be called from Ratis StateMachine.
    // Just the DeletedDb will be loaded here.

    // We don't need to handle transactionBuffer, deletingTxIDs
    // and skippingRetryTxIDs here, since onFlush() will be called
    // before reinitialization. Just update deletedTable here.
    Preconditions.checkArgument(deletingTxIDs.isEmpty());
    this.deletedTable = deletedBlocksTXTable;
    this.statefulConfigTable = configTable;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public DeletedBlocksTransactionSummary getTransactionSummary() {
    return DeletedBlocksTransactionSummary.newBuilder()
        .setFirstTxID(firstTxIdForDataDistribution)
        .setTotalTransactionCount(totalTxCount.get())
        .setTotalBlockCount(totalBlockCount.get())
        .setTotalBlockSize(totalBlocksSize.get())
        .setTotalBlockReplicatedSize(totalReplicatedBlocksSize.get())
        .build();
  }

  private void initDataDistributionData() throws IOException {
    if (VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.DATA_DISTRIBUTION)) {
      DeletedBlocksTransactionSummary summary = loadDeletedBlocksSummary();
      if (summary != null) {
        isFirstTxIdForDataDistributionSet = true;
        firstTxIdForDataDistribution = summary.getFirstTxID();
        totalTxCount.set(summary.getTotalTransactionCount());
        totalBlockCount.set(summary.getTotalBlockCount());
        totalBlocksSize.set(summary.getTotalBlockSize());
        totalReplicatedBlocksSize.set(summary.getTotalBlockReplicatedSize());
        LOG.info("Data distribution is enabled with totalBlockCount {} totalBlocksSize {} lastTxIdBeforeUpgrade {}",
            totalBlockCount.get(), totalBlocksSize.get(), firstTxIdForDataDistribution);
      }
    } else {
      LOG.info(HDDSLayoutFeature.DATA_DISTRIBUTION + " is not finalized");
    }
  }

  private DeletedBlocksTransactionSummary loadDeletedBlocksSummary() throws IOException {
    try {
      ByteString byteString = statefulConfigTable.get(SERVICE_NAME);
      if (byteString == null) {
        // for a new Ozone cluster, property not found is an expected state.
        LOG.info("Property {} for service {} not found. ",
            DeletedBlocksTransactionSummary.class.getSimpleName(), SERVICE_NAME);
        return null;
      }
      return DeletedBlocksTransactionSummary.class.cast(ReflectionUtil.getMethod(
          DeletedBlocksTransactionSummary.class, "parseFrom", ByteString.class).invoke(null, byteString));
    } catch (IOException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      LOG.error("Failed to get property {} for service {}. DataDistribution function will be disabled.",
          DeletedBlocksTransactionSummary.class.getSimpleName(), SERVICE_NAME, e);
      throw new IOException("Failed to get property " + DeletedBlocksTransactionSummary.class.getSimpleName(), e);
    }
  }

  /**
   * Builder for ContainerStateManager.
   */
  public static class Builder {
    private ConfigurationSource conf;
    private SCMRatisServer scmRatisServer;
    private Table<Long, DeletedBlocksTransaction> deletedBlocksTransactionTable;
    private DBTransactionBuffer transactionBuffer;
    private ContainerManager containerManager;
    private Map<Long, DeletedBlockLogImpl.TxBlockInfo> txBlockInfoMap;
    private Table<String, ByteString> statefulServiceConfigTable;
    private ScmBlockDeletingServiceMetrics scmBlockDeletingServiceMetrics;

    public Builder setConfiguration(final ConfigurationSource config) {
      conf = config;
      return this;
    }

    public Builder setRatisServer(final SCMRatisServer ratisServer) {
      scmRatisServer = ratisServer;
      return this;
    }

    public Builder setDeletedBlocksTable(
        final Table<Long, DeletedBlocksTransaction> deletedBlocksTable) {
      deletedBlocksTransactionTable = deletedBlocksTable;
      return this;
    }

    public Builder setSCMDBTransactionBuffer(DBTransactionBuffer buffer) {
      this.transactionBuffer = buffer;
      return this;
    }

    public Builder setContainerManager(ContainerManager contManager) {
      this.containerManager = contManager;
      return this;
    }

    public Builder setTxBlockInfoMap(Map<Long, DeletedBlockLogImpl.TxBlockInfo> map) {
      this.txBlockInfoMap = map;
      return this;
    }

    public Builder setStatefulConfigTable(final Table<String, ByteString> table) {
      this.statefulServiceConfigTable = table;
      return this;
    }

    public Builder setMetrics(ScmBlockDeletingServiceMetrics metrics) {
      this.scmBlockDeletingServiceMetrics = metrics;
      return this;
    }

    public DeletedBlockLogStateManager build() throws IOException {
      Preconditions.checkNotNull(conf);
      Preconditions.checkNotNull(deletedBlocksTransactionTable);

      final DeletedBlockLogStateManager impl = new DeletedBlockLogStateManagerImpl(
          conf, deletedBlocksTransactionTable, statefulServiceConfigTable, containerManager, transactionBuffer,
          txBlockInfoMap, scmBlockDeletingServiceMetrics);

      return scmRatisServer.getProxyHandler(RequestType.BLOCK,
          DeletedBlockLogStateManager.class, impl);
    }
  }
}
