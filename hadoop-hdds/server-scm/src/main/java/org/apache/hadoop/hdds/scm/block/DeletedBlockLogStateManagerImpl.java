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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link DeletedBlockLogStateManager} implementation
 * based on {@link DeletedBlocksTransaction}.
 */
public class DeletedBlockLogStateManagerImpl
    implements DeletedBlockLogStateManager {

  public static final Logger LOG =
      LoggerFactory.getLogger(DeletedBlockLogStateManagerImpl.class);

  private Table<Long, DeletedBlocksTransaction> deletedTable;
  private ContainerManager containerManager;
  private final DBTransactionBuffer transactionBuffer;
  private final Set<Long> deletingTxIDs;
  private final Set<Long> skippingRetryTxIDs;

  public DeletedBlockLogStateManagerImpl(ConfigurationSource conf,
             Table<Long, DeletedBlocksTransaction> deletedTable,
             ContainerManager containerManager, DBTransactionBuffer txBuffer) {
    this.deletedTable = deletedTable;
    this.containerManager = containerManager;
    this.transactionBuffer = txBuffer;
    final boolean isRatisEnabled = SCMHAUtils.isSCMHAEnabled(conf);
    this.deletingTxIDs = isRatisEnabled ? ConcurrentHashMap.newKeySet() : null;
    this.skippingRetryTxIDs =
        isRatisEnabled ? ConcurrentHashMap.newKeySet() : null;
  }

  public TableIterator<Long, TypedTable.KeyValue<Long,
      DeletedBlocksTransaction>> getReadOnlyIterator() {
    return new TableIterator<Long, TypedTable.KeyValue<Long,
        DeletedBlocksTransaction>>() {

      private TableIterator<Long,
          ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iter =
          deletedTable.iterator();
      private TypedTable.KeyValue<Long, DeletedBlocksTransaction> nextTx;

      {
        findNext();
      }

      private void findNext() {
        while (iter.hasNext()) {
          TypedTable.KeyValue<Long, DeletedBlocksTransaction> next = iter
              .next();
          long txID;
          try {
            txID = next.getKey();
          } catch (IOException e) {
            throw new IllegalStateException("");
          }

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
      public void close() throws IOException {
        iter.close();
      }

      @Override
      public void seekToFirst() {
        throw new UnsupportedOperationException("seekToFirst");
      }

      @Override
      public void seekToLast() {
        throw new UnsupportedOperationException("seekToLast");
      }

      @Override
      public TypedTable.KeyValue<Long, DeletedBlocksTransaction> seek(
          Long key) throws IOException {
        throw new UnsupportedOperationException("seek");
      }

      @Override
      public void removeFromDB() throws IOException {
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
    }
    containerManager.updateDeleteTransactionId(containerIdToTxnIdMap);
  }

  @Override
  public void removeTransactionsFromDB(ArrayList<Long> txIDs)
      throws IOException {
    if (deletingTxIDs != null) {
      deletingTxIDs.addAll(txIDs);
    }
    for (Long txID : txIDs) {
      transactionBuffer.removeFromBuffer(deletedTable, txID);
    }
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

  public void onFlush() {
    // onFlush() can be invoked only when ratis is enabled.
    Preconditions.checkNotNull(deletingTxIDs);
    Preconditions.checkNotNull(skippingRetryTxIDs);
    deletingTxIDs.clear();
    skippingRetryTxIDs.clear();
  }

  @Override
  public void reinitialize(
      Table<Long, DeletedBlocksTransaction> deletedBlocksTXTable) {
    // Before Reinitialization, flush will be called from Ratis StateMachine.
    // Just the DeletedDb will be loaded here.

    // We don't need to handle transactionBuffer, deletingTxIDs
    // and skippingRetryTxIDs here, since onFlush() will be called
    // before reinitialization. Just update deletedTable here.
    Preconditions.checkArgument(deletingTxIDs.isEmpty());
    this.deletedTable = deletedBlocksTXTable;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for ContainerStateManager.
   */
  public static class Builder {
    private ConfigurationSource conf;
    private SCMRatisServer scmRatisServer;
    private Table<Long, DeletedBlocksTransaction> table;
    private DBTransactionBuffer transactionBuffer;
    private ContainerManager containerManager;

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
      table = deletedBlocksTable;
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

    public DeletedBlockLogStateManager build() {
      Preconditions.checkNotNull(conf);
      Preconditions.checkNotNull(table);

      final DeletedBlockLogStateManager impl =
          new DeletedBlockLogStateManagerImpl(conf, table, containerManager,
              transactionBuffer);

      final SCMHAInvocationHandler invocationHandler =
          new SCMHAInvocationHandler(SCMRatisProtocol.RequestType.BLOCK,
              impl, scmRatisServer);

      return (DeletedBlockLogStateManager) Proxy.newProxyInstance(
          SCMHAInvocationHandler.class.getClassLoader(),
          new Class<?>[]{DeletedBlockLogStateManager.class},
          invocationHandler);
    }
  }
}
