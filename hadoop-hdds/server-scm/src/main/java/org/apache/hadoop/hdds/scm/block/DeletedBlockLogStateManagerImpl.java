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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBuffer;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TypedTable;
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

  private Table<Long, DeletedBlocksTransaction> deletedTable;
  private Table<String, ByteString> statefulConfigTable;
  private ContainerManager containerManager;
  private final SCMHADBTransactionBuffer transactionBuffer;
  private final Set<Long> deletingTxIDs;
  public static final String SERVICE_NAME = DeletedBlockLogStateManager.class.getSimpleName();

  public DeletedBlockLogStateManagerImpl(Table<Long, DeletedBlocksTransaction> deletedTable,
             Table<String, ByteString> statefulServiceConfigTable,
             ContainerManager containerManager, SCMHADBTransactionBuffer txBuffer) {
    this.deletedTable = deletedTable;
    this.containerManager = containerManager;
    this.transactionBuffer = txBuffer;
    this.deletingTxIDs = ConcurrentHashMap.newKeySet();
    this.statefulConfigTable = statefulServiceConfigTable;
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
          final TypedTable.KeyValue<Long, DeletedBlocksTransaction> next = iter.next();
          final long txID = next.getKey();

          if ((!deletingTxIDs.contains(txID))) {
            nextTx = next;
            if (LOG.isTraceEnabled()) {
              LOG.trace("DeletedBlocksTransaction matching txID:{}", txID);
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
  public void addTransactionsToDB(ArrayList<DeletedBlocksTransaction> txs,
      DeletedBlocksTransactionSummary summary) throws IOException {
    Map<ContainerID, Long> containerIdToTxnIdMap = new HashMap<>();
    for (DeletedBlocksTransaction tx : txs) {
      long tid = tx.getTxID();
      containerIdToTxnIdMap.compute(ContainerID.valueOf(tx.getContainerID()),
          (k, v) -> v != null && v > tid ? v : tid);
      transactionBuffer.addToBuffer(deletedTable, tx.getTxID(), tx);
    }
    transactionBuffer.addToBuffer(statefulConfigTable, SERVICE_NAME, summary.toByteString());
    containerManager.updateDeleteTransactionId(containerIdToTxnIdMap);
  }

  @Override
  public void addTransactionsToDB(ArrayList<DeletedBlocksTransaction> txs) throws IOException {
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
  public void removeTransactionsFromDB(ArrayList<Long> txIDs, DeletedBlocksTransactionSummary summary)
      throws IOException {
    if (deletingTxIDs != null) {
      deletingTxIDs.addAll(txIDs);
    }
    for (Long txID : txIDs) {
      transactionBuffer.removeFromBuffer(deletedTable, txID);
    }
    transactionBuffer.addToBuffer(statefulConfigTable, SERVICE_NAME, summary.toByteString());
  }

  @Override
  public void removeTransactionsFromDB(ArrayList<Long> txIDs) throws IOException {
    if (deletingTxIDs != null) {
      deletingTxIDs.addAll(txIDs);
    }
    for (Long txID : txIDs) {
      transactionBuffer.removeFromBuffer(deletedTable, txID);
    }
  }

  @Deprecated
  @Override
  public void increaseRetryCountOfTransactionInDB(
      ArrayList<Long> txIDs) throws IOException {
    // We don't store retry count in DB anymore.
    // This method is being retained to ensure backward compatibility and prevent
    // issues during minor upgrades. It will be removed in the future, during a major release.
  }

  @Deprecated
  @Override
  public int resetRetryCountOfTransactionInDB(ArrayList<Long> txIDs)
      throws IOException {
    // We don't reset retry count anymore.
    // This method is being retained to ensure backward compatibility and prevent
    // issues during minor upgrades. It will be removed in the future, during a major release.
    return 0;
  }

  @Override
  public void onFlush() {
    // onFlush() can be invoked only when ratis is enabled.
    Objects.requireNonNull(deletingTxIDs, "deletingTxIDs == null");
    deletingTxIDs.clear();
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

  /**
   * Builder for ContainerStateManager.
   */
  public static class Builder {
    private SCMRatisServer scmRatisServer;
    private Table<Long, DeletedBlocksTransaction> deletedBlocksTransactionTable;
    private SCMHADBTransactionBuffer transactionBuffer;
    private ContainerManager containerManager;
    private Table<String, ByteString> statefulServiceConfigTable;

    public Builder setRatisServer(final SCMRatisServer ratisServer) {
      scmRatisServer = ratisServer;
      return this;
    }

    public Builder setDeletedBlocksTable(
        final Table<Long, DeletedBlocksTransaction> deletedBlocksTable) {
      deletedBlocksTransactionTable = deletedBlocksTable;
      return this;
    }

    public Builder setSCMDBTransactionBuffer(SCMHADBTransactionBuffer buffer) {
      this.transactionBuffer = buffer;
      return this;
    }

    public Builder setContainerManager(ContainerManager contManager) {
      this.containerManager = contManager;
      return this;
    }

    public Builder setStatefulConfigTable(final Table<String, ByteString> table) {
      this.statefulServiceConfigTable = table;
      return this;
    }

    public DeletedBlockLogStateManager build() throws IOException {
      Objects.requireNonNull(deletedBlocksTransactionTable, "deletedBlocksTransactionTable == null");

      final DeletedBlockLogStateManager impl = new DeletedBlockLogStateManagerImpl(
          deletedBlocksTransactionTable, statefulServiceConfigTable, containerManager, transactionBuffer);

      return scmRatisServer.getProxyHandler(RequestType.BLOCK,
          DeletedBlockLogStateManager.class, impl);
    }
  }
}
