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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_NOT_FOUND;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V1;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V2;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V3;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto.DeleteBlockTransactionResult;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.BlockDeletingServiceMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;
import org.apache.hadoop.ozone.container.common.helpers.DeletedContainerBlocksSummary;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.metadata.DeleteTransactionStore;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlockCommandStatus;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle block deletion commands.
 */
public class DeleteBlocksCommandHandler implements CommandHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeleteBlocksCommandHandler.class);

  private final ContainerSet containerSet;
  private final ConfigurationSource conf;
  private int invocationCount;
  private final ThreadPoolExecutor executor;
  private final LinkedBlockingQueue<DeleteCmdInfo> deleteCommandQueues;
  private final Daemon handlerThread;
  private final OzoneContainer ozoneContainer;
  private final BlockDeletingServiceMetrics blockDeleteMetrics;
  private final long tryLockTimeoutMs;
  private final Map<String, SchemaHandler> schemaHandlers;
  private final MutableRate opsLatencyMs;

  public DeleteBlocksCommandHandler(OzoneContainer container,
      ConfigurationSource conf, DatanodeConfiguration dnConf,
      String threadNamePrefix) {
    this.ozoneContainer = container;
    this.containerSet = container.getContainerSet();
    this.conf = conf;
    this.blockDeleteMetrics = BlockDeletingServiceMetrics.create();
    this.tryLockTimeoutMs = dnConf.getBlockDeleteMaxLockWaitTimeoutMs();
    schemaHandlers = new HashMap<>();
    schemaHandlers.put(SCHEMA_V1, this::markBlocksForDeletionSchemaV1);
    schemaHandlers.put(SCHEMA_V2, this::markBlocksForDeletionSchemaV2);
    schemaHandlers.put(SCHEMA_V3, this::markBlocksForDeletionSchemaV3);

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat(threadNamePrefix +
            "DeleteBlocksCommandHandlerThread-%d")
        .build();
    this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(
        dnConf.getBlockDeleteThreads(), threadFactory);
    this.deleteCommandQueues =
        new LinkedBlockingQueue<>(dnConf.getBlockDeleteQueueLimit());
    MetricsRegistry registry = new MetricsRegistry(
        DeleteBlocksCommandHandler.class.getSimpleName());
    this.opsLatencyMs = registry.newRate(SCMCommandProto.Type.deleteBlocksCommand + "Ms");
    long interval = dnConf.getBlockDeleteCommandWorkerInterval().toMillis();
    handlerThread = new Daemon(new DeleteCmdWorker(interval));
    handlerThread.start();
  }

  @Override
  public void handle(SCMCommand<?> command, OzoneContainer container,
      StateContext context, SCMConnectionManager connectionManager) {
    if (command.getType() != SCMCommandProto.Type.deleteBlocksCommand) {
      LOG.warn("Skipping handling command, expected command "
              + "type {} but found {}",
          SCMCommandProto.Type.deleteBlocksCommand, command.getType());
      return;
    }
    DeleteCmdInfo cmd = new DeleteCmdInfo((DeleteBlocksCommand) command,
        container, context, connectionManager);
    try {
      deleteCommandQueues.add(cmd);
    } catch (IllegalStateException e) {
      String dnId = context.getParent().getDatanodeDetails().getUuidString();
      Consumer<CommandStatus> updateFailure = (cmdStatus) -> {
        cmdStatus.markAsFailed();
        ContainerBlocksDeletionACKProto emptyACK =
            ContainerBlocksDeletionACKProto
                .newBuilder()
                .setDnId(dnId)
                .build();
        ((DeleteBlockCommandStatus)cmdStatus).setBlocksDeletionAck(emptyACK);
      };
      updateCommandStatus(cmd.getContext(), cmd.getCmd(), updateFailure, LOG);
      LOG.warn("Command is discarded because of the command queue is full");
    }
  }

  /**
   * The count returned here, is the count of queued SCM delete block commands.
   * We get at most one such command per heartbeat, but the command can contain
   * many blocks to delete.
   * @return The number of SCM delete block commands pending in the queue.
   */
  @Override
  public int getQueuedCount() {
    return deleteCommandQueues.size();
  }

  @Override
  public int getThreadPoolMaxPoolSize() {
    return executor.getMaximumPoolSize();
  }

  @Override
  public int getThreadPoolActivePoolSize() {
    return executor.getActiveCount();
  }

  /**
   * A delete command info.
   */
  public static final class DeleteCmdInfo {
    private DeleteBlocksCommand cmd;
    private StateContext context;
    private OzoneContainer container;
    private SCMConnectionManager connectionManager;

    public DeleteCmdInfo(DeleteBlocksCommand command, OzoneContainer container,
        StateContext context, SCMConnectionManager connectionManager) {
      this.cmd = command;
      this.context = context;
      this.container = container;
      this.connectionManager = connectionManager;
    }

    public DeleteBlocksCommand getCmd() {
      return this.cmd;
    }

    public StateContext getContext() {
      return this.context;
    }

    public OzoneContainer getContainer() {
      return this.container;
    }

    public SCMConnectionManager getConnectionManager() {
      return this.connectionManager;
    }
  }

  /**
   * This class represents the result of executing a delete block transaction.
   */
  public static final class DeleteBlockTransactionExecutionResult {
    private final DeleteBlockTransactionResult result;
    private final boolean lockAcquisitionFailed;

    public DeleteBlockTransactionExecutionResult(
        DeleteBlockTransactionResult result, boolean lockAcquisitionFailed) {
      this.result = result;
      this.lockAcquisitionFailed = lockAcquisitionFailed;
    }

    public DeleteBlockTransactionResult getResult() {
      return result;
    }

    public boolean isLockAcquisitionFailed() {
      return lockAcquisitionFailed;
    }
  }

  /**
   * Process delete commands.
   */
  public final class DeleteCmdWorker implements Runnable {

    private long intervalInMs;

    public DeleteCmdWorker(long interval) {
      this.intervalInMs = interval;
    }

    @VisibleForTesting
    public long getInterval() {
      return this.intervalInMs;
    }

    @Override
    public void run() {
      while (true) {
        while (!deleteCommandQueues.isEmpty()) {
          DeleteCmdInfo cmd = deleteCommandQueues.poll();
          try {
            processCmd(cmd);
          } catch (Throwable e) {
            LOG.error("taskProcess failed.", e);
          }
        }

        try {
          Thread.sleep(this.intervalInMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  /**
   * Process one delete transaction.
   */
  public final class ProcessTransactionTask implements
      Callable<DeleteBlockTransactionExecutionResult> {
    private DeletedBlocksTransaction tx;

    public ProcessTransactionTask(DeletedBlocksTransaction transaction) {
      this.tx = transaction;
    }

    @Override
    public DeleteBlockTransactionExecutionResult call() {
      DeleteBlockTransactionResult.Builder txResultBuilder =
          DeleteBlockTransactionResult.newBuilder();
      txResultBuilder.setTxID(tx.getTxID());
      long containerId = tx.getContainerID();
      boolean lockAcquisitionFailed = false;
      try {
        Container cont = containerSet.getContainer(containerId);
        if (cont == null) {
          throw new StorageContainerException("Unable to find the " +
              "container " + containerId, CONTAINER_NOT_FOUND);
        }

        ContainerType containerType = cont.getContainerType();
        switch (containerType) {
        case KeyValueContainer:
          KeyValueContainer keyValueContainer = (KeyValueContainer)cont;
          KeyValueContainerData containerData =
              keyValueContainer.getContainerData();
          if (keyValueContainer.
              writeLockTryLock(tryLockTimeoutMs, TimeUnit.MILLISECONDS)) {
            try {
              String schemaVersion = containerData
                  .getSupportedSchemaVersionOrDefault();
              if (getSchemaHandlers().containsKey(schemaVersion)) {
                schemaHandlers.get(schemaVersion).handle(containerData, tx);
              } else {
                throw new UnsupportedOperationException(
                    "Only schema version 1,2,3 are supported.");
              }
            } finally {
              keyValueContainer.writeUnlock();
            }
            txResultBuilder.setContainerID(containerId)
                .setSuccess(true);
          } else {
            lockAcquisitionFailed = true;
            txResultBuilder.setContainerID(containerId)
                .setSuccess(false);
          }
          break;
        default:
          LOG.error(
              "Delete Blocks Command Handler is not implemented for " +
                  "containerType {}", containerType);
        }
      } catch (IOException e) {
        LOG.warn("Failed to delete blocks for container={}, TXID={}",
            tx.getContainerID(), tx.getTxID(), e);
        txResultBuilder.setContainerID(containerId).setSuccess(false);
      } catch (InterruptedException e) {
        LOG.warn("InterruptedException while deleting blocks for " +
                "container={}, TXID={}", tx.getContainerID(), tx.getTxID(), e);
        Thread.currentThread().interrupt();
        txResultBuilder.setContainerID(containerId).setSuccess(false);
      } catch (Exception e) {
        LOG.error("Unexpected exception while deleting blocks for " +
                "container={}, TXID={}", tx.getContainerID(), tx.getTxID(), e);
        txResultBuilder.setContainerID(containerId).setSuccess(false);
      }
      return new DeleteBlockTransactionExecutionResult(
          txResultBuilder.build(), lockAcquisitionFailed);
    }
  }

  private void processCmd(DeleteCmdInfo cmd) {
    LOG.debug("Processing block deletion command.");
    ContainerBlocksDeletionACKProto blockDeletionACK = null;
    long startTime = Time.monotonicNow();
    boolean cmdExecuted = false;
    int successCount = 0, failedCount = 0;
    try {
      // move blocks to deleting state.
      // this is a metadata update, the actual deletion happens in another
      // recycling thread.
      List<DeletedBlocksTransaction> containerBlocks =
          cmd.getCmd().blocksTobeDeleted();
      blockDeleteMetrics.incrReceivedTransactionCount(containerBlocks.size());

      DeletedContainerBlocksSummary summary =
          DeletedContainerBlocksSummary.getFrom(containerBlocks);
      LOG.info("Summary of deleting container blocks, numOfTransactions={}, "
              + "numOfContainers={}, numOfBlocks={}, commandId={}.",
          summary.getNumOfTxs(),
          summary.getNumOfContainers(),
          summary.getNumOfBlocks(),
          cmd.getCmd().getId());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Start to delete container blocks, TXIDs={}",
            summary.getTxIDSummary());
      }
      blockDeleteMetrics.incrReceivedContainerCount(
          summary.getNumOfContainers());
      blockDeleteMetrics.incrReceivedRetryTransactionCount(
          summary.getNumOfRetryTxs());
      blockDeleteMetrics.incrReceivedBlockCount(summary.getNumOfBlocks());

      List<DeleteBlockTransactionResult> results =
          executeCmdWithRetry(containerBlocks);

      ContainerBlocksDeletionACKProto.Builder resultBuilder =
          ContainerBlocksDeletionACKProto.newBuilder().addAllResults(results);
      resultBuilder.setDnId(cmd.getContext().getParent().getDatanodeDetails().getUuidString());
      blockDeletionACK = resultBuilder.build();

      // Send ACK back to SCM as long as meta updated
      // TODO Or we should wait until the blocks are actually deleted?
      if (!containerBlocks.isEmpty()) {

        for (DeleteBlockTransactionResult result : blockDeletionACK.getResultsList()) {
          boolean success = result.getSuccess();

          if (LOG.isDebugEnabled()) {
            LOG.debug("TxId = {} : ContainerId = {} : {}", result.getTxID(),
                result.getContainerID(), success);
          }

          if (success) {
            ++successCount;
            blockDeleteMetrics.incrProcessedTransactionSuccessCount(1);
          } else {
            ++failedCount;
            blockDeleteMetrics.incrProcessedTransactionFailCount(1);
          }
        }
      }
      cmdExecuted = true;
    } finally {
      final ContainerBlocksDeletionACKProto deleteAck =
          blockDeletionACK;
      final boolean executedStatus = cmdExecuted;
      Consumer<CommandStatus> statusUpdater = (cmdStatus) -> {
        if (executedStatus) {
          cmdStatus.markAsExecuted();
        } else {
          cmdStatus.markAsFailed();
        }
        ((DeleteBlockCommandStatus)cmdStatus).setBlocksDeletionAck(deleteAck);
      };
      updateCommandStatus(cmd.getContext(), cmd.getCmd(), statusUpdater, LOG);
      long endTime = Time.monotonicNow();
      this.opsLatencyMs.add(endTime - startTime);
      LOG.info("Sending deletion ACK to SCM, successTransactionCount = {}," +
          "failedTransactionCount = {}, time elapsed = {}", successCount, failedCount, opsLatencyMs);
      invocationCount++;
    }
  }

  @VisibleForTesting
  public List<DeleteBlockTransactionResult> executeCmdWithRetry(
      List<DeletedBlocksTransaction> transactions) {
    List<DeleteBlockTransactionResult> results =
        new ArrayList<>(transactions.size());
    Map<Long, DeletedBlocksTransaction> idToTransaction =
        new HashMap<>(transactions.size());
    transactions.forEach(tx -> idToTransaction.put(tx.getTxID(), tx));
    List<DeletedBlocksTransaction> retryTransaction = new ArrayList<>();

    List<Future<DeleteBlockTransactionExecutionResult>> futures =
        submitTasks(transactions);
    // Wait for tasks to finish
    handleTasksResults(futures, result -> {
      if (result.isLockAcquisitionFailed()) {
        retryTransaction.add(idToTransaction.get(result.getResult().getTxID()));
      } else {
        results.add(result.getResult());
      }
    });

    idToTransaction.clear();
    // Wait for all tasks to complete before retrying, usually it takes
    // some time for all tasks to complete, then the retry may be successful.
    // We will only retry once
    if (!retryTransaction.isEmpty()) {
      futures = submitTasks(retryTransaction);
      handleTasksResults(futures, result -> {
        if (result.isLockAcquisitionFailed()) {
          blockDeleteMetrics.incrTotalLockTimeoutTransactionCount();
        }
        results.add(result.getResult());
      });
    }
    return results;
  }

  @VisibleForTesting
  public List<Future<DeleteBlockTransactionExecutionResult>> submitTasks(
      List<DeletedBlocksTransaction> deletedBlocksTransactions) {
    List<Future<DeleteBlockTransactionExecutionResult>> futures =
        new ArrayList<>(deletedBlocksTransactions.size());

    for (DeletedBlocksTransaction tx : deletedBlocksTransactions) {
      Future<DeleteBlockTransactionExecutionResult> future =
          executor.submit(new ProcessTransactionTask(tx));
      futures.add(future);
    }
    return futures;
  }

  public void handleTasksResults(
      List<Future<DeleteBlockTransactionExecutionResult>> futures,
      Consumer<DeleteBlockTransactionExecutionResult> handler) {
    futures.forEach(f -> {
      try {
        DeleteBlockTransactionExecutionResult result = f.get();
        handler.accept(result);
      } catch (ExecutionException e) {
        LOG.error("task failed.", e);
      } catch (InterruptedException e) {
        LOG.error("task interrupted.", e);
        Thread.currentThread().interrupt();
      }
    });
  }

  private void markBlocksForDeletionSchemaV3(
      KeyValueContainerData containerData, DeletedBlocksTransaction delTX)
      throws IOException {
    DeletionMarker schemaV3Marker = (table, batch, tid, txn) -> {
      Table<String, DeletedBlocksTransaction> delTxTable =
          (Table<String, DeletedBlocksTransaction>) table;
      delTxTable.putWithBatch(batch, containerData.getDeleteTxnKey(tid),
          txn);
    };

    markBlocksForDeletionTransaction(containerData, delTX,
        delTX.getTxID(), schemaV3Marker);
  }

  private void markBlocksForDeletionSchemaV2(
      KeyValueContainerData containerData, DeletedBlocksTransaction delTX)
      throws IOException {
    DeletionMarker schemaV2Marker = (table, batch, tid, txn) -> {
      Table<Long, DeletedBlocksTransaction> delTxTable =
          (Table<Long, DeletedBlocksTransaction>) table;
      delTxTable.putWithBatch(batch, tid, txn);
    };

    markBlocksForDeletionTransaction(containerData, delTX,
        delTX.getTxID(), schemaV2Marker);
  }

  /**
   * Move a bunch of blocks from a container to deleting state. This is a meta
   * update, the actual deletes happen in async mode.
   *
   * @param containerData - KeyValueContainerData
   * @param delTX a block deletion transaction.
   * @throws IOException if I/O error occurs.
   */
  private void markBlocksForDeletionTransaction(
      KeyValueContainerData containerData, DeletedBlocksTransaction delTX,
      long txnID, DeletionMarker marker)
      throws IOException {
    int newDeletionBlocks = 0;
    long containerId = delTX.getContainerID();
    if (isDuplicateTransaction(containerId, containerData, delTX, blockDeleteMetrics)) {
      return;
    }
    try (DBHandle containerDB = BlockUtils.getDB(containerData, conf)) {
      DeleteTransactionStore<?> store =
          (DeleteTransactionStore<?>) containerDB.getStore();
      Table<?, DeletedBlocksTransaction> delTxTable =
          store.getDeleteTransactionTable();
      try (BatchOperation batch = containerDB.getStore().getBatchHandler()
          .initBatchOperation()) {
        marker.apply(delTxTable, batch, txnID, delTX);
        newDeletionBlocks += delTX.getLocalIDList().size();
        updateMetaData(containerData, delTX, newDeletionBlocks, containerDB,
            batch);
        containerDB.getStore().getBatchHandler().commitBatchOperation(batch);
      }
    }
    blockDeleteMetrics.incrMarkedBlockCount(delTX.getLocalIDCount());
  }

  private void markBlocksForDeletionSchemaV1(
      KeyValueContainerData containerData, DeletedBlocksTransaction delTX)
      throws IOException {
    long containerId = delTX.getContainerID();
    if (isDuplicateTransaction(containerId, containerData, delTX, blockDeleteMetrics)) {
      return;
    }
    int newDeletionBlocks = 0;
    try (DBHandle containerDB = BlockUtils.getDB(containerData, conf)) {
      Table<String, BlockData> blockDataTable =
          containerDB.getStore().getBlockDataTable();
      Table<String, ChunkInfoList> deletedBlocksTable =
          containerDB.getStore().getDeletedBlocksTable();

      try (BatchOperation batch = containerDB.getStore().getBatchHandler()
          .initBatchOperation()) {
        for (Long blkLong : delTX.getLocalIDList()) {
          String blk = containerData.getBlockKey(blkLong);
          BlockData blkInfo = blockDataTable.get(blk);
          if (blkInfo != null) {
            String deletingKey = containerData.getDeletingBlockKey(blkLong);
            if (blockDataTable.get(deletingKey) != null
                || deletedBlocksTable.get(blk) != null) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Ignoring delete for block {} in container {}."
                        + " Entry already added.", blkLong, containerId);
              }
              continue;
            }
            // Found the block in container db,
            // use an atomic update to change its state to deleting.
            blockDataTable.putWithBatch(batch, deletingKey, blkInfo);
            blockDataTable.deleteWithBatch(batch, blk);
            newDeletionBlocks++;
            if (LOG.isDebugEnabled()) {
              LOG.debug("Transited Block {} to DELETING state in container {}",
                  blkLong, containerId);
            }
          } else {
            // try clean up the possibly onDisk but unreferenced blocks/chunks
            try {
              Container<?> container = containerSet.getContainer(containerId);
              ozoneContainer.getDispatcher().getHandler(container
                  .getContainerType()).deleteUnreferenced(container, blkLong);
            } catch (IOException e) {
              LOG.error("Failed to delete files for unreferenced block {} of " +
                      "container {}", blkLong, containerId, e);
            }
          }
        }
        updateMetaData(containerData, delTX, newDeletionBlocks, containerDB,
            batch);
        containerDB.getStore().getBatchHandler().commitBatchOperation(batch);
      } catch (IOException e) {
        // if some blocks failed to delete, we fail this TX,
        // without sending this ACK to SCM, SCM will resend the TX
        // with a certain number of retries.
        throw new IOException(
            "Failed to delete blocks for TXID = " + delTX.getTxID(), e);
      }
      blockDeleteMetrics.incrMarkedBlockCount(delTX.getLocalIDCount());
    }
  }

  private void updateMetaData(KeyValueContainerData containerData,
      DeletedBlocksTransaction delTX, int newDeletionBlocks,
      DBHandle containerDB, BatchOperation batchOperation)
      throws IOException {
    if (newDeletionBlocks > 0) {
      // Finally commit the DB counters.
      Table<String, Long> metadataTable =
          containerDB.getStore().getMetadataTable();

      // In memory is updated only when existing delete transactionID is
      // greater.
      if (delTX.getTxID() > containerData.getDeleteTransactionId()) {
        // Update in DB pending delete key count and delete transaction ID.
        metadataTable
            .putWithBatch(batchOperation,
                containerData.getLatestDeleteTxnKey(), delTX.getTxID());
      }

      long pendingDeleteBlocks =
          containerData.getNumPendingDeletionBlocks() + newDeletionBlocks;
      metadataTable
          .putWithBatch(batchOperation,
              containerData.getPendingDeleteBlockCountKey(),
              pendingDeleteBlocks);

      // Update pending deletion blocks count, blocks bytes and delete transaction ID in in-memory container status.
      // Persist pending bytes only if the feature is finalized.
      if (VersionedDatanodeFeatures.isFinalized(
          HDDSLayoutFeature.STORAGE_SPACE_DISTRIBUTION) && delTX.hasTotalBlockSize()) {
        long pendingBytes = containerData.getBlockPendingDeletionBytes();
        pendingBytes += delTX.getTotalBlockSize();
        metadataTable
            .putWithBatch(batchOperation,
                containerData.getPendingDeleteBlockBytesKey(),
                pendingBytes);
      }
      containerData.incrPendingDeletionBlocks(newDeletionBlocks,
          delTX.hasTotalBlockSize() ? delTX.getTotalBlockSize() : 0);
      containerData.updateDeleteTransactionId(delTX.getTxID());
    }
  }

  public static boolean isDuplicateTransaction(long containerId, KeyValueContainerData containerData,
      DeletedBlocksTransaction delTX, BlockDeletingServiceMetrics metrics) {
    boolean duplicate = false;

    if (delTX.getTxID() < containerData.getDeleteTransactionId()) {
      if (metrics != null) {
        metrics.incOutOfOrderDeleteBlockTransactionCount();
      }
      LOG.debug(String.format("Delete blocks for containerId: %d"
              + " is received out of order, %d < %d", containerId, delTX.getTxID(),
          containerData.getDeleteTransactionId()));
    } else if (delTX.getTxID() == containerData.getDeleteTransactionId()) {
      duplicate = true;
      LOG.info(String.format("Delete blocks with txID %d for containerId: %d"
              + " is retried.", delTX.getTxID(), containerId));
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Processing Container : {}, DB path : {}, transaction {}",
            containerId, containerData.getMetadataPath(), delTX.getTxID());
      }
    }
    return duplicate;
  }

  @Override
  public SCMCommandProto.Type getCommandType() {
    return SCMCommandProto.Type.deleteBlocksCommand;
  }

  @Override
  public int getInvocationCount() {
    return this.invocationCount;
  }

  @Override
  public long getAverageRunTime() {
    return (long) this.opsLatencyMs.lastStat().mean();
  }

  @Override
  public long getTotalRunTime() {
    return (long) this.opsLatencyMs.lastStat().total();
  }

  @Override
  public void stop() {
    if (executor != null) {
      try {
        executor.shutdown();
        if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
          executor.shutdownNow();
        }
      } catch (InterruptedException ie) {
        // Ignore, we don't really care about the failure.
        Thread.currentThread().interrupt();
      }
    }

    if (handlerThread != null) {
      try {
        handlerThread.interrupt();
        handlerThread.join(3000);
      } catch (InterruptedException ie) {
        // Ignore, we don't really care about the failure.
        Thread.currentThread().interrupt();
      }
    }
  }

  private interface DeletionMarker {
    void apply(Table<?, DeletedBlocksTransaction> deleteTxnsTable,
        BatchOperation batch, long txnID, DeletedBlocksTransaction delTX)
        throws IOException;
  }

  /**
   * The SchemaHandler interface provides a contract for handling
   * KeyValueContainerData and DeletedBlocksTransaction based
   * on different schema versions.
   */
  public interface SchemaHandler {
    void handle(KeyValueContainerData containerData,
        DeletedBlocksTransaction tx) throws IOException;
  }

  @VisibleForTesting
  public Map<String, SchemaHandler> getSchemaHandlers() {
    return schemaHandlers;
  }

  @VisibleForTesting
  public BlockDeletingServiceMetrics getBlockDeleteMetrics() {
    return blockDeleteMetrics;
  }

  @VisibleForTesting
  public ThreadPoolExecutor getExecutor() {
    return executor;
  }

  public void setPoolSize(int size) {
    HddsServerUtil.setPoolSize(executor, size, LOG);
  }
}
