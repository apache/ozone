/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.keyvalue.statemachine.background;

import java.io.File;
import java.io.IOException;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Objects;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.BlockDeletingServiceMetrics;
import org.apache.hadoop.ozone.container.common.impl.BlockDeletingService;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.metadata.DeleteTransactionStore;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;

import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V1;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V2;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V3;

import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BlockDeletingTask for KeyValueContainer.
 */
public class BlockDeletingTask implements BackgroundTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(BlockDeletingTask.class);

  private final BlockDeletingServiceMetrics metrics;
  private final int priority;
  private final KeyValueContainerData containerData;
  private long blocksToDelete;
  private final OzoneContainer ozoneContainer;
  private final ConfigurationSource conf;
  private Duration blockDeletingMaxLockHoldingTime;

  public BlockDeletingTask(
      BlockDeletingService blockDeletingService,
      BlockDeletingService.ContainerBlockInfo containerBlockInfo,
      int priority) {
    this.ozoneContainer = blockDeletingService.getOzoneContainer();
    this.metrics = blockDeletingService.getMetrics();
    this.conf = blockDeletingService.getConf();
    this.blockDeletingMaxLockHoldingTime =
        blockDeletingService.getBlockDeletingMaxLockHoldingTime();
    this.priority = priority;
    this.containerData =
        (KeyValueContainerData) containerBlockInfo.getContainerData();
    this.blocksToDelete = containerBlockInfo.getNumBlocksToDelete();
  }

  private static class ContainerBackgroundTaskResult
      implements BackgroundTaskResult {
    private List<String> deletedBlockIds;

    ContainerBackgroundTaskResult() {
      deletedBlockIds = new LinkedList<>();
    }

    public void addBlockId(String blockId) {
      deletedBlockIds.add(blockId);
    }

    public void addAll(List<String> blockIds) {
      deletedBlockIds.addAll(blockIds);
    }

    public List<String> getDeletedBlocks() {
      return deletedBlockIds;
    }

    @Override
    public int getSize() {
      return deletedBlockIds.size();
    }
  }


  @Override
  public BackgroundTaskResult call() throws Exception {
    ContainerBackgroundTaskResult result =
        new ContainerBackgroundTaskResult();
    while (blocksToDelete > 0) {
      ContainerBackgroundTaskResult crr = handleDeleteTask();
      if (blocksToDelete > 0 && crr.getSize() == 0) {
        LOG.warn("Block deletion failed, remaining Blocks to be deleted {}," +
                " but no Block be deleted. Container" +
                " {}, pending block count {}",
            blocksToDelete, containerData.getContainerID(),
            containerData.getNumPendingDeletionBlocks());
        break;
      }
      blocksToDelete -= crr.getSize();
      result.addAll(crr.getDeletedBlocks());
    }
    return result;
  }

  private ContainerBackgroundTaskResult handleDeleteTask() throws Exception {
    ContainerBackgroundTaskResult crr;
    final Container container = ozoneContainer.getContainerSet()
        .getContainer(containerData.getContainerID());
    container.writeLock();
    File dataDir = new File(containerData.getChunksPath());
    long startTime = Time.monotonicNow();
    // Scan container's db and get list of under deletion blocks
    try (DBHandle meta = BlockUtils.getDB(containerData, conf)) {
      if (containerData.hasSchema(SCHEMA_V1)) {
        crr = deleteViaSchema1(meta, container, dataDir, startTime);
      } else if (containerData.hasSchema(SCHEMA_V2)) {
        crr = deleteViaSchema2(meta, container, dataDir, startTime);
      } else if (containerData.hasSchema(SCHEMA_V3)) {
        crr = deleteViaSchema3(meta, container, dataDir, startTime);
      } else {
        throw new UnsupportedOperationException(
            "Only schema version 1,2,3 are supported.");
      }
      return crr;
    } finally {
      container.writeUnlock();
    }
  }

  public boolean checkDataDir(File dataDir) {
    boolean b = true;
    if (!dataDir.exists() || !dataDir.isDirectory()) {
      LOG.error("Invalid container data dir {} : "
          + "does not exist or not a directory", dataDir.getAbsolutePath());
      b = false;
    }
    return b;
  }

  public ContainerBackgroundTaskResult deleteViaSchema1(
      DBHandle meta, Container container, File dataDir,
      long startTime) throws IOException {
    ContainerBackgroundTaskResult crr = new ContainerBackgroundTaskResult();
    if (!checkDataDir(dataDir)) {
      return crr;
    }
    try {
      Table<String, BlockData> blockDataTable =
          meta.getStore().getBlockDataTable();

      // # of blocks to delete is throttled
      KeyPrefixFilter filter = containerData.getDeletingBlockKeyFilter();
      List<? extends Table.KeyValue<String, BlockData>> toDeleteBlocks =
          blockDataTable
              .getSequentialRangeKVs(containerData.startKeyEmpty(),
                  (int) blocksToDelete, containerData.containerPrefix(),
                  filter);
      if (toDeleteBlocks.isEmpty()) {
        LOG.debug("No under deletion block found in container : {}",
            containerData.getContainerID());
        return crr;
      }

      List<String> succeedBlocks = new LinkedList<>();
      LOG.debug("Container : {}, To-Delete blocks : {}",
          containerData.getContainerID(), toDeleteBlocks.size());

      Handler handler = Objects.requireNonNull(ozoneContainer.getDispatcher()
          .getHandler(container.getContainerType()));

      long releasedBytes = 0;
      for (Table.KeyValue<String, BlockData> entry : toDeleteBlocks) {
        String blockName = entry.getKey();
        LOG.debug("Deleting block {}", blockName);
        if (entry.getValue() == null) {
          LOG.warn("Missing delete block(Container = " +
              container.getContainerData().getContainerID() +
              ", Block = " + blockName);
          continue;
        }
        try {
          handler.deleteBlock(container, entry.getValue());
          releasedBytes += KeyValueContainerUtil.getBlockLength(
              entry.getValue());
          succeedBlocks.add(blockName);
        } catch (InvalidProtocolBufferException e) {
          LOG.error("Failed to parse block info for block {}", blockName, e);
        } catch (IOException e) {
          LOG.error("Failed to delete files for block {}", blockName, e);
        }
      }

      // Once chunks in the blocks are deleted... remove the blockID from
      // blockDataTable.
      try (BatchOperation batch = meta.getStore().getBatchHandler()
          .initBatchOperation()) {
        for (String entry : succeedBlocks) {
          blockDataTable.deleteWithBatch(batch, entry);
        }

        // Handler.deleteBlock calls deleteChunk to delete all the chunks
        // in the block. The ContainerData stats (DB and in-memory) are not
        // updated with decremented used bytes during deleteChunk. This is
        // done here so that all the DB update for block delete can be
        // batched together while committing to DB.
        int deletedBlocksCount = succeedBlocks.size();
        containerData.updateAndCommitDBCounters(meta, batch,
            deletedBlocksCount, releasedBytes);
        // Once DB update is persisted, check if there are any blocks
        // remaining in the DB. This will determine whether the container
        // can be deleted by SCM.
        if (!container.hasBlocks()) {
          containerData.markAsEmpty();
        }

        // update count of pending deletion blocks, block count and used
        // bytes in in-memory container status.
        containerData.decrPendingDeletionBlocks(deletedBlocksCount);
        containerData.decrBlockCount(deletedBlocksCount);
        containerData.decrBytesUsed(releasedBytes);
        containerData.getVolume().decrementUsedSpace(releasedBytes);
        metrics.incrSuccessCount(deletedBlocksCount);
        metrics.incrSuccessBytes(releasedBytes);
      }

      if (!succeedBlocks.isEmpty()) {
        LOG.debug("Container: {}, deleted blocks: {}, space reclaimed: {}, " +
                "task elapsed time: {}ms", containerData.getContainerID(),
            succeedBlocks.size(), releasedBytes,
            Time.monotonicNow() - startTime);
      }
      crr.addAll(succeedBlocks);
      return crr;
    } catch (IOException exception) {
      LOG.warn("Deletion operation was not successful for container: " +
          container.getContainerData().getContainerID(), exception);
      metrics.incrFailureCount();
      throw exception;
    }
  }

  public ContainerBackgroundTaskResult deleteViaSchema2(
      DBHandle meta, Container container, File dataDir,
      long startTime) throws IOException {
    Deleter schema2Deleter = (table, batch, tid) -> {
      Table<Long, DeletedBlocksTransaction> delTxTable =
          (Table<Long, DeletedBlocksTransaction>) table;
      delTxTable.deleteWithBatch(batch, tid);
    };
    Table<Long, DeletedBlocksTransaction> deleteTxns =
        ((DeleteTransactionStore<Long>) meta.getStore())
            .getDeleteTransactionTable();
    try (TableIterator<Long,
        ? extends Table.KeyValue<Long, DeletedBlocksTransaction>>
             iterator = deleteTxns.iterator()) {
      return deleteViaTransactionStore(
          iterator, meta,
          container, dataDir, startTime, schema2Deleter);
    }
  }

  public ContainerBackgroundTaskResult deleteViaSchema3(
      DBHandle meta, Container container, File dataDir,
      long startTime) throws IOException {
    Deleter schema3Deleter = (table, batch, tid) -> {
      Table<String, DeletedBlocksTransaction> delTxTable =
          (Table<String, DeletedBlocksTransaction>) table;
      delTxTable.deleteWithBatch(batch,
          containerData.getDeleteTxnKey(tid));
    };
    Table<String, DeletedBlocksTransaction> deleteTxns =
        ((DeleteTransactionStore<String>) meta.getStore())
            .getDeleteTransactionTable();
    try (TableIterator<String,
        ? extends Table.KeyValue<String, DeletedBlocksTransaction>>
             iterator = deleteTxns.iterator(containerData.containerPrefix())) {
      return deleteViaTransactionStore(
          iterator, meta,
          container, dataDir, startTime, schema3Deleter);
    }
  }

  private ContainerBackgroundTaskResult deleteViaTransactionStore(
      TableIterator<?, ? extends Table.KeyValue<?, DeletedBlocksTransaction>>
          iter, DBHandle meta, Container container, File dataDir,
      long startTime, Deleter deleter) throws IOException {
    ContainerBackgroundTaskResult crr = new ContainerBackgroundTaskResult();
    if (!checkDataDir(dataDir)) {
      return crr;
    }
    try {
      Table<String, BlockData> blockDataTable =
          meta.getStore().getBlockDataTable();
      DeleteTransactionStore<?> txnStore =
          (DeleteTransactionStore<?>) meta.getStore();
      Table<?, DeletedBlocksTransaction> deleteTxns =
          txnStore.getDeleteTransactionTable();
      List<DeletedBlocksTransaction> delBlocks = new ArrayList<>();
      int numBlocks = 0;
      while (iter.hasNext() && (numBlocks < blocksToDelete)) {
        DeletedBlocksTransaction delTx = iter.next().getValue();
        numBlocks += delTx.getLocalIDList().size();
        delBlocks.add(delTx);
      }
      if (delBlocks.isEmpty()) {
        LOG.info("No transaction found in container {} with pending delete " +
                "block count {}",
            containerData.getContainerID(),
            containerData.getNumPendingDeletionBlocks());
        // If the container was queued for delete, it had a positive
        // pending delete block count. After checking the DB there were
        // actually no delete transactions for the container, so reset the
        // pending delete block count to the correct value of zero.
        containerData.resetPendingDeleteBlockCount(meta);
        return crr;
      }

      LOG.debug("Container : {}, To-Delete blocks : {}",
          containerData.getContainerID(), delBlocks.size());

      Handler handler = Objects.requireNonNull(ozoneContainer.getDispatcher()
          .getHandler(container.getContainerType()));

      DeleteTransactionStats deleteBlocksResult =
          deleteTransactions(delBlocks, handler, blockDataTable, container);
      int deletedBlocksProcessed = deleteBlocksResult.getBlocksProcessed();
      int deletedBlocksCount = deleteBlocksResult.getBlocksDeleted();
      long releasedBytes = deleteBlocksResult.getBytesReleased();
      List<DeletedBlocksTransaction> deletedBlocksTxs =
          deleteBlocksResult.deletedBlocksTxs();
      deleteBlocksResult.deletedBlocksTxs().forEach(
          tx -> crr.addAll(tx.getLocalIDList().stream()
              .map(String::valueOf).collect(Collectors.toList()))
      );

      // Once blocks are deleted... remove the blockID from blockDataTable
      // and also remove the transactions from txnTable.
      try (BatchOperation batch = meta.getStore().getBatchHandler()
          .initBatchOperation()) {
        for (DeletedBlocksTransaction delTx : deletedBlocksTxs) {
          deleter.apply(deleteTxns, batch, delTx.getTxID());
          for (Long blk : delTx.getLocalIDList()) {
            blockDataTable.deleteWithBatch(batch,
                containerData.getBlockKey(blk));
          }
        }

        // Handler.deleteBlock calls deleteChunk to delete all the chunks
        // in the block. The ContainerData stats (DB and in-memory) are not
        // updated with decremented used bytes during deleteChunk. This is
        // done here so that all the DB updates for block delete can be
        // batched together while committing to DB.
        containerData.updateAndCommitDBCounters(meta, batch,
            deletedBlocksCount, releasedBytes);
        // Once DB update is persisted, check if there are any blocks
        // remaining in the DB. This will determine whether the container
        // can be deleted by SCM.
        if (!container.hasBlocks()) {
          containerData.markAsEmpty();
        }

        // update count of pending deletion blocks, block count and used
        // bytes in in-memory container status and used space in volume.
        containerData.decrPendingDeletionBlocks(deletedBlocksProcessed);
        containerData.decrBlockCount(deletedBlocksCount);
        containerData.decrBytesUsed(releasedBytes);
        containerData.getVolume().decrementUsedSpace(releasedBytes);
        metrics.incrSuccessCount(deletedBlocksCount);
        metrics.incrSuccessBytes(releasedBytes);
      }

      LOG.debug("Container: {}, deleted blocks: {}, space reclaimed: {}, " +
              "task elapsed time: {}ms", containerData.getContainerID(),
          deletedBlocksCount, releasedBytes, Time.monotonicNow() - startTime);

      return crr;
    } catch (IOException exception) {
      LOG.warn("Deletion operation was not successful for container: " +
          container.getContainerData().getContainerID(), exception);
      metrics.incrFailureCount();
      throw exception;
    }
  }

  /**
   * Delete the chunks for the given blocks.
   * Return the deletedBlocks count and number of bytes released.
   */
  private DeleteTransactionStats deleteTransactions(
      List<DeletedBlocksTransaction> delBlocks, Handler handler,
      Table<String, BlockData> blockDataTable, Container container)
      throws IOException {
    int blocksProcessed = 0;
    int blocksDeleted = 0;
    long bytesReleased = 0;
    List<DeletedBlocksTransaction> deletedBlocksTxs = new ArrayList<>();
    Instant startTime = Instant.now();

    for (DeletedBlocksTransaction entry : delBlocks) {
      for (Long blkLong : entry.getLocalIDList()) {
        String blk = containerData.getBlockKey(blkLong);
        BlockData blkInfo = blockDataTable.get(blk);
        LOG.debug("Deleting block {}", blkLong);
        if (blkInfo == null) {
          try {
            handler.deleteUnreferenced(container, blkLong);
          } catch (IOException e) {
            LOG.error("Failed to delete files for unreferenced block {} of" +
                    " container {}", blkLong,
                container.getContainerData().getContainerID(), e);
          } finally {
            blocksProcessed++;
          }
          continue;
        }

        boolean deleted = false;
        try {
          handler.deleteBlock(container, blkInfo);
          blocksDeleted++;
          deleted = true;
        } catch (IOException e) {
          // TODO: if deletion of certain block retries exceed the certain
          //  number of times, service should skip deleting it,
          //  otherwise invalid numPendingDeletionBlocks could accumulate
          //  beyond the limit and the following deletion will stop.
          LOG.error("Failed to delete files for block {}", blkLong, e);
        } finally {
          blocksProcessed++;
        }

        if (deleted) {
          try {
            bytesReleased += KeyValueContainerUtil.getBlockLength(blkInfo);
          } catch (IOException e) {
            // TODO: handle the bytesReleased correctly for the unexpected
            //  exception.
            LOG.error("Failed to get block length for block {}", blkLong, e);
          }
        }
      }
      deletedBlocksTxs.add(entry);
      Duration execTime = Duration.between(startTime, Instant.now());
      if (deletedBlocksTxs.size() < delBlocks.size() &&
          execTime.compareTo(getBlockDeletingMaxLockHoldingTime()) > 0) {
        LOG.info("Max lock hold time ({} ms) reached after {} ms. " +
                "Completed {} transactions, deleted {} blocks. " +
                "In container: {}. " +
                "Releasing lock and resuming deletion later.",
            getBlockDeletingMaxLockHoldingTime().toMillis(),
            execTime.toMillis(), deletedBlocksTxs.size(), blocksDeleted,
            container.getContainerData().getContainerID());
        break;
      }
    }
    return new DeleteTransactionStats(blocksProcessed,
        blocksDeleted, bytesReleased, deletedBlocksTxs);
  }

  @Override
  public int getPriority() {
    return priority;
  }

  public Duration getBlockDeletingMaxLockHoldingTime() {
    return blockDeletingMaxLockHoldingTime;
  }

  private interface Deleter {
    void apply(Table<?, DeletedBlocksTransaction> deleteTxnsTable,
               BatchOperation batch, long txnID) throws IOException;
  }

  /**
   * The wrapper class of the result of deleting transactions.
   */
  private static class DeleteTransactionStats {

    private final int blocksProcessed;
    private final int blocksDeleted;
    private final long bytesReleased;
    private final List<DeletedBlocksTransaction> delBlockTxs;

    DeleteTransactionStats(int proceeded, int deleted, long released,
        List<DeletedBlocksTransaction> delBlocks) {
      blocksProcessed = proceeded;
      blocksDeleted = deleted;
      bytesReleased = released;
      delBlockTxs = delBlocks;
    }

    public int getBlocksProcessed() {
      return blocksProcessed;
    }

    public int getBlocksDeleted() {
      return blocksDeleted;
    }

    public long getBytesReleased() {
      return bytesReleased;
    }

    public List<DeletedBlocksTransaction> deletedBlocksTxs() {
      return delBlockTxs;
    }
  }
}
