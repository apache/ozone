
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
import java.util.UUID;
import java.util.LinkedList;
import java.util.Objects;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.BlockDeletingServiceMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.TopNOrderedContainerDeletionChoosingPolicy;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDeletionChoosingPolicy;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.metadata.DeleteTransactionStore;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;

import com.google.common.collect.Lists;

import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V1;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V2;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V3;

import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A per-datanode container block deleting service takes in charge
 * of deleting staled ozone blocks.
 */

public class BlockDeletingService extends BackgroundService {

  private static final Logger LOG =
      LoggerFactory.getLogger(BlockDeletingService.class);

  private OzoneContainer ozoneContainer;
  private ContainerDeletionChoosingPolicy containerDeletionPolicy;
  private final ConfigurationSource conf;

  private final int blockLimitPerInterval;

  private final BlockDeletingServiceMetrics metrics;

  // Task priority is useful when a to-delete block has weight.
  private static final int TASK_PRIORITY_DEFAULT = 1;

  public BlockDeletingService(OzoneContainer ozoneContainer,
                              long serviceInterval, long serviceTimeout,
                              TimeUnit timeUnit, int workerSize,
                              ConfigurationSource conf) {
    super("BlockDeletingService", serviceInterval, timeUnit,
        workerSize, serviceTimeout);
    this.ozoneContainer = ozoneContainer;
    try {
      containerDeletionPolicy = conf.getClass(
          ScmConfigKeys.OZONE_SCM_KEY_VALUE_CONTAINER_DELETION_CHOOSING_POLICY,
          TopNOrderedContainerDeletionChoosingPolicy.class,
          ContainerDeletionChoosingPolicy.class).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    this.conf = conf;
    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    this.blockLimitPerInterval = dnConf.getBlockDeletionLimit();
    metrics = BlockDeletingServiceMetrics.create();
  }

  /**
   * Pair of container data and the number of blocks to delete.
   */
  public static class ContainerBlockInfo {
    private final ContainerData containerData;
    private final Long numBlocksToDelete;

    public ContainerBlockInfo(ContainerData containerData, Long blocks) {
      this.containerData = containerData;
      this.numBlocksToDelete = blocks;
    }

    public ContainerData getContainerData() {
      return containerData;
    }

    public Long getBlocks() {
      return numBlocksToDelete;
    }

  }


  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    List<ContainerBlockInfo> containers = Lists.newArrayList();
    try {
      // We at most list a number of containers a time,
      // in case there are too many containers and start too many workers.
      // We must ensure there is no empty container in this result.
      // The chosen result depends on what container deletion policy is
      // configured.
      containers = chooseContainerForBlockDeletion(blockLimitPerInterval,
          containerDeletionPolicy);

      BlockDeletingTask containerBlockInfos = null;
      long totalBlocks = 0;
      for (ContainerBlockInfo containerBlockInfo : containers) {
        containerBlockInfos =
            new BlockDeletingTask(containerBlockInfo.containerData,
                TASK_PRIORITY_DEFAULT, containerBlockInfo.numBlocksToDelete);
        queue.add(containerBlockInfos);
        totalBlocks += containerBlockInfo.numBlocksToDelete;
      }
      if (containers.size() > 0) {
        LOG.debug("Queued {} blocks from {} containers for deletion",
            totalBlocks, containers.size());
      }
    } catch (StorageContainerException e) {
      LOG.warn("Failed to initiate block deleting tasks, "
          + "caused by unable to get containers info. "
          + "Retry in next interval. ", e);
    } catch (Exception e) {
      // In case listContainer call throws any uncaught RuntimeException.
      LOG.error("Unexpected error occurs during deleting blocks.", e);
    }
    return queue;
  }

  public List<ContainerBlockInfo> chooseContainerForBlockDeletion(
      int blockLimit, ContainerDeletionChoosingPolicy deletionPolicy)
      throws StorageContainerException {
    Map<Long, ContainerData> containerDataMap =
        ozoneContainer.getContainerSet().getContainerMap().entrySet().stream()
            .filter(e -> isDeletionAllowed(e.getValue().getContainerData(),
                deletionPolicy)).collect(Collectors
            .toMap(Map.Entry::getKey, e -> e.getValue().getContainerData()));
    return deletionPolicy
        .chooseContainerForBlockDeletion(blockLimit, containerDataMap);
  }

  private boolean isDeletionAllowed(ContainerData containerData,
      ContainerDeletionChoosingPolicy deletionPolicy) {
    if (!deletionPolicy
        .isValidContainerType(containerData.getContainerType())) {
      return false;
    } else if (!containerData.isClosed()) {
      return false;
    } else {
      if (ozoneContainer.getWriteChannel() instanceof XceiverServerRatis) {
        XceiverServerRatis ratisServer =
            (XceiverServerRatis) ozoneContainer.getWriteChannel();
        final String originPipelineId = containerData.getOriginPipelineId();
        if (originPipelineId == null || originPipelineId.isEmpty()) {
          // In case the pipelineID is empty, just mark it for deletion.
          // TODO: currently EC container goes through this path.
          return true;
        }
        UUID pipelineUUID;
        try {
          pipelineUUID = UUID.fromString(originPipelineId);
        } catch (IllegalArgumentException e) {
          LOG.warn("Invalid pipelineID {} for container {}",
              originPipelineId, containerData.getContainerID());
          return false;
        }
        PipelineID pipelineID = PipelineID.valueOf(pipelineUUID);
        // in case the ratis group does not exist, just mark it for deletion.
        if (!ratisServer.isExist(pipelineID.getProtobuf())) {
          return true;
        }
        try {
          long minReplicatedIndex =
              ratisServer.getMinReplicatedIndex(pipelineID);
          long containerBCSID = containerData.getBlockCommitSequenceId();
          if (minReplicatedIndex >= 0 && minReplicatedIndex < containerBCSID) {
            LOG.warn("Close Container log Index {} is not replicated across all"
                    + " the servers in the pipeline {} as the min replicated "
                    + "index is {}. Deletion is not allowed in this container "
                    + "yet.", containerBCSID,
                containerData.getOriginPipelineId(), minReplicatedIndex);
            return false;
          } else {
            return true;
          }
        } catch (IOException ioe) {
          // in case of any exception check again whether the pipeline exist
          // and in case the pipeline got destroyed, just mark it for deletion
          if (!ratisServer.isExist(pipelineID.getProtobuf())) {
            return true;
          } else {
            LOG.info(ioe.getMessage());
            return false;
          }
        }
      }
      return true;
    }
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

  private class BlockDeletingTask implements BackgroundTask {

    private final int priority;
    private final KeyValueContainerData containerData;
    private final long blocksToDelete;

    BlockDeletingTask(ContainerData containerName, int priority,
        long blocksToDelete) {
      this.priority = priority;
      this.containerData = (KeyValueContainerData) containerName;
      this.blocksToDelete = blocksToDelete;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      ContainerBackgroundTaskResult crr;
      final Container container = ozoneContainer.getContainerSet()
          .getContainer(containerData.getContainerID());
      container.writeLock();
      File dataDir = new File(containerData.getChunksPath());
      long startTime = Time.monotonicNow();
      // Scan container's db and get list of under deletion blocks
      try (DBHandle meta = BlockUtils.getDB(containerData, conf)) {
        if (containerData.getSchemaVersion().equals(SCHEMA_V1)) {
          crr = deleteViaSchema1(meta, container, dataDir, startTime);
        } else if (containerData.getSchemaVersion().equals(SCHEMA_V2)) {
          crr = deleteViaSchema2(meta, container, dataDir, startTime);
        } else if (containerData.getSchemaVersion().equals(SCHEMA_V3)) {
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
        for (Table.KeyValue<String, BlockData> entry: toDeleteBlocks) {
          String blockName = entry.getKey();
          LOG.debug("Deleting block {}", blockName);
          if (entry.getValue() == null) {
            LOG.warn("Missing delete block(Container = " +
                container.getContainerData().getContainerID() + ", Block = " +
                blockName);
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
        delTxTable.deleteWithBatch(batch, containerData.deleteTxnKey(tid));
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
          LOG.debug("No transaction found in container : {}",
              containerData.getContainerID());
          return crr;
        }

        LOG.debug("Container : {}, To-Delete blocks : {}",
            containerData.getContainerID(), delBlocks.size());

        Handler handler = Objects.requireNonNull(ozoneContainer.getDispatcher()
            .getHandler(container.getContainerType()));

        Pair<Integer, Long> deleteBlocksResult =
            deleteTransactions(delBlocks, handler, blockDataTable, container);
        int deletedBlocksCount = deleteBlocksResult.getLeft();
        long releasedBytes = deleteBlocksResult.getRight();

        // Once blocks are deleted... remove the blockID from blockDataTable
        // and also remove the transactions from txnTable.
        try (BatchOperation batch = meta.getStore().getBatchHandler()
            .initBatchOperation()) {
          for (DeletedBlocksTransaction delTx : delBlocks) {
            deleter.apply(deleteTxns, batch, delTx.getTxID());
            for (Long blk : delTx.getLocalIDList()) {
              String bID = blk.toString();
              meta.getStore().getBlockDataTable().deleteWithBatch(batch, bID);
            }
          }

          // Handler.deleteBlock calls deleteChunk to delete all the chunks
          // in the block. The ContainerData stats (DB and in-memory) are not
          // updated with decremented used bytes during deleteChunk. This is
          // done here so that all the DB updates for block delete can be
          // batched together while committing to DB.
          containerData.updateAndCommitDBCounters(meta, batch,
              deletedBlocksCount, releasedBytes);

          // update count of pending deletion blocks, block count and used
          // bytes in in-memory container status and used space in volume.
          containerData.decrPendingDeletionBlocks(deletedBlocksCount);
          containerData.decrBlockCount(deletedBlocksCount);
          containerData.decrBytesUsed(releasedBytes);
          containerData.getVolume().decrementUsedSpace(releasedBytes);
          metrics.incrSuccessCount(deletedBlocksCount);
          metrics.incrSuccessBytes(releasedBytes);
        }

        LOG.debug("Container: {}, deleted blocks: {}, space reclaimed: {}, " +
                "task elapsed time: {}ms", containerData.getContainerID(),
            deletedBlocksCount, Time.monotonicNow() - startTime);

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
    private Pair<Integer, Long> deleteTransactions(
        List<DeletedBlocksTransaction> delBlocks, Handler handler,
        Table<String, BlockData> blockDataTable, Container container)
        throws IOException {
      int blocksDeleted = 0;
      long bytesReleased = 0;
      for (DeletedBlocksTransaction entry : delBlocks) {
        for (Long blkLong : entry.getLocalIDList()) {
          String blk = containerData.blockKey(blkLong);
          BlockData blkInfo = blockDataTable.get(blk);
          LOG.debug("Deleting block {}", blkLong);
          if (blkInfo == null) {
            LOG.warn("Missing delete block(Container = " +
                container.getContainerData().getContainerID() + ", Block = " +
                blkLong);
            continue;
          }
          try {
            handler.deleteBlock(container, blkInfo);
            blocksDeleted++;
            bytesReleased += KeyValueContainerUtil.getBlockLength(blkInfo);
          } catch (InvalidProtocolBufferException e) {
            LOG.error("Failed to parse block info for block {}", blkLong, e);
          } catch (IOException e) {
            LOG.error("Failed to delete files for block {}", blkLong, e);
          }
        }
      }
      return Pair.of(blocksDeleted, bytesReleased);
    }

    @Override
    public int getPriority() {
      return priority;
    }
  }

  private interface Deleter {
    void apply(Table<?, DeletedBlocksTransaction> deleteTxnsTable,
        BatchOperation batch, long txnID) throws IOException;
  }

  public BlockDeletingServiceMetrics getMetrics() {
    return metrics;
  }
}
