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
package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.ozone.container.common.helpers.BlockDeletingServiceMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDeletionChoosingPolicy;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.keyvalue.statemachine.background.BlockDeletingTask;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A per-datanode container block deleting service takes in charge
 * of deleting staled ozone blocks.
 */
public class BlockDeletingService extends BackgroundService {

  private static final Logger LOG =
      LoggerFactory.getLogger(BlockDeletingService.class);

  private final OzoneContainer ozoneContainer;
  private final ContainerDeletionChoosingPolicy containerDeletionPolicy;
  private final ConfigurationSource conf;
  private final DatanodeConfiguration dnConf;
  private final BlockDeletingServiceMetrics metrics;

  // Task priority is useful when a to-delete block has weight.
  private static final int TASK_PRIORITY_DEFAULT = 1;

  private final Duration blockDeletingMaxLockHoldingTime;

  @VisibleForTesting
  public BlockDeletingService(
      OzoneContainer ozoneContainer, long serviceInterval, long serviceTimeout,
      TimeUnit timeUnit, int workerSize, ConfigurationSource conf
  ) {
    this(ozoneContainer, serviceInterval, serviceTimeout, timeUnit, workerSize,
        conf, "", null);
  }

  @SuppressWarnings("checkstyle:parameternumber")
  public BlockDeletingService(
      OzoneContainer ozoneContainer, long serviceInterval, long serviceTimeout,
      TimeUnit timeUnit, int workerSize, ConfigurationSource conf,
      String threadNamePrefix, ReconfigurationHandler reconfigurationHandler
  ) {
    super("BlockDeletingService", serviceInterval, timeUnit,
        workerSize, serviceTimeout, threadNamePrefix);
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
    dnConf = conf.getObject(DatanodeConfiguration.class);
    if (reconfigurationHandler != null) {
      reconfigurationHandler.register(dnConf);
    }
    this.blockDeletingMaxLockHoldingTime =
        dnConf.getBlockDeletingMaxLockHoldingTime();
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

    public Long getNumBlocksToDelete() {
      return numBlocksToDelete;
    }

  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();

    try {
      // We at most list a number of containers a time,
      // in case there are too many containers and start too many workers.
      // We must ensure there is no empty container in this result.
      // The chosen result depends on what container deletion policy is
      // configured.
      List<ContainerBlockInfo> containers =
          chooseContainerForBlockDeletion(getBlockLimitPerInterval(),
              containerDeletionPolicy);

      BackgroundTask containerBlockInfos = null;
      long totalBlocks = 0;
      for (ContainerBlockInfo containerBlockInfo : containers) {
        BlockDeletingTaskBuilder builder =
            new BlockDeletingTaskBuilder();
        builder.setBlockDeletingService(this)
            .setContainerBlockInfo(containerBlockInfo)
            .setPriority(TASK_PRIORITY_DEFAULT);
        containerBlockInfos = builder.build();
        queue.add(containerBlockInfos);
        totalBlocks += containerBlockInfo.getNumBlocksToDelete();
        LOG.debug("Queued- Container: {}, deleted blocks: {}",
            containerBlockInfo.getContainerData().getContainerID(), containerBlockInfo.getNumBlocksToDelete());
      }
      metrics.incrTotalBlockChosenCount(totalBlocks);
      metrics.incrTotalContainerChosenCount(containers.size());
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

    AtomicLong totalPendingBlockCount = new AtomicLong(0L);
    Map<Long, ContainerData> containerDataMap =
        ozoneContainer.getContainerSet().getContainerMap().entrySet().stream()
            .filter(e -> (checkPendingDeletionBlocks(
                e.getValue().getContainerData())))
            .filter(e -> isDeletionAllowed(e.getValue().getContainerData(),
                deletionPolicy)).collect(Collectors
            .toMap(Map.Entry::getKey, e -> {
              ContainerData containerData =
                  e.getValue().getContainerData();
              totalPendingBlockCount
                  .addAndGet(
                      ContainerUtils.getPendingDeletionBlocks(containerData));
              return containerData;
            }));

    metrics.setTotalPendingBlockCount(totalPendingBlockCount.get());
    return deletionPolicy
        .chooseContainerForBlockDeletion(blockLimit, containerDataMap);
  }

  private boolean checkPendingDeletionBlocks(ContainerData containerData) {
    return ContainerUtils.getPendingDeletionBlocks(containerData) > 0;
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
          if (minReplicatedIndex < containerBCSID) {
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

  public OzoneContainer getOzoneContainer() {
    return ozoneContainer;
  }

  public ConfigurationSource getConf() {
    return conf;
  }

  public BlockDeletingServiceMetrics getMetrics() {
    return metrics;
  }

  public Duration getBlockDeletingMaxLockHoldingTime() {
    return blockDeletingMaxLockHoldingTime;
  }

  public int getBlockLimitPerInterval() {
    return dnConf.getBlockDeletionLimit();
  }

  private static class BlockDeletingTaskBuilder {
    private BlockDeletingService blockDeletingService;
    private BlockDeletingService.ContainerBlockInfo containerBlockInfo;
    private int priority;

    public BlockDeletingTaskBuilder setBlockDeletingService(
        BlockDeletingService blockDeletingService) {
      this.blockDeletingService = blockDeletingService;
      return this;
    }

    public BlockDeletingTaskBuilder setContainerBlockInfo(
        ContainerBlockInfo containerBlockInfo) {
      this.containerBlockInfo = containerBlockInfo;
      return this;
    }

    public BlockDeletingTaskBuilder setPriority(int priority) {
      this.priority = priority;
      return this;
    }

    public BackgroundTask build() {
      ContainerProtos.ContainerType containerType =
          containerBlockInfo.getContainerData().getContainerType();
      if (containerType
          .equals(ContainerProtos.ContainerType.KeyValueContainer)) {
        return
            new BlockDeletingTask(blockDeletingService, containerBlockInfo,
                priority);
      } else {
        // If another ContainerType is available later, implement it
        throw new IllegalArgumentException(
            "BlockDeletingTask for ContainerType: " + containerType +
                "doesn't exist.");
      }
    }
  }
}
