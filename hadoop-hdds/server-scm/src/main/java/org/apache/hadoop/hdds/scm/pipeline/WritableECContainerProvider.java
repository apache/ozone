/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hdds.conf.StorageUnit.BYTES;

/**
 * Writable Container provider to obtain a writable container for EC pipelines.
 */
public class WritableECContainerProvider
    implements WritableContainerProvider<ECReplicationConfig> {

  private static final Logger LOG = LoggerFactory
      .getLogger(WritableECContainerProvider.class);

  private final ConfigurationSource conf;
  private final PipelineManager pipelineManager;
  private final PipelineChoosePolicy pipelineChoosePolicy;
  private final ContainerManager containerManager;
  private final NodeManager nodeManager;
  private final long containerSize;
  private final WritableECContainerProviderConfig providerConfig;
  // We track for pipelines not containers, since containers may be closed
  // on events triggered by DNs, and we'll lose track of their allocated
  // space and result in orphan records.
  // But EC pipelines are explicitly closed here, then we can safely
  // clean them up.
  private final Map<PipelineID, Long> pipelineAllocatedSpaceMap;
  // Whether we reached the max pipelines limit once.
  private volatile boolean maxPipelinesLimitReached;

  public WritableECContainerProvider(ConfigurationSource conf,
      PipelineManager pipelineManager, ContainerManager containerManager,
      NodeManager nodeManager, PipelineChoosePolicy pipelineChoosePolicy) {
    this.conf = conf;
    this.providerConfig =
        conf.getObject(WritableECContainerProviderConfig.class);
    this.pipelineManager = pipelineManager;
    this.containerManager = containerManager;
    this.pipelineChoosePolicy = pipelineChoosePolicy;
    this.containerSize = getConfiguredContainerSize();
    this.nodeManager = nodeManager;
    this.pipelineAllocatedSpaceMap = new ConcurrentHashMap<>();
    this.maxPipelinesLimitReached = false;
  }

  /**
   * @param size The max size of block in bytes which will be written. This
   *             comes from Ozone Manager and will be the block size configured
   *             for the cluster. The client cannot pass any arbitrary value
   *             from this setting.
   * @param repConfig The replication Config indicating the EC data and partiy
   *                  block counts.
   * @param owner The owner of the container
   * @param excludeList A set of datanodes, container and pipelines which should
   *                    not be considered.
   * @return A containerInfo representing a block group with with space for the
   *         write, or null if no container can be allocated.
   * @throws IOException
   */
  @Override
  public ContainerInfo getContainer(final long size,
      ECReplicationConfig repConfig, String owner, ExcludeList excludeList)
      throws IOException {
    // Bound this at a minimum of 1 byte in case a request is made for a very
    // small size, which when divided by EC DataNum is zero.
    long requiredSpace = Math.max(1, size / repConfig.getData());
    synchronized (this) {
      int openPipelineCount = pipelineManager.getPipelineCount(repConfig,
          Pipeline.PipelineState.OPEN);
      if (openPipelineCount < calcMinimumPipelines(repConfig)) {
        try {
          ContainerInfo newContainer = allocateContainer(
              repConfig, requiredSpace, owner, excludeList);
          pipelineAllocatedSpaceMap.put(newContainer.getPipelineID(),
              requiredSpace);
          return newContainer;
        } catch (IOException e) {
          LOG.warn("Unable to allocate a container for {} with {} existing "
              + "containers", repConfig, openPipelineCount, e);
        }
      }

      int maxPipelinesLimit = calcMaximumPipelines(repConfig);
      if (openPipelineCount >= maxPipelinesLimit) {
        // Here we don't force throttling on max pipelines limit reached,
        // just hint that we should stop pre-allocation and always reuse
        // existing pipelines.
        maxPipelinesLimitReached = true;
        LOG.warn("Number of pipelines {} greater than max {} for "
            + "EC Replication {}", openPipelineCount, maxPipelinesLimit,
            repConfig);
      }
    }

    List<Pipeline> existingPipelines = pipelineManager.getPipelines(
        repConfig, Pipeline.PipelineState.OPEN,
        excludeList.getDatanodes(), excludeList.getPipelineIds());

    PipelineRequestInformation pri =
        PipelineRequestInformation.Builder.getBuilder()
            .setSize(requiredSpace)
            .build();
    ContainerInfo containerInfo = null;

    while (existingPipelines.size() > 0) {
      Pipeline pipeline =
          pipelineChoosePolicy.choosePipeline(existingPipelines, pri);
      if (pipeline == null) {
        LOG.warn("Unable to select a pipeline from {} in the list",
            existingPipelines.size());
        break;
      }

      synchronized (pipeline.getId()) {
        try {
          containerInfo = getContainerFromPipeline(pipeline);
          if (containerInfo == null
              || !containerHasSpace(containerInfo, requiredSpace)) {
            // This is O(n), which isn't great if there are a lot of pipelines
            // and we keep finding pipelines without enough space.
            existingPipelines.remove(pipeline);
            pipelineAllocatedSpaceMap.remove(pipeline.getId());
            pipelineManager.closePipeline(pipeline, true);
          } else {
            if (containerIsExcluded(containerInfo, excludeList)) {
              existingPipelines.remove(pipeline);
            } else {
              // Found a matching container
              break;
            }
          }
        } catch (PipelineNotFoundException | ContainerNotFoundException e) {
          LOG.warn("Pipeline or container not found when selecting a writable "
              + "container", e);
          existingPipelines.remove(pipeline);
          pipelineAllocatedSpaceMap.remove(pipeline.getId());
          pipelineManager.closePipeline(pipeline, true);
        }
      }
    }

    if (containerInfo != null) {
      // Preallocate a new container if an existing container is predicted
      // to be full. We don't return the pre-allocated one, just offer an
      // extra choice for the next round, it helps to spread loads across.
      preallocateContainerIfNeeded(containerInfo, requiredSpace, repConfig,
          owner, excludeList, existingPipelines);
      pipelineAllocatedSpaceMap.compute(containerInfo.getPipelineID(),
          (pipelineID, space) -> space == null ?
              requiredSpace : space + requiredSpace);
      return containerInfo;
    }

    // If we get here, all the pipelines we tried were no good. So try to
    // allocate a new one.
    try {
      synchronized (this) {
        ContainerInfo newContainer = allocateContainer(
            repConfig, requiredSpace, owner, excludeList);
        pipelineAllocatedSpaceMap.put(newContainer.getPipelineID(),
            requiredSpace);
        return newContainer;
      }
    } catch (IOException e) {
      LOG.error("Unable to allocate a container for {} after trying all "
          + "existing containers", repConfig, e);
      return null;
    }
  }

  private ContainerInfo allocateContainer(ReplicationConfig repConfig,
      long size, String owner, ExcludeList excludeList) throws IOException {

    List<DatanodeDetails> excludedNodes = Collections.emptyList();
    if (excludeList.getDatanodes().size() > 0) {
      excludedNodes = new ArrayList<>(excludeList.getDatanodes());
    }

    Pipeline newPipeline = pipelineManager.createPipeline(repConfig,
        excludedNodes, Collections.emptyList());
    ContainerInfo container =
        containerManager.getMatchingContainer(size, owner, newPipeline);
    pipelineManager.openPipeline(newPipeline.getId());
    return container;
  }

  private synchronized void preallocateContainerIfNeeded(
      ContainerInfo containerInfo, long requiredSpace,
      ECReplicationConfig repConfig, String owner,
      ExcludeList excludeList, List<Pipeline> existingPipelines) {

    if (maxPipelinesLimitReached) {
      // If we reached the max once, stop pre-allocation.
      if (existingPipelines.size() >= calcMinimumPipelines(repConfig)) {
        return;
      }
      // Below the min again after max reached, restart pre-allocation.
      maxPipelinesLimitReached = false;
    }

    long allocatedSpace = pipelineAllocatedSpaceMap.getOrDefault(
        containerInfo.getPipelineID(), 0L);

    if (allocatedSpace + requiredSpace > containerSize) {
      // Rollback the tracked space so we don't preallocate on every
      // request after container full predicted.
      pipelineAllocatedSpaceMap.put(containerInfo.getPipelineID(), 0L);
      try {
        allocateContainer(repConfig, requiredSpace, owner, excludeList);
      } catch (IOException e) {
        LOG.error("Unable to preallocate a container for {} upon detecting" +
            " container {} is possibly full",
            repConfig, containerInfo.containerID(), e);
      }
    }
  }

  private boolean containerIsExcluded(ContainerInfo container,
      ExcludeList excludeList) {
    return excludeList.getContainerIds().contains(container.containerID());
  }

  private ContainerInfo getContainerFromPipeline(Pipeline pipeline)
      throws IOException {
    // Assume the container is still open if the below method returns it. On
    // container FINALIZE, ContainerManager will remove the container from the
    // pipeline list in PipelineManager. Finalize can be triggered by a DN
    // sending a message that the container is full, or on close pipeline or
    // on a stale / dead node event (via close pipeline).
    NavigableSet<ContainerID> containers =
        pipelineManager.getContainersInPipeline(pipeline.getId());
    // Assume 1 container per pipeline for EC
    if (containers.size() == 0) {
      return null;
    }
    ContainerID containerID = containers.first();
    return containerManager.getContainer(containerID);
  }

  private boolean containerHasSpace(ContainerInfo container, long size) {
    // The size passed from OM will be the cluster block size. Therefore we
    // just check if the container has enough free space to accommodate another
    // full block.
    return container.getUsedBytes() + size <= containerSize;
  }

  private long getConfiguredContainerSize() {
    return (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, BYTES);
  }

  private int calcMinimumPipelines(ECReplicationConfig repConfig) {
    int minPipelines = providerConfig.getMinimumPipelines();
    if (minPipelines != 0) {
      return minPipelines;
    }

    int maxPipelines = calcMaximumPipelines(repConfig);
    return (int)Math.ceil(providerConfig.getMinMaxRatio() * maxPipelines);
  }

  private int calcMaximumPipelines(ECReplicationConfig repConfig) {
    return providerConfig.getMaximumPipelinesPerDatanode() *
        nodeManager.getNodeCount(NodeStatus.inServiceHealthy()) /
        repConfig.getRequiredNodes();
  }

  /**
   * Class to hold configuration for WriteableECContainerProvider.
   */
  @ConfigGroup(prefix = "ozone.scm.ec")
  public static class WritableECContainerProviderConfig {

    @Config(key = "pipeline.minimum",
        defaultValue = "0",
        type = ConfigType.INT,
        description = "The minimum number of pipelines to have open for each " +
            "Erasure Coding configuration",
        tags = ConfigTag.STORAGE)
    private int minimumPipelines = 0;

    public int getMinimumPipelines() {
      return minimumPipelines;
    }

    public void setMinimumPipelines(int minPipelines) {
      this.minimumPipelines = minPipelines;
    }

    @Config(key = "pipeline.minimum.maximum.ratio",
        defaultValue = "50",
        type = ConfigType.INT,
        description = "The ratio of the minimum and maximum number of" +
            " pipelines for each Erasure Coding configuration. The value" +
            " is an int in range [0, 100]",
        tags = ConfigTag.STORAGE)
    private int minMaxRatio = 50;

    public double getMinMaxRatio() {
      return (double)minMaxRatio / 100;
    }

    public void setMinMaxRatio(int minMaxRatio) {
      this.minMaxRatio = minMaxRatio;
    }

    @Config(key = "pipeline.maximum.per.datanode",
        defaultValue = "5",
        type = ConfigType.INT,
        description = "The maximum number of pipelines to join in for each " +
            "Datanode per each Erasure Coding configuration",
        tags = ConfigTag.STORAGE)
    private int maximumPipelinesPerDatanode = 5;

    public int getMaximumPipelinesPerDatanode() {
      return maximumPipelinesPerDatanode;
    }

    public void setMaximumPipelinesPerDatanode(int maxPipelinesPerDN) {
      this.maximumPipelinesPerDatanode = maxPipelinesPerDN;
    }

  }

}
