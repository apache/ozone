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
import org.apache.hadoop.hdds.conf.PostConstruct;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;

import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;

/**
 * Writable Container provider to obtain a writable container for EC pipelines.
 */
public class WritableECContainerProvider
    implements WritableContainerProvider<ECReplicationConfig> {

  private static final Logger LOG = LoggerFactory
      .getLogger(WritableECContainerProvider.class);

  private final NodeManager nodeManager;
  private final PipelineManager pipelineManager;
  private final PipelineChoosePolicy pipelineChoosePolicy;
  private final ContainerManager containerManager;
  private final long containerSize;
  private final WritableECContainerProviderConfig providerConfig;

  public WritableECContainerProvider(WritableECContainerProviderConfig config,
      long containerSize,
      NodeManager nodeManager,
      PipelineManager pipelineManager,
      ContainerManager containerManager,
      PipelineChoosePolicy pipelineChoosePolicy) {
    this.providerConfig = config;
    this.nodeManager = nodeManager;
    this.pipelineManager = pipelineManager;
    this.containerManager = containerManager;
    this.pipelineChoosePolicy = pipelineChoosePolicy;
    this.containerSize = containerSize;
  }

  /**
   *
   * @param size The max size of block in bytes which will be written. This
   *             comes from Ozone Manager and will be the block size configured
   *             for the cluster. The client cannot pass any arbitrary value
   *             from this setting.
   * @param repConfig The replication Config indicating the EC data and partiy
   *                  block counts.
   * @param owner The owner of the container
   * @param excludeList A set of datanodes, container and pipelines which should
   *                    not be considered.
   * @return A containerInfo representing a block group with space for the
   *         write, or null if no container can be allocated.
   */
  @Override
  public ContainerInfo getContainer(final long size,
      ECReplicationConfig repConfig, String owner, ExcludeList excludeList)
      throws IOException {
    int maximumPipelines = getMaximumPipelines(repConfig);
    int openPipelineCount = 0;
    synchronized (this) {
      openPipelineCount = pipelineManager.getPipelineCount(repConfig,
          Pipeline.PipelineState.OPEN);
      if (openPipelineCount < maximumPipelines) {
        try {
          return allocateContainer(repConfig, size, owner, excludeList);
        } catch (IOException e) {
          LOG.warn("Unable to allocate a container for {} with {} existing "
              + "containers", repConfig, openPipelineCount, e);
        }
      }
    }
    List<Pipeline> existingPipelines = pipelineManager.getPipelines(
        repConfig, Pipeline.PipelineState.OPEN,
        excludeList.getDatanodes(), excludeList.getPipelineIds());

    PipelineRequestInformation pri =
        PipelineRequestInformation.Builder.getBuilder()
            .setSize(size)
            .build();
    while (existingPipelines.size() > 0) {
      int pipelineIndex =
          pipelineChoosePolicy.choosePipelineIndex(existingPipelines, pri);
      if (pipelineIndex < 0) {
        LOG.warn("Unable to select a pipeline from {} in the list",
            existingPipelines.size());
        break;
      }
      Pipeline pipeline = existingPipelines.get(pipelineIndex);
      synchronized (pipeline.getId()) {
        try {
          ContainerInfo containerInfo = getContainerFromPipeline(pipeline);
          if (containerInfo == null
              || !containerHasSpace(containerInfo, size)) {
            existingPipelines.remove(pipelineIndex);
            pipelineManager.closePipeline(pipeline, true);
            openPipelineCount--;
          } else {
            if (containerIsExcluded(containerInfo, excludeList)) {
              existingPipelines.remove(pipelineIndex);
            } else {
              containerInfo.updateLastUsedTime();
              return containerInfo;
            }
          }
        } catch (PipelineNotFoundException | ContainerNotFoundException e) {
          LOG.warn("Pipeline or container not found when selecting a writable "
              + "container", e);
          existingPipelines.remove(pipelineIndex);
          pipelineManager.closePipeline(pipeline, true);
          openPipelineCount--;
        }
      }
    }
    // If we get here, all the pipelines we tried were no good. So try to
    // allocate a new one.
    try {
      synchronized (this) {
        if (openPipelineCount < maximumPipelines) {
          return allocateContainer(repConfig, size, owner, excludeList);
        }
        throw new IOException("Unable to allocate a pipeline for "
            + repConfig + " after trying all existing pipelines as the max "
            + "limit has been reached and no pipelines where closed");
      }
    } catch (IOException e) {
      LOG.error("Unable to allocate a container for {} after trying all "
          + "existing containers", repConfig, e);
      throw e;
    }
  }

  private int getMaximumPipelines(ECReplicationConfig repConfig) {
    final double factor = providerConfig.getPipelinePerVolumeFactor();
    int volumeBasedCount = 0;
    if (factor > 0) {
      int volumes = nodeManager.totalHealthyVolumeCount();
      volumeBasedCount = (int) factor * volumes / repConfig.getRequiredNodes();
    }
    return Math.max(volumeBasedCount, providerConfig.getMinimumPipelines());
  }

  private ContainerInfo allocateContainer(ReplicationConfig repConfig,
      long size, String owner, ExcludeList excludeList)
      throws IOException {

    List<DatanodeDetails> excludedNodes = Collections.emptyList();
    if (excludeList.getDatanodes().size() > 0) {
      excludedNodes = new ArrayList<>(excludeList.getDatanodes());
    }

    Pipeline newPipeline = pipelineManager.createPipeline(repConfig,
        excludedNodes, Collections.emptyList());
    ContainerInfo container =
        containerManager.getMatchingContainer(size, owner, newPipeline);
    pipelineManager.openPipeline(newPipeline.getId());
    LOG.info("Created and opened new pipeline {}", newPipeline);
    return container;
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

  /**
   * Class to hold configuration for WriteableECContainerProvider.
   */
  @ConfigGroup(prefix = WritableECContainerProviderConfig.PREFIX)
  public static class WritableECContainerProviderConfig {

    private static final String PREFIX = "ozone.scm.ec";

    @Config(key = "pipeline.minimum",
        defaultValue = "5",
        type = ConfigType.INT,
        description = "The minimum number of pipelines to have open for each " +
            "Erasure Coding configuration",
        tags = ConfigTag.STORAGE)
    private int minimumPipelines = 5;

    public int getMinimumPipelines() {
      return minimumPipelines;
    }

    public void setMinimumPipelines(int minPipelines) {
      this.minimumPipelines = minPipelines;
    }

    private static final String PIPELINE_PER_VOLUME_FACTOR_KEY =
        "pipeline.per.volume.factor";
    private static final double PIPELINE_PER_VOLUME_FACTOR_DEFAULT = 1;
    private static final String PIPELINE_PER_VOLUME_FACTOR_DEFAULT_VALUE = "1";
    private static final String EC_PIPELINE_PER_VOLUME_FACTOR_KEY =
        PREFIX + "." + PIPELINE_PER_VOLUME_FACTOR_KEY;

    @Config(key = PIPELINE_PER_VOLUME_FACTOR_KEY,
        type = ConfigType.DOUBLE,
        defaultValue = PIPELINE_PER_VOLUME_FACTOR_DEFAULT_VALUE,
        tags = {SCM},
        description = "TODO"
    )
    private double pipelinePerVolumeFactor = PIPELINE_PER_VOLUME_FACTOR_DEFAULT;

    public double getPipelinePerVolumeFactor() {
      return pipelinePerVolumeFactor;
    }

    @PostConstruct
    public void validate() {
      if (pipelinePerVolumeFactor < 0) {
        LOG.warn("{} must be non-negative, but was {}. Defaulting to {}",
            EC_PIPELINE_PER_VOLUME_FACTOR_KEY, pipelinePerVolumeFactor,
            PIPELINE_PER_VOLUME_FACTOR_DEFAULT);
        pipelinePerVolumeFactor = PIPELINE_PER_VOLUME_FACTOR_DEFAULT;
      }
    }

    public void setPipelinePerVolumeFactor(double v) {
      pipelinePerVolumeFactor = v;
    }
  }

}
