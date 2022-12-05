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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.TimeoutException;

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
  private final long containerSize;
  private final WritableECContainerProviderConfig providerConfig;

  public WritableECContainerProvider(ConfigurationSource conf,
      PipelineManager pipelineManager, ContainerManager containerManager,
      PipelineChoosePolicy pipelineChoosePolicy) {
    this.conf = conf;
    this.providerConfig =
        conf.getObject(WritableECContainerProviderConfig.class);
    this.pipelineManager = pipelineManager;
    this.containerManager = containerManager;
    this.pipelineChoosePolicy = pipelineChoosePolicy;
    this.containerSize = getConfiguredContainerSize();
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
   * @return A containerInfo representing a block group with with space for the
   *         write, or null if no container can be allocated.
   * @throws IOException
   */
  @Override
  public ContainerInfo getContainer(final long size,
      ECReplicationConfig repConfig, String owner, ExcludeList excludeList)
      throws IOException, TimeoutException {
    synchronized (this) {
      int openPipelineCount = pipelineManager.getPipelineCount(repConfig,
          Pipeline.PipelineState.OPEN);
      if (openPipelineCount < providerConfig.getMinimumPipelines()) {
        try {
          return allocateContainer(
              repConfig, size, owner, excludeList);
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
      Pipeline pipeline =
          pipelineChoosePolicy.choosePipeline(existingPipelines, pri);
      if (pipeline == null) {
        LOG.warn("Unable to select a pipeline from {} in the list",
            existingPipelines.size());
        break;
      }
      synchronized (pipeline.getId()) {
        try {
          ContainerInfo containerInfo = getContainerFromPipeline(pipeline);
          if (containerInfo == null
              || !containerHasSpace(containerInfo, size)) {
            // This is O(n), which isn't great if there are a lot of pipelines
            // and we keep finding pipelines without enough space.
            existingPipelines.remove(pipeline);
            pipelineManager.closePipeline(pipeline, true);
          } else {
            if (containerIsExcluded(containerInfo, excludeList)) {
              existingPipelines.remove(pipeline);
            } else {
              return containerInfo;
            }
          }
        } catch (PipelineNotFoundException | ContainerNotFoundException e) {
          LOG.warn("Pipeline or container not found when selecting a writable "
              + "container", e);
          existingPipelines.remove(pipeline);
          pipelineManager.closePipeline(pipeline, true);
        }
      }
    }
    // If we get here, all the pipelines we tried were no good. So try to
    // allocate a new one and usePipelineManagerV2Impl.java it.
    try {
      synchronized (this) {
        return allocateContainer(repConfig, size, owner, excludeList);
      }
    } catch (IOException e) {
      LOG.error("Unable to allocate a container for {} after trying all "
          + "existing containers", repConfig, e);
      throw e;
    }
  }

  private ContainerInfo allocateContainer(ReplicationConfig repConfig,
      long size, String owner, ExcludeList excludeList)
      throws IOException, TimeoutException {

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

  /**
   * Class to hold configuration for WriteableECContainerProvider.
   */
  @ConfigGroup(prefix = "ozone.scm.ec")
  public static class WritableECContainerProviderConfig {

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

  }

}
