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

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

import static org.apache.hadoop.hdds.conf.StorageUnit.BYTES;

/**
 * Writable Container provider to obtain a writable container for EC pipelines.
 */
public class WritableECContainerProvider implements WritableContainerProvider {

  private static final Logger LOG = LoggerFactory
      .getLogger(WritableECContainerProvider.class);

  private static int minPipelines = 5;

  private final ConfigurationSource conf;
  private final PipelineManager pipelineManager;
  private final PipelineChoosePolicy pipelineChoosePolicy;
  private final ContainerManagerV2 containerManager;
  private final long containerSize;

  public WritableECContainerProvider(ConfigurationSource conf,
      PipelineManager pipelineManager, ContainerManagerV2 containerManager,
      PipelineChoosePolicy pipelineChoosePolicy) {
    this.conf = conf;
    this.pipelineManager = pipelineManager;
    this.containerManager = containerManager;
    this.pipelineChoosePolicy = pipelineChoosePolicy;
    this.containerSize = getConfiguredContainerSize();
  }

  @Override
  public ContainerInfo getContainer(final long size,
      ReplicationConfig repConfig, String owner, ExcludeList excludeList)
      throws IOException {
    // TODO - Locking on Pipeline ID to prevent concurrent issues?
    // TODO - exclude vs total available.
    List<Pipeline> existingPipelines = pipelineManager.getPipelines(
        repConfig, Pipeline.PipelineState.OPEN,
        excludeList.getDatanodes(), excludeList.getPipelineIds());

    if (existingPipelines.size() < minPipelines) {
      try {
        // TODO - PipelineManager should allow for creating a pipeline with
        //        excluded nodes.
        return allocateContainer(repConfig, size, owner);
      } catch (IOException e) {
        LOG.warn("Unable to allocate a container for {} with {} existing "
            + "containers", repConfig, existingPipelines.size(), e);
      }
    }

    PipelineRequestInformation pri =
        PipelineRequestInformation.Builder.getBuilder().setSize(size).build();
    Pipeline pipeline = null;
    while (existingPipelines.size() > 0) {
      try {
        pipeline = pipelineChoosePolicy.choosePipeline(existingPipelines, pri);
        ContainerInfo containerInfo = getContainerFromPipeline(pipeline);
        if (!containerHasSpace(containerInfo, size)) {
          // This is O(n), which isn't great if there are a lot of pipelines
          // and we keep finding pipelines without enough space.
          existingPipelines.remove(pipeline);
          // TODO - when does it make sense to close the pipeline? What if the client makes
          // unreasonable size requests, eg 2GB rather than the block size? Maybe we should
          // only close if there is less space than the cluster block size.
          // TODO - Also, for EC, what is the block size? If the client says 128MB, is that
          // 128MB / 6 (for EC-6-3?)
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
        if (pipeline != null) {
          existingPipelines.remove(pipeline);
          pipelineManager.closePipeline(pipeline, true);
        }
      }
    }
    // TODO - handle excluded nodes
    // TODO - skip this if the first allocate was tried and failed or try again?

    // If we get here, all the pipelines we tried were no good. So try to
    // allocate a new one and use it.
    try {
      return allocateContainer(repConfig, size, owner);
    } catch (IOException e) {
      LOG.error("Unable to allocate a container for {} after trying all "
          + "existing containers", repConfig, e);
      return null;
    }
  }

  // TODO - this could throw IOEXception (SCMException) from container policy
  //        if not enough nodes etc.
  private ContainerInfo allocateContainer(ReplicationConfig repConfig,
      long size, String owner) throws IOException {
    Pipeline newPipeline = pipelineManager.createPipeline(repConfig);
    return containerManager.getMatchingContainer(size, owner, newPipeline);
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
    ContainerID containerID = containers.first();
    if (containerID == null) {
      return null;
    }
    return containerManager.getContainer(containerID);
  }

  private boolean containerHasSpace(ContainerInfo container, long size) {
    return container.getUsedBytes() + size <= containerSize;
  }

  private long getConfiguredContainerSize() {
    return (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, BYTES);
  }

}
