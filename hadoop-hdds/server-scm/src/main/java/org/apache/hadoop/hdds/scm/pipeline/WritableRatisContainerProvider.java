/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Class to obtain a writable container for Ratis and Standalone pipelines.
 */
public class WritableRatisContainerProvider
    implements WritableContainerProvider<ReplicationConfig> {

  private static final Logger LOG = LoggerFactory
      .getLogger(WritableRatisContainerProvider.class);

  private final ConfigurationSource conf;
  private final PipelineManager pipelineManager;
  private final PipelineChoosePolicy pipelineChoosePolicy;
  private final ContainerManager containerManager;

  public WritableRatisContainerProvider(ConfigurationSource conf,
      PipelineManager pipelineManager,
      ContainerManager containerManager,
      PipelineChoosePolicy pipelineChoosePolicy) {
    this.conf = conf;
    this.pipelineManager = pipelineManager;
    this.containerManager = containerManager;
    this.pipelineChoosePolicy = pipelineChoosePolicy;
  }


  @Override
  public ContainerInfo getContainer(final long size,
      ReplicationConfig repConfig, String owner, ExcludeList excludeList)
      throws IOException, TimeoutException {
    /*
      Here is the high level logic.

      1. We try to find pipelines in open state.

      2. If there are no pipelines in OPEN state, then we try to create one.

      3. We allocate a block from the available containers in the selected
      pipeline.

      TODO : #CLUTIL Support random picking of two containers from the list.
      So we can use different kind of policies.
    */

    ContainerInfo containerInfo = null;

    //TODO we need to continue the refactor to use repConfig everywhere
    //in downstream managers.


    while (true) {
      List<Pipeline> availablePipelines;
      Pipeline pipeline;
      // Acquire pipeline manager lock, to avoid any updates to pipeline
      // while allocate container happens. This is to avoid scenario like
      // mentioned in HDDS-5655.
      pipelineManager.acquireReadLock();
      try {
        availablePipelines =
                findPipelinesByState(repConfig,
                        excludeList,
                        Pipeline.PipelineState.OPEN);
        if (availablePipelines.size() != 0) {
          containerInfo = selectContainer(availablePipelines, size, owner,
              excludeList);
        }
        if (containerInfo != null) {
          return containerInfo;
        }
      } finally {
        pipelineManager.releaseReadLock();
      }

      if (availablePipelines.size() == 0) {
        try {
          // TODO: #CLUTIL Remove creation logic when all replication types
          //  and factors are handled by pipeline creator
          pipeline = pipelineManager.createPipeline(repConfig);

          // wait until pipeline is ready
          pipelineManager.waitPipelineReady(pipeline.getId(), 0);

        } catch (SCMException se) {
          LOG.warn("Pipeline creation failed for repConfig {} " +
              "Datanodes may be used up. Try to see if any pipeline is in " +
                  "ALLOCATED state, and then will wait for it to be OPEN",
                  repConfig, se);
          List<Pipeline> allocatedPipelines = findPipelinesByState(repConfig,
                  excludeList,
                  Pipeline.PipelineState.ALLOCATED);
          if (!allocatedPipelines.isEmpty()) {
            List<PipelineID> allocatedPipelineIDs =
                    allocatedPipelines.stream()
                            .map(p -> p.getId())
                            .collect(Collectors.toList());
            try {
              pipelineManager
                      .waitOnePipelineReady(allocatedPipelineIDs, 0);
            } catch (IOException e) {
              LOG.warn("Waiting for one of pipelines {} to be OPEN failed. ",
                      allocatedPipelineIDs, e);
            }
          }
        } catch (IOException e) {
          LOG.warn("Pipeline creation failed for repConfig: {}. "
              + "Retrying get pipelines call once.", repConfig, e);
        }

        pipelineManager.acquireReadLock();
        try {
          // If Exception occurred or successful creation of pipeline do one
          // final try to fetch pipelines.
          availablePipelines = findPipelinesByState(repConfig,
                  excludeList,
                  Pipeline.PipelineState.OPEN);
          if (availablePipelines.size() == 0) {
            LOG.info("Could not find available pipeline of repConfig: {} "
                + "even after retrying", repConfig);
            break;
          }
          containerInfo = selectContainer(availablePipelines, size, owner,
              excludeList);
          if (containerInfo != null) {
            return containerInfo;
          }
        } finally {
          pipelineManager.releaseReadLock();
        }
      }
    }

    // we have tried all strategies we know and but somehow we are not able
    // to get a container for this block. Log that info and return a null.
    LOG.error(
        "Unable to allocate a block for the size: {}, repConfig: {}",
        size, repConfig);
    return null;
  }

  private List<Pipeline> findPipelinesByState(
          final ReplicationConfig repConfig,
          final ExcludeList excludeList,
          final Pipeline.PipelineState pipelineState) {
    List<Pipeline> pipelines = pipelineManager.getPipelines(repConfig,
            pipelineState, excludeList.getDatanodes(),
            excludeList.getPipelineIds());
    if (pipelines.size() == 0 && !excludeList.isEmpty()) {
      // if no pipelines can be found, try finding pipeline without
      // exclusion
      pipelines = pipelineManager.getPipelines(repConfig, pipelineState);
    }
    return pipelines;
  }

  private ContainerInfo selectContainer(List<Pipeline> availablePipelines,
      long size, String owner, ExcludeList excludeList) {
    Pipeline pipeline;
    ContainerInfo containerInfo;

    PipelineRequestInformation pri =
        PipelineRequestInformation.Builder.getBuilder().setSize(size)
                .build();
    pipeline = pipelineChoosePolicy.choosePipeline(
            availablePipelines, pri);

    // look for OPEN containers that match the criteria.
    containerInfo = containerManager.getMatchingContainer(size, owner,
        pipeline, excludeList.getContainerIds());

    return containerInfo;

  }

}
