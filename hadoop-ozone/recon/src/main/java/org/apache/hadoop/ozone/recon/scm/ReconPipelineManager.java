/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.CLOSED;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_SCM_PIPELINE_DB;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineStateManager;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Recon's overriding implementation of SCM's Pipeline Manager.
 */
public class ReconPipelineManager extends SCMPipelineManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconPipelineManager.class);

  public ReconPipelineManager(Configuration conf,
                              NodeManager nodeManager,
                              EventPublisher eventPublisher)
      throws IOException {
    super(conf, nodeManager, eventPublisher, new PipelineStateManager(),
        new ReconPipelineFactory());
    initializePipelineState();
  }

  @Override
  protected File getPipelineDBPath(Configuration conf) {
    File metaDir = ReconUtils.getReconScmDbDir(conf);
    return new File(metaDir, RECON_SCM_PIPELINE_DB);
  }

  @Override
  public void triggerPipelineCreation() {
    // Don't do anything in Recon.
  }

  @Override
  protected void destroyPipeline(Pipeline pipeline) throws IOException {
    // remove the pipeline from the pipeline manager
    removePipeline(pipeline.getId());
  }


  /**
   * Bootstrap Recon's pipeline metadata with that from SCM.
   * @param pipelinesFromScm pipelines from SCM.
   * @throws IOException on exception.
   */
  void initializePipelines(List<Pipeline> pipelinesFromScm) throws IOException {

    getLock().writeLock().lock();
    try {
      List<Pipeline> pipelinesInHouse = getPipelines();
      LOG.info("Recon has {} pipelines in house.", pipelinesInHouse.size());
      for (Pipeline pipeline : pipelinesFromScm) {
        if (!containsPipeline(pipeline.getId())) {
          // New pipeline got from SCM. Recon does not know anything about it,
          // so let's add it.
          LOG.info("Adding new pipeline {} from SCM.", pipeline.getId());
          addPipeline(pipeline);
        } else {
          // Recon already has this pipeline. Just update state and creation
          // time.
          getStateManager().updatePipelineState(pipeline.getId(),
              pipeline.getPipelineState());
          getPipeline(pipeline.getId()).setCreationTimestamp(
              pipeline.getCreationTimestamp());
        }
        removeInvalidPipelines(pipelinesFromScm);
      }
    } finally {
      getLock().writeLock().unlock();
    }
  }

  public void removeInvalidPipelines(List<Pipeline> pipelinesFromScm) {
    getLock().writeLock().lock();
    try {
      List<Pipeline> pipelinesInHouse = getPipelines();
      // Removing pipelines in Recon that are no longer in SCM.
      // TODO Recon may need to track inactive pipelines as well. So this can be
      // removed in a followup JIRA.
      List<Pipeline> invalidPipelines = pipelinesInHouse
          .stream()
          .filter(p -> !pipelinesFromScm.contains(p))
          .collect(Collectors.toList());
      invalidPipelines.forEach(p -> {
        PipelineID pipelineID = p.getId();
        if (!p.getPipelineState().equals(CLOSED)) {
          try {
            getStateManager().updatePipelineState(pipelineID, CLOSED);
          } catch (PipelineNotFoundException e) {
            LOG.warn("Pipeline {} not found while updating state. ",
                p.getId(), e);
          }
        }
        try {
          LOG.info("Removing invalid pipeline {} from Recon.", pipelineID);
          finalizeAndDestroyPipeline(p, false);
        } catch (IOException e) {
          LOG.warn("Unable to remove pipeline {}", pipelineID, e);
        }
      });
    } finally {
      getLock().writeLock().unlock();
    }
  }
  /**
   * Add a new pipeline to the pipeline metadata.
   * @param pipeline pipeline
   * @throws IOException
   */
  @VisibleForTesting
  void addPipeline(Pipeline pipeline) throws IOException {
    getLock().writeLock().lock();
    try {
      getPipelineStore().put(pipeline.getId().getProtobuf().toByteArray(),
          pipeline.getProtobufMessage().toByteArray());
      getStateManager().addPipeline(pipeline);
      getNodeManager().addPipeline(pipeline);
    } finally {
      getLock().writeLock().unlock();
    }
  }
}
