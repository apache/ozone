/*
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

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineFactory;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
import org.apache.hadoop.hdds.scm.pipeline.PipelineStateManagerImpl;
import org.apache.hadoop.hdds.scm.pipeline.PipelineStateManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.ozone.common.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.CLOSED;

/**
 * Recon's overriding implementation of SCM's Pipeline Manager.
 */
public final class ReconPipelineManager extends PipelineManagerImpl {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconPipelineManager.class);

  private ReconPipelineManager(ConfigurationSource conf,
                               SCMHAManager scmhaManager,
                               NodeManager nodeManager,
                               PipelineStateManager pipelineStateManager,
                               PipelineFactory pipelineFactory,
                               EventPublisher eventPublisher,
                               SCMContext scmContext) {
    super(conf, scmhaManager, nodeManager, pipelineStateManager,
        pipelineFactory, eventPublisher, scmContext,
        new MonotonicClock(ZoneOffset.UTC));
  }

  public static ReconPipelineManager newReconPipelineManager(
      ConfigurationSource conf,
      NodeManager nodeManager,
      Table<PipelineID, Pipeline> pipelineStore,
      EventPublisher eventPublisher,
      SCMHAManager scmhaManager,
      SCMContext scmContext) throws IOException {

    // Create PipelineStateManagerImpl
    PipelineStateManager stateManager = PipelineStateManagerImpl
        .newBuilder()
        .setPipelineStore(pipelineStore)
        .setNodeManager(nodeManager)
        .setRatisServer(scmhaManager.getRatisServer())
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();

    // Create PipelineFactory
    PipelineFactory pipelineFactory = new ReconPipelineFactory();

    return new ReconPipelineManager(conf, scmhaManager, nodeManager,
        stateManager, pipelineFactory, eventPublisher, scmContext);
  }

  /**
   * Bootstrap Recon's pipeline metadata with that from SCM.
   * @param pipelinesFromScm pipelines from SCM.
   * @throws IOException on exception.
   */
  void initializePipelines(List<Pipeline> pipelinesFromScm)
      throws IOException, TimeoutException {

    acquireWriteLock();
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
          getStateManager().updatePipelineState(
              pipeline.getId().getProtobuf(),
              Pipeline.PipelineState.getProtobuf(pipeline.getPipelineState()));
          getPipeline(pipeline.getId()).setCreationTimestamp(
              pipeline.getCreationTimestamp());
        }
        removeInvalidPipelines(pipelinesFromScm);
      }
    } finally {
      releaseWriteLock();
    }
  }

  public void removeInvalidPipelines(List<Pipeline> pipelinesFromScm) {
    acquireWriteLock();
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
            getStateManager().updatePipelineState(
                pipelineID.getProtobuf(),
                HddsProtos.PipelineState.PIPELINE_CLOSED);
          } catch (IOException | TimeoutException e) {
            LOG.warn("Pipeline {} not found while updating state. ",
                p.getId(), e);
          }
        }
        try {
          LOG.info("Removing invalid pipeline {} from Recon.", pipelineID);
          closePipeline(p, false);
        } catch (IOException | TimeoutException e) {
          LOG.warn("Unable to remove pipeline {}", pipelineID, e);
        }
      });
    } finally {
      releaseWriteLock();
    }
  }
  /**
   * Add a new pipeline to the pipeline metadata.
   * @param pipeline pipeline
   * @throws IOException
   */
  @VisibleForTesting
  public void addPipeline(Pipeline pipeline)
      throws IOException, TimeoutException {
    acquireWriteLock();
    try {
      getStateManager().addPipeline(
          pipeline.getProtobufMessage(ClientVersion.CURRENT_VERSION));
    } finally {
      releaseWriteLock();
    }
  }
}
