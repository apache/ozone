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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.utils.db.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;

/**
 * Manages the state of pipelines in SCM. All write operations like pipeline
 * creation, removal and updates should come via SCMPipelineManager.
 * PipelineStateMap class holds the data structures related to pipeline and its
 * state. All the read and write operations in PipelineStateMap are protected
 * by a read write lock.
 */
public class PipelineStateManager implements StateManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineStateManager.class);

  private final PipelineStateMap pipelineStateMap;

  public PipelineStateManager() {
    this.pipelineStateMap = new PipelineStateMap();
  }
  @Override
  public void addPipeline(Pipeline pipeline) throws IOException {
    pipelineStateMap.addPipeline(pipeline);
    LOG.info("Created pipeline {}", pipeline);
  }

  @Override
  public void addContainerToPipeline(PipelineID pipelineId,
                                     ContainerID containerID)
      throws IOException {
    pipelineStateMap.addContainerToPipeline(pipelineId, containerID);
  }

  @Override
  public Pipeline getPipeline(PipelineID pipelineID)
      throws PipelineNotFoundException {
    return pipelineStateMap.getPipeline(pipelineID);
  }

  @Override
  public List<Pipeline> getPipelines() {
    return pipelineStateMap.getPipelines();
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationType type) {
    return pipelineStateMap.getPipelines(type);
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationType type,
                                     ReplicationFactor factor) {
    return pipelineStateMap.getPipelines(type, factor);
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationType type,
                                     ReplicationFactor factor,
      PipelineState state) {
    return pipelineStateMap.getPipelines(type, factor, state);
  }

  @Override
  public List<Pipeline> getPipelines(
      ReplicationType type, ReplicationFactor factor,
      PipelineState state, Collection<DatanodeDetails> excludeDns,
      Collection<PipelineID> excludePipelines) {
    return pipelineStateMap
        .getPipelines(type, factor, state, excludeDns, excludePipelines);
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationType type,
                                     PipelineState... states) {
    return pipelineStateMap.getPipelines(type, states);
  }

  @Override
  public NavigableSet<ContainerID> getContainers(PipelineID pipelineID)
      throws IOException {
    return pipelineStateMap.getContainers(pipelineID);
  }

  @Override
  public int getNumberOfContainers(PipelineID pipelineID) throws IOException {
    return pipelineStateMap.getNumberOfContainers(pipelineID);
  }

  @Override
  public Pipeline removePipeline(PipelineID pipelineID) throws IOException {
    Pipeline pipeline = pipelineStateMap.removePipeline(pipelineID);
    LOG.info("Pipeline {} removed from db", pipeline);
    return pipeline;
  }

  @Override
  public void removeContainerFromPipeline(PipelineID pipelineID,
      ContainerID containerID) throws IOException {
    pipelineStateMap.removeContainerFromPipeline(pipelineID, containerID);
  }

  @Override
  public Pipeline finalizePipeline(PipelineID pipelineId)
      throws IOException {
    Pipeline pipeline = pipelineStateMap.getPipeline(pipelineId);
    if (!pipeline.isClosed()) {
      pipeline = pipelineStateMap
          .updatePipelineState(pipelineId, PipelineState.CLOSED);
      LOG.info("Pipeline {} moved to CLOSED state", pipeline);
    }
    return pipeline;
  }

  @Override
  public Pipeline openPipeline(PipelineID pipelineId) throws IOException {
    Pipeline pipeline = pipelineStateMap.getPipeline(pipelineId);
    if (pipeline.isClosed()) {
      throw new IOException("Closed pipeline can not be opened");
    }
    if (pipeline.getPipelineState() == PipelineState.ALLOCATED) {
      LOG.info("Pipeline {} moved to OPEN state", pipeline);
      pipeline = pipelineStateMap
          .updatePipelineState(pipelineId, PipelineState.OPEN);
    }
    return pipeline;
  }

  /**
   * Activates a dormant pipeline.
   *
   * @param pipelineID ID of the pipeline to activate.
   * @throws IOException in case of any Exception
   */
  @Override
  public void activatePipeline(PipelineID pipelineID)
      throws IOException {
    pipelineStateMap
        .updatePipelineState(pipelineID, PipelineState.OPEN);
  }

  /**
   * Deactivates an active pipeline.
   *
   * @param pipelineID ID of the pipeline to deactivate.
   * @throws IOException in case of any Exception
   */
  @Override
  public void deactivatePipeline(PipelineID pipelineID)
      throws IOException {
    pipelineStateMap
        .updatePipelineState(pipelineID, PipelineState.DORMANT);
  }

  @Override
  public void reinitialize(Table<PipelineID, Pipeline> pipelineStore)
      throws IOException {
  }

  @Override
  public void updatePipelineState(PipelineID id, PipelineState newState)
      throws PipelineNotFoundException {
    pipelineStateMap.updatePipelineState(id, newState);
  }

  @Override
  public void addPipeline(HddsProtos.Pipeline pipelineProto)
      throws IOException {
    throw new IOException("Not supported.");
  }

  @Override
  public void removePipeline(HddsProtos.PipelineID pipelineIDProto)
      throws IOException {
    throw new IOException("Not supported.");
  }

  @Override
  public void updatePipelineState(
      HddsProtos.PipelineID pipelineIDProto, HddsProtos.PipelineState newState)
      throws IOException {
    throw new IOException("Not supported.");
  }

  @Override
  public void close() {
    // Do nothing
  }
}
