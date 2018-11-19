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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Manages the state of pipelines in SCM. All write operations like pipeline
 * creation, removal and updates should come via SCMPipelineManager.
 * PipelineStateMap class holds the data structures related to pipeline and its
 * state. All the read and write operations in PipelineStateMap are protected
 * by a read write lock.
 */
class PipelineStateManager {

  private final PipelineStateMap pipelineStateMap;

  PipelineStateManager(Configuration conf) {
    this.pipelineStateMap = new PipelineStateMap();
  }

  void addPipeline(Pipeline pipeline) throws IOException {
    pipelineStateMap.addPipeline(pipeline);
  }

  void addContainerToPipeline(PipelineID pipelineId, ContainerID containerID)
      throws IOException {
    pipelineStateMap.addContainerToPipeline(pipelineId, containerID);
  }

  Pipeline getPipeline(PipelineID pipelineID) throws PipelineNotFoundException {
    return pipelineStateMap.getPipeline(pipelineID);
  }

  public List<Pipeline> getPipelines() {
    return pipelineStateMap.getPipelines();
  }

  List<Pipeline> getPipelines(ReplicationType type) {
    return pipelineStateMap.getPipelines(type);
  }

  List<Pipeline> getPipelines(ReplicationType type, ReplicationFactor factor) {
    return pipelineStateMap.getPipelines(type, factor);
  }

  List<Pipeline> getPipelines(ReplicationType type, ReplicationFactor factor,
      PipelineState state) {
    return pipelineStateMap.getPipelines(type, factor, state);
  }

  List<Pipeline> getPipelines(ReplicationType type, PipelineState... states) {
    return pipelineStateMap.getPipelines(type, states);
  }

  Set<ContainerID> getContainers(PipelineID pipelineID) throws IOException {
    return pipelineStateMap.getContainers(pipelineID);
  }

  int getNumberOfContainers(PipelineID pipelineID) throws IOException {
    return pipelineStateMap.getNumberOfContainers(pipelineID);
  }

  Pipeline removePipeline(PipelineID pipelineID) throws IOException {
    return pipelineStateMap.removePipeline(pipelineID);
  }

  void removeContainerFromPipeline(PipelineID pipelineID,
      ContainerID containerID) throws IOException {
    pipelineStateMap.removeContainerFromPipeline(pipelineID, containerID);
  }

  Pipeline finalizePipeline(PipelineID pipelineId)
      throws PipelineNotFoundException {
    Pipeline pipeline = pipelineStateMap.getPipeline(pipelineId);
    if (!pipeline.isClosed()) {
      pipeline = pipelineStateMap
          .updatePipelineState(pipelineId, PipelineState.CLOSED);
    }
    return pipeline;
  }

  Pipeline openPipeline(PipelineID pipelineId) throws IOException {
    Pipeline pipeline = pipelineStateMap.getPipeline(pipelineId);
    if (pipeline.isClosed()) {
      throw new IOException("Closed pipeline can not be opened");
    }
    if (pipeline.getPipelineState() == PipelineState.ALLOCATED) {
      pipeline = pipelineStateMap
          .updatePipelineState(pipelineId, PipelineState.OPEN);
    }
    return pipeline;
  }
}
