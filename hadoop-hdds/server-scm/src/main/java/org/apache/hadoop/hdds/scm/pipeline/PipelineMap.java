/*
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
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;

/**
 * Holds the data structures which maintain the information about pipeline and
 * its state.
 */
public interface PipelineMap {
  /**
   * Adds provided pipeline in the data structures.
   *
   * @param pipeline - Pipeline to add
   * @throws IOException if pipeline with provided pipelineID already exists
   */
  void addPipeline(Pipeline pipeline) throws IOException;

  /**
   * Add container to an existing pipeline.
   *
   * @param pipelineID  - PipelineID of the pipeline to which container is added
   * @param containerID - ContainerID of the container to add
   * @throws IOException if pipeline is not in open state or does not exist
   */
  void addContainerToPipeline(PipelineID pipelineID, ContainerID containerID)
      throws IOException;

  /**
   * Add container to an existing pipeline during SCM Start.
   *
   * @param pipelineID  - PipelineID of the pipeline to which container is added
   * @param containerID - ContainerID of the container to add
   */
  void addContainerToPipelineSCMStart(PipelineID pipelineID,
                                      ContainerID containerID)
      throws IOException;

  /**
   * Get pipeline corresponding to specified pipelineID.
   *
   * @param pipelineID - PipelineID of the pipeline to be retrieved
   * @return Pipeline
   * @throws PipelineNotFoundException if pipeline is not found
   */
  Pipeline getPipeline(PipelineID pipelineID) throws PipelineNotFoundException;

  /**
   * Get list of pipelines in SCM.
   *
   * @return List of pipelines
   */
  List<Pipeline> getPipelines();

  /**
   * Get pipeline corresponding to specified replication type.
   *
   * @param replicationConfig - ReplicationConfig
   * @return List of pipelines which have the specified replication type
   */
  List<Pipeline> getPipelines(ReplicationConfig replicationConfig);

  /**
   * Get list of pipeline corresponding to specified replication type,
   * replication factor and pipeline state.
   *
   * @param replicationConfig - ReplicationConfig
   * @param state             - Required PipelineState
   * @return List of pipelines with specified replication type,
   * replication factor and pipeline state
   */
  List<Pipeline> getPipelines(ReplicationConfig replicationConfig,
                              Pipeline.PipelineState state);

  /**
   * Get a count of pipelines with the given replicationConfig and state.
   * This method is most efficient when getting a count for OPEN pipeline
   * as the result can be obtained directly from the cached open list.
   *
   * @param replicationConfig - ReplicationConfig
   * @param state             - Required PipelineState
   * @return Count of pipelines with the specified replication config and state
   */
  int getPipelineCount(ReplicationConfig replicationConfig,
                       Pipeline.PipelineState state);

  /**
   * Get list of pipeline corresponding to specified replication type,
   * replication factor and pipeline state.
   *
   * @param replicationConfig - ReplicationType
   * @param state             - Required PipelineState
   * @param excludeDns        dns to exclude
   * @param excludePipelines  pipelines to exclude
   * @return List of pipelines with specified replication type,
   * replication factor and pipeline state
   */
  List<Pipeline> getPipelines(ReplicationConfig replicationConfig,
                              Pipeline.PipelineState state,
                              Collection<DatanodeDetails> excludeDns,
                              Collection<PipelineID> excludePipelines);

  /**
   * Get set of containerIDs corresponding to a pipeline.
   *
   * @param pipelineID - PipelineID
   * @return Set of containerIDs belonging to the pipeline
   * @throws PipelineNotFoundException if pipeline is not found
   */
  NavigableSet<ContainerID> getContainers(PipelineID pipelineID)
      throws PipelineNotFoundException;

  /**
   * Get number of containers corresponding to a pipeline.
   *
   * @param pipelineID - PipelineID
   * @return Number of containers belonging to the pipeline
   * @throws PipelineNotFoundException if pipeline is not found
   */
  int getNumberOfContainers(PipelineID pipelineID)
      throws PipelineNotFoundException;

  /**
   * Remove pipeline from the data structures.
   *
   * @param pipelineID - PipelineID of the pipeline to be removed
   * @throws IOException if the pipeline is not empty or does not exist
   */
  Pipeline removePipeline(PipelineID pipelineID) throws IOException;

  /**
   * Remove container from a pipeline.
   *
   * @param pipelineID  - PipelineID of the pipeline from which container needs
   *                    to be removed
   * @param containerID - ContainerID of the container to remove
   * @throws IOException if pipeline does not exist
   */
  void removeContainerFromPipeline(PipelineID pipelineID,
                                   ContainerID containerID) throws IOException;

  /**
   * Updates the state of pipeline.
   *
   * @param pipelineID - PipelineID of the pipeline whose state needs
   *                   to be updated
   * @param state      - new state of the pipeline
   * @return Pipeline with the updated state
   * @throws PipelineNotFoundException if pipeline does not exist
   */
  Pipeline updatePipelineState(PipelineID pipelineID,
                               Pipeline.PipelineState state)
      throws PipelineNotFoundException;
}
