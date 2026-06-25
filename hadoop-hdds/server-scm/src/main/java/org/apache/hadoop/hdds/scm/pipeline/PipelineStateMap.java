/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import static java.lang.String.format;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds the data structures which maintain the information about pipeline and
 * its state.
 * Invariant: If a pipeline exists in PipelineStateMap, both pipelineMap and
 * pipeline2container would have a non-null mapping for it.
 *
 * Concurrency consideration:
 *   - thread-unsafe
 */
class PipelineStateMap {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineStateMap.class);

  // TODO: Use TreeMap for range operations?
  private final Map<PipelineID, Pipeline> pipelineMap = new HashMap<>();
  private final Map<PipelineID, NavigableSet<ContainerID>> pipeline2container = new HashMap<>();
  private final Map<ReplicationConfig, List<Pipeline>> query2OpenPipelines = new HashMap<>();

  PipelineStateMap() { }

  /**
   * Adds provided pipeline in the data structures.
   *
   * @param pipeline - Pipeline to add
   * @throws IOException if pipeline with provided pipelineID already exists
   */
  void addPipeline(Pipeline pipeline) throws DuplicatedPipelineIdException {
    Objects.requireNonNull(pipeline, "Pipeline cannot be null");
    Preconditions.checkArgument(
        pipeline.getNodes().size() == pipeline.getReplicationConfig()
            .getRequiredNodes(),
        "Nodes size=%s, replication factor=%s do not match ",
            pipeline.getNodes().size(), pipeline.getReplicationConfig()
                .getRequiredNodes());

    if (pipelineMap.putIfAbsent(pipeline.getId(), pipeline) != null) {
      LOG.warn("Duplicate pipeline ID detected. {}", pipeline.getId());
      throw new DuplicatedPipelineIdException(
          format("Duplicate pipeline ID %s detected.", pipeline.getId()));
    }
    pipeline2container.put(pipeline.getId(), new TreeSet<>());
    if (pipeline.getPipelineState() == PipelineState.OPEN) {
      query2OpenPipelines.computeIfAbsent(pipeline.getReplicationConfig(), any -> new ArrayList<>())
          .add(pipeline);
    }
  }

  /**
   * Add container to an existing pipeline.
   *
   * @param pipelineID - PipelineID of the pipeline to which container is added
   * @param containerID - ContainerID of the container to add
   */
  void addContainerToPipeline(PipelineID pipelineID, ContainerID containerID)
      throws InvalidPipelineStateException, PipelineNotFoundException {
    Objects.requireNonNull(pipelineID,
        "Pipeline Id cannot be null");
    Objects.requireNonNull(containerID,
        "Container Id cannot be null");

    Pipeline pipeline = getPipeline(pipelineID);
    if (pipeline.isClosed()) {
      throw new InvalidPipelineStateException(format(
          "Cannot add container to pipeline=%s in closed state", pipelineID));
    }
    pipeline2container.get(pipelineID).add(containerID);
  }

  /**
   * Add container to an existing pipeline during SCM Start.
   *
   * @param pipelineID - PipelineID of the pipeline to which container is added
   * @param containerID - ContainerID of the container to add
   */
  void addContainerToPipelineSCMStart(PipelineID pipelineID, ContainerID containerID)
      throws PipelineNotFoundException {
    Objects.requireNonNull(pipelineID,
            "Pipeline Id cannot be null");
    Objects.requireNonNull(containerID,
            "Container Id cannot be null");

    Pipeline pipeline = getPipeline(pipelineID);
    if (pipeline.isClosed()) {
      /*
      When SCM restarts,the SCM DB may not be upto date where some
      containers are in an OPEN state for a CLOSED pipeline. This happens when
      close pipeline transaction in flushed before SCM goes down and close
      container is not flushed into DB.
      */
      LOG.info("Container {} in open state for pipeline={} in closed state",
              containerID, pipelineID);
    }
    pipeline2container.get(pipelineID).add(containerID);
  }

  /**
   * Get pipeline corresponding to specified pipelineID.
   *
   * @param pipelineID - PipelineID of the pipeline to be retrieved
   * @return Pipeline
   * @throws PipelineNotFoundException if pipeline is not found
   */
  Pipeline getPipeline(PipelineID pipelineID) throws PipelineNotFoundException {
    Objects.requireNonNull(pipelineID,
        "Pipeline Id cannot be null");

    Pipeline pipeline = pipelineMap.get(pipelineID);
    if (pipeline == null) {
      throw new PipelineNotFoundException(
          format("%s not found", pipelineID));
    }
    return pipeline;
  }

  /**
   * Get list of pipelines in SCM.
   * @return List of pipelines
   */
  public List<Pipeline> getPipelines() {
    return new ArrayList<>(pipelineMap.values());
  }

  /**
   * Get pipeline corresponding to specified replication type.
   *
   * @param replicationConfig - ReplicationConfig
   * @return List of pipelines which have the specified replication type
   */
  List<Pipeline> getPipelines(ReplicationConfig replicationConfig) {
    Objects.requireNonNull(replicationConfig, "ReplicationConfig cannot be null");

    List<Pipeline> pipelines = new ArrayList<>();
    for (Pipeline pipeline : pipelineMap.values()) {
      if (pipeline.getReplicationConfig().equals(replicationConfig)) {
        pipelines.add(pipeline);
      }
    }

    return pipelines;
  }

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
      PipelineState state) {
    Objects.requireNonNull(replicationConfig, "ReplicationConfig cannot be null");
    Objects.requireNonNull(state, "Pipeline state cannot be null");

    if (state == PipelineState.OPEN) {
      return new ArrayList<>(
          query2OpenPipelines.getOrDefault(
              replicationConfig, Collections.emptyList()));
    }

    List<Pipeline> pipelines = new ArrayList<>();
    for (Pipeline pipeline : pipelineMap.values()) {
      if (pipeline.getReplicationConfig().equals(replicationConfig)
          && pipeline.getPipelineState() == state) {
        pipelines.add(pipeline);
      }
    }

    return pipelines;
  }

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
      PipelineState state) {
    Objects.requireNonNull(replicationConfig, "ReplicationConfig cannot be null");
    Objects.requireNonNull(state, "Pipeline state cannot be null");

    if (state == PipelineState.OPEN) {
      return query2OpenPipelines.getOrDefault(
              replicationConfig, Collections.emptyList()).size();
    }

    int count = 0;
    for (Pipeline pipeline : pipelineMap.values()) {
      if (pipeline.getReplicationConfig().equals(replicationConfig)
          && pipeline.getPipelineState() == state) {
        count++;
      }
    }
    return count;
  }

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
      PipelineState state, Collection<DatanodeDetails> excludeDns,
      Collection<PipelineID> excludePipelines) {
    Objects.requireNonNull(replicationConfig, "ReplicationConfig cannot be null");
    Objects.requireNonNull(state, "Pipeline state cannot be null");
    Objects.requireNonNull(excludeDns, "Datanode exclude list cannot be null");
    Objects.requireNonNull(excludePipelines, "Pipeline exclude list cannot be null");

    List<Pipeline> pipelines = null;
    if (state == PipelineState.OPEN) {
      pipelines = new ArrayList<>(query2OpenPipelines.getOrDefault(
          replicationConfig, Collections.emptyList()));
      if (excludeDns.isEmpty() && excludePipelines.isEmpty()) {
        return pipelines;
      }
    } else {
      pipelines = new ArrayList<>(pipelineMap.values());
    }

    Iterator<Pipeline> iter = pipelines.iterator();
    while (iter.hasNext()) {
      Pipeline pipeline = iter.next();
      if (!pipeline.getReplicationConfig().equals(replicationConfig) ||
          pipeline.getPipelineState() != state ||
          excludePipelines.contains(pipeline.getId())) {
        iter.remove();
      } else {
        for (DatanodeDetails dn : pipeline.getNodes()) {
          if (excludeDns.contains(dn)) {
            iter.remove();
            break;
          }
        }
      }
    }

    return pipelines;
  }

  /**
   * Get set of containerIDs corresponding to a pipeline.
   *
   * @param pipelineID - PipelineID
   * @return Set of containerIDs belonging to the pipeline
   * @throws PipelineNotFoundException if pipeline is not found
   */
  NavigableSet<ContainerID> getContainers(PipelineID pipelineID)
      throws PipelineNotFoundException {
    Objects.requireNonNull(pipelineID,
        "Pipeline Id cannot be null");

    NavigableSet<ContainerID> containerIDs = pipeline2container.get(pipelineID);
    if (containerIDs == null) {
      throw new PipelineNotFoundException(
          format("%s not found", pipelineID));
    }
    return new TreeSet<>(containerIDs);
  }

  /**
   * Get number of containers corresponding to a pipeline.
   *
   * @param pipelineID - PipelineID
   * @return Number of containers belonging to the pipeline
   * @throws PipelineNotFoundException if pipeline is not found
   */
  int getNumberOfContainers(PipelineID pipelineID)
      throws PipelineNotFoundException {
    Objects.requireNonNull(pipelineID,
        "Pipeline Id cannot be null");

    Set<ContainerID> containerIDs = pipeline2container.get(pipelineID);
    if (containerIDs == null) {
      throw new PipelineNotFoundException(
          format("%s not found", pipelineID));
    }
    return containerIDs.size();
  }

  /**
   * Remove pipeline from the data structures.
   *
   * @param pipelineID - PipelineID of the pipeline to be removed
   */
  Pipeline removePipeline(PipelineID pipelineID) throws PipelineNotFoundException, InvalidPipelineStateException {
    Objects.requireNonNull(pipelineID, "Pipeline Id cannot be null");

    Pipeline pipeline = getPipeline(pipelineID);
    if (!pipeline.isClosed()) {
      throw new InvalidPipelineStateException(
          format("Pipeline with %s is not yet closed", pipelineID));
    }

    pipelineMap.remove(pipelineID);
    pipeline2container.remove(pipelineID);
    return pipeline;
  }

  /**
   * Remove container from a pipeline.
   *
   * @param pipelineID - PipelineID of the pipeline from which container needs
   *                   to be removed
   * @param containerID - ContainerID of the container to remove
   */
  void removeContainerFromPipeline(PipelineID pipelineID, ContainerID containerID) throws PipelineNotFoundException {
    Objects.requireNonNull(pipelineID,
        "Pipeline Id cannot be null");
    Objects.requireNonNull(containerID,
        "container Id cannot be null");

    Set<ContainerID> containerIDs = pipeline2container.get(pipelineID);
    if (containerIDs == null) {
      throw new PipelineNotFoundException(
          format("%s not found", pipelineID));
    }
    containerIDs.remove(containerID);
  }

  /**
   * Updates the state of pipeline.
   *
   * @param pipelineID - PipelineID of the pipeline whose state needs
   *                   to be updated
   * @param state - new state of the pipeline
   * @return Pipeline with the updated state
   * @throws PipelineNotFoundException if pipeline does not exist
   */
  Pipeline updatePipelineState(PipelineID pipelineID, PipelineState state)
      throws PipelineNotFoundException {
    Objects.requireNonNull(pipelineID, "Pipeline Id cannot be null");
    Objects.requireNonNull(state, "Pipeline LifeCycleState cannot be null");

    final Pipeline pipeline = getPipeline(pipelineID);
    // Return the old pipeline if updating same state
    if (pipeline.getPipelineState() == state) {
      LOG.debug("CurrentState and NewState are the same, return from " +
          "updatePipelineState directly.");
      return pipeline;
    }
    Pipeline updatedPipeline = pipelineMap.compute(pipelineID,
        (id, p) -> pipeline.toBuilder().setState(state).build());

    List<Pipeline> pipelineList =
        query2OpenPipelines.get(pipeline.getReplicationConfig());

    if (updatedPipeline.getPipelineState() == PipelineState.OPEN) {
      // for transition to OPEN state add pipeline to query2OpenPipelines
      if (pipelineList == null) {
        pipelineList = new ArrayList<>();
        query2OpenPipelines.put(pipeline.getReplicationConfig(), pipelineList);
      }
      pipelineList.add(updatedPipeline);
    } else {
      // for transition from OPEN to CLOSED state remove pipeline from
      // query2OpenPipelines
      if (pipelineList != null) {
        pipelineList.remove(pipeline);
      }
    }
    return updatedPipeline;
  }

}
