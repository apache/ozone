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
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
import java.util.function.Predicate;
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
  private final Map<PipelineID, PipelineInfo> pipelineMap = new HashMap<>();
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

    final PipelineInfo info = new PipelineInfo(pipeline);
    if (pipelineMap.putIfAbsent(pipeline.getId(), info) != null) {
      LOG.warn("Duplicate pipeline ID detected. {}", pipeline.getId());
      throw new DuplicatedPipelineIdException(
          format("Duplicate pipeline ID %s detected.", pipeline.getId()));
    }
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
    getPipeline(pipelineID).addContainerToOpenPipeline(containerID);
  }

  /**
   * Add container to an existing pipeline during SCM Start.
   *
   * @param pipelineID - PipelineID of the pipeline to which container is added
   * @param containerID - ContainerID of the container to add
   */
  void addContainerToPipelineSCMStart(PipelineID pipelineID, ContainerID containerID)
      throws PipelineNotFoundException {
    getPipeline(pipelineID).addContainer(containerID);
  }

  /**
   * Get pipeline corresponding to specified pipelineID.
   *
   * @param pipelineID - PipelineID of the pipeline to be retrieved
   * @return Pipeline
   * @throws PipelineNotFoundException if pipeline is not found
   */
  PipelineInfo getPipeline(PipelineID pipelineID) throws PipelineNotFoundException {
    Objects.requireNonNull(pipelineID, "pipelineID == null");
    final PipelineInfo info = pipelineMap.get(pipelineID);

    if (info == null) {
      throw new PipelineNotFoundException(
          "Pipeline not found: " + pipelineID);
    }
    return info;
  }

  /**
   * Get list of pipelines in SCM.
   * @return List of pipelines
   */
  List<Pipeline> getPipelines() {
    final List<Pipeline> pipelines = new ArrayList<>(pipelineMap.size());
    for (PipelineInfo info : pipelineMap.values()) {
      pipelines.add(info.getPipeline());
    }
    return pipelines;
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
    for (PipelineInfo info: pipelineMap.values()) {
      final Pipeline pipeline = info.getPipeline();
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
      return getOpenPipelines(replicationConfig);
    }

    List<Pipeline> pipelines = new ArrayList<>();
    for (PipelineInfo info : pipelineMap.values()) {
      final Pipeline pipeline = info.getPipeline();
      if (pipeline.getReplicationConfig().equals(replicationConfig)
          && pipeline.getPipelineState() == state) {
        pipelines.add(pipeline);
      }
    }

    return pipelines;
  }

  private List<Pipeline> getOpenPipelines(ReplicationConfig replicationConfig) {
    final List<Pipeline> pipelines = query2OpenPipelines.get(replicationConfig);
    return pipelines != null && !pipelines.isEmpty() ? new ArrayList<>(pipelines) : Collections.emptyList();
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
      final List<Pipeline> pipelines = query2OpenPipelines.get(replicationConfig);
      return pipelines != null && !pipelines.isEmpty() ? pipelines.size() : 0;
    }

    int count = 0;
    for (PipelineInfo info : pipelineMap.values()) {
      final Pipeline pipeline = info.getPipeline();
      if (pipeline.getReplicationConfig().equals(replicationConfig)
          && pipeline.getPipelineState() == state) {
        count++;
      }
    }
    return count;
  }

  static Predicate<Pipeline> notInExcludeDatanodes(Collection<DatanodeDetails> excludeDatanodes) {
    return p -> p.getNodeSet().stream().noneMatch(excludeDatanodes::contains);
  }

  static Predicate<Pipeline> notInExcludePipelines(Collection<PipelineID> excludePipelines) {
    return p -> !excludePipelines.contains(p.getId());
  }

  static Predicate<Pipeline> getPredicate(
      Collection<DatanodeDetails> excludeDatanodes,
      Collection<PipelineID> excludePipelines) {
    if (excludeDatanodes.isEmpty()) {
      return excludePipelines.isEmpty() ? p -> true : notInExcludePipelines(excludePipelines);
    } else {
      final Predicate<Pipeline> n = notInExcludeDatanodes(excludeDatanodes);
      return excludePipelines.isEmpty() ? n : p -> notInExcludePipelines(excludePipelines).test(p) && n.test(p);
    }
  }

  static Predicate<Pipeline> getPredicate(
      Collection<DatanodeDetails> excludeDatanodes,
      Collection<PipelineID> excludePipelines,
      ReplicationConfig replicationConfig,
      PipelineState state) {
    final Predicate<Pipeline> include = getPredicate(excludeDatanodes, excludePipelines);
    return p -> p.getPipelineState() == state
        && p.getReplicationConfig().equals(replicationConfig)
        && include.test(p);
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

    if (state == PipelineState.OPEN) {
      final List<Pipeline> pipelines = getOpenPipelines(replicationConfig);
      if (excludeDns.isEmpty() && excludePipelines.isEmpty()) {
        return pipelines;
      }

      final Predicate<Pipeline> include = getPredicate(excludeDns, excludePipelines);
      pipelines.removeIf(pipeline -> !include.test(pipeline));
      return pipelines;
    }

    final Predicate<Pipeline> include = getPredicate(excludeDns, excludePipelines, replicationConfig, state);
    final List<Pipeline> pipelines = new ArrayList<>(pipelineMap.size() / 2 + 1); // only resize once
    for (PipelineInfo info : pipelineMap.values()) {
      final Pipeline pipeline = info.getPipeline();
      if (include.test(pipeline)) {
        pipelines.add(pipeline);
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
    return getPipeline(pipelineID).getContainers();
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
    return getPipeline(pipelineID).getContainers().size();
  }

  /**
   * Remove pipeline from the data structures.
   *
   * @param pipelineID - PipelineID of the pipeline to be removed
   */
  Pipeline removePipeline(PipelineID pipelineID) throws PipelineNotFoundException, InvalidPipelineStateException {
    Objects.requireNonNull(pipelineID, "Pipeline Id cannot be null");

    // Check existence first, before removing
    final PipelineInfo info = pipelineMap.get(pipelineID);
    if (info == null) {
      throw new PipelineNotFoundException("Pipeline not found: " + pipelineID);
    }
    final Pipeline pipeline = info.getPipeline();
    if (!pipeline.isClosed()) {
      throw new InvalidPipelineStateException(
          format("Pipeline with %s is not yet closed", pipelineID));
    }
    List<Pipeline> pipelineList = query2OpenPipelines.get(pipeline.getReplicationConfig());

    if (pipelineList != null) { 
      pipelineList.remove(pipeline);
    }
    pipelineMap.remove(pipelineID);
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
    getPipeline(pipelineID).removeContainer(containerID);
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
    Objects.requireNonNull(state, "Pipeline LifeCycleState cannot be null");

    final PipelineInfo info = getPipeline(pipelineID);
    final Pipeline pipeline = info.getPipeline();
    // Return the old pipeline if updating same state
    if (pipeline.getPipelineState() == state) {
      LOG.debug("CurrentState and NewState are the same, return from " +
          "updatePipelineState directly.");
      return pipeline;
    }
    final Pipeline updated = pipeline.toBuilder().setState(state).build();
    PipelineInfo oldInfo = getPipeline(pipelineID);

    PipelineInfo newInfo = new PipelineInfo(updated);

    for (ContainerID cid : oldInfo.getContainers()) {
      newInfo.addContainer(cid);
    }

    pipelineMap.put(pipelineID, newInfo);

    List<Pipeline> pipelineList =
        query2OpenPipelines.get(pipeline.getReplicationConfig());

    if (updated.getPipelineState() == PipelineState.OPEN) {
      // for transition to OPEN state add pipeline to query2OpenPipelines
      if (pipelineList == null) {
        pipelineList = new ArrayList<>();
        query2OpenPipelines.put(pipeline.getReplicationConfig(), pipelineList);
      }
      pipelineList.add(updated);
    } else {
      // for transition from OPEN to CLOSED state remove pipeline from
      // query2OpenPipelines
      if (pipelineList != null) {
        pipelineList.remove(pipeline);
      }
    }
    return updated;
  }

  static class PipelineInfo {
    private final Pipeline pipeline;
    private final NavigableSet<ContainerID> containers = new TreeSet<>();

    PipelineInfo(Pipeline pipeline) {
      this.pipeline = pipeline;
    }

    Pipeline getPipeline() {
      return pipeline;
    }

    NavigableSet<ContainerID> getContainers() {
      return new TreeSet<>(containers);
    }

    void addContainerToOpenPipeline(ContainerID containerID) throws InvalidPipelineStateException {
      Objects.requireNonNull(containerID, "Container Id == null");
      if (pipeline.isClosed()) {
        throw new InvalidPipelineStateException(
            "Pipeline closed: Failed add container " + containerID + " to pipeline " + pipeline.getId());
      }
      containers.add(containerID);
    }

    void addContainer(ContainerID containerID) {
      Objects.requireNonNull(containerID, "Container Id == null");
      if (pipeline.isClosed()) {
        // When SCM restarts, the SCM DB may not be up-to-dated,
        // where some containers are in an OPEN state for a CLOSED pipeline.
        // This happens when close pipeline transaction in flushed
        // before SCM goes down and close container is not flushed into DB.
        LOG.info("Container {} in open state for pipeline={} in closed state", containerID, pipeline.getId());
      }
      containers.add(containerID);
    }

    void removeContainer(ContainerID containerID) {
      Objects.requireNonNull(containerID, "Container Id == null");
      containers.remove(containerID);
    }
  }
}
