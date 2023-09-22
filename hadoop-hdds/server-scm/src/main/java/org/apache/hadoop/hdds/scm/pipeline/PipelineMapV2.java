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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class PipelineMapV2 implements PipelineMap {

  private final Map<PipelineID, PipelineWithContainers> pipelines = new ConcurrentHashMap<>();

  public PipelineMapV2() {
  }

  @Override
  public void addPipeline(Pipeline pipeline) throws IOException {
    Preconditions.checkNotNull(pipeline, "Pipeline cannot be null");
    Preconditions.checkArgument(
        pipeline.getNodes().size() == pipeline.getReplicationConfig()
            .getRequiredNodes(),
        "Nodes size=%s, replication factor=%s do not match ",
        pipeline.getNodes().size(), pipeline.getReplicationConfig()
            .getRequiredNodes());

    if (pipelines.putIfAbsent(pipeline.getId(), new PipelineWithContainers(pipeline)) != null) {
      throw new DuplicatedPipelineIdException(
          format("Duplicate pipeline ID %s detected.", pipeline.getId()));
    }
  }

  @Override
  public void addContainerToPipeline(PipelineID pipelineID, ContainerID containerID)
      throws InvalidPipelineStateException {
    addContainerToPipeline(pipelineID, containerID, true);
  }

  @Override
  public void addContainerToPipelineSCMStart(PipelineID pipelineID, ContainerID containerID)
      throws InvalidPipelineStateException {
    addContainerToPipeline(pipelineID, containerID, false);
  }

  private void addContainerToPipeline(PipelineID pipelineID, ContainerID containerID, boolean declineClosed) {
    pipelines.computeIfPresent(pipelineID, (pipelineID1, pipelineWithContainers) -> {
      if (pipelineWithContainers.pipeline.isClosed() && declineClosed) {
        throw new InvalidPipelineStateException(format(
            "Cannot add container to pipeline=%s in closed state", pipelineID));
      }
      pipelineWithContainers.addContainer(containerID);
      return pipelineWithContainers;
    });
  }

  @Override
  public Pipeline getPipeline(PipelineID pipelineID) throws PipelineNotFoundException {
    return pipelines.get(pipelineID).getPipeline();
  }

  @Override
  public List<Pipeline> getPipelines() {
//    List<Pipeline> result = new ArrayList<>();
    return pipelines.values().stream()
        .map(PipelineWithContainers::getPipeline)
        .collect(Collectors.toList());
//    return result;
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationConfig replicationConfig, Pipeline.PipelineState state) {
    return pipelines.values().stream()
        .filter(pipelineWithContainers -> pipelineWithContainers.getPipeline().getReplicationConfig().equals(replicationConfig)
            && pipelineWithContainers.getPipeline().getPipelineState() == state)
        .map(PipelineWithContainers::getPipeline)
        .collect(Collectors.toList());
  }

  @Override
  public int getPipelineCount(ReplicationConfig replicationConfig, Pipeline.PipelineState state) {
    return getPipelines(replicationConfig, state).size();
  }

  @Override
  public List<Pipeline> getPipelines(
      ReplicationConfig replicationConfig,
      Pipeline.PipelineState state,
      Collection<DatanodeDetails> excludeDns,
      Collection<PipelineID> excludePipelines
  ) {
    return getPipelines(pipeline -> pipeline.getPipelineState() == state
        && pipeline.getReplicationConfig().equals(replicationConfig)
        && pipeline.getNodes().stream().filter(excludeDns::contains).distinct().isEmpty()
        && excludePipelines.contains(pipeline.getId())
    );
  }

  private List<Pipeline> getPipelines(Predicate<Pipeline> filter) {
    return pipelines.values().stream()
        .map(PipelineWithContainers::getPipeline)
        .filter(filter)
        .collect(Collectors.toList());
  }

  @Override
  public NavigableSet<ContainerID> getContainers(PipelineID pipelineID) throws PipelineNotFoundException {
    return null;
  }

  @Override
  public int getNumberOfContainers(PipelineID pipelineID) throws PipelineNotFoundException {
    return 0;
  }

  @Override
  public Pipeline removePipeline(PipelineID pipelineID) throws IOException {
    return null;
  }

  @Override
  public void removeContainerFromPipeline(PipelineID pipelineID, ContainerID containerID) throws IOException {

  }

  @Override
  public Pipeline updatePipelineState(PipelineID pipelineID, Pipeline.PipelineState state) throws PipelineNotFoundException {
    return null;
  }

  private static class PipelineWithContainers {
    private final Pipeline pipeline;

    private final NavigableSet<ContainerID> containers = new TreeSet<>();

    PipelineWithContainers(Pipeline pipeline) {
      this.pipeline = pipeline;
    }

    public Pipeline getPipeline() {
      return pipeline;
    }

    public void addContainer(ContainerID containerID) {
      containers.add(containerID);
    }

    public boolean hasContainer(ContainerID containerID) {
      return containers.contains(containerID);
    }

    public void removeContainer(ContainerID containerID) {
      containers.remove(containerID);

    }
  }
}
