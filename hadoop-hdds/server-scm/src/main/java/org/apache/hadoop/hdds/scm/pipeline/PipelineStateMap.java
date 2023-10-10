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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * This is a thread-safe implementation of {@link PipelineMap}.
 */
public class PipelineStateMap implements PipelineMap {

  private final Map<PipelineID, PipelineWithContainers> pipelines =
      new ConcurrentHashMap<>();

  public PipelineStateMap() {
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

    if (pipelines.putIfAbsent(pipeline.getId(),
        new PipelineWithContainers(pipeline)) != null) {
      throw new DuplicatedPipelineIdException(
          format("Duplicate pipeline ID %s detected.", pipeline.getId()));
    }
  }

  @Override
  public void addContainerToPipeline(PipelineID pipelineID,
                                     ContainerID containerID)
      throws IOException {
    addContainerToPipeline(pipelineID, containerID, true);
  }

  @Override
  public void addContainerToPipelineSCMStart(PipelineID pipelineID,
         ContainerID containerID) throws IOException {
    addContainerToPipeline(pipelineID, containerID, false);
  }

  private void addContainerToPipeline(PipelineID pipelineID,
                                      ContainerID containerID,
                                      boolean declineClosed)
      throws InvalidPipelineStateException, PipelineNotFoundException {
    Preconditions.checkNotNull(pipelineID,
        "Pipeline Id cannot be null");
    Preconditions.checkNotNull(containerID,
        "Container Id cannot be null");

    // attempt to add container
    PipelineWithContainers newPipelineWithContainers =
        pipelines.compute(pipelineID, (pipelineID1, pipelineWithContainers) -> {
          if (pipelineWithContainers != null) {
            // if updating is not allowed - do nothing
            if (!(pipelineWithContainers.getPipeline().isClosed() &&
                declineClosed)) {
              pipelineWithContainers.addContainer(containerID);
            }
            return pipelineWithContainers;
          }
          return null;
        });

    if (newPipelineWithContainers == null) {
      throw new PipelineNotFoundException(format("%s not found", pipelineID));
    }
    // check again if updating was allowed and throw exception if not
    if (newPipelineWithContainers.getPipeline().isClosed() && declineClosed) {
      throw new InvalidPipelineStateException(
          format("Cannot add container to pipeline=%s in closed state",
              pipelineID));
    }
  }

  @Override
  public Pipeline getPipeline(PipelineID pipelineID)
      throws PipelineNotFoundException {
    Preconditions.checkNotNull(pipelineID,
        "Pipeline Id cannot be null");
    PipelineWithContainers pipelineWithContainers = pipelines.get(pipelineID);
    if (pipelineWithContainers == null) {
      throw new PipelineNotFoundException(
          format("%s not found", pipelineID));
    }
    return pipelineWithContainers.getPipeline();
  }

  @Override
  public List<Pipeline> getPipelines() {
    return pipelines.values().stream().map(PipelineWithContainers::getPipeline)
        .collect(Collectors.toList());
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationConfig replicationConfig) {
    Preconditions
        .checkNotNull(replicationConfig, "ReplicationConfig cannot be null");
    return getPipelines(
        pipeline -> pipeline.getReplicationConfig().equals(replicationConfig));
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationConfig replicationConfig,
                                       PipelineState state) {
    Preconditions
        .checkNotNull(replicationConfig, "ReplicationConfig cannot be null");
    Preconditions.checkNotNull(state, "Pipeline state cannot be null");

    return pipelines.values().stream().filter(pipelineWithContainers ->
            pipelineWithContainers.getPipeline()
                .getReplicationConfig().equals(replicationConfig) &&
                pipelineWithContainers.getPipeline()
                    .getPipelineState() == state)
        .map(PipelineWithContainers::getPipeline).collect(Collectors.toList());
  }

  @Override
  public int getPipelineCount(ReplicationConfig replicationConfig,
                              PipelineState state) {
    Preconditions
        .checkNotNull(replicationConfig, "ReplicationConfig cannot be null");
    Preconditions.checkNotNull(state, "Pipeline state cannot be null");
    return getPipelines(replicationConfig, state).size();
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationConfig replicationConfig,
                                     PipelineState state,
                                     Collection<DatanodeDetails> excludeDns,
                                     Collection<PipelineID> excludePipelines) {
    Preconditions
        .checkNotNull(replicationConfig, "ReplicationConfig cannot be null");
    Preconditions.checkNotNull(state, "Pipeline state cannot be null");
    Preconditions
        .checkNotNull(excludeDns, "Datanode exclude list cannot be null");
    Preconditions
        .checkNotNull(excludePipelines, "Pipeline exclude list cannot be null");

    return getPipelines(pipeline -> pipeline.getPipelineState() == state &&
        pipeline.getReplicationConfig().equals(replicationConfig) &&
        pipeline.getNodes().stream().noneMatch(excludeDns::contains) &&
        !excludePipelines.contains(pipeline.getId()));
  }

  @Override
  public NavigableSet<ContainerID> getContainers(PipelineID pipelineID)
      throws PipelineNotFoundException {
    Preconditions.checkNotNull(pipelineID,
        "Pipeline Id cannot be null");
    PipelineWithContainers pipelineWithContainers = pipelines.get(pipelineID);
    if (pipelineWithContainers == null) {
      throw new PipelineNotFoundException(
          format("%s not found", pipelineID));
    }
    return new TreeSet<>(pipelineWithContainers.getContainers());
  }

  @Override
  public int getNumberOfContainers(PipelineID pipelineID)
      throws PipelineNotFoundException {
    Preconditions.checkNotNull(pipelineID,
        "Pipeline Id cannot be null");
    PipelineWithContainers pipelineWithContainers = pipelines.get(pipelineID);
    if (pipelineWithContainers == null) {
      throw new PipelineNotFoundException(
          format("%s not found", pipelineID));
    }
    return pipelineWithContainers.getContainers().size();
  }

  @Override
  public Pipeline removePipeline(PipelineID pipelineID) throws IOException {
    Preconditions.checkNotNull(pipelineID, "Pipeline Id cannot be null");
    // final containers to retrieve result from lambda
    final AtomicBoolean pipelineInWrongState = new AtomicBoolean(false);
    final AtomicBoolean pipelineNotFound = new AtomicBoolean(false);
    final AtomicReference<PipelineWithContainers> removedValue =
        new AtomicReference<>(null);

    pipelines.compute(pipelineID, (pipelineID1, oldPwC) -> {
      if (oldPwC == null) {
        pipelineNotFound.set(true);
        return null;
      }
      if (!oldPwC.getPipeline().isClosed()) {
        // do not change the value if pipeline in the wrong state
        pipelineInWrongState.set(true);
        return oldPwC;
      }
      // remove value
      removedValue.set(oldPwC);
      return null;
    });

    if (pipelineNotFound.get()) {
      throw new PipelineNotFoundException(format("%s not found", pipelineID));
    }
    if (pipelineInWrongState.get()) {
      throw new InvalidPipelineStateException(
          format("Pipeline with %s is not yet closed", pipelineID));
    }
    return removedValue.get().getPipeline();
  }

  @Override
  public void removeContainerFromPipeline(PipelineID pipelineID,
      ContainerID containerID) throws IOException {
    Preconditions.checkNotNull(pipelineID,
        "Pipeline Id cannot be null");
    Preconditions.checkNotNull(containerID,
        "container Id cannot be null");

    PipelineWithContainers pipelineWithContainers = pipelines.get(pipelineID);
    if (pipelineWithContainers == null) {
      throw new PipelineNotFoundException(
          format("%s not found", pipelineID));
    }
    Set<ContainerID> containerIDs = pipelineWithContainers.getContainers();
    containerIDs.remove(containerID);
  }

  @Override
  public Pipeline updatePipelineState(PipelineID pipelineID,
                                      PipelineState state)
      throws PipelineNotFoundException {
    Preconditions.checkNotNull(pipelineID, "Pipeline Id cannot be null");
    Preconditions.checkNotNull(state, "Pipeline LifeCycleState cannot be null");

    PipelineWithContainers updatedPipelineWithContainers =
        pipelines.compute(pipelineID, (id, oldPwC) -> {
          if (oldPwC != null) {
            return new PipelineWithContainers(
                Pipeline.newBuilder(oldPwC.getPipeline()).setState(state)
                    .build(), oldPwC.getContainers());
          }
          return null;
        });
    if (updatedPipelineWithContainers == null) {
      throw new PipelineNotFoundException(format("%s not found", pipelineID));
    }
    return updatedPipelineWithContainers.getPipeline();
  }

  private List<Pipeline> getPipelines(Predicate<Pipeline> filter) {
    return pipelines.values().stream().map(PipelineWithContainers::getPipeline)
        .filter(filter).collect(Collectors.toList());
  }

  private static class PipelineWithContainers {
    private final Pipeline pipeline;

    private final NavigableSet<ContainerID> containers = new TreeSet<>();

    PipelineWithContainers(Pipeline pipeline) {
      this.pipeline = pipeline;
    }

    PipelineWithContainers(Pipeline pipeline,
                           NavigableSet<ContainerID> containers) {
      this.pipeline = pipeline;
      this.containers.addAll(containers);
    }

    public Pipeline getPipeline() {
      return pipeline;
    }

    public NavigableSet<ContainerID> getContainers() {
      return containers;
    }

    public void addContainer(ContainerID containerID) {
      containers.add(containerID);
    }

    public void removeContainer(ContainerID containerID) {
      containers.remove(containerID);
    }
  }
}
