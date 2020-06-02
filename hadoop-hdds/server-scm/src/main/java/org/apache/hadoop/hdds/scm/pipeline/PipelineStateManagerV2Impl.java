/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;

/**
 * Implementation of pipeline state manager.
 * PipelineStateMap class holds the data structures related to pipeline and its
 * state. All the read and write operations in PipelineStateMap are protected
 * by a read write lock.
 */
public class PipelineStateManagerV2Impl implements StateManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineStateManager.class);

  private final PipelineStateMap pipelineStateMap;
  private final NodeManager nodeManager;
  private Table<PipelineID, Pipeline> pipelineStore;

  public PipelineStateManagerV2Impl(
      Table<PipelineID, Pipeline> pipelineStore, NodeManager nodeManager)
      throws IOException {
    this.pipelineStateMap = new PipelineStateMap();
    this.nodeManager = nodeManager;
    this.pipelineStore = pipelineStore;
    initialize();
  }

  private void initialize() throws IOException {
    if (pipelineStore == null || nodeManager == null) {
      throw new IOException("PipelineStore cannot be null");
    }
    if (pipelineStore.isEmpty()) {
      LOG.info("No pipeline exists in current db");
      return;
    }
    TableIterator<PipelineID, ? extends Table.KeyValue<PipelineID, Pipeline>>
        iterator = pipelineStore.iterator();
    while (iterator.hasNext()) {
      Pipeline pipeline = iterator.next().getValue();
      addPipeline(pipeline.getProtobufMessage());
    }
  }

  @Override
  public void addPipeline(HddsProtos.Pipeline pipelineProto)
      throws IOException {
    Pipeline pipeline = Pipeline.getFromProtobuf(pipelineProto);
    pipelineStore.put(pipeline.getId(), pipeline);
    pipelineStateMap.addPipeline(pipeline);
    nodeManager.addPipeline(pipeline);
    LOG.info("Created pipeline {}.", pipeline);
  }

  @Override
  public void addContainerToPipeline(
      PipelineID pipelineId, ContainerID containerID)
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
  public List<Pipeline> getPipelines(HddsProtos.ReplicationType type) {
    return pipelineStateMap.getPipelines(type);
  }

  @Override
  public List<Pipeline> getPipelines(
      HddsProtos.ReplicationType type, HddsProtos.ReplicationFactor factor) {
    return pipelineStateMap.getPipelines(type, factor);
  }

  @Override
  public List<Pipeline> getPipelines(
      HddsProtos.ReplicationType type, HddsProtos.ReplicationFactor factor,
                              Pipeline.PipelineState state) {
    return pipelineStateMap.getPipelines(type, factor, state);
  }

  @Override
  public List<Pipeline> getPipelines(
      HddsProtos.ReplicationType type, HddsProtos.ReplicationFactor factor,
      Pipeline.PipelineState state, Collection<DatanodeDetails> excludeDns,
      Collection<PipelineID> excludePipelines) {
    return pipelineStateMap
        .getPipelines(type, factor, state, excludeDns, excludePipelines);
  }

  @Override
  public List<Pipeline> getPipelines(HddsProtos.ReplicationType type,
                                     Pipeline.PipelineState... states) {
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
  public void removePipeline(HddsProtos.PipelineID pipelineIDProto)
      throws IOException {
    PipelineID pipelineID = PipelineID.getFromProtobuf(pipelineIDProto);
    pipelineStore.delete(pipelineID);
    Pipeline pipeline = pipelineStateMap.removePipeline(pipelineID);
    nodeManager.removePipeline(pipeline);
    LOG.info("Pipeline {} removed.", pipeline);
    return;
  }


  @Override
  public void removeContainerFromPipeline(
      PipelineID pipelineID, ContainerID containerID) throws IOException {
    pipelineStateMap.removeContainerFromPipeline(pipelineID, containerID);
  }

  @Override
  public void updatePipelineState(
      HddsProtos.PipelineID pipelineIDProto, HddsProtos.PipelineState newState)
      throws IOException {
    pipelineStateMap.updatePipelineState(
        PipelineID.getFromProtobuf(pipelineIDProto),
        Pipeline.PipelineState.fromProtobuf(newState));
  }

  @Override
  public void close() throws Exception {
    pipelineStore.close();
  }

  // TODO Remove legacy
  @Override
  public void addPipeline(Pipeline pipeline) throws IOException {
    throw new IOException("Not supported.");
  }

  @Override
  public Pipeline removePipeline(PipelineID pipelineID) throws IOException {
    throw new IOException("Not supported.");
  }

  @Override
  public void updatePipelineState(PipelineID id,
                                  Pipeline.PipelineState newState)
      throws IOException {
    throw new IOException("Not supported.");
  }

  @Override
  public Pipeline finalizePipeline(PipelineID pipelineId)
      throws IOException {
    throw new IOException("Not supported.");
  }


  @Override
  public Pipeline openPipeline(PipelineID pipelineId) throws IOException {
    throw new IOException("Not supported.");
  }

  @Override
  public void activatePipeline(PipelineID pipelineID) throws IOException {
    throw new IOException("Not supported.");
  }

  @Override
  public void deactivatePipeline(PipelineID pipelineID) throws IOException {
    throw new IOException("Not supported.");
  }

  // legacy interfaces end

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for PipelineStateManager.
   */
  public static class Builder {
    private Table<PipelineID, Pipeline> pipelineStore;
    private NodeManager nodeManager;
    private SCMRatisServer scmRatisServer;

    public Builder setRatisServer(final SCMRatisServer ratisServer) {
      scmRatisServer = ratisServer;
      return this;
    }

    public Builder setNodeManager(final NodeManager scmNodeManager) {
      nodeManager = scmNodeManager;
      return this;
    }

    public Builder setPipelineStore(
        final Table<PipelineID, Pipeline> pipelineTable) {
      this.pipelineStore = pipelineTable;
      return this;
    }

    public StateManager build() throws IOException {
      Preconditions.checkNotNull(pipelineStore);

      final StateManager pipelineStateManager =
          new PipelineStateManagerV2Impl(pipelineStore, nodeManager);

      final SCMHAInvocationHandler invocationHandler =
          new SCMHAInvocationHandler(SCMRatisProtocol.RequestType.PIPELINE,
              pipelineStateManager, scmRatisServer);

      return (StateManager) Proxy.newProxyInstance(
          SCMHAInvocationHandler.class.getClassLoader(),
          new Class<?>[]{StateManager.class}, invocationHandler);
    }
  }
}
