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
public class PipelineStateManagerV2Impl implements PipelineStateManagerV2 {

  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineStateManager.class);

  private final PipelineStateMap pipelineStateMap;
  private Table<PipelineID, Pipeline> pipelineStore;

  public PipelineStateManagerV2Impl(Table<PipelineID, Pipeline> pipelineStore) {
    this.pipelineStateMap = new PipelineStateMap();
    this.pipelineStore = pipelineStore;
  }

  @Override
  public void addPipeline(HddsProtos.Pipeline pipelineProto)
      throws IOException {
    Pipeline pipeline = Pipeline.getFromProtobuf(pipelineProto);
    if (pipelineStore != null) {
      pipelineStore.put(pipeline.getId(), pipeline);
    }
    pipelineStateMap.addPipeline(pipeline);
    LOG.info("Created pipeline {}", pipeline);
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
  public Pipeline removePipeline(HddsProtos.PipelineID pipelineIDProto)
      throws IOException {
    PipelineID pipelineID = PipelineID.getFromProtobuf(pipelineIDProto);
    if (pipelineStore != null) {
      pipelineStore.delete(pipelineID);
    }
    Pipeline pipeline = pipelineStateMap.removePipeline(pipelineID);
    LOG.info("Pipeline {} removed from db", pipeline);
    return pipeline;
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
  public boolean isPipelineStoreEmpty() throws IOException {
    if (pipelineStore == null) {
      throw new IOException("PipelineStore cannot be null");
    }
    return pipelineStore.isEmpty();
  }

  @Override
  public TableIterator<PipelineID, ? extends Table.KeyValue<
      PipelineID, Pipeline>> getPipelineStoreIterator() throws IOException {
    if (pipelineStore == null) {
      throw new IOException("PipelineStore cannot be null");
    }
    return pipelineStore.iterator();
  }

  @Override
  public void close() throws Exception {
    pipelineStore.close();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for PipelineStateManager.
   */
  public static class Builder {
    private Table<PipelineID, Pipeline> pipelineStore;
    private SCMRatisServer scmRatisServer;

    public Builder setRatisServer(final SCMRatisServer ratisServer) {
      scmRatisServer = ratisServer;
      return this;
    }

    public Builder setPipelineStore(
        final Table<PipelineID, Pipeline> pipelineTable) {
      this.pipelineStore = pipelineTable;
      return this;
    }

    public PipelineStateManagerV2 build() {
      Preconditions.checkNotNull(pipelineStore);

      final PipelineStateManagerV2 pipelineStateManager =
          new PipelineStateManagerV2Impl(pipelineStore);

      final SCMHAInvocationHandler invocationHandler =
          new SCMHAInvocationHandler(SCMRatisProtocol.RequestType.PIPELINE,
              pipelineStateManager, scmRatisServer);

      return (PipelineStateManagerV2) Proxy.newProxyInstance(
          SCMHAInvocationHandler.class.getClassLoader(),
          new Class<?>[]{PipelineStateManagerV2.class}, invocationHandler);
    }
  }
}
