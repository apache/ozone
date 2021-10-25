/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersions;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Mock PipelineManager implementation for testing.
 */
public final class MockPipelineManager implements PipelineManager {

  private PipelineStateManager stateManager;

  public MockPipelineManager(DBStore dbStore, SCMHAManager scmhaManager,
                             NodeManager nodeManager) throws IOException {
    stateManager = PipelineStateManagerImpl
        .newBuilder().setNodeManager(nodeManager)
        .setRatisServer(scmhaManager.getRatisServer())
        .setPipelineStore(SCMDBDefinition.PIPELINES.getTable(dbStore))
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();
  }

  @Override
  public Pipeline createPipeline(ReplicationConfig replicationConfig)
      throws IOException {
    final List<DatanodeDetails> nodes = Stream.generate(
        MockDatanodeDetails::randomDatanodeDetails)
        .limit(replicationConfig.getRequiredNodes())
        .collect(Collectors.toList());
    final Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(replicationConfig)
        .setNodes(nodes)
        .setState(Pipeline.PipelineState.OPEN)
        .build();

    stateManager.addPipeline(pipeline.getProtobufMessage(
        ClientVersions.CURRENT_VERSION));
    return pipeline;
  }

  @Override
  public Pipeline createPipeline(final ReplicationConfig replicationConfig,
      final List<DatanodeDetails> nodes) {
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(replicationConfig)
        .setNodes(nodes)
        .setState(Pipeline.PipelineState.OPEN)
        .build();
  }

  @Override
  public Pipeline getPipeline(final PipelineID pipelineID)
      throws PipelineNotFoundException {
    return stateManager.getPipeline(pipelineID);
  }

  @Override
  public boolean containsPipeline(final PipelineID pipelineID) {
    try {
      stateManager.getPipeline(pipelineID);
      return true;
    } catch (PipelineNotFoundException e) {
      return false;
    }
  }

  @Override
  public List<Pipeline> getPipelines() {
    return stateManager.getPipelines();
  }

  @Override
  public List<Pipeline> getPipelines(
      final ReplicationConfig replicationConfig) {
    return stateManager.getPipelines(replicationConfig);
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationConfig replicationConfig,
      final Pipeline.PipelineState state) {
    return stateManager.getPipelines(replicationConfig, state);
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationConfig replicationConfig,
      final Pipeline.PipelineState state,
      final Collection<DatanodeDetails> excludeDns,
      final Collection<PipelineID> excludePipelines) {
    return stateManager.getPipelines(replicationConfig, state,
        excludeDns, excludePipelines);
  }

  @Override
  public void addContainerToPipeline(final PipelineID pipelineID,
                                     final ContainerID containerID)
      throws IOException {
    stateManager.addContainerToPipeline(pipelineID, containerID);
  }

  @Override
  public void removeContainerFromPipeline(final PipelineID pipelineID,
                                          final ContainerID containerID)
      throws IOException {
    stateManager.removeContainerFromPipeline(pipelineID, containerID);
  }

  @Override
  public NavigableSet<ContainerID> getContainersInPipeline(
      final PipelineID pipelineID) throws IOException {
    return stateManager.getContainers(pipelineID);
  }

  @Override
  public int getNumberOfContainers(final PipelineID pipelineID)
      throws IOException {
    return getContainersInPipeline(pipelineID).size();
  }

  @Override
  public void openPipeline(final PipelineID pipelineId)
      throws IOException {
  }

  @Override
  public void closePipeline(final Pipeline pipeline, final boolean onTimeout)
      throws IOException {
  }

  @Override
  public void scrubPipeline(ReplicationConfig replicationConfig)
      throws IOException {

  }

  @Override
  public void startPipelineCreator() {

  }

  @Override
  public void triggerPipelineCreation() {

  }

  @Override
  public void incNumBlocksAllocatedMetric(final PipelineID id) {

  }

  @Override
  public int minHealthyVolumeNum(Pipeline pipeline) {
    return 0;
  }

  @Override
  public int minPipelineLimit(Pipeline pipeline) {
    return 0;
  }

  @Override
  public void activatePipeline(final PipelineID pipelineID)
      throws IOException {
  }

  @Override
  public void deactivatePipeline(final PipelineID pipelineID)
      throws IOException {
  }

  @Override
  public boolean getSafeModeStatus() {
    return false;
  }

  @Override
  public void reinitialize(Table<PipelineID, Pipeline> pipelineStore)
      throws IOException {

  }

  @Override
  public void freezePipelineCreation() {

  }

  @Override
  public void resumePipelineCreation() {

  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public Map<String, Integer> getPipelineInfo() {
    return null;
  }

  @Override
  public void acquireReadLock() {

  }

  @Override
  public void releaseReadLock() {

  }

  @Override
  public void acquireWriteLock() {

  }

  @Override
  public void releaseWriteLock() {

  }
}