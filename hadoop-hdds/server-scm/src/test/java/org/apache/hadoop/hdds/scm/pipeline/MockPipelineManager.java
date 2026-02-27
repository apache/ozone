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

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;

/**
 * Mock PipelineManager implementation for testing.
 */
public class MockPipelineManager implements PipelineManager {

  private final PipelineStateManager stateManager;

  public MockPipelineManager(DBStore dbStore, SCMHAManager scmhaManager, NodeManager nodeManager)
      throws RocksDatabaseException, CodecException, DuplicatedPipelineIdException {
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
    return createPipeline(replicationConfig, Collections.emptyList(),
        Collections.emptyList());
  }

  @Override
  public Pipeline createPipeline(ReplicationConfig replicationConfig,
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes)
      throws IOException {
    Pipeline pipeline;
    if (replicationConfig.getReplicationType()
        == HddsProtos.ReplicationType.EC) {
      pipeline = buildECPipeline(
          replicationConfig, excludedNodes, favoredNodes);
    } else {
      pipeline = createPipeline(replicationConfig,
          ImmutableList.of(MockDatanodeDetails.randomDatanodeDetails(),
              MockDatanodeDetails.randomDatanodeDetails(),
              MockDatanodeDetails.randomDatanodeDetails()));
    }

    stateManager.addPipeline(pipeline.getProtobufMessage(
        ClientVersion.CURRENT.serialize()));
    return pipeline;
  }

  @Override
  public Pipeline buildECPipeline(ReplicationConfig replicationConfig,
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes) {
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
    return pipeline;
  }

  @Override
  public void addEcPipeline(Pipeline pipeline)
      throws IOException {
    stateManager.addPipeline(pipeline.getProtobufMessage(
        ClientVersion.CURRENT.serialize()));
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
  public Pipeline createPipelineForRead(
      final ReplicationConfig replicationConfig,
      final Set<ContainerReplica> replicas) {
    List<DatanodeDetails> dns = new ArrayList<>();
    Map<DatanodeDetails, Integer> map = new HashMap<>();
    for (ContainerReplica r : replicas) {
      map.put(r.getDatanodeDetails(), r.getReplicaIndex());
      dns.add(r.getDatanodeDetails());
    }
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(replicationConfig)
        .setNodes(dns)
        .setReplicaIndexes(map)
        .setState(Pipeline.PipelineState.CLOSED)
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
  /**
   * Returns the count of pipelines meeting the given ReplicationConfig and
   * state.
   * @param replicationConfig The ReplicationConfig of the pipelines to count
   * @param state The current state of the pipelines to count
   * @return The count of pipelines meeting the above criteria
   */
  public int getPipelineCount(ReplicationConfig replicationConfig,
      final Pipeline.PipelineState state) {
    return stateManager.getPipelineCount(replicationConfig, state);
  }

  @Override
  public void addContainerToPipeline(final PipelineID pipelineID, final ContainerID containerID)
      throws PipelineNotFoundException, InvalidPipelineStateException {
    stateManager.addContainerToPipeline(pipelineID, containerID);
  }

  @Override
  public void addContainerToPipelineSCMStart(PipelineID pipelineID, ContainerID containerID)
      throws PipelineNotFoundException {
    stateManager.addContainerToPipelineForce(pipelineID, containerID);
  }

  @Override
  public void removeContainerFromPipeline(PipelineID pipelineID, ContainerID containerID) {
    stateManager.removeContainerFromPipeline(pipelineID, containerID);
  }

  @Override
  public NavigableSet<ContainerID> getContainersInPipeline(PipelineID pipelineID) throws PipelineNotFoundException {
    return stateManager.getContainers(pipelineID);
  }

  @Override
  public int getNumberOfContainers(final PipelineID pipelineID) throws PipelineNotFoundException {
    return getContainersInPipeline(pipelineID).size();
  }

  @Override
  public void openPipeline(final PipelineID pipelineId)
      throws IOException {
    stateManager.updatePipelineState(
        pipelineId.getProtobuf(), HddsProtos.PipelineState.PIPELINE_OPEN);
  }

  @Override
  public void closePipeline(final PipelineID pipelineId)
      throws IOException {
    stateManager.updatePipelineState(pipelineId.getProtobuf(),
        HddsProtos.PipelineState.PIPELINE_CLOSED);
  }

  @Override
  public void deletePipeline(PipelineID pipelineID) {
  }

  @Override
  public void closeStalePipelines(DatanodeDetails datanodeDetails) {

  }

  @Override
  public void scrubPipelines() {

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
  public void activatePipeline(final PipelineID pipelineID) {
  }

  @Override
  public void deactivatePipeline(final PipelineID pipelineID)
      throws IOException {
    stateManager.updatePipelineState(pipelineID.getProtobuf(),
        HddsProtos.PipelineState.PIPELINE_DORMANT);
  }

  @Override
  public boolean getSafeModeStatus() {
    return false;
  }

  @Override
  public void reinitialize(Table<PipelineID, Pipeline> pipelineStore) {
  }

  @Override
  public void close() {
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

  @Override
  public boolean hasEnoughSpace(Pipeline pipeline, long containerSize) {
    return false;
  }

  @Override
  public int openContainerLimit(List<DatanodeDetails> datanodes) {
    // For tests that do not care about this limit, return a large value.
    return Integer.MAX_VALUE;
  }

  @Override
  public SCMPipelineMetrics getMetrics() {
    return null;
  }
}
