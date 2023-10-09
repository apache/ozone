/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.pipeline;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicatedReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;

import java.io.IOException;
import java.util.*;

/**
 * Provides {@link Pipeline} factory methods for testing.
 */
public final class MockPipeline {
  /**
   * Create a pipeline with single node replica.
   *
   * @return Pipeline with single node in it.
   */
  public static Pipeline createSingleNodePipeline() throws IOException {
    return createPipeline(1);
  }

  /**
   * Create a pipeline with single node replica.
   *
   * @return Pipeline with single node in it.
   */
  public static Pipeline createPipeline(int numNodes) throws IOException {
    return createPipeline(getDatanodeDetails(numNodes));
  }

  public static Pipeline createPipeline() throws IOException {
    return getDefaultPipelineBuilder().build();
  }

  public static Pipeline createPipeline(Iterable<DatanodeDetails> ids) {
    Objects.requireNonNull(ids, "ids == null");
    Preconditions.checkArgument(ids.iterator().hasNext());
    List<DatanodeDetails> dns = new ArrayList<>();
    ids.forEach(dns::add);
    return getDefaultPipelineBuilder()
        .setNodes(dns)
        .build();
  }

  public static Pipeline createPipeline(Pipeline.PipelineState pipelineState)
      throws IOException {
    Objects.requireNonNull(pipelineState, "pipelineState == null");
    return getDefaultPipelineBuilder()
        .setState(pipelineState)
        .build();
  }

  public static Pipeline createPipeline(
      ReplicatedReplicationConfig replicationConfig) throws IOException {
    Objects.requireNonNull(replicationConfig, "replicationConfig == null");
    return getDefaultPipelineBuilder()
        .setReplicationConfig(replicationConfig)
        .build();
  }

  public static Pipeline createPipeline(
      ReplicatedReplicationConfig replicationConfig,
      Pipeline.PipelineState pipelineState) throws IOException {
    Objects.requireNonNull(replicationConfig, "replicationConfig == null");
    Objects.requireNonNull(pipelineState, "pipelineState == null");
    return getDefaultPipelineBuilder()
        .setState(pipelineState)
        .setReplicationConfig(replicationConfig)
        .build();
  }

  public static Pipeline createPipeline(
      ReplicatedReplicationConfig replicationConfig,
      Pipeline.PipelineState pipelineState,
      List<DatanodeDetails> datanodeDetails) throws IOException {
    Objects.requireNonNull(replicationConfig, "replicationConfig == null");
    Objects.requireNonNull(pipelineState, "pipelineState == null");
    return getDefaultPipelineBuilder()
        .setState(pipelineState)
        .setNodes(datanodeDetails)
        .setReplicationConfig(replicationConfig)
        .build();
  }

  public static Pipeline createPipeline(
      ReplicatedReplicationConfig replicationConfig, int numNodes)
      throws IOException {
    Objects.requireNonNull(replicationConfig, "replicationConfig == null");
    return getDefaultPipelineBuilder()
        .setReplicationConfig(replicationConfig)
        .setNodes(getDatanodeDetails(numNodes))
        .build();
  }

  public static Pipeline createRatisPipeline() {

    List<DatanodeDetails> nodes = new ArrayList<>();
    nodes.add(MockDatanodeDetails.randomDatanodeDetails());
    nodes.add(MockDatanodeDetails.randomDatanodeDetails());
    nodes.add(MockDatanodeDetails.randomDatanodeDetails());

    return getDefaultPipelineBuilder()
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setNodes(nodes)
        .setLeaderId(UUID.randomUUID())
        .build();
  }

  public static Pipeline createEcPipeline() {
    return createEcPipeline(new ECReplicationConfig(3, 2));
  }

  public static Pipeline createEcPipeline(ECReplicationConfig repConfig) {

    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < repConfig.getRequiredNodes(); i++) {
      nodes.add(MockDatanodeDetails.randomDatanodeDetails());
    }
    Map<DatanodeDetails, Integer> nodeIndexes = new HashMap<>();

    int index = nodes.size() - 1;
    for (DatanodeDetails dn : nodes) {
      nodeIndexes.put(dn, index);
      index--;
    }

    return Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(repConfig)
        .setNodes(nodes)
        .setReplicaIndexes(nodeIndexes)
        .build();
  }

  private MockPipeline() {
    throw new UnsupportedOperationException("no instances");
  }

  private static List<DatanodeDetails> getDatanodeDetails(int numNodes) {
    Preconditions.checkArgument(numNodes >= 1);
    final List<DatanodeDetails> ids = new ArrayList<>(numNodes);
    for (int i = 0; i < numNodes; i++) {
      ids.add(MockDatanodeDetails.randomLocalDatanodeDetails());
    }
    return ids;
  }

  private static Pipeline.Builder getDefaultPipelineBuilder() {
    return Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setNodes(getDatanodeDetails(1));
  }
}
