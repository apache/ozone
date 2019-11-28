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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.commons.collections.CollectionUtils.intersection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for RatisPipelineProvider.
 */
public class TestRatisPipelineProvider {

  private static final HddsProtos.ReplicationType REPLICATION_TYPE =
      HddsProtos.ReplicationType.RATIS;

  private NodeManager nodeManager;
  private PipelineProvider provider;
  private PipelineStateManager stateManager;

  @Before
  public void init() throws Exception {
    nodeManager = new MockNodeManager(true, 10);
    stateManager = new PipelineStateManager(new OzoneConfiguration());
    provider = new MockRatisPipelineProvider(nodeManager,
        stateManager, new OzoneConfiguration());
  }

  private void createPipelineAndAssertions(
          HddsProtos.ReplicationFactor factor) throws IOException {
    Pipeline pipeline = provider.create(factor);
    assertPipelineProperties(pipeline, factor, REPLICATION_TYPE,
        Pipeline.PipelineState.ALLOCATED);
    stateManager.addPipeline(pipeline);

    Pipeline pipeline1 = provider.create(factor);
    assertPipelineProperties(pipeline1, factor, REPLICATION_TYPE,
        Pipeline.PipelineState.ALLOCATED);
    // New pipeline should not overlap with the previous created pipeline
    assertTrue(
        intersection(pipeline.getNodes(), pipeline1.getNodes())
            .isEmpty());
    stateManager.addPipeline(pipeline1);
  }

  @Test
  public void testCreatePipelineWithFactor() throws IOException {
    HddsProtos.ReplicationFactor factor = HddsProtos.ReplicationFactor.THREE;
    Pipeline pipeline = provider.create(factor);
    assertPipelineProperties(pipeline, factor, REPLICATION_TYPE,
        Pipeline.PipelineState.ALLOCATED);
    stateManager.addPipeline(pipeline);

    factor = HddsProtos.ReplicationFactor.ONE;
    Pipeline pipeline1 = provider.create(factor);
    assertPipelineProperties(pipeline1, factor, REPLICATION_TYPE,
        Pipeline.PipelineState.ALLOCATED);
    stateManager.addPipeline(pipeline1);
    // New pipeline should overlap with the previous created pipeline,
    // and one datanode should overlap between the two types.
    assertEquals(1,
        intersection(pipeline.getNodes(), pipeline1.getNodes()).size());
  }

  @Test
  public void testCreatePipelineWithFactorThree() throws IOException {
    createPipelineAndAssertions(HddsProtos.ReplicationFactor.THREE);
  }

  @Test
  public void testCreatePipelineWithFactorOne() throws IOException {
    createPipelineAndAssertions(HddsProtos.ReplicationFactor.ONE);
  }

  private List<DatanodeDetails> createListOfNodes(int nodeCount) {
    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < nodeCount; i++) {
      nodes.add(TestUtils.randomDatanodeDetails());
    }
    return nodes;
  }

  @Test
  public void testCreatePipelineWithNodes() {
    HddsProtos.ReplicationFactor factor = HddsProtos.ReplicationFactor.THREE;
    Pipeline pipeline =
        provider.create(factor, createListOfNodes(factor.getNumber()));
    assertPipelineProperties(pipeline, factor, REPLICATION_TYPE,
        Pipeline.PipelineState.OPEN);

    factor = HddsProtos.ReplicationFactor.ONE;
    pipeline = provider.create(factor, createListOfNodes(factor.getNumber()));
    assertPipelineProperties(pipeline, factor, REPLICATION_TYPE,
        Pipeline.PipelineState.OPEN);
  }

  @Test
  public void testCreatePipelinesDnExclude() throws IOException {
    List<DatanodeDetails> healthyNodes =
        nodeManager.getNodes(HddsProtos.NodeState.HEALTHY);

    Assume.assumeTrue(healthyNodes.size() == 8);

    HddsProtos.ReplicationFactor factor = HddsProtos.ReplicationFactor.THREE;

    // Use up first 3 DNs for an open pipeline.
    List<DatanodeDetails> dns = healthyNodes.subList(0, 3);
    addPipeline(dns, factor, Pipeline.PipelineState.OPEN, REPLICATION_TYPE);
    Set<DatanodeDetails> membersOfOpenPipelines = new HashSet<>(dns);

    // Use up next 3 DNs for a closed pipeline.
    dns = healthyNodes.subList(3, 6);
    addPipeline(dns, factor, Pipeline.PipelineState.CLOSED, REPLICATION_TYPE);
    Set<DatanodeDetails> membersOfClosedPipelines = new HashSet<>(dns);

    // only 2 healthy DNs left that are not part of any pipeline
    Pipeline pipeline = provider.create(factor);
    assertPipelineProperties(pipeline, factor, REPLICATION_TYPE,
        Pipeline.PipelineState.ALLOCATED);

    List<DatanodeDetails> nodes = pipeline.getNodes();

    assertTrue(
        "nodes of new pipeline cannot be from open pipelines",
        nodes.stream().noneMatch(membersOfOpenPipelines::contains));

    assertTrue(
        "at least 1 node should have been from members of closed pipelines",
        nodes.stream().anyMatch(membersOfClosedPipelines::contains));
  }

  private static void assertPipelineProperties(
      Pipeline pipeline, HddsProtos.ReplicationFactor expectedFactor,
      HddsProtos.ReplicationType expectedReplicationType,
      Pipeline.PipelineState expectedState) {
    assertEquals(expectedState, pipeline.getPipelineState());
    assertEquals(expectedReplicationType, pipeline.getType());
    assertEquals(expectedFactor, pipeline.getFactor());
    assertEquals(expectedFactor.getNumber(), pipeline.getNodes().size());
  }

  private void addPipeline(
      List<DatanodeDetails> dns, HddsProtos.ReplicationFactor factor,
      Pipeline.PipelineState open, HddsProtos.ReplicationType replicationType)
      throws IOException {
    Pipeline openPipeline = Pipeline.newBuilder()
        .setType(replicationType)
        .setFactor(factor)
        .setNodes(dns)
        .setState(open)
        .setId(PipelineID.randomId())
        .build();

    stateManager.addPipeline(openPipeline);
  }
}