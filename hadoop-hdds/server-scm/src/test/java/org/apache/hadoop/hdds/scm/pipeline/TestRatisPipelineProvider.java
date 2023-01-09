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

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.TestContainerManagerImpl;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackScatter;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.Assume;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.commons.collections.CollectionUtils.intersection;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for RatisPipelineProvider.
 */
public class TestRatisPipelineProvider {

  private static final HddsProtos.ReplicationType REPLICATION_TYPE =
      HddsProtos.ReplicationType.RATIS;

  private MockNodeManager nodeManager;
  private RatisPipelineProvider provider;
  private PipelineStateManager stateManager;
  private File testDir;
  private DBStore dbStore;

  public void init(int maxPipelinePerNode) throws Exception {
    init(maxPipelinePerNode, new OzoneConfiguration());
  }

  public void init(int maxPipelinePerNode, OzoneConfiguration conf)
      throws Exception {
    testDir = GenericTestUtils.getTestDir(
        TestContainerManagerImpl.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(
        conf, new SCMDBDefinition());
    nodeManager = new MockNodeManager(true, 10);
    nodeManager.setNumPipelinePerDatanode(maxPipelinePerNode);
    SCMHAManager scmhaManager = SCMHAManagerStub.getInstance(true);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT,
        maxPipelinePerNode);
    stateManager = PipelineStateManagerImpl.newBuilder()
        .setPipelineStore(SCMDBDefinition.PIPELINES.getTable(dbStore))
        .setRatisServer(scmhaManager.getRatisServer())
        .setNodeManager(nodeManager)
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();
    provider = new MockRatisPipelineProvider(nodeManager,
        stateManager, conf);
  }

  private void cleanup() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }

    FileUtil.fullyDelete(testDir);
  }

  private static void assertPipelineProperties(
      Pipeline pipeline, HddsProtos.ReplicationFactor expectedFactor,
      HddsProtos.ReplicationType expectedReplicationType,
      Pipeline.PipelineState expectedState) {
    assertEquals(expectedState, pipeline.getPipelineState());
    assertEquals(expectedReplicationType, pipeline.getType());
    assertEquals(expectedFactor.getNumber(),
        pipeline.getReplicationConfig().getRequiredNodes());
    assertEquals(expectedFactor.getNumber(), pipeline.getNodes().size());
  }

  private void createPipelineAndAssertions(
      HddsProtos.ReplicationFactor factor)
      throws IOException, TimeoutException {
    Pipeline pipeline = provider.create(RatisReplicationConfig
        .getInstance(factor));
    assertPipelineProperties(pipeline, factor, REPLICATION_TYPE,
        Pipeline.PipelineState.ALLOCATED);
    HddsProtos.Pipeline pipelineProto = pipeline.getProtobufMessage(
        ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    nodeManager.addPipeline(pipeline);

    Pipeline pipeline1 = provider.create(RatisReplicationConfig
        .getInstance(factor));
    HddsProtos.Pipeline pipelineProto1 = pipeline1.getProtobufMessage(
        ClientVersion.CURRENT_VERSION);
    assertPipelineProperties(pipeline1, factor, REPLICATION_TYPE,
        Pipeline.PipelineState.ALLOCATED);
    // New pipeline should not overlap with the previous created pipeline
    assertTrue(
        intersection(pipeline.getNodes(), pipeline1.getNodes())
            .size() < factor.getNumber());
    if (pipeline.getReplicationConfig().getRequiredNodes() == 3) {
      assertNotEquals(pipeline.getNodeSet(), pipeline1.getNodeSet());
    }
    stateManager.addPipeline(pipelineProto1);
    nodeManager.addPipeline(pipeline1);
  }

  @Test
  public void testCreatePipelineWithFactorThree() throws Exception {
    init(1);
    createPipelineAndAssertions(HddsProtos.ReplicationFactor.THREE);
    cleanup();
  }

  @Test
  public void testCreatePipelineWithFactorOne() throws Exception {
    init(1);
    createPipelineAndAssertions(HddsProtos.ReplicationFactor.ONE);
    cleanup();
  }

  private List<DatanodeDetails> createListOfNodes(int nodeCount) {
    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < nodeCount; i++) {
      nodes.add(MockDatanodeDetails.randomDatanodeDetails());
    }
    return nodes;
  }

  @Test
  public void testCreatePipelineWithFactor() throws Exception {
    init(1);
    HddsProtos.ReplicationFactor factor = HddsProtos.ReplicationFactor.THREE;
    Pipeline pipeline = provider.create(RatisReplicationConfig
        .getInstance(factor));
    assertPipelineProperties(pipeline, factor, REPLICATION_TYPE,
        Pipeline.PipelineState.ALLOCATED);
    HddsProtos.Pipeline pipelineProto = pipeline.getProtobufMessage(
        ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);

    factor = HddsProtos.ReplicationFactor.ONE;
    Pipeline pipeline1 = provider.create(RatisReplicationConfig
        .getInstance(factor));
    assertPipelineProperties(pipeline1, factor, REPLICATION_TYPE,
        Pipeline.PipelineState.ALLOCATED);
    HddsProtos.Pipeline pipelineProto1 = pipeline1.getProtobufMessage(
        ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto1);
    // With enough pipeline quote on datanodes, they should not share
    // the same set of datanodes.
    assertNotEquals(pipeline.getNodeSet(), pipeline1.getNodeSet());
    cleanup();
  }

  @Test
  public void testCreatePipelineWithNodes() throws Exception {
    init(1);
    HddsProtos.ReplicationFactor factor = HddsProtos.ReplicationFactor.THREE;
    Pipeline pipeline =
        provider.create(RatisReplicationConfig.getInstance(factor),
            createListOfNodes(factor.getNumber()));
    assertPipelineProperties(pipeline, factor, REPLICATION_TYPE,
        Pipeline.PipelineState.OPEN);

    factor = HddsProtos.ReplicationFactor.ONE;
    pipeline = provider.create(RatisReplicationConfig.getInstance(factor),
        createListOfNodes(factor.getNumber()));
    assertPipelineProperties(pipeline, factor, REPLICATION_TYPE,
        Pipeline.PipelineState.OPEN);
    cleanup();
  }

  @Test
  public void testCreateFactorTHREEPipelineWithSameDatanodes()
      throws Exception {
    init(2);
    List<DatanodeDetails> healthyNodes = nodeManager
        .getNodes(NodeStatus.inServiceHealthy()).stream()
        .limit(3).collect(Collectors.toList());
    Set<ContainerReplica> replicas = createContainerReplicas(healthyNodes);

    Pipeline pipeline1 = provider.create(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
        healthyNodes);
    Pipeline pipeline2 = provider.create(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
        healthyNodes);
    Pipeline pipeline3 = provider.createForRead(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
        replicas);

    Assertions.assertEquals(pipeline1.getNodeSet(), pipeline2.getNodeSet());
    Assertions.assertEquals(pipeline2.getNodeSet(), pipeline3.getNodeSet());
    cleanup();
  }

  @Test
  public void testCreatePipelinesDnExclude() throws Exception {

    int maxPipelinePerNode = 2;
    init(maxPipelinePerNode);
    List<DatanodeDetails> healthyNodes =
        nodeManager.getNodes(NodeStatus.inServiceHealthy());

    Assume.assumeTrue(healthyNodes.size() == 8);

    HddsProtos.ReplicationFactor factor = HddsProtos.ReplicationFactor.THREE;

    // Use up first 3 DNs for an open pipeline.
    List<DatanodeDetails> dns = healthyNodes.subList(0, 3);
    for (int i = 0; i < maxPipelinePerNode; i++) {
      // Saturate pipeline counts on all the 1st 3 DNs.
      addPipeline(dns, Pipeline.PipelineState.OPEN,
          RatisReplicationConfig.getInstance(factor));
    }
    Set<DatanodeDetails> membersOfOpenPipelines = new HashSet<>(dns);

    // Use up next 3 DNs for a closed pipeline.
    dns = healthyNodes.subList(3, 6);
    addPipeline(dns, Pipeline.PipelineState.CLOSED,
        RatisReplicationConfig.getInstance(factor));
    Set<DatanodeDetails> membersOfClosedPipelines = new HashSet<>(dns);

    // only 2 healthy DNs left that are not part of any pipeline
    Pipeline pipeline = provider.create(
        RatisReplicationConfig.getInstance(factor));
    assertPipelineProperties(pipeline, factor, REPLICATION_TYPE,
        Pipeline.PipelineState.ALLOCATED);
    HddsProtos.Pipeline pipelineProto = pipeline.getProtobufMessage(
        ClientVersion.CURRENT_VERSION);
    nodeManager.addPipeline(pipeline);
    stateManager.addPipeline(pipelineProto);

    List<DatanodeDetails> nodes = pipeline.getNodes();

    assertTrue(
        nodes.stream().noneMatch(membersOfOpenPipelines::contains),
        "nodes of new pipeline cannot be all from open pipelines");

    assertTrue(
        nodes.stream().anyMatch(membersOfClosedPipelines::contains),
        "at least 1 node should have been from members of closed pipelines");
    cleanup();
  }

  @Test
  // Test excluded nodes work correctly. Note that for Ratis, the
  // PipelinePlacementPolicy, which Ratis is hardcoded to use, does not consider
  // favored nodes.
  public void testCreateFactorTHREEPipelineWithExcludedDatanodes()
      throws Exception {
    init(1);
    int healthyCount = nodeManager.getNodes(NodeStatus.inServiceHealthy())
        .size();
    // Add all but 3 nodes to the exclude list and ensure that the 3 picked
    // nodes are not in the excluded list.
    List<DatanodeDetails> excludedNodes = nodeManager
        .getNodes(NodeStatus.inServiceHealthy()).stream()
        .limit(healthyCount - 3).collect(Collectors.toList());

    Pipeline pipeline1 = provider.create(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
        excludedNodes, Collections.EMPTY_LIST);

    for (DatanodeDetails dn : pipeline1.getNodes()) {
      assertFalse(excludedNodes.contains(dn));
    }
  }

  @Test
  // Test pipeline provider with RackScatter policy cannot create
  // pipeline due to nodes with full pipeline engagement.
  public void testFactorTHREEPipelineRackScatterEngagement()
      throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementRackScatter.class.getCanonicalName());
    conf.set(OZONE_DATANODE_PIPELINE_LIMIT, "0");
    init(0, conf);
    List<DatanodeDetails> excludedNodes = new ArrayList<>();

    Assertions.assertThrows(SCMException.class, () ->
        provider.create(RatisReplicationConfig
                .getInstance(ReplicationFactor.THREE),
            excludedNodes, Collections.EMPTY_LIST));
  }

  @Test
  public void testCreatePipelinesWhenNotEnoughSpace() throws Exception {
    String expectedErrorSubstring = "Unable to find enough" +
        " nodes that meet the space requirement";

    // Use large enough container or metadata sizes that no node will have
    // enough space to hold one.
    OzoneConfiguration largeContainerConf = new OzoneConfiguration();
    largeContainerConf.set(OZONE_SCM_CONTAINER_SIZE, "300TB");
    init(1, largeContainerConf);
    for (ReplicationFactor factor: ReplicationFactor.values()) {
      if (factor == ReplicationFactor.ZERO) {
        continue;
      }
      try {
        provider.create(RatisReplicationConfig.getInstance(factor));
        Assertions.fail("Expected SCMException for large container size with " +
            "replication factor " + factor.toString());
      } catch (SCMException ex) {
        Assertions.assertTrue(ex.getMessage().contains(expectedErrorSubstring));
      }
    }

    OzoneConfiguration largeMetadataConf = new OzoneConfiguration();
    largeMetadataConf.set(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN, "300TB");
    init(1, largeMetadataConf);
    for (ReplicationFactor factor: ReplicationFactor.values()) {
      if (factor == ReplicationFactor.ZERO) {
        continue;
      }
      try {
        provider.create(RatisReplicationConfig.getInstance(factor));
        Assertions.fail("Expected SCMException for large metadata size with " +
            "replication factor " + factor.toString());
      } catch (SCMException ex) {
        Assertions.assertTrue(ex.getMessage().contains(expectedErrorSubstring));
      }
    }
    cleanup();
  }

  private void addPipeline(
      List<DatanodeDetails> dns,
      Pipeline.PipelineState open, ReplicationConfig replicationConfig)
      throws IOException, TimeoutException {
    Pipeline openPipeline = Pipeline.newBuilder()
        .setReplicationConfig(replicationConfig)
        .setNodes(dns)
        .setState(open)
        .setId(PipelineID.randomId())
        .build();
    HddsProtos.Pipeline pipelineProto = openPipeline.getProtobufMessage(
        ClientVersion.CURRENT_VERSION);

    stateManager.addPipeline(pipelineProto);
    nodeManager.addPipeline(openPipeline);
  }

  private Set<ContainerReplica> createContainerReplicas(
      List<DatanodeDetails> dns) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (DatanodeDetails dn : dns) {
      ContainerReplica r = ContainerReplica.newBuilder()
          .setBytesUsed(1)
          .setContainerID(ContainerID.valueOf(1))
          .setContainerState(StorageContainerDatanodeProtocolProtos
              .ContainerReplicaProto.State.CLOSED)
          .setKeyCount(1)
          .setOriginNodeId(UUID.randomUUID())
          .setSequenceId(1)
          .setReplicaIndex(0)
          .setDatanodeDetails(dn)
          .build();
      replicas.add(r);
    }
    return replicas;
  }
}
