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

import static org.apache.commons.collections4.CollectionUtils.intersection;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackScatter;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.ClientVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Test for {@link RatisPipelineProvider}.
 */
public class TestRatisPipelineProvider {

  private static final HddsProtos.ReplicationType REPLICATION_TYPE =
      HddsProtos.ReplicationType.RATIS;

  private MockNodeManager nodeManager;
  private RatisPipelineProvider provider;
  private PipelineStateManager stateManager;
  @TempDir
  private File testDir;
  private DBStore dbStore;
  private int nodeCount = 10;

  public void init(int maxPipelinePerNode) throws Exception {
    init(maxPipelinePerNode, new OzoneConfiguration());
  }

  public void init(int maxPipelinePerNode, OzoneConfiguration conf)
      throws Exception {
    init(maxPipelinePerNode, conf, testDir);
  }

  public void init(int maxPipelinePerNode, OzoneConfiguration conf, File dir) throws Exception {
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(conf, SCMDBDefinition.get());
    nodeManager = new MockNodeManager(true, nodeCount);
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

  @AfterEach
  void cleanup() throws Exception {
    nodeCount = 10;
    if (dbStore != null) {
      dbStore.close();
    }
  }

  private static void assertPipelineProperties(
      Pipeline pipeline, HddsProtos.ReplicationFactor expectedFactor,
      HddsProtos.ReplicationType expectedReplicationType,
      Pipeline.PipelineState expectedState) {
    assertEquals(expectedState, pipeline.getPipelineState());
    assertEquals(expectedReplicationType, pipeline.getType());
    assertEquals(expectedFactor.getNumber(), pipeline.getReplicationConfig().getRequiredNodes());
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
        ClientVersion.CURRENT.serialize());
    stateManager.addPipeline(pipelineProto);
    nodeManager.addPipeline(pipeline);

    Pipeline pipeline1 = provider.create(RatisReplicationConfig
        .getInstance(factor));
    HddsProtos.Pipeline pipelineProto1 = pipeline1.getProtobufMessage(
        ClientVersion.CURRENT.serialize());
    assertPipelineProperties(pipeline1, factor, REPLICATION_TYPE,
        Pipeline.PipelineState.ALLOCATED);
    // New pipeline should not overlap with the previous created pipeline
    assertThat(intersection(pipeline.getNodes(), pipeline1.getNodes()).size())
        .isLessThan(factor.getNumber());
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
  }

  @Test
  public void testCreatePipelineWithFactorOne() throws Exception {
    init(1);
    createPipelineAndAssertions(HddsProtos.ReplicationFactor.ONE);
  }

  private List<DatanodeDetails> createListOfNodes(int count) {
    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < count; i++) {
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
        ClientVersion.CURRENT.serialize());
    stateManager.addPipeline(pipelineProto);

    factor = HddsProtos.ReplicationFactor.ONE;
    Pipeline pipeline1 = provider.create(RatisReplicationConfig
        .getInstance(factor));
    assertPipelineProperties(pipeline1, factor, REPLICATION_TYPE,
        Pipeline.PipelineState.ALLOCATED);
    HddsProtos.Pipeline pipelineProto1 = pipeline1.getProtobufMessage(
        ClientVersion.CURRENT.serialize());
    stateManager.addPipeline(pipelineProto1);
    // With enough pipeline quote on datanodes, they should not share
    // the same set of datanodes.
    assertNotEquals(pipeline.getNodeSet(), pipeline1.getNodeSet());
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

    assertEquals(pipeline1.getNodeSet(), pipeline2.getNodeSet());
    assertEquals(pipeline2.getNodeSet(), pipeline3.getNodeSet());
  }

  @Test
  public void testCreatePipelinesDnExclude() throws Exception {

    int maxPipelinePerNode = 2;
    init(maxPipelinePerNode);
    List<DatanodeDetails> healthyNodes =
        nodeManager.getNodes(NodeStatus.inServiceHealthy());

    Assumptions.assumeTrue(healthyNodes.size() == 8);

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
        ClientVersion.CURRENT.serialize());
    nodeManager.addPipeline(pipeline);
    stateManager.addPipeline(pipelineProto);

    List<DatanodeDetails> nodes = pipeline.getNodes();

    assertTrue(
        nodes.stream().noneMatch(membersOfOpenPipelines::contains),
        "nodes of new pipeline cannot be all from open pipelines");

    assertTrue(
        nodes.stream().anyMatch(membersOfClosedPipelines::contains),
        "at least 1 node should have been from members of closed pipelines");
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
      assertThat(excludedNodes).doesNotContain(dn);
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

    assertThrows(SCMException.class, () ->
        provider.create(RatisReplicationConfig
                .getInstance(ReplicationFactor.THREE),
            excludedNodes, Collections.EMPTY_LIST));
  }

  @Test
  public void testCreatePipelinesWhenNotEnoughSpace(@TempDir File tempDir) throws Exception {
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
      SCMException ex =
          assertThrows(SCMException.class, () -> provider.create(RatisReplicationConfig.getInstance(factor)),
              "Expected SCMException for large container size with replication factor " + factor.toString());
      assertThat(ex.getMessage()).contains(expectedErrorSubstring);
    }

    OzoneConfiguration largeMetadataConf = new OzoneConfiguration();
    largeMetadataConf.set(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN, "300TB");
    init(1, largeMetadataConf, tempDir);
    for (ReplicationFactor factor: ReplicationFactor.values()) {
      if (factor == ReplicationFactor.ZERO) {
        continue;
      }
      SCMException ex =
          assertThrows(SCMException.class, () -> provider.create(RatisReplicationConfig.getInstance(factor)),
              "Expected SCMException for large metadata size with replication factor " + factor.toString());
      assertThat(ex.getMessage()).contains(expectedErrorSubstring);
    }
  }

  @Test
  public void testCreatePipelineWithDefaultLimit() throws Exception {
    // Create conf without setting OZONE_DATANODE_PIPELINE_LIMIT
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());

    dbStore = DBStoreBuilder.createDBStore(conf, SCMDBDefinition.get());

    // MockNodeManager(true, 10) typically gives 8 healthy nodes in this test suite.
    nodeManager = new MockNodeManager(true, nodeCount);
    // Give a large quota in MockNodeManager so we don't fail early due to mock quota.
    nodeManager.setNumPipelinePerDatanode(100);

    SCMHAManager scmhaManager = SCMHAManagerStub.getInstance(true);
    stateManager = PipelineStateManagerImpl.newBuilder()
        .setPipelineStore(SCMDBDefinition.PIPELINES.getTable(dbStore))
        .setRatisServer(scmhaManager.getRatisServer())
        .setNodeManager(nodeManager)
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();

    provider = new MockRatisPipelineProvider(nodeManager, stateManager, conf);

    int healthyCount = nodeManager.getNodes(NodeStatus.inServiceHealthy()).size();
    int defaultLimit = ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT;
    assertEquals(2, defaultLimit);

    // Max pipelines before exceeding per-DN default limit.
    int maxPipelines = (healthyCount * defaultLimit)
        / ReplicationFactor.THREE.getNumber();

    // Create pipelines up to maxPipelines.
    for (int i = 0; i < maxPipelines; i++) {
      Pipeline p = provider.create(
          RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
          new ArrayList<>(), new ArrayList<>());
      stateManager.addPipeline(p.getProtobufMessage(ClientVersion.CURRENT.serialize()));
    }

    // Next pipeline creation should fail with default limit message.
    SCMException ex = assertThrows(SCMException.class, () ->
        provider.create(RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
            new ArrayList<>(), new ArrayList<>())
    );

    assertThat(ex.getMessage())
        .contains("limit per datanode: " + defaultLimit)
        .contains("replicationConfig: RATIS/THREE");
  }

  @ParameterizedTest
  @CsvSource({ "1, 3", "2, 6"})
  public void testCreatePipelineThrowErrorWithDataNodeLimit(int limit, int pipelineCount) throws Exception {
    // increasing node count to avoid intermittent failures due to unhealthy nodes.
    nodeCount = 13;
    init(limit, new OzoneConfiguration(), testDir);

    // Create pipelines up to the limit (3 for limit=1, 6 for limit=2).
    for (int i = 0; i < pipelineCount; i++) {
      stateManager.addPipeline(
          provider.create(RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
              new ArrayList<>(), new ArrayList<>()).getProtobufMessage(ClientVersion.CURRENT.serialize())
      );
    }

    // Verify that creating an additional pipeline throws an exception.
    SCMException exception = assertThrows(SCMException.class, () ->
        provider.create(RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
            new ArrayList<>(), new ArrayList<>())
    );

    // Validate exception message.
    String expectedError = String.format(
        "Cannot create pipeline as it would exceed the limit per datanode: %d replicationConfig: RATIS/THREE", limit);
    assertEquals(expectedError, exception.getMessage());
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
        ClientVersion.CURRENT.serialize());

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
          .setOriginNodeId(DatanodeID.randomID())
          .setSequenceId(1)
          .setReplicaIndex(0)
          .setDatanodeDetails(dn)
          .build();
      replicas.add(r);
    }
    return replicas;
  }
}
