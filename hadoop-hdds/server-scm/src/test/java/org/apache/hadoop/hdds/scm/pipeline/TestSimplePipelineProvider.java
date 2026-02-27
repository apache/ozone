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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for SimplePipelineProvider.
 */
public class TestSimplePipelineProvider {

  private PipelineProvider provider;
  private PipelineStateManager stateManager;
  @TempDir
  private File testDir;
  private DBStore dbStore;

  @BeforeEach
  public void init() throws Exception {
    NodeManager nodeManager = new MockNodeManager(true, 10);
    final OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    dbStore = DBStoreBuilder.createDBStore(conf, SCMDBDefinition.get());
    SCMHAManager scmhaManager = SCMHAManagerStub.getInstance(true);
    stateManager = PipelineStateManagerImpl.newBuilder()
        .setPipelineStore(SCMDBDefinition.PIPELINES.getTable(dbStore))
        .setRatisServer(scmhaManager.getRatisServer())
        .setNodeManager(nodeManager)
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();
    provider = new SimplePipelineProvider(nodeManager, stateManager);
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }
  }

  @Test
  public void testCreatePipelineWithFactor() throws Exception {
    HddsProtos.ReplicationFactor factor = HddsProtos.ReplicationFactor.THREE;
    Pipeline pipeline =
        provider.create(StandaloneReplicationConfig.getInstance(factor));
    HddsProtos.Pipeline pipelineProto = pipeline.getProtobufMessage(
        ClientVersion.CURRENT.serialize());
    stateManager.addPipeline(pipelineProto);
    assertEquals(pipeline.getType(), HddsProtos.ReplicationType.STAND_ALONE);
    assertEquals(pipeline.getReplicationConfig().getRequiredNodes(), factor.getNumber());
    assertEquals(pipeline.getPipelineState(), Pipeline.PipelineState.OPEN);
    assertEquals(pipeline.getNodes().size(), factor.getNumber());

    factor = HddsProtos.ReplicationFactor.ONE;
    Pipeline pipeline1 =
        provider.create(StandaloneReplicationConfig.getInstance(factor));
    HddsProtos.Pipeline pipelineProto1 = pipeline1.getProtobufMessage(
        ClientVersion.CURRENT.serialize());
    stateManager.addPipeline(pipelineProto1);
    assertEquals(pipeline1.getType(), HddsProtos.ReplicationType.STAND_ALONE);
    assertEquals(
        ((StandaloneReplicationConfig) pipeline1.getReplicationConfig())
            .getReplicationFactor(), factor);
    assertEquals(pipeline1.getPipelineState(), Pipeline.PipelineState.OPEN);
    assertEquals(pipeline1.getNodes().size(), factor.getNumber());
  }

  private List<DatanodeDetails> createListOfNodes(int nodeCount) {
    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < nodeCount; i++) {
      nodes.add(MockDatanodeDetails.randomDatanodeDetails());
    }
    return nodes;
  }

  @Test
  public void testCreatePipelineWithNodes() throws IOException {
    HddsProtos.ReplicationFactor factor = HddsProtos.ReplicationFactor.THREE;
    Pipeline pipeline =
        provider.create(StandaloneReplicationConfig.getInstance(factor),
            createListOfNodes(factor.getNumber()));
    assertEquals(pipeline.getType(),
        HddsProtos.ReplicationType.STAND_ALONE);
    assertEquals(
        ((StandaloneReplicationConfig) pipeline.getReplicationConfig())
            .getReplicationFactor(), factor);
    assertEquals(pipeline.getPipelineState(), Pipeline.PipelineState.OPEN);
    assertEquals(pipeline.getNodes().size(), factor.getNumber());

    factor = HddsProtos.ReplicationFactor.ONE;
    pipeline = provider.create(StandaloneReplicationConfig.getInstance(factor),
        createListOfNodes(factor.getNumber()));
    assertEquals(pipeline.getType(), HddsProtos.ReplicationType.STAND_ALONE);
    assertEquals(
        ((StandaloneReplicationConfig) pipeline.getReplicationConfig())
            .getReplicationFactor(), factor);
    assertEquals(pipeline.getPipelineState(), Pipeline.PipelineState.OPEN);
    assertEquals(pipeline.getNodes().size(), factor.getNumber());
  }
}
