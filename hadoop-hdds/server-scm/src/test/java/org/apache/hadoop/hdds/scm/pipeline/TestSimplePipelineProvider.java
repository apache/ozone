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

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.TestContainerManagerImpl;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Test for SimplePipelineProvider.
 */
public class TestSimplePipelineProvider {

  private NodeManager nodeManager;
  private PipelineProvider provider;
  private PipelineStateManager stateManager;
  private File testDir;
  private DBStore dbStore;

  @BeforeEach
  public void init() throws Exception {
    nodeManager = new MockNodeManager(true, 10);
    final OzoneConfiguration conf = SCMTestUtils.getConf();
    testDir = GenericTestUtils.getTestDir(
        TestContainerManagerImpl.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(
        conf, new SCMDBDefinition());
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

    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testCreatePipelineWithFactor() throws Exception {
    HddsProtos.ReplicationFactor factor = HddsProtos.ReplicationFactor.THREE;
    Pipeline pipeline =
        provider.create(StandaloneReplicationConfig.getInstance(factor));
    HddsProtos.Pipeline pipelineProto = pipeline.getProtobufMessage(
        ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    Assertions.assertEquals(pipeline.getType(),
        HddsProtos.ReplicationType.STAND_ALONE);
    Assertions.assertEquals(pipeline.getReplicationConfig().getRequiredNodes(),
        factor.getNumber());
    Assertions.assertEquals(pipeline.getPipelineState(),
        Pipeline.PipelineState.OPEN);
    Assertions.assertEquals(pipeline.getNodes().size(), factor.getNumber());

    factor = HddsProtos.ReplicationFactor.ONE;
    Pipeline pipeline1 =
        provider.create(StandaloneReplicationConfig.getInstance(factor));
    HddsProtos.Pipeline pipelineProto1 = pipeline1.getProtobufMessage(
        ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto1);
    Assertions.assertEquals(pipeline1.getType(),
        HddsProtos.ReplicationType.STAND_ALONE);
    Assertions.assertEquals(
        ((StandaloneReplicationConfig) pipeline1.getReplicationConfig())
            .getReplicationFactor(), factor);
    Assertions.assertEquals(pipeline1.getPipelineState(),
        Pipeline.PipelineState.OPEN);
    Assertions.assertEquals(pipeline1.getNodes().size(), factor.getNumber());
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
    Assertions.assertEquals(pipeline.getType(),
        HddsProtos.ReplicationType.STAND_ALONE);
    Assertions.assertEquals(
        ((StandaloneReplicationConfig) pipeline.getReplicationConfig())
            .getReplicationFactor(), factor);
    Assertions.assertEquals(pipeline.getPipelineState(),
        Pipeline.PipelineState.OPEN);
    Assertions.assertEquals(pipeline.getNodes().size(), factor.getNumber());

    factor = HddsProtos.ReplicationFactor.ONE;
    pipeline = provider.create(StandaloneReplicationConfig.getInstance(factor),
        createListOfNodes(factor.getNumber()));
    Assertions.assertEquals(pipeline.getType(),
        HddsProtos.ReplicationType.STAND_ALONE);
    Assertions.assertEquals(
        ((StandaloneReplicationConfig) pipeline.getReplicationConfig())
            .getReplicationFactor(), factor);
    Assertions.assertEquals(pipeline.getPipelineState(),
        Pipeline.PipelineState.OPEN);
    Assertions.assertEquals(pipeline.getNodes().size(), factor.getNumber());
  }
}