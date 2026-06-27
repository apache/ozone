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

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.CANNOT_CREATE_PIPELINE_FOR_EMPTY_TIER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for SimplePipelineProvider.
 */
public class TestSimplePipelineProvider {

  private PipelineProvider provider;
  private PipelineStateManager stateManager;
  @TempDir
  private File testDir;
  private DBStore dbStore;
  private List<DatanodeDetails> datanodeList;

  @BeforeEach
  public void startup() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        new File(testDir, "metadata").getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(conf, SCMDBDefinition.get());
  }

  public void init(StorageTier storageTier) throws Exception {
    assertTrue(storageTier.isUniform(), "Only support uniform StorageTier");
    assertFalse(storageTier.equals(StorageTier.EMPTY), "not support the EMPTY StorageTier");
    StorageType storageType = storageTier.getStorageTypes(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE).getRequiredNodes()).get(0);
    NodeManager nodeManager = new MockNodeManager(true, 10, storageType);
    datanodeList = nodeManager.getNodes(NodeStatus.inServiceHealthy());
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

  @ParameterizedTest
  @MethodSource("storageTiers")
  public void testCreatePipelineWithFactor(StorageTier storageTier) throws Exception {
    init(storageTier);
    HddsProtos.ReplicationFactor factor = HddsProtos.ReplicationFactor.THREE;
    Pipeline pipeline =
        provider.create(StandaloneReplicationConfig.getInstance(factor), storageTier);
    HddsProtos.Pipeline pipelineProto = pipeline.getProtobufMessage(
        ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    assertEquals(pipeline.getType(), HddsProtos.ReplicationType.STAND_ALONE);
    assertEquals(pipeline.getReplicationConfig().getRequiredNodes(), factor.getNumber());
    assertEquals(pipeline.getPipelineState(), Pipeline.PipelineState.OPEN);
    assertEquals(pipeline.getNodes().size(), factor.getNumber());
    assertTrue(pipeline.getSupportedStorageTier().contains(storageTier));

    factor = HddsProtos.ReplicationFactor.ONE;
    Pipeline pipeline1 =
        provider.create(StandaloneReplicationConfig.getInstance(factor), storageTier);
    HddsProtos.Pipeline pipelineProto1 = pipeline1.getProtobufMessage(
        ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto1);
    assertEquals(pipeline1.getType(), HddsProtos.ReplicationType.STAND_ALONE);
    assertEquals(
        ((StandaloneReplicationConfig) pipeline1.getReplicationConfig())
            .getReplicationFactor(), factor);
    assertEquals(pipeline1.getPipelineState(), Pipeline.PipelineState.OPEN);
    assertEquals(pipeline1.getNodes().size(), factor.getNumber());
    assertTrue(pipeline.getSupportedStorageTier().contains(storageTier));
  }

  private List<DatanodeDetails> createListOfNodes(int nodeCount) {
    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < nodeCount; i++) {
      nodes.add(datanodeList.get(i));
    }
    return nodes;
  }

  @Test
  public void testCreatePipelineWithNodes()
      throws Exception {
    init(StorageTier.getDefaultTier());
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
    assertTrue(pipeline.getSupportedStorageTier().contains(StorageTier.getDefaultTier()));

    factor = HddsProtos.ReplicationFactor.ONE;
    pipeline = provider.create(StandaloneReplicationConfig.getInstance(factor),
        createListOfNodes(factor.getNumber()));
    assertEquals(pipeline.getType(), HddsProtos.ReplicationType.STAND_ALONE);
    assertEquals(
        ((StandaloneReplicationConfig) pipeline.getReplicationConfig())
            .getReplicationFactor(), factor);
    assertEquals(pipeline.getPipelineState(), Pipeline.PipelineState.OPEN);
    assertEquals(pipeline.getNodes().size(), factor.getNumber());
    assertTrue(pipeline.getSupportedStorageTier().contains(StorageTier.getDefaultTier()));
  }

  @Test
  public void testCreatePipelinesInEmptyTier() throws Exception {
    init(StorageTier.getDefaultTier());
    for (HddsProtos.ReplicationFactor factor: HddsProtos.ReplicationFactor.values()) {
      if (factor == HddsProtos.ReplicationFactor.ZERO) {
        continue;
      }
      SCMException ex = assertThrows(SCMException.class, () ->
              provider.create(StandaloneReplicationConfig.getInstance(factor), StorageTier.EMPTY),
          "Expected SCMException for empty StorageTier" + factor.toString());
      assertEquals(ex.getResult(), CANNOT_CREATE_PIPELINE_FOR_EMPTY_TIER);
    }
  }

  static Stream<StorageTier> storageTiers() {
    return Stream.of(
        StorageTier.DISK,
        StorageTier.SSD,
        StorageTier.ARCHIVE
    );
  }
}
