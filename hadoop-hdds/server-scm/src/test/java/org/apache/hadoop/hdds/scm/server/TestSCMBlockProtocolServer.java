/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.server;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.ozone.test.GenericTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;

/**
 * Test class for @{@link SCMBlockProtocolServer}.
 */
public class TestSCMBlockProtocolServer {
  private OzoneConfiguration config;
  private SCMBlockProtocolServer server;
  private StorageContainerManager scm;
  private NodeManager nodeManager;
  private ScmBlockLocationProtocolServerSideTranslatorPB service;
  private static final int NODE_COUNT = 10;

  @BeforeEach
  public void setUp() throws Exception {
    config = new OzoneConfiguration();
    File dir = GenericTestUtils.getRandomizedTestDir();
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());
    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true));
    configurator.setScmContext(SCMContext.emptyContext());
    scm = HddsTestUtils.getScm(config, configurator);
    scm.start();
    scm.exitSafeMode();
    // add nodes to scm node manager
    nodeManager = scm.getScmNodeManager();
    for (int i = 0; i < NODE_COUNT; i++) {
      nodeManager.register(randomDatanodeDetails(), null, null);

    }
    server = scm.getBlockProtocolServer();
    service = new ScmBlockLocationProtocolServerSideTranslatorPB(server, scm,
        Mockito.mock(ProtocolMessageMetrics.class));
    BlockManager mockedBlockManager = Mockito.mock(BlockManager.class);
    Mockito.when(mockedBlockManager.allocateBlock(
        Mockito.anyLong(), Mockito.any(ReplicationConfig.class),
            Mockito.anyString(), Mockito.any(ExcludeList.class)))
        .thenAnswer(invocation -> new AllocatedBlock.Builder()
            .setSize((long) invocation.getArguments()[0])
            .setContainerBlockID(new ContainerBlockID(
                RandomUtils.nextInt(), RandomUtils.nextInt()))
            .setPipeline(new Pipeline.Builder()
                .setId(PipelineID.randomId())
                .setCreateTimestamp(System.currentTimeMillis())
                .setState(Pipeline.PipelineState.OPEN)
                .setReplicationConfig(invocation.getArgument(1))
                .setNodes(Collections.emptyList())
                .build())
            .build()
        );
    scm.setScmBlockManager(mockedBlockManager);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (scm != null) {
      scm.stop();
      scm.join();
    }
  }

  private static final ReplicationConfig RATIS_THREE =
      ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
          ReplicationFactor.THREE);

  private static final ReplicationConfig EC_3_2 =
      new ECReplicationConfig(3, 2);

  private static final ReplicationConfig EC_6_3 =
      new ECReplicationConfig(6, 3);

  private static Stream<Arguments> allocateBlockParams() {
    return Stream.of(
        Arguments.of(10, 10, RATIS_THREE, 1),
        Arguments.of(11, 10, RATIS_THREE, 2),
        Arguments.of(11, 0, RATIS_THREE, 1), // scm decide block size
        Arguments.of(30, 10, EC_3_2, 1), // 1 stripe is 3 x 10
        Arguments.of(31, 10, EC_3_2, 2),
        Arguments.of(60, 10, EC_6_3, 1), // 1 stripe is 6 x 10
        Arguments.of(61, 10, EC_6_3, 2)
    );
  }

  @ParameterizedTest
  @MethodSource("allocateBlockParams")
  public void testAllocateBlock(long requestedSize, long blockSize,
      ReplicationConfig repConfig, int expectedBlocks)
      throws Exception {
    List<AllocatedBlock> blocks = server.allocateBlock(
        requestedSize, blockSize, repConfig,
        UUID.randomUUID().toString(), new ExcludeList());
    Assertions.assertEquals(expectedBlocks, blocks.size());

    final long totalBlockSize = blocks.stream()
        .mapToLong(AllocatedBlock::getSize)
        .sum();
    final long writableSize = repConfig instanceof ECReplicationConfig
        ? ((ECReplicationConfig) repConfig).getData() * totalBlockSize
        : totalBlockSize;
    Assertions.assertTrue(writableSize >= requestedSize);

    // TODO: allow smaller block size for the last block
    final long expectedBlockSize = blockSize > 0 ?
        blockSize : server.getScmBlockSize();
    for (AllocatedBlock block : blocks) {
      Assertions.assertEquals(expectedBlockSize, block.getSize());
      Assertions.assertEquals(repConfig, block.getPipeline()
          .getReplicationConfig());
    }
  }

  @Test
  public void testSortDatanodes() throws Exception {
    List<String> nodes = new ArrayList();
    nodeManager.getAllNodes().stream().forEach(
        node -> nodes.add(node.getNetworkName()));

    // sort normal datanodes
    String client;
    client = nodes.get(0);
    List<DatanodeDetails> datanodeDetails =
        server.sortDatanodes(nodes, client);
    System.out.println("client = " + client);
    datanodeDetails.stream().forEach(
        node -> System.out.println(node.toString()));
    Assertions.assertTrue(datanodeDetails.size() == NODE_COUNT);

    // illegal client 1
    client += "X";
    datanodeDetails = server.sortDatanodes(nodes, client);
    System.out.println("client = " + client);
    datanodeDetails.stream().forEach(
        node -> System.out.println(node.toString()));
    Assertions.assertTrue(datanodeDetails.size() == NODE_COUNT);
    // illegal client 2
    client = "/default-rack";
    datanodeDetails = server.sortDatanodes(nodes, client);
    System.out.println("client = " + client);
    datanodeDetails.stream().forEach(
        node -> System.out.println(node.toString()));
    Assertions.assertTrue(datanodeDetails.size() == NODE_COUNT);

    // unknown node to sort
    nodes.add(UUID.randomUUID().toString());
    ScmBlockLocationProtocolProtos.SortDatanodesRequestProto request =
        ScmBlockLocationProtocolProtos.SortDatanodesRequestProto
            .newBuilder()
            .addAllNodeNetworkName(nodes)
            .setClient(client)
            .build();
    ScmBlockLocationProtocolProtos.SortDatanodesResponseProto resp =
        service.sortDatanodes(request, ClientVersion.CURRENT_VERSION);
    Assertions.assertTrue(resp.getNodeList().size() == NODE_COUNT);
    System.out.println("client = " + client);
    resp.getNodeList().stream().forEach(
        node -> System.out.println(node.getNetworkName()));

    // all unknown nodes
    nodes.clear();
    nodes.add(UUID.randomUUID().toString());
    nodes.add(UUID.randomUUID().toString());
    nodes.add(UUID.randomUUID().toString());
    request = ScmBlockLocationProtocolProtos.SortDatanodesRequestProto
        .newBuilder()
        .addAllNodeNetworkName(nodes)
        .setClient(client)
        .build();
    resp = service.sortDatanodes(request, ClientVersion.CURRENT_VERSION);
    System.out.println("client = " + client);
    Assertions.assertTrue(resp.getNodeList().size() == 0);
    resp.getNodeList().stream().forEach(
        node -> System.out.println(node.getNetworkName()));
  }
}