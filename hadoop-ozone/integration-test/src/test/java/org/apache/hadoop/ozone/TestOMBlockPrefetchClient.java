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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCHED_BLOCKS_EXPIRY_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCH_MAX_BLOCKS;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createDatanodeDetails;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.ozone.om.OMBlockPrefetchClient;
import org.apache.hadoop.ozone.om.OMBlockPrefetchClient.ExpiringAllocatedBlock;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is to test the boundary cases for prefetching and caching of blocks from SCM.
 */
@ExtendWith(MockitoExtension.class)
public class TestOMBlockPrefetchClient {
  private static final Logger LOG = LoggerFactory.getLogger(TestOMBlockPrefetchClient.class);
  @Mock
  private ScmBlockLocationProtocol scmBlockLocationProtocol;
  @Mock
  private OzoneManager ozoneManager;
  @Mock
  private NetworkTopology networkTopology;
  private OMBlockPrefetchClient omBlockPrefetchClient;
  private OzoneConfiguration conf;
  private static final long BLOCK_SIZE = 1024 * 1024 * 256;
  private static final String SERVICE_ID = "testService";
  private static final String CLIENT_RACK = "/rack-client";
  private static final ReplicationConfig REP_CONFIG =
      ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE);
  private static final ReplicationConfig UNTESTED_REP_CONFIG =
      new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, 2048);
  private static final int MAX_BLOCKS_PER_QUEUE = 10;
  private static final long EXPIRY_INTERVAL_MS = 1000 * 60;
  private static final int NUM_DATANODES = 5;
  private static final String RACK_0 = "/rack0";
  private static final String RACK_1 = "/rack1";
  private Map<ReplicationConfig, ConcurrentLinkedDeque<ExpiringAllocatedBlock>> blockQueueMap;
  private Constructor<?> expiringBlockConstructor;
  private List<DatanodeDetails> datanodes;

  @BeforeEach
  void setUp() throws Exception {
    conf = new OzoneConfiguration();
    conf.setInt(OZONE_OM_PREFETCH_MAX_BLOCKS, MAX_BLOCKS_PER_QUEUE);
    conf.setTimeDuration(OZONE_OM_PREFETCHED_BLOCKS_EXPIRY_INTERVAL, EXPIRY_INTERVAL_MS, TimeUnit.MILLISECONDS);
    conf.setBoolean(HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME, true);
    conf.setClass(ScmConfigKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        StaticMapping.class, DNSToSwitchMapping.class);
    when(ozoneManager.isAllocateBlockCacheEnabled()).thenReturn(true);
    omBlockPrefetchClient = new OMBlockPrefetchClient(ozoneManager, scmBlockLocationProtocol);
    datanodes = createFixedDatanodes(NUM_DATANODES);
    StaticMapping.resetMap();
    StaticMapping.addNodeToRack(null, CLIENT_RACK);
    datanodes.forEach(dn -> StaticMapping.addNodeToRack(dn.getHostName(), dn.getNetworkLocation()));
    omBlockPrefetchClient.start(conf);
    Field queueMapField = OMBlockPrefetchClient.class.getDeclaredField("blockQueueMap");
    queueMapField.setAccessible(true);
    blockQueueMap = (Map<ReplicationConfig, ConcurrentLinkedDeque<ExpiringAllocatedBlock>>)
        queueMapField.get(omBlockPrefetchClient);

    Class<?> expiringBlockClass =
        Class.forName("org.apache.hadoop.ozone.om.OMBlockPrefetchClient$ExpiringAllocatedBlock");
    expiringBlockConstructor = expiringBlockClass.getDeclaredConstructor(AllocatedBlock.class, long.class);
    expiringBlockConstructor.setAccessible(true);
  }

  private List<DatanodeDetails> createFixedDatanodes(int count) {
    List<DatanodeDetails> dns = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String rack = (i % 2 == 0) ? RACK_0 : RACK_1;
      String host = "dn" + i + ".example.com";
      dns.add(DatanodeDetails.newBuilder()
          .setUuid(UUID.randomUUID())
          .setHostName(host)
          .setIpAddress("10.0." + (i / 2) + "." + (i % 2 + 1))
          .setNetworkLocation(rack)
          .build());
    }
    return dns;
  }

  @AfterEach
  void tearDown() {
    if (omBlockPrefetchClient != null) {
      omBlockPrefetchClient.stop();
    }
    StaticMapping.resetMap();
  }

  private AllocatedBlock createMockAllocatedBlock(long containerId, long localId, List<DatanodeDetails> pipelineNodes) {
    Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(REP_CONFIG)
        .setNodes(pipelineNodes)
        .setLeaderId(pipelineNodes.isEmpty() ? null : pipelineNodes.get(0).getID())
        .setState(Pipeline.PipelineState.OPEN)
        .build();
    return new AllocatedBlock.Builder()
        .setContainerBlockID(new ContainerBlockID(containerId, localId))
        .setPipeline(pipeline)
        .build();
  }

  private Queue<ExpiringAllocatedBlock> getInternalQueue(ReplicationConfig config) {
    return blockQueueMap.computeIfAbsent(config, k -> new ConcurrentLinkedDeque<>());
  }

  private void addToQueue(Queue<ExpiringAllocatedBlock> queue, AllocatedBlock block, long expiryTime) {
    try {
      OMBlockPrefetchClient.ExpiringAllocatedBlock expiringBlock =
          (OMBlockPrefetchClient.ExpiringAllocatedBlock) expiringBlockConstructor.newInstance(block, expiryTime);
      queue.offer(expiringBlock);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create ExpiringAllocatedBlock via reflection. Constructor: "
          + expiringBlockConstructor + ", Args: [" + block + ", " + expiryTime + "]", e);
    }
  }

  private List<AllocatedBlock> getBlocksFromQueue(Queue<ExpiringAllocatedBlock> queue) {
    return queue.stream()
        .map(ExpiringAllocatedBlock::getBlock)
        .collect(Collectors.toList());
  }

  private void assertNodesSorted(List<DatanodeDetails> sortedNodes, List<DatanodeDetails> originalNodes) {
    assertNotNull(sortedNodes, "Sorted node list should not be null");
    assertEquals(originalNodes.size(), sortedNodes.size(), "Sorted list size should match original");

    List<DatanodeDetails> expectedOrder = new ArrayList<>(originalNodes);
    expectedOrder.sort(Comparator.<DatanodeDetails, Integer>comparing(dn -> {
      if (dn.getNetworkLocation() == null) {
        return 2;
      }
      return CLIENT_RACK.equals(dn.getNetworkLocation()) ? 0 : 1;
    }).thenComparing(DatanodeDetails::getHostName));

    List<String> actualHostnames = sortedNodes.stream().map(DatanodeDetails::getHostName).collect(Collectors.toList());
    List<String> expectedHostnames =
        expectedOrder.stream().map(DatanodeDetails::getHostName).collect(Collectors.toList());

    assertEquals(expectedHostnames, actualHostnames, "Nodes are not sorted as expected by mock topology");
  }

  @Test
  void testGetBlocksFullHitCheckSort() throws IOException {
    int initialCacheSize = 1;
    int numBlocksToRequest = 1;

    List<DatanodeDetails> unsortedNodes = Arrays.asList(datanodes.get(4), datanodes.get(1), datanodes.get(0));
    AllocatedBlock cachedBlock = createMockAllocatedBlock(300, 0, unsortedNodes);
    Queue<ExpiringAllocatedBlock> queue = getInternalQueue(REP_CONFIG);
    long expiry = System.currentTimeMillis() + EXPIRY_INTERVAL_MS;
    addToQueue(queue, cachedBlock, expiry);
    assertEquals(initialCacheSize, queue.size());

    List<AllocatedBlock> resultBlocks = omBlockPrefetchClient.getBlocks(
        BLOCK_SIZE, numBlocksToRequest, REP_CONFIG, SERVICE_ID,
        new ExcludeList(), null, networkTopology);

    verify(scmBlockLocationProtocol, never()).allocateBlock(anyLong(), anyInt(), any(), anyString(), any(),
        anyString());
    assertEquals(numBlocksToRequest, resultBlocks.size());
    AllocatedBlock returnedBlock = resultBlocks.get(0);
    assertEquals(cachedBlock.getBlockID(), returnedBlock.getBlockID());
    assertTrue(queue.isEmpty(), "Queue should be empty after taking the block");
  }

  @Test
  void testBoundaryRequestingZeroBlocks() throws IOException {
    int numBlocksToRequest = 0;
    List<AllocatedBlock> resultBlocks = omBlockPrefetchClient.getBlocks(
        BLOCK_SIZE, numBlocksToRequest, REP_CONFIG, SERVICE_ID, new ExcludeList(), null, networkTopology);

    assertTrue(resultBlocks.isEmpty());
    verify(scmBlockLocationProtocol, never()).allocateBlock(anyLong(), anyInt(), any(), anyString(), any(),
        anyString());
  }

  @Test
  void testBoundaryCacheFullNoPrefetch() throws IOException {
    int initialCacheSize = MAX_BLOCKS_PER_QUEUE;
    int numBlocksToRequest = 1;

    Queue<ExpiringAllocatedBlock> queue = getInternalQueue(REP_CONFIG);
    List<DatanodeDetails> defaultNodes = datanodes.subList(0, 3);
    List<AllocatedBlock> initialCachedBlocks = IntStream.range(0, initialCacheSize)
        .mapToObj(i -> createMockAllocatedBlock(500, i, defaultNodes))
        .collect(Collectors.toList());
    long expiry = System.currentTimeMillis() + EXPIRY_INTERVAL_MS;
    for (AllocatedBlock block : initialCachedBlocks) {
      addToQueue(queue, block, expiry);
    }
    assertEquals(initialCacheSize, queue.size());

    List<AllocatedBlock> resultBlocks = omBlockPrefetchClient.getBlocks(
        BLOCK_SIZE, numBlocksToRequest, REP_CONFIG, SERVICE_ID, new ExcludeList(), null, networkTopology);

    verify(scmBlockLocationProtocol, never()).allocateBlock(anyLong(), anyInt(), any(), anyString(), any(),
        anyString());
    assertEquals(numBlocksToRequest, resultBlocks.size());
    assertEquals(initialCachedBlocks.get(0).getBlockID(), resultBlocks.get(0).getBlockID());
    assertNodesSorted(resultBlocks.get(0).getPipeline().getNodesInOrder(), defaultNodes);
    assertEquals(initialCacheSize - numBlocksToRequest, queue.size());
    List<AllocatedBlock> actualRemaining = getBlocksFromQueue(queue);
    assertEquals(initialCachedBlocks.subList(1, initialCacheSize).stream().map(AllocatedBlock::getBlockID)
            .collect(Collectors.toList()),
        actualRemaining.stream().map(AllocatedBlock::getBlockID).collect(Collectors.toList()));
    actualRemaining.forEach(b -> assertEquals(defaultNodes, b.getPipeline().getNodesInOrder()));
  }

  @Test
  void testExpiredBlockIsSkipped() throws IOException, InterruptedException {
    int numBlocksToRequest = 1;
    Queue<ExpiringAllocatedBlock> queue = getInternalQueue(REP_CONFIG);
    List<DatanodeDetails> defaultNodes = datanodes.subList(0, 3);
    AllocatedBlock expiredBlock = createMockAllocatedBlock(700, 0, defaultNodes);
    AllocatedBlock validBlock = createMockAllocatedBlock(700, 1, defaultNodes);
    long expiredTime = System.currentTimeMillis() - 1000;
    long validTime = System.currentTimeMillis() + EXPIRY_INTERVAL_MS;
    addToQueue(queue, expiredBlock, expiredTime);
    addToQueue(queue, validBlock, validTime);
    assertEquals(2, queue.size());

    List<AllocatedBlock> resultBlocks = omBlockPrefetchClient.getBlocks(
        BLOCK_SIZE, numBlocksToRequest, REP_CONFIG, SERVICE_ID, new ExcludeList(), null, networkTopology);

    verify(scmBlockLocationProtocol, never()).allocateBlock(anyLong(), anyInt(), any(), anyString(), any(),
        anyString());
    assertEquals(numBlocksToRequest, resultBlocks.size());
    assertEquals(validBlock.getBlockID(), resultBlocks.get(0).getBlockID());
    assertNodesSorted(resultBlocks.get(0).getPipeline().getNodesInOrder(), defaultNodes);
    assertTrue(queue.isEmpty());
  }

  @Test
  void testGetBlocksWithExcludeListSkipsCache() throws IOException {
    int initialCacheSize = 3;
    int numBlocksToRequest = 2;

    Queue<ExpiringAllocatedBlock> queue = getInternalQueue(REP_CONFIG);
    List<DatanodeDetails> defaultNodes = datanodes.subList(0, 3);
    List<AllocatedBlock> initialCachedBlocks = IntStream.range(0, initialCacheSize)
        .mapToObj(i -> createMockAllocatedBlock(800, i, defaultNodes))
        .collect(Collectors.toList());
    long expiry = System.currentTimeMillis() + EXPIRY_INTERVAL_MS;
    for (AllocatedBlock block : initialCachedBlocks) {
      addToQueue(queue, block, expiry);
    }
    assertEquals(initialCacheSize, queue.size());

    ExcludeList excludeList = new ExcludeList();
    excludeList.addDatanode(createDatanodeDetails());

    List<DatanodeDetails> scmNodes = datanodes.subList(1, 4);
    List<AllocatedBlock> scmBlocks = IntStream.range(10, 10 + numBlocksToRequest)
        .mapToObj(i -> createMockAllocatedBlock(800, i, scmNodes))
        .collect(Collectors.toList());
    when(scmBlockLocationProtocol.allocateBlock(
        eq(BLOCK_SIZE), eq(numBlocksToRequest), eq(REP_CONFIG), eq(SERVICE_ID), eq(excludeList), eq(null)))
        .thenReturn(scmBlocks);

    List<AllocatedBlock> resultBlocks = omBlockPrefetchClient.getBlocks(
        BLOCK_SIZE, numBlocksToRequest, REP_CONFIG, SERVICE_ID, excludeList, null, networkTopology);

    verify(scmBlockLocationProtocol, times(1)).allocateBlock(
        eq(BLOCK_SIZE), eq(numBlocksToRequest), eq(REP_CONFIG), eq(SERVICE_ID), eq(excludeList), eq(null));
    assertEquals(numBlocksToRequest, resultBlocks.size());
    resultBlocks.forEach(b -> assertNodesSorted(b.getPipeline().getNodesInOrder(), scmNodes));
    assertEquals(initialCacheSize, queue.size());
    List<AllocatedBlock> actualCached = getBlocksFromQueue(queue);
    assertEquals(initialCachedBlocks.stream().map(AllocatedBlock::getBlockID).collect(Collectors.toList()),
        actualCached.stream().map(AllocatedBlock::getBlockID).collect(Collectors.toList()));
    assertTrue(Collections.disjoint(resultBlocks.stream().map(AllocatedBlock::getBlockID).collect(Collectors.toSet()),
        actualCached.stream().map(AllocatedBlock::getBlockID).collect(Collectors.toSet())));
  }

  @Test
  void testGetBlocksWithUntestedReplicationConfigSkipsCache() throws IOException {
    int numBlocksToRequest = 2;
    String clientMachineName = "test-client-machine";
    ExcludeList excludeList = new ExcludeList();
    List<DatanodeDetails> scmPipelineNodes = datanodes.subList(0, 3);
    List<AllocatedBlock> expectedScmBlocks = new ArrayList<>();
    for (int i = 0; i < numBlocksToRequest; i++) {
      Pipeline pipeline = Pipeline.newBuilder()
          .setId(PipelineID.randomId())
          .setReplicationConfig(UNTESTED_REP_CONFIG)
          .setNodes(scmPipelineNodes)
          .setLeaderId(scmPipelineNodes.get(0).getID())
          .setState(Pipeline.PipelineState.OPEN)
          .build();
      expectedScmBlocks.add(
          new AllocatedBlock.Builder()
              .setContainerBlockID(new ContainerBlockID(1000 + i, i))
              .setPipeline(pipeline)
              .build());
    }

    when(scmBlockLocationProtocol.allocateBlock(
        eq(BLOCK_SIZE),
        eq(numBlocksToRequest),
        eq(UNTESTED_REP_CONFIG),
        eq(SERVICE_ID),
        eq(excludeList),
        eq(clientMachineName)))
        .thenReturn(expectedScmBlocks);

    List<AllocatedBlock> actualBlocks = omBlockPrefetchClient.getBlocks(
        BLOCK_SIZE,
        numBlocksToRequest,
        UNTESTED_REP_CONFIG,
        SERVICE_ID,
        excludeList,
        clientMachineName,
        networkTopology);

    verify(scmBlockLocationProtocol, times(1)).allocateBlock(
        eq(BLOCK_SIZE),
        eq(numBlocksToRequest),
        eq(UNTESTED_REP_CONFIG),
        eq(SERVICE_ID),
        eq(excludeList),
        eq(clientMachineName));

    assertEquals(numBlocksToRequest, actualBlocks.size(), "Number of returned blocks mismatch.");
    assertEquals(expectedScmBlocks, actualBlocks, "Returned blocks do not match expected SCM blocks.");
    actualBlocks.forEach(b -> assertNodesSorted(b.getPipeline().getNodesInOrder(), scmPipelineNodes));

    // Ensure no blocks were unexpectedly added to any tested config's queue.
    Queue<ExpiringAllocatedBlock> testedConfigQueue = getInternalQueue(REP_CONFIG);
    assertTrue(testedConfigQueue.isEmpty(), "Cache for a tested config should remain empty if not used.");
  }
}
