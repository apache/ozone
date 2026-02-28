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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.junit.jupiter.api.Test;

/**
 * Tests for PipelineStorageTypeFilter.
 */
class TestPipelineStorageTypeFilter {

  @Test
  void testNullStorageTypeReturnsAllPipelines() {
    NodeManager nodeManager = mock(NodeManager.class);
    List<Pipeline> pipelines = Arrays.asList(
        MockPipeline.createRatisPipeline(),
        MockPipeline.createRatisPipeline());
    List<Pipeline> result = PipelineStorageTypeFilter.filter(
        pipelines, nodeManager, null);
    assertEquals(2, result.size());
  }

  @Test
  void testFilterRetainsPipelinesWithMatchingNodes() {
    NodeManager nodeManager = mock(NodeManager.class);

    // Create nodes with SSD storage
    DatanodeDetails ssdNode1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails ssdNode2 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails ssdNode3 = MockDatanodeDetails.randomDatanodeDetails();

    // Create nodes with DISK storage only
    DatanodeDetails diskNode1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails diskNode2 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails diskNode3 = MockDatanodeDetails.randomDatanodeDetails();

    List<DatanodeDetails> allNodes = Arrays.asList(
        ssdNode1, ssdNode2, ssdNode3, diskNode1, diskNode2, diskNode3);
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenReturn(allNodes);

    // Configure SSD nodes to report SSD storage
    for (DatanodeDetails ssdNode : Arrays.asList(ssdNode1, ssdNode2,
        ssdNode3)) {
      DatanodeInfo ssdInfo = mockDatanodeInfo(ssdNode,
          StorageTypeProto.SSD);
      when(nodeManager.getDatanodeInfo(ssdNode)).thenReturn(ssdInfo);
    }

    // Configure DISK nodes to report DISK storage
    for (DatanodeDetails diskNode : Arrays.asList(diskNode1, diskNode2,
        diskNode3)) {
      DatanodeInfo diskInfo = mockDatanodeInfo(diskNode,
          StorageTypeProto.DISK);
      when(nodeManager.getDatanodeInfo(diskNode)).thenReturn(diskInfo);
    }

    // Pipeline with all SSD nodes
    Pipeline ssdPipeline = createPipelineWithNodes(
        Arrays.asList(ssdNode1, ssdNode2, ssdNode3));

    // Pipeline with all DISK nodes
    Pipeline diskPipeline = createPipelineWithNodes(
        Arrays.asList(diskNode1, diskNode2, diskNode3));

    // Mixed pipeline
    Pipeline mixedPipeline = createPipelineWithNodes(
        Arrays.asList(ssdNode1, diskNode1, ssdNode2));

    List<Pipeline> pipelines = new ArrayList<>(
        Arrays.asList(ssdPipeline, diskPipeline, mixedPipeline));

    // Filter for SSD — only the all-SSD pipeline should remain
    List<Pipeline> ssdResult = PipelineStorageTypeFilter.filter(
        pipelines, nodeManager, StorageType.SSD);
    assertEquals(1, ssdResult.size());
    assertEquals(ssdPipeline.getId(), ssdResult.get(0).getId());

    // Filter for DISK — only the all-DISK pipeline should remain
    List<Pipeline> diskResult = PipelineStorageTypeFilter.filter(
        pipelines, nodeManager, StorageType.DISK);
    assertEquals(1, diskResult.size());
    assertEquals(diskPipeline.getId(), diskResult.get(0).getId());
  }

  @Test
  void testFilterReturnsEmptyWhenNoMatch() {
    NodeManager nodeManager = mock(NodeManager.class);

    DatanodeDetails diskNode1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails diskNode2 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails diskNode3 = MockDatanodeDetails.randomDatanodeDetails();

    when(nodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenReturn(Arrays.asList(diskNode1, diskNode2, diskNode3));

    for (DatanodeDetails dn : Arrays.asList(diskNode1, diskNode2,
        diskNode3)) {
      DatanodeInfo info = mockDatanodeInfo(dn, StorageTypeProto.DISK);
      when(nodeManager.getDatanodeInfo(dn)).thenReturn(info);
    }

    Pipeline pipeline = createPipelineWithNodes(
        Arrays.asList(diskNode1, diskNode2, diskNode3));
    List<Pipeline> pipelines = new ArrayList<>(
        Collections.singletonList(pipeline));

    // Filter for SSD — no pipeline should match
    List<Pipeline> result = PipelineStorageTypeFilter.filter(
        pipelines, nodeManager, StorageType.SSD);
    assertTrue(result.isEmpty());
  }

  @Test
  void testGetNodesWithStorageType() {
    NodeManager nodeManager = mock(NodeManager.class);

    DatanodeDetails ssdNode = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails diskNode = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails bothNode = MockDatanodeDetails.randomDatanodeDetails();

    when(nodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenReturn(Arrays.asList(ssdNode, diskNode, bothNode));

    // Create mock DatanodeInfo objects first, then stub nodeManager
    DatanodeInfo ssdInfo = mockDatanodeInfo(ssdNode, StorageTypeProto.SSD);
    DatanodeInfo diskInfo = mockDatanodeInfo(diskNode, StorageTypeProto.DISK);
    DatanodeInfo bothInfo = mock(DatanodeInfo.class);
    when(bothInfo.getStorageReports()).thenReturn(Arrays.asList(
        createStorageReport(StorageTypeProto.SSD),
        createStorageReport(StorageTypeProto.DISK)));

    when(nodeManager.getDatanodeInfo(ssdNode)).thenReturn(ssdInfo);
    when(nodeManager.getDatanodeInfo(diskNode)).thenReturn(diskInfo);
    when(nodeManager.getDatanodeInfo(bothNode)).thenReturn(bothInfo);

    Set<UUID> ssdNodes = PipelineStorageTypeFilter
        .getNodesWithStorageType(nodeManager, StorageType.SSD);
    assertEquals(2, ssdNodes.size());
    assertTrue(ssdNodes.contains(ssdNode.getUuid()));
    assertTrue(ssdNodes.contains(bothNode.getUuid()));

    Set<UUID> diskNodes = PipelineStorageTypeFilter
        .getNodesWithStorageType(nodeManager, StorageType.DISK);
    assertEquals(2, diskNodes.size());
    assertTrue(diskNodes.contains(diskNode.getUuid()));
    assertTrue(diskNodes.contains(bothNode.getUuid()));
  }

  @Test
  void testNodeWithNullDatanodeInfoIsSkipped() {
    NodeManager nodeManager = mock(NodeManager.class);

    DatanodeDetails node = MockDatanodeDetails.randomDatanodeDetails();
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenReturn(Collections.singletonList(node));
    when(nodeManager.getDatanodeInfo(node)).thenReturn(null);

    Set<UUID> result = PipelineStorageTypeFilter
        .getNodesWithStorageType(nodeManager, StorageType.SSD);
    assertTrue(result.isEmpty());
  }

  private static DatanodeInfo mockDatanodeInfo(DatanodeDetails dn,
      StorageTypeProto storageType) {
    DatanodeInfo info = mock(DatanodeInfo.class);
    when(info.getStorageReports()).thenReturn(
        Collections.singletonList(createStorageReport(storageType)));
    return info;
  }

  private static StorageReportProto createStorageReport(
      StorageTypeProto storageType) {
    return StorageReportProto.newBuilder()
        .setStorageUuid("uuid-" + UUID.randomUUID())
        .setStorageLocation("/data")
        .setCapacity(100L * 1024 * 1024 * 1024)
        .setScmUsed(10L * 1024 * 1024 * 1024)
        .setRemaining(90L * 1024 * 1024 * 1024)
        .setStorageType(storageType)
        .build();
  }

  private static Pipeline createPipelineWithNodes(
      List<DatanodeDetails> nodes) {
    return Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            org.apache.hadoop.hdds.client.RatisReplicationConfig.getInstance(
                org.apache.hadoop.hdds.protocol.proto.HddsProtos
                    .ReplicationFactor.THREE))
        .setNodes(nodes)
        .build();
  }
}
