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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test ssd storage aware pipeline placement policy.
 * Set of DataNodes:
 *
 *  DataNode1
 *       Data Storage Volumes (remaining space):
 *          SSD: 200 bytes
 *          DISK: 400 bytes
 *       Metadata Storage (remaining space): 50 bytes
 *  DataNode2
 *       Data Storage Volumes (remaining space):
 *          DISK: 300 bytes
 *          DISK: 500 bytes
 *       Metadata Storage (remaining space): 100 bytes
 *  DataNode3
 *       Data Storage Volumes (remaining space):
 *          SSD: 140 bytes
 *          SSD: 200 bytes
 *       Metadata Storage (remaining space): 10 bytes
 *  DataNode4
 *       Data Storage Volumes (remaining space):
 *          DISK: 180 bytes
 *          SSD: 240 bytes
 *       Metadata Storage (remaining space): 60 bytes
 *  DataNode5
 *       Data Storage Volumes (remaining space):
 *          DISK: 30 bytes
 *          SSD: 70 bytes
 *       Metadata Storage (remaining space): 100 bytes
 *  There are 3 non-CLOSED pipelines
 *  DataNode1 not engaged anywhere
 *  DataNode2 engaged in: pipeline1, pipeline2
 *  DataNode3 engaged in: pipeline1, pipeline2 and pipeline3
 *  DataNode4 engaged in: pipeline1, pipeline3
 *  DataNode5 engaged in: pipeline2, pipeline3
 */
public class TestSsdStorageAwarePipelinePlacementPolicy {

  private static final NodeStatus HEALTHY_IN_SERVICE_NODE = new NodeStatus(
      HddsProtos.NodeOperationalState.IN_SERVICE,
      HddsProtos.NodeState.HEALTHY);
  private static final String RACK = "RACK";

  private static final String DATANODES_COUNT_IS_NOT_ENOUGH_EXCEPTION_MESSAGE =
      "No healthy node found to allocate container.";

  private static NodeManager nodeManager = mock(NodeManager.class);

  private static PipelineStateManager pipelineStateManager =
      mock(PipelineStateManager.class);

  private static ConfigurationSource configurationSource =
      mock(ConfigurationSource.class);

  private static PipelinePlacementPolicy pipelinePlacementPolicy;


  private static DatanodeDetails dn1;
  private static DatanodeDetails dn2;
  private static DatanodeDetails dn3;
  private static DatanodeDetails dn4;
  private static DatanodeDetails dn5;

  @BeforeAll
  public static void setup() throws PipelineNotFoundException {
    when(configurationSource.get(
        eq(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT))).thenReturn("3");
    pipelinePlacementPolicy =
        new SsdStorageAwarePipelinePlacementPolicy(nodeManager,
            pipelineStateManager, configurationSource);

    dn1 = mockDatanode(
        StorageContainerDatanodeProtocolProtos.StorageTypeProto.SSD, 200L,
        StorageContainerDatanodeProtocolProtos.StorageTypeProto.DISK, 400L,
        50L);
    dn2 = mockDatanode(
        StorageContainerDatanodeProtocolProtos.StorageTypeProto.DISK, 300L,
        StorageContainerDatanodeProtocolProtos.StorageTypeProto.DISK, 500L,
        100L);
    dn3 = mockDatanode(
        StorageContainerDatanodeProtocolProtos.StorageTypeProto.SSD, 140L,
        StorageContainerDatanodeProtocolProtos.StorageTypeProto.SSD, 200L,
        10L);
    dn4 = mockDatanode(
        StorageContainerDatanodeProtocolProtos.StorageTypeProto.DISK, 180L,
        StorageContainerDatanodeProtocolProtos.StorageTypeProto.SSD, 240L,
        60L);
    dn5 = mockDatanode(
        StorageContainerDatanodeProtocolProtos.StorageTypeProto.DISK, 30L,
        StorageContainerDatanodeProtocolProtos.StorageTypeProto.SSD, 70L,
        100L);

    ReplicationConfig replConfig = RatisReplicationConfig
        .getInstance(HddsProtos.ReplicationFactor.THREE);

    PipelineID pipeline1Id = mockPipelineId(replConfig);
    PipelineID pipeline2Id = mockPipelineId(replConfig);
    PipelineID pipeline3Id = mockPipelineId(replConfig);

    datanodeEngagement(dn2, pipeline1Id, pipeline2Id);
    datanodeEngagement(dn3, pipeline1Id, pipeline2Id, pipeline3Id);
    datanodeEngagement(dn4, pipeline1Id, pipeline3Id);
    datanodeEngagement(dn5, pipeline2Id, pipeline3Id);

    when(nodeManager.pipelineLimit(any(DatanodeDetails.class))).thenReturn(4);
  }

  @Test
  public void testPipelineWithSsdStorages() throws SCMException {
    // given
    when(nodeManager.getNodes(HEALTHY_IN_SERVICE_NODE)).thenReturn(
            Arrays.asList(dn1, dn2, dn3, dn4, dn5));

    // when
    List<DatanodeDetails> datanodeDetails = pipelinePlacementPolicy
        .filterViableNodes(Collections.emptyList(),
            Collections.unmodifiableList(new ArrayList<>()), 3, 10, 60);

    // then
    assertTrue(datanodeDetails.stream().allMatch(dn ->
        ((DatanodeInfo)dn).getStorageReports().stream().anyMatch(
            report -> report.getStorageType()
                .equals(StorageContainerDatanodeProtocolProtos
                    .StorageTypeProto.SSD))));
    assertArrayEquals(new DatanodeDetails[]{dn1, dn4, dn5},
        datanodeDetails.toArray());
  }

  @Test
  public void testExceptionOnNonSufficientCountOFDatanodesDueToUsedOnes() {
    when(nodeManager.getNodes(HEALTHY_IN_SERVICE_NODE))
        .thenReturn(Collections.emptyList());

    checkFailureOnNonSufficientCountOfDatanodes(Collections.emptyList(),
        Collections.singletonList(dn5), 3, 5, 120,
        DATANODES_COUNT_IS_NOT_ENOUGH_EXCEPTION_MESSAGE,
        SCMException.ResultCodes.FAILED_TO_FIND_HEALTHY_NODES);
  }

  @Test
  public void testExceptionOnEmptyListOfHealthyNodes() {
    when(nodeManager.getNodes(HEALTHY_IN_SERVICE_NODE))
        .thenReturn(Collections.emptyList());

    checkFailureOnNonSufficientCountOfDatanodes(Collections.emptyList(),
        Collections.unmodifiableList(new ArrayList<>()), 3, 5, 120,
        DATANODES_COUNT_IS_NOT_ENOUGH_EXCEPTION_MESSAGE,
        SCMException.ResultCodes.FAILED_TO_FIND_HEALTHY_NODES);
  }

  @Test
  public void testExceptionOnListOfDISKAwareStorages() {
    when(nodeManager.getNodes(HEALTHY_IN_SERVICE_NODE))
        .thenReturn(Collections.singletonList(dn2));
    String expectedExceptionMessage = "Unable to find enough nodes that meet "
        + "the space requirement of 20 bytes for metadata and 100 bytes for "
        + "data in healthy node set. Required 3. Found 0.";

    checkFailureOnNonSufficientCountOfDatanodes(Collections.emptyList(),
        Collections.unmodifiableList(new ArrayList<>()), 3, 20, 100,
        expectedExceptionMessage,
        SCMException.ResultCodes.FAILED_TO_FIND_NODES_WITH_SPACE);
  }

  @Test
  public void testNodesCountIsNotEnoughDueToExcludedNodes() {
    when(nodeManager.getNodes(HEALTHY_IN_SERVICE_NODE)).thenReturn(
        Arrays.asList(dn1, dn2, dn3, dn4, dn5));
    String expectedExceptionMessage =
        "Pipeline creation failed due to no sufficient healthy datanodes. "
            + "Required 3. Found 2. Excluded 1.";

    checkFailureOnNonSufficientCountOfDatanodes(Collections.singletonList(dn4),
        Collections.unmodifiableList(new ArrayList<>()), 3, 10, 60,
        expectedExceptionMessage,
        SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
  }

  @Test
  public void testNodesCountIsNotEnoughDueToViableNodesEngagement() {
    when(nodeManager.getNodes(HEALTHY_IN_SERVICE_NODE)).thenReturn(
        Arrays.asList(dn1, dn2, dn3, dn4, dn5));
    when(nodeManager.pipelineLimit(any(DatanodeDetails.class))).thenReturn(2);
    String expectedExceptionMessage = "Pipeline creation failed because nodes "
        + "are engaged in other pipelines and every node can only be engaged in"
        + " max 3 pipelines. Required 3. Found 1. Excluded: 0.";

    checkFailureOnNonSufficientCountOfDatanodes(Collections.emptyList(),
        Collections.unmodifiableList(new ArrayList<>()), 3, 10, 60,
        expectedExceptionMessage,
        SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);

    // reset pipelines count per node to 4
    when(nodeManager.pipelineLimit(any(DatanodeDetails.class))).thenReturn(4);
  }

  @Test
  public void testNodesCountIsNotEnoughDueToViableNodesSpaceLimit() {
    when(nodeManager.getNodes(HEALTHY_IN_SERVICE_NODE)).thenReturn(
        Arrays.asList(dn1, dn2, dn3, dn4, dn5));
    String expectedExceptionMessage = "Unable to find enough nodes that meet "
        + "the space requirement of 1000 bytes for metadata and 80 bytes for "
        + "data in healthy node set. Required 3. Found 0.";

    checkFailureOnNonSufficientCountOfDatanodes(Collections.emptyList(),
        Collections.unmodifiableList(new ArrayList<>()), 3, 1000, 80,
        expectedExceptionMessage,
        SCMException.ResultCodes.FAILED_TO_FIND_NODES_WITH_SPACE);

  }

  @Test
  public void testExceptionOnDataNodesPlacedInMultipleRacks() {
    NetworkTopology networkTopology = mock(NetworkTopology.class);
    when(networkTopology.getNumOfNodes(anyInt())).thenReturn(2);
    when(nodeManager.getClusterNetworkTopologyMap())
        .thenReturn(networkTopology);
    when(dn1.getNetworkLocation()).thenReturn("ANOTHER_RACK").thenReturn(RACK);
    when(nodeManager.getNodes(HEALTHY_IN_SERVICE_NODE)).thenReturn(
        Arrays.asList(dn1, dn2, dn3, dn4, dn5));

    checkFailureOnNonSufficientCountOfDatanodes(Collections.emptyList(),
        Collections.emptyList(), 3, 10, 60,
        PipelinePlacementPolicy.MULTIPLE_RACK_PIPELINE_MSG,
        SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
  }


  private static void datanodeEngagement(DatanodeDetails dn,
                                         PipelineID... pipelineIds) {
    when(nodeManager.getPipelines(eq(dn)))
        .thenReturn(new HashSet<>(Arrays.asList(pipelineIds)));
  }

  @NotNull
  private static PipelineID mockPipelineId(ReplicationConfig replConfig)
      throws PipelineNotFoundException {
    UUID pipelineUuid = UUID.randomUUID();
    PipelineID pipelineId = PipelineID.valueOf(pipelineUuid);
    Pipeline pipeline = mock(Pipeline.class);
    when(pipeline.getReplicationConfig()).thenReturn(replConfig);
    when(pipeline.isClosed()).thenReturn(false);
    when(pipeline.getId()).thenReturn(pipelineId);
    when(pipelineStateManager.getPipeline(eq(pipelineId)))
        .thenReturn(pipeline);
    return pipelineId;
  }

  private static DatanodeDetails mockDatanode(
      StorageContainerDatanodeProtocolProtos.StorageTypeProto vol1Type,
      long vol1RemainingSpace,
      StorageContainerDatanodeProtocolProtos.StorageTypeProto vol2Type,
      long vol2RemainingSpace, long metadataVolRemainingSpace) {
    DatanodeDetails dn = mock(DatanodeInfo.class);
    when(dn.getNetworkLocation()).thenReturn(RACK);
    StorageContainerDatanodeProtocolProtos.StorageReportProto dnStorage1 =
        mock(StorageContainerDatanodeProtocolProtos.StorageReportProto.class);
    when(dnStorage1.getStorageType()).thenReturn(
        vol1Type);
    when(dnStorage1.getRemaining()).thenReturn(vol1RemainingSpace);
    StorageContainerDatanodeProtocolProtos.StorageReportProto dnStorage2 =
        mock(StorageContainerDatanodeProtocolProtos.StorageReportProto.class);
    when(dnStorage2.getStorageType()).thenReturn(
        vol2Type);
    when(dnStorage2.getRemaining()).thenReturn(vol2RemainingSpace);
    when(((DatanodeInfo) dn).getStorageReports()).thenReturn(Arrays.asList(
        dnStorage1, dnStorage2));
    StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto
        dn1MetadataStorageReportProto = mock(
        StorageContainerDatanodeProtocolProtos
            .MetadataStorageReportProto.class);
    when(dn1MetadataStorageReportProto.getRemaining())
        .thenReturn(metadataVolRemainingSpace);
    when(((DatanodeInfo) dn).getMetadataStorageReports())
        .thenReturn(Arrays.asList(dn1MetadataStorageReportProto));
    return dn;
  }

  private void checkFailureOnNonSufficientCountOfDatanodes(
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> datanodes,
      int nodesRequired, int metadataSizeRequired, int dataSizeRequired,
      String expectedExceptionMessage,
      SCMException.ResultCodes failedToFindHealthyNodes) {
    SCMException scmException = assertThrows(SCMException.class,
        () -> pipelinePlacementPolicy.filterViableNodes(excludedNodes,
            datanodes, nodesRequired, metadataSizeRequired, dataSizeRequired),
        expectedExceptionMessage);

    assertEquals(expectedExceptionMessage, scmException.getMessage());
    assertEquals(failedToFindHealthyNodes,
        scmException.getResult());
  }


}
