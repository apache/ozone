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

package org.apache.hadoop.hdds.scm.protocol;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateScmBlockRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateScmBlockResponseProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link ScmBlockLocationProtocolServerSideTranslatorPB} forwards the
 * pipeline-wide minimum {@code currentVersion} to every pipeline member it
 * returns on block allocation.
 * <p>
 * Pipeline members are frozen copies (rebuilt from the replicated pipeline
 * proto), so the translator must source each member's current version from the
 * live node registry, which the heartbeat handler keeps up to date. These tests
 * pin the pipeline copies at {@link HDDSVersion#DEFAULT_VERSION} and register a
 * different, higher version per node so a translator that read the stale
 * pipeline copy would compute the wrong minimum.
 */
class TestScmBlockLocationProtocolServerSideTranslatorPB {

  private ScmBlockLocationProtocol impl;
  private ScmBlockLocationProtocolServerSideTranslatorPB service;
  private Map<DatanodeID, DatanodeInfo> registry;
  private List<DatanodeDetails> nodes;
  private NodeManager nodeManager;

  @BeforeEach
  void setUp() throws Exception {
    impl = mock(ScmBlockLocationProtocol.class);
    StorageContainerManager scm = mock(StorageContainerManager.class);

    registry = new HashMap<>();
    nodeManager = mock(NodeManager.class);
    when(nodeManager.getNode(any(DatanodeID.class))).thenAnswer(inv -> registry.get(inv.getArgument(0)));
    when(scm.getScmNodeManager()).thenReturn(nodeManager);

    // Pipeline copies stay at DEFAULT_VERSION; the live version lives in the registry.
    nodes = registerNodes(3, HDDSVersion.SOFTWARE_VERSION);

    Pipeline pipeline = buildPipeline(nodes);
    AllocatedBlock block = blockInPipeline(1L, pipeline);

    when(impl.allocateBlock(anyLong(), anyInt(), any(ReplicationConfig.class),
        anyString(), any(), anyString())).thenReturn(Collections.singletonList(block));

    service = new ScmBlockLocationProtocolServerSideTranslatorPB(impl, scm, mock(ProtocolMessageMetrics.class));
  }

  /**
   * Creates {@code count} pipeline members (left at {@link HDDSVersion#DEFAULT_VERSION}) and registers each one in the
   * mocked node manager with the given live {@code currentVersion}.
   */
  private List<DatanodeDetails> registerNodes(int count, HDDSVersion liveVersion) {
    List<DatanodeDetails> created = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      DatanodeDetails dn = randomDatanodeDetails();
      created.add(dn);
      setLiveVersion(dn, liveVersion);
    }
    return created;
  }

  /**
   * Sets the version the node registry reports for the given pipeline member, mirroring a heartbeat update on SCM.
   */
  private void setLiveVersion(DatanodeDetails member, HDDSVersion version) {
    DatanodeInfo info = new DatanodeInfo(member, NodeStatus.inServiceHealthy(),
        UpgradeUtils.defaultVersionProto(), HddsTestUtils.ROLL_INTERVAL_MS_DEFAULT);
    info.setCurrentVersion(version);
    registry.put(member.getID(), info);
  }

  private AllocateScmBlockResponseProto allocate(int numBlocks) throws Exception {
    AllocateScmBlockRequestProto request = AllocateScmBlockRequestProto.newBuilder()
        .setSize(1024)
        .setNumBlocks(numBlocks)
        .setType(ReplicationType.RATIS)
        .setFactor(ReplicationFactor.THREE)
        .setOwner("owner")
        .build();
    return service.allocateScmBlock(request, ClientVersion.CURRENT);
  }

  private Pipeline buildPipeline(List<DatanodeDetails> pipelineNodes) {
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.OPEN)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setNodes(pipelineNodes)
        .build();
  }

  private AllocatedBlock blockInPipeline(long localId, Pipeline pipeline) {
    return new AllocatedBlock.Builder()
        .setContainerBlockID(new ContainerBlockID(1L, localId))
        .setPipeline(pipeline)
        .build();
  }

  private void setAllocatedBlocks(List<AllocatedBlock> blocks) throws Exception {
    when(impl.allocateBlock(anyLong(), anyInt(), any(ReplicationConfig.class),
        anyString(), any(), anyString())).thenReturn(blocks);
  }

  private void assertAllMembersHaveVersion(int expected,
      ScmBlockLocationProtocolProtos.AllocateBlockResponse response) {
    List<DatanodeDetailsProto> members = response.getPipeline().getMembersList();
    assertEquals(nodes.size(), members.size());
    for (DatanodeDetailsProto member : members) {
      assertEquals(expected, member.getCurrentVersion());
    }
  }

  @Test
  public void testMinimumVersionAcrossPipeline() throws Exception {
    // The pipeline mixes datanodes at different reported versions (as during a
    // rolling upgrade). The lowest is forwarded to every member so clients do
    // not enable a feature an non-upgraded datanode cannot handle.
    setLiveVersion(nodes.get(1), HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC);

    assertAllMembersHaveVersion(HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC.serialize(),
        allocate(1).getBlocks(0));
  }

  @Test
  public void testUniformPipelineForwardsThatVersion() throws Exception {
    assertAllMembersHaveVersion(HDDSVersion.SOFTWARE_VERSION.serialize(), allocate(1).getBlocks(0));
  }

  @Test
  public void testEmptyPipelineFailsAllocation() throws Exception {
    setAllocatedBlocks(Collections.singletonList(blockInPipeline(1L, buildPipeline(Collections.emptyList()))));

    SCMException e = assertThrows(SCMException.class, () -> allocate(1));
    assertEquals(SCMException.ResultCodes.FAILED_TO_FIND_ACTIVE_PIPELINE, e.getResult());
  }

  @Test
  public void testBlocksSharingPipelineAllGetMinVersion() throws Exception {
    // One old datanode should determine the shared pipeline's minimum version.
    setLiveVersion(nodes.get(1), HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC);

    // Three blocks allocated on the same pipeline object; the memoized proto
    // must be returned for each block with the minimum version intact.
    Pipeline pipeline = buildPipeline(nodes);
    setAllocatedBlocks(Arrays.asList(
        blockInPipeline(1L, pipeline), blockInPipeline(2L, pipeline), blockInPipeline(3L, pipeline)));

    AllocateScmBlockResponseProto response = allocate(3);

    assertEquals(3, response.getBlocksCount());
    for (int i = 0; i < response.getBlocksCount(); i++) {
      assertAllMembersHaveVersion(HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC.serialize(), response.getBlocks(i));
    }

    // The write version is memoized per pipeline: each node is looked up once
    // for the whole batch, not once per block.
    for (DatanodeDetails dn : nodes) {
      verify(nodeManager, times(1)).getNode(dn.getID());
    }
  }

  @Test
  public void testBlocksOnDistinctPipelinesGetOwnMinVersion() throws Exception {
    // A second, distinct pipeline whose datanodes are at an older version than
    // the software-version pipeline built in setUp().
    List<DatanodeDetails> otherNodes = registerNodes(3, HDDSVersion.STREAM_BLOCK_SUPPORT);

    Pipeline uniform = buildPipeline(nodes);
    Pipeline oneOld = buildPipeline(otherNodes);
    setAllocatedBlocks(Arrays.asList(blockInPipeline(1L, uniform), blockInPipeline(2L, oneOld)));

    AllocateScmBlockResponseProto response = allocate(2);

    assertEquals(2, response.getBlocksCount());
    assertAllMembersHaveVersion(HDDSVersion.SOFTWARE_VERSION.serialize(), response.getBlocks(0));
    assertAllMembersHaveVersion(HDDSVersion.STREAM_BLOCK_SUPPORT.serialize(), response.getBlocks(1));
  }

  /**
   * Tests that the original DatanodeDetails object is not modified when the current version is assigned and returned
   * to the client.
   */
  @Test
  public void testDatanodeDetailsVersionOverride() throws Exception {
    // Pipeline members carry the DEFAULT_VERSION baked in at pipeline creation,
    // but the live registry reports a higher version for every node. The
    // forwarded version must come from the registry, not the stale copy.
    DatanodeDetails oldNode = nodes.get(0);
    setLiveVersion(oldNode, HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC);

    for (DatanodeDetails member : nodes) {
      if (member.equals(oldNode)) {
        assertEquals(HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC, member.getCurrentVersion());
      } else {
        assertEquals(HDDSVersion.SOFTWARE_VERSION, member.getCurrentVersion());
      }
    }

    assertAllMembersHaveVersion(HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC.serialize(),
        allocate(1).getBlocks(0));

    // Serialization overrides only the outgoing proto; the source pipeline copies and the
    // registered node info keep their own versions.
    for (DatanodeDetails member : nodes) {
      if (member.equals(oldNode)) {
        assertEquals(HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC, member.getCurrentVersion());
      } else {
        assertEquals(HDDSVersion.SOFTWARE_VERSION, member.getCurrentVersion());
      }
    }
  }

  @Test
  public void testUnknownNodeThrows() {
    DatanodeDetails unknownNode = nodes.get(1);
    registry.remove(unknownNode.getID());
    SCMException ex = assertThrows(SCMException.class, () -> allocate(1));
    assertThat(ex.getMessage()).contains(unknownNode.toString());
  }
}
