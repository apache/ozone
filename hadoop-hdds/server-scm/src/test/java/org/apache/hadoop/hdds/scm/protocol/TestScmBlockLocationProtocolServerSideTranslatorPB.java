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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateScmBlockRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateScmBlockResponseProto;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ozone.ClientVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link ScmBlockLocationProtocolServerSideTranslatorPB} forwards a
 * clamped write version (based on the cluster's finalization status) in the
 * {@code currentVersion} of every pipeline member it returns on block
 * allocation, without mutating SCM's in-memory state.
 */
class TestScmBlockLocationProtocolServerSideTranslatorPB {

  /** The version each source datanode reports as its own software version. */
  private static final int DN_SOFTWARE_VERSION =
      HDDSVersion.ZDU.serialize();

  private ScmBlockLocationProtocol impl;
  private NodeManager nodeManager;
  private ScmBlockLocationProtocolServerSideTranslatorPB service;
  private List<DatanodeDetails> nodes;

  @BeforeEach
  void setUp() throws Exception {
    impl = mock(ScmBlockLocationProtocol.class);
    StorageContainerManager scm = mock(StorageContainerManager.class);
    nodeManager = mock(NodeManager.class);

    nodes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      DatanodeDetails dn = randomDatanodeDetails();
      dn.setCurrentVersion(DN_SOFTWARE_VERSION);
      nodes.add(dn);
    }

    Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.OPEN)
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setNodes(nodes)
        .build();
    AllocatedBlock block = new AllocatedBlock.Builder()
        .setContainerBlockID(new ContainerBlockID(1L, 1L))
        .setPipeline(pipeline)
        .build();

    List<AllocatedBlock> blocks = new ArrayList<>();
    blocks.add(block);
    when(impl.allocateBlock(anyLong(), anyInt(), any(ReplicationConfig.class),
        anyString(), any(), anyString())).thenReturn(blocks);
    when(scm.getScmNodeManager()).thenReturn(nodeManager);
    // Exercise the real min-across-nodes helper, driven by the getNode stubs.
    lenient().when(nodeManager.getLowestApparentVersion(any(DatanodeDetails[].class)))
        .thenCallRealMethod();

    // Default: every datanode finalized to ZDU.
    for (DatanodeDetails dn : nodes) {
      setDatanodeApparentVersion(dn, HDDSVersion.ZDU);
    }

    service = new ScmBlockLocationProtocolServerSideTranslatorPB(
        impl, scm, mock(ProtocolMessageMetrics.class));
  }

  private void setDatanodeApparentVersion(DatanodeDetails dn,
      ComponentVersion version) {
    DatanodeInfo info = mock(DatanodeInfo.class);
    lenient().when(info.getLastKnownApparentVersion()).thenReturn(version);
    when(nodeManager.getNode(dn.getID())).thenReturn(info);
  }

  private List<DatanodeDetailsProto> allocateAndGetMembers() throws Exception {
    return allocate(1).getBlocks(0).getPipeline().getMembersList();
  }

  private AllocateScmBlockResponseProto allocate(int numBlocks)
      throws Exception {
    AllocateScmBlockRequestProto request =
        AllocateScmBlockRequestProto.newBuilder()
            .setSize(1024)
            .setNumBlocks(numBlocks)
            .setType(ReplicationType.RATIS)
            .setFactor(ReplicationFactor.THREE)
            .setOwner("owner")
            .build();
    return service.allocateScmBlock(request, ClientVersion.CURRENT.serialize());
  }

  private Pipeline buildPipeline(List<DatanodeDetails> pipelineNodes) {
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.OPEN)
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setNodes(pipelineNodes)
        .build();
  }

  private AllocatedBlock blockOn(long localId, Pipeline pipeline) {
    return new AllocatedBlock.Builder()
        .setContainerBlockID(new ContainerBlockID(1L, localId))
        .setPipeline(pipeline)
        .build();
  }

  private void setAllocatedBlocks(List<AllocatedBlock> blocks)
      throws Exception {
    when(impl.allocateBlock(anyLong(), anyInt(), any(ReplicationConfig.class),
        anyString(), any(), anyString())).thenReturn(blocks);
  }

  private void assertAllMembersHaveVersion(int expected,
      List<DatanodeDetailsProto> members) {
    assertEquals(nodes.size(), members.size());
    for (DatanodeDetailsProto member : members) {
      assertEquals(expected, member.getCurrentVersion());
    }
  }

  @Test
  void preFinalizedClusterClampsClientVersionDown() throws Exception {
    // Datanodes still run ZDU software but have not finalized past
    // STREAM_BLOCK_SUPPORT, so their apparent version is the older one.
    for (DatanodeDetails dn : nodes) {
      setDatanodeApparentVersion(dn, HDDSVersion.STREAM_BLOCK_SUPPORT);
    }

    assertAllMembersHaveVersion(HDDSVersion.STREAM_BLOCK_SUPPORT.serialize(),
        allocateAndGetMembers());
  }

  @Test
  void finalizedClusterForwardsRealVersion() throws Exception {
    // Default setup: SCM and all datanodes at ZDU.
    assertAllMembersHaveVersion(HDDSVersion.ZDU.serialize(),
        allocateAndGetMembers());
  }

  @Test
  void unfinalizedDatanodeClampsToMinimum() throws Exception {
    // SCM is finalized, but one straggler datanode is still at an older
    // apparent version; the client must be clamped down to it.
    setDatanodeApparentVersion(nodes.get(1),
        HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC);

    assertAllMembersHaveVersion(
        HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC.serialize(),
        allocateAndGetMembers());
  }

  @Test
  void missingDatanodeInfoFailsAllocation() throws Exception {
    when(nodeManager.getNode(nodes.get(2).getID())).thenReturn(null);

    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> allocate(1));
    assertInstanceOf(NodeNotFoundException.class, e.getCause());
  }

  @Test
  void blocksSharingPipelineAllGetClampedVersion() throws Exception {
    // One straggler datanode clamps the shared pipeline's write version.
    setDatanodeApparentVersion(nodes.get(1),
        HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC);

    // Three blocks allocated on the same pipeline object; the memoized proto
    // must be returned for each block with the clamped version intact.
    Pipeline pipeline = buildPipeline(nodes);
    setAllocatedBlocks(Arrays.asList(
        blockOn(1L, pipeline), blockOn(2L, pipeline), blockOn(3L, pipeline)));

    AllocateScmBlockResponseProto response = allocate(3);

    assertEquals(3, response.getBlocksCount());
    for (int i = 0; i < response.getBlocksCount(); i++) {
      assertAllMembersHaveVersion(
          HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC.serialize(),
          response.getBlocks(i).getPipeline().getMembersList());
    }

    // The write version is memoized per pipeline: each node is looked up once
    // for the whole batch, not once per block.
    for (DatanodeDetails dn : nodes) {
      verify(nodeManager, times(1)).getNode(dn.getID());
    }
  }

  @Test
  void blocksOnDistinctPipelinesGetOwnVersion() throws Exception {
    // A second, distinct pipeline whose datanodes are clamped to an older
    // version than the default (ZDU) pipeline built in setUp().
    List<DatanodeDetails> otherNodes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      DatanodeDetails dn = randomDatanodeDetails();
      dn.setCurrentVersion(DN_SOFTWARE_VERSION);
      setDatanodeApparentVersion(dn, HDDSVersion.STREAM_BLOCK_SUPPORT);
      otherNodes.add(dn);
    }

    Pipeline finalized = buildPipeline(nodes);
    Pipeline straggler = buildPipeline(otherNodes);
    setAllocatedBlocks(
        Arrays.asList(blockOn(1L, finalized), blockOn(2L, straggler)));

    AllocateScmBlockResponseProto response = allocate(2);

    assertEquals(2, response.getBlocksCount());
    assertAllMembersHaveVersion(HDDSVersion.ZDU.serialize(),
        response.getBlocks(0).getPipeline().getMembersList());
    assertEquals(otherNodes.size(),
        response.getBlocks(1).getPipeline().getMembersCount());
    for (DatanodeDetailsProto member
        : response.getBlocks(1).getPipeline().getMembersList()) {
      assertEquals(HDDSVersion.STREAM_BLOCK_SUPPORT.serialize(),
          member.getCurrentVersion());
    }
  }

  @Test
  void doesNotMutateSourcePipelineDatanodes() throws Exception {
    for (DatanodeDetails dn : nodes) {
      setDatanodeApparentVersion(dn, HDDSVersion.STREAM_BLOCK_SUPPORT);
    }

    allocateAndGetMembers();

    // The in-memory DatanodeDetails (shared with SCM internal state) must keep
    // their real software version; only the outgoing proto is overridden.
    for (DatanodeDetails dn : nodes) {
      assertEquals(DN_SOFTWARE_VERSION, dn.getCurrentVersion());
    }
  }
}
