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
import java.util.Collections;
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
import org.apache.hadoop.hdds.scm.container.SimpleMockNodeManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.ScmVersionManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
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

  private ScmBlockLocationProtocol impl;
  private NodeManager nodeManager;
  private ScmVersionManager versionManager;
  private ScmBlockLocationProtocolServerSideTranslatorPB service;
  private List<DatanodeDetails> nodes;

  @BeforeEach
  void setUp() throws Exception {
    impl = mock(ScmBlockLocationProtocol.class);
    StorageContainerManager scm = mock(StorageContainerManager.class);
    nodeManager = new SimpleMockNodeManager();
    versionManager = mock(ScmVersionManager.class);

    nodes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      DatanodeDetails dn = randomDatanodeDetails();
      dn.setCurrentVersion(HDDSVersion.SOFTWARE_VERSION);
      nodes.add(dn);
    }

    Pipeline pipeline = buildPipeline(nodes);
    AllocatedBlock block = blockOn(1L, pipeline);

    when(impl.allocateBlock(anyLong(), anyInt(), any(ReplicationConfig.class),
        anyString(), any(), anyString())).thenReturn(Collections.singletonList(block));
    when(scm.getScmNodeManager()).thenReturn(nodeManager);
    when(scm.getVersionManager()).thenReturn(versionManager);
//    // Simulate the real computeCommonVersion: resolve DatanodeInfo for each node, then delegate to versionManager.
//    lenient().when(nodeManager.computeCommonVersion(any())).thenAnswer(invocation -> {
//      List<DatanodeDetails> dns = invocation.getArgument(0);
//      if (dns.isEmpty()) {
//        throw new NodeNotFoundException();
//      }
//      List<DatanodeInfo> infos = new ArrayList<>();
//      for (DatanodeDetails dn : dns) {
//        DatanodeInfo info = nodeManager.getNode(dn.getID());
//        if (info == null) {
//          throw new NodeNotFoundException(dn.getID());
//        }
//        infos.add(info);
//      }
//      return versionManager.computeCommonVersion(infos);
//    });

    // Default: ZDU is finalized and every datanode is at the software version.
    when(versionManager.isAllowed(HDDSVersion.ZDU)).thenReturn(true);
//    when(versionManager.computeCommonVersion(any())).thenCallRealMethod();
    for (DatanodeDetails dn : nodes) {
      setDatanodeApparentVersion(dn, HDDSVersion.SOFTWARE_VERSION);
    }

    service = new ScmBlockLocationProtocolServerSideTranslatorPB(impl, scm, mock(ProtocolMessageMetrics.class));
  }

  private void setDatanodeApparentVersion(DatanodeDetails dn, ComponentVersion version) {
    DatanodeInfo info = mock(DatanodeInfo.class);
    lenient().when(info.getLastKnownApparentVersion()).thenReturn(version);
    when(nodeManager.getNode(dn.getID())).thenReturn(info);
  }

  private List<DatanodeDetailsProto> allocateAndGetMembers() throws Exception {
    return allocate(1).getBlocks(0).getPipeline().getMembersList();
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

  private AllocatedBlock blockOn(long localId, Pipeline pipeline) {
    return new AllocatedBlock.Builder()
        .setContainerBlockID(new ContainerBlockID(1L, localId))
        .setPipeline(pipeline)
        .build();
  }

  private void setAllocatedBlocks(List<AllocatedBlock> blocks) throws Exception {
    when(impl.allocateBlock(anyLong(), anyInt(), any(ReplicationConfig.class),
        anyString(), any(), anyString())).thenReturn(blocks);
  }

  private void assertAllMembersHaveVersion(int expected, List<DatanodeDetailsProto> members) {
    assertEquals(nodes.size(), members.size());
    for (DatanodeDetailsProto member : members) {
      assertEquals(expected, member.getCurrentVersion());
    }
  }

  @Test
  void preFinalizedClusterClampsClientVersionDown() throws Exception {
    // Before ZDU is finalized, datanodes report apparent versions from the
    // HDDSLayoutFeature enum. Regardless of what they report, clients must be
    // clamped to the last HDDSVersion before ZDU.
    when(versionManager.isAllowed(HDDSVersion.ZDU)).thenReturn(false);
    for (DatanodeDetails dn : nodes) {
      setDatanodeApparentVersion(dn, HDDSLayoutFeature.STORAGE_SPACE_DISTRIBUTION);
    }

    assertAllMembersHaveVersion(HDDSVersion.STREAM_BLOCK_SUPPORT.serialize(), allocateAndGetMembers());
  }

  @Test
  void finalizedClusterForwardsRealVersion() throws Exception {
    assertAllMembersHaveVersion(HDDSVersion.SOFTWARE_VERSION.serialize(), allocateAndGetMembers());
  }

  @Test
  void finalizedPipelineForwardsLowestApparentVersion() throws Exception {
    // ZDU is finalized, but the pipeline mixes datanodes at different apparent
    // versions (as during a later rolling upgrade). The lowest is forwarded so
    // clients do not enable a feature an un-upgraded datanode cannot handle.
    setDatanodeApparentVersion(nodes.get(1), HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC);

    assertAllMembersHaveVersion(HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC.serialize(), allocateAndGetMembers());
  }

  @Test
  void missingDatanodeInfoFailsAllocation() throws Exception {
    when(nodeManager.getNode(nodes.get(2).getID())).thenReturn(null);

    SCMException e = assertThrows(SCMException.class, () -> allocate(1));
    assertEquals(SCMException.ResultCodes.NO_SUCH_DATANODE, e.getResult());
  }

  @Test
  void emptyPipelineFailsAllocation() throws Exception {
    setAllocatedBlocks(Collections.singletonList(blockOn(1L, buildPipeline(Collections.emptyList()))));

    SCMException e = assertThrows(SCMException.class, () -> allocate(1));
    assertEquals(SCMException.ResultCodes.FAILED_TO_FIND_ACTIVE_PIPELINE, e.getResult());
  }

  @Test
  void blocksSharingPipelineAllGetClampedVersion() throws Exception {
    // One straggler datanode clamps the shared pipeline's write version.
    setDatanodeApparentVersion(nodes.get(1), HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC);

    // Three blocks allocated on the same pipeline object; the memoized proto
    // must be returned for each block with the clamped version intact.
    Pipeline pipeline = buildPipeline(nodes);
    setAllocatedBlocks(Arrays.asList(blockOn(1L, pipeline), blockOn(2L, pipeline), blockOn(3L, pipeline)));

    AllocateScmBlockResponseProto response = allocate(3);

    assertEquals(3, response.getBlocksCount());
    for (int i = 0; i < response.getBlocksCount(); i++) {
      assertAllMembersHaveVersion(HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC.serialize(),
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
    // A second, distinct pipeline whose datanodes are at an older
    // version than the software-version pipeline built in setUp().
    List<DatanodeDetails> otherNodes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      DatanodeDetails dn = randomDatanodeDetails();
      dn.setCurrentVersion(HDDSVersion.SOFTWARE_VERSION);
      setDatanodeApparentVersion(dn, HDDSVersion.STREAM_BLOCK_SUPPORT);
      otherNodes.add(dn);
    }

    Pipeline finalized = buildPipeline(nodes);
    Pipeline straggler = buildPipeline(otherNodes);
    setAllocatedBlocks(Arrays.asList(blockOn(1L, finalized), blockOn(2L, straggler)));

    AllocateScmBlockResponseProto response = allocate(2);

    assertEquals(2, response.getBlocksCount());
    assertAllMembersHaveVersion(HDDSVersion.SOFTWARE_VERSION.serialize(),
        response.getBlocks(0).getPipeline().getMembersList());
    assertEquals(otherNodes.size(), response.getBlocks(1).getPipeline().getMembersCount());
    for (DatanodeDetailsProto member : response.getBlocks(1).getPipeline().getMembersList()) {
      assertEquals(HDDSVersion.STREAM_BLOCK_SUPPORT.serialize(), member.getCurrentVersion());
    }
  }

  @Test
  void doesNotMutateSourcePipelineDatanodes() throws Exception {
    setDatanodeApparentVersion(nodes.get(1), HDDSVersion.STREAM_BLOCK_SUPPORT);

    allocateAndGetMembers();

    // The in-memory DatanodeDetails (shared with SCM internal state) must keep
    // their real software version; only the outgoing proto is overridden.
    for (DatanodeDetails dn : nodes) {
      assertEquals(HDDSVersion.SOFTWARE_VERSION, dn.getCurrentVersion());
    }
  }
}
