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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineBatchRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetExistContainerWithPipelinesInBatchRequestProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
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
 * Tests that {@link StorageContainerLocationProtocolServerSideTranslatorPB} forwards each datanode's own current
 * {@code currentVersion} to read clients on the container-with-pipeline responses.
 * <p>
 * Pipeline members are frozen copies (rebuilt from the replicated pipeline proto), so their currentVersion can be
 * stale. The translator must source each member's version from the live node registry, which the heartbeat handler
 * keeps up to date. Unlike the write path (which forwards the pipeline-wide minimum), reads forward each member's
 * own version, so these tests register a <em>different</em> version per node and assert every member keeps its own.
 */
class TestStorageContainerLocationProtocolServerSideTranslatorPB {

  private StorageContainerLocationProtocol impl;
  private StorageContainerLocationProtocolServerSideTranslatorPB service;
  private Map<DatanodeID, DatanodeInfo> registry;
  private List<DatanodeDetails> nodes;

  @BeforeEach
  void setUp() throws Exception {
    impl = mock(StorageContainerLocationProtocol.class);
    StorageContainerManager scm = mock(StorageContainerManager.class);

    registry = new HashMap<>();
    NodeManager nodeManager = mock(NodeManager.class);
    when(nodeManager.getNode(any(DatanodeID.class))).thenAnswer(inv -> registry.get(inv.getArgument(0)));
    when(scm.getScmNodeManager()).thenReturn(nodeManager);

    // Pipeline copies stay at DEFAULT_VERSION; the registry holds a distinct live version per node.
    nodes = new ArrayList<>();
    nodes.add(registerNode(HDDSVersion.SEPARATE_RATIS_PORTS_AVAILABLE));
    nodes.add(registerNode(HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC));
    nodes.add(registerNode(HDDSVersion.STREAM_BLOCK_SUPPORT));

    service = new StorageContainerLocationProtocolServerSideTranslatorPB(impl, scm,
        mock(ProtocolMessageMetrics.class));
  }

  /**
   * Creates a pipeline member (left at {@link HDDSVersion#DEFAULT_VERSION}) and registers it in the mocked node
   * manager with the given live {@code currentVersion}, mirroring what a heartbeat records on SCM.
   */
  private DatanodeDetails registerNode(HDDSVersion liveVersion) {
    DatanodeDetails dn = randomDatanodeDetails();
    setLiveVersion(dn, liveVersion);
    return dn;
  }

  private void setLiveVersion(DatanodeDetails member, HDDSVersion version) {
    DatanodeInfo info = new DatanodeInfo(member, NodeStatus.inServiceHealthy(),
        UpgradeUtils.defaultVersionProto(), HddsTestUtils.ROLL_INTERVAL_MS_DEFAULT);
    info.setCurrentVersion(version);
    registry.put(member.getID(), info);
  }

  private ContainerWithPipeline containerWithPipeline() {
    Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.OPEN)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setNodes(nodes)
        .build();
    return new ContainerWithPipeline(HddsTestUtils.getContainer(LifeCycleState.CLOSED), pipeline);
  }

  /** Asserts each serialized member carries its own live (registry) version. */
  private void assertCurrentVersionsMatch(List<DatanodeDetailsProto> members) {
    assertEquals(nodes.size(), members.size());
    for (DatanodeDetailsProto member : members) {
      DatanodeID id = DatanodeDetails.getFromProtoBuf(member).getID();
      assertEquals(registry.get(id).getCurrentVersion().serialize(), member.getCurrentVersion());
    }
  }

  @Test
  public void testGetContainerWithPipelineCurrentVersion() throws Exception {
    when(impl.getContainerWithPipeline(anyLong())).thenReturn(containerWithPipeline());

    List<DatanodeDetailsProto> members = service.getContainerWithPipeline(
        GetContainerWithPipelineRequestProto.newBuilder().setContainerID(1L).build(), ClientVersion.CURRENT)
        .getContainerWithPipeline().getPipeline().getMembersList();

    assertCurrentVersionsMatch(members);
  }

  @Test
  public void testGetContainerWithPipelineBatchCurrentVersion() throws Exception {
    when(impl.getContainerWithPipelineBatch(any())).thenReturn(Collections.singletonList(containerWithPipeline()));

    List<DatanodeDetailsProto> members = service.getContainerWithPipelineBatch(
        GetContainerWithPipelineBatchRequestProto.newBuilder().addContainerIDs(1L).build(), ClientVersion.CURRENT)
        .getContainerWithPipelines(0).getPipeline().getMembersList();

    assertCurrentVersionsMatch(members);
  }

  @Test
  public void testGetExistContainerWithPipelinesInBatchCurrentVersion() throws Exception {
    when(impl.getExistContainerWithPipelinesInBatch(any()))
        .thenReturn(Collections.singletonList(containerWithPipeline()));

    List<DatanodeDetailsProto> members = service.getExistContainerWithPipelinesInBatch(
        GetExistContainerWithPipelinesInBatchRequestProto.newBuilder().addContainerIDs(1L).build(),
        ClientVersion.CURRENT)
        .getContainerWithPipelines(0).getPipeline().getMembersList();

    assertCurrentVersionsMatch(members);
  }

  @Test
  public void testAllocateContainerCurrentVersion() throws Exception {
    when(impl.allocateContainer(any(ReplicationConfig.class), anyString())).thenReturn(containerWithPipeline());

    List<DatanodeDetailsProto> members = service.allocateContainer(
        ContainerRequestProto.newBuilder()
            .setReplicationType(ReplicationType.RATIS)
            .setReplicationFactor(ReplicationFactor.THREE)
            .setOwner("owner")
            .build(), ClientVersion.CURRENT)
        .getContainerWithPipeline().getPipeline().getMembersList();

    assertCurrentVersionsMatch(members);
  }

  /**
   * Tests that the original DatanodeDetails object is not modified when the current version is assigned and returned
   * to the client.
   */
  @Test
  public void testDatanodeDetailsVersionOverride() throws Exception {
    for (DatanodeDetails member : nodes) {
      assertEquals(HDDSVersion.DEFAULT_VERSION, member.getCurrentVersion());
    }
    when(impl.getContainerWithPipeline(anyLong())).thenReturn(containerWithPipeline());

    assertCurrentVersionsMatch(service.getContainerWithPipeline(
        GetContainerWithPipelineRequestProto.newBuilder().setContainerID(1L).build(), ClientVersion.CURRENT)
        .getContainerWithPipeline().getPipeline().getMembersList());

    // Serialization overrides only the outgoing proto; the source pipeline copies keep their own version.
    for (DatanodeDetails member : nodes) {
      assertEquals(HDDSVersion.DEFAULT_VERSION, member.getCurrentVersion());
    }
  }

  @Test
  public void testUnknownNodeThrows() throws Exception {
    DatanodeDetails unknownNode = nodes.get(1);
    registry.remove(unknownNode.getID());
    when(impl.getContainerWithPipeline(anyLong())).thenReturn(containerWithPipeline());

    SCMException ex = assertThrows(SCMException.class, () -> service.getContainerWithPipeline(
        GetContainerWithPipelineRequestProto.newBuilder().setContainerID(1L).build(), ClientVersion.CURRENT));
    assertThat(ex.getMessage()).contains(unknownNode.toString());
  }
}
