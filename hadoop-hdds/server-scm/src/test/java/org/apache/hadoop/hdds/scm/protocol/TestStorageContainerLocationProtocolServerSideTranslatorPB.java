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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerIdExportStatusRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerIdExportStatusResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineBatchRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetExistContainerWithPipelinesInBatchRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetExistingContainersInBatchRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationRequest;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SubmitContainerIdExportRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.Type;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.NonRetriableException;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.scm.proxy.SCMContainerLocationFailoverProxyProvider;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io_.retry.RetryPolicies;
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

  private StorageContainerLocationInternalInterface impl;
  private StorageContainerLocationProtocolServerSideTranslatorPB service;
  private Map<DatanodeID, DatanodeInfo> registry;
  private List<DatanodeDetails> nodes;

  @BeforeEach
  void setUp() throws Exception {
    impl = mock(StorageContainerLocationInternalInterface.class);
    StorageContainerManager scm = mock(StorageContainerManager.class);
    when(scm.checkLeader()).thenReturn(true);

    registry = new HashMap<>();
    NodeManager nodeManager = mock(NodeManager.class);
    when(nodeManager.getNode(any(DatanodeID.class))).thenAnswer(inv -> registry.get(inv.getArgument(0)));
    when(scm.getScmNodeManager()).thenReturn(nodeManager);

    // Pipeline copies stay at SOFTWARE_VERSION; the registry holds a distinct live version per node.
    nodes = new ArrayList<>();
    nodes.add(registerNode(HDDSVersion.SEPARATE_RATIS_PORTS_AVAILABLE));
    nodes.add(registerNode(HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC));
    nodes.add(registerNode(HDDSVersion.STREAM_BLOCK_SUPPORT));

    service = new StorageContainerLocationProtocolServerSideTranslatorPB(impl, scm,
        mock(ProtocolMessageMetrics.class));
  }

  @Test
  void testGetExistingContainersInBatchRoundTrip() throws Exception {
    List<Long> ids = Arrays.asList(1L, 2L);
    ContainerInfo info = HddsTestUtils.getContainer(LifeCycleState.CLOSED);
    info.setSuppressed(true);
    when(impl.getExistingContainersInBatch(ids)).thenReturn(Collections.singletonList(info));
    StorageContainerLocationProtocolClientSideTranslatorPB client = metadataClient();

    List<ContainerInfo> containers = client.getExistingContainersInBatch(ids);

    assertEquals(1, containers.size());
    assertEquals(info.getProtobuf(), containers.get(0).getProtobuf());
    assertThat(client.getExistingContainersInBatch(Collections.emptyList())).isEmpty();
    assertThrows(IllegalStateException.class,
        () -> client.getExistingContainersInBatch(Collections.singletonList(-1L)));
  }

  @Test
  void testGetExistingContainersInBatchPropagatesFailure() throws Exception {
    List<Long> ids = Collections.singletonList(1L);
    when(impl.getExistingContainersInBatch(ids)).thenThrow(new IOException("SCM unavailable"));
    StorageContainerLocationProtocolClientSideTranslatorPB client = metadataClient();

    assertThrows(IOException.class, () -> client.getExistingContainersInBatch(ids));
  }

  @Test
  void testGetExistingContainersInBatchRejectsECForLegacyClient() throws Exception {
    ContainerInfo info = new ContainerInfo.Builder()
        .setContainerID(1L)
        .setState(LifeCycleState.CLOSED)
        .setReplicationConfig(new ECReplicationConfig(3, 2))
        .setOwner("test")
        .build();
    when(impl.getExistingContainersInBatch(Collections.singletonList(1L))).thenReturn(Collections.singletonList(info));
    ScmContainerLocationRequest request = ScmContainerLocationRequest.newBuilder()
        .setCmdType(Type.GetExistingContainersInBatch)
        .setVersion(ClientVersion.DEFAULT_VERSION.serialize())
        .setGetExistingContainersInBatchRequest(
            GetExistingContainersInBatchRequestProto.newBuilder().addContainerIDs(1L))
        .build();

    assertThrows(ServiceException.class, () -> service.submitRequest(null, request));
  }

  @Test
  void testSubmitContainerIdExportRoundTrip() throws Exception {
    when(impl.submitContainerIdExport(any(), any(), any(), anyLong(), anyInt(), anyInt())).thenReturn("job-1");
    StorageContainerLocationProtocolClientSideTranslatorPB client = metadataClient();

    assertEquals("job-1", client.submitContainerIdExport(
        ContainerID.valueOf(7), LifeCycleState.CLOSED, ContainerHealthState.MISSING, 1000, 20, 30));
    verify(impl).submitContainerIdExport(
        ContainerID.valueOf(7), LifeCycleState.CLOSED, ContainerHealthState.MISSING, 1000, 20, 30);

    // Every optional argument left out reaches the server as null / 0.
    client.submitContainerIdExport(null, null, ContainerHealthState.EMPTY, 0, 0, 0);
    verify(impl).submitContainerIdExport(isNull(), isNull(), eq(ContainerHealthState.EMPTY), eq(0L), eq(0), eq(0));
  }

  @Test
  void testSubmitContainerIdExportRejectsUnknownHealthStateWithoutRetry() throws Exception {
    SubmitContainerIdExportRequestProto bad = SubmitContainerIdExportRequestProto.newBuilder()
        .setHealthState("NOT_A_STATE").build();
    ScmContainerLocationRequest request = ScmContainerLocationRequest.newBuilder()
        .setCmdType(Type.SubmitContainerIdExport)
        .setVersion(ClientVersion.CURRENT.serialize())
        .setSubmitContainerIdExportRequest(bad)
        .build();

    ServiceException e = assertThrows(ServiceException.class, () -> service.submitRequest(null, request));
    assertThat(e.getCause()).isInstanceOf(NonRetriableException.class);
    assertThat(e.getCause().getMessage()).contains("NOT_A_STATE");
    verify(impl, never()).submitContainerIdExport(any(), any(), any(), anyLong(), anyInt(), anyInt());
  }

  @Test
  void testGetContainerIdExportStatusRoundTrip() throws Exception {
    GetContainerIdExportStatusResponseProto succeeded = GetContainerIdExportStatusResponseProto.newBuilder()
        .setJobId("job-1")
        .setState(GetContainerIdExportStatusResponseProto.ExecutionState.SUCCEEDED)
        .setLifecycleState(LifeCycleState.OPEN)
        .setHealthState(ContainerHealthState.UNDER_REPLICATED.name())
        .setMaxRows(5).setBatchSize(2).setPartSize(3)
        .setIdsWritten(4).setElapsedMs(1234)
        .setTarPath("/data/exports/job-1.tar.gz")
        .build();
    GetContainerIdExportStatusResponseProto failed = GetContainerIdExportStatusResponseProto.newBuilder()
        .setJobId("job-2")
        .setState(GetContainerIdExportStatusResponseProto.ExecutionState.FAILED)
        .setHealthState(ContainerHealthState.MISSING.name())
        .setErrorMessage("SCM lost leadership")
        .build();
    when(impl.getContainerIdExportStatus("job-1")).thenReturn(succeeded);
    when(impl.getContainerIdExportStatus("job-2")).thenReturn(failed);
    StorageContainerLocationProtocolClientSideTranslatorPB client = metadataClient();

    assertEquals(succeeded, client.getContainerIdExportStatus("job-1"));
    GetContainerIdExportStatusResponseProto actual = client.getContainerIdExportStatus("job-2");
    assertEquals(failed, actual);
    assertThat(actual.hasLifecycleState()).isFalse();
    assertThat(actual.hasTarPath()).isFalse();
  }

  @Test
  void testGetContainerIdExportStatusOfUnknownJobFailsWithoutRetry() throws Exception {
    when(impl.getContainerIdExportStatus("gone")).thenThrow(new NonRetriableException("job not found: gone"));
    ScmContainerLocationRequest request = ScmContainerLocationRequest.newBuilder()
        .setCmdType(Type.GetContainerIdExportStatus)
        .setVersion(ClientVersion.CURRENT.serialize())
        .setGetContainerIdExportStatusRequest(
            GetContainerIdExportStatusRequestProto.newBuilder().setJobId("gone"))
        .build();

    ServiceException e = assertThrows(ServiceException.class, () -> service.submitRequest(null, request));
    // The wrapped type is what makes the client's retry policy give up at once.
    assertThat(e.getCause()).isInstanceOf(NonRetriableException.class);
    assertThat(e.getCause().getMessage()).contains("gone");
  }

  private StorageContainerLocationProtocolClientSideTranslatorPB metadataClient() throws Exception {
    StorageContainerLocationProtocolPB rpc = mock(StorageContainerLocationProtocolPB.class);
    when(rpc.submitRequest(any(), any()))
        .thenAnswer(inv -> service.submitRequest(inv.getArgument(0), inv.getArgument(1)));
    SCMContainerLocationFailoverProxyProvider provider = mock(SCMContainerLocationFailoverProxyProvider.class);
    when(provider.getInterface()).thenReturn(StorageContainerLocationProtocolPB.class);
    when(provider.getProxy()).thenReturn(new FailoverProxyProvider.ProxyInfo<>(rpc, "test"));
    when(provider.getRetryPolicy()).thenReturn(RetryPolicies.TRY_ONCE_THEN_FAIL);
    return new StorageContainerLocationProtocolClientSideTranslatorPB(provider);
  }

  /**
   * Creates a pipeline member (left at {@link HDDSVersion#SOFTWARE_VERSION}) and registers it in the mocked node
   * manager registry with the given live {@code currentVersion}, mirroring what a heartbeat records on SCM.
   */
  private DatanodeDetails registerNode(HDDSVersion liveVersion) {
    DatanodeDetails dn = randomDatanodeDetails();
    // Test setup expects nodes to be created with the latest software version by default.
    assertEquals(HDDSVersion.SOFTWARE_VERSION, dn.getCurrentVersion());

    // Add the node to the registry with a live version representing what it is reporting on heartbeat.
    DatanodeInfo info = new DatanodeInfo(dn, NodeStatus.inServiceHealthy(),
        UpgradeUtils.defaultVersionProto(), HddsTestUtils.ROLL_INTERVAL_MS_DEFAULT);
    info.setCurrentVersion(liveVersion);
    registry.put(dn.getID(), info);
    return dn;
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
  private void assertCurrentVersionsOverriddenAndMatch(List<DatanodeDetailsProto> members) {
    assertEquals(nodes.size(), members.size());
    for (DatanodeDetailsProto member : members) {
      DatanodeID id = DatanodeDetails.getFromProtoBuf(member).getID();
      assertEquals(registry.get(id).getCurrentVersion().serialize(), member.getCurrentVersion());
      // All datanodes in the `nodes` list are in SOFTWARE_VERSION, but their live overrides have different versions
      // meaning that no pipeline should be using SOFTWARE_VERSION as its overall version.
      assertNotEquals(HDDSVersion.SOFTWARE_VERSION.serialize(), member.getCurrentVersion());
    }
  }

  @Test
  public void testGetContainerWithPipelineCurrentVersion() throws Exception {
    when(impl.getContainerWithPipeline(anyLong())).thenReturn(containerWithPipeline());

    List<DatanodeDetailsProto> members = service.getContainerWithPipeline(
        GetContainerWithPipelineRequestProto.newBuilder().setContainerID(1L).build(), ClientVersion.CURRENT)
        .getContainerWithPipeline().getPipeline().getMembersList();

    assertCurrentVersionsOverriddenAndMatch(members);
  }

  @Test
  public void testGetContainerWithPipelineBatchCurrentVersion() throws Exception {
    when(impl.getContainerWithPipelineBatch(any())).thenReturn(Collections.singletonList(containerWithPipeline()));

    List<DatanodeDetailsProto> members = service.getContainerWithPipelineBatch(
        GetContainerWithPipelineBatchRequestProto.newBuilder().addContainerIDs(1L).build(), ClientVersion.CURRENT)
        .getContainerWithPipelines(0).getPipeline().getMembersList();

    assertCurrentVersionsOverriddenAndMatch(members);
  }

  @Test
  public void testGetExistContainerWithPipelinesInBatchCurrentVersion() throws Exception {
    when(impl.getExistContainerWithPipelinesInBatch(any()))
        .thenReturn(Collections.singletonList(containerWithPipeline()));

    List<DatanodeDetailsProto> members = service.getExistContainerWithPipelinesInBatch(
        GetExistContainerWithPipelinesInBatchRequestProto.newBuilder().addContainerIDs(1L).build(),
        ClientVersion.CURRENT)
        .getContainerWithPipelines(0).getPipeline().getMembersList();

    assertCurrentVersionsOverriddenAndMatch(members);
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

    assertCurrentVersionsOverriddenAndMatch(members);
  }

  /**
   * Tests that the original DatanodeDetails object is not modified when the current version is assigned and returned
   * to the client.
   */
  @Test
  public void testDatanodeDetailsVersionOverride() throws Exception {
    for (DatanodeDetails member : nodes) {
      assertEquals(HDDSVersion.SOFTWARE_VERSION, member.getCurrentVersion());
    }
    when(impl.getContainerWithPipeline(anyLong())).thenReturn(containerWithPipeline());

    GetContainerWithPipelineRequestProto requestProto = GetContainerWithPipelineRequestProto
        .newBuilder()
        .setContainerID(1L)
        .build();
    assertCurrentVersionsOverriddenAndMatch(service.getContainerWithPipeline(requestProto, ClientVersion.CURRENT)
        .getContainerWithPipeline().getPipeline().getMembersList());

    // Serialization overrides only the outgoing proto; the source pipeline copies keep their own version.
    for (DatanodeDetails member : nodes) {
      assertEquals(HDDSVersion.SOFTWARE_VERSION, member.getCurrentVersion());
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
