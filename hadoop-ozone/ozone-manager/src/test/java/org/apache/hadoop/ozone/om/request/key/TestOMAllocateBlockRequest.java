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

package org.apache.hadoop.ozone.om.request.key;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.annotation.Nonnull;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

/**
 * Tests OMAllocateBlockRequest class.
 */
public class TestOMAllocateBlockRequest extends OMKeyRequestTests {

  @Test
  public void testPreExecute() throws Exception {

    doPreExecute(createAllocateBlockRequest());

  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    // Add volume, bucket, key entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    addKeyToOpenKeyTable(volumeName, bucketName);

    OMRequest modifiedOmRequest =
        doPreExecute(createAllocateBlockRequest());

    OMAllocateBlockRequest omAllocateBlockRequest =
            getOmAllocateBlockRequest(modifiedOmRequest);

    // Check before calling validateAndUpdateCache. As adding DB entry has
    // not added any blocks, so size should be zero.

    OmKeyInfo omKeyInfo = verifyPathInOpenKeyTable(keyName, clientID,
            true);

    List<OmKeyLocationInfo> omKeyLocationInfo =
        omKeyInfo.getLatestVersionLocations().getLocationList();

    assertEquals(0, omKeyLocationInfo.size());

    OMClientResponse omAllocateBlockResponse =
        omAllocateBlockRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omAllocateBlockResponse.getOMResponse().getStatus());

    // Check open table whether new block is added or not.

    omKeyInfo = verifyPathInOpenKeyTable(keyName, clientID,
            true);

    // Check modification time
    assertEquals(modifiedOmRequest.getAllocateBlockRequest()
        .getKeyArgs().getModificationTime(), omKeyInfo.getModificationTime());

    // creationTime was assigned at OMRequestTestUtils.addKeyToTable
    // modificationTime was assigned at
    // doPreExecute(createAllocateBlockRequest())
    assertThat(omKeyInfo.getCreationTime())
        .isLessThanOrEqualTo(omKeyInfo.getModificationTime());

    // Check data of the block
    OzoneManagerProtocolProtos.KeyLocation keyLocation =
        modifiedOmRequest.getAllocateBlockRequest().getKeyLocation();

    omKeyLocationInfo =
        omKeyInfo.getLatestVersionLocations().getLocationList();

    assertEquals(1, omKeyLocationInfo.size());

    assertEquals(keyLocation.getBlockID().getContainerBlockID()
        .getContainerID(), omKeyLocationInfo.get(0).getContainerID());

    assertEquals(keyLocation.getBlockID().getContainerBlockID()
            .getLocalID(), omKeyLocationInfo.get(0).getLocalID());

  }

  @Nonnull
  protected OMAllocateBlockRequest getOmAllocateBlockRequest(
          OMRequest modifiedOmRequest) {
    return new OMAllocateBlockRequest(modifiedOmRequest, BucketLayout.DEFAULT);
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createAllocateBlockRequest());

    OMAllocateBlockRequest omAllocateBlockRequest =
            getOmAllocateBlockRequest(modifiedOmRequest);


    OMClientResponse omAllocateBlockResponse =
        omAllocateBlockRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertSame(omAllocateBlockResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND);

  }

  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createAllocateBlockRequest());

    OMAllocateBlockRequest omAllocateBlockRequest =
            getOmAllocateBlockRequest(modifiedOmRequest);


    // Added only volume to DB.
    OMRequestTestUtils.addVolumeToDB(volumeName, OzoneConsts.OZONE,
        omMetadataManager);

    OMClientResponse omAllocateBlockResponse =
        omAllocateBlockRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertSame(omAllocateBlockResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND);

  }

  @Test
  public void testValidateAndUpdateCacheWithKeyNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createAllocateBlockRequest());

    OMAllocateBlockRequest omAllocateBlockRequest =
            getOmAllocateBlockRequest(modifiedOmRequest);

    // Add volume, bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, omAllocateBlockRequest.getBucketLayout());


    OMClientResponse omAllocateBlockResponse =
        omAllocateBlockRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertSame(omAllocateBlockResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND);

  }

  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOMRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */
  protected OMRequest doPreExecute(OMRequest originalOMRequest)
      throws Exception {

    OMAllocateBlockRequest omAllocateBlockRequest =
            getOmAllocateBlockRequest(originalOMRequest);

    OMRequest modifiedOmRequest =
        omAllocateBlockRequest.preExecute(ozoneManager);


    assertEquals(originalOMRequest.getCmdType(),
        modifiedOmRequest.getCmdType());
    assertEquals(originalOMRequest.getClientId(),
        modifiedOmRequest.getClientId());

    assertTrue(modifiedOmRequest.hasAllocateBlockRequest());
    AllocateBlockRequest allocateBlockRequest =
        modifiedOmRequest.getAllocateBlockRequest();
    // Time should be set
    assertThat(allocateBlockRequest.getKeyArgs().getModificationTime())
        .isGreaterThan(0);

    // KeyLocation should be set.
    assertTrue(allocateBlockRequest.hasKeyLocation());
    assertEquals(CONTAINER_ID,
        allocateBlockRequest.getKeyLocation().getBlockID()
            .getContainerBlockID().getContainerID());
    assertEquals(LOCAL_ID,
        allocateBlockRequest.getKeyLocation().getBlockID()
            .getContainerBlockID().getLocalID());
    assertTrue(allocateBlockRequest.getKeyLocation().hasPipeline());

    assertEquals(allocateBlockRequest.getClientID(),
        allocateBlockRequest.getClientID());

    return modifiedOmRequest;
  }

  @Test
  public void testAllocateBlockSendsClientMachineToScmWhenFlagOff() throws Exception {
    // Flag off (default): OM must NOT sort; SCM receives the real client address
    // so it performs the sort.
    KeyManager mockKeyManager = mock(KeyManager.class);
    when(mockKeyManager.isSortDatanodesForWriteEnabled()).thenReturn(false);
    when(ozoneManager.getKeyManager()).thenReturn(mockKeyManager);

    OMAllocateBlockRequest request =
        getOmAllocateBlockRequest(createAllocateBlockRequestWithSort());
    preExecuteWithClient(request, "1.2.3.4");

    ArgumentCaptor<String> clientMachine = ArgumentCaptor.forClass(String.class);
    verify(scmBlockLocationProtocol).allocateBlock(anyLong(), anyInt(), any(),
        any(), any(), clientMachine.capture());
    assertEquals("1.2.3.4", clientMachine.getValue());
    verify(mockKeyManager, never()).sortDatanodesForWrite(any(), anyString(), any());
  }

  @Test
  public void testAllocateBlockDoesNotSendClientMachineToScm() throws Exception {
    // OM now sorts the write pipeline locally, so SCM must receive an empty
    // clientMachine even when the client requests sorted datanodes.
    KeyManager mockKeyManager = mock(KeyManager.class);
    when(mockKeyManager.isSortDatanodesForWriteEnabled()).thenReturn(true);
    when(mockKeyManager.sortDatanodesForWrite(any(), any(), any()))
        .thenAnswer(inv -> inv.getArgument(0));
    when(ozoneManager.getKeyManager()).thenReturn(mockKeyManager);
    when(ozoneManager.getClusterMapAllowNull()).thenReturn(mock(NetworkTopology.class));

    OMAllocateBlockRequest request =
        getOmAllocateBlockRequest(createAllocateBlockRequestWithSort());
    preExecuteWithClient(request, "1.2.3.4");

    ArgumentCaptor<String> clientMachine = ArgumentCaptor.forClass(String.class);
    verify(scmBlockLocationProtocol).allocateBlock(anyLong(), anyInt(), any(),
        any(), any(), clientMachine.capture());
    assertEquals("", clientMachine.getValue());
  }

  @Test
  public void testAllocateBlockFallsBackToScmWhenTopologyUnavailable() throws Exception {
    KeyManager mockKeyManager = mock(KeyManager.class);
    when(mockKeyManager.isSortDatanodesForWriteEnabled()).thenReturn(true);
    when(ozoneManager.getKeyManager()).thenReturn(mockKeyManager);

    OMAllocateBlockRequest request =
        getOmAllocateBlockRequest(createAllocateBlockRequestWithSort());
    preExecuteWithClient(request, "1.2.3.4");

    ArgumentCaptor<String> clientMachine = ArgumentCaptor.forClass(String.class);
    verify(scmBlockLocationProtocol).allocateBlock(anyLong(), anyInt(), any(),
        any(), any(), clientMachine.capture());
    assertEquals("1.2.3.4", clientMachine.getValue());
    verify(mockKeyManager, never()).sortDatanodesForWrite(any(), anyString(), any());
  }

  @Test
  public void testAllocateBlockSortsSharedPipelineOnce() throws Exception {
    // Two blocks on the same 3-node pipeline must be sorted once, and the
    // sorted order must land in every block's pipeline.
    List<DatanodeDetails> nodes = Arrays.asList(
        MockDatanodeDetails.randomDatanodeDetails(),
        MockDatanodeDetails.randomDatanodeDetails(),
        MockDatanodeDetails.randomDatanodeDetails());
    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setNodes(nodes)
        .build();
    AllocatedBlock.Builder blockBuilder =
        new AllocatedBlock.Builder().setPipeline(pipeline);
    when(scmBlockLocationProtocol.allocateBlock(anyLong(), anyInt(), any(),
        anyString(), any(ExcludeList.class), anyString())).thenAnswer(inv -> {
          int num = inv.getArgument(1);
          List<AllocatedBlock> blocks = new ArrayList<>(num);
          for (int i = 0; i < num; i++) {
            blockBuilder.setContainerBlockID(
                new ContainerBlockID(CONTAINER_ID + i, LOCAL_ID + i));
            blocks.add(blockBuilder.build());
          }
          return blocks;
        });

    List<DatanodeDetails> sortedOrder = new ArrayList<>(nodes);
    Collections.reverse(sortedOrder);
    KeyManager mockKeyManager = mock(KeyManager.class);
    when(mockKeyManager.isSortDatanodesForWriteEnabled()).thenReturn(true);
    when(mockKeyManager.sortDatanodesForWrite(any(), any(), any()))
        .thenAnswer(inv -> sortedOrder);
    when(ozoneManager.getKeyManager()).thenReturn(mockKeyManager);
    when(ozoneManager.getClusterMapAllowNull()).thenReturn(mock(NetworkTopology.class));

    OMAllocateBlockRequest request =
        getOmAllocateBlockRequest(createAllocateBlockRequest());
    // requestedSize spans two scmBlockSize blocks on the same pipeline.
    List<OmKeyLocationInfo> locations = request.allocateBlock(replicationConfig,
        new ExcludeList(), 2 * scmBlockSize, true,
        UserInfo.newBuilder().setRemoteAddress("1.2.3.4").build(), ozoneManager);

    // Sorted once for the shared pipeline...
    verify(mockKeyManager, times(1)).sortDatanodesForWrite(any(), eq("1.2.3.4"), any());
    // ...and the sorted order is applied to every block's pipeline.
    assertEquals(2, locations.size());
    for (OmKeyLocationInfo location : locations) {
      assertEquals(sortedOrder, location.getPipeline().getNodesInOrder());
    }
  }

  @Test
  public void testAllocateBlockKeepsPerPipelineOrderWhenSortSkipped() throws Exception {
    // Two pipelines share the same datanode set but in a different order. When
    // the sort is skipped (sortDatanodesForWrite returns the input unchanged),
    // each pipeline must keep its own order: the unsorted result must not be
    // cached under the node set and reused for the other pipeline.
    DatanodeDetails a = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails b = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails c = MockDatanodeDetails.randomDatanodeDetails();
    List<DatanodeDetails> nodes1 = Arrays.asList(a, b, c);
    List<DatanodeDetails> nodes2 = Arrays.asList(c, b, a);
    Pipeline pipeline1 = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setNodes(nodes1)
        .build();
    Pipeline pipeline2 = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setNodes(nodes2)
        .build();
    AllocatedBlock block1 = new AllocatedBlock.Builder().setPipeline(pipeline1)
        .setContainerBlockID(new ContainerBlockID(CONTAINER_ID, LOCAL_ID)).build();
    AllocatedBlock block2 = new AllocatedBlock.Builder().setPipeline(pipeline2)
        .setContainerBlockID(new ContainerBlockID(CONTAINER_ID + 1, LOCAL_ID + 1)).build();
    when(scmBlockLocationProtocol.allocateBlock(anyLong(), anyInt(), any(),
        anyString(), any(ExcludeList.class), anyString()))
        .thenReturn(Arrays.asList(block1, block2));

    KeyManager mockKeyManager = mock(KeyManager.class);
    when(mockKeyManager.isSortDatanodesForWriteEnabled()).thenReturn(true);
    // Skip the sort: return the input list instance unchanged.
    when(mockKeyManager.sortDatanodesForWrite(any(), any(), any()))
        .thenAnswer(inv -> inv.getArgument(0));
    when(ozoneManager.getKeyManager()).thenReturn(mockKeyManager);
    when(ozoneManager.getClusterMapAllowNull()).thenReturn(mock(NetworkTopology.class));

    OMAllocateBlockRequest request =
        getOmAllocateBlockRequest(createAllocateBlockRequest());
    List<OmKeyLocationInfo> locations = request.allocateBlock(replicationConfig,
        new ExcludeList(), 2 * scmBlockSize, true,
        UserInfo.newBuilder().setRemoteAddress("1.2.3.4").build(), ozoneManager);

    assertEquals(2, locations.size());
    // Each pipeline keeps its own order; the skipped-sort result is not shared.
    assertEquals(nodes1, locations.get(0).getPipeline().getNodesInOrder());
    assertEquals(nodes2, locations.get(1).getPipeline().getNodesInOrder());
    // Sorted per pipeline, since the unsorted result is not cached.
    verify(mockKeyManager, times(2)).sortDatanodesForWrite(any(), eq("1.2.3.4"), any());
  }

  @Test
  public void testAllocateBlockKeepsOrderWhenRemoteAddressEmpty() throws Exception {
    // Sort enabled and topology available, but the client has no remote address:
    // OM must not sort, SCM receives an empty clientMachine, and the pipeline
    // order is preserved.
    List<DatanodeDetails> nodes = Arrays.asList(
        MockDatanodeDetails.randomDatanodeDetails(),
        MockDatanodeDetails.randomDatanodeDetails(),
        MockDatanodeDetails.randomDatanodeDetails());
    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setNodes(nodes)
        .build();
    AllocatedBlock block = new AllocatedBlock.Builder().setPipeline(pipeline)
        .setContainerBlockID(new ContainerBlockID(CONTAINER_ID, LOCAL_ID)).build();
    ArgumentCaptor<String> clientMachine = ArgumentCaptor.forClass(String.class);
    when(scmBlockLocationProtocol.allocateBlock(anyLong(), anyInt(), any(),
        anyString(), any(ExcludeList.class), clientMachine.capture()))
        .thenReturn(Collections.singletonList(block));

    KeyManager mockKeyManager = mock(KeyManager.class);
    when(mockKeyManager.isSortDatanodesForWriteEnabled()).thenReturn(true);
    when(ozoneManager.getKeyManager()).thenReturn(mockKeyManager);
    when(ozoneManager.getClusterMapAllowNull()).thenReturn(mock(NetworkTopology.class));

    OMAllocateBlockRequest request =
        getOmAllocateBlockRequest(createAllocateBlockRequest());
    List<OmKeyLocationInfo> locations = request.allocateBlock(replicationConfig,
        new ExcludeList(), scmBlockSize, true,
        UserInfo.newBuilder().setRemoteAddress("").build(), ozoneManager);

    assertEquals("", clientMachine.getValue());
    verify(mockKeyManager, never()).sortDatanodesForWrite(any(), anyString(), any());
    assertEquals(1, locations.size());
    // Assert the write order (nodesInOrder), which copyWithNodesInOrder would
    // have changed had OM sorted; it must stay as the original pipeline order.
    assertEquals(nodes, locations.get(0).getPipeline().getNodesInOrder());
  }

  @Test
  public void sortDatanodesForWriteRequiresClientMachine() {
    List<DatanodeDetails> nodes = Arrays.asList(
        MockDatanodeDetails.randomDatanodeDetails(),
        MockDatanodeDetails.randomDatanodeDetails(),
        MockDatanodeDetails.randomDatanodeDetails());
    assertThrows(IllegalArgumentException.class,
        () -> keyManager.sortDatanodesForWrite(nodes, "", mock(NetworkTopology.class)));
  }

  // Like createAllocateBlockRequest, but sets sortDatanodes so preExecute
  // resolves the client address from the RPC context.
  private OMRequest createAllocateBlockRequestWithSort() {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName).setKeyName(keyName)
        .setFactor(((RatisReplicationConfig) replicationConfig).getReplicationFactor())
        .setType(replicationConfig.getReplicationType())
        .setSortDatanodes(true)
        .build();
    AllocateBlockRequest allocateBlockRequest = AllocateBlockRequest.newBuilder()
        .setClientID(clientID).setKeyArgs(keyArgs).build();
    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.AllocateBlock)
        .setClientId(UUID.randomUUID().toString())
        .setAllocateBlockRequest(allocateBlockRequest).build();
  }

  // Run preExecute with a mocked RPC context so UserInfo carries clientAddress,
  // the way an OM RPC handler thread would see it.
  private void preExecuteWithClient(OMAllocateBlockRequest request, String clientAddress) throws Exception {
    InetAddress clientIp = InetAddress.getByAddress(clientAddress, InetAddress.getByName(clientAddress).getAddress());
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    try (MockedStatic<Server> mockedRpcServer = mockStatic(Server.class)) {
      mockedRpcServer.when(Server::getRemoteUser).thenReturn(ugi);
      mockedRpcServer.when(Server::getRemoteIp).thenReturn(clientIp);
      request.preExecute(ozoneManager);
    }
  }

  protected OMRequest createAllocateBlockRequest() {

    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName)
        .setFactor(((RatisReplicationConfig) replicationConfig).getReplicationFactor())
        .setType(replicationConfig.getReplicationType())
        .build();

    AllocateBlockRequest allocateBlockRequest =
        AllocateBlockRequest.newBuilder().setClientID(clientID)
            .setKeyArgs(keyArgs).build();

    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.AllocateBlock)
        .setClientId(UUID.randomUUID().toString())
        .setAllocateBlockRequest(allocateBlockRequest).build();

  }

  protected String addKeyToOpenKeyTable(String volumeName, String bucketName)
          throws Exception {
    OMRequestTestUtils.addKeyToTable(true, volumeName, bucketName,
        keyName, clientID, replicationConfig,
        omMetadataManager);
    return "";
  }
}
