/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.report.IncrementalReportSender;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;

import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_UNHEALTHY;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.SUCCESS;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNKNOWN_BCSID;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.DATANODE_UUID;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getDummyCommandRequestProto;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getPutBlockRequest;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getTestBlockID;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getWriteChunkRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test that KeyValueHandler fails certain operations when the
 * container is unhealthy.
 */
public class TestKeyValueHandlerWithUnhealthyContainer {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestKeyValueHandlerWithUnhealthyContainer.class);

  @TempDir
  private File tempDir;

  private IncrementalReportSender<Container> mockIcrSender;

  @BeforeEach
  public void init() {
    mockIcrSender = mock(IncrementalReportSender.class);
  }

  @Test
  public void testRead() {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleReadContainer(
            getDummyCommandRequestProto(ContainerProtos.Type.ReadContainer),
            container);
    assertEquals(SUCCESS, response.getResult());
  }

  @Test
  public void testGetBlock() {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleGetBlock(
            getDummyCommandRequestProto(ContainerProtos.Type.GetBlock),
            container);
    assertEquals(UNKNOWN_BCSID, response.getResult());
  }

  private static Stream<Arguments> getAllClientVersions() {
    return Arrays.stream(ClientVersion.values()).flatMap(client -> IntStream.range(0, 6)
        .mapToObj(rid -> Arguments.of(client, rid)));
  }


  @ParameterizedTest
  @MethodSource("getAllClientVersions")
  public void testGetBlockWithReplicaIndexMismatch(ClientVersion clientVersion, int replicaIndex) {
    KeyValueContainer container = getMockContainerWithReplicaIndex(replicaIndex);
    KeyValueHandler handler = getDummyHandler();
    for (int rid = 0; rid <= 5; rid++) {
      ContainerProtos.ContainerCommandResponseProto response =
          handler.handleGetBlock(
              getDummyCommandRequestProto(clientVersion, ContainerProtos.Type.GetBlock, rid),
              container);
      assertEquals((replicaIndex > 0 && rid != replicaIndex && clientVersion.toProtoValue() >=
              ClientVersion.EC_REPLICA_INDEX_REQUIRED_IN_BLOCK_REQUEST.toProtoValue()) ?
              ContainerProtos.Result.CONTAINER_NOT_FOUND : UNKNOWN_BCSID,
          response.getResult());
    }

  }

  @Test
  public void testGetCommittedBlockLength() {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleGetCommittedBlockLength(
            getDummyCommandRequestProto(
                ContainerProtos.Type.GetCommittedBlockLength),
            container);
    assertEquals(UNKNOWN_BCSID, response.getResult());
  }

  @Test
  public void testReadChunk() {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleReadChunk(
            getDummyCommandRequestProto(
                ContainerProtos.Type.ReadChunk),
            container, null);
    assertEquals(UNKNOWN_BCSID, response.getResult());
  }

  @ParameterizedTest
  @MethodSource("getAllClientVersions")
  public void testReadChunkWithReplicaIndexMismatch(ClientVersion clientVersion, int replicaIndex) {
    KeyValueContainer container = getMockContainerWithReplicaIndex(replicaIndex);
    KeyValueHandler handler = getDummyHandler();
    for (int rid = 0; rid <= 5; rid++) {
      ContainerProtos.ContainerCommandResponseProto response =
          handler.handleReadChunk(getDummyCommandRequestProto(clientVersion, ContainerProtos.Type.ReadChunk, rid),
              container, null);
      assertEquals((replicaIndex > 0 && rid != replicaIndex &&
              clientVersion.toProtoValue() >= ClientVersion.EC_REPLICA_INDEX_REQUIRED_IN_BLOCK_REQUEST.toProtoValue()) ?
              ContainerProtos.Result.CONTAINER_NOT_FOUND : UNKNOWN_BCSID,
          response.getResult());
    }

  }

  @Test
  public void testFinalizeBlock() {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleFinalizeBlock(
            getDummyCommandRequestProto(
                ContainerProtos.Type.FinalizeBlock),
            container);
    assertEquals(CONTAINER_UNHEALTHY, response.getResult());
  }

  @Test
  public void testGetSmallFile() {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleGetSmallFile(
            getDummyCommandRequestProto(
                ContainerProtos.Type.GetSmallFile),
            container);
    assertEquals(UNKNOWN_BCSID, response.getResult());
  }

  @Test
  void testNPEFromPutBlock() throws IOException {
    KeyValueContainer container = new KeyValueContainer(
        mock(KeyValueContainerData.class),
        new OzoneConfiguration());
    KeyValueHandler subject = getDummyHandler();

    BlockID blockID = getTestBlockID(1);
    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        getWriteChunkRequest(MockPipeline.createSingleNodePipeline(),
            blockID, 123);
    ContainerProtos.ContainerCommandResponseProto response =
        subject.handle(
            getPutBlockRequest(writeChunkRequest),
            container, null);
    assertEquals(CONTAINER_INTERNAL_ERROR, response.getResult());
  }

  @Test
  public void testMarkContainerUnhealthyInFailedVolume() throws IOException {
    KeyValueHandler handler = getDummyHandler();
    KeyValueContainerData mockContainerData = mock(KeyValueContainerData.class);
    HddsVolume mockVolume = mock(HddsVolume.class);
    when(mockContainerData.getVolume()).thenReturn(mockVolume);
    when(mockContainerData.getMetadataPath()).thenReturn(tempDir.getAbsolutePath());
    KeyValueContainer container = new KeyValueContainer(
        mockContainerData, new OzoneConfiguration());

    // When volume is failed, the call to mark the container unhealthy should
    // be ignored.
    when(mockVolume.isFailed()).thenReturn(true);
    handler.markContainerUnhealthy(container,
        ContainerTestUtils.getUnhealthyScanResult());
    verify(mockIcrSender, never()).send(any());

    // When volume is healthy, ICR should be sent when container is marked
    // unhealthy.
    when(mockVolume.isFailed()).thenReturn(false);
    handler.markContainerUnhealthy(container,
        ContainerTestUtils.getUnhealthyScanResult());
    verify(mockIcrSender, atMostOnce()).send(any());
  }

  // -- Helper methods below.

  private KeyValueHandler getDummyHandler() {
    DatanodeDetails dnDetails = DatanodeDetails.newBuilder()
        .setUuid(UUID.fromString(DATANODE_UUID))
        .setHostName("dummyHost")
        .setIpAddress("1.2.3.4")
        .build();
    DatanodeStateMachine stateMachine = mock(DatanodeStateMachine.class);
    when(stateMachine.getDatanodeDetails()).thenReturn(dnDetails);

    return new KeyValueHandler(
        new OzoneConfiguration(),
        stateMachine.getDatanodeDetails().getUuidString(),
        mock(ContainerSet.class),
        mock(MutableVolumeSet.class),
        mock(ContainerMetrics.class), mockIcrSender);
  }

  private KeyValueContainer getMockUnhealthyContainer() {
    KeyValueContainerData containerData = mock(KeyValueContainerData.class);
    when(containerData.getState()).thenReturn(
        ContainerProtos.ContainerDataProto.State.UNHEALTHY);
    when(containerData.getBlockCommitSequenceId()).thenReturn(100L);
    when(containerData.getProtoBufMessage()).thenReturn(ContainerProtos
        .ContainerDataProto.newBuilder().setContainerID(1).build());
    return new KeyValueContainer(containerData, new OzoneConfiguration());
  }

  private KeyValueContainer getMockContainerWithReplicaIndex(int replicaIndex) {
    KeyValueContainerData containerData = mock(KeyValueContainerData.class);
    when(containerData.getState()).thenReturn(
        ContainerProtos.ContainerDataProto.State.CLOSED);
    when(containerData.getBlockCommitSequenceId()).thenReturn(100L);
    when(containerData.getReplicaIndex()).thenReturn(replicaIndex);
    when(containerData.getProtoBufMessage()).thenReturn(ContainerProtos
        .ContainerDataProto.newBuilder().setContainerID(1).build());
    return new KeyValueContainer(containerData, new OzoneConfiguration());
  }
}
