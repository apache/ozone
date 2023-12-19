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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.report.IncrementalReportSender;

import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.SUCCESS;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNKNOWN_BCSID;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.DATANODE_UUID;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getDummyCommandRequestProto;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getPutBlockRequest;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getTestBlockID;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getWriteChunkRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test that KeyValueHandler fails certain operations when the
 * container is unhealthy.
 */
public class TestKeyValueHandlerWithUnhealthyContainer {
  public static final Logger LOG = LoggerFactory.getLogger(TestKeyValueHandlerWithUnhealthyContainer.class);

  @Test
  public void testRead() {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    final ContainerCommandRequestProto request = getDummyCommandRequestProto(Type.ReadContainer);
    final ContainerCommandResponseProto response = handler.handleReadContainer(request, container);
    assertEquals(SUCCESS, response.getResult());
  }

  @Test
  public void testGetBlock() {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    final ContainerCommandRequestProto request = getDummyCommandRequestProto(Type.GetBlock);
    final ContainerCommandResponseProto response = handler.handleGetBlock(request, container);
    assertEquals(UNKNOWN_BCSID, response.getResult());
  }

  @Test
  public void testGetCommittedBlockLength() {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    final ContainerCommandRequestProto request = getDummyCommandRequestProto(Type.GetCommittedBlockLength);
    final ContainerCommandResponseProto response = handler.handleGetCommittedBlockLength(request, container);
    assertEquals(UNKNOWN_BCSID, response.getResult());
  }

  @Test
  public void testReadChunk() {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    try (DispatcherContext ctx = DispatcherContext.getHandleReadChunk()) {
      final ContainerCommandRequestProto request = getDummyCommandRequestProto(Type.ReadChunk);
      final ContainerCommandResponseProto response = handler.handleReadChunk(request, container, ctx);
      assertEquals(UNKNOWN_BCSID, response.getResult());
    }
  }

  @Test
  public void testGetSmallFile() {
    KeyValueContainer container = getMockUnhealthyContainer();
    KeyValueHandler handler = getDummyHandler();

    try (DispatcherContext ctx = DispatcherContext.getHandleGetSmallFile()) {
      final ContainerCommandRequestProto request = getDummyCommandRequestProto(Type.GetSmallFile);
      final ContainerCommandResponseProto response = handler.handleGetSmallFile(request, container, ctx);
      assertEquals(UNKNOWN_BCSID, response.getResult());
    }
  }

  @Test
  void testNPEFromPutBlock() throws IOException {
    KeyValueContainer container = new KeyValueContainer(
        mock(KeyValueContainerData.class),
        new OzoneConfiguration());
    final KeyValueHandler handler = getDummyHandler();

    BlockID blockID = getTestBlockID(1);
    final Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    final ContainerCommandRequestProto writeChunkRequest = getWriteChunkRequest(pipeline, blockID, 123);
    final ContainerCommandRequestProto putBlockRequest = getPutBlockRequest(writeChunkRequest);
    final ContainerCommandResponseProto response = handler.handle(putBlockRequest, container, null);
    assertEquals(CONTAINER_INTERNAL_ERROR, response.getResult());
  }

  @Test
  public void testMarkContainerUnhealthyInFailedVolume() throws IOException {
    final IncrementalReportSender<Container> mockIcrSender = mock(IncrementalReportSender.class);
    KeyValueHandler handler = getDummyHandler(mockIcrSender);
    KeyValueContainerData mockContainerData = mock(KeyValueContainerData.class);
    HddsVolume mockVolume = mock(HddsVolume.class);
    Mockito.when(mockContainerData.getVolume()).thenReturn(mockVolume);
    KeyValueContainer container = new KeyValueContainer(
        mockContainerData, new OzoneConfiguration());

    // When volume is failed, the call to mark the container unhealthy should
    // be ignored.
    Mockito.when(mockVolume.isFailed()).thenReturn(true);
    handler.markContainerUnhealthy(container,
        ContainerTestUtils.getUnhealthyScanResult());
    Mockito.verify(mockIcrSender, Mockito.never()).send(Mockito.any());

    // When volume is healthy, ICR should be sent when container is marked
    // unhealthy.
    Mockito.when(mockVolume.isFailed()).thenReturn(false);
    handler.markContainerUnhealthy(container,
        ContainerTestUtils.getUnhealthyScanResult());
    Mockito.verify(mockIcrSender, Mockito.atMostOnce()).send(Mockito.any());
  }

  // -- Helper methods below.

  private static KeyValueHandler getDummyHandler() {
    return getDummyHandler(mock(IncrementalReportSender.class));
  }

  private static KeyValueHandler getDummyHandler(IncrementalReportSender<Container> sender) {
    return new KeyValueHandler(new OzoneConfiguration(),
        DATANODE_UUID,
        mock(ContainerSet.class),
        mock(MutableVolumeSet.class),
        mock(ContainerMetrics.class), sender);
  }

  private static KeyValueContainer getMockUnhealthyContainer() {
    KeyValueContainerData containerData = mock(KeyValueContainerData.class);
    when(containerData.getState()).thenReturn(ContainerDataProto.State.UNHEALTHY);
    when(containerData.getBlockCommitSequenceId()).thenReturn(100L);
    final ContainerDataProto data = ContainerDataProto.newBuilder().setContainerID(1).build();
    when(containerData.getProtoBufMessage()).thenReturn(data);
    return new KeyValueContainer(containerData, new OzoneConfiguration());
  }
}
