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

package org.apache.hadoop.hdds.scm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamingReadResponse;
import org.apache.hadoop.hdds.scm.StreamingReaderSpi;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

/**
 * Tests for StreamBlockInputStream custom configuration behavior.
 */
public class TestStreamBlockInputStream {

  private static final Duration STREAM_READ_TIMEOUT = Duration.ofSeconds(5);
  private static final Function<BlockID, BlockLocationInfo> NO_REFRESH = b -> null;

  @Test
  public void testCustomStreamReadConfigIsApplied() throws Exception {
    // Arrange: create a config with non-default values
    OzoneClientConfig clientConfig = newStreamReadConfig();
    clientConfig.setStreamReadPreReadSize(64L << 20);
    clientConfig.setStreamReadResponseDataSize(2 << 20);

    // Sanity check
    assertEquals(STREAM_READ_TIMEOUT, clientConfig.getStreamReadTimeout());
    // Create a dummy BlockID for the test
    BlockID blockID = new BlockID(1L, 1L);
    long length = 1024L;
    // Create a mock Pipeline instance.
    Pipeline pipeline = mock(Pipeline.class);

    Token<OzoneBlockTokenIdentifier> token = null;
    // Mock XceiverClientFactory since StreamBlockInputStream requires it in the constructor
    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    // Create a StreamBlockInputStream instance
    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, length, pipeline, token,
        xceiverClientFactory, NO_REFRESH, clientConfig)) {

      // Assert: fields should match config values
      assertEquals(64L << 20, sbis.getPreReadSize());
      assertEquals(2 << 20, sbis.getResponseDataSize());
      assertEquals(STREAM_READ_TIMEOUT, sbis.getReadTimeout());
    }
  }

  @Test
  public void testReleasesStreamPermitAtBlockEof() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    BlockID blockID = new BlockID(1L, 2L);
    byte[] data = new byte[] {1, 2, 3, 4};
    long length = data.length;
    Pipeline pipeline = mockStandalonePipeline();
    XceiverClientGrpc xceiverClient = mockStreamingReadClient(data);
    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class)))
        .thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, length, pipeline, null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {
      ByteBuffer firstRead = ByteBuffer.allocate((int) length - 1);
      int first = sbis.read(firstRead);
      assertEquals(length - 1, first);
      assertEquals(length - 1, sbis.getPos());
      verify(xceiverClient, never()).completeStreamRead();

      int last = sbis.read();
      assertEquals(data[(int) length - 1] & 0xFF, last);
      assertEquals(length, sbis.getPos());
      verify(xceiverClient, times(1)).completeStreamRead();

      // Subsequent reads should return EOF and must not trigger duplicate permit release.
      assertEquals(-1, sbis.read());
      assertEquals(-1, sbis.read());
    }

    verify(xceiverClient, times(1)).completeStreamRead();
  }

  private OzoneClientConfig newStreamReadConfig() {
    OzoneClientConfig clientConfig = new OzoneClientConfig();
    clientConfig.setChecksumVerify(false);
    clientConfig.setStreamReadPreReadSize(0);
    clientConfig.setStreamReadResponseDataSize(1024);
    clientConfig.setStreamReadTimeout(STREAM_READ_TIMEOUT);
    return clientConfig;
  }

  private Pipeline mockStandalonePipeline() throws Exception {
    Pipeline pipeline = mock(Pipeline.class);
    DatanodeDetails datanode = mock(DatanodeDetails.class);

    when(pipeline.getNodes()).thenReturn(Collections.singletonList(datanode));
    when(pipeline.getNodesInOrder()).thenReturn(Collections.singletonList(datanode));
    when(pipeline.getFirstNode()).thenReturn(datanode);
    when(pipeline.getClosestNode()).thenReturn(datanode);
    when(pipeline.getType()).thenReturn(HddsProtos.ReplicationType.STAND_ALONE);
    when(pipeline.getReplicaIndex(datanode)).thenReturn(1);
    when(datanode.getID()).thenReturn(mock(DatanodeID.class));
    when(datanode.getUuidString()).thenReturn("00000000-0000-0000-0000-000000000001");

    return pipeline;
  }

  private XceiverClientGrpc mockStreamingReadClient(byte[] data) throws Exception {
    XceiverClientGrpc xceiverClient = mock(XceiverClientGrpc.class);
    StreamingReadResponse streamingReadResponse = mock(StreamingReadResponse.class);
    ReadBlockResponseProto readBlock = buildReadBlockResponse(data);

    doNothing().when(xceiverClient)
        .streamRead(any(ContainerCommandRequestProto.class),
            any(StreamingReadResponse.class));
    doAnswer(invocation -> {
      StreamingReaderSpi reader = invocation.getArgument(1);
      reader.setStreamingReadResponse(streamingReadResponse);
      reader.onNext(ContainerCommandResponseProto.newBuilder()
          .setCmdType(Type.ReadBlock)
          .setResult(ContainerProtos.Result.SUCCESS)
          .setReadBlock(readBlock)
          .build());
      return null;
    }).when(xceiverClient).initStreamRead(any(BlockID.class), any());

    return xceiverClient;
  }

  private ReadBlockResponseProto buildReadBlockResponse(byte[] data) {
    return ReadBlockResponseProto.newBuilder()
        .setOffset(0)
        .setData(ByteString.copyFrom(data))
        .setChecksumData(ChecksumData.newBuilder()
            .setType(ContainerProtos.ChecksumType.NONE)
            .setBytesPerChecksum(data.length)
            .build())
        .build();
  }
}
