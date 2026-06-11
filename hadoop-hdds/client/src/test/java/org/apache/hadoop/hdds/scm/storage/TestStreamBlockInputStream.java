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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCallStreamObserver;
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
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver = mock(ClientCallStreamObserver.class);
    XceiverClientGrpc xceiverClient = mockStreamingReadClient(data, requestObserver);
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
      verify(requestObserver, times(1)).onCompleted();

      // Subsequent reads should return EOF and must not trigger duplicate permit release.
      assertEquals(-1, sbis.read());
      assertEquals(-1, sbis.read());
    }

    verify(xceiverClient, times(1)).completeStreamRead();
    verify(requestObserver, times(1)).onCompleted();
    verify(requestObserver, never()).cancel(any(), any());
  }

  @Test
  public void testCancelsRequestStreamWhenOnCompletedThrows() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    BlockID blockID = new BlockID(1L, 3L);
    byte[] data = new byte[] {1, 2, 3, 4};
    Pipeline pipeline = mockStandalonePipeline();
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver = mock(ClientCallStreamObserver.class);
    RuntimeException closeFailure = new RuntimeException("close failed");
    doThrow(closeFailure).when(requestObserver).onCompleted();

    XceiverClientGrpc xceiverClient = mockStreamingReadClient(data, requestObserver);
    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class))).thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, data.length, pipeline, null, xceiverClientFactory, NO_REFRESH, clientConfig)) {
      ByteBuffer all = ByteBuffer.allocate(data.length);
      assertEquals(data.length, sbis.read(all));
      assertEquals(data.length, sbis.getPos());
      assertEquals(-1, sbis.read());
    }

    verify(requestObserver, times(1)).onCompleted();
    verify(requestObserver, times(1)).cancel(eq("StreamBlockInputStream closed"), eq(closeFailure));
    verify(xceiverClient, times(1)).completeStreamRead();
  }

  @Test
  public void testCloseDoesNotFailWhenOnCompletedAndCancelThrow() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    BlockID blockID = new BlockID(1L, 4L);
    byte[] data = new byte[] {1, 2, 3, 4};
    Pipeline pipeline = mockStandalonePipeline();
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver = mock(ClientCallStreamObserver.class);
    RuntimeException closeFailure = new RuntimeException("close failed");
    RuntimeException cancelFailure = new RuntimeException("cancel failed");
    doThrow(closeFailure).when(requestObserver).onCompleted();
    doThrow(cancelFailure).when(requestObserver)
        .cancel(eq("StreamBlockInputStream closed"), eq(closeFailure));

    XceiverClientGrpc xceiverClient = mockStreamingReadClient(data, requestObserver);
    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class))).thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, data.length, pipeline, null, xceiverClientFactory, NO_REFRESH, clientConfig)) {
      ByteBuffer all = ByteBuffer.allocate(data.length);
      assertEquals(data.length, sbis.read(all));
      assertEquals(data.length, sbis.getPos());
      assertEquals(-1, sbis.read());
    }

    verify(requestObserver, times(1)).onCompleted();
    verify(requestObserver, times(1)).cancel(eq("StreamBlockInputStream closed"), eq(closeFailure));
    verify(xceiverClient, times(1)).completeStreamRead();
  }

  /**
   * Reproduces Bug 2: poll() checks future.isDone() before draining the queue.
   *
   * When the server delivers a response (onNext) and immediately closes the stream
   * (onCompleted) — which can happen on the same gRPC thread in rapid succession —
   * the item is in the queue and the future is already complete by the time poll()
   * first runs. poll() sees isDone()==true and returns null without ever checking
   * the queue, so readFromQueue() throws NullPointerException on the null proto.
   *
   * This test will FAIL with NullPointerException on the current code and should
   * PASS once the bug is fixed (poll must drain the queue before checking isDone).
   */
  @Test
  public void testPollDoesNotDropQueuedItemWhenFutureCompletesFirst() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    BlockID blockID = new BlockID(1L, 10L);
    byte[] data = {1, 2, 3, 4};
    Pipeline pipeline = mockStandalonePipeline();
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver =
        mock(ClientCallStreamObserver.class);
    StreamingReadResponse streamingReadResponse = mock(StreamingReadResponse.class);
    when(streamingReadResponse.getRequestObserver()).thenReturn(requestObserver);

    // Capture the StreamingReaderSpi during initStreamRead so we can drive
    // its callbacks from the streamRead mock below.
    AtomicReference<StreamingReaderSpi> readerRef = new AtomicReference<>();
    XceiverClientGrpc xceiverClient = mock(XceiverClientGrpc.class);
    doAnswer(inv -> {
      StreamingReaderSpi reader = inv.getArgument(1);
      reader.setStreamingReadResponse(streamingReadResponse);
      readerRef.set(reader);
      return null;
    }).when(xceiverClient).initStreamRead(any(BlockID.class), any());

    // Simulate the race: when the client sends a ReadBlock request, the server
    // responds with data (onNext) and closes the stream (onCompleted) before
    // poll() has had a chance to run — both callbacks fire on the same call stack
    // before streamRead() returns. This means when poll() is entered, the queue
    // already has the response item AND future.isDone() is already true.
    // poll() checks isDone() first and returns null, dropping the queued item.
    doAnswer(inv -> {
      StreamingReaderSpi reader = readerRef.get();
      reader.onNext(ContainerCommandResponseProto.newBuilder()
          .setCmdType(Type.ReadBlock)
          .setResult(ContainerProtos.Result.SUCCESS)
          .setReadBlock(buildReadBlockResponse(data))
          .build());
      reader.onCompleted(); // future is now done; item is already in the queue
      return null;
    }).when(xceiverClient).streamRead(any(), any());

    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class)))
        .thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, data.length, pipeline, null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {

      ByteBuffer buf = ByteBuffer.allocate(data.length);
      // With the bug: poll() returns null (future done, queue unchecked) and
      // readFromQueue() throws NullPointerException.
      // After the fix: all 4 bytes are returned successfully.
      assertDoesNotThrow(() -> sbis.read(buf), "should not NPE when onCompleted fires before poll");
      assertEquals(data.length, buf.position(), "all bytes should be read");
    }
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

  private XceiverClientGrpc mockStreamingReadClient(byte[] data,
      ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver) throws Exception {
    XceiverClientGrpc xceiverClient = mock(XceiverClientGrpc.class);
    StreamingReadResponse streamingReadResponse = mock(StreamingReadResponse.class);
    ReadBlockResponseProto readBlock = buildReadBlockResponse(data);
    when(streamingReadResponse.getRequestObserver()).thenReturn(requestObserver);

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

  /**
   * When the server delivers multiple responses plus onCompleted() inside a
   * single streamRead() call (all on the same call stack), the first response
   * is consumed correctly, but by the time read() is invoked again for the
   * second chunk, future.isDone() is already true. read() sees isDone() and
   * returns null immediately without checking the queue, so the second (and
   * any further) queued responses are silently dropped.
   */
  @Test
  public void testReadDoesNotDropQueuedItemsWhenFutureIsDoneOnSecondCall() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    BlockID blockID = new BlockID(1L, 11L);
    byte[] firstChunk = {1, 2, 3, 4};
    byte[] secondChunk = {5, 6, 7, 8};
    long length = firstChunk.length + secondChunk.length; // 8 bytes total

    Pipeline pipeline = mockStandalonePipeline();
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver =
        mock(ClientCallStreamObserver.class);
    StreamingReadResponse streamingReadResponse = mock(StreamingReadResponse.class);
    when(streamingReadResponse.getRequestObserver()).thenReturn(requestObserver);

    AtomicReference<StreamingReaderSpi> readerRef = new AtomicReference<>();
    XceiverClientGrpc xceiverClient = mock(XceiverClientGrpc.class);
    doAnswer(inv -> {
      StreamingReaderSpi reader = inv.getArgument(1);
      reader.setStreamingReadResponse(streamingReadResponse);
      readerRef.set(reader);
      return null;
    }).when(xceiverClient).initStreamRead(any(BlockID.class), any());

    // Server delivers both 4-byte chunks plus onCompleted() in one synchronous
    // call. After streamRead() returns: queue=[chunk1, chunk2], isDone=true.
    // read() correctly returns chunk1 on the first call, but on the second call
    // it sees isDone()==true and returns null before draining chunk2.
    doAnswer(inv -> {
      StreamingReaderSpi reader = readerRef.get();
      reader.onNext(buildResponseProto(firstChunk, 0));
      reader.onNext(buildResponseProto(secondChunk, firstChunk.length));
      reader.onCompleted(); // future done; both items still in queue
      return null;
    }).when(xceiverClient).streamRead(any(), any());

    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class)))
        .thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, length, pipeline, null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {
      ByteBuffer buf = ByteBuffer.allocate((int) length);
      // With the bug: read() returns null on the second call (isDone is true),
      // so only 4 bytes are read and buf.position() == 4.
      // After the fix: all 8 bytes are read and buf.position() == 8.
      int bytesRead = sbis.read(buf);
      assertEquals(length, bytesRead, "expected all bytes to be read");
      assertEquals(length, buf.position(), "buffer position should be at end of block");
    }
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

  private ContainerCommandResponseProto buildResponseProto(byte[] data, long offset) {
    return ContainerCommandResponseProto.newBuilder()
        .setCmdType(Type.ReadBlock)
        .setResult(ContainerProtos.Result.SUCCESS)
        .setReadBlock(ReadBlockResponseProto.newBuilder()
            .setOffset(offset)
            .setData(ByteString.copyFrom(data))
            .setChecksumData(ChecksumData.newBuilder()
                .setType(ContainerProtos.ChecksumType.NONE)
                .setBytesPerChecksum(data.length)
                .build())
            .build())
        .build();
  }
}
