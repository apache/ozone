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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.ratis.client.api.DataStreamApi;
import org.apache.ratis.client.api.DataStreamInput;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuf;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamReplyHeader;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.util.ReferenceCountedObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.stubbing.Answer;

/**
 * Tests for {@link RatisDataStreamBlockInputStream}.
 */
class TestRatisDataStreamBlockInputStream {
  private static final long ONE_GB = 1L << 30;
  private static final long READ_AHEAD_REQUEST = 4L << 20;
  private static final byte[] DATA = {1, 2, 3, 4};

  private final BlockID blockID = new BlockID(1L, 1L);
  private final DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
  private final DatanodeDetails dn2 = MockDatanodeDetails.randomDatanodeDetails();
  private final DatanodeDetails dn3 = MockDatanodeDetails.randomDatanodeDetails();
  private final PipelineID pipelineID = PipelineID.randomId();

  @Test
  void requestLengthReadsAheadOnlyWhenSequential() {
    final int smallRead = 4 << 10;

    assertEquals(smallRead,
        RatisDataStreamBlockInputStream.requestLength(ONE_GB, 0, smallRead, false, READ_AHEAD_REQUEST));
    assertEquals(READ_AHEAD_REQUEST,
        RatisDataStreamBlockInputStream.requestLength(ONE_GB, smallRead, smallRead, true, READ_AHEAD_REQUEST));
    assertEquals(2 * READ_AHEAD_REQUEST, RatisDataStreamBlockInputStream.requestLength(
        ONE_GB, smallRead, (int) (2 * READ_AHEAD_REQUEST), true, READ_AHEAD_REQUEST));
    assertEquals(1024,
        RatisDataStreamBlockInputStream.requestLength(ONE_GB, ONE_GB - 1024, smallRead, true, READ_AHEAD_REQUEST));
  }

  /**
   * Once a reader has read the first read-ahead size in a row, it keeps one read-ahead request of half the window in
   * flight after the one it consumes, so it never has more than the window requested ahead, and the requests cover the
   * block without overlap.
   */
  @Test
  void sequentialReadPipelinesBoundedRequests() throws Exception {
    final byte[] block = block(64);
    final Streams streams = new Streams(block);
    final XceiverClientFactory factory = factory(invocation -> streams.client(invocation.getArgument(0)));

    final ByteBuffer out = ByteBuffer.allocate(block.length);
    try (RatisDataStreamBlockInputStream in = newStream(pipeline(dn1, dn2, dn3), factory, block.length, 16)) {
      final ByteBuffer chunk = ByteBuffer.allocate(4);
      while (in.read(chunk) > 0) {
        chunk.flip();
        out.put(chunk);
        chunk.clear();
      }
    }

    assertArrayEquals(block, out.array());
    assertEquals(Arrays.asList("0+4", "4+4", "8+8", "16+8", "24+8", "32+8", "40+8", "48+8", "56+8"),
        streams.ranges());
    assertEquals(2, streams.maxOpen);
    assertEquals(0, streams.open);
  }

  /** A seek closes the requests in flight, and the first read after it requests exactly what the caller asked for. */
  @Test
  void seekClosesReadAheadAndReadsExactly() throws Exception {
    final byte[] block = block(64);
    final Streams streams = new Streams(block);
    final XceiverClientFactory factory = factory(invocation -> streams.client(invocation.getArgument(0)));

    try (RatisDataStreamBlockInputStream in = newStream(pipeline(dn1, dn2, dn3), factory, block.length, 16)) {
      final ByteBuffer chunk = ByteBuffer.allocate(4);
      for (int i = 0; i < 3; i++) {
        chunk.clear();
        in.read(chunk);
      }
      assertEquals(2, streams.open);

      in.seek(40);
      assertEquals(0, streams.open);
      chunk.clear();
      in.read(chunk);
      assertArrayEquals(Arrays.copyOfRange(block, 40, 44), chunk.array());
    }

    assertEquals(Arrays.asList("0+4", "4+4", "8+8", "16+8", "40+4"), streams.ranges());
  }

  /**
   * A reader that asks for at least half the window per read (like Parquet's 8 MB buffers) gets exactly what it asks
   * for, including a smaller last read, since read-ahead would be wasted at its next seek. After the seek, a reader of
   * small pieces gets read-ahead again.
   */
  @Test
  void largeReadsAreNotReadAhead() throws Exception {
    final byte[] block = block(64);
    final Streams streams = new Streams(block);
    final XceiverClientFactory factory = factory(invocation -> streams.client(invocation.getArgument(0)));

    try (RatisDataStreamBlockInputStream in = newStream(pipeline(dn1, dn2, dn3), factory, block.length, 16)) {
      final ByteBuffer large = ByteBuffer.allocate(8);
      in.read(large);
      large.clear();
      in.read(large);
      final ByteBuffer small = ByteBuffer.allocate(4);
      in.read(small);
      assertArrayEquals(Arrays.copyOfRange(block, 16, 20), small.array());
      assertEquals(1, streams.maxOpen);

      in.seek(32);
      for (int i = 0; i < 3; i++) {
        small.clear();
        in.read(small);
      }
      assertArrayEquals(Arrays.copyOfRange(block, 40, 44), small.array());
    }

    assertEquals(Arrays.asList("0+8", "8+8", "16+4", "32+4", "36+4", "40+8", "48+8"), streams.ranges());
  }

  /**
   * Read-ahead starts only once the reader has read 128 KB in a row, so a short run of small reads before a seek
   * requests only what it reads. It then starts at 128 KB and doubles with each request the reader consumes, up to half
   * the window, so a long scan soon reads ahead fully.
   */
  @Test
  void readAheadStartsSmallAndDoubles() throws Exception {
    final int kb = 1 << 10;
    final byte[] block = block(1088 * kb);
    final Streams streams = new Streams(block);
    final XceiverClientFactory factory = factory(invocation -> streams.client(invocation.getArgument(0)));

    try (RatisDataStreamBlockInputStream in = newStream(pipeline(dn1, dn2, dn3), factory, block.length, 1024 * kb)) {
      final ByteBuffer small = ByteBuffer.allocate(4 * kb);
      for (int i = 0; i < 3; i++) {
        small.clear();
        in.read(small);
      }
      in.seek(0);
      final ByteBuffer out = ByteBuffer.allocate(block.length);
      final ByteBuffer chunk = ByteBuffer.allocate(64 * kb);
      while (in.read(chunk) > 0) {
        chunk.flip();
        out.put(chunk);
        chunk.clear();
      }
      assertArrayEquals(block, out.array());
    }

    assertEquals(Arrays.asList(
        // three 4 KB reads, then a seek: no read-ahead
        "0+4096", "4096+4096", "8192+4096",
        // 64 KB reads to the end of the block: read-ahead starts after 128 KB and doubles up to 512 KB
        "0+65536", "65536+65536", "131072+131072", "262144+131072", "393216+262144", "655360+458752"),
        streams.ranges());
  }

  /**
   * Only requests read to their end count toward starting read-ahead, so a single read spanning several replies does
   * not start it halfway through; a read that continues right after it does.
   */
  @Test
  void readAheadStartsAfterWholeRequests() throws Exception {
    final int kb = 1 << 10;
    final byte[] block = block(1088 * kb);
    final Streams streams = new Streams(block, 1, 64 * kb);
    final XceiverClientFactory factory = factory(invocation -> streams.client(invocation.getArgument(0)));

    try (RatisDataStreamBlockInputStream in = newStream(pipeline(dn1, dn2, dn3), factory, block.length, 1024 * kb)) {
      final ByteBuffer large = ByteBuffer.allocate(256 * kb);
      assertEquals(large.capacity(), in.read(large));
      assertEquals(Collections.singletonList("0+262144"), streams.ranges());

      final ByteBuffer next = ByteBuffer.allocate(4 * kb);
      in.read(next);
      assertArrayEquals(Arrays.copyOfRange(block, 256 * kb, 260 * kb), next.array());
    }

    assertEquals(Arrays.asList("0+262144", "262144+131072", "393216+131072"), streams.ranges());
  }

  /**
   * A reply covers whole checksum intervals around the requested range, so a reader stepping back or forward within it
   * keeps reading from it without a new request; a seek outside it requests again. This holds also for a reply
   * received in many buffers, back and forth between them.
   */
  @ParameterizedTest
  @ValueSource(ints = {Integer.MAX_VALUE, 3})
  void seekWithinTheBufferedReplyReusesIt(int bufferSize) throws Exception {
    final byte[] block = block(64);
    final Streams streams = new Streams(block, 16, Integer.MAX_VALUE);
    streams.bufferSize = bufferSize;
    final XceiverClientFactory factory = factory(invocation -> streams.client(invocation.getArgument(0)));

    try (RatisDataStreamBlockInputStream in = newStream(pipeline(dn1, dn2, dn3), factory, block.length, 16)) {
      in.seek(20);
      assertEquals(block[20], in.read());
      in.seek(17);
      assertEquals(block[17], in.read());
      in.seek(30);
      assertEquals(block[30], in.read());
      assertEquals(Collections.singletonList("20+1"), streams.ranges());

      in.seek(40);
      assertEquals(block[40], in.read());
    }

    assertEquals(Arrays.asList("20+1", "40+1"), streams.ranges());
  }

  /**
   * Netty decodes a reply that spans many reads into as many buffers. The stream reads the data from them without
   * merging them, and verifies the checksums across them, also when the header, the metadata or a checksum window is
   * split between two.
   */
  @ParameterizedTest
  @ValueSource(ints = {1, 3, 7, 64})
  void readsReplyInManyBuffers(int bufferSize) throws Exception {
    final byte[] block = block(256);
    final Streams streams = new Streams(block, 16, 64);
    streams.bufferSize = bufferSize;
    streams.chunks = chunks(block, 16);
    final XceiverClientFactory factory = factory(invocation -> streams.client(invocation.getArgument(0)));

    final ByteBuffer out = ByteBuffer.allocate(block.length);
    try (RatisDataStreamBlockInputStream in = newStream(pipeline(dn1, dn2, dn3), factory, block.length, 128, true)) {
      final ByteBuffer chunk = ByteBuffer.allocate(5);
      while (in.read(chunk) > 0) {
        chunk.flip();
        out.put(chunk);
        chunk.clear();
      }
    }

    assertArrayEquals(block, out.array());
  }

  @Test
  void detectsCorruptionInReplyInManyBuffers() throws Exception {
    final byte[] block = block(64);
    final byte[] corrupt = block.clone();
    corrupt[37]++;
    final Streams streams = new Streams(corrupt, 16, 64);
    streams.bufferSize = 3;
    streams.chunks = chunks(block, 16);
    final XceiverClientFactory factory = factory(invocation -> streams.client(invocation.getArgument(0)));

    try (RatisDataStreamBlockInputStream in = newStream(pipeline(dn1, dn2, dn3), factory, block.length, 128, true)) {
      assertThrows(OzoneChecksumException.class, () -> in.read(ByteBuffer.allocate(block.length)));
    }
  }

  /** Parsing a reply leaves its data in the buffers it was received in: it does not copy it. */
  @Test
  void parsedDataIsInTheReceivedBuffers() throws Exception {
    final byte[] data = block(64);
    final ByteBuffer frame = frame(0, data, Collections.emptyList());
    final List<ByteBuffer> parsed = ReadBlockData.parse(dataReply(frame, 7)).getData();

    final byte[] received = frame.array();
    for (int j = frame.limit() - data.length; j < frame.limit(); j++) {
      received[j] = (byte) ~received[j];
    }
    int i = 0;
    for (ByteBuffer buffer : parsed) {
      while (buffer.hasRemaining()) {
        assertEquals((byte) ~data[i++], buffer.get());
      }
    }
    assertEquals(data.length, i);
  }

  /** A single-byte read returns the byte as 0 to 255: 0xFF must not look like the end of the stream. */
  @Test
  void readReturnsUnsignedByte() throws Exception {
    final byte[] block = block(256);
    final Streams streams = new Streams(block);
    final XceiverClientFactory factory = factory(invocation -> streams.client(invocation.getArgument(0)));

    try (RatisDataStreamBlockInputStream in = newStream(pipeline(dn1, dn2, dn3), factory, block.length, 16)) {
      in.seek(254);
      assertEquals(254, in.read());
      assertEquals(255, in.read());
      assertEquals(-1, in.read());
    }
  }

  /**
   * A request is closed as soon as its data is consumed, without waiting for its terminal reply: the reply would
   * otherwise keep the stream, and the netty buffer it was decoded from, until the next read or seek.
   */
  @Test
  void closesRequestOnceItsDataIsConsumed() throws Exception {
    final Streams streams = new Streams(DATA);
    final XceiverClientFactory factory = factory(invocation -> streams.client(invocation.getArgument(0)));

    try (RatisDataStreamBlockInputStream in = newStream(pipeline(dn1, dn2, dn3), factory, DATA.length, 16)) {
      assertArrayEquals(DATA, readAll(in));
      assertEquals(0, streams.open);
      verify(streams.inputs.get(0), times(1)).readAsync();
    }
  }

  /**
   * The request must name the datanode the client streams to (the closest node of the client's pipeline), even
   * when the block's pipeline orders the nodes differently; otherwise a closed-container read is not resolved.
   */
  @Test
  void requestNamesTheDatanodeTheClientStreamsTo() throws Exception {
    final Pipeline blockPipeline = pipeline(dn2, dn1, dn3);
    final Pipeline clientPipeline = pipeline(dn1, dn2, dn3);
    final List<ByteBuffer> headers = new ArrayList<>();
    final XceiverClientRatis client = client(clientPipeline, headers, dataReply());
    final XceiverClientFactory factory = factory(invocation -> client);

    try (RatisDataStreamBlockInputStream in = newStream(blockPipeline, factory)) {
      assertArrayEquals(DATA, readAll(in));
    }

    assertEquals(1, headers.size());
    assertEquals(dn1.getUuidString(), toRequest(headers.get(0)).getDatanodeUuid());
  }

  /** A follower refuses reads of an open container; the read must be retried on the suggested leader. */
  @Test
  void redirectsToSuggestedLeader() throws Exception {
    final Pipeline blockPipeline = pipeline(dn1, dn2, dn3);
    final List<ByteBuffer> followerHeaders = new ArrayList<>();
    final List<ByteBuffer> leaderHeaders = new ArrayList<>();
    final List<XceiverClientRatis> followers = new ArrayList<>();
    final XceiverClientFactory factory = factory(invocation -> {
      final Pipeline p = invocation.getArgument(0);
      if (p.getClosestNode().equals(dn3)) {
        return client(p, leaderHeaders, dataReply());
      }
      final XceiverClientRatis follower = client(p, followerHeaders, notLeaderReply(p.getClosestNode(), dn3));
      followers.add(follower);
      return follower;
    });

    try (RatisDataStreamBlockInputStream in = newStream(blockPipeline, factory)) {
      assertArrayEquals(DATA, readAll(in));
    }

    assertEquals(1, followerHeaders.size());
    assertEquals(dn1.getUuidString(), toRequest(followerHeaders.get(0)).getDatanodeUuid());
    assertEquals(1, leaderHeaders.size());
    assertEquals(dn3.getUuidString(), toRequest(leaderHeaders.get(0)).getDatanodeUuid());
    assertEquals(1, followers.size());
    verify(factory).releaseClient(followers.get(0), false, true);
  }

  /** A factory that answers every way of acquiring a read client with {@code answer}. */
  private static XceiverClientFactory factory(Answer<XceiverClientRatis> answer) throws Exception {
    final XceiverClientFactory factory = mock(XceiverClientFactory.class);
    when(factory.acquireClient(any(Pipeline.class), eq(true))).thenAnswer(answer);
    when(factory.acquireClientForReadData(any(Pipeline.class))).thenAnswer(answer);
    return factory;
  }

  private Pipeline pipeline(DatanodeDetails... nodesInOrder) {
    return Pipeline.newBuilder()
        .setId(pipelineID)
        .setState(Pipeline.PipelineState.CLOSED)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setNodes(Arrays.asList(dn1, dn2, dn3))
        .setNodesInOrder(Arrays.asList(nodesInOrder))
        .build();
  }

  private RatisDataStreamBlockInputStream newStream(Pipeline pipeline, XceiverClientFactory factory)
      throws IOException {
    return newStream(pipeline, factory, DATA.length, new OzoneClientConfig().getRatisStreamReadWindowSize());
  }

  private RatisDataStreamBlockInputStream newStream(Pipeline pipeline, XceiverClientFactory factory,
      long blockLength, long window) throws IOException {
    return newStream(pipeline, factory, blockLength, window, false);
  }

  private RatisDataStreamBlockInputStream newStream(Pipeline pipeline, XceiverClientFactory factory,
      long blockLength, long window, boolean verifyChecksum) throws IOException {
    final OzoneClientConfig config = new OzoneClientConfig();
    config.setChecksumVerify(verifyChecksum);
    config.setRatisStreamReadWindowSize(window);
    return new RatisDataStreamBlockInputStream(blockID, blockLength, pipeline, null, factory, config);
  }

  private static byte[] block(int length) {
    final byte[] block = new byte[length];
    for (int i = 0; i < length; i++) {
      block[i] = (byte) i;
    }
    return block;
  }

  /**
   * Read-only streams that each serve the requested range of a block, then a successful terminal reply. Like a
   * datanode, they can widen the range to multiples of {@code align} bytes and split it into replies of
   * {@code replySize} bytes.
   */
  private final class Streams {
    private final byte[] block;
    private final int align;
    private final int replySize;
    private final List<ContainerCommandRequestProto> requests = new ArrayList<>();
    private final List<DataStreamInput> inputs = new ArrayList<>();
    private int open;
    private int maxOpen;
    /** The size of the buffers each reply is received in. */
    private int bufferSize = Integer.MAX_VALUE;
    /** The chunks, with their checksums, that each reply carries. */
    private List<ChunkInfo> chunks = Collections.emptyList();

    Streams(byte[] block) {
      this(block, 1, Integer.MAX_VALUE);
    }

    Streams(byte[] block, int align, int replySize) {
      this.block = block;
      this.align = align;
      this.replySize = replySize;
    }

    XceiverClientRatis client(Pipeline pipeline) throws Exception {
      final DataStreamApi api = mock(DataStreamApi.class);
      when(api.streamReadOnly(any(ByteBuffer.class))).thenAnswer(invocation -> newInput(invocation.getArgument(0)));
      final XceiverClientRatis client = mock(XceiverClientRatis.class);
      when(client.getPipeline()).thenReturn(pipeline);
      when(client.getReadStreamApi()).thenReturn(api);
      return client;
    }

    private DataStreamInput newInput(ByteBuffer header) throws Exception {
      final ContainerCommandRequestProto request = toRequest(header);
      requests.add(request);
      final int requested = Math.toIntExact(request.getReadBlock().getOffset());
      final int offset = requested - requested % align;
      final int end = Math.min(block.length,
          (requested + Math.toIntExact(request.getReadBlock().getLength()) + align - 1) / align * align);
      final Deque<DataStreamReply> replies = new ArrayDeque<>();
      for (int o = offset; o < end;) {
        final int length = Math.min(end - o, replySize);
        replies.add(dataReply(o, Arrays.copyOfRange(block, o, o + length), chunks, bufferSize));
        o += length;
      }
      replies.add(successReply());
      final DataStreamInput input = mock(DataStreamInput.class);
      when(input.readAsync()).thenAnswer(invocation -> CompletableFuture.completedFuture(retained(replies.remove())));
      doAnswer(invocation -> {
        open--;
        return null;
      }).when(input).close();
      inputs.add(input);
      maxOpen = Math.max(maxOpen, ++open);
      return input;
    }

    List<String> ranges() {
      return requests.stream()
          .map(r -> r.getReadBlock().getOffset() + "+" + r.getReadBlock().getLength())
          .collect(Collectors.toList());
    }
  }

  private static byte[] readAll(RatisDataStreamBlockInputStream in) throws Exception {
    final ByteBuffer buffer = ByteBuffer.allocate(DATA.length);
    assertEquals(DATA.length, in.read(buffer));
    return buffer.array();
  }

  /** A client of {@code pipeline} whose read-only stream returns {@code replies} and records its request header. */
  private static XceiverClientRatis client(Pipeline pipeline, List<ByteBuffer> headers, DataStreamReply... replies)
      throws IOException {
    final Deque<DataStreamReply> queue = new ArrayDeque<>(Arrays.asList(replies));
    final DataStreamInput input = mock(DataStreamInput.class);
    when(input.readAsync()).thenAnswer(invocation -> CompletableFuture.completedFuture(retained(queue.remove())));
    final DataStreamApi api = mock(DataStreamApi.class);
    when(api.streamReadOnly(any(ByteBuffer.class))).thenAnswer(invocation -> {
      headers.add(invocation.getArgument(0));
      return input;
    });
    final XceiverClientRatis client = mock(XceiverClientRatis.class);
    when(client.getPipeline()).thenReturn(pipeline);
    when(client.getReadStreamApi()).thenReturn(api);
    return client;
  }

  private static ReferenceCountedObject<DataStreamReply> retained(DataStreamReply reply) {
    final ReferenceCountedObject<DataStreamReply> ref =
        ReferenceCountedObject.<DataStreamReply>newBuilder().setValue(reply).build();
    ref.retain();
    return ref;
  }

  private DataStreamReply dataReply() {
    return dataReply(0, DATA);
  }

  private static DataStreamReply dataReply(long offset, byte[] data) {
    return dataReply(offset, data, Collections.emptyList(), Integer.MAX_VALUE);
  }

  /** A data reply carrying {@code chunks}, received in buffers of {@code bufferSize} bytes. */
  private static DataStreamReply dataReply(long offset, byte[] data, List<ChunkInfo> chunks, int bufferSize) {
    return dataReply(frame(offset, data, chunks), bufferSize);
  }

  /** The metadata length, the metadata and the data of a data reply, as a datanode sends them. */
  private static ByteBuffer frame(long offset, byte[] data, List<ChunkInfo> chunks) {
    final ContainerCommandResponseProto response = ContainerCommandResponseProto.newBuilder()
        .setCmdType(Type.ReadBlock)
        .setResult(Result.SUCCESS)
        .setReadBlock(ReadBlockResponseProto.newBuilder()
            .setOffset(offset)
            .setData(ByteString.EMPTY)
            .addAllChunkInfoList(chunks))
        .build();
    final byte[] metadata = response.toByteArray();
    final ByteBuffer frame = ByteBuffer.allocate(Integer.BYTES + metadata.length + data.length);
    frame.putInt(metadata.length).put(metadata).put(data).flip();
    return frame;
  }

  /** A data reply of {@code frame}, received in buffers of {@code bufferSize} bytes. */
  private static DataStreamReply dataReply(ByteBuffer frame, int bufferSize) {
    if (bufferSize >= frame.remaining()) {
      return reply(DataStreamPacketHeaderProto.Type.STREAM_DATA, frame, true);
    }
    final List<ByteBuf> buffers = new ArrayList<>();
    for (int i = 0; i < frame.limit(); i += bufferSize) {
      buffers.add(Unpooled.wrappedBuffer(frame.array(), i, Math.min(bufferSize, frame.limit() - i)));
    }
    final ByteBuf buf = Unpooled.wrappedBuffer(buffers.toArray(new ByteBuf[0]));
    return DataStreamReplyByteBuf.newBuilder()
        .setDataStreamReplyHeader(new DataStreamReplyHeader(ClientId.randomId(),
            DataStreamPacketHeaderProto.Type.STREAM_DATA, 1, 0, buf.readableBytes(), 0, true, Collections.emptyList()))
        .setBuf(buf)
        .build();
  }

  /** The block as one chunk, with a CRC32C checksum per {@code bytesPerChecksum} bytes. */
  private static List<ChunkInfo> chunks(byte[] block, int bytesPerChecksum) throws OzoneChecksumException {
    return Collections.singletonList(ChunkInfo.newBuilder()
        .setChunkName("chunk")
        .setOffset(0)
        .setLen(block.length)
        .setChecksumData(new Checksum(ChecksumType.CRC32C, bytesPerChecksum).computeChecksum(block)
            .getProtoBufMessage())
        .build());
  }

  private DataStreamReply successReply() {
    final RaftClientReply reply = RaftClientReply.newBuilder()
        .setClientId(ClientId.randomId())
        .setServerId(RaftGroupMemberId.valueOf(RatisHelper.toRaftPeerId(dn1), RaftGroupId.valueOf(pipelineID.getId())))
        .setCallId(1)
        .setSuccess()
        .build();
    return reply(DataStreamPacketHeaderProto.Type.STREAM_HEADER,
        ClientProtoUtils.toRaftClientReplyProto(reply).toByteString().asReadOnlyByteBuffer(), true);
  }

  private DataStreamReply notLeaderReply(DatanodeDetails follower, DatanodeDetails leader) {
    final RaftGroupMemberId member = RaftGroupMemberId.valueOf(
        RatisHelper.toRaftPeerId(follower), RaftGroupId.valueOf(pipelineID.getId()));
    final RaftClientReply reply = RaftClientReply.newBuilder()
        .setClientId(ClientId.randomId())
        .setServerId(member)
        .setCallId(1)
        .setException(new NotLeaderException(member, RatisHelper.toRaftPeer(leader), Collections.emptyList()))
        .build();
    return reply(DataStreamPacketHeaderProto.Type.STREAM_HEADER,
        ClientProtoUtils.toRaftClientReplyProto(reply).toByteString().asReadOnlyByteBuffer(), false);
  }

  private static DataStreamReply reply(DataStreamPacketHeaderProto.Type type, ByteBuffer buffer, boolean success) {
    return DataStreamReplyByteBuffer.newBuilder()
        .setDataStreamReplyHeader(new DataStreamReplyHeader(ClientId.randomId(), type, 1, 0, buffer.remaining(), 0,
            success, Collections.emptyList()))
        .setBuffer(buffer)
        .build();
  }

  private static ContainerCommandRequestProto toRequest(ByteBuffer header) throws Exception {
    return ContainerCommandRequestMessage.toProto(ByteString.copyFrom(header.duplicate()), null);
  }
}
