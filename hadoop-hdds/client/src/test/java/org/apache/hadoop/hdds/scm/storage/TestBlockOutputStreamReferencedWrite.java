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

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.apache.hadoop.ozone.OzoneConsts.INCREMENTAL_CHUNK_LIST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetCommittedBlockLengthResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamBufferArgs;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

/**
 * Verifies {@link BlockOutputStream} referenced-buffer fast path.
 */
class TestBlockOutputStreamReferencedWrite {

  private static final int CHUNK_SIZE = 256 * 1024;

  @Test
  void referencedChunkSafeWrapMatchesWriteChunkPath() {
    final byte[] data = RandomUtils.secure().randomBytes(CHUNK_SIZE);
    final BufferPool bufferPool = new BufferPool(CHUNK_SIZE, 4);
    final ChunkBuffer original = ChunkBuffer.wrapReadOnly(data, 0, CHUNK_SIZE);
    final ChunkBuffer chunk = original.duplicate(0, original.position());
    assertEquals(CHUNK_SIZE, chunk.remaining());
    assertEquals(CHUNK_SIZE,
        chunk.toByteString(bufferPool.byteStringConversion()).size());
  }

  @Test
  void fullChunkWriteUsesReferencedBuffer() throws IOException {
    final byte[] data = RandomUtils.secure().randomBytes(CHUNK_SIZE * 4);
    final BufferPool bufferPool = new BufferPool(CHUNK_SIZE, 4);
    final CapturingXceiverClient client = new CapturingXceiverClient(
        MockPipeline.createRatisPipeline());

    try (BlockOutputStream stream = createStream(bufferPool, client, true,
        CHUNK_SIZE, CHUNK_SIZE)) {
      for (int i = 0; i < 4; i++) {
        stream.write(data, i * CHUNK_SIZE, CHUNK_SIZE);
      }
    }

    assertEquals(4, client.writeChunkPayloads.size());
    for (int i = 0; i < 4; i++) {
      final byte[] expected = new byte[CHUNK_SIZE];
      System.arraycopy(data, i * CHUNK_SIZE, expected, 0, CHUNK_SIZE);
      assertArrayPrefix(expected, client.writeChunkPayloads.get(i));
    }
    assertEquals(0, bufferPool.getNumberOfUsedBuffers());
  }

  @Test
  void subStreamBufferWriteUsesReferencedBufferWithDefaultMinSize()
      throws IOException {
    final int writeSize = 1024 * 1024;
    final int streamBufferSize = 4 * 1024 * 1024;
    final byte[] data = RandomUtils.secure().randomBytes(writeSize * 4);
    final BufferPool bufferPool = new BufferPool(streamBufferSize, 8);
    final CapturingXceiverClient client = new CapturingXceiverClient(
        MockPipeline.createRatisPipeline());

    try (BlockOutputStream stream = createStream(bufferPool, client, true,
        streamBufferSize, writeSize)) {
      for (int i = 0; i < 4; i++) {
        stream.write(data, i * writeSize, writeSize);
      }
    }

    assertEquals(4, client.writeChunkPayloads.size());
    for (int i = 0; i < 4; i++) {
      final byte[] expected = new byte[writeSize];
      System.arraycopy(data, i * writeSize, expected, 0, writeSize);
      assertArrayPrefix(expected, client.writeChunkPayloads.get(i));
    }
    assertEquals(0, bufferPool.getNumberOfUsedBuffers());
  }

  @Test
  void readOnlyWriteReturnsAfterCommit() throws IOException {
    final byte[] data = RandomUtils.secure().randomBytes(CHUNK_SIZE);
    final BufferPool bufferPool = new BufferPool(CHUNK_SIZE, 4);
    final CapturingXceiverClient client = new CapturingXceiverClient(
        MockPipeline.createRatisPipeline());

    try (BlockOutputStream stream = createStream(bufferPool, client, true,
        CHUNK_SIZE, CHUNK_SIZE)) {
      stream.write(data, 0, CHUNK_SIZE);
      data[0] = (byte) 0x7A;
      stream.write(data, 0, CHUNK_SIZE);
    }

    assertEquals(2, client.writeChunkPayloads.size());
    assertEquals((byte) 0x7A, client.writeChunkPayloads.get(1).byteAt(0));
    assertEquals(0, bufferPool.getNumberOfUsedBuffers());
  }

  @Test
  void readOnlyWriteWithNonZeroBufferIncrement() throws IOException {
    final byte[] data = RandomUtils.secure().randomBytes(CHUNK_SIZE);
    final BufferPool bufferPool = new BufferPool(CHUNK_SIZE, 4);
    final CapturingXceiverClient client = new CapturingXceiverClient(
        MockPipeline.createRatisPipeline());

    try (BlockOutputStream stream = createStream(bufferPool, client, true,
        CHUNK_SIZE, CHUNK_SIZE, 64 * 1024)) {
      stream.write(data, 0, CHUNK_SIZE);
    }

    assertEquals(1, client.writeChunkPayloads.size());
    assertArrayPrefix(data, client.writeChunkPayloads.get(0));
    assertEquals(0, bufferPool.getNumberOfUsedBuffers());
  }

  @Test
  void readOnlyWriteWithIncrementalChunkList() throws IOException {
    final byte[] data = RandomUtils.secure().randomBytes(CHUNK_SIZE * 2);
    final BufferPool bufferPool = new BufferPool(CHUNK_SIZE, 4);
    final CapturingXceiverClient client = new CapturingXceiverClient(
        MockPipeline.createRatisPipeline());

    try (BlockOutputStream stream = createStream(bufferPool, client, true,
        CHUNK_SIZE, CHUNK_SIZE, 0, true)) {
      stream.write(data, 0, CHUNK_SIZE);
      stream.write(data, CHUNK_SIZE, CHUNK_SIZE);
    }

    assertEquals(2, client.writeChunkPayloads.size());
    assertArrayPrefix(data, 0, CHUNK_SIZE, client.writeChunkPayloads.get(0));
    assertArrayPrefix(data, CHUNK_SIZE, CHUNK_SIZE, client.writeChunkPayloads.get(1));
    assertTrue(client.putBlockDataList.size() >= 2);
    for (BlockData blockData : client.putBlockDataList) {
      assertIncrementalChunkListMetadata(blockData);
    }
    assertEquals(0, bufferPool.getNumberOfUsedBuffers());
  }

  @Test
  void readOnlyWriteReturnsAfterCommitWithIncrementalChunkList() throws IOException {
    final byte[] data = RandomUtils.secure().randomBytes(CHUNK_SIZE);
    final BufferPool bufferPool = new BufferPool(CHUNK_SIZE, 4);
    final CapturingXceiverClient client = new CapturingXceiverClient(
        MockPipeline.createRatisPipeline());

    try (BlockOutputStream stream = createStream(bufferPool, client, true,
        CHUNK_SIZE, CHUNK_SIZE, 0, true)) {
      stream.write(data, 0, CHUNK_SIZE);
      data[0] = (byte) 0x7A;
      stream.write(data, 0, CHUNK_SIZE);
    }

    assertEquals(2, client.writeChunkPayloads.size());
    assertEquals((byte) 0x7A, client.writeChunkPayloads.get(1).byteAt(0));
    assertEquals(0, bufferPool.getNumberOfUsedBuffers());
  }

  @Test
  void fullPooledChunkPrecedesReferencedWrite() throws IOException {
    final byte[] data = RandomUtils.secure().randomBytes(CHUNK_SIZE * 2);
    final BufferPool bufferPool = new BufferPool(CHUNK_SIZE, 4);
    final CapturingXceiverClient client = new CapturingXceiverClient(
        MockPipeline.createRatisPipeline());

    try (BlockOutputStream stream = createStream(bufferPool, client, true,
        CHUNK_SIZE, CHUNK_SIZE)) {
      for (int i = 0; i < CHUNK_SIZE; i++) {
        stream.write((byte) data[i]);
      }
      stream.write(data, CHUNK_SIZE, CHUNK_SIZE);
    }

    assertEquals(2, client.writeChunkPayloads.size());
    assertArrayPrefix(data, 0, CHUNK_SIZE, client.writeChunkPayloads.get(0));
    assertArrayPrefix(data, CHUNK_SIZE, CHUNK_SIZE, client.writeChunkPayloads.get(1));
    assertEquals(0, bufferPool.getNumberOfUsedBuffers());
  }

  @Test
  void partialChunkWriteUsesPooledBuffer() throws IOException {
    final byte[] data = RandomUtils.secure().randomBytes(CHUNK_SIZE / 2);
    final BufferPool bufferPool = new BufferPool(CHUNK_SIZE, 4);
    final CapturingXceiverClient client = new CapturingXceiverClient(
        MockPipeline.createRatisPipeline());

    try (BlockOutputStream stream = createStream(bufferPool, client, true,
        CHUNK_SIZE, CHUNK_SIZE)) {
      stream.write(data);
      stream.close();
    }

    assertEquals(1, client.writeChunkPayloads.size());
    assertArrayPrefix(data, client.writeChunkPayloads.get(0));
    assertEquals(1, bufferPool.getSize());
  }

  private static BlockOutputStream createStream(BufferPool bufferPool,
      CapturingXceiverClient client, boolean referenceWrite) throws IOException {
    return createStream(bufferPool, client, referenceWrite, CHUNK_SIZE,
        CHUNK_SIZE);
  }

  private static BlockOutputStream createStream(BufferPool bufferPool,
      CapturingXceiverClient client, boolean referenceWrite, int streamBufferSize,
      int referenceWriteMinSize) throws IOException {
    return createStream(bufferPool, client, referenceWrite, streamBufferSize,
        referenceWriteMinSize, 0);
  }

  private static BlockOutputStream createStream(BufferPool bufferPool,
      CapturingXceiverClient client, boolean referenceWrite, int streamBufferSize,
      int referenceWriteMinSize, int bufferIncrement) throws IOException {
    return createStream(bufferPool, client, referenceWrite, streamBufferSize,
        referenceWriteMinSize, bufferIncrement, false);
  }

  private static BlockOutputStream createStream(BufferPool bufferPool,
      CapturingXceiverClient client, boolean referenceWrite, int streamBufferSize,
      int referenceWriteMinSize, int bufferIncrement,
      boolean incrementalChunkList) throws IOException {
    final Pipeline pipeline = client.getPipeline();
    final XceiverClientManager xcm = mock(XceiverClientManager.class);
    when(xcm.acquireClient(any())).thenReturn(client);

    OzoneClientConfig config = new OzoneClientConfig();
    config.setStreamBufferSize(streamBufferSize);
    config.setStreamBufferMaxSize(streamBufferSize * 4);
    config.setStreamBufferFlushSize(streamBufferSize * 4);
    config.setStreamBufferFlushDelay(false);
    config.setChecksumType(ChecksumType.CRC32);
    config.setBytesPerChecksum(64 * 1024);
    config.setStreamBufferReferenceWrite(referenceWrite);
    config.setStreamBufferReferenceWriteMinSize(referenceWriteMinSize);
    if (bufferIncrement > 0) {
      config = configWithBufferIncrement(config, bufferIncrement);
    }
    if (incrementalChunkList) {
      config = configWithIncrementalChunkList(config);
    }

    final StreamBufferArgs streamBufferArgs =
        StreamBufferArgs.getDefaultStreamBufferArgs(pipeline.getReplicationConfig(),
            config);

    return new RatisBlockOutputStream(
        new BlockID(1L, 1L),
        -1,
        xcm,
        pipeline,
        bufferPool,
        config,
        null,
        ContainerClientMetrics.acquire(),
        streamBufferArgs,
        () -> newSingleThreadExecutor());
  }

  private static OzoneClientConfig configWithBufferIncrement(OzoneClientConfig base,
      int bufferIncrement) {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.setFromObject(base);
    conf.setInt("ozone.client.stream.buffer.increment", bufferIncrement);
    return conf.getObject(OzoneClientConfig.class);
  }

  private static OzoneClientConfig configWithIncrementalChunkList(OzoneClientConfig base) {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.setFromObject(base);
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    conf.setBoolean("ozone.client.incremental.chunk.list", true);
    return conf.getObject(OzoneClientConfig.class);
  }

  private static void assertArrayPrefix(byte[] expected, ByteString actual) {
    assertArrayPrefix(expected, 0, expected.length, actual);
  }

  private static void assertArrayPrefix(byte[] expected, int offset, int length,
      ByteString actual) {
    assertEquals(length, actual.size());
    final byte[] received = actual.toByteArray();
    for (int i = 0; i < length; i++) {
      assertEquals(expected[offset + i], received[i], "byte mismatch at index " + i);
    }
  }

  private static void assertIncrementalChunkListMetadata(BlockData blockData) {
    assertTrue(blockData.getMetadataList().stream()
        .anyMatch(kv -> INCREMENTAL_CHUNK_LIST.equals(kv.getKey())));
  }

  private static final class CapturingXceiverClient extends XceiverClientSpi {
    private final Pipeline pipeline;
    private final AtomicInteger logIndex = new AtomicInteger();
    private final List<ByteString> writeChunkPayloads = new ArrayList<>();
    private final List<BlockData> putBlockDataList = new ArrayList<>();
    private CapturingXceiverClient(Pipeline pipeline) {
      this.pipeline = pipeline;
    }

    @Override
    public void connect() {
    }

    @Override
    public void close() {
    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    @Override
    public XceiverClientReply sendCommandAsync(
        ContainerCommandRequestProto request) {
      if (!request.hasVersion()) {
        request = ContainerCommandRequestProto.newBuilder(request)
            .setVersion(ClientVersion.CURRENT.toProtoValue()).build();
      }
      if (request.getCmdType() == Type.WriteChunk) {
        writeChunkPayloads.add(request.getWriteChunk().getData());
      } else if (request.getCmdType() == Type.PutBlock) {
        putBlockDataList.add(request.getPutBlock().getBlockData());
      }
      final ContainerCommandResponseProto.Builder builder =
          ContainerCommandResponseProto.newBuilder()
              .setResult(Result.SUCCESS)
              .setCmdType(request.getCmdType());
      if (request.getCmdType() == Type.PutBlock) {
        builder.setPutBlock(PutBlockResponseProto.newBuilder()
            .setCommittedBlockLength(
                GetCommittedBlockLengthResponseProto.newBuilder()
                    .setBlockID(request.getPutBlock().getBlockData().getBlockID())
                    .setBlockLength(request.getPutBlock().getBlockData().getSize())
                    .build())
            .build());
      }
      final XceiverClientReply reply = new XceiverClientReply(
          CompletableFuture.completedFuture(builder.build()));
      reply.setLogIndex(logIndex.incrementAndGet());
      return reply;
    }

    @Override
    public ReplicationType getPipelineType() {
      return pipeline.getType();
    }

    @Override
    public CompletableFuture<XceiverClientReply> watchForCommit(long index) {
      final ContainerCommandResponseProto response =
          ContainerCommandResponseProto.newBuilder()
              .setCmdType(Type.WriteChunk)
              .setResult(Result.SUCCESS)
              .build();
      final XceiverClientReply reply =
          new XceiverClientReply(CompletableFuture.completedFuture(response));
      reply.setLogIndex(index);
      return CompletableFuture.completedFuture(reply);
    }

    @Override
    public long getReplicatedMinCommitIndex() {
      return logIndex.get();
    }

    @Override
    public Map<DatanodeDetails, ContainerCommandResponseProto> sendCommandOnAllNodes(
        ContainerCommandRequestProto request) {
      return null;
    }
  }
}
