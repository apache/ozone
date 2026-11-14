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

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamBufferArgs;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RatisBlockOutputStream} exercised through {@link java.io.OutputStream} with a
 * {@link MockRatisPipeline}.
 *
 * <p>Parallels {@link TestBlockDataStreamOutput} where the Ratis block path applies. DataStream-only cases
 * (stream init, mid-stream PutBlock without Raft) are intentionally omitted.
 */
class TestRatisBlockOutputStream {

  // Config: CHUNK=100, flush boundary=4 chunks (400B), max window=8 chunks (800B)
  private static final int CHUNK_SIZE = 100;
  private static final long FLUSH_SIZE = 400;
  private static final long BUFFER_MAX_SIZE = 800;
  private static final int BUFFER_POOL_CAPACITY = (int) (BUFFER_MAX_SIZE / CHUNK_SIZE);

  private static OzoneClientConfig createConfig() {
    OzoneClientConfig config = new OzoneClientConfig();
    config.setStreamBufferSize(CHUNK_SIZE);
    config.setStreamBufferFlushSize(FLUSH_SIZE);
    config.setStreamBufferMaxSize(BUFFER_MAX_SIZE);
    config.setStreamBufferFlushDelay(false);
    config.setChecksumType(ContainerProtos.ChecksumType.NONE);
    config.setBytesPerChecksum(CHUNK_SIZE);
    // Prefer standalone PutBlock RPCs so assertions can use getReceivedPutBlocks().
    config.setEnablePutblockPiggybacking(false);
    return config;
  }

  private RatisBlockOutputStream createStream(MockRatisPipeline pipeline) throws IOException {
    return createStream(pipeline, createConfig());
  }

  private RatisBlockOutputStream createStream(MockRatisPipeline pipeline, OzoneClientConfig config)
      throws IOException {
    StreamBufferArgs streamBufferArgs = StreamBufferArgs.getDefaultStreamBufferArgs(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE), config);
    BufferPool bufferPool = new BufferPool(
        streamBufferArgs.getStreamBufferSize(),
        BUFFER_POOL_CAPACITY);
    return createStream(pipeline, config, bufferPool, streamBufferArgs);
  }

  private RatisBlockOutputStream createStream(
      MockRatisPipeline pipeline,
      OzoneClientConfig config,
      BufferPool bufferPool,
      StreamBufferArgs streamBufferArgs) throws IOException {
    return new RatisBlockOutputStream(
        pipeline.getBlockID(),
        -1,
        pipeline.getClientFactory(),
        pipeline.getPipeline(),
        bufferPool,
        config,
        null,
        ContainerClientMetrics.acquire(),
        streamBufferArgs,
        () -> newFixedThreadPool(4));
  }

  /**
   * Block metadata from the first standalone PutBlock, or from piggybacked WriteChunk if piggybacking is enabled.
   */
  private static BlockData firstCommittedBlockData(MockRatisPipeline pipeline) {
    if (!pipeline.getReceivedPutBlocks().isEmpty()) {
      return pipeline.getReceivedPutBlocks().get(0).getPutBlock().getBlockData();
    }
    return pipeline.getReceivedBlockData().get(0);
  }

  private static List<ChunkInfo> firstPutBlockChunkList(MockRatisPipeline pipeline) {
    return firstCommittedBlockData(pipeline).getChunksList();
  }

  @Test
  void writeSubChunkThenClose() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    try (RatisBlockOutputStream stream = createStream(pipeline)) {
      byte[] data = randomBytes(50);
      stream.write(data, 0, data.length);
      // No chunk shipped yet — data is in currentBuffer
      assertEquals(0, pipeline.getReceivedChunks().size(),
          "No chunk should be shipped before close for sub-chunk write");
    }
    // After close: 1 chunk flushed + 1 putBlock
    assertEquals(1, pipeline.getReceivedChunks().size());
    assertEquals(1, pipeline.getReceivedPutBlocks().size());
  }

  @Test
  void writeExactChunkThenClose() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    byte[] data = randomBytes(CHUNK_SIZE);
    try (RatisBlockOutputStream stream = createStream(pipeline)) {
      stream.write(data, 0, data.length);
      // Exact chunk → shipped immediately (currentBuffer full)
      assertEquals(1, pipeline.getReceivedChunks().size());
    }
    assertEquals(1, pipeline.getReceivedPutBlocks().size());
    assertArrayEquals(data, pipeline.getAllReceivedData());
  }

  @Test
  void writeFlushBoundaryTriggersPutBlock() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    byte[] data = randomBytes(400);
    try (RatisBlockOutputStream stream = createStream(pipeline)) {
      stream.write(data, 0, data.length);
      // 4 chunks of 100B each → hits flush boundary → 1 putBlock
      assertEquals(4, pipeline.getReceivedChunks().size());
      assertEquals(1, pipeline.getReceivedPutBlocks().size());
    }
    // Close adds another putBlock
    assertEquals(2, pipeline.getReceivedPutBlocks().size());
  }

  @Test
  void writeUntilBufferPoolFullTriggersWatchForCommit() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    // Write 8 chunks of 100B each → hits buffer pool capacity → watchForCommit
    byte[] data = randomBytes((int) BUFFER_MAX_SIZE);
    try (RatisBlockOutputStream stream = createStream(pipeline)) {
      stream.write(data, 0, data.length);
      assertEquals(1, pipeline.getWatchForCommitCount());
    }
    assertArrayEquals(data, pipeline.getAllReceivedData());
  }

  @Test
  void flushTriggersPutBlockWithoutClose() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    try (RatisBlockOutputStream stream = createStream(pipeline)) {
      stream.write(randomBytes(200), 0, 200);
      stream.flush();
      assertEquals(1, pipeline.getReceivedPutBlocks().size(),
          "flush() should trigger PutBlock");
      // proves that the stream is still open and can accept more writes
      stream.write(randomBytes(50));
    }
  }

  @Test
  void hsyncFlushesAndWaitsForCommit() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    try (RatisBlockOutputStream stream = createStream(pipeline)) {
      stream.write(randomBytes(200), 0, 200);
      stream.hsync();
      // 2 chunks, 1 putBlock (from hsync), watch called
      assertEquals(2, pipeline.getReceivedChunks().size());
      assertEquals(1, pipeline.getReceivedPutBlocks().size());
      assertEquals(1, pipeline.getWatchForCommitCount());
    }
  }

  @Test
  void hsyncPropagatesIOException() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    pipeline.failPutBlockAfter(0, () -> new IOException("simulated putBlock fail"));
    RatisBlockOutputStream stream = createStream(pipeline);
    stream.write(randomBytes(200), 0, 200);

    // hsync should propagate the IOException from the failed putBlock
    assertThrows(IOException.class, stream::hsync, "hsync() must propagate IOException from failed putBlock");
    assertThrows(IOException.class, stream::close);
  }

  @Test
  void hsyncPropagatesWatchFailure() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    pipeline.failWatchAfter(0, () -> new IOException("simulated watch timeout"));
    RatisBlockOutputStream stream = createStream(pipeline);
    stream.write(randomBytes(200), 0, 200);

    // hsync should propagate the watch failure
    assertThrows(IOException.class, stream::hsync, "hsync() must propagate IOException from failed watchForCommit");
    assertThrows(IOException.class, stream::close);
  }

  @Test
  void closeAfterWriteFailureThrows() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    pipeline.failWriteChunkAfter(1, () -> new IOException("chunk write failed due to injected failure"));
    RatisBlockOutputStream stream = createStream(pipeline);
    stream.write(randomBytes(50), 0, 50);
    stream.write(randomBytes(CHUNK_SIZE + 50), 0, CHUNK_SIZE + 50);
    // The exception wrapping depends on thread scheduling, so verify the root cause.
    IOException e = assertThrows(IOException.class, stream::close);
    assertThat(e)
        .hasRootCauseInstanceOf(IOException.class)
        .hasRootCauseMessage("chunk write failed due to injected failure");
  }

  @Test
  void writeAfterCloseThrows() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    RatisBlockOutputStream stream = createStream(pipeline);
    byte[] data = randomBytes(CHUNK_SIZE);
    stream.write(data, 0, data.length);
    stream.close();
    IOException e = assertThrows(IOException.class, () -> stream.write(randomBytes(CHUNK_SIZE), 0, CHUNK_SIZE),
        "write after close throws IOException");
    assertThat(e.getMessage()).contains("has been closed");
  }

  @Test
  void ackDataLengthTracksCommittedData() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    RatisBlockOutputStream stream = createStream(pipeline);
    stream.write(randomBytes(400), 0, 400);
    stream.close();
    assertEquals(400, stream.getTotalAckDataLength(),
        "After close, all written data should be acknowledged");
  }

  @Test
  void chunkDataIntegrity() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    byte[] data = randomBytes(350);
    try (RatisBlockOutputStream stream = createStream(pipeline)) {
      stream.write(data, 0, data.length);
    }
    // 3 full chunks of 100B + 1 partial chunk of 50B
    assertEquals(4, pipeline.getReceivedChunks().size());
    assertArrayEquals(data, pipeline.getAllReceivedData(), "Concatenated chunk data must match original input");
  }

  @Test
  void putBlockContainsAllChunkMetadata() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    try (RatisBlockOutputStream stream = createStream(pipeline)) {
      stream.write(randomBytes(300), 0, 300);
    }

    // One putBlock should have been sent
    assertEquals(1, pipeline.getReceivedPutBlocks().size());

    // 3 chunks with 300 bytes were sent and all chunk file name is correct.
    List<ContainerProtos.ChunkInfo> chunksList = firstPutBlockChunkList(pipeline);

    assertEquals(3, chunksList.size());
    assertEquals(300, chunksList.stream().mapToLong(ContainerProtos.ChunkInfo::getLen).sum());
    assertTrue(chunksList.stream().allMatch(c -> c.getChunkName().contains("_chunk_")));
  }

  @Test
  void writeOnRetryReplaysBufferedData() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    OzoneClientConfig config = createConfig();
    StreamBufferArgs streamBufferArgs = StreamBufferArgs.getDefaultStreamBufferArgs(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE), config);
    BufferPool bufferPool = new BufferPool(streamBufferArgs.getStreamBufferSize(), BUFFER_POOL_CAPACITY);

    // Simulate KeyOutputStream retry handoff: data sits in the shared pool on a new block stream
    // without a prior successful write/commit on this stream instance.
    int retryLen = 3 * CHUNK_SIZE;
    byte[] data = randomBytes(retryLen);
    for (int i = 0; i < 3; i++) {
      ChunkBuffer buffer = bufferPool.allocateBuffer(0);
      buffer.put(data, i * CHUNK_SIZE, CHUNK_SIZE);
    }

    RatisBlockOutputStream stream = createStream(pipeline, config, bufferPool, streamBufferArgs);
    stream.writeOnRetry(retryLen);

    assertEquals(3, pipeline.getReceivedChunks().size(),
        "writeOnRetry should issue one WriteChunk per allocated buffer");
    assertArrayEquals(data, pipeline.getAllReceivedData());
    for (int i = 0; i < 3; i++) {
      assertArrayEquals(
          Arrays.copyOfRange(data, i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE),
          pipeline.getReceivedChunks().get(i));
    }
    assertEquals(3, pipeline.getReceivedPutBlocks().size(),
        "writeOnRetry should PutBlock once per buffer in the retry loop");
    assertEquals(3, pipeline.getWatchForCommitCount(),
        "writeOnRetry should synchronously watchForCommit once per buffer");

    stream.close();
  }

  private static byte[] randomBytes(int length) {
    return RandomUtils.secure().randomBytes(length);
  }
}
