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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link ShortCircuitStreamCache}.
 */
class TestShortCircuitStreamCache {

  private static final long EXPIRY_MS = TimeUnit.MINUTES.toMillis(5);

  @TempDir
  private Path dir;

  private static ScheduledExecutorService cleanupExecutor;
  private static ExecutorService closeExecutor;

  @BeforeAll
  static void init() {
    closeExecutor = new ThreadPoolExecutor(0, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder().setDaemon(true).build());
    cleanupExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).build());
  }

  @AfterAll
  static void cleanup() {
    closeExecutor.shutdownNow();
    cleanupExecutor.shutdownNow();
  }

  @Test
  void testPut() throws Exception {
    try (ShortCircuitStreamCache cache = new ShortCircuitStreamCache(10, 1, closeExecutor, cleanupExecutor)) {
      ShortCircuitStreamCache.Replica replica = cache.put(blockID(1, 5), blockData(1, 5, 100), newStream());
      cache.release(replica);
      Thread.sleep(10);

      assertNull(cache.acquire(blockID(1, 5), 100));
      assertTrue(replica.isClosed());
    }
  }

  @Test
  void testAcquire() throws Exception {
    try (ShortCircuitStreamCache cache = new ShortCircuitStreamCache(10, EXPIRY_MS, closeExecutor, cleanupExecutor)) {
      ShortCircuitStreamCache.Replica replica = cache.put(blockID(1, 5), blockData(1, 5, 100), newStream());
      cache.release(replica);

      ShortCircuitStreamCache.Replica hit = cache.acquire(blockID(1, 5), 100);
      assertSame(replica, hit);
      cache.release(hit);
      assertSame(replica, cache.acquire(blockID(1, 4), 50));
      cache.release(replica);
      assertFalse(replica.isClosed());

      // Acquire failure will evict the old replica. BCSID is not match.
      assertNull(cache.acquire(blockID(1, 6), 100));
      final ShortCircuitStreamCache.Replica staleBcsId = replica;
      GenericTestUtils.waitFor(staleBcsId::isClosed, 10, 1000);

      replica = cache.put(blockID(1, 5), blockData(1, 5, 100), newStream());
      cache.release(replica);
      // Acquire failure will evict the old replica. Size is not match.
      assertNull(cache.acquire(blockID(1, 5), 101));
      final ShortCircuitStreamCache.Replica staleSize = replica;
      GenericTestUtils.waitFor(staleSize::isClosed, 10, 1000);
    }
  }

  @Test
  void testReplace() throws Exception {
    try (ShortCircuitStreamCache cache = new ShortCircuitStreamCache(10, EXPIRY_MS, closeExecutor, cleanupExecutor)) {
      ShortCircuitStreamCache.Replica old = cache.put(blockID(1, 5), blockData(1, 5, 100), newStream());
      cache.release(old);
      ShortCircuitStreamCache.Replica replacement = cache.put(blockID(1, 6), blockData(1, 6, 200), newStream());

      GenericTestUtils.waitFor(() -> old.isClosed(), 100, 2000);
      assertFalse(replacement.isClosed());
      cache.release(replacement);
      assertSame(replacement, cache.acquire(blockID(1, 6), 200));
    }
  }

  @Test
  void testOverlyRelease() throws Exception {
    try (ShortCircuitStreamCache cache = new ShortCircuitStreamCache(10, EXPIRY_MS, closeExecutor, cleanupExecutor)) {
      ShortCircuitStreamCache.Replica replica = cache.put(blockID(1, 5), blockData(1, 5, 100), newStream());
      cache.release(replica);
      assertThrows(IllegalStateException.class, () -> cache.release(replica));
    }
  }

  @Test
  void testInvalidate() throws Exception {
    try (ShortCircuitStreamCache cache = new ShortCircuitStreamCache(10, EXPIRY_MS, closeExecutor, cleanupExecutor)) {
      ShortCircuitStreamCache.Replica replica = cache.put(blockID(1, 5), blockData(1, 5, 100), newStream());
      cache.invalidate(replica);
      assertNull(cache.acquire(blockID(1, 5), 100));
      assertFalse(replica.isClosed());

      cache.release(replica);
      GenericTestUtils.waitFor(() -> replica.isClosed(), 10, 1000);
    }
  }

  @Test
  void testRepeatedInvalidate() throws Exception {
    try (ShortCircuitStreamCache cache = new ShortCircuitStreamCache(10, EXPIRY_MS, closeExecutor, cleanupExecutor)) {
      ShortCircuitStreamCache.Replica replica = cache.put(blockID(1, 5), blockData(1, 5, 100), newStream());
      cache.invalidate(replica);
      cache.invalidate(replica);
      assertFalse(replica.isClosed());

      cache.release(replica);
      GenericTestUtils.waitFor(() -> replica.isClosed(), 10, 1000);
      assertThrows(IllegalStateException.class, () -> cache.release(replica));
    }
  }

  @Test
  void testCacheSize() throws Exception {
    try (ShortCircuitStreamCache cache = new ShortCircuitStreamCache(1, EXPIRY_MS, closeExecutor, cleanupExecutor)) {
      ShortCircuitStreamCache.Replica first = cache.put(blockID(1, 5), blockData(1, 5, 100), newStream());
      ShortCircuitStreamCache.Replica second = cache.put(blockID(2, 5), blockData(2, 5, 100), newStream());
      cache.release(first);
      GenericTestUtils.waitFor(() -> first.isClosed(), 10, 1000);
      cache.release(second);
      assertFalse(second.isClosed());
    }
  }

  @Test
  void testCachedReplicaRemainsOpenWhileBlockInputStreamRetries() throws Exception {
    final int len = 100;
    final ChunkInputStream chunkStream = mock(ChunkInputStream.class);
    when(chunkStream.read(any(), anyInt(), anyInt()))
        .thenThrow(new IOException("first failure"))
        .thenThrow(new IOException("second failure"))
        .thenReturn(len);
    when(chunkStream.getRemaining()).thenReturn((long) len);

    try (ShortCircuitStreamCache cache = new ShortCircuitStreamCache(10, EXPIRY_MS, closeExecutor, cleanupExecutor)) {
      BlockData blockData = blockData(1, 5, len).toBuilder()
          .addChunks(ChunkInfo.newBuilder().setChunkName("chunk").setOffset(0).setLen(len)
              .setChecksumData(ChecksumData.newBuilder().setType(ChecksumType.NONE).setBytesPerChecksum(len)))
          .build();
      ShortCircuitStreamCache.Replica replica = cache.put(blockID(1, 5), blockData, newStream());
      cache.release(replica);

      XceiverClientFactory clientFactory = mock(XceiverClientFactory.class);
      when(clientFactory.getShortCircuitStreamCache()).thenReturn(cache);
      BlockInputStream stream = new BlockInputStream(
          new BlockLocationInfo(new BlockLocationInfo.Builder().setBlockID(blockID(1, 5)).setLength(len)),
          MockPipeline.createSingleNodePipeline(), null, clientFactory, null,
          new OzoneConfiguration().getObject(OzoneClientConfig.class)) {
        @Override
        protected ChunkInputStream createChunkInputStream(ChunkInfo chunkInfo) {
          return chunkStream;
        }
      };

      assertEquals(len, stream.read(new byte[len], 0, len));
      assertFalse(replica.isClosed());
      assertNull(cache.acquire(blockID(1, 5), len));

      stream.close();
      GenericTestUtils.waitFor(replica::isClosed, 10, 1000);
    }
  }

  private static BlockID blockID(long localID, long bcsId) {
    BlockID blockID = new BlockID(1, localID);
    blockID.setBlockCommitSequenceId(bcsId);
    return blockID;
  }

  private static BlockData blockData(long localID, long bcsId, long size) {
    return BlockData.newBuilder()
        .setBlockID(blockID(localID, bcsId).getDatanodeBlockIDProtobuf())
        .setSize(size)
        .build();
  }

  @SuppressWarnings("PMD.AvoidFileStream")
  private FileInputStream newStream() throws IOException {
    File file = Files.createTempFile(dir, "block", ".data").toFile();
    return new FileInputStream(file);
  }
}
