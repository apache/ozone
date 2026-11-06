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

package org.apache.hadoop.ozone.client.io;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamBufferArgs;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.MockRatisPipeline;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

/**
 * Tests KeyOutputStream.
 * This is a unit test meant to verify specific behaviors of KeyOutputStream.
 */
public class TestKeyOutputStream {

  private static final int CHUNK_SIZE = 100;
  private static final long FLUSH_SIZE = 400;
  private static final long BLOCK_SIZE = 800;

  @BeforeAll
  static void init() {
    GenericTestUtils.setLogLevel(KeyOutputStreamSemaphore.class, Level.DEBUG);
  }

  private static OzoneClientConfig createConfig() {
    OzoneClientConfig config = new OzoneClientConfig();
    config.setStreamBufferSize(CHUNK_SIZE);
    config.setStreamBufferFlushSize(FLUSH_SIZE);
    config.setStreamBufferMaxSize(2 * FLUSH_SIZE);
    config.setStreamBufferFlushDelay(false);
    config.setChecksumType(ContainerProtos.ChecksumType.NONE);
    config.setBytesPerChecksum(CHUNK_SIZE);
    config.setEnablePutblockPiggybacking(false);
    return config;
  }

  /**
   * Creates a shared XceiverClientFactory that routes acquireClient calls to the correct MockRatisPipeline.
   */
  private XceiverClientFactory createSharedClientFactory(MockRatisPipeline... pipelines) throws IOException {
    XceiverClientFactory factory = mock(XceiverClientFactory.class);
    doAnswer(invocation -> {
      Pipeline p = invocation.getArgument(0);
      for (MockRatisPipeline pipeline : pipelines) {
        if (pipeline.getPipeline().getId().equals(p.getId())) {
          return pipeline.getXceiverClient();
        }
      }
      throw new IOException("Unknown pipeline: " + p.getId());
    }).when(factory).acquireClient(any(Pipeline.class), anyBoolean());

    doAnswer(invocation -> {
      Pipeline p = invocation.getArgument(0);
      for (MockRatisPipeline pipeline : pipelines) {
        if (pipeline.getPipeline().getId().equals(p.getId())) {
          return pipeline.getXceiverClient();
        }
      }
      throw new IOException("Unknown pipeline: " + p.getId());
    }).when(factory).acquireClient(any(Pipeline.class));

    return factory;
  }

  /**
   * Creates a KeyOutputStream with a mocked OM client and an initial preallocated block.
   */
  private KeyOutputStream createKeyStream(OzoneManagerProtocol omClient, MockRatisPipeline... pipelines)
      throws IOException {
    OzoneClientConfig config = createConfig();
    ReplicationConfig replicationConfig = RatisReplicationConfig.getInstance(ReplicationFactor.THREE);

    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setKeyName("testkey")
        .setDataSize(BLOCK_SIZE)
        .setReplicationConfig(replicationConfig)
        .build();

    OpenKeySession session = new OpenKeySession(1L, keyInfo, 0L);
    StreamBufferArgs streamBufferArgs = StreamBufferArgs.getDefaultStreamBufferArgs(replicationConfig, config);

    KeyOutputStream keyStream = new KeyOutputStream.Builder()
        .setHandler(session)
        .setXceiverClientManager(createSharedClientFactory(pipelines))
        .setOmClient(omClient)
        .setConfig(config)
        .setStreamBufferArgs(streamBufferArgs)
        .setReplicationConfig(replicationConfig)
        .setClientMetrics(ContainerClientMetrics.acquire())
        .setExecutorServiceSupplier(() -> newFixedThreadPool(4))
        .setOmVersion(OzoneManagerVersion.SOFTWARE_VERSION)
        .build();

    OmKeyLocationInfo firstBlock = new OmKeyLocationInfo.Builder()
        .setBlockID(pipelines[0].getBlockID())
        .setPipeline(pipelines[0].getPipeline())
        .setLength(BLOCK_SIZE)
        .build();
    OmKeyLocationInfoGroup version = new OmKeyLocationInfoGroup(0, Collections.singletonList(firstBlock));
    keyStream.addPreallocateBlocks(version, 0);

    return keyStream;
  }

  /**
   * Creates a mock OM client that allocates blocks from mocked pipelines in order.
   */
  private OzoneManagerProtocol createOmClient(MockRatisPipeline... pipelines) throws IOException {
    OzoneManagerProtocol omClient = mock(OzoneManagerProtocol.class);
    AtomicInteger allocIndex = new AtomicInteger(0);
    doAnswer(invocation -> {
      int idx = allocIndex.getAndIncrement();
      if (idx >= pipelines.length) {
        throw new IOException("No more blocks to allocate");
      }
      MockRatisPipeline pipeline = pipelines[idx];
      return new OmKeyLocationInfo.Builder()
          .setBlockID(pipeline.getBlockID())
          .setPipeline(pipeline.getPipeline())
          .setLength(BLOCK_SIZE)
          .build();
    }).when(omClient).allocateBlock(any(OmKeyArgs.class), anyLong(), any(ExcludeList.class));
    return omClient;
  }

  @Test
  void writeAndCloseCommitsKey() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    OzoneManagerProtocol omClient = createOmClient(pipeline);

    try (KeyOutputStream stream = createKeyStream(omClient, pipeline)) {
      writeRandom(stream, 300);
    }

    verify(omClient, times(1)).commitKey(any(OmKeyArgs.class), anyLong());
  }

  @Test
  void writeCrossBlockBoundary() throws Exception {
    MockRatisPipeline pipeline1 = new MockRatisPipeline(new BlockID(1, 1));
    MockRatisPipeline pipeline2 = new MockRatisPipeline(new BlockID(2, 2));

    // OM returns pipeline2 when allocateBlock is called
    OzoneManagerProtocol omClient = createOmClient(pipeline2);

    // The first block (pipeline1) has BLOCK_SIZE=800 capacity. Both mocks must be known to the shared client factory.
    try (KeyOutputStream stream = createKeyStream(omClient, pipeline1, pipeline2)) {
      writeRandom(stream, 850);
    }

    // allocateBlock should have been called for the second block
    verify(omClient, times(1)).allocateBlock(any(OmKeyArgs.class), anyLong(), any(ExcludeList.class));
    verify(omClient, times(1)).commitKey(any(OmKeyArgs.class), anyLong());

    // pipeline1 should have received 800 bytes, pipeline2 should have received 50
    assertEquals(800, totalReceived(pipeline1));
    assertEquals(50, totalReceived(pipeline2));
  }

  @Test
  void hsyncCallsOmHsyncKey() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    OzoneManagerProtocol omClient = createOmClient(pipeline);

    try (KeyOutputStream stream = createKeyStream(omClient, pipeline)) {
      writeRandom(stream, 200);
      stream.hsync();

      verify(omClient, times(1)).hsyncKey(any(OmKeyArgs.class), anyLong());
    }
  }

  @Test
  void hsyncWithBlockErrorDoesNotCallOmHsync() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    // First putBlock will fail
    pipeline.failPutBlockAfter(0, () -> new IOException("putBlock failed"));
    OzoneManagerProtocol omClient = createOmClient(pipeline);

    KeyOutputStream stream = createKeyStream(omClient, pipeline);
    writeRandom(stream, 200);

    // hsync should throw because the block-level flush failed
    assertThrows(IOException.class, stream::hsync, "hsync() must throw when block-level flush fails");

    // OM hsyncKey must NOT have been called — data was not committed
    verify(omClient, never()).hsyncKey(any(OmKeyArgs.class), anyLong());

    stream.close();
  }

  @Test
  void containerCloseTriggersRetryOnNewBlock() throws Exception {
    MockRatisPipeline pipeline1 = new MockRatisPipeline(new BlockID(1, 1));
    MockRatisPipeline pipeline2 = new MockRatisPipeline(new BlockID(2, 2));

    // First pipeline: putBlock fails with ContainerNotOpen
    pipeline1.failPutBlockAfter(0,
        () -> new StorageContainerException("Container closed", ContainerProtos.Result.CLOSED_CONTAINER_IO));

    OzoneManagerProtocol omClient = createOmClient(pipeline2);

    try (KeyOutputStream stream = createKeyStream(omClient, pipeline1, pipeline2)) {
      writeRandom(stream, 200);
      // The flush on close will hit the container closed error, trigger exception handling, allocate a new block on
      // pipeline2, and retry to write there.
    }

    // allocateBlock should have been called (for the retry block)
    verify(omClient, times(1)).allocateBlock(any(OmKeyArgs.class), anyLong(), any(ExcludeList.class));
    verify(omClient, times(1)).commitKey(any(OmKeyArgs.class), anyLong());
  }

  @Test
  void multipleHsyncsCallOmAtLeastOnce() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    OzoneManagerProtocol omClient = createOmClient(pipeline);

    try (KeyOutputStream stream = createKeyStream(omClient, pipeline)) {
      writeRandom(stream, 200);
      stream.hsync();

      writeRandom(stream, 200);
      stream.hsync();

      // hsyncKey is called at least once; the second call is skipped because the block ID hasn't changed
      // (OM optimization at BlockOutputStreamEntryPool.hsyncKey).
      verify(omClient, times(1)).hsyncKey(any(OmKeyArgs.class), anyLong());

      // But both hsyncs should have flushed data to the datanode
      assertEquals(400, totalReceived(pipeline));
    }
  }

  @Test
  void writeAfterCloseThrows() throws Exception {
    MockRatisPipeline pipeline = new MockRatisPipeline();
    KeyOutputStream stream = createKeyStream(createOmClient(pipeline), pipeline);

    writeRandom(stream, 100);
    stream.close();

    assertThrows(IOException.class, () -> writeRandom(stream, 100), "write() after close() should throw");
  }

  @Test
  void testConcurrentWriteLimitOne() throws Exception {
    // Verify the semaphore is working to limit the number of concurrent writes allowed.
    KeyOutputStreamSemaphore sema1 = new KeyOutputStreamSemaphore(1);
    KeyOutputStream keyOutputStream = spy(KeyOutputStream.class);
    when(keyOutputStream.getRequestSemaphore()).thenReturn(sema1);

    final AtomicInteger countWrite = new AtomicInteger(0);
    // mock write()
    doAnswer(invocation -> {
      countWrite.getAndIncrement();
      return invocation.callRealMethod();
    }).when(keyOutputStream).write(any(), anyInt(), anyInt());

    final ConcurrentHashMap<Long, CountDownLatch> mapNotifiers = new ConcurrentHashMap<>();

    final AtomicInteger countHandleWrite = new AtomicInteger(0);
    // mock handleWrite()
    doAnswer(invocation -> {
      final long tid = Thread.currentThread().getId();
      System.out.println("handleWrite() called from tid " + tid);
      final CountDownLatch latch = mapNotifiers.compute(tid, (k, v) ->
          v != null ? v : new CountDownLatch(1));
      countHandleWrite.getAndIncrement();
      // doing some "work"
      latch.await();
      return null;
    }).when(keyOutputStream).handleWrite(any(), anyInt(), anyLong(), anyBoolean());

    final Runnable writeRunnable = () -> {
      try {
        keyOutputStream.write(new byte[4], 0, 4);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };

    final Thread thread1 = new Thread(writeRunnable);
    thread1.start();

    final Thread thread2 = new Thread(writeRunnable);
    thread2.start();

    // Wait for both threads to enter write()
    GenericTestUtils.waitFor(() -> countWrite.get() == 2, 100, 3000);
    // One thread should enter handleWrite()
    GenericTestUtils.waitFor(() -> countHandleWrite.get() == 1, 100, 3000);
    // The other thread is waiting on the semaphore
    GenericTestUtils.waitFor(() -> sema1.getQueueLength() == 1, 100, 3000);

    // handleWrite is triggered only once because of the semaphore and the synchronized block
    verify(keyOutputStream, times(1)).handleWrite(any(), anyInt(), anyLong(), anyBoolean());

    // Now, allow the current thread to finish handleWrite
    // There is only one thread in handleWrite() so mapNotifiers should have only one entry.
    assertEquals(1, mapNotifiers.size());
    Entry<Long, CountDownLatch> entry = mapNotifiers.entrySet().stream().findFirst().get();
    mapNotifiers.remove(entry.getKey());
    entry.getValue().countDown();

    // Wait for the other thread to proceed
    GenericTestUtils.waitFor(() -> countHandleWrite.get() == 2, 100, 3000);
    verify(keyOutputStream, times(2)).handleWrite(any(), anyInt(), anyLong(), anyBoolean());

    // Allow the other thread to finish handleWrite
    entry = mapNotifiers.entrySet().stream().findFirst().get();
    mapNotifiers.remove(entry.getKey());
    entry.getValue().countDown();

    // Let threads finish
    thread2.join();
    thread1.join();
  }
  
  @Test
  void testGetCurrentStreamEntryDuringBlockAllocation() throws Exception {
    CountDownLatch allocationStarted = new CountDownLatch(1);
    CountDownLatch finishAllocation = new CountDownLatch(1);
    OzoneManagerProtocol om = mock(OzoneManagerProtocol.class);
    OmKeyLocationInfo next = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(1, 2)).setLength(1024)
        .setPipeline(mock(Pipeline.class)).build();
    // Pause inside allocateBlock() to keep the pool in the intermediate state.
    when(om.allocateBlock(any(), anyLong(), any())).thenAnswer(invocation -> {
      allocationStarted.countDown();
      assertTrue(finishAllocation.await(10, TimeUnit.SECONDS));
      return next;
    });
    BlockOutputStreamEntryPool pool = new BlockOutputStreamEntryPool(new KeyOutputStream.Builder()
        .setConfig(new OzoneClientConfig()).setOmClient(om)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setHandler(new OpenKeySession(1, new OmKeyInfo.Builder()
            .setVolumeName("v").setBucketName("b").setKeyName("k").build(), 0))
        .setStreamBufferArgs(StreamBufferArgs.Builder.getNewBuilder()
            .setBufferSize(1024).setBufferFlushSize(1024).setBufferMaxSize(2048).build()));
    // Add a closed entry so allocateBlockIfNeeded() must advance the index and allocate.
    BlockOutputStreamEntry closed = mock(BlockOutputStreamEntry.class);
    when(closed.isClosed()).thenReturn(true);
    pool.getStreamEntries().add(closed);

    // writer allocates (holds the pool monitor) and the reader does the concurrent lookup (as hsync).
    FutureTask<BlockOutputStreamEntry> allocation = new FutureTask<>(() -> pool.allocateBlockIfNeeded(false));
    FutureTask<BlockOutputStreamEntry> read = new FutureTask<>(pool::getCurrentStreamEntry);
    Thread writer = new Thread(allocation, "block-allocator");
    Thread reader = new Thread(read, "entry-reader");
    ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    writer.start();
    try {
      // Wait until the writer is paused inside allocateBlock().
      assertTrue(allocationStarted.await(5, TimeUnit.SECONDS));
      reader.start();
      // Deterministic sync proceed once reader is done or BLOCKED on the pool monitor.
      GenericTestUtils.waitFor(() -> {
        ThreadInfo info = threads.getThreadInfo(reader.getId());
        return read.isDone() || (info != null && info.getThreadState() == Thread.State.BLOCKED
            && info.getLockOwnerId() == writer.getId()
            && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(pool));
      }, 10, 5000);
    } finally {
      // Release allocation (appends the entry) and wait for both threads.
      finishAllocation.countDown();
      writer.join(5000);
      reader.join(5000);
    }
    assertFalse(writer.isAlive(), "Allocation should complete");
    assertFalse(reader.isAlive(), "Lookup should complete");
    // Reader must see the allocated entry, not the intermediate null (fails without the fix).
    assertSame(allocation.get(5, TimeUnit.SECONDS), read.get(5, TimeUnit.SECONDS),
        "Reader must not observe the intermediate index before the new entry is added");
  }

  private static int totalReceived(MockRatisPipeline pipeline) {
    return pipeline.getReceivedChunks().stream().mapToInt(c -> c.length).sum();
  }

  private static void writeRandom(KeyOutputStream stream, int length) throws IOException {
    byte[] data = RandomUtils.secure().randomBytes(length);
    stream.write(data, 0, data.length);
  }
}
