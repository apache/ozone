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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamBufferArgs;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.MockDatanodeStorage;
import org.apache.hadoop.ozone.client.MockOmTransport;
import org.apache.hadoop.ozone.client.MockXceiverClientFactory;
import org.apache.hadoop.ozone.client.MockXceiverClientSpi;
import org.apache.hadoop.ozone.client.MultiNodePipelineBlockAllocator;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.event.Level;

/**
 * Tests KeyOutputStream.
 * This is a unit test meant to verify specific behaviors of KeyOutputStream.
 */
public class TestKeyOutputStream {

  @BeforeAll
  static void init() {
    GenericTestUtils.setLogLevel(KeyOutputStreamSemaphore.class, Level.DEBUG);
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

  /**
   * A call fails for good and the stream is closed while a second call is in progress. The buffered data is dropped
   * with the stream, so the second call must fail as well.
   * @param call the second call
   * @param pauseInFlush whether the second call starts when the first call is sending its data to the datanodes, or
   *                     when it is handling the failure
   * @param maxRetries how often the client retries a failed write
   */
  @ParameterizedTest
  @CsvSource({"hsync, true, 5", "hsync, false, 5", "hsync, false, 0", "flush, false, 5", "write, false, 0"})
  @Timeout(120)
  void testCallInProgressFailsWhenStreamFails(String call, boolean pauseInFlush, int maxRetries) throws Exception {
    final byte[] synced = "synced".getBytes(UTF_8);
    final byte[] notSynced = "not synced".getBytes(UTF_8);
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    conf.setInt("ozone.client.key.write.concurrency", -1);
    conf.setInt("ozone.client.max.retries", maxRetries);

    AtomicBoolean failWrites = new AtomicBoolean();
    AtomicBoolean paused = new AtomicBoolean();
    CountDownLatch firstCallPaused = new CountDownLatch(1);
    CountDownLatch secondCallBlocked = new CountDownLatch(1);
    MockXceiverClientFactory factory = new MockXceiverClientFactory() {
      private void pauseFirstCall(boolean inFlush) {
        if (failWrites.get() && inFlush == pauseInFlush && paused.compareAndSet(false, true)) {
          pause(firstCallPaused, secondCallBlocked);
        }
      }

      @Override
      public XceiverClientSpi acquireClient(Pipeline pipeline, boolean topologyAware, boolean allowShortCircuit)
          throws IOException {
        return new MockXceiverClientSpi(pipeline,
            getStorages().computeIfAbsent(pipeline.getFirstNode(), dn -> new MockDatanodeStorage())) {
          @Override
          public XceiverClientReply sendCommandAsync(ContainerCommandRequestProto request) {
            if (!failWrites.get()) {
              return super.sendCommandAsync(request);
            }
            // The caller holds the monitor of the block stream.
            pauseFirstCall(true);
            return new XceiverClientReply(CompletableFuture.completedFuture(ContainerCommandResponseProto.newBuilder()
                .setCmdType(request.getCmdType()).setResult(Result.CLOSED_CONTAINER_IO).build()));
          }
        };
      }

      @Override
      public void releaseClient(XceiverClientSpi client, boolean invalidateClient) {
        // The failure handling releases the client of the failed block with the write lock of the key stream held.
        pauseFirstCall(false);
      }
    };

    Thread first = null;
    Thread second = null;
    try (OzoneClient client = newClient(conf, factory)) {
      OzoneBucket bucket = createBucket(client);
      OzoneOutputStream out = bucket.createKey("key", 0, RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
          new HashMap<>());
      out.write(synced);
      out.hsync();
      out.write(notSynced);
      failWrites.set(true);

      FutureTask<Void> firstCall = new FutureTask<>(() -> {
        out.hsync();
        return null;
      });
      FutureTask<Void> secondCall = new FutureTask<>(() -> {
        if ("hsync".equals(call)) {
          out.hsync();
        } else if ("flush".equals(call)) {
          out.flush();
        } else {
          out.write(notSynced);
        }
        return null;
      });
      first = new Thread(firstCall, "first-call");
      second = new Thread(secondCall, "second-call");
      first.start();
      assertThat(firstCallPaused.await(30, TimeUnit.SECONDS)).isTrue();
      second.start();
      // The second call is in progress when it waits for a lock that the first call holds.
      waitUntilBlockedBy(second, first);
      secondCallBlocked.countDown();

      // Either call can be the one that handles the failure and gives up after its retries.
      assertThatThrownBy(() -> firstCall.get(30, TimeUnit.SECONDS)).hasCauseInstanceOf(IOException.class);
      assertThatThrownBy(() -> secondCall.get(30, TimeUnit.SECONDS)).hasCauseInstanceOf(IOException.class);
      assertThatThrownBy(out::hsync).isInstanceOf(IOException.class).hasMessageContaining("Stream is closed");
      // The data synced before the failure is not affected.
      try (OzoneInputStream in = bucket.readKey("key")) {
        byte[] read = new byte[synced.length + notSynced.length];
        assertThat(in.read(read)).isEqualTo(synced.length);
        assertThat(read).startsWith(synced);
      }
    } finally {
      secondCallBlocked.countDown();
      joinThreads(first, second);
    }
  }

  /**
   * With the default configuration, a flush is in progress when another thread closes the stream. Nothing has
   * failed and close() commits the data, so the flush must return normally.
   */
  @Test
  @Timeout(120)
  void testFlushInProgressSucceedsWhenStreamIsClosed() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    // One full buffer, so that the flush has something to send with the default flush delay.
    final byte[] data = new byte[conf.getObject(OzoneClientConfig.class).getStreamBufferSize()];
    Arrays.fill(data, (byte) 'a');

    AtomicBoolean pauseNextRequest = new AtomicBoolean();
    CountDownLatch flushPaused = new CountDownLatch(1);
    CountDownLatch closeBlocked = new CountDownLatch(1);
    MockXceiverClientFactory factory = new MockXceiverClientFactory() {
      @Override
      public XceiverClientSpi acquireClient(Pipeline pipeline, boolean topologyAware, boolean allowShortCircuit)
          throws IOException {
        return new MockXceiverClientSpi(pipeline,
            getStorages().computeIfAbsent(pipeline.getFirstNode(), dn -> new MockDatanodeStorage())) {
          @Override
          public XceiverClientReply sendCommandAsync(ContainerCommandRequestProto request) {
            if (pauseNextRequest.compareAndSet(true, false)) {
              // The caller holds the monitor of the block stream.
              pause(flushPaused, closeBlocked);
            }
            return super.sendCommandAsync(request);
          }
        };
      }
    };

    Thread flusher = null;
    Thread closer = null;
    try (OzoneClient client = newClient(conf, factory)) {
      OzoneBucket bucket = createBucket(client);
      OzoneOutputStream out = bucket.createKey("key", 0, RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
          new HashMap<>());
      out.write(data);
      pauseNextRequest.set(true);

      // OzoneOutputStream runs hflush() as flush() of the key stream unless hsync is enabled, and does not
      // synchronize it with close().
      FutureTask<Void> flush = new FutureTask<>(() -> {
        out.hflush();
        return null;
      });
      FutureTask<Void> close = new FutureTask<>(() -> {
        out.close();
        return null;
      });
      flusher = new Thread(flush, "flusher");
      closer = new Thread(close, "closer");
      flusher.start();
      assertThat(flushPaused.await(30, TimeUnit.SECONDS)).isTrue();
      closer.start();
      // close() holds the write lock of the key stream and waits for the block stream.
      waitUntilBlockedBy(closer, flusher);
      closeBlocked.countDown();

      flush.get(30, TimeUnit.SECONDS);
      close.get(30, TimeUnit.SECONDS);
      assertThat(bucket.getKey("key").getDataSize()).isEqualTo(data.length);
      try (OzoneInputStream in = bucket.readKey("key")) {
        byte[] read = new byte[data.length];
        IOUtils.readFully(in, read);
        assertThat(read).isEqualTo(data);
      }
    } finally {
      closeBlocked.countDown();
      joinThreads(flusher, closer);
    }
  }

  private static OzoneClient newClient(OzoneConfiguration conf, XceiverClientFactory factory) throws IOException {
    return new OzoneClient(conf, new RpcClient(conf, null) {
      @Override
      protected OmTransport createOmTransport(String omServiceId) {
        return new MockOmTransport(new MultiNodePipelineBlockAllocator(conf, 3, 9));
      }

      @Nonnull
      @Override
      protected XceiverClientFactory createXceiverClientFactory(ServiceInfoEx serviceInfo) {
        return factory;
      }
    });
  }

  private static OzoneBucket createBucket(OzoneClient client) throws IOException {
    ObjectStore store = client.getObjectStore();
    store.createVolume("vol");
    store.getVolume("vol").createBucket("bucket");
    return store.getVolume("vol").getBucket("bucket");
  }

  /** Report that the calling thread has reached the point and hold it there until it is released. */
  private static void pause(CountDownLatch reached, CountDownLatch release) {
    reached.countDown();
    try {
      release.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Wait until the thread waits for a lock that the owner holds. */
  private static void waitUntilBlockedBy(Thread thread, Thread owner) throws Exception {
    ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    GenericTestUtils.waitFor(() -> {
      ThreadInfo info = threads.getThreadInfo(thread.getId());
      return info != null && info.getLockOwnerId() == owner.getId();
    }, 10, 30000);
  }

  private static void joinThreads(Thread... threads) throws InterruptedException {
    for (Thread thread : threads) {
      if (thread != null) {
        thread.interrupt();
        thread.join(30000);
      }
    }
  }
}
