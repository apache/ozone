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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

/**
 * Test for {@link BufferPool}.
 */
class TestBufferPool {

  @BeforeAll
  static void init() {
    GenericTestUtils.setLogLevel(BufferPool.class, Level.DEBUG);
  }

  @Test
  void testBufferPool() throws Exception {
    testBufferPool(BufferPool.empty());
    testBufferPool(1, 1);
    testBufferPool(3, 1 << 20);
    testBufferPool(10, 1 << 10);
  }

  @Test
  void testBufferPoolConcurrently() throws Exception {
    final BufferPool pool = new BufferPool(1 << 20, 10);
    final Deque<ChunkBuffer> buffers = assertAllocate(pool);

    assertAllocationBlocked(pool);
    assertAllocationBlockedUntilReleased(pool, buffers);
  }

  private void assertAllocationBlockedUntilReleased(BufferPool pool, Deque<ChunkBuffer> buffers) throws Exception {
    // As the pool is full, allocation will need to wait until a buffer is released.
    assertFull(pool);

    LogCapturer logCapturer = LogCapturer.captureLogs(BufferPool.class);
    AtomicReference<ChunkBuffer> allocated = new AtomicReference<>();
    AtomicBoolean allocatorStarted = new AtomicBoolean();
    Thread allocator = new Thread(() -> {
      try {
        allocatorStarted.set(true);
        allocated.set(pool.allocateBuffer(0));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    ChunkBuffer toRelease = buffers.removeFirst();
    Thread releaser = new Thread(() -> pool.releaseBuffer(toRelease));

    allocator.start();
    // ensure allocator has already started.
    while (!allocatorStarted.get()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    releaser.start();
    allocator.join();
    assertEquals(toRelease, allocated.get());
    assertTrue(logCapturer.getOutput().contains("Allocation needs to wait the pool is at capacity"));
  }

  private void assertAllocationBlocked(BufferPool pool) throws Exception {
    // As the pool is full, new allocation will be blocked interruptably if no allocated buffer is released.
    assertFull(pool);

    LogCapturer logCapturer = LogCapturer.captureLogs(BufferPool.class);
    AtomicBoolean allocatorStarted = new AtomicBoolean();
    AtomicBoolean interrupted = new AtomicBoolean(false);
    Thread allocator = new Thread(() -> {
      try {
        allocatorStarted.set(true);
        pool.allocateBuffer(0);
      } catch (InterruptedException e) {
        interrupted.set(true);
      }
    });

    allocator.start();
    // ensure allocator has already started.
    while (!allocatorStarted.get()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    // Now the allocator is stuck because pool is full and no one releases.
    // And it can be interrupted.
    allocator.interrupt();
    allocator.join();
    assertTrue(interrupted.get());
    assertTrue(logCapturer.getOutput().contains("Allocation needs to wait the pool is at capacity"));
  }

  private static void testBufferPool(final int capacity, final int bufferSize) throws Exception {
    final BufferPool pool = new BufferPool(bufferSize, capacity);
    assertEquals(capacity, pool.getCapacity());
    assertEquals(bufferSize, pool.getBufferSize());
    testBufferPool(pool);
  }

  private static void testBufferPool(final BufferPool pool) throws Exception {
    assertEmpty(pool);
    final Deque<ChunkBuffer> buffers = assertAllocate(pool);
    assertFull(pool);
    assertReallocate(pool, buffers);
    assertRelease(pool, buffers);
  }

  private static void assertEmpty(BufferPool pool) {
    assertEquals(0, pool.getNumberOfUsedBuffers());
    assertEquals(0, pool.getSize());
    assertNull(pool.getCurrentBuffer());
  }

  private static Deque<ChunkBuffer> assertAllocate(BufferPool pool) throws Exception {
    final int capacity = pool.getCapacity();
    final int size = pool.getBufferSize();
    final Deque<ChunkBuffer> buffers = new LinkedList<>();

    for (int i = 0; i < capacity; i++) {
      final int n = i;
      assertEquals(n, pool.getSize());
      assertEquals(n, pool.getNumberOfUsedBuffers());

      final ChunkBuffer allocated = pool.allocateBuffer(0);
      assertEmpty(allocated, size);
      fill(allocated); // make buffer contents unique, for equals check

      assertThat(buffers).withFailMessage("buffer " + n + ": " + allocated + " already in: " + buffers)
          .doesNotContain(allocated);
      buffers.addLast(allocated);
    }

    return buffers;
  }

  private static void assertEmpty(ChunkBuffer buf, final int size) {
    assertEquals(0, buf.position());
    assertEquals(size, buf.limit());
  }

  private static void assertFull(BufferPool pool) {
    final int capacity = pool.getCapacity();
    assertEquals(capacity, pool.getSize());
    assertEquals(capacity, pool.getNumberOfUsedBuffers());
  }

  // buffers are released and reallocated
  private static void assertReallocate(BufferPool pool,
      Deque<ChunkBuffer> buffers) throws Exception {
    final int capacity = pool.getCapacity();
    for (int i = 0; i < 3 * capacity; i++) {
      final ChunkBuffer released = buffers.removeFirst();
      pool.releaseBuffer(released);
      assertEquals(0, released.position());
      assertEquals(capacity - 1, pool.getNumberOfUsedBuffers());
      assertEquals(capacity, pool.getSize());

      final ChunkBuffer allocated = pool.allocateBuffer(0);
      buffers.addLast(allocated);
      assertSame(released, allocated);
      assertEquals(capacity, pool.getNumberOfUsedBuffers());
    }
  }

  private static void assertRelease(BufferPool pool, Deque<ChunkBuffer> buffers) {
    // assert that allocated buffers can be released in any order.
    final int capacity = pool.getCapacity();
    boolean pickFirst = false;
    for (int i = capacity - 1; i >= 0; i--) {
      final ChunkBuffer released = pickFirst ? buffers.removeFirst() : buffers.removeLast();
      pickFirst = !pickFirst;
      pool.releaseBuffer(released);
      assertEquals(i, pool.getNumberOfUsedBuffers());
      assertThrows(IllegalStateException.class,
          () -> pool.releaseBuffer(released));
    }

    assertEquals(0, pool.computeBufferData());
  }

  private static void fill(ChunkBuffer buf) {
    buf.clear();
    final byte[] bytes = new byte[buf.remaining()];
    ThreadLocalRandom.current().nextBytes(bytes);
    buf.put(bytes);
    buf.rewind();
  }

}
