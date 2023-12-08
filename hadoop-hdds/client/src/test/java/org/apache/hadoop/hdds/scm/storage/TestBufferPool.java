/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import org.apache.hadoop.ozone.common.ChunkBuffer;

import org.junit.jupiter.api.Test;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test for {@link BufferPool}.
 */
class TestBufferPool {

  @Test
  void testBufferPool() {
    testBufferPool(BufferPool.empty());
    testBufferPool(1, 1);
    testBufferPool(3, 1 << 20);
    testBufferPool(10, 1 << 10);
  }

  private static void testBufferPool(final int capacity, final int bufferSize) {
    final BufferPool pool = new BufferPool(bufferSize, capacity);
    assertEquals(capacity, pool.getCapacity());
    assertEquals(bufferSize, pool.getBufferSize());
    testBufferPool(pool);
  }

  private static void testBufferPool(final BufferPool pool) {
    assertEmpty(pool);
    final Deque<ChunkBuffer> buffers = assertAllocate(pool);
    assertFull(pool);
    assertReallocate(pool, buffers);
    assertRelease(pool, buffers);
  }

  private static void assertEmpty(BufferPool pool) {
    assertEquals(0, pool.getNumberOfUsedBuffers());
    assertEquals(0, pool.getSize());
    assertEquals(-1, pool.getCurrentBufferIndex());
  }

  private static Deque<ChunkBuffer> assertAllocate(BufferPool pool) {
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

      assertFalse(buffers.contains(allocated),
          () -> "buffer " + n + ": " + allocated + " already in: " + buffers);
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
    assertThrows(IllegalStateException.class, () -> pool.allocateBuffer(0));
  }

  // buffers are released and reallocated FIFO
  private static void assertReallocate(BufferPool pool,
      Deque<ChunkBuffer> buffers) {
    final int capacity = pool.getCapacity();
    for (int i = 0; i < 3 * capacity; i++) {
      if (capacity > 1) {
        assertThrows(IllegalStateException.class,
            () -> pool.releaseBuffer(buffers.getLast()));
      }

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

  private static void assertRelease(BufferPool pool,
      Deque<ChunkBuffer> buffers) {
    final int capacity = pool.getCapacity();
    for (int i = capacity - 1; i >= 0; i--) {
      final ChunkBuffer released = buffers.removeFirst();
      pool.releaseBuffer(released);
      assertEquals(i, pool.getNumberOfUsedBuffers());
      assertThrows(IllegalStateException.class,
          () -> pool.releaseBuffer(released));
    }

    pool.checkBufferPoolEmpty();
  }

  private static void fill(ChunkBuffer buf) {
    buf.clear();
    final byte[] bytes = new byte[buf.remaining()];
    ThreadLocalRandom.current().nextBytes(bytes);
    buf.put(bytes);
    buf.rewind();
  }

}
