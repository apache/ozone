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

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bounded pool implementation that provides {@link ChunkBuffer}s. This pool allows allocating and releasing
 * {@link ChunkBuffer}.
 * This pool is designed for concurrent access to allocation and release. It imposes a maximum number of buffers to be
 * allocated at the same time and once the limit has been approached, the thread requesting a new allocation needs to
 * wait until a allocated buffer is released.
 */
public class BufferPool {
  private static final Logger LOG = LoggerFactory.getLogger(BufferPool.class);

  private static final BufferPool EMPTY = new BufferPool(0, 0);
  private final int bufferSize;
  private final int capacity;
  private final Function<ByteBuffer, ByteString> byteStringConversion;

  private final LinkedList<ChunkBuffer> allocated = new LinkedList<>();
  private final LinkedList<ChunkBuffer> released = new LinkedList<>();
  private ChunkBuffer currentBuffer = null;
  private final Lock lock = new ReentrantLock();
  private final Condition notFull = lock.newCondition();

  public static BufferPool empty() {
    return EMPTY;
  }

  public BufferPool(int bufferSize, int capacity) {
    this(bufferSize, capacity,
        ByteStringConversion.createByteBufferConversion(false));
  }

  public BufferPool(int bufferSize, int capacity,
      Function<ByteBuffer, ByteString> byteStringConversion) {
    this.capacity = capacity;
    this.bufferSize = bufferSize;
    this.byteStringConversion = byteStringConversion;
  }

  public Function<ByteBuffer, ByteString> byteStringConversion() {
    return byteStringConversion;
  }

  ChunkBuffer getCurrentBuffer() {
    return doInLock(() -> currentBuffer);
  }

  /**
   * Allocate a new {@link ChunkBuffer}, waiting for a buffer to be released when this pool already allocates at
   * capacity.
   */
  public ChunkBuffer allocateBuffer(int increment) throws InterruptedException {
    lock.lockInterruptibly();
    try {
      Preconditions.assertTrue(allocated.size() + released.size() <= capacity, () ->
          "Total created buffer must not exceed capacity.");

      while (allocated.size() == capacity) {
        LOG.debug("Allocation needs to wait the pool is at capacity (allocated = capacity = {}).", capacity);
        notFull.await();
      }
      // Get a buffer to allocate, preferably from the released ones.
      final ChunkBuffer buffer = released.isEmpty() ?
          ChunkBuffer.allocate(bufferSize, increment) : released.removeFirst();
      allocated.add(buffer);
      currentBuffer = buffer;

      LOG.debug("Allocated new buffer {}, number of used buffers {}, capacity {}.",
          buffer, allocated.size(), capacity);
      return buffer;
    } finally {
      lock.unlock();
    }
  }

  void releaseBuffer(ChunkBuffer buffer) {
    LOG.debug("Releasing buffer {}", buffer);
    lock.lock();
    try {
      Preconditions.assertTrue(removeByIdentity(allocated, buffer), "Releasing unknown buffer");
      buffer.clear();
      released.add(buffer);
      if (buffer == currentBuffer) {
        currentBuffer = null;
      }
      notFull.signal();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Remove an item from a list by identity.
   * @return true if the item is found and removed from the list, otherwise false.
   */
  private static <T> boolean removeByIdentity(List<T> list, T toRemove) {
    int i = 0;
    for (T item : list) {
      if (item == toRemove) {
        break;
      } else {
        i++;
      }
    }
    if (i < list.size()) {
      list.remove(i);
      return true;
    }
    return false;
  }

  /**
   * Wait until one buffer is available.
   * @throws InterruptedException
   */
  @VisibleForTesting
  public void waitUntilAvailable() throws InterruptedException {
    lock.lockInterruptibly();
    try {
      while (allocated.size() == capacity) {
        notFull.await();
      }
    } finally {
      lock.unlock();
    }
  }

  public void clearBufferPool() {
    lock.lock();
    try {
      allocated.forEach(ChunkBuffer::close);
      released.forEach(ChunkBuffer::close);
      allocated.clear();
      released.clear();
      currentBuffer = null;
    } finally {
      lock.unlock();
    }
  }

  public long computeBufferData() {
    return doInLock(() -> {
      long totalBufferSize = 0;
      for (ChunkBuffer buf : allocated) {
        totalBufferSize += buf.position();
      }
      return totalBufferSize;
    });
  }

  public int getSize() {
    return doInLock(() -> allocated.size() + released.size());
  }

  public List<ChunkBuffer> getAllocatedBuffers() {
    return doInLock(() -> new ArrayList<>(allocated));
  }

  public int getNumberOfUsedBuffers() {
    return doInLock(allocated::size);
  }

  private <T> T doInLock(Supplier<T> supplier) {
    lock.lock();
    try {
      return supplier.get();
    } finally {
      lock.unlock();
    }
  }

  public boolean isAtCapacity() {
    return getNumberOfUsedBuffers() == capacity;
  }

  public int getCapacity() {
    return capacity;
  }

  public int getBufferSize() {
    return bufferSize;
  }

}
