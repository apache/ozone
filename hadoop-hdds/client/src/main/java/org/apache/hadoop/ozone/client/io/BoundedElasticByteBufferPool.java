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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ComparisonChain;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.ByteBufferPool;

/**
 * A bounded version of ElasticByteBufferPool that limits the total size
 * of buffers that can be cached in the pool. This prevents unbounded memory
 * growth in long-lived rpc clients like S3 Gateway.
 *
 * When the pool reaches its maximum size, newly returned buffers are not
 * added back to the pool and will be garbage collected instead.
 */
public class BoundedElasticByteBufferPool implements ByteBufferPool {
  private final TreeMap<Key, ByteBuffer> buffers = new TreeMap<>();
  private final TreeMap<Key, ByteBuffer> directBuffers = new TreeMap<>();
  private final long maxPoolSize;
  private  final AtomicLong currentPoolSize = new AtomicLong(0);

  /**
   * A logical timestamp counter used for creating unique Keys in the TreeMap.
   * This is used as the insertionTime for the Key instead of System.nanoTime()
   * to guarantee uniqueness and avoid a potential spin-wait in putBuffer
   * if two buffers of the same capacity are added at the same nanosecond.
   */
  private long logicalTimestamp = 0;

  public BoundedElasticByteBufferPool(long maxPoolSize) {
    super();
    this.maxPoolSize = maxPoolSize;
  }

  private TreeMap<Key, ByteBuffer> getBufferTree(boolean direct) {
    return direct ? this.directBuffers : this.buffers;
  }

  @Override
  public synchronized ByteBuffer getBuffer(boolean direct, int length) {
    TreeMap<Key, ByteBuffer> tree = this.getBufferTree(direct);
    Map.Entry<Key, ByteBuffer> entry = tree.ceilingEntry(new Key(length, 0L));
    if (entry == null) {
      // Pool is empty or has no suitable buffer. Allocate a new one.
      return direct ? ByteBuffer.allocateDirect(length) : ByteBuffer.allocate(length);
    }
    tree.remove(entry.getKey());
    ByteBuffer buffer = entry.getValue();

    // Decrement the size because we are taking a buffer OUT of the pool.
    currentPoolSize.addAndGet(-buffer.capacity());
    buffer.clear();
    return buffer;
  }

  @Override
  public synchronized void putBuffer(ByteBuffer buffer) {
    if (buffer == null) {
      return;
    }

    if (currentPoolSize.get() + buffer.capacity() > maxPoolSize) {
      // Pool is full, do not add the buffer back.
      // It will be garbage collected by JVM.
      return;
    }

    buffer.clear();
    TreeMap<Key, ByteBuffer> tree = getBufferTree(buffer.isDirect());
    Key key = new Key(buffer.capacity(), logicalTimestamp++);

    tree.put(key, buffer);
    // Increment the size because we have successfully added buffer back to the pool.
    currentPoolSize.addAndGet(buffer.capacity());
  }

  /**
   * Get the current size of buffers in the pool.
   *
   * @return Current pool size in bytes
   */
  @VisibleForTesting
  public synchronized long getCurrentPoolSize() {
    return currentPoolSize.get();
  }

  /**
   * The Key for the buffer TreeMaps.
   * This is copied directly from the original ElasticByteBufferPool.
   */
  protected static final class Key implements Comparable<Key> {
    private final int capacity;
    private final long insertionTime;

    Key(int capacity, long insertionTime) {
      this.capacity = capacity;
      this.insertionTime = insertionTime;
    }

    @Override
    public int compareTo(Key other) {
      return ComparisonChain.start()
          .compare(this.capacity, other.capacity)
          .compare(this.insertionTime, other.insertionTime)
          .result();
    }

    @Override
    public boolean equals(Object rhs) {
      if (rhs == null) {
        return false;
      }
      try {
        Key o = (Key) rhs;
        return compareTo(o) == 0;
      } catch (ClassCastException e) {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(capacity).append(insertionTime)
          .toHashCode();
    }
  }
}
