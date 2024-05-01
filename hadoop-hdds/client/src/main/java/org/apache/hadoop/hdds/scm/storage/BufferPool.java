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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.apache.hadoop.ozone.common.ChunkBuffer;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyList;

/**
 * This class creates and manages pool of n buffers.
 */
public class BufferPool {

  public static final Logger LOG = LoggerFactory.getLogger(BufferPool.class);

  private static final BufferPool EMPTY = new BufferPool(0, 0);

  private final List<ChunkBuffer> bufferList;
  private int currentBufferIndex;
  private final int bufferSize;
  private final int capacity;
  private final Function<ByteBuffer, ByteString> byteStringConversion;

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
    bufferList = capacity == 0 ? emptyList() : new ArrayList<>(capacity);
    currentBufferIndex = -1;
    this.byteStringConversion = byteStringConversion;
  }

  public synchronized Function<ByteBuffer, ByteString> byteStringConversion() {
    return byteStringConversion;
  }

  synchronized ChunkBuffer getCurrentBuffer() {
    return currentBufferIndex == -1 ? null : bufferList.get(currentBufferIndex);
  }

  /**
   * If the currentBufferIndex is less than the buffer size - 1,
   * it means, the next buffer in the list has been freed up for
   * rewriting. Reuse the next available buffer in such cases.
   * <p>
   * In case, the currentBufferIndex == buffer.size and buffer size is still
   * less than the capacity to be allocated, just allocate a buffer of size
   * chunk size.
   */
  public synchronized ChunkBuffer allocateBuffer(int increment) {
    final int nextBufferIndex = currentBufferIndex + 1;

    Preconditions.assertTrue(nextBufferIndex < capacity, () ->
        "next index: " + nextBufferIndex + " >= capacity: " + capacity);

    currentBufferIndex = nextBufferIndex;

    LOG.warn("!! allocateBuffer(increment = {}): " +
            "capacity = {}, currentBufferIndex = {}, nextBufferIndex = {}, bufferList.size() = {}",
        increment,
        capacity, currentBufferIndex, nextBufferIndex, bufferList.size());

    if (currentBufferIndex < bufferList.size()) {
      return getBuffer(currentBufferIndex);
    } else {
      final ChunkBuffer newBuffer = ChunkBuffer.allocate(bufferSize, increment);
      LOG.warn("!! allocateBuffer(): ChunkBuffer allocated: {}", newBuffer);
      bufferList.add(newBuffer);
      return newBuffer;
    }
  }

  synchronized void releaseBuffer(ChunkBuffer chunkBuffer) {
    LOG.warn("!! releaseBuffer(chunkBuffer = {}):" +
            "currentBufferIndex = {}, bufferList.indexOf(chunkBuffer) = {}",
        chunkBuffer,
        currentBufferIndex, bufferList.indexOf(chunkBuffer));

    Preconditions.assertTrue(!bufferList.isEmpty(), "empty buffer list");
//    Preconditions.assertSame(bufferList.get(0), chunkBuffer,
//        "only the first buffer can be released");
    Preconditions.assertTrue(currentBufferIndex >= 0,
        () -> "current buffer: " + currentBufferIndex);

    // always remove from head of the list and append at last
//    boolean res = bufferList.remove(chunkBuffer);  // This does NOT work as intended
    int i;
    ChunkBuffer buffer = null;
    for (i = 0; i < bufferList.size(); i++) {
      // force comparison by object ID, overriding ByteBuffer's equals() method
      if (chunkBuffer == bufferList.get(i)) {
        buffer = bufferList.remove(i);
        break;
      }
    }
    Preconditions.assertSame(chunkBuffer, buffer, "Should be the same buffer");
    chunkBuffer.clear();
    bufferList.add(chunkBuffer);
    currentBufferIndex--;
  }

  public synchronized void clearBufferPool() {
    bufferList.forEach(ChunkBuffer::close);
    bufferList.clear();
    currentBufferIndex = -1;
  }

  public synchronized void checkBufferPoolEmpty() {
    Preconditions.assertSame(0, computeBufferData(), "total buffer size");
  }

  public synchronized long computeBufferData() {
    long totalBufferSize = 0;
    for (ChunkBuffer buf : bufferList) {
      totalBufferSize += buf.position();
    }
    return totalBufferSize;
  }

  public synchronized int getSize() {
    return bufferList.size();
  }

  public synchronized ChunkBuffer getBuffer(int index) {
    return bufferList.get(index);
  }

  synchronized int getCurrentBufferIndex() {
    return currentBufferIndex;
  }

  public synchronized int getNumberOfUsedBuffers() {
    return currentBufferIndex + 1;
  }

  public synchronized int getCapacity() {
    return capacity;
  }

  public synchronized int getBufferSize() {
    return bufferSize;
  }
}
