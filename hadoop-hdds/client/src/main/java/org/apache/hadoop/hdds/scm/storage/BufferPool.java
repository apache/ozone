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

import com.google.common.base.Preconditions;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * This class creates and manages pool of n buffers.
 */
public class BufferPool {

  private List<ChunkBuffer> bufferList;
  private int currentBufferIndex;
  private final int bufferSize;
  private final int capacity;
  private final Function<ByteBuffer, ByteString> byteStringConversion;

  public BufferPool(int bufferSize, int capacity) {
    this(bufferSize, capacity,
        ByteStringConversion.createByteBufferConversion(false));
  }

  public BufferPool(int bufferSize, int capacity,
      Function<ByteBuffer, ByteString> byteStringConversion){
    this.capacity = capacity;
    this.bufferSize = bufferSize;
    bufferList = new ArrayList<>(capacity);
    currentBufferIndex = -1;
    this.byteStringConversion = byteStringConversion;
  }

  public Function<ByteBuffer, ByteString> byteStringConversion() {
    return byteStringConversion;
  }

  ChunkBuffer getCurrentBuffer() {
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
  public ChunkBuffer allocateBuffer(int increment) {
    currentBufferIndex++;
    Preconditions.checkArgument(currentBufferIndex <= capacity - 1);
    if (currentBufferIndex < bufferList.size()) {
      return getBuffer(currentBufferIndex);
    } else {
      final ChunkBuffer newBuffer = ChunkBuffer.allocate(bufferSize, increment);
      bufferList.add(newBuffer);
      Preconditions.checkArgument(bufferList.size() <= capacity);
      return newBuffer;
    }
  }

  void releaseBuffer(ChunkBuffer chunkBuffer) {
    // always remove from head of the list and append at last
    final ChunkBuffer buffer = bufferList.remove(0);
    // Ensure the buffer to be removed is always at the head of the list.
    Preconditions.checkArgument(buffer == chunkBuffer);
    buffer.clear();
    bufferList.add(buffer);
    Preconditions.checkArgument(currentBufferIndex >= 0);
    currentBufferIndex--;
  }

  public void clearBufferPool() {
    bufferList.clear();
    currentBufferIndex = -1;
  }

  public void checkBufferPoolEmpty() {
    Preconditions.checkArgument(computeBufferData() == 0);
  }

  public long computeBufferData() {
    long totalBufferSize = 0;
    for (ChunkBuffer buf : bufferList) {
      totalBufferSize += buf.position();
    }
    return totalBufferSize;
  }

  public int getSize() {
    return bufferList.size();
  }

  public ChunkBuffer getBuffer(int index) {
    return bufferList.get(index);
  }

  int getCurrentBufferIndex() {
    return currentBufferIndex;
  }

  public int getNumberOfUsedBuffers() {
    return currentBufferIndex + 1;
  }

  public int getCapacity() {
    return capacity;
  }
}
