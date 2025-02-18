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

package org.apache.hadoop.ozone.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * {@link ChunkBuffer} implementation using a list of {@link ByteBuffer}s.
 * Not thread-safe.
 */
public class ChunkBufferImplWithByteBufferList implements ChunkBuffer {

  private static final List<ByteBuffer> EMPTY_BUFFER
      = Collections.singletonList(ByteBuffer.allocate(0));

  /** Buffer list backing the ChunkBuffer. */
  private final List<ByteBuffer> buffers;
  private final int limit;

  private int limitPrecedingCurrent;
  private int currentIndex;

  ChunkBufferImplWithByteBufferList(List<ByteBuffer> buffers) {
    Objects.requireNonNull(buffers, "buffers == null");
    this.buffers = !buffers.isEmpty() ? ImmutableList.copyOf(buffers) :
        EMPTY_BUFFER;
    this.limit = buffers.stream().mapToInt(ByteBuffer::limit).sum();

    findCurrent();
  }

  private void findCurrent() {
    boolean found = false;
    limitPrecedingCurrent = 0;
    for (int i = 0; i < buffers.size(); i++) {
      final ByteBuffer buf = buffers.get(i);
      final int pos = buf.position();
      if (found) {
        Preconditions.checkArgument(pos == 0,
            "all buffers after current one should have position=0");
      } else if (pos < buf.limit()) {
        found = true;
        currentIndex = i;
      } else {
        limitPrecedingCurrent += buf.limit();
      }
    }
    if (!found) {
      currentIndex = buffers.size() - 1;
      limitPrecedingCurrent -= current().limit();
    }
  }

  private ByteBuffer current() {
    return buffers.get(currentIndex);
  }

  private void advanceCurrent() {
    if (currentIndex < buffers.size() - 1) {
      final ByteBuffer current = buffers.get(currentIndex);
      if (!current.hasRemaining()) {
        currentIndex++;
        limitPrecedingCurrent += current.limit();
      }
    }
  }

  private void rewindCurrent() {
    currentIndex = 0;
    limitPrecedingCurrent = 0;
  }

  @Override
  public int position() {
    return limitPrecedingCurrent + current().position();
  }

  @Override
  public int remaining() {
    return limit - position();
  }

  @Override
  public int limit() {
    return limit;
  }

  @Override
  public boolean hasRemaining() {
    return position() < limit;
  }

  @Override
  public ChunkBuffer rewind() {
    buffers.forEach(ByteBuffer::rewind);
    rewindCurrent();
    return this;
  }

  @Override
  public ChunkBuffer clear() {
    buffers.forEach(ByteBuffer::clear);
    rewindCurrent();
    return this;
  }

  @Override
  public ChunkBuffer put(ByteBuffer that) {
    final int thisRemaining = remaining();
    int thatRemaining = that.remaining();
    if (thatRemaining > thisRemaining) {
      final BufferOverflowException boe = new BufferOverflowException();
      boe.initCause(new IllegalArgumentException(
          "Failed to put since that.remaining() = " + thatRemaining
              + " > this.remaining() = " + thisRemaining));
      throw boe;
    }

    while (thatRemaining > 0) {
      final ByteBuffer b = current();
      final int bytes = Math.min(b.remaining(), thatRemaining);
      that.limit(that.position() + bytes);
      b.put(that);
      thatRemaining -= bytes;
      advanceCurrent();
    }

    return this;
  }

  @Override
  public ChunkBuffer duplicate(int newPosition, int newLimit) {
    Preconditions.checkArgument(newPosition >= 0);
    Preconditions.checkArgument(newPosition <= newLimit);
    Preconditions.checkArgument(newLimit <= limit());

    final List<ByteBuffer> duplicates = new ArrayList<>(buffers.size());
    int min = 0;
    for (final ByteBuffer buf : buffers) {
      final int max = min + buf.limit();
      final int pos = relativeToRange(newPosition, min, max);
      final int lim = relativeToRange(newLimit, min, max);

      final ByteBuffer duplicate = buf.duplicate();
      duplicate.position(pos).limit(lim);
      duplicates.add(duplicate);

      min = max;
    }

    return new ChunkBufferImplWithByteBufferList(duplicates);
  }

  /**
   * Returns the next buffer in the list irrespective of the bufferSize.
   */
  @Override
  public Iterable<ByteBuffer> iterate(int bufferSize) {
    Preconditions.checkArgument(bufferSize > 0);

    return () -> new Iterator<ByteBuffer>() {
      @Override
      public boolean hasNext() {
        return hasRemaining();
      }

      @Override
      public ByteBuffer next() {
        if (!hasRemaining()) {
          throw new NoSuchElementException();
        }
        findCurrent();
        ByteBuffer current = buffers.get(currentIndex);

        // If current buffer has enough space or it's the last one, return it.
        if (current.remaining() >= bufferSize || currentIndex == buffers.size() - 1) {
          final ByteBuffer duplicated = current.duplicate();
          int duplicatedLimit = Math.min(current.position() + bufferSize, current.limit());
          duplicated.limit(duplicatedLimit);
          duplicated.position(current.position());

          current.position(duplicatedLimit);
          return duplicated;
        }

        // Otherwise, create a new buffer.
        int newBufferSize = Math.min(bufferSize, remaining());
        ByteBuffer allocated = ByteBuffer.allocate(newBufferSize);
        int remainingToFill = allocated.remaining();

        while (remainingToFill > 0) {
          final ByteBuffer b = current();
          int bytes = Math.min(b.remaining(), remainingToFill);
          b.limit(b.position() + bytes);
          allocated.put(b);
          remainingToFill -= bytes;
          advanceCurrent();
        }

        allocated.flip();

        // Up-to-date current.
        current = buffers.get(currentIndex);
        // Reset
        current.limit(current.capacity());

        return allocated;
      }
    };
  }

  @Override
  public List<ByteBuffer> asByteBufferList() {
    return buffers;
  }

  @Override
  public long writeTo(GatheringByteChannel channel) throws IOException {
    final long written = BufferUtils.writeFully(channel, buffers);
    findCurrent();
    return written;
  }

  @Override
  public ByteString toByteStringImpl(Function<ByteBuffer, ByteString> f) {
    ByteString result = ByteString.EMPTY;
    for (ByteBuffer buffer : buffers) {
      result = result.concat(f.apply(buffer));
    }
    return result;
  }

  @Override
  public List<ByteString> toByteStringListImpl(
      Function<ByteBuffer, ByteString> f) {
    List<ByteString> byteStringList = new ArrayList<>();
    for (ByteBuffer buffer : buffers) {
      byteStringList.add(f.apply(buffer));
    }
    return byteStringList;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + ":n=" + buffers.size()
        + ":p=" + position()
        + ":l=" + limit();
  }

  private static int relativeToRange(int value, int min, int max) {
    final int pos;
    if (value <= min) {
      pos = 0;
    } else if (value <= max) {
      pos = value - min;
    } else {
      pos = max - min;
    }
    return pos;
  }
}
