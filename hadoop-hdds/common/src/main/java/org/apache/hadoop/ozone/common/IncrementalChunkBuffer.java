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
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * Use a list of {@link ByteBuffer} to implement a single {@link ChunkBuffer}
 * so that the buffer can be allocated incrementally.
 */
final class IncrementalChunkBuffer implements ChunkBuffer {
  /**
   * The limit of the entire {@link ChunkBuffer},
   * but not individual {@link ByteBuffer}(s) in the list.
   */
  private final int limit;
  /** Increment is the capacity of each {@link ByteBuffer} in the list. */
  private final int increment;
  /** The index at limit. */
  private final int limitIndex;
  /** Buffer list to be allocated incrementally. */
  private final List<ByteBuffer> buffers;
  /** The underlying buffers. */
  private final List<CodecBuffer> underlying;
  /** Is this a duplicated buffer? (for debug only) */
  private final boolean isDuplicated;
  /** The index of the first non-full buffer. */
  private int firstNonFullIndex = 0;

  IncrementalChunkBuffer(int limit, int increment, boolean isDuplicated) {
    Preconditions.checkArgument(limit >= 0);
    Preconditions.checkArgument(increment > 0);
    this.limit = limit;
    this.increment = increment;
    this.limitIndex = limit / increment;
    int size = limitIndex + (limit % increment == 0 ? 0 : 1);
    this.buffers = new ArrayList<>(size);
    this.underlying = isDuplicated ? Collections.emptyList() : new ArrayList<>(size);
    this.isDuplicated = isDuplicated;
  }

  @Override
  public void close() {
    underlying.forEach(CodecBuffer::release);
    underlying.clear();
  }

  /** @return the capacity for the buffer at the given index. */
  private int getBufferCapacityAtIndex(int i) {
    Preconditions.checkArgument(i >= 0);
    Preconditions.checkArgument(i <= limitIndex);
    return i < limitIndex ? increment : limit % increment;
  }

  private void assertInt(int expected, int computed, String name, int i) {
    ChunkBufferToByteString.assertInt(expected, computed,
        () -> this + ": Unexpected " + name + " at index " + i);
  }

  /** @return the i-th buffer if it exists; otherwise, return null. */
  private ByteBuffer getAtIndex(int i) {
    Preconditions.checkArgument(i >= 0);
    Preconditions.checkArgument(i <= limitIndex);
    final ByteBuffer ith = i < buffers.size() ? buffers.get(i) : null;
    if (ith != null) {
      // assert limit/capacity
      if (!isDuplicated) {
        assertInt(getBufferCapacityAtIndex(i), ith.capacity(), "capacity", i);
      } else {
        if (i < limitIndex) {
          assertInt(increment, ith.capacity(), "capacity", i);
        } else if (i == limitIndex) {
          assertInt(getBufferCapacityAtIndex(i), ith.limit(), "capacity", i);
        } else {
          assertInt(0, ith.limit(), "capacity", i);
        }
      }
    }
    return ith;
  }

  /** @return the i-th buffer. It may allocate buffers. */
  private ByteBuffer getAndAllocateAtIndex(int index) {
    Preconditions.checkState(!isDuplicated, "Duplicated buffer is readonly.");
    Preconditions.checkArgument(index >= 0);
    // never allocate over limit
    if (limit % increment == 0) {
      Preconditions.checkArgument(index < limitIndex);
    } else {
      Preconditions.checkArgument(index <= limitIndex);
    }

    int i = buffers.size();
    if (index < i) {
      return getAtIndex(index);
    }

    // allocate upto the given index
    ByteBuffer b = null;
    for (; i <= index; i++) {
      final CodecBuffer c = CodecBuffer.allocateDirect(getBufferCapacityAtIndex(i));
      underlying.add(c);
      b = c.asWritableByteBuffer();
      buffers.add(b);
    }
    return b;
  }

  /** @return the buffer containing the position. It may allocate buffers. */
  private ByteBuffer getAndAllocateAtPosition(int position) {
    Preconditions.checkArgument(position >= 0);
    Preconditions.checkArgument(position < limit);
    final int i = position / increment;
    final ByteBuffer ith = getAndAllocateAtIndex(i);
    assertInt(position % increment, ith.position(), "position", i);
    return ith;
  }

  /** @return the index of the first non-full buffer. */
  private int firstNonFullIndex() {
    for (int i = firstNonFullIndex; i < buffers.size(); i++) {
      if (getAtIndex(i).position() != increment) {
        firstNonFullIndex = i;
        return firstNonFullIndex;
      }
    }
    firstNonFullIndex = buffers.size();
    return firstNonFullIndex;
  }

  @Override
  public int position() {
    // The buffers list must be in the following orders:
    // full buffers, buffer containing the position, empty buffers, null buffers
    final int i = firstNonFullIndex();
    final ByteBuffer ith = getAtIndex(i);
    final int position = i * increment + Optional.ofNullable(ith)
        .map(ByteBuffer::position).orElse(0);
    // remaining buffers must be empty
    assert assertRemainingList(ith, i);
    return position;
  }

  private boolean assertRemainingList(ByteBuffer ith, int i) {
    if (ith != null) {
      // buffers must be empty
      for (i++; i < buffers.size(); i++) {
        ith = getAtIndex(i);
        if (ith == null) {
          break; // found the first non-null
        }
        assertInt(0, ith.position(), "position", i);
      }
    }
    final int j = i;
    ChunkBufferToByteString.assertInt(buffers.size(), i,
        () -> "i = " + j + " != buffers.size() = " + buffers.size());
    return true;
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
  public ChunkBuffer rewind() {
    buffers.forEach(ByteBuffer::rewind);
    firstNonFullIndex = 0;
    return this;
  }

  @Override
  public ChunkBuffer clear() {
    buffers.forEach(ByteBuffer::clear);
    firstNonFullIndex = 0;
    return this;
  }

  @Override
  public ChunkBuffer put(ByteBuffer that) {
    if (that.remaining() > this.remaining()) {
      final BufferOverflowException boe = new BufferOverflowException();
      boe.initCause(new IllegalArgumentException(
          "Failed to put since that.remaining() = " + that.remaining()
              + " > this.remaining() = " + this.remaining()));
      throw boe;
    }

    final int thatLimit = that.limit();
    for (int p = position(); that.position() < thatLimit;) {
      final ByteBuffer b = getAndAllocateAtPosition(p);
      final int min = Math.min(b.remaining(), thatLimit - that.position());
      that.limit(that.position() + min);
      b.put(that);
      p += min;
    }
    return this;
  }

  @Override
  public ChunkBuffer duplicate(int newPosition, int newLimit) {
    Preconditions.checkArgument(newPosition >= 0);
    Preconditions.checkArgument(newPosition <= newLimit);
    Preconditions.checkArgument(newLimit <= limit);
    final IncrementalChunkBuffer duplicated = new IncrementalChunkBuffer(
        newLimit, increment, true);

    final int pi = newPosition / increment;
    final int pr = newPosition % increment;
    final int li = newLimit / increment;
    final int lr = newLimit % increment;
    final int newSize = lr == 0 ? li : li + 1;

    for (int i = 0; i < newSize; i++) {
      final int pos = i < pi ? increment : i == pi ? pr : 0;
      final int lim = i < li ? increment : i == li ? lr : 0;
      duplicated.buffers.add(duplicate(i, pos, lim));
    }
    return duplicated;
  }

  private ByteBuffer duplicate(int i, int pos, int lim) {
    final ByteBuffer ith = getAtIndex(i);
    Objects.requireNonNull(ith, () -> "buffers[" + i + "] == null");
    final ByteBuffer b = ith.duplicate();
    b.position(pos).limit(lim);
    return b;
  }

  /** Support only when bufferSize == increment. */
  @Override
  public Iterable<ByteBuffer> iterate(int bufferSize) {
    if (bufferSize != increment) {
      throw new UnsupportedOperationException(
          "Buffer size and increment mismatched: bufferSize = " + bufferSize
          + " but increment = " + increment);
    }
    return asByteBufferList();
  }

  @Override
  public List<ByteBuffer> asByteBufferList() {
    return Collections.unmodifiableList(buffers);
  }

  @Override
  public long writeTo(GatheringByteChannel channel) throws IOException {
    return BufferUtils.writeFully(channel, buffers);
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
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (!(obj instanceof IncrementalChunkBuffer)) {
      return false;
    }
    final IncrementalChunkBuffer that = (IncrementalChunkBuffer)obj;
    return this.limit == that.limit && this.buffers.equals(that.buffers);
  }

  @Override
  public int hashCode() {
    return buffers.hashCode();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + ":limit=" + limit + ",increment=" + increment;
  }
}

