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
package org.apache.hadoop.ozone.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.hadoop.hdds.scm.ByteStringConversion;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Buffer for a block chunk. */
public interface ChunkBuffer {

  /** Similar to {@link ByteBuffer#allocate(int)}. */
  static ChunkBuffer allocate(int capacity) {
    return allocate(capacity, 0);
  }

  /**
   * Similar to {@link ByteBuffer#allocate(int)}
   * except that it can specify the increment.
   *
   * @param increment
   *   the increment size so that this buffer is allocated incrementally.
   *   When increment <= 0, entire buffer is allocated in the beginning.
   */
  static ChunkBuffer allocate(int capacity, int increment) {
    if (increment > 0 && increment < capacity) {
      return new IncrementalChunkBuffer(capacity, increment, false);
    }
    return new ChunkBufferImplWithByteBuffer(ByteBuffer.allocate(capacity));
  }

  /** Wrap the given {@link ByteBuffer} as a {@link ChunkBuffer}. */
  static ChunkBuffer wrap(ByteBuffer buffer) {
    return new ChunkBufferImplWithByteBuffer(buffer);
  }

  /** Wrap the given list of {@link ByteBuffer}s as a {@link ChunkBuffer}. */
  static ChunkBuffer wrap(List<ByteBuffer> buffers) {
    return new ChunkBufferImplWithByteBufferList(buffers);
  }

  /** Similar to {@link ByteBuffer#position()}. */
  int position();

  /** Similar to {@link ByteBuffer#remaining()}. */
  int remaining();

  /** Similar to {@link ByteBuffer#limit()}. */
  int limit();

  /** Similar to {@link ByteBuffer#rewind()}. */
  ChunkBuffer rewind();

  /** Similar to {@link ByteBuffer#hasRemaining()}. */
  default boolean hasRemaining() {
    return remaining() > 0;
  }

  /** Similar to {@link ByteBuffer#clear()}. */
  ChunkBuffer clear();

  /** Similar to {@link ByteBuffer#put(ByteBuffer)}. */
  ChunkBuffer put(ByteBuffer b);

  /** Similar to {@link ByteBuffer#put(byte[])}. */
  default ChunkBuffer put(byte[] b) {
    return put(ByteBuffer.wrap(b));
  }

  /** Similar to {@link ByteBuffer#put(byte[])}. */
  default ChunkBuffer put(byte b) {
    byte[] buf = new byte[1];
    buf[0] = (byte) b;
    return put(buf, 0, 1);
  }

  /** Similar to {@link ByteBuffer#put(byte[], int, int)}. */
  default ChunkBuffer put(byte[] b, int offset, int length) {
    return put(ByteBuffer.wrap(b, offset, length));
  }

  /** The same as put(b.asReadOnlyByteBuffer()). */
  default ChunkBuffer put(ByteString b) {
    return put(b.asReadOnlyByteBuffer());
  }

  /**
   * Duplicate and then set the position and limit on the duplicated buffer.
   * The new limit cannot be larger than the limit of this buffer.
   *
   * @see ByteBuffer#duplicate()
   */
  ChunkBuffer duplicate(int newPosition, int newLimit);

  /**
   * Iterate the buffer from the current position to the current limit.
   *
   * Upon the iteration complete,
   * the buffer's position will be equal to its limit.
   *
   * @param bufferSize the size of each buffer in the iteration.
   */
  Iterable<ByteBuffer> iterate(int bufferSize);

  List<ByteBuffer> asByteBufferList();

  /**
   * Write the contents of the buffer from the current position to the limit
   * to {@code channel}.
   *
   * @return The number of bytes written, possibly zero
   */
  long writeTo(GatheringByteChannel channel) throws IOException;

  /**
   * Convert this buffer to a {@link ByteString}.
   * The position and limit of this {@link ChunkBuffer} remains unchanged.
   * The given function must preserve the position and limit
   * of the input {@link ByteBuffer}.
   */
  default ByteString toByteString(Function<ByteBuffer, ByteString> function) {
    return toByteStringImpl(b -> applyAndAssertFunction(b, function, this));
  }

  /**
   * Convert this buffer(s) to a list of {@link ByteString}.
   * The position and limit of this {@link ChunkBuffer} remains unchanged.
   * The given function must preserve the position and limit
   * of the input {@link ByteBuffer}.
   */
  default List<ByteString> toByteStringList(
      Function<ByteBuffer, ByteString> function) {
    return toByteStringListImpl(b -> applyAndAssertFunction(b, function, this));
  }

  // for testing
  default ByteString toByteString() {
    return toByteString(ByteStringConversion::safeWrap);
  }

  ByteString toByteStringImpl(Function<ByteBuffer, ByteString> function);

  List<ByteString> toByteStringListImpl(
      Function<ByteBuffer, ByteString> function);

  static void assertInt(int expected, int computed, Supplier<String> prefix) {
    if (expected != computed) {
      throw new IllegalStateException(prefix.get()
          + ": expected = " + expected + " but computed = " + computed);
    }
  }

  /** Apply the function and assert if it preserves position and limit. */
  static ByteString applyAndAssertFunction(ByteBuffer buffer,
      Function<ByteBuffer, ByteString> function, Object name) {
    final int pos = buffer.position();
    final int lim = buffer.limit();
    final ByteString bytes = function.apply(buffer);
    assertInt(pos, buffer.position(), () -> name + ": Unexpected position");
    assertInt(lim, buffer.limit(), () -> name + ": Unexpected limit");
    return bytes;
  }
}
