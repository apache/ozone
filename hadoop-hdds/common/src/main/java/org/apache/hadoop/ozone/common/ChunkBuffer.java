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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.UncheckedAutoCloseable;

/** Buffer for a block chunk. */
public interface ChunkBuffer extends ChunkBufferToByteString, UncheckedAutoCloseable {

  /** Similar to {@link ByteBuffer#allocate(int)}. */
  static ChunkBuffer allocate(int capacity) {
    return allocate(capacity, 0);
  }

  /** Similar to {@link ByteBuffer#allocate(int)}
   * except that it can specify the increment.
   *
   * @param increment
   *   the increment size so that this buffer is allocated incrementally.
   *   When increment {@literal <= 0}, entire buffer is allocated in the beginning.
   */
  static ChunkBuffer allocate(int capacity, int increment) {
    if (increment > 0 && increment < capacity) {
      return new IncrementalChunkBuffer(capacity, increment, false);
    }
    CodecBuffer codecBuffer = CodecBuffer.allocateDirect(capacity);
    return new ChunkBufferImplWithByteBuffer(codecBuffer.asWritableByteBuffer(), codecBuffer);
  }

  /** Wrap the given {@link ByteBuffer} as a {@link ChunkBuffer}. */
  static ChunkBuffer wrap(ByteBuffer buffer) {
    return new ChunkBufferImplWithByteBuffer(buffer);
  }

  /** Wrap the given list of {@link ByteBuffer}s as a {@link ChunkBuffer},
   * with a function called when buffers are released.*/
  static ChunkBuffer wrap(List<ByteBuffer> buffers) {
    Objects.requireNonNull(buffers, "buffers == null");
    if (buffers.size() == 1) {
      return wrap(buffers.get(0));
    }
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

  @Override
  default void close() {
  }

  /** Similar to {@link ByteBuffer#put(ByteBuffer)}. */
  ChunkBuffer put(ByteBuffer b);

  /** Similar to {@link ByteBuffer#put(byte[])}. */
  default ChunkBuffer put(byte[] b) {
    return put(ByteBuffer.wrap(b));
  }

  /** Similar to {@link ByteBuffer#put(byte[])}. */
  default ChunkBuffer put(byte b) {
    final byte[] buf = {b};
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
}
