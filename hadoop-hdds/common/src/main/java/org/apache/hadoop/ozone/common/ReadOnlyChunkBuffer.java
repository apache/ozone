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
import java.nio.ReadOnlyBufferException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * A read-only {@link ChunkBuffer} view over caller-owned {@code byte[]} memory.
 *
 * <p>Used by {@link BlockOutputStream} to skip copying a full stream-buffer
 * write into the {@link org.apache.hadoop.hdds.scm.storage.BufferPool}.
 * The backing array must remain unchanged until the buffer is released after
 * Ratis commit (or RPC completion on standalone clients).
 */
final class ReadOnlyChunkBuffer implements ChunkBuffer {

  private final byte[] array;
  private final ByteBuffer buffer;

  ReadOnlyChunkBuffer(byte[] array, int offset, int length) {
    validateBounds(array, offset, length);
    this.array = array;
    this.buffer = ByteBuffer.wrap(array, offset, length).slice();
    this.buffer.position(length);
  }

  private ReadOnlyChunkBuffer(byte[] array, ByteBuffer buffer) {
    this.array = array;
    this.buffer = buffer;
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public int position() {
    return buffer.position();
  }

  @Override
  public int remaining() {
    return buffer.remaining();
  }

  @Override
  public int limit() {
    return buffer.limit();
  }

  @Override
  public ChunkBuffer rewind() {
    buffer.rewind();
    return this;
  }

  @Override
  public ChunkBuffer clear() {
    buffer.clear();
    return this;
  }

  @Override
  public ChunkBuffer put(ByteBuffer b) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ChunkBuffer put(byte b) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ChunkBuffer put(byte[] b, int srcOffset, int srcLength) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ChunkBuffer duplicate(int newPosition, int newLimit) {
    if (newPosition < 0 || newLimit < newPosition || newLimit > buffer.limit()) {
      throw new IndexOutOfBoundsException("newPosition=" + newPosition
          + ", newLimit=" + newLimit + ", limit=" + buffer.limit());
    }
    final ByteBuffer duplicated = buffer.duplicate();
    duplicated.position(newPosition).limit(newLimit);
    return new ReadOnlyChunkBuffer(array, duplicated);
  }

  @Override
  public Iterable<ByteBuffer> iterate(int bufferSize) {
    return () -> new Iterator<ByteBuffer>() {
      @Override
      public boolean hasNext() {
        return buffer.hasRemaining();
      }

      @Override
      public ByteBuffer next() {
        if (!buffer.hasRemaining()) {
          throw new NoSuchElementException();
        }
        final ByteBuffer duplicated = buffer.duplicate();
        final int min = Math.min(buffer.position() + bufferSize, buffer.limit());
        duplicated.limit(min);
        buffer.position(min);
        return duplicated.asReadOnlyBuffer();
      }
    };
  }

  @Override
  public List<ByteBuffer> asByteBufferList() {
    return Collections.singletonList(buffer.duplicate().asReadOnlyBuffer());
  }

  @Override
  public long writeTo(GatheringByteChannel channel) throws IOException {
    return BufferUtils.writeFully(channel, buffer.duplicate().asReadOnlyBuffer());
  }

  @Override
  public ByteString toByteStringImpl(Function<ByteBuffer, ByteString> f) {
    return f.apply(buffer.duplicate());
  }

  @Override
  public List<ByteString> toByteStringListImpl(
      Function<ByteBuffer, ByteString> f) {
    return Collections.singletonList(toByteStringImpl(f));
  }

  private static void validateBounds(byte[] array, int offset, int length) {
    if (array == null) {
      throw new NullPointerException("array is null");
    }
    if (offset < 0 || length < 0 || offset > array.length - length) {
      throw new IndexOutOfBoundsException("offset=" + offset + ", length="
          + length + ", array.length=" + array.length);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ":length=" + buffer.limit()
        + "@" + Integer.toHexString(System.identityHashCode(this));
  }
}
