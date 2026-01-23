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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.UncheckedAutoCloseable;

/** {@link ChunkBuffer} implementation using a single {@link ByteBuffer}. */
final class ChunkBufferImplWithByteBuffer implements ChunkBuffer {
  private final ByteBuffer buffer;
  private final UncheckedAutoCloseable underlying;

  ChunkBufferImplWithByteBuffer(ByteBuffer buffer) {
    this(buffer, null);
  }

  ChunkBufferImplWithByteBuffer(ByteBuffer buffer, UncheckedAutoCloseable underlying) {
    this.buffer = Objects.requireNonNull(buffer, "buffer == null");
    this.underlying = underlying;
  }

  @Override
  public void close() {
    if (underlying != null) {
      underlying.close();
    }
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
        final int min = Math.min(
            buffer.position() + bufferSize, buffer.limit());
        duplicated.limit(min);
        buffer.position(min);
        return duplicated;
      }
    };
  }

  @Override
  public List<ByteBuffer> asByteBufferList() {
    return Collections.singletonList(buffer);
  }

  @Override
  public long writeTo(GatheringByteChannel channel) throws IOException {
    return BufferUtils.writeFully(channel, buffer);
  }

  @Override
  public ChunkBuffer duplicate(int newPosition, int newLimit) {
    final ByteBuffer duplicated = buffer.duplicate();
    duplicated.position(newPosition).limit(newLimit);
    return new ChunkBufferImplWithByteBuffer(duplicated);
  }

  @Override
  public ChunkBuffer put(ByteBuffer b) {
    buffer.put(b);
    return this;
  }

  @Override
  public ChunkBuffer put(byte b) {
    buffer.put(b);
    return this;
  }

  @Override
  public ChunkBuffer clear() {
    buffer.clear();
    return this;
  }

  @Override
  public ByteString toByteStringImpl(Function<ByteBuffer, ByteString> f) {
    return f.apply(buffer);
  }

  @Override
  public List<ByteString> toByteStringListImpl(
      Function<ByteBuffer, ByteString> f) {
    return Collections.singletonList(f.apply(buffer));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (!(obj instanceof ChunkBufferImplWithByteBuffer)) {
      return false;
    }
    final ChunkBufferImplWithByteBuffer that
        = (ChunkBufferImplWithByteBuffer)obj;
    return this.buffer.equals(that.buffer);
  }

  @Override
  public int hashCode() {
    return buffer.hashCode();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ":limit=" + buffer.limit()
        + "@" + Integer.toHexString(super.hashCode());
  }
}
