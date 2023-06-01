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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.utils.db;

import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBufAllocator;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBufInputStream;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBufOutputStream;
import org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToIntFunction;

/**
 * A buffer used by {@link Codec}
 * for supporting RocksDB direct {@link ByteBuffer} APIs.
 */
public final class CodecBuffer implements AutoCloseable {
  public static final Logger LOG = LoggerFactory.getLogger(CodecBuffer.class);

  private static final ByteBufAllocator POOL
      = PooledByteBufAllocator.DEFAULT;

  /** Allocate a direct buffer. */
  public static CodecBuffer allocateDirect(int exactSize) {
    return new CodecBuffer(POOL.directBuffer(exactSize, exactSize));
  }

  /** Allocate a heap buffer. */
  public static CodecBuffer allocateHeap(int exactSize) {
    return new CodecBuffer(POOL.heapBuffer(exactSize, exactSize));
  }

  /** Wrap the given array. */
  public static CodecBuffer wrap(byte[] array) {
    return new CodecBuffer(Unpooled.wrappedBuffer(array));
  }

  private static final AtomicInteger LEAK_COUNT = new AtomicInteger();

  /** Assert the number of leak detected is zero. */
  public static void assertNoLeaks() {
    final long leak = LEAK_COUNT.get();
    if (leak > 0) {
      throw new AssertionError("Found " + leak + " leaked objects, check logs");
    }
  }

  private final ByteBuf buf;
  private final CompletableFuture<Void> released = new CompletableFuture<>();

  private CodecBuffer(ByteBuf buf) {
    this.buf = buf;
    assertRefCnt(1);
  }

  private void assertRefCnt(int expected) {
    Preconditions.assertSame(expected, buf.refCnt(), "refCnt");
  }

  @Override
  protected void finalize() throws Throwable {
    // leak detection
    final int capacity = buf.capacity();
    if (!released.isDone() && capacity > 0) {
      final int refCnt = buf.refCnt();
      if (refCnt > 0) {
        final int leak = LEAK_COUNT.incrementAndGet();
        LOG.warn("LEAK {}: {}, refCnt={}, capacity={}",
            leak, this, refCnt, capacity);
        buf.release(refCnt);
      }
    }
    super.finalize();
  }

  @Override
  public void close() {
    release();
  }

  /** Release this buffer and return it back to the pool. */
  public void release() {
    final boolean set = released.complete(null);
    Preconditions.assertTrue(set, () -> "Already released: " + this);
    if (buf.release()) {
      assertRefCnt(0);
    } else {
      // A zero capacity buffer, possibly singleton, may not be able released.
      Preconditions.assertSame(0, buf.capacity(), "capacity");
    }
  }

  /** @return the future of {@link #release()}. */
  public CompletableFuture<Void> getReleaseFuture() {
    return released;
  }

  /** @return the number of bytes can be read. */
  public int readableBytes() {
    return buf.readableBytes();
  }

  /** @return a readonly {@link ByteBuffer} view of this buffer. */
  public ByteBuffer asReadOnlyByteBuffer() {
    assertRefCnt(1);
    Preconditions.assertTrue(buf.nioBufferCount() > 0);
    return buf.nioBuffer().asReadOnlyBuffer();
  }

  /** @return an {@link InputStream} reading from this buffer. */
  public InputStream getInputStream() {
    return new ByteBufInputStream(buf.duplicate());
  }

  /**
   * Similar to {@link ByteBuffer#putShort(short)}.
   *
   * @return this object.
   */
  public CodecBuffer putShort(short n) {
    assertRefCnt(1);
    buf.writeShort(n);
    return this;
  }

  /**
   * Similar to {@link ByteBuffer#putInt(int)}.
   *
   * @return this object.
   */
  public CodecBuffer putInt(int n) {
    assertRefCnt(1);
    buf.writeInt(n);
    return this;
  }

  /**
   * Similar to {@link ByteBuffer#putLong(long)}.
   *
   * @return this object.
   */
  public CodecBuffer putLong(long n) {
    assertRefCnt(1);
    buf.writeLong(n);
    return this;
  }

  /**
   * Similar to {@link ByteBuffer#put(byte[])}.
   *
   * @return this object.
   */
  public CodecBuffer put(byte[] array) {
    assertRefCnt(1);
    buf.writeBytes(array);
    return this;
  }

  /**
   * Similar to {@link ByteBuffer#put(ByteBuffer)}.
   *
   * @return this object.
   */
  public CodecBuffer put(ByteBuffer buffer) {
    assertRefCnt(1);
    buf.writeBytes(buffer);
    return this;
  }

  /**
   * Put bytes from the given source to this buffer.
   *
   * @param source put bytes to a {@link ByteBuffer} and return the size.
   * @return this object.
   */
  CodecBuffer put(ToIntFunction<ByteBuffer> source) {
    assertRefCnt(1);
    final int w = buf.writerIndex();
    final ByteBuffer buffer = buf.nioBuffer(w, buf.writableBytes());
    final int size = source.applyAsInt(buffer);
    buf.setIndex(buf.readerIndex(), w + size);
    return this;
  }

  /**
   * Put bytes from the given source to this buffer.
   *
   * @param source put bytes to an {@link OutputStream} and return the size.
   *               The returned size must be non-null and non-negative.
   * @return this object.
   * @throws IOException in case the source throws an {@link IOException}.
   */
  CodecBuffer put(
      CheckedFunction<OutputStream, Integer, IOException> source)
      throws IOException {
    assertRefCnt(1);
    final int w = buf.writerIndex();
    final int size;
    try (ByteBufOutputStream out = new ByteBufOutputStream(buf)) {
      size = source.apply(out);
    }
    buf.setIndex(buf.readerIndex(), w + size);
    return this;
  }

  /**
   * Put bytes from a source to this buffer.
   * The source may or may not be available.
   * The given source function must return the required size (possibly 0)
   * if the source is available; otherwise, return null.
   * When the buffer is smaller than the required size,
   * it may write partial result to the buffer.
   *
   * @param source put bytes to a {@link ByteBuffer}.
   * @return the return value from the source function.
   * @throws IOException in case the source throws an {@link IOException}.
   */
  Integer putFromSource(
      CheckedFunction<ByteBuffer, Integer, IOException> source)
      throws IOException {
    assertRefCnt(1);
    final int i = buf.writerIndex();
    final int writable = buf.writableBytes();
    final ByteBuffer buffer = buf.nioBuffer(i, writable);
    final Integer size = source.apply(buffer);
    if (size != null) {
      Preconditions.assertTrue(size >= 0, () -> "size = " + size + " < 0");
      if (size > 0 && size <= writable) {
        buf.setIndex(buf.readerIndex(), i + size);
      }
    }
    return size;
  }
}
