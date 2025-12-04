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

package org.apache.hadoop.hdds.utils.db;

import static org.apache.hadoop.hdds.HddsUtils.formatStackTrace;
import static org.apache.hadoop.hdds.HddsUtils.getStackTrace;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.function.ToIntFunction;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBufAllocator;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBufInputStream;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBufOutputStream;
import org.apache.ratis.thirdparty.io.netty.buffer.EmptyByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A buffer used by {@link Codec}
 * for supporting RocksDB direct {@link ByteBuffer} APIs.
 */
public class CodecBuffer implements UncheckedAutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(CodecBuffer.class);

  private static final ByteBufAllocator POOL = PooledByteBufAllocator.DEFAULT;

  private static final IntFunction<ByteBuf> POOL_DIRECT = c -> c >= 0
      ? POOL.directBuffer(c, c) // allocate exact size
      : POOL.directBuffer(-c);  // allocate a resizable buffer

  private static final IntFunction<ByteBuf> POOL_HEAP = c -> c >= 0
      ? POOL.heapBuffer(c, c)   // allocate exact size
      : POOL.heapBuffer(-c);    // allocate a resizable buffer

  private static final CodecBuffer EMPTY_BUFFER = new CodecBuffer(new EmptyByteBuf(POOL), null);

  private static final AtomicInteger LEAK_COUNT = new AtomicInteger();

  private final StackTraceElement[] elements;

  private final ByteBuf buf;

  private final Object wrapped;

  private final CompletableFuture<Void> released = new CompletableFuture<>();

  /** To create {@link CodecBuffer} instances. */
  private static class Factory {
    private static volatile BiFunction<ByteBuf, Object, CodecBuffer> constructor
        = CodecBuffer::new;

    static void set(BiFunction<ByteBuf, Object, CodecBuffer> f, String name) {
      constructor = f;
      LOG.info("Successfully set constructor to {}: {}", name, f);
    }

    static CodecBuffer newCodecBuffer(ByteBuf buf) {
      return newCodecBuffer(buf, null);
    }

    static CodecBuffer newCodecBuffer(ByteBuf buf, Object wrapped) {
      return constructor.apply(buf, wrapped);
    }
  }

  /** To detect buffer leak. */
  private static class LeakDetector {
    static CodecBuffer newCodecBuffer(ByteBuf buf, Object wrapped) {
      return new CodecBuffer(buf, wrapped) {
        @Override
        protected void finalize() {
          detectLeaks();
        }
      };
    }
  }

  /**
   * Detect buffer leak in runtime.
   * Note that there is a severe performance penalty for leak detection.
   */
  public static void enableLeakDetection() {
    Factory.set(LeakDetector::newCodecBuffer, "LeakDetector::newCodecBuffer");
  }

  /** The size of a buffer. */
  public static class Capacity {
    private final Object name;
    private final AtomicInteger value;

    public Capacity(Object name, int initialCapacity) {
      this.name = name;
      this.value = new AtomicInteger(initialCapacity);
    }

    public int get() {
      return value.get();
    }

    private static int nextValue(int n) {
      // round up to the next power of 2.
      final long roundUp = Long.highestOneBit(n) << 1;
      return roundUp > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) roundUp;
    }

    /** Increase this size to accommodate the given required size. */
    public void increase(int required) {
      final MemoizedSupplier<Integer> newBufferSize = MemoizedSupplier.valueOf(
          () -> nextValue(required));
      final int previous = value.getAndUpdate(
          current -> required <= current ? current : newBufferSize.get());
      if (newBufferSize.isInitialized()) {
        LOG.info("{}: increase {} -> {}", name, previous, newBufferSize.get());
      }
    }
  }

  public static CodecBuffer getEmptyBuffer() {
    return EMPTY_BUFFER;
  }

  /** To allocate {@link CodecBuffer} objects. */
  public interface Allocator extends IntFunction<CodecBuffer> {
    Allocator DIRECT = new Allocator() {
      @Override
      public CodecBuffer apply(int capacity) {
        return allocate(capacity, POOL_DIRECT);
      }

      @Override
      public boolean isDirect() {
        return true;
      }
    };

    Allocator HEAP = new Allocator() {
      @Override
      public CodecBuffer apply(int capacity) {
        return allocate(capacity, POOL_HEAP);
      }

      @Override
      public boolean isDirect() {
        return false;
      }
    };

    static Allocator getDirect() {
      return DIRECT;
    }

    static Allocator getHeap() {
      return HEAP;
    }

    /** Does this object allocate direct buffers? */
    boolean isDirect();
  }

  /**
   * Allocate a buffer using the given allocator.
   *
   * @param allocator Take a capacity parameter and return an allocated buffer.
   *                  When the capacity is non-negative,
   *                  allocate a buffer by setting the initial capacity
   *                  and the maximum capacity to the given capacity.
   *                  When the capacity is negative,
   *                  allocate a buffer by setting only the initial capacity
   *                  to the absolute value of it and, as a result,
   *                  the buffer's capacity can be increased if necessary.
   */
  static CodecBuffer allocate(int capacity, IntFunction<ByteBuf> allocator) {
    return Factory.newCodecBuffer(allocator.apply(capacity));
  }

  /**
   * Allocate a pooled direct buffer.
   * @see #allocate(int, IntFunction)
   */
  public static CodecBuffer allocateDirect(int capacity) {
    return allocate(capacity, POOL_DIRECT);
  }

  /**
   * Allocate a pooled heap buffer.
   * @see #allocate(int, IntFunction)
   */
  public static CodecBuffer allocateHeap(int capacity) {
    return allocate(capacity, POOL_HEAP);
  }

  /** Wrap the given array. */
  public static CodecBuffer wrap(byte[] array) {
    return Factory.newCodecBuffer(Unpooled.wrappedBuffer(array), array);
  }

  /** Wrap the given {@link ByteString}. */
  public static CodecBuffer wrap(ByteString bytes) {
    return Factory.newCodecBuffer(
        Unpooled.wrappedBuffer(bytes.asReadOnlyByteBuffer()), bytes);
  }

  /** Assert the number of leak detected is zero. */
  public static void assertNoLeaks() {
    final long leak = LEAK_COUNT.get();
    if (leak > 0) {
      throw new AssertionError("Found " + leak + " leaked objects, check logs");
    }
  }

  private CodecBuffer(ByteBuf buf, Object wrapped) {
    this.buf = buf;
    this.wrapped = wrapped;
    this.elements = getStackTrace(LOG);
    assertRefCnt(1);
  }

  public boolean isDirect() {
    return buf.isDirect();
  }

  /**
   * @return the wrapped object if this buffer is created by wrapping it;
   *         otherwise, return null.
   */
  public Object getWrapped() {
    return wrapped;
  }

  private void assertRefCnt(int expected) {
    Preconditions.assertSame(expected, buf.refCnt(), "refCnt");
  }

  /**
   * Detect buffer leak by asserting that the underlying buffer is released
   * when this object is garbage collected.
   * This method may be invoked inside the {@link #finalize()} method
   * or using a {@link java.lang.ref.ReferenceQueue}.
   * For performance reason, this class does not override {@link #finalize()}.
   *
   * @see #enableLeakDetection()
   */
  void detectLeaks() {
    // leak detection
    final int capacity = buf.capacity();
    if (!released.isDone() && capacity > 0) {
      final int refCnt = buf.refCnt();
      if (refCnt > 0) {
        final int leak = LEAK_COUNT.incrementAndGet();
        LOG.warn("LEAK {}: {}, refCnt={}, capacity={}{}",
            leak, this, refCnt, capacity,
            elements != null
                ? " allocation:\n" + formatStackTrace(elements, 3)
                : "");
        buf.release(refCnt);
      }
    }
  }

  @Override
  public void close() {
    release();
  }

  /** Release this buffer and return it back to the pool. */
  public void release() {
    final boolean set = released.complete(null);
    if (!set) {
      // Allow a zero capacity buffer to be released multiple times.
      Preconditions.assertSame(0, buf.capacity(), "capacity");
    }
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

  /** Clear this buffer. */
  public void clear() {
    buf.clear();
  }

  /**
   * Set the capacity of this buffer.
   *
   * @return true iff it has successfully changed the capacity.
   */
  public boolean setCapacity(int newCapacity) {
    if (newCapacity < 0) {
      throw new IllegalArgumentException(
          "newCapacity = " + newCapacity + " < 0");
    }
    LOG.debug("setCapacity: {} -> {}, max={}",
        buf.capacity(), newCapacity, buf.maxCapacity());
    if (newCapacity <= buf.maxCapacity()) {
      final ByteBuf returned = buf.capacity(newCapacity);
      Preconditions.assertSame(buf, returned, "buf");
      return true;
    }
    return false;
  }

  /** @return the number of bytes can be read. */
  public int readableBytes() {
    return buf.readableBytes();
  }

  /** @return a writable {@link ByteBuffer}. */
  public ByteBuffer asWritableByteBuffer() {
    assertRefCnt(1);
    return buf.nioBuffer(0, buf.maxCapacity());
  }

  /** @return a readonly {@link ByteBuffer} view of this buffer. */
  public ByteBuffer asReadOnlyByteBuffer() {
    assertRefCnt(1);
    Preconditions.assertTrue(buf.nioBufferCount() > 0);
    return buf.nioBuffer().asReadOnlyBuffer();
  }

  /**
   * @return a new array containing the readable bytes.
   * @see #readableBytes()
   */
  public byte[] getArray() {
    final byte[] array = new byte[readableBytes()];
    buf.readBytes(array);
    return array;
  }

  /** Does the content of this buffer start with the given prefix? */
  public boolean startsWith(CodecBuffer prefix) {
    Objects.requireNonNull(prefix, "prefix == null");
    final int length = prefix.readableBytes();
    if (this.readableBytes() < length) {
      return false;
    }
    return buf.slice(buf.readerIndex(), length).equals(prefix.buf);
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
    final ByteBuf returned = buf.writeShort(n);
    Preconditions.assertSame(buf, returned, "buf");
    return this;
  }

  /**
   * Similar to {@link ByteBuffer#putInt(int)}.
   *
   * @return this object.
   */
  public CodecBuffer putInt(int n) {
    assertRefCnt(1);
    final ByteBuf returned = buf.writeInt(n);
    Preconditions.assertSame(buf, returned, "buf");
    return this;
  }

  /**
   * Similar to {@link ByteBuffer#putLong(long)}.
   *
   * @return this object.
   */
  public CodecBuffer putLong(long n) {
    assertRefCnt(1);
    final ByteBuf returned = buf.writeLong(n);
    Preconditions.assertSame(buf, returned, "buf");
    return this;
  }

  /**
   * Similar to {@link ByteBuffer#put(byte)}.
   *
   * @return this object.
   */
  public CodecBuffer put(byte val) {
    assertRefCnt(1);
    final ByteBuf returned = buf.writeByte(val);
    Preconditions.assertSame(buf, returned, "buf");
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
    final ByteBuf returned = buf.setIndex(buf.readerIndex(), w + size);
    Preconditions.assertSame(buf, returned, "buf");
    return this;
  }

  /**
   * Put bytes from the given source to this buffer.
   *
   * @param source put bytes to an {@link OutputStream} and return the size.
   *               The returned size must be non-null and non-negative.
   * @return this object.
   * @throws CodecException in case the source throws an {@link IOException}.
   */
  public CodecBuffer put(CheckedFunction<OutputStream, Integer, IOException> source) throws CodecException {
    assertRefCnt(1);
    final int w = buf.writerIndex();
    final int size;
    try (ByteBufOutputStream out = new ByteBufOutputStream(buf)) {
      size = source.apply(out);
    } catch (IOException e) {
      throw new CodecException("Failed to apply source to " + this + ", " + source, e);
    }
    final ByteBuf returned = buf.setIndex(buf.readerIndex(), w + size);
    Preconditions.assertSame(buf, returned, "buf");
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
   * @param <E> The {@link Exception} type may be thrown by the given source.
   * @throws E in case the source throws it.
   */
  <E extends Exception> Integer putFromSource(
      PutToByteBuffer<E> source) throws E {
    assertRefCnt(1);
    final int i = buf.writerIndex();
    final int writable = buf.writableBytes();
    final ByteBuffer buffer = buf.nioBuffer(i, writable);
    final Integer size = source.apply(buffer);
    if (size != null) {
      Preconditions.assertTrue(size >= 0, () -> "size = " + size + " < 0");
      if (size > 0 && size <= writable) {
        final ByteBuf returned = buf.setIndex(buf.readerIndex(), i + size);
        Preconditions.assertSame(buf, returned, "buf");
      }
    }
    return size;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "[" + buf.readerIndex()
        + "<=" + buf.writerIndex()
        + "<=" + buf.capacity()
        + ": "
        + StringUtils.bytes2Hex(asReadOnlyByteBuffer(), 10)
        + "]";
  }
}
