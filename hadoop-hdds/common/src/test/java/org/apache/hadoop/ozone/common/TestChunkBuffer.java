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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.utils.MockGatheringChannel;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.CodecTestUtil;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test {@link ChunkBuffer} implementations.
 */
public class TestChunkBuffer {
  private static int nextInt(int n) {
    return ThreadLocalRandom.current().nextInt(n);
  }

  @BeforeAll
  public static void beforeAll() {
    CodecBuffer.enableLeakDetection();
  }

  @AfterEach
  public void after() throws Exception {
    CodecTestUtil.gc();
  }

  static Stream<Arguments> chunkBufferTypes() {
    return Stream.of(
        Arguments.of("ByteBuffer", (IntFunction<ChunkBuffer>) ChunkBuffer::allocate),
        Arguments.of("Incremental", (IntFunction<ChunkBuffer>) cap ->
            ChunkBuffer.allocate(cap, Math.max(1, cap / 3))),
        Arguments.of("ByteBufferList", (IntFunction<ChunkBuffer>) cap -> {
          final int first = cap / 2;
          return ChunkBuffer.wrap(ImmutableList.of(
              ByteBuffer.allocate(first),
              ByteBuffer.allocate(cap - first)));
        }));
  }

  static Stream<Arguments> putTestCases() {
    final List<Arguments> cases = new ArrayList<>();
    chunkBufferTypes().forEach(impl -> {
      final String name = (String) impl.get()[0];
      @SuppressWarnings("unchecked")
      final IntFunction<ChunkBuffer> factory =
          (IntFunction<ChunkBuffer>) impl.get()[1];
      for (PutMethod method : PutMethod.values()) {
        cases.add(Arguments.of(name, factory, method));
      }
    });
    return cases.stream();
  }

  @ParameterizedTest(name = "put rejects null [{0} via {1}]")
  @MethodSource("putTestCases")
  void putRejectsNull(String ignored, IntFunction<ChunkBuffer> factory,
      PutMethod method) {
    try (ChunkBuffer buffer = factory.apply(16)) {
      method.assertRejectsNull(buffer);
    }
  }

  @ParameterizedTest(name = "put(byte[]) rejects negative offset/length [{0}]")
  @MethodSource("chunkBufferTypes")
  void putByteArrayRejectsNegativeOffsetOrLength(String ignored,
      IntFunction<ChunkBuffer> factory) {
    final byte[] source = new byte[8];
    try (ChunkBuffer buffer = factory.apply(16)) {
      assertThrows(IllegalArgumentException.class, () -> buffer.put(source, -1, 4));
      assertThrows(IllegalArgumentException.class, () -> buffer.put(source, 0, -1));
    }
  }

  @ParameterizedTest(name = "put(byte[]) rejects out-of-range slice [{0}]")
  @MethodSource("chunkBufferTypes")
  void putByteArrayRejectsOutOfRangeSlice(String ignored,
      IntFunction<ChunkBuffer> factory) {
    final byte[] source = new byte[8];
    try (ChunkBuffer buffer = factory.apply(16)) {
      assertThrows(IllegalArgumentException.class, () -> buffer.put(source, 5, 4));
      assertThrows(IllegalArgumentException.class, () -> buffer.put(source, 0, 9));
    }
  }

  @ParameterizedTest(name = "put rejects buffer overflow [{0} via {1}]")
  @MethodSource("putTestCases")
  void putRejectsBufferOverflow(String type, IntFunction<ChunkBuffer> factory,
      PutMethod method) {
    final byte[] source = new byte[8];
    try (ChunkBuffer buffer = factory.apply(4)) {
      final BufferOverflowException overflow = assertThrows(
          BufferOverflowException.class,
          () -> method.putSlice(buffer, source, 0, 5));
      if (method.expectsCheckArgumentOverflowCause(type)) {
        assertInstanceOf(IllegalArgumentException.class, overflow.getCause());
      }
    }
  }

  @ParameterizedTest(name = "put rejects overflow after partial fill [{0} via {1}]")
  @MethodSource("putTestCases")
  void putRejectsOverflowAfterPartialFill(String type, IntFunction<ChunkBuffer> factory,
      PutMethod method) throws IOException {
    final byte[] first = {1, 2, 3, 4, 5};
    final byte[] second = {6, 7, 8, 9};
    try (ChunkBuffer buffer = factory.apply(8)) {
      method.putSlice(buffer, first, 0, 5);
      assertEquals(5, buffer.position());
      final BufferOverflowException overflow = assertThrows(
          BufferOverflowException.class, () -> method.putSlice(buffer, second, 0, 4));
      if (method.expectsCheckArgumentOverflowCause(type)) {
        assertInstanceOf(IllegalArgumentException.class, overflow.getCause());
      }
      assertArrayEquals(first, readWrittenBytes(buffer));
    }
  }

  @ParameterizedTest(name = "put writes array slice [{0} via {1}]")
  @MethodSource("putTestCases")
  void putWritesSlice(String ignored, IntFunction<ChunkBuffer> factory, PutMethod method)
      throws IOException {
    final byte[] source = {1, 2, 3, 4, 5, 6, 7, 8};
    try (ChunkBuffer buffer = factory.apply(4)) {
      method.putSlice(buffer, source, 2, 4);
      assertEquals(4, buffer.position());
      assertEquals(0, buffer.remaining());
      assertArrayEquals(new byte[] {3, 4, 5, 6}, readWrittenBytes(buffer));
    }
  }

  @ParameterizedTest(name = "put zero-length payload is no-op [{0} via {1}]")
  @MethodSource("putTestCases")
  void putZeroLengthPayloadIsNoOp(String ignored, IntFunction<ChunkBuffer> factory,
      PutMethod method) throws IOException {
    final byte[] source = {1, 2, 3, 4};
    try (ChunkBuffer buffer = factory.apply(4)) {
      method.putSlice(buffer, source, 0, 2);
      method.putEmptySlice(buffer, source, 2, 2);
      assertEquals(2, buffer.position());
      assertArrayEquals(new byte[] {1, 2}, readWrittenBytes(buffer));
    }
  }

  @ParameterizedTest(name = "put fills entire buffer [{0} via {1}]")
  @MethodSource("putTestCases")
  void putFillsEntireBuffer(String ignored, IntFunction<ChunkBuffer> factory,
      PutMethod method) throws IOException {
    final byte[] source = new byte[32];
    ThreadLocalRandom.current().nextBytes(source);
    try (ChunkBuffer buffer = factory.apply(source.length)) {
      method.putAll(buffer, source);
      assertEquals(source.length, buffer.position());
      assertArrayEquals(source, readWrittenBytes(buffer));
    }
  }

  @ParameterizedTest(name = "put spans incremental chunks [{0} via {1}]")
  @MethodSource("putTestCases")
  void putSpansIncrementalChunks(String ignored, IntFunction<ChunkBuffer> ignoredFactory,
      PutMethod method) throws IOException {
    final byte[] source = new byte[10];
    ThreadLocalRandom.current().nextBytes(source);
    try (ChunkBuffer buffer = ChunkBuffer.allocate(10, 3)) {
      method.putSlice(buffer, source, 1, 7);
      assertEquals(7, buffer.position());
      assertArrayEquals(Arrays.copyOfRange(source, 1, 8), readWrittenBytes(buffer));
    }
  }

  @ParameterizedTest(name = "put spans list chunks [{0} via {1}]")
  @MethodSource("putTestCases")
  void putSpansListChunks(String ignored, IntFunction<ChunkBuffer> ignoredFactory,
      PutMethod method) throws IOException {
    final byte[] source = new byte[10];
    ThreadLocalRandom.current().nextBytes(source);
    try (ChunkBuffer buffer = ChunkBuffer.wrap(ImmutableList.of(
        ByteBuffer.allocate(4), ByteBuffer.allocate(6)))) {
      method.putSlice(buffer, source, 1, 7);
      assertEquals(7, buffer.position());
      assertArrayEquals(Arrays.copyOfRange(source, 1, 8), readWrittenBytes(buffer));
    }
  }

  @Test
  void testImplWithByteBuffer() throws IOException {
    runTestImplWithByteBuffer(1);
    runTestImplWithByteBuffer(1 << 10);
    for (int i = 0; i < 10; i++) {
      runTestImplWithByteBuffer(nextInt(100) + 1);
    }
  }

  private static void runTestImplWithByteBuffer(int n) throws IOException {
    final byte[] expected = new byte[n];
    ThreadLocalRandom.current().nextBytes(expected);
    try (ChunkBuffer c = ChunkBuffer.allocate(n)) {
      runTestImpl(expected, 0, c);
    }
  }

  @Test
  void testIncrementalChunkBuffer() throws IOException {
    runTestIncrementalChunkBuffer(1, 1);
    runTestIncrementalChunkBuffer(4, 8);
    runTestIncrementalChunkBuffer(16, 1 << 10);
    for (int i = 0; i < 10; i++) {
      final int a = ThreadLocalRandom.current().nextInt(100) + 1;
      final int b = ThreadLocalRandom.current().nextInt(100) + 1;
      runTestIncrementalChunkBuffer(Math.min(a, b), Math.max(a, b));
    }
  }

  private static void runTestIncrementalChunkBuffer(int increment, int n) throws IOException {
    final byte[] expected = new byte[n];
    ThreadLocalRandom.current().nextBytes(expected);
    try (IncrementalChunkBuffer c = new IncrementalChunkBuffer(n, increment, false)) {
      runTestImpl(expected, increment, c);
    }
  }

  @Test
  void testImplWithList() throws IOException {
    runTestImplWithList(4, 8);
    runTestImplWithList(16, 1 << 10);
    for (int i = 0; i < 10; i++) {
      final int a = ThreadLocalRandom.current().nextInt(10) + 1;
      final int b = ThreadLocalRandom.current().nextInt(100) + 1;
      runTestImplWithList(Math.min(a, b), Math.max(a, b));
    }
  }

  private static void runTestImplWithList(int count, int n) throws IOException {
    final byte[] expected = new byte[n];
    ThreadLocalRandom.current().nextBytes(expected);

    final int avg = n / count;
    final List<ByteBuffer> buffers = new ArrayList<>(count);

    int offset = 0;
    for (int i = 0; i < count - 1; i++) {
      final int length = ThreadLocalRandom.current().nextInt(avg) + 1;
      buffers.add(ByteBuffer.allocate(length));
      offset += length;
    }

    if (n > offset) {
      buffers.add(ByteBuffer.allocate(n - offset));
    }

    ChunkBuffer impl = ChunkBuffer.wrap(buffers);
    runTestImpl(expected, -1, impl);
  }

  private static void runTestImpl(byte[] expected, int bpc, ChunkBuffer impl) throws IOException {
    final int n = expected.length;
    System.out.println("n=" + n + ", impl=" + impl);

    // check position, remaining
    assertEquals(0, impl.position());
    assertEquals(n, impl.remaining());
    assertEquals(n, impl.limit());

    impl.put(expected);
    assertEquals(n, impl.position());
    assertEquals(0, impl.remaining());
    assertEquals(n, impl.limit());

    // duplicate
    assertDuplicate(expected, impl);

    // test iterate
    if (bpc > 0) {
      assertIterate(expected, impl, bpc);
    } else if (bpc == 0) {
      for (int d = 1; d < 5; d++) {
        final int bytesPerChecksum = n / d;
        if (bytesPerChecksum > 0) {
          assertIterate(expected, impl, bytesPerChecksum);
        }
      }
      for (int d = 1; d < 10; d++) {
        final int bytesPerChecksum = nextInt(n) + 1;
        assertIterate(expected, impl, bytesPerChecksum);
      }
    }

    assertWrite(expected, impl);
  }

  private static void assertDuplicate(byte[] expected, ChunkBuffer impl) {
    final int n = expected.length;
    assertToByteString(expected, 0, n, impl);
    for (int i = 0; i < 10; i++) {
      final int offset = nextInt(n);
      final int length = nextInt(n - offset + 1);
      assertToByteString(expected, offset, length, impl);
    }
  }

  private static void assertIterate(
      byte[] expected, ChunkBuffer impl, int bpc) {
    final int n = expected.length;
    final ChunkBuffer duplicated = impl.duplicate(0, n);
    assertEquals(0, duplicated.position());
    assertEquals(n, duplicated.remaining());

    final int numChecksums = (n + bpc - 1) / bpc;
    final Iterator<ByteBuffer> i = duplicated.iterate(bpc).iterator();
    int count = 0;
    for (int j = 0; j < numChecksums; j++) {
      final ByteBuffer b = i.next();
      final int expectedRemaining = j < numChecksums - 1 ?
          bpc : n - bpc * (numChecksums - 1);
      assertEquals(expectedRemaining, b.remaining());

      final int offset = j * bpc;
      for (int k = 0; k < expectedRemaining; k++) {
        assertEquals(expected[offset + k], b.get());
        count++;
      }
    }
    assertEquals(n, count);
    assertFalse(i.hasNext());
    assertThrows(NoSuchElementException.class, i::next);
  }

  private static void assertToByteString(
      byte[] expected, int offset, int length, ChunkBuffer impl) {
    final ChunkBuffer duplicated = impl.duplicate(offset, offset + length);
    assertEquals(offset, duplicated.position());
    assertEquals(length, duplicated.remaining());
    final ByteString computed = duplicated.toByteString(buffer -> {
      buffer.mark();
      final ByteString string = ByteString.copyFrom(buffer);
      buffer.reset();
      return string;
    });
    assertEquals(offset, duplicated.position());
    assertEquals(length, duplicated.remaining());
    assertEquals(ByteString.copyFrom(expected, offset, length), computed,
        "offset=" + offset + ", length=" + length);
  }

  private static void assertWrite(byte[] expected, ChunkBuffer impl) throws IOException {
    impl.rewind();
    assertEquals(0, impl.position());

    ByteArrayOutputStream output = new ByteArrayOutputStream(expected.length);
    impl.writeTo(new MockGatheringChannel(Channels.newChannel(output)));
    assertArrayEquals(expected, output.toByteArray());
    assertFalse(impl.hasRemaining());
  }

  private static byte[] readWrittenBytes(ChunkBuffer buffer) throws IOException {
    final int written = buffer.position();
    final ChunkBuffer slice = buffer.duplicate(0, written);
    final ByteArrayOutputStream output = new ByteArrayOutputStream(written);
    slice.writeTo(new MockGatheringChannel(Channels.newChannel(output)));
    return output.toByteArray();
  }

  private static ByteBuffer emptySlice(byte[] source, int offset, int length) {
    final ByteBuffer slice = ByteBuffer.wrap(source, offset, length);
    slice.position(slice.limit());
    return slice;
  }

  /**
   * Exercises {@link ChunkBuffer#put(byte[], int, int)} vs {@link ChunkBuffer#put(ByteBuffer)}
   * with equivalent payloads.
   */
  private enum PutMethod {
    BYTE_ARRAY {
      @Override
      void putSlice(ChunkBuffer buffer, byte[] source, int offset, int length) {
        buffer.put(source, offset, length);
      }

      @Override
      void putAll(ChunkBuffer buffer, byte[] source) {
        buffer.put(source);
      }

      @Override
      void putEmptySlice(ChunkBuffer buffer, byte[] source, int offset, int length) {
        buffer.put(source, offset, 0);
      }

      @Override
      void assertRejectsNull(ChunkBuffer buffer) {
        assertThrows(NullPointerException.class, () -> buffer.put((byte[]) null));
        assertThrows(NullPointerException.class, () -> buffer.put(null, 0, 0));
      }

      @Override
      boolean expectsCheckArgumentOverflowCause(String implType) {
        return true;
      }
    },
    BYTE_BUFFER {
      @Override
      void putSlice(ChunkBuffer buffer, byte[] source, int offset, int length) {
        buffer.put(ByteBuffer.wrap(source, offset, length));
      }

      @Override
      void putAll(ChunkBuffer buffer, byte[] source) {
        buffer.put(ByteBuffer.wrap(source));
      }

      @Override
      void putEmptySlice(ChunkBuffer buffer, byte[] source, int offset, int length) {
        buffer.put(emptySlice(source, offset, length));
      }

      @Override
      void assertRejectsNull(ChunkBuffer buffer) {
        assertThrows(NullPointerException.class, () -> buffer.put((ByteBuffer) null));
      }

      @Override
      boolean expectsCheckArgumentOverflowCause(String implType) {
        return !"ByteBuffer".equals(implType);
      }
    };

    abstract void putSlice(ChunkBuffer buffer, byte[] source, int offset, int length);

    abstract void putAll(ChunkBuffer buffer, byte[] source);

    abstract void putEmptySlice(ChunkBuffer buffer, byte[] source, int offset, int length);

    abstract void assertRejectsNull(ChunkBuffer buffer);

    abstract boolean expectsCheckArgumentOverflowCause(String implType);
  }
}
