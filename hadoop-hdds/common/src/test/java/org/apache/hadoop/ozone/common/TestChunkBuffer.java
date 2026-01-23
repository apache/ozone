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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.utils.MockGatheringChannel;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.CodecTestUtil;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
}
