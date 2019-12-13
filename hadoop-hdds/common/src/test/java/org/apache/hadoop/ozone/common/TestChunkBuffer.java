/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.common;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Test {@link ChunkBuffer} implementations.
 */
public class TestChunkBuffer {
  private static int nextInt(int n) {
    return ThreadLocalRandom.current().nextInt(n);
  }

  @Test(timeout = 1_000)
  public void testImplWithByteBuffer() {
    runTestImplWithByteBuffer(1);
    runTestImplWithByteBuffer(1 << 10);
    for(int i = 0; i < 10; i++) {
      runTestImplWithByteBuffer(nextInt(100) + 1);
    }
  }

  private static void runTestImplWithByteBuffer(int n) {
    final byte[] expected = new byte[n];
    ThreadLocalRandom.current().nextBytes(expected);
    runTestImpl(expected, 0, ChunkBuffer.allocate(n));
  }

  @Test(timeout = 1_000)
  public void testIncrementalChunkBuffer() {
    runTestIncrementalChunkBuffer(1, 1);
    runTestIncrementalChunkBuffer(4, 8);
    runTestIncrementalChunkBuffer(16, 1 << 10);
    for(int i = 0; i < 10; i++) {
      final int a = ThreadLocalRandom.current().nextInt(100) + 1;
      final int b = ThreadLocalRandom.current().nextInt(100) + 1;
      runTestIncrementalChunkBuffer(Math.min(a, b), Math.max(a, b));
    }
  }

  private static void runTestIncrementalChunkBuffer(int increment, int n) {
    final byte[] expected = new byte[n];
    ThreadLocalRandom.current().nextBytes(expected);
    runTestImpl(expected, increment, ChunkBuffer.allocate(n, increment));
  }

  private static void runTestImpl(byte[] expected, int bpc, ChunkBuffer impl) {
    final int n = expected.length;
    System.out.println("n=" + n + ", impl=" + impl);

    // check position, remaining
    Assert.assertEquals(0, impl.position());
    Assert.assertEquals(n, impl.remaining());

    impl.put(expected, 0, expected.length);
    Assert.assertEquals(n, impl.position());
    Assert.assertEquals(0, impl.remaining());

    // duplicate
    assertDuplicate(expected, impl);

    // test iterate
    if (bpc > 0) {
      assertIterate(expected, impl, bpc);
    } else {
      for (int d = 1; d < 5; d++) {
        final int bytesPerChecksum = n/d;
        if (bytesPerChecksum > 0) {
          assertIterate(expected, impl, bytesPerChecksum);
        }
      }
      for (int d = 1; d < 10; d++) {
        final int bytesPerChecksum = nextInt(n) + 1;
        assertIterate(expected, impl, bytesPerChecksum);
      }
    }
  }

  private static void assertDuplicate(byte[] expected, ChunkBuffer impl) {
    final int n = expected.length;
    assertToByteString(expected, 0, n, impl);
    for(int i = 0; i < 10; i++) {
      final int offset = nextInt(n);
      final int length = nextInt(n - offset + 1);
      assertToByteString(expected, offset, length, impl);
    }
  }

  private static void assertIterate(
      byte[] expected, ChunkBuffer impl, int bpc) {
    final int n = expected.length;
    final ChunkBuffer duplicated = impl.duplicate(0, n);
    Assert.assertEquals(0, duplicated.position());
    Assert.assertEquals(n, duplicated.remaining());

    final int numChecksums = (n + bpc - 1) / bpc;
    final Iterator<ByteBuffer> i = duplicated.iterate(bpc).iterator();
    int count = 0;
    for(int j = 0; j < numChecksums; j++) {
      final ByteBuffer b = i.next();
      final int expectedRemaining = j < numChecksums - 1?
          bpc : n - bpc *(numChecksums - 1);
      Assert.assertEquals(expectedRemaining, b.remaining());

      final int offset = j* bpc;
      for(int k = 0; k < expectedRemaining; k++) {
        Assert.assertEquals(expected[offset + k], b.get());
        count++;
      }
    }
    Assert.assertEquals(n, count);
    Assert.assertFalse(i.hasNext());
    Assertions.assertThrows(NoSuchElementException.class, i::next);
  }

  private static void assertToByteString(
      byte[] expected, int offset, int length, ChunkBuffer impl) {
    final ChunkBuffer duplicated = impl.duplicate(offset, offset + length);
    Assert.assertEquals(offset, duplicated.position());
    Assert.assertEquals(length, duplicated.remaining());
    final ByteString computed = duplicated.toByteString(buffer -> {
      buffer.mark();
      final ByteString string = ByteString.copyFrom(buffer);
      buffer.reset();
      return string;
    });
    Assert.assertEquals(offset, duplicated.position());
    Assert.assertEquals(length, duplicated.remaining());
    Assert.assertEquals("offset=" + offset + ", length=" + length,
        ByteString.copyFrom(expected, offset, length), computed);
  }
}
