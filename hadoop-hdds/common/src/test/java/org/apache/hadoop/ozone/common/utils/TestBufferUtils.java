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

package org.apache.hadoop.ozone.common.utils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.junit.jupiter.api.Test;

class TestBufferUtils {

  @Test
  void slicePreservesSourceAndUsesRequestedRange() {
    ByteBuffer source = ByteBuffer.allocate(16);
    source.position(2);
    source.limit(12);

    ByteBuffer slice = BufferUtils.slice(source, 4, 5);

    assertEquals(2, source.position());
    assertEquals(12, source.limit());
    assertEquals(4, slice.position());
    assertEquals(9, slice.limit());
  }

  @Test
  void sliceRejectsNegativePosition() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> BufferUtils.slice(ByteBuffer.allocate(8), -1, 1));

    assertEquals("position (-1) must not be negative", exception.getMessage());
  }

  @Test
  void sliceRejectsNegativeLength() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> BufferUtils.slice(ByteBuffer.allocate(8), 0, -1));

    assertEquals("length (-1) must not be negative", exception.getMessage());
  }

  @Test
  void sliceRejectsPositionBeyondLimit() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> BufferUtils.slice(ByteBuffer.allocate(8), 9, 0));

    assertEquals("position (9) exceeds source limit (8)",
        exception.getMessage());
  }

  @Test
  void sliceRejectsRangeBeyondLimitWithoutOverflow() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> BufferUtils.slice(ByteBuffer.allocate(8), 1,
            Integer.MAX_VALUE));

    assertEquals(
        "position (1) + length (2147483647) exceeds source limit (8)",
        exception.getMessage());
  }

  @Test
  void testCloseAllClosesEveryBufferAndSuppressesLaterFailures() {
    final IllegalStateException first = new IllegalStateException("first");
    final IllegalStateException second = new IllegalStateException("second");
    final List<String> closed = new ArrayList<>();
    final List<UncheckedAutoCloseable> buffers = Arrays.asList(
        () -> closed.add("a"),
        () -> {
          throw first;
        },
        () -> closed.add("b"),
        () -> {
          throw second;
        },
        () -> closed.add("c"));

    assertSame(first, BufferUtils.closeAll(buffers, null));
    assertEquals(Arrays.asList("a", "b", "c"), closed);
    assertArrayEquals(new Throwable[] {second}, first.getSuppressed());
  }

  @Test
  void testCloseAllKeepsAnEarlierFailure() {
    final IllegalStateException earlier = new IllegalStateException("earlier");
    final IllegalStateException failure = new IllegalStateException("failure");
    final UncheckedAutoCloseable failing = () -> {
      throw failure;
    };

    assertSame(earlier, BufferUtils.closeAll(Arrays.asList(failing), earlier));
    assertArrayEquals(new Throwable[] {failure}, earlier.getSuppressed());
    assertNull(BufferUtils.closeAll(Arrays.asList(() -> { }), null));
  }

  @Test
  void testCloseAllWithTheSameExceptionTwice() {
    final IllegalStateException failure = new IllegalStateException("cached");
    final AtomicInteger closed = new AtomicInteger();
    final UncheckedAutoCloseable failing = () -> {
      throw failure;
    };

    assertSame(failure, BufferUtils.closeAll(Arrays.asList(failing, failing, closed::incrementAndGet), null));
    assertEquals(1, closed.get());
    assertEquals(0, failure.getSuppressed().length);
  }
}
