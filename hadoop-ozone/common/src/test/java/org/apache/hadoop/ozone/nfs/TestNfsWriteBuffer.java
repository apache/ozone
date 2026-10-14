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

package org.apache.hadoop.ozone.nfs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class TestNfsWriteBuffer {
  @Test
  void buffersOutOfOrderWritesUntilThePrefixIsContiguous() {
    NfsWriteBuffer buffer = new NfsWriteBuffer(16, 4);

    assertArrayEquals(new byte[0], buffer.offer(4, bytes("ef")));
    assertEquals(0, buffer.getNextOffset());
    assertEquals(2, buffer.getBufferedBytes());
    assertArrayEquals(bytes("abcdef"), buffer.offer(0, bytes("abcd")));
    buffer.acknowledgeWritten(6);
    assertEquals(6, buffer.getNextOffset());
    assertEquals(0, buffer.getBufferedBytes());
  }

  @Test
  void acceptsIdenticalOverlapsAndMergesAdjacentRanges() {
    NfsWriteBuffer buffer = new NfsWriteBuffer(16, 4);

    assertArrayEquals(new byte[0], buffer.offer(2, bytes("cdef")));
    assertArrayEquals(new byte[0], buffer.offer(4, bytes("efgh")));
    assertArrayEquals(bytes("abcdefgh"), buffer.offer(0, bytes("ab")));
    buffer.acknowledgeWritten(8);
  }

  @Test
  void retainsContiguousBytesUntilTheCallerAcknowledgesTheWrite() {
    NfsWriteBuffer buffer = new NfsWriteBuffer(16, 4);
    byte[] offered = buffer.offer(0, bytes("abcdef"));

    assertArrayEquals(bytes("abcdef"), offered);
    assertArrayEquals(offered, buffer.offer(0, bytes("abcdef")));
    assertEquals(0, buffer.getNextOffset());
    assertEquals(6, buffer.getBufferedBytes());

    buffer.acknowledgeWritten(3);
    assertEquals(3, buffer.getNextOffset());
    assertArrayEquals(bytes("def"), buffer.offer(3, bytes("def")));
    buffer.acknowledgeWritten(3);
    assertEquals(6, buffer.getNextOffset());
    assertEquals(0, buffer.getBufferedBytes());
  }

  @Test
  void rejectsConflictingOverlapWithoutChangingBufferedData() {
    NfsWriteBuffer buffer = new NfsWriteBuffer(16, 4);
    buffer.offer(2, bytes("cdef"));

    assertThrows(IllegalArgumentException.class, () -> buffer.offer(3, bytes("X")));
    assertEquals(4, buffer.getBufferedBytes());
    assertArrayEquals(bytes("abcdef"), buffer.offer(0, bytes("ab")));
    buffer.acknowledgeWritten(6);
  }

  @Test
  void enforcesBufferedByteAndRangeLimits() {
    NfsWriteBuffer buffer = new NfsWriteBuffer(3, 2);
    buffer.offer(4, bytes("ab"));
    assertThrows(IllegalArgumentException.class, () -> buffer.offer(8, bytes("cd")));
    assertThrows(IllegalArgumentException.class, () -> {
      NfsWriteBuffer rangeLimited = new NfsWriteBuffer(16, 1);
      rangeLimited.offer(2, bytes("a"));
      rangeLimited.offer(4, bytes("b"));
    });
    assertEquals(2, buffer.getBufferedBytes());
  }

  @Test
  void rejectsRangesBehindStreamedPrefixAndOverflow() {
    NfsWriteBuffer buffer = new NfsWriteBuffer(16, 4);
    buffer.offer(0, bytes("abc"));
    buffer.acknowledgeWritten(3);

    assertThrows(IllegalArgumentException.class, () -> buffer.offer(1, bytes("b")));
    assertThrows(IllegalArgumentException.class, () -> buffer.offer(Long.MAX_VALUE, bytes("x")));
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
