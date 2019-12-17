/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.common;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link ChunkBufferImplWithByteBufferList}.
 */
public class TestChunkBufferImplWithByteBufferList {

  @Test
  public void rejectsNullList() {
    List<ByteBuffer> list = null;
    assertThrows(IllegalArgumentException.class, () -> ChunkBuffer.wrap(list));
  }

  @Test
  public void acceptsEmptyList() {
    ChunkBuffer subject = ChunkBuffer.wrap(ImmutableList.of());
    assertEmpty(subject);
    assertEmpty(subject.duplicate(0, 0));
    assertThrows(BufferOverflowException.class,
        () -> subject.put(ByteBuffer.allocate(1)));
  }

  @Test
  public void rejectsMultipleCurrentBuffers() {
    ByteBuffer b1 = allocate();
    ByteBuffer b2 = allocate();
    ByteBuffer b3 = allocate();
    List<ByteBuffer> list = ImmutableList.of(b1, b2, b3);

    // buffer with remaining immediately followed by non-zero pos
    b1.position(b1.limit() - 1);
    b2.position(1);
    assertThrows(IllegalArgumentException.class, () -> ChunkBuffer.wrap(list));

    // buffer with remaining followed by non-zero pos
    b1.position(b1.limit() - 1);
    b2.position(0);
    b3.position(1);
    assertThrows(IllegalArgumentException.class, () -> ChunkBuffer.wrap(list));
  }

  private static void assertEmpty(ChunkBuffer subject) {
    assertEquals(0, subject.position());
    assertEquals(0, subject.remaining());
    assertEquals(0, subject.limit());
  }

  private static ByteBuffer allocate() {
    return ByteBuffer.allocate(ThreadLocalRandom.current().nextInt(10, 20));
  }

}
