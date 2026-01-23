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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ChunkBufferImplWithByteBufferList}.
 */
public class TestChunkBufferImplWithByteBufferList {

  @Test
  public void rejectsNullList() {
    List<ByteBuffer> list = null;
    assertThrows(NullPointerException.class, () -> ChunkBuffer.wrap(list));
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

  @Test
  public void testIterateSmallerOverSingleChunk() {
    ChunkBuffer subject = ChunkBuffer.wrap(ImmutableList.of(ByteBuffer.allocate(100)));

    assertEquals(0, subject.position());
    assertEquals(100, subject.remaining());
    assertEquals(100, subject.limit());

    subject.iterate(25).forEach(buffer -> assertEquals(25, buffer.remaining()));

    assertEquals(100, subject.position());
    assertEquals(0, subject.remaining());
    assertEquals(100, subject.limit());
  }

  @Test
  public void testIterateOverMultipleChunksFitChunkSize() {
    ByteBuffer b1 = ByteBuffer.allocate(100);
    ByteBuffer b2 = ByteBuffer.allocate(100);
    ByteBuffer b3 = ByteBuffer.allocate(100);
    ChunkBuffer subject = ChunkBuffer.wrap(ImmutableList.of(b1, b2, b3));

    assertEquals(0, subject.position());
    assertEquals(300, subject.remaining());
    assertEquals(300, subject.limit());

    subject.iterate(100).forEach(buffer -> assertEquals(100, buffer.remaining()));

    assertEquals(300, subject.position());
    assertEquals(0, subject.remaining());
    assertEquals(300, subject.limit());
  }

  @Test
  public void testIterateOverMultipleChunksSmallerChunks() {
    ByteBuffer b1 = ByteBuffer.allocate(100);
    ByteBuffer b2 = ByteBuffer.allocate(100);
    ByteBuffer b3 = ByteBuffer.allocate(100);
    ChunkBuffer subject = ChunkBuffer.wrap(ImmutableList.of(b1, b2, b3));

    assertEquals(0, subject.position());
    assertEquals(300, subject.remaining());
    assertEquals(300, subject.limit());

    subject.iterate(50).forEach(buffer -> assertEquals(50, buffer.remaining()));

    assertEquals(300, subject.position());
    assertEquals(0, subject.remaining());
    assertEquals(300, subject.limit());
  }

  @Test
  public void testIterateOverMultipleChunksBiggerChunks() {
    ByteBuffer b1 = ByteBuffer.allocate(100);
    ByteBuffer b2 = ByteBuffer.allocate(100);
    ByteBuffer b3 = ByteBuffer.allocate(100);
    ByteBuffer b4 = ByteBuffer.allocate(100);
    ChunkBuffer subject = ChunkBuffer.wrap(ImmutableList.of(b1, b2, b3, b4));

    assertEquals(0, subject.position());
    assertEquals(400, subject.remaining());
    assertEquals(400, subject.limit());

    subject.iterate(200).forEach(buffer -> assertEquals(200, buffer.remaining()));

    assertEquals(400, subject.position());
    assertEquals(0, subject.remaining());
    assertEquals(400, subject.limit());
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
