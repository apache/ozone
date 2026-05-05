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

package org.apache.hadoop.hdds.utils.db.managed;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.junit.jupiter.api.Test;

/**
 * Tests for ManagedDirectSlice.
 */
public class TestManagedDirectSlice {
  static {
    ManagedRocksObjectUtils.loadRocksDBLibrary();
  }

  static final Random RANDOM = new Random();
  static final byte[] ZEROS = new byte[1 << 10];
  private static int count = 0;

  @Test
  public void testManagedDirectSlice() {
    // test small sizes
    final int small = 8;
    for (int size = 0; size <= small; size++) {
      testManagedDirectSlice(size);
    }

    // test power of 2 sizes
    for (int i = 1; i <= 10; i++) {
      final int size = small << i;
      testManagedDirectSlice(size - 1);
      testManagedDirectSlice(size);
      testManagedDirectSlice(size + 1);
    }

    // test random sizes
    final int bound = ZEROS.length << 10;
    for (int i = 0; i < 4; i++) {
      final int size = RANDOM.nextInt(bound);
      testManagedDirectSlice(size);
    }
    for (int i = 0; i < 4; i++) {
      final int size = RANDOM.nextInt(bound) + ZEROS.length;
      testManagedDirectSlice(size);
    }
  }

  static void testManagedDirectSlice(int size) {
    // test small positions
    final int small = 3;
    for (int position = 0; position < small; position++) {
      testManagedDirectSlice(size, position);
    }
    // test large positions
    for (int i = 0; i < small; i++) {
      testManagedDirectSlice(size, ZEROS.length - i);
    }
    // test random positions
    for (int i = 0; i < 4; i++) {
      final int bound = ZEROS.length - 2 * small + 1;
      final int position = RANDOM.nextInt(bound) + small; // small <= position <= ZEROS.length-small
      testManagedDirectSlice(size, position);
    }
  }

  static void testManagedDirectSlice(int size, int position) {
    System.out.printf("%3d: size %d and position %d%n", ++count, size, position);
    final byte[] bytes = new byte[size];
    RANDOM.nextBytes(bytes);
    try (CodecBuffer buffer = CodecBuffer.allocateDirect(size + position)
            .put(ByteBuffer.wrap(ZEROS, 0, position))
            .put(ByteBuffer.wrap(bytes));
         ManagedDirectSlice directSlice = new ManagedDirectSlice(getByteBuffer(buffer, position));
         ManagedSlice slice = new ManagedSlice(bytes)) {
      assertEquals(slice.size(), directSlice.size());
      assertEquals(slice, directSlice);
    }
  }

  static ByteBuffer getByteBuffer(CodecBuffer buffer, int position) {
    final ByteBuffer byteBuffer = buffer.asReadOnlyByteBuffer();
    byteBuffer.position(position);
    return byteBuffer;
  }
}
