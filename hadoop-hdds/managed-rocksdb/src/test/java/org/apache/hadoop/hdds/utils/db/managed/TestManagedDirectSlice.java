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

  @Test
  public void testManagedDirectSlice() {
    testManagedDirectSlice(0);
    testManagedDirectSlice(1);
    for (int size = 2; size <= 10; size <<= 1) {
      testManagedDirectSlice(size - 1);
      testManagedDirectSlice(size);
      testManagedDirectSlice(size + 1);
    }
    for (int i = 0; i < 5; i++) {
      final int size = RANDOM.nextInt(1024);
      testManagedDirectSlice(size);
    }
  }

  static void testManagedDirectSlice(int size) {
    try {
      runTestManagedDirectSlice(size);
    } catch (Throwable e) {
      System.out.printf("Failed for size %d%n", size);
      throw e;
    }
  }

  static void runTestManagedDirectSlice(int size) {
    final byte[] bytes = new byte[size];
    RANDOM.nextBytes(bytes);
    try (CodecBuffer buffer = CodecBuffer.allocateDirect(size).put(ByteBuffer.wrap(bytes));
         ManagedDirectSlice directSlice = new ManagedDirectSlice(buffer.asReadOnlyByteBuffer());
         ManagedSlice slice = new ManagedSlice(bytes)) {
      assertEquals(slice.size(), directSlice.size());
      assertEquals(slice, directSlice);
    }
  }
}
