/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.utils.db;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Test {@link Codec} implementations.
 */
public final class TestCodec {
  static final int NUM_LOOPS = 10;

  @Test
  public void testIntegerCodec() throws Exception {
    final IntegerCodec codec = new IntegerCodec();
    runTest(codec, 0, Integer.BYTES);
    runTest(codec, 1, Integer.BYTES);
    runTest(codec, -1, Integer.BYTES);
    runTest(codec, Integer.MAX_VALUE, Integer.BYTES);
    runTest(codec, Integer.MIN_VALUE, Integer.BYTES);

    for (int i = 0; i < NUM_LOOPS; i++) {
      final int original = ThreadLocalRandom.current().nextInt();
      runTest(codec, original, Integer.BYTES);
    }
  }

  @Test
  public void testLongCodec() throws Exception {
    final LongCodec codec = new LongCodec();
    runTest(codec, 0L, Long.BYTES);
    runTest(codec, 1L, Long.BYTES);
    runTest(codec, -1L, Long.BYTES);
    runTest(codec, Long.MAX_VALUE, Long.BYTES);
    runTest(codec, Long.MIN_VALUE, Long.BYTES);

    for (int i = 0; i < NUM_LOOPS; i++) {
      final long original = ThreadLocalRandom.current().nextLong();
      runTest(codec, original, Long.BYTES);
    }
  }

  @Test
  public void testStringCodec() throws Exception {
    final StringCodec codec = new StringCodec();
    runTest(codec, "", 0);

    for (int i = 0; i < NUM_LOOPS; i++) {
      final String original = "test" + ThreadLocalRandom.current().nextLong();
      runTest(codec, original, original.length());
    }
  }

  static <T> void runTest(Codec<T> codec, T original,
      Integer serializedSize) throws Exception {
    final byte[] array = codec.toPersistedFormat(original);
    if (serializedSize != null) {
      Assertions.assertEquals(serializedSize, array.length);
    }
    final T fromArray = codec.fromPersistedFormat(array);
    Assertions.assertEquals(original, fromArray);

    final ByteBuffer buffer = codec.toByteBuffer(original);
    Assertions.assertEquals(array.length, buffer.remaining());
    for (int i = 0; i < array.length; i++) {
      Assertions.assertEquals(array[i], buffer.get(i));
    }

    final T fromBuffer = codec.fromByteBuffer(buffer);
    Assertions.assertEquals(original, fromBuffer);

    final T fromWrappedArray = codec.fromByteBuffer(ByteBuffer.wrap(array));
    Assertions.assertEquals(original, fromWrappedArray);
  }
}
