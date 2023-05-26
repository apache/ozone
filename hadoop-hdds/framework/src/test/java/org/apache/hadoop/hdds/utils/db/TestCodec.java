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
 */
package org.apache.hadoop.hdds.utils.db;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.utils.db.RDBBatchOperation.Bytes;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Test {@link Codec} implementations.
 */
public final class TestCodec {
  static final Logger LOG = LoggerFactory.getLogger(TestCodec.class);
  static final int NUM_LOOPS = 10;

  /** Force gc to check leakage. */
  static void gc() throws InterruptedException {
    // use WeakReference to detect gc
    Object obj = new Object();
    final WeakReference<Object> weakRef = new WeakReference<>(obj);
    obj = null;

    // loop until gc has completed.
    for (int i = 0; weakRef.get() != null; i++) {
      LOG.info("gc {}", i);
      System.gc();
      Thread.sleep(100);
    }
    CodecBuffer.assertNoLeaks();
  }

  @Test
  public void testIntegerCodec() throws Exception {
    final Codec<Integer> codec = IntegerCodec.get();
    runTest(codec, 0, Integer.BYTES);
    runTest(codec, 1, Integer.BYTES);
    runTest(codec, -1, Integer.BYTES);
    runTest(codec, Integer.MAX_VALUE, Integer.BYTES);
    runTest(codec, Integer.MIN_VALUE, Integer.BYTES);

    for (int i = 0; i < NUM_LOOPS; i++) {
      final int original = ThreadLocalRandom.current().nextInt();
      runTest(codec, original, Integer.BYTES);
    }
    gc();
  }

  @Test
  public void testLongCodec() throws Exception {
    final LongCodec codec = LongCodec.get();
    runTest(codec, 0L, Long.BYTES);
    runTest(codec, 1L, Long.BYTES);
    runTest(codec, -1L, Long.BYTES);
    runTest(codec, Long.MAX_VALUE, Long.BYTES);
    runTest(codec, Long.MIN_VALUE, Long.BYTES);

    for (int i = 0; i < NUM_LOOPS; i++) {
      final long original = ThreadLocalRandom.current().nextLong();
      runTest(codec, original, Long.BYTES);
    }
    gc();
  }

  @Test
  public void testStringCodec() throws Exception {
    final StringCodec codec = StringCodec.get();
    runTest(codec, "", 0);

    for (int i = 0; i < NUM_LOOPS; i++) {
      final String original = "test" + ThreadLocalRandom.current().nextLong();
      runTest(codec, original, original.length());
    }
    gc();
  }

  @Test
  public void testUuidCodec() throws Exception {
    final int size = UuidCodec.getSerializedSize();
    final UuidCodec codec = UuidCodec.get();
    runTest(codec, new UUID(0L, 0L), size);
    runTest(codec, new UUID(-1L, -1L), size);

    for (int i = 0; i < NUM_LOOPS; i++) {
      final UUID original = UUID.randomUUID();
      runTest(codec, original, size);
    }
    gc();
  }

  static <T> void runTest(Codec<T> codec, T original,
      Integer serializedSize) throws Exception {
    Assertions.assertTrue(codec.supportCodecBuffer());

    // serialize to byte[]
    final byte[] array = codec.toPersistedFormat(original);
    if (serializedSize != null) {
      Assertions.assertEquals(serializedSize, array.length);
    }
    // deserialize from byte[]
    final T fromArray = codec.fromPersistedFormat(array);
    Assertions.assertEquals(original, fromArray);

    // serialize to CodecBuffer
    final CodecBuffer codecBuffer = codec.toCodecBuffer(
        original, CodecBuffer::allocateHeap);
    final ByteBuffer byteBuffer = codecBuffer.asReadOnlyByteBuffer();
    Assertions.assertEquals(array.length, byteBuffer.remaining());
    for (int i = 0; i < array.length; i++) {
      // assert exact content
      Assertions.assertEquals(array[i], byteBuffer.get(i));
    }

    // deserialize from CodecBuffer
    final T fromBuffer = codec.fromCodecBuffer(codecBuffer);
    codecBuffer.release();
    Assertions.assertEquals(original, fromBuffer);

    // deserialize from wrapped buffer
    final CodecBuffer wrapped = CodecBuffer.wrap(array);
    final T fromWrappedArray = codec.fromCodecBuffer(wrapped);
    wrapped.release();
    Assertions.assertEquals(original, fromWrappedArray);

    runTestBytes(original, codec);
  }

  static <T> void runTestBytes(T object, Codec<T> codec) throws IOException {
    final byte[] array = codec.toPersistedFormat(object);
    final Bytes fromArray = new Bytes(array);

    try (CodecBuffer buffer = codec.toCodecBuffer(object,
        CodecBuffer::allocateHeap)) {
      final Bytes fromBuffer = new Bytes(buffer);

      Assertions.assertEquals(fromArray.hashCode(), fromBuffer.hashCode());
      Assertions.assertEquals(fromArray, fromBuffer);
      Assertions.assertEquals(fromBuffer, fromArray);
    }
  }
}
