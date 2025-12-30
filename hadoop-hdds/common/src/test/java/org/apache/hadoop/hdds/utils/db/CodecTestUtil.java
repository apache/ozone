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

package org.apache.hadoop.hdds.utils.db;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link Codec} implementations.
 */
public final class CodecTestUtil {
  static final Logger LOG = LoggerFactory.getLogger(CodecTestUtil.class);

  private CodecTestUtil() {
  }

  /**
   * Force gc to check leakage.
   */
  public static void gc() throws InterruptedException {
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

  public static <T> void runTest(Codec<T> codec, T original,
      Integer serializedSize, Codec<T> oldCodec) throws Exception {
    assertTrue(codec.supportCodecBuffer());

    // serialize to byte[]
    final byte[] array = codec.toPersistedFormat(original);
    LOG.info("encoded length = " + array.length);
    if (serializedSize != null) {
      assertEquals(serializedSize, array.length);
    }
    if (oldCodec != null) {
      final byte[] expected = oldCodec.toPersistedFormat(original);
      assertArrayEquals(expected, array);
    }
    // deserialize from byte[]
    final T fromArray = codec.fromPersistedFormat(array);
    assertEquals(original, fromArray);

    // serialize to CodecBuffer
    final CodecBuffer codecBuffer = codec.toCodecBuffer(
        original, CodecBuffer.Allocator.getHeap());
    assertEquals(array.length, codecBuffer.readableBytes());
    final ByteBuffer byteBuffer = codecBuffer.asReadOnlyByteBuffer();
    assertEquals(array.length, byteBuffer.remaining());
    for (int i = 0; i < array.length; i++) {
      // assert exact content
      assertEquals(array[i], byteBuffer.get(i));
    }
    if (oldCodec != null && oldCodec.supportCodecBuffer()) {
      try (CodecBuffer expected = oldCodec.toHeapCodecBuffer(original)) {
        assertEquals(expected.asReadOnlyByteBuffer(),
            codecBuffer.asReadOnlyByteBuffer());
      }
    }

    // deserialize from CodecBuffer
    final T fromBuffer = codec.fromCodecBuffer(codecBuffer);
    codecBuffer.release();
    assertEquals(original, fromBuffer);

    // deserialize from wrapped buffer
    final CodecBuffer wrapped = CodecBuffer.wrap(array);
    final T fromWrappedArray = codec.fromCodecBuffer(wrapped);
    wrapped.release();
    assertEquals(original, fromWrappedArray);
  }

  public static <T> Codec<T> newCodecWithoutCodecBuffer(Codec<T> codec) {
    assertTrue(codec.supportCodecBuffer());
    final Codec<T> newCodec = new Codec<T>() {
      @Override
      public byte[] toPersistedFormat(T object) throws CodecException {
        return codec.toPersistedFormat(object);
      }

      @Override
      public T fromPersistedFormat(byte[] rawData) throws CodecException {
        return codec.fromPersistedFormat(rawData);
      }

      @Override
      public Class<T> getTypeClass() {
        return codec.getTypeClass();
      }

      @Override
      public T copyObject(T object) {
        return codec.copyObject(object);
      }
    };

    assertFalse(newCodec.supportCodecBuffer());
    return newCodec;
  }
}
