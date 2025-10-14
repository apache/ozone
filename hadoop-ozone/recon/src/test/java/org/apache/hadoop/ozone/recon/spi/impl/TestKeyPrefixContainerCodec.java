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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.ozone.recon.api.types.KeyPrefixContainer;
import org.junit.jupiter.api.Test;

/**
 * Class to test {@link KeyPrefixContainerCodec}.
 */
public class TestKeyPrefixContainerCodec {

  private final Codec<KeyPrefixContainer> codec = KeyPrefixContainerCodec.get();

  @Test
  public void testKeyPrefixWithDelimiter() throws Exception {
    runTest("testKey", 123L, 456L);
    runTest("test_key_with_underscores", 789L, 101112L);
    runTest("test___________________________________Key", 1L, 2L);
    runTest("", 0L, 0L);
  }

  @Test
  public void testCodecBufferSupport() {
    assertTrue(codec.supportCodecBuffer());
  }

  @Test
  public void testTypeClass() {
    assertEquals(KeyPrefixContainer.class, codec.getTypeClass());
  }

  void runTest(String keyPrefix, long version, long containerId) throws Exception {
    final KeyPrefixContainer original = KeyPrefixContainer.get(keyPrefix, version, containerId);
    final KeyPrefixContainer keyAndVersion = KeyPrefixContainer.get(keyPrefix, version);
    final KeyPrefixContainer keyOnly = KeyPrefixContainer.get(keyPrefix);

    final CodecBuffer.Allocator allocator = CodecBuffer.Allocator.getHeap();
    try (CodecBuffer originalBuffer = codec.toCodecBuffer(original, allocator);
         CodecBuffer keyOnlyBuffer = codec.toCodecBuffer(keyOnly, allocator);
         CodecBuffer keyAndVersionBuffer = codec.toCodecBuffer(keyAndVersion, allocator)) {
      assertEquals(original, codec.fromCodecBuffer(originalBuffer));
      assertTrue(originalBuffer.startsWith(keyAndVersionBuffer));
      assertTrue(originalBuffer.startsWith(keyOnlyBuffer));

      final byte[] originalBytes = assertCodecBuffer(original, originalBuffer);
      assertEquals(original, codec.fromPersistedFormat(originalBytes));

      final byte[] keyAndVersionBytes = assertCodecBuffer(keyAndVersion, keyAndVersionBuffer);
      assertPrefix(originalBytes.length - KeyPrefixContainerCodec.LONG_SERIALIZED_SIZE,
          originalBytes, keyAndVersionBytes);

      final byte[] keyOnlyBytes = assertCodecBuffer(keyOnly, keyOnlyBuffer);
      assertPrefix(originalBytes.length - 2 * KeyPrefixContainerCodec.LONG_SERIALIZED_SIZE,
          originalBytes, keyOnlyBytes);
    }
  }

  static void assertPrefix(int expectedLength, byte[] array, byte[] prefix) {
    assertEquals(expectedLength, prefix.length);
    for (int i = 0; i < prefix.length; i++) {
      assertEquals(array[i], prefix[i]);
    }
  }

  byte[] assertCodecBuffer(KeyPrefixContainer original, CodecBuffer buffer) throws Exception {
    final byte[] bytes = codec.toPersistedFormat(original);
    assertArrayEquals(bytes, buffer.getArray());
    return bytes;
  }
}
