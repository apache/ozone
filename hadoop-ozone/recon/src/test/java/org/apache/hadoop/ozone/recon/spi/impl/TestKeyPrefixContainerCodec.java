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
  public void testKeyPrefixOnly() throws Exception {
    KeyPrefixContainer original = KeyPrefixContainer.get("testKey");
    testCodecBuffer(original);
  }

  @Test
  public void testKeyPrefixAndVersion() throws Exception {
    KeyPrefixContainer original = KeyPrefixContainer.get("testKey", 123L);
    testCodecBuffer(original);
  }

  @Test
  public void testKeyPrefixVersionAndContainer() throws Exception {
    KeyPrefixContainer original = KeyPrefixContainer.get("testKey", 123L, 456L);
    testCodecBuffer(original);
  }

  @Test
  public void testEmptyKeyPrefix() throws Exception {
    KeyPrefixContainer original = KeyPrefixContainer.get("");
    testCodecBuffer(original);
  }

  @Test
  public void testKeyPrefixWithDelimiter() throws Exception {
    KeyPrefixContainer original = KeyPrefixContainer.get("test_key_with_underscores", 789L, 101112L);
    testCodecBuffer(original);
  }

  @Test
  public void testCodecBufferSupport() {
    assertTrue(codec.supportCodecBuffer());
  }

  @Test
  public void testTypeClass() {
    assertEquals(KeyPrefixContainer.class, codec.getTypeClass());
  }

  private void testCodecBuffer(KeyPrefixContainer original) throws Exception {

    final CodecBuffer codecBuffer = codec.toCodecBuffer(
        original, CodecBuffer.Allocator.getHeap());
    final KeyPrefixContainer fromBuffer = codec.fromCodecBuffer(codecBuffer);

    final byte[] bytes = codec.toPersistedFormat(original);
    assertArrayEquals(bytes, codecBuffer.getArray());

    codecBuffer.release();
    assertEquals(original, fromBuffer);
    assertEquals(original, codec.fromPersistedFormat(bytes));
  }
}
