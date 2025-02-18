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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link CodecRegistry}.
 */
public final class TestCodecRegistry {
  static final Logger LOG = LoggerFactory.getLogger(TestCodecRegistry.class);

  private final CodecRegistry registry = CodecRegistry.newBuilder()
      .addCodec(ByteString.class, ByteStringCodec.get())
      .build();

  <T> void assertGetCodec(Class<?> expectedCodecClass, T object) {
    final Codec<T> codec = registry.getCodec(object);
    LOG.info("object {}", object.getClass());
    LOG.info("codec {}", codec.getClass());
    assertTrue(expectedCodecClass.isInstance(codec));
    assertSame(expectedCodecClass, codec.getClass());
  }

  @Test
  public void testGetCodec() {
    assertGetCodec(IntegerCodec.class, 1);
    assertGetCodec(LongCodec.class, 2L);
    assertGetCodec(StringCodec.class, "3");
    assertGetCodec(ByteArrayCodec.class, Codec.EMPTY_BYTE_ARRAY);
    assertGetCodec(ByteStringCodec.class, ByteString.EMPTY);
  }

  <T> void assertGetCodecFromClass(Class<?> expectedCodecClass,
      Class<T> format) {
    final Codec<T> codec = registry.getCodecFromClass(format);
    LOG.info("format {}", format);
    LOG.info("codec {}", codec.getClass());
    assertSame(expectedCodecClass, codec.getClass());
  }

  @Test
  public void testGetCodecFromClass() {
    assertGetCodecFromClass(IntegerCodec.class, Integer.class);
    assertGetCodecFromClass(LongCodec.class, Long.class);
    assertGetCodecFromClass(StringCodec.class, String.class);
    assertGetCodecFromClass(ByteArrayCodec.class, byte[].class);
    assertGetCodecFromClass(ByteStringCodec.class, ByteString.class);
  }
}
