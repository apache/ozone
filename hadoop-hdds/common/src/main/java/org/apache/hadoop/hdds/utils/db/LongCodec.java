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

import jakarta.annotation.Nonnull;
import java.nio.ByteBuffer;

/**
 * Codec to serialize/deserialize {@link Long}.
 */
public final class LongCodec implements Codec<Long> {
  private static final LongCodec CODEC = new LongCodec();

  public static LongCodec get() {
    return CODEC;
  }

  private LongCodec() { }

  @Override
  public Class<Long> getTypeClass() {
    return Long.class;
  }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull Long object,
      CodecBuffer.Allocator allocator) {
    return allocator.apply(Long.BYTES).putLong(object);
  }

  @Override
  public Long fromCodecBuffer(@Nonnull CodecBuffer buffer) {
    return buffer.asReadOnlyByteBuffer().getLong();
  }

  @Override
  public byte[] toPersistedFormat(Long object) {
    if (object == null) {
      return null;
    }
    return ByteBuffer.wrap(new byte[Long.BYTES]).putLong(object).array();
  }

  @Override
  public Long fromPersistedFormat(byte[] rawData) {
    if (rawData != null) {
      return ByteBuffer.wrap(rawData).getLong();
    } else {
      return null;
    }
  }

  @Override
  public Long copyObject(Long object) {
    return object;
  }
}
