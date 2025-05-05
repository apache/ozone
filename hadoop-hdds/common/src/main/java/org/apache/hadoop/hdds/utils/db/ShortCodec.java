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
 * Codec to serialize/deserialize {@link Short}.
 */
public final class ShortCodec implements Codec<Short> {

  private static final ShortCodec INSTANCE = new ShortCodec();

  public static ShortCodec get() {
    return INSTANCE;
  }

  private ShortCodec() {
    // singleton
  }

  @Override
  public Class<Short> getTypeClass() {
    return Short.class;
  }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull Short object,
      CodecBuffer.Allocator allocator) {
    return allocator.apply(Short.BYTES).putShort(object);
  }

  @Override
  public Short fromCodecBuffer(@Nonnull CodecBuffer buffer) {
    return buffer.asReadOnlyByteBuffer().getShort();
  }

  @Override
  public byte[] toPersistedFormat(Short object) {
    return ByteBuffer.wrap(new byte[Short.BYTES]).putShort(object).array();
  }

  @Override
  public Short fromPersistedFormat(byte[] rawData) {
    return ByteBuffer.wrap(rawData).getShort();
  }

  @Override
  public Short copyObject(Short object) {
    return object;
  }
}
