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
 * Codec to serialize/deserialize {@link Integer}.
 */
public final class IntegerCodec implements Codec<Integer> {

  private static final IntegerCodec INSTANCE = new IntegerCodec();

  public static IntegerCodec get() {
    return INSTANCE;
  }

  private IntegerCodec() {
    // singleton
  }

  @Override
  public Class<Integer> getTypeClass() {
    return Integer.class;
  }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull Integer object,
      CodecBuffer.Allocator allocator) {
    return allocator.apply(Integer.BYTES).putInt(object);
  }

  @Override
  public Integer fromCodecBuffer(@Nonnull CodecBuffer buffer) {
    return buffer.asReadOnlyByteBuffer().getInt();
  }

  @Override
  public byte[] toPersistedFormat(Integer object) {
    return ByteBuffer.wrap(new byte[Integer.BYTES]).putInt(object).array();
  }

  @Override
  public Integer fromPersistedFormat(byte[] rawData) {
    return ByteBuffer.wrap(rawData).getInt();
  }

  @Override
  public Integer copyObject(Integer object) {
    return object;
  }
}
