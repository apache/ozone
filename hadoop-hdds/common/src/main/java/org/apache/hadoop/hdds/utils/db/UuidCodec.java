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
import java.util.UUID;

/**
 * Codec to serialize/deserialize {@link UUID}.
 */
public final class UuidCodec implements Codec<UUID> {
  private static final int SERIALIZED_SIZE = 16;

  private static final UuidCodec CODEC = new UuidCodec();

  public static UuidCodec get() {
    return CODEC;
  }

  public static int getSerializedSize() {
    return SERIALIZED_SIZE;
  }

  private UuidCodec() { }

  @Override
  public Class<UUID> getTypeClass() {
    return UUID.class;
  }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull UUID id,
      CodecBuffer.Allocator allocator) {
    return allocator.apply(SERIALIZED_SIZE)
        .putLong(id.getMostSignificantBits())
        .putLong(id.getLeastSignificantBits());
  }

  @Override
  public UUID fromCodecBuffer(@Nonnull CodecBuffer buffer) {
    return getUuid(buffer.asReadOnlyByteBuffer());
  }

  private static UUID getUuid(ByteBuffer buffer) {
    return new UUID(buffer.getLong(), buffer.getLong());
  }

  @Override
  public byte[] toPersistedFormat(UUID id) {
    final byte[] array = new byte[SERIALIZED_SIZE];
    final ByteBuffer buffer = ByteBuffer.wrap(array);
    buffer.putLong(id.getMostSignificantBits());
    buffer.putLong(id.getLeastSignificantBits());
    return array;
  }

  @Override
  public UUID fromPersistedFormat(byte[] array) {
    if (array.length != SERIALIZED_SIZE) {
      throw new IllegalArgumentException("Unexpected array length: "
          + array.length + " != " + SERIALIZED_SIZE);
    }
    return getUuid(ByteBuffer.wrap(array));
  }

  @Override
  public UUID copyObject(UUID object) {
    return object;
  }
}
