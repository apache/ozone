/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.utils.db;

import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;
import java.util.function.IntFunction;

/**
 * Codec to serialize/deserialize a {@link ByteString}.
 */
public final class ByteStringCodec implements Codec<ByteString> {
  private static final ByteStringCodec INSTANCE = new ByteStringCodec();

  public static ByteStringCodec getInstance() {
    return INSTANCE;
  }

  private ByteStringCodec() { }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull ByteString object,
      IntFunction<CodecBuffer> allocator) {
    return allocator.apply(object.size()).put(object.asReadOnlyByteBuffer());
  }

  @Override
  public ByteString fromCodecBuffer(@Nonnull CodecBuffer buffer) {
    return ByteString.copyFrom(buffer.asReadOnlyByteBuffer());
  }

  /**
   * Convert object to raw persisted format.
   *
   * @param object The original java object. Should not be null.
   */
  @Override
  public byte[] toPersistedFormat(ByteString object) {
    if (object == null) {
      return EMPTY_BYTE_ARRAY;
    }
    return object.toByteArray();
  }

  /**
   * Convert object from raw persisted format.
   *
   * @param rawData Byte array from the key/value store. Should not be null.
   */
  @Override
  public ByteString fromPersistedFormat(byte[] rawData) {
    if (rawData == null) {
      return ByteString.EMPTY;
    }
    return ByteString.copyFrom(rawData);
  }

  /**
   * Copy Object from the provided object, and returns a new object.
   *
   * @param object a ByteString
   */
  @Override
  public ByteString copyObject(ByteString object) {
    if (object == null) {
      return ByteString.EMPTY;
    }
    return object;
  }
}
