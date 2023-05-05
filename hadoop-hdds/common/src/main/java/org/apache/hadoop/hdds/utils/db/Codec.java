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
 *
 */
package org.apache.hadoop.hdds.utils.db;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Codec interface to serialize/deserialize objects to/from bytes.
 * A codec implementation must support the byte[] methods
 * and may optionally support the {@link ByteBuffer} methods.
 *
 * @param <T> The object type.
 */
public interface Codec<T> {
  /**
   * Does this {@link Codec} support the {@link ByteBuffer} methods?
   * If this method returns true, this class must implement both
   * {@link #toByteBuffer(Object)} and {@link #fromByteBuffer(ByteBuffer)}.
   *
   * @return ture iff this class supports the {@link ByteBuffer} methods.
   */
  default boolean supportByteBuffer() {
    return false;
  }

  /**
   * Serialize the given object to bytes.
   *
   * @param object The object to be serialized.
   * @return the serialized bytes.
   */
  default ByteBuffer toByteBuffer(@Nonnull T object) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Deserialize an object from the given buffer.
   * The position of the given buffer is incremented by the serialized size.
   *
   * @param buffer Serialized bytes of an object.
   * @return the deserialized object.
   */
  default T fromByteBuffer(@Nonnull ByteBuffer buffer) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Convert object to raw persisted format.
   * @param object The original java object. Should not be null.
   */
  byte[] toPersistedFormat(T object) throws IOException;

  /**
   * Convert object from raw persisted format.
   *
   * @param rawData Byte array from the key/value store. Should not be null.
   */
  T fromPersistedFormat(byte[] rawData) throws IOException;

  /**
   * Copy the given object.
   * When the given object is immutable,
   * the implementation of this method may safely return the given object.
   *
   * @param object The object to be copied.
   * @return a copy of the given object.  When the given object is immutable,
   *         the returned object can possibly be the same as the given object.
   */
  T copyObject(T object);
}
