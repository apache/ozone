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
import java.util.Objects;

/**
 * Codec interface to serialize/deserialize objects to/from bytes.
 * A codec implementation must support the byte[] methods
 * and may optionally support the {@link CodecBuffer} methods.
 *
 * @param <T> The object type.
 */
public interface Codec<T> {
  byte[] EMPTY_BYTE_ARRAY = {};

  /** @return the class of the {@link T}. */
  Class<T> getTypeClass();

  /**
   * Does this {@link Codec} support the {@link CodecBuffer} methods?
   * If this method returns true, this class must implement both
   * {@link #toCodecBuffer(Object, CodecBuffer.Allocator)} and
   * {@link #fromCodecBuffer(CodecBuffer)}.
   *
   * @return ture iff this class supports the {@link CodecBuffer} methods.
   */
  default boolean supportCodecBuffer() {
    return false;
  }

  /**
   * Serialize the given object to bytes.
   *
   * @param object The object to be serialized.
   * @param allocator To allocate a buffer.
   * @return a buffer storing the serialized bytes.
   */
  default CodecBuffer toCodecBuffer(@Nonnull T object, CodecBuffer.Allocator allocator) throws CodecException {
    throw new UnsupportedOperationException();
  }

  /**
   * Serialize the given object to bytes.
   *
   * @param object The object to be serialized.
   * @return a direct buffer storing the serialized bytes.
   */
  default CodecBuffer toDirectCodecBuffer(@Nonnull T object) throws CodecException {
    return toCodecBuffer(object, CodecBuffer.Allocator.getDirect());
  }

  /**
   * Serialize the given object to bytes.
   *
   * @param object The object to be serialized.
   * @return a heap buffer storing the serialized bytes.
   */
  default CodecBuffer toHeapCodecBuffer(@Nonnull T object) throws CodecException {
    return toCodecBuffer(object, CodecBuffer.Allocator.getHeap());
  }

  /**
   * Deserialize an object from the given buffer.
   *
   * @param buffer Storing the serialized bytes of an object.
   * @return the deserialized object.
   */
  default T fromCodecBuffer(@Nonnull CodecBuffer buffer) throws CodecException {
    throw new UnsupportedOperationException();
  }

  /**
   * Convert object to raw persisted format.
   * @param object The original java object. Should not be null.
   */
  default byte[] toPersistedFormat(T object) throws CodecException {
    Objects.requireNonNull(object, "object == null");
    try {
      return toPersistedFormatImpl(object);
    } catch (Exception e) {
      throw new CodecException("Failed to serialize " + object
          + " for " + object.getClass(), e);
    }
  }

  /**
   * The same as {@link #toPersistedFormat} except that this method throws {@link Exception}.
   * A subclass must implement either {@link #toPersistedFormat} or this method.
   */
  default byte[] toPersistedFormatImpl(T object) throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Convert object from raw persisted format.
   *
   * @param rawData Byte array from the key/value store. Should not be null.
   */
  default T fromPersistedFormat(byte[] rawData) throws CodecException {
    Objects.requireNonNull(rawData, "rawData == null");
    try {
      return fromPersistedFormatImpl(rawData);
    } catch (Exception e) {
      throw new CodecException("Failed to deserialize rawData (length=" + rawData.length
          + ") for " + getTypeClass(), e);
    }
  }

  /**
   * The same as {@link #fromPersistedFormat} except that this method throws {@link Exception}.
   * A subclass must implement either {@link #fromPersistedFormat} or this method.
   */
  default T fromPersistedFormatImpl(byte[] rawData) throws Exception {
    throw new UnsupportedOperationException();
  }

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
