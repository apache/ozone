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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.ClassUtils;

/**
 * Collection of available codecs.
 * This class is immutable.
 */
public final class CodecRegistry {

  private final CodecMap valueCodecs;

  /** To build {@link CodecRegistry}. */
  public static class Builder {
    private final Map<Class<?>, Codec<?>> codecs = new HashMap<>();

    public <T> Builder addCodec(Class<T> type, Codec<T> codec) {
      codecs.put(type, codec);
      return this;
    }

    public CodecRegistry build() {
      return new CodecRegistry(codecs);
    }
  }

  public static Builder newBuilder() {
    return new Builder()
        .addCodec(String.class, StringCodec.get())
        .addCodec(Long.class, LongCodec.get())
        .addCodec(Integer.class, IntegerCodec.get())
        .addCodec(byte[].class, ByteArrayCodec.get())
        .addCodec(Boolean.class, BooleanCodec.get());
  }

  private static final class CodecMap {
    private final Map<Class<?>, Codec<?>> map;

    private CodecMap(Map<Class<?>, Codec<?>> map) {
      this.map = Collections.unmodifiableMap(new HashMap<>(map));
    }

    <T> Codec<T> get(Class<T> clazz) {
      Objects.requireNonNull(clazz, "clazz == null");
      final Codec<?> codec = map.get(clazz);
      return (Codec<T>) codec;
    }

    Codec<?> get(List<Class<?>> classes) {
      for (Class<?> clazz : classes) {
        final Codec<?> codec = get(clazz);
        if (codec != null) {
          return codec;
        }
      }
      return null;
    }
  }

  private CodecRegistry(Map<Class<?>, Codec<?>> valueCodecs) {
    this.valueCodecs = new CodecMap(valueCodecs);
  }

  /**
   * Convert raw value to strongly typed value/key with the help of a codec.
   *
   * @param rawData original byte array from the db.
   * @param format  Class of the return value
   * @param <T>     Type of the return value.
   * @return the object with the parsed field data
   */
  public <T> T asObject(byte[] rawData, Class<T> format)
      throws IOException {
    if (rawData == null) {
      return null;
    }
    return getCodecFromClass(format).fromPersistedFormat(rawData);
  }

  /**
   * Copy object, and return a new object.
   * @param object
   * @param format
   * @param <T>
   * @return new object copied from the given object.
   */
  public <T> T copyObject(T object, Class<T> format) {
    if (object == null) {
      return null;
    }
    return getCodecFromClass(format).copyObject(object);
  }

  /**
   * Convert strongly typed object to raw data to store it in the kv store.
   *
   * @param object typed object.
   * @param <T>    Type of the typed object.
   * @return byte array to store it ini the kv store.
   */
  public <T> byte[] asRawData(T object) throws IOException {
    Objects.requireNonNull(object,
        "Null value shouldn't be persisted in the database");
    Codec<T> codec = getCodec(object);
    return codec.toPersistedFormat(object);
  }

  /**
   * Get a codec for the typed object
   * including its class and the super classes/interfaces.
   *
   * @param <T>    Type of the typed object.
   * @param <SUPER> A super class/interface type.
   * @param object typed object.
   * @return Codec for the given object.
   */
  public <SUPER, T extends SUPER> Codec<SUPER> getCodec(T object) {
    final Class<T> clazz = (Class<T>) object.getClass();
    Codec<?> codec = valueCodecs.get(clazz);
    if (codec == null) {
      codec = valueCodecs.get(ClassUtils.getAllSuperclasses(clazz));
    }
    if (codec == null) {
      codec = valueCodecs.get(ClassUtils.getAllInterfaces(clazz));
    }
    if (codec != null) {
      return (Codec<SUPER>) codec;
    }
    throw new IllegalStateException(
        "Codec is not registered for type: " + clazz);
  }

  /**
   * Get a codec for the given class
   * including its super classes/interfaces.
   *
   * @param <T>    Type of the typed object.
   * @return Codec for the given class.
   */
  <T> Codec<T> getCodecFromClass(Class<T> clazz) {
    final Codec<?> codec = valueCodecs.get(clazz);
    if (codec != null) {
      return (Codec<T>) codec;
    }
    throw new IllegalStateException(
        "Codec is not registered for type: " + clazz);
  }
}
