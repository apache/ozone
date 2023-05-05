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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.ClassUtils;

/**
 * Collection of available codecs.
 * This class is immutable.
 */
public final class CodecRegistry {
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
        .addCodec(String.class, new StringCodec())
        .addCodec(Long.class, new LongCodec())
        .addCodec(Integer.class, new IntegerCodec())
        .addCodec(byte[].class, new ByteArrayCodec());
  }

  private final Map<Class<?>, Codec<?>> valueCodecs;

  private CodecRegistry(Map<Class<?>, Codec<?>> valueCodecs) {
    this.valueCodecs = Collections.unmodifiableMap(new HashMap<>(valueCodecs));
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
    return getCodec(format).fromPersistedFormat(rawData);
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
    return getCodec(format).copyObject(object);
  }

  /**
   * Convert strongly typed object to raw data to store it in the kv store.
   *
   * @param object typed object.
   * @param <T>    Type of the typed object.
   * @return byte array to store it ini the kv store.
   */
  public <T> byte[] asRawData(T object) throws IOException {
    Preconditions.checkNotNull(object,
        "Null value shouldn't be persisted in the database");
    Codec<T> codec = getCodec(object);
    return codec.toPersistedFormat(object);
  }

  /**
   * Get codec for the typed object including class and subclass.
   * @param object typed object.
   * @return Codec for the typed object.
   */
  public <T> Codec<T> getCodec(T object) {
    Class<T> format = (Class<T>) object.getClass();
    return getCodec(format);
  }


  /**
   * Get codec for the typed object including class and subclass.
   * @param <T>    Type of the typed object.
   * @return Codec for the typed object.
   */
  private <T> Codec<T> getCodec(Class<T> format) {
    final List<Class<?>> classes = new ArrayList<>();
    classes.add(format);
    classes.addAll(ClassUtils.getAllSuperclasses(format));
    classes.addAll(ClassUtils.getAllInterfaces(format));
    for (Class<?> clazz : classes) {
      final Codec<?> codec = valueCodecs.get(clazz);
      if (codec != null) {
        return (Codec<T>) codec;
      }
    }
    throw new IllegalStateException(
        "Codec is not registered for type: " + format);
  }
}
