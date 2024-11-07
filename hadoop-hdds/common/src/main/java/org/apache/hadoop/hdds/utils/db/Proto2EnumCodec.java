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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.utils.db;

import org.apache.ratis.thirdparty.com.google.protobuf.ProtocolMessageEnum;
import jakarta.annotation.Nonnull;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Codecs to serialize/deserialize Protobuf v2 enums.
 */
public final class Proto2EnumCodec<M extends ProtocolMessageEnum>
    implements Codec<M> {
  private static final ConcurrentMap<Class<? extends ProtocolMessageEnum>,
                                     Codec<? extends ProtocolMessageEnum>> CODECS
      = new ConcurrentHashMap<>();
  private static final IntegerCodec INTEGER_CODEC = IntegerCodec.get();

  /**
   * @return the {@link Codec} for the given class.
   */
  public static <T extends ProtocolMessageEnum> Codec<T> get(T t) {
    final Codec<?> codec = CODECS.computeIfAbsent(t.getClass(),
        key -> new Proto2EnumCodec<>(t));
    return (Codec<T>) codec;
  }

  private final Class<M> clazz;

  private Proto2EnumCodec(M m) {
    this.clazz = (Class<M>) m.getClass();
  }

  @Override
  public Class<M> getTypeClass() {
    return clazz;
  }

  @Override
  public boolean supportCodecBuffer() {
    return INTEGER_CODEC.supportCodecBuffer();
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull M value,
      CodecBuffer.Allocator allocator) throws IOException {
    return INTEGER_CODEC.toCodecBuffer(value.getNumber(), allocator);
  }

  private M parseFrom(Integer value) throws IOException {
    try {
      return (M) this.clazz.getDeclaredMethod("forNumber", int.class).invoke(null, value);
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IOException(e);
    }
  }

  @Override
  public M fromCodecBuffer(@Nonnull CodecBuffer buffer)
      throws IOException {
    return parseFrom(INTEGER_CODEC.fromCodecBuffer(buffer));
  }

  @Override
  public byte[] toPersistedFormat(M value) {
    return INTEGER_CODEC.toPersistedFormat(value.getNumber());
  }

  @Override
  public M fromPersistedFormat(byte[] bytes) throws IOException {
    return parseFrom(INTEGER_CODEC.fromPersistedFormat(bytes));
  }

  @Override
  public M copyObject(M message) {
    // proto messages are immutable
    return message;
  }
}
