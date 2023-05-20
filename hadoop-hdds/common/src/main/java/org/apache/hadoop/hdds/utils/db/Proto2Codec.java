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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import org.apache.ratis.util.function.CheckedFunction;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.IntFunction;

/**
 * Codecs to serialize/deserialize Protobuf v2 messages.
 */
public final class Proto2Codec<M extends MessageLite>
    implements Codec<M> {
  private static final ConcurrentMap<Class<? extends MessageLite>,
                                     Codec<? extends MessageLite>> CODECS
      = new ConcurrentHashMap<>();

  /**
   * @return the {@link Codec} for the given class.
   */
  public static <T extends MessageLite> Codec<T> get(Class<T> clazz) {
    final Codec<?> codec = CODECS.computeIfAbsent(clazz, Proto2Codec::new);
    return (Codec<T>) codec;
  }

  private static <T extends MessageLite> Parser<T> getParser(Class<T> clazz) {
    final String name = "PARSER";
    try {
      return (Parser<T>) clazz.getField(name).get(null);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to get " + name + " field from " + clazz, e);
    }
  }

  private final Parser<M> parser;

  private Proto2Codec(Class<M> clazz) {
    this.parser = getParser(clazz);
  }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull M message,
      IntFunction<CodecBuffer> allocator) throws IOException {
    final int size = message.getSerializedSize();
    return allocator.apply(size).put(writeTo(message, size));
  }

  private CheckedFunction<OutputStream, Integer, IOException> writeTo(
      M message, int size) {
    return out -> {
      message.writeTo(out);
      return size;
    };
  }

  @Override
  public M fromCodecBuffer(@Nonnull CodecBuffer buffer)
      throws IOException {
    try (InputStream in = buffer.getInputStream()) {
      return parser.parseFrom(in);
    }
  }

  @Override
  public byte[] toPersistedFormat(M message) {
    return message.toByteArray();
  }

  @Override
  public M fromPersistedFormat(byte[] bytes)
      throws InvalidProtocolBufferException {
    return parser.parseFrom(bytes);
  }

  @Override
  public M copyObject(M message) {
    // proto messages are immutable
    return message;
  }
}
