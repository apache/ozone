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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.ratis.util.function.CheckedFunction;

/**
 * Codecs to serialize/deserialize Protobuf v2 messages.
 */
public final class Proto2Codec<M extends MessageLite> implements Codec<M> {
  private static final ConcurrentMap<Class<? extends MessageLite>, Codec<? extends MessageLite>> CODECS =
      new ConcurrentHashMap<>();

  private final Class<M> clazz;
  private final Parser<M> parser;

  /**
   * @return the {@link Codec} for the given class.
   */
  public static <T extends MessageLite> Codec<T> get(T t) {
    final Codec<?> codec = CODECS.computeIfAbsent(t.getClass(),
        key -> new Proto2Codec<>(t));
    return (Codec<T>) codec;
  }

  private Proto2Codec(M m) {
    this.clazz = (Class<M>) m.getClass();
    this.parser = (Parser<M>) m.getParserForType();
  }

  @Override
  public Class<M> getTypeClass() {
    return clazz;
  }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull M message,
      CodecBuffer.Allocator allocator) throws CodecException {
    final int size = message.getSerializedSize();
    return allocator.apply(size).put(writeTo(message, size));
  }

  private CheckedFunction<OutputStream, Integer, IOException> writeTo(
      M message, int size) {
    return new CheckedFunction<OutputStream, Integer, IOException>() {
      @Override
      public Integer apply(OutputStream out) throws IOException {
        message.writeTo(out);
        return size;
      }

      @Override
      public String toString() {
        return "source: size=" + size + ", message=" + message;
      }
    };
  }

  @Override
  public M fromCodecBuffer(@Nonnull CodecBuffer buffer)
      throws CodecException {
    final InputStream in = buffer.getInputStream();
    try {
      return parser.parseFrom(in);
    } catch (InvalidProtocolBufferException e) {
      throw new CodecException("Failed to parse " + buffer + " for " + getTypeClass(), e);
    } finally {
      IOUtils.closeQuietly(in);
    }
  }

  @Override
  public byte[] toPersistedFormat(M message) {
    return message.toByteArray();
  }

  @Override
  public M fromPersistedFormatImpl(byte[] bytes)
      throws InvalidProtocolBufferException {
    return parser.parseFrom(bytes);
  }

  @Override
  public M copyObject(M message) {
    // proto messages are immutable
    return message;
  }
}
