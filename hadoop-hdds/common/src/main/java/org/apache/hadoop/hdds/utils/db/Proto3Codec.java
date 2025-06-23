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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.ToIntFunction;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.com.google.protobuf.MessageLite;
import org.apache.ratis.thirdparty.com.google.protobuf.Parser;

/**
 * Codecs to serialize/deserialize Protobuf v3 messages.
 */
public final class Proto3Codec<M extends MessageLite> implements Codec<M> {
  private static final ConcurrentMap<Class<? extends MessageLite>, Codec<? extends MessageLite>> CODECS =
      new ConcurrentHashMap<>();

  private final Class<M> clazz;
  private final Parser<M> parser;
  private final boolean allowInvalidProtocolBufferException;

  /**
   * @return the {@link Codec} for the given class.
   */
  public static <T extends MessageLite> Codec<T> get(T t) {
    return get(t, false);
  }

  public static <T extends MessageLite> Codec<T> get(T t, boolean allowInvalidProtocolBufferException) {
    final Codec<?> codec = CODECS.computeIfAbsent(t.getClass(),
        key -> new Proto3Codec<>(t, allowInvalidProtocolBufferException));
    return (Codec<T>) codec;
  }

  private Proto3Codec(M m, boolean allowInvalidProtocolBufferException) {
    this.clazz = (Class<M>) m.getClass();
    this.parser = (Parser<M>) m.getParserForType();
    this.allowInvalidProtocolBufferException = allowInvalidProtocolBufferException;
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
  public CodecBuffer toCodecBuffer(@Nonnull M message, CodecBuffer.Allocator allocator) {
    final int size = message.getSerializedSize();
    final CodecBuffer codecBuffer = allocator.apply(size);
    final ToIntFunction<ByteBuffer> writeTo = buffer -> {
      try {
        message.writeTo(CodedOutputStream.newInstance(buffer));
      } catch (IOException e) {
        // The buffer was allocated with the message size, it should never throw an IOException
        throw new IllegalStateException(
            "Failed to writeTo: message=" + message, e);
      }
      return size;
    };
    codecBuffer.put(writeTo);
    return codecBuffer;
  }

  @Override
  public M fromCodecBuffer(@Nonnull CodecBuffer buffer) throws CodecException {
    try {
      return parser.parseFrom(buffer.asReadOnlyByteBuffer());
    } catch (InvalidProtocolBufferException e) {
      if (allowInvalidProtocolBufferException) {
        return null;
      }
      throw new CodecException("Failed to parse " + buffer + " for " + getTypeClass(), e);
    }
  }

  @Override
  public byte[] toPersistedFormat(M message) {
    return message.toByteArray();
  }

  @Override
  public M fromPersistedFormatImpl(byte[] bytes)
      throws InvalidProtocolBufferException {
    try {
      return parser.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      if (allowInvalidProtocolBufferException) {
        return null;
      }
      throw e;
    }
  }

  @Override
  public M copyObject(M message) {
    // proto messages are immutable
    return message;
  }
}
