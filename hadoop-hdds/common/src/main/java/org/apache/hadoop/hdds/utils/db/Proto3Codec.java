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

import org.apache.ratis.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.com.google.protobuf.MessageLite;
import org.apache.ratis.thirdparty.com.google.protobuf.Parser;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.ToIntFunction;

/**
 * Codecs to serialize/deserialize Protobuf v3 messages.
 */
public final class Proto3Codec<M extends MessageLite>
    implements Codec<M> {
  private static final ConcurrentMap<Class<? extends MessageLite>,
                                     Codec<? extends MessageLite>> CODECS
      = new ConcurrentHashMap<>();

  /**
   * @return the {@link Codec} for the given class.
   */
  public static <T extends MessageLite> Codec<T> get(T t) {
    final Codec<?> codec = CODECS.computeIfAbsent(t.getClass(),
        key -> new Proto3Codec<>(t));
    return (Codec<T>) codec;
  }

  private final Parser<M> parser;

  private Proto3Codec(M m) {
    this.parser = (Parser<M>) m.getParserForType();
  }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  private ToIntFunction<ByteBuffer> writeTo(M message, int size) {
    return buffer -> {
      try {
        message.writeTo(CodedOutputStream.newInstance(buffer));
      } catch (IOException e) {
        throw new IllegalStateException(
            "Failed to writeTo: message=" + message, e);
      }
      return size;
    };
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull M message,
      CodecBuffer.Allocator allocator) {
    final int size = message.getSerializedSize();
    return allocator.apply(size).put(writeTo(message, size));
  }

  @Override
  public M fromCodecBuffer(@Nonnull CodecBuffer buffer)
      throws InvalidProtocolBufferException {
    return parser.parseFrom(buffer.asReadOnlyByteBuffer());
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
