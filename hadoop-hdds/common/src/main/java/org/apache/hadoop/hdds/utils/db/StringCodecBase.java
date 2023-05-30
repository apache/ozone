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

import org.apache.hadoop.hdds.StringUtils;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * An abstract {@link Codec} to serialize/deserialize {@link String}
 * using a {@link Charset} provided by subclasses.
 */
abstract class StringCodecBase implements Codec<String> {
  static final Logger LOG = LoggerFactory.getLogger(StringCodecBase.class);

  private final Charset charset;
  private final CharsetEncoder encoder;
  private final CharsetDecoder decoder;

  StringCodecBase(Charset charset) {
    this.charset = charset;
    this.encoder = charset.newEncoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);
    this.decoder = charset.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);
  }

  /**
   * Return the exact serialized size of the given string.
   * For variable-length {@link Charset}s,
   * this method usually is more expensive than
   * {@link #getSerializedSizeUpperBound(String)}
   * since it may require encoding the string.
   */
  int getSerializedSize(String s) {
    return charset.encode(s).remaining();
  }

  /**
   * Returns an upper bound of the serialized size.
   * For variable-length {@link Charset}s,
   * an upper bound can possibly be obtained without encoding.
   * This method usually is much more efficient than
   * {@link #getSerializedSize(String)}.
   *
   * @return an upper bound of {@link #getSerializedSize(String)}.
   */
  int getSerializedSizeUpperBound(String s) {
    return getSerializedSize(s);
  }

  private <E extends Exception> CheckedFunction<ByteBuffer, Integer, E> encode(
      String string, int serializedSize, Function<String, E> newE) {
    return buffer -> {
      final CoderResult result = encoder.encode(
          CharBuffer.wrap(string), buffer, true);
      if (result.isError()) {
        throw newE.apply("Failed to encode with " + charset + ": " + result
            + ", string=" + string);
      }
      final int remaining = buffer.flip().remaining();
      if (remaining != serializedSize) {
        throw newE.apply("Size mismatched: Expected size is " + serializedSize
            + " but actual size is " + remaining + ", string=" + string);
      }
      return serializedSize;
    };
  }

  String decode(ByteBuffer buffer) {
    Runnable error = null;
    try {
      return decoder.decode(buffer).toString();
    } catch (CharacterCodingException e) {
      error = () -> LOG.warn("Failed to decode buffer with " + charset
          + ", buffer = (hex) " + StringUtils.bytes2Hex(buffer, 10), e);

      // For compatibility, try decoding using StringUtils.
      final String decoded = StringUtils.bytes2String(buffer, charset);
      // Decoded successfully, update error message.
      error = () -> LOG.warn("Failed to decode buffer with " + charset
          + ", buffer = (hex) " + StringUtils.bytes2Hex(buffer, 10)
          + ". Successfully decoded to " + decoded + " after retry.", e);
      return decoded;
    } finally {
      if (error != null) {
        error.run();
      }
    }
  }

  /** Encode the given {@link String} to a byte array. */
  <E extends Exception> byte[] string2Bytes(String string,
      Function<String, E> newE) throws E {
    final int size = getSerializedSize(string);
    final byte[] array = new byte[size];
    encode(string, size, newE).apply(ByteBuffer.wrap(array));
    return array;
  }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull String object,
      IntFunction<CodecBuffer> allocator) throws IOException {
    // allocate a larger buffer to avoid encoding twice.
    final int size = getSerializedSizeUpperBound(object);
    final CodecBuffer buffer = allocator.apply(size);
    buffer.putFromSource(encode(object, size, IOException::new));
    return buffer;
  }

  @Override
  public String fromCodecBuffer(@Nonnull CodecBuffer buffer)
      throws IOException {
    return decode(buffer.asReadOnlyByteBuffer());
  }

  @Override
  public byte[] toPersistedFormat(String object) throws IOException {
    return string2Bytes(object, IOException::new);
  }

  @Override
  public String fromPersistedFormat(byte[] bytes) {
    return decode(ByteBuffer.wrap(bytes));
  }

  @Override
  public String copyObject(String object) {
    return object;
  }
}
