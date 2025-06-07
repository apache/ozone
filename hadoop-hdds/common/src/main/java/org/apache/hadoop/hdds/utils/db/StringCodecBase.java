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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Objects;
import java.util.function.Function;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract {@link Codec} to serialize/deserialize {@link String}
 * using a {@link Charset} provided by subclasses.
 */
abstract class StringCodecBase implements Codec<String> {
  static final Logger LOG = LoggerFactory.getLogger(StringCodecBase.class);

  private final Charset charset;
  private final boolean fixedLength;
  private final int maxBytesPerChar;

  StringCodecBase(Charset charset) {
    this.charset = charset;

    final CharsetEncoder encoder = charset.newEncoder();
    final float max = encoder.maxBytesPerChar();
    this.maxBytesPerChar = (int) max;
    if (maxBytesPerChar != max) {
      throw new ArithmeticException("Round off error in " + charset
          + ": maxBytesPerChar = " + max + " is not an integer.");
    }
    this.fixedLength = max == encoder.averageBytesPerChar();
  }

  @Override
  public final Class<String> getTypeClass() {
    return String.class;
  }

  CharsetEncoder newEncoder() {
    return charset.newEncoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);
  }

  CharsetDecoder newDecoder() {
    return charset.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);
  }

  /**
   * Is this a fixed-length {@link Codec}?
   * <p>
   * For a fixed-length {@link Codec},
   * each character is encoded to the same number of bytes and
   * {@link #getSerializedSizeUpperBound(String)} equals to the serialized size.
   */
  public boolean isFixedLength() {
    return fixedLength;
  }

  /**
   * @return an upper bound, which can be obtained without encoding,
   *         of the serialized size of the given {@link String}.
   *         When {@link #isFixedLength()} is true,
   *         the upper bound equals to the serialized size.
   */
  private int getSerializedSizeUpperBound(String s) {
    return maxBytesPerChar * s.length();
  }

  private <E extends Exception> PutToByteBuffer<E> encode(
      String string, Integer serializedSize, Function<String, E> newE) {
    return buffer -> {
      final CoderResult result = newEncoder().encode(
          CharBuffer.wrap(string), buffer, true);
      if (result.isError()) {
        throw newE.apply("Failed to encode with " + charset + ": " + result
            + ", string=" + string);
      }
      final int remaining = buffer.flip().remaining();
      if (serializedSize != null && serializedSize != remaining) {
        throw newE.apply("Size mismatched: Expected size is " + serializedSize
            + " but actual size is " + remaining + ", string=" + string);
      }
      return remaining;
    };
  }

  String decode(ByteBuffer buffer) {
    Runnable error = null;
    try {
      return newDecoder().decode(buffer.asReadOnlyBuffer()).toString();
    } catch (Exception e) {
      error = () -> LOG.warn("Failed to decode buffer with " + charset
          + ", buffer = (hex) " + StringUtils.bytes2Hex(buffer), e);

      // For compatibility, try decoding using StringUtils.
      final String decoded = StringUtils.bytes2String(buffer, charset);
      // Decoded successfully, update error message.
      error = () -> LOG.warn("Decode (hex) " + StringUtils.bytes2Hex(buffer, 20)
          + "\n  Attempt failed : " + charset + " (see exception below)"
          + "\n  Retry succeeded: decoded to " + decoded, e);
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
    final int upperBound = getSerializedSizeUpperBound(string);
    final Integer serializedSize = isFixedLength() ? upperBound : null;
    final PutToByteBuffer<E> encoder = encode(string, serializedSize, newE);

    if (serializedSize != null) {
      // When the serialized size is known, create an array
      // and then wrap it as a buffer for encoding.
      final byte[] array = new byte[serializedSize];
      final Integer encoded = encoder.apply(ByteBuffer.wrap(array));
      Objects.requireNonNull(encoded, "encoded == null");
      Preconditions.assertSame(serializedSize.intValue(), encoded.intValue(),
          "serializedSize");
      return array;
    } else {
      // When the serialized size is unknown, allocate a larger buffer
      // and then get an array.
      try (CodecBuffer buffer = CodecBuffer.allocateHeap(upperBound)) {
        buffer.putFromSource(encoder);

        // copy the buffer to an array in order to release the buffer.
        return buffer.getArray();
      }
    }
  }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull String object, CodecBuffer.Allocator allocator) throws CodecException {
    // allocate a larger buffer to avoid encoding twice.
    final int upperBound = getSerializedSizeUpperBound(object);
    final CodecBuffer buffer = allocator.apply(upperBound);
    buffer.putFromSource(encode(object, null, CodecException::new));
    return buffer;
  }

  @Override
  public String fromCodecBuffer(@Nonnull CodecBuffer buffer) {
    return decode(buffer.asReadOnlyByteBuffer());
  }

  @Override
  public byte[] toPersistedFormat(String object) throws CodecException {
    return string2Bytes(object, CodecException::new);
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
