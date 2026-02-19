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

package org.apache.hadoop.hdds;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.util.Preconditions;

/**
 * Simple utility class to collection string conversion methods.
 */
public final class StringUtils {
  private static final Charset UTF8 = StandardCharsets.UTF_8;

  private StringUtils() {
  }

  /**
   * Decode a specific range of bytes of the given byte array to a string
   * using UTF8.
   *
   * @param bytes  The bytes to be decoded into characters
   * @param offset The index of the first byte to decode
   * @param length The number of bytes to decode
   * @return The decoded string
   */
  public static String bytes2String(byte[] bytes, int offset, int length) {
    return new String(bytes, offset, length, UTF8);
  }

  public static String bytes2String(ByteBuffer bytes) {
    return bytes2String(bytes, UTF8);
  }

  public static String bytes2String(ByteBuffer bytes, Charset charset) {
    return Unpooled.wrappedBuffer(bytes.asReadOnlyBuffer()).toString(charset);
  }

  public static String bytes2Hex(ByteBuffer buffer, int max) {
    Preconditions.assertTrue(max > 0, () -> "max = " + max + " <= 0");
    buffer = buffer.asReadOnlyBuffer();
    final int remaining = buffer.remaining();
    final boolean overflow = max < remaining;
    final int n = overflow ? max : remaining;
    final StringBuilder builder = new StringBuilder(3 * n + (overflow ? 3 : 0));
    if (n > 0) {
      for (int i = 0; i < n; i++) {
        builder.append(String.format("%02X ", buffer.get()));
      }
      builder.setLength(builder.length() - 1);
    }
    if (overflow) {
      builder.append("...");
    }
    return builder.toString();
  }

  public static String bytes2Hex(ByteBuffer buffer) {
    return bytes2Hex(buffer, buffer.remaining());
  }

  public static String bytes2Hex(byte[] array) {
    return bytes2Hex(ByteBuffer.wrap(array));
  }

  /**
   * Decode a specific range of bytes of the given byte array to a string
   * using UTF8.
   *
   * @param bytes The bytes to be decoded into characters
   * @return The decoded string
   */
  public static String bytes2String(byte[] bytes) {
    return bytes2String(bytes, 0, bytes.length);
  }

  /**
   * Converts a string to a byte array using UTF8 encoding.
   */
  public static byte[] string2Bytes(String str) {
    return str.getBytes(UTF8);
  }

  public static String getLexicographicallyLowerString(String val) {
    if (val == null || val.isEmpty()) {
      throw new IllegalArgumentException("Input string must not be null or empty");
    }
    char[] charVal = val.toCharArray();
    int lastIdx = charVal.length - 1;
    if (charVal[lastIdx] == Character.MIN_VALUE) {
      throw new IllegalArgumentException("Cannot decrement character below Character.MIN_VALUE");
    }
    charVal[lastIdx] -= 1;
    return String.valueOf(charVal);
  }

  public static String getLexicographicallyHigherString(String val) {
    if (val == null || val.isEmpty()) {
      throw new IllegalArgumentException("Input string must not be null or empty");
    }
    char[] charVal = val.toCharArray();
    int lastIdx = charVal.length - 1;
    if (charVal[lastIdx] == Character.MAX_VALUE) {
      throw new IllegalArgumentException("Cannot increment character above Character.MAX_VALUE");
    }
    charVal[lastIdx] += 1;
    return String.valueOf(charVal);
  }

  public static String getFirstNChars(String str, int n) {
    if (str == null || str.length() < n) {
      return str;
    }
    return str.substring(0, n);
  }
}
