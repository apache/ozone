/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

import com.google.common.base.Preconditions;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;

/**
 * Simple utility class to collection string conversion methods.
 */
public final class StringUtils {

  private StringUtils() {
  }

  private static final Charset UTF8 = StandardCharsets.UTF_8;

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
    buffer = buffer.asReadOnlyBuffer();
    final int remaining = buffer.remaining();
    final int n = Math.min(max, remaining);
    final StringBuilder builder = new StringBuilder(3 * n);
    for (int i = 0; i < n; i++) {
      builder.append(String.format("%02X ", buffer.get()));
    }
    return builder + (remaining > max ? "..." : "");
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

  public static String appendIfNotPresent(String str, char c) {
    Preconditions.checkNotNull(str, "Input string is null");
    return str.isEmpty() || str.charAt(str.length() - 1) != c ? str + c : str;
  }
}
