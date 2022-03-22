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
package org.apache.hadoop.hdds.utils.db;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

/**
 * String utility class for conversion between byte[] and string
 * which requires string to be of non-variable-length encoding(e.g. ASCII).
 * This is different from StringUtils which uses UTF-8 encoding which is
 * a variable-length encoding style.
 * This is mainly for PrefixedStringCodec which requires a fixed-length
 * prefix.
 */
public final class FixedLengthStringUtils {

  private FixedLengthStringUtils() {
  }

  // An ASCII extension: https://en.wikipedia.org/wiki/ISO/IEC_8859-1
  // Each character is encoded as a single eight-bit code value.
  private static final String ASCII_CSN = StandardCharsets.ISO_8859_1.name();

  public static String bytes2String(byte[] bytes) {
    try {
      return new String(bytes, 0, bytes.length, ASCII_CSN);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(
          "ISO_8859_1 encoding is not supported", e);
    }
  }

  public static byte[] string2Bytes(String str) {
    try {
      return str.getBytes(ASCII_CSN);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(
          "ISO_8859_1 decoding is not supported", e);
    }
  }
}