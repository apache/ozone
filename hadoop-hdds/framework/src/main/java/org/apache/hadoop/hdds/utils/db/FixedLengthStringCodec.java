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

import java.nio.charset.StandardCharsets;

/**
 * A {@link Codec} to serialize/deserialize {@link String}
 * using {@link StandardCharsets#ISO_8859_1},
 * a fixed-length one-byte-per-character encoding,
 * i.e. the serialized size equals to {@link String#length()}.
 */
public final class FixedLengthStringCodec extends StringCodecBase {

  private static final FixedLengthStringCodec INSTANCE
      = new FixedLengthStringCodec();

  public static FixedLengthStringCodec get() {
    return INSTANCE;
  }

  private FixedLengthStringCodec() {
    // singleton
    super(StandardCharsets.ISO_8859_1);
  }

  /**
   * Encode the given {@link String} to a byte array.
   * @throws IllegalStateException in case an encoding error occurs.
   */
  public static byte[] string2Bytes(String string) {
    return get().string2Bytes(string, IllegalStateException::new);
  }

  /**
   * Decode the given byte array to a {@link String}.
   */
  public static String bytes2String(byte[] bytes) {
    return get().fromPersistedFormat(bytes);
  }
}
