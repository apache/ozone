/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import java.security.SecureRandom;
import java.util.UUID;

/**
 * Helper methods to deal with random UUIDs.
 */
public final class UUIDUtil {

  private UUIDUtil() {

  }

  private static final ThreadLocal<SecureRandom> GENERATOR =
      ThreadLocal.withInitial(SecureRandom::new);

  public static UUID randomUUID() {
    long r1;
    long r2;
    byte[] uuidBytes = randomUUIDBytes();
    r1 = toLong(uuidBytes, 0);
    r2 = toLong(uuidBytes, 1);

    // first, ensure type is ok
    r1 &= ~0xF000L; // remove high nibble of 6th byte
    r1 |= (long) (4 << 12);
    // second, ensure variant is properly set too
    // (8th byte; most-sig byte of second long)
    r2 = ((r2 << 2) >>> 2); // remove 2 MSB
    r2 |= (2L << 62); // set 2 MSB to '10'
    return new UUID(r1, r2);
  }

  public static byte[] randomUUIDBytes() {
    final byte[] bytes = new byte[16];
    GENERATOR.get().nextBytes(bytes);
    // See RFC 4122 section 4.4
    bytes[6]  &= 0x0f;
    bytes[6]  |= 0x40;
    bytes[8]  &= 0x3f;
    bytes[8]  |= 0x80;
    return bytes;
  }

  private static long toLong(byte[] buffer, int offset) {
    long l1 = toInt(buffer, offset);
    long l2 = toInt(buffer, offset + 4);
    long l = (l1 << 32) + ((l2 << 32) >>> 32);
    return l;
  }

  private static long toInt(byte[] buffer, int offset) {
    return (buffer[offset] << 24)
        + ((buffer[++offset] & 0xFF) << 16)
        + ((buffer[++offset] & 0xFF) << 8)
        + (buffer[++offset] & 0xFF);
  }

}
