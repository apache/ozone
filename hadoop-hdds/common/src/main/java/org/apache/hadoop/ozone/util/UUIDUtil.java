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

package org.apache.hadoop.ozone.util;

import java.security.SecureRandom;

/**
 * Helper methods to deal with random UUIDs.
 */
public final class UUIDUtil {
  private static final ThreadLocal<SecureRandom> GENERATOR = ThreadLocal.withInitial(SecureRandom::new);

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

  private UUIDUtil() {
  }
}
