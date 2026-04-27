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

package org.apache.hadoop.ozone.client.checksum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Test {@link CrcUtil}. */
@Timeout(10)
public class TestCrcUtil {
  /** Assert that the given throwable contains the given message. */
  public static void assertContains(Throwable t, String message) {
    assertTrue(t.getMessage().contains(message), () -> "Message \"" + message + "\" not found: " + t);
  }

  @Test
  public void testIntSerialization() {
    byte[] bytes = CrcUtil.intToBytes(0xCAFEBEEF);
    assertEquals(0xCAFEBEEF, CrcUtil.readInt(bytes, 0));

    bytes = new byte[8];
    CrcUtil.writeInt(bytes, 0, 0xCAFEBEEF);
    assertEquals(0xCAFEBEEF, CrcUtil.readInt(bytes, 0));
    CrcUtil.writeInt(bytes, 4, 0xABCDABCD);
    assertEquals(0xABCDABCD, CrcUtil.readInt(bytes, 4));

    // Assert big-endian format for general Java consistency.
    assertEquals(0xBEEFABCD, CrcUtil.readInt(bytes, 2));
  }

  @Test
  public void testToSingleCrcStringBadLength() {
    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> CrcUtil.toSingleCrcString(new byte[8]));
    assertContains(e, "length");
  }

  @Test
  public void testToSingleCrcString() {
    byte[] buf = CrcUtil.intToBytes(0xcafebeef);
    assertEquals("0xcafebeef", CrcUtil.toSingleCrcString(buf));
  }

  @Test
  public void testToMultiCrcStringBadLength() {
    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> CrcUtil.toMultiCrcString(new byte[6]));
    assertContains(e, "length");
  }

  @Test
  public void testToMultiCrcStringMultipleElements() {
    byte[] buf = new byte[12];
    CrcUtil.writeInt(buf, 0, 0xcafebeef);
    CrcUtil.writeInt(buf, 4, 0xababcccc);
    CrcUtil.writeInt(buf, 8, 0xddddefef);
    assertEquals("[0xcafebeef, 0xababcccc, 0xddddefef]", CrcUtil.toMultiCrcString(buf));
  }

  @Test
  public void testToMultiCrcStringSingleElement() {
    byte[] buf = new byte[4];
    CrcUtil.writeInt(buf, 0, 0xcafebeef);
    assertEquals("[0xcafebeef]", CrcUtil.toMultiCrcString(buf));
  }

  @Test
  public void testToMultiCrcStringNoElements() {
    assertEquals("[]", CrcUtil.toMultiCrcString(new byte[0]));
  }
}
