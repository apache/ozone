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

package org.apache.hadoop.ozone.om.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.IntStream;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for {@link OmMultipartPartKey}.
 */
public class TestOmMultipartPartKey {

  private final Codec<OmMultipartPartKey> codec = OmMultipartPartKey.getCodec();

  /**
   * Generate a stream of part numbers with low byte separator as 2F,
   * in the supported MPU range [1, 10000], values with low byte 0x2f.
   * AND operation with 0xFF gives the low byte of the part number.
   * @return IntStream of part numbers with low byte separator as 2F.
   */
  private static IntStream partNumbersWithLowByteSeparator() {
    return IntStream.rangeClosed(1, 10000)
        .filter(partNumber -> (partNumber & 0xFF) == 0x2F);
  }

  @Test
  public void testRoundTripFullKey() throws Exception {
    OmMultipartPartKey key = OmMultipartPartKey.of("upload-123", 9);

    byte[] raw = codec.toPersistedFormat(key);
    OmMultipartPartKey decoded = codec.fromPersistedFormat(raw);

    assertEquals(key, decoded);
    assertTrue(decoded.hasPartNumber());
    assertEquals("upload-123/9", decoded.toString());
  }

  @Test
  public void testRoundTripPrefixKey() throws Exception {
    OmMultipartPartKey key = OmMultipartPartKey.prefix("upload-abc");

    byte[] raw = codec.toPersistedFormat(key);
    OmMultipartPartKey decoded = codec.fromPersistedFormat(raw);

    assertEquals(key, decoded);
    assertFalse(decoded.hasPartNumber());
    assertEquals("upload-abc", decoded.toString());
  }

  // Test to validate that the full key is decoded correctly
  // when the part number has a low byte same as the separator byte i.e. 2F.
  @ParameterizedTest
  @MethodSource("partNumbersWithLowByteSeparator")
  public void testDecodeFullKeyWhenPartLowByteIsSeparator(int partNumber)
      throws Exception {
    OmMultipartPartKey key = OmMultipartPartKey.of("upload-sep", partNumber);

    byte[] raw = codec.toPersistedFormat(key);
    OmMultipartPartKey decoded = codec.fromPersistedFormat(raw);

    assertTrue(decoded.hasPartNumber());
    assertEquals(partNumber, decoded.getPartNumber().intValue());
    assertEquals("upload-sep", decoded.getUploadId());
  }

  @Test
  public void testDecodeRejectsInvalidKeyWithoutSeparator() {
    assertThrows(IllegalArgumentException.class,
        () -> codec.fromPersistedFormat("invalid".getBytes()));
  }

  @Test
  public void testDecodeRejectsEmptyKey() {
    assertThrows(IllegalArgumentException.class,
        () -> codec.fromPersistedFormat(new byte[0]));
  }
}
