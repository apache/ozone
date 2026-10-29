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

package org.apache.hadoop.ozone.s3.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test class to test RangeHeaderParserUtil.
 */
public class TestRangeHeaderParserUtil {

  @Test
  public void testRangeHeaderParser() {

    RangeHeader rangeHeader;


    //range is with in file length
    rangeHeader = RangeHeaderParserUtil.parseRangeHeader("bytes=0-8", 10);
    assertEquals(0, rangeHeader.getStartOffset());
    assertEquals(8, rangeHeader.getEndOffset());
    assertFalse(rangeHeader.isReadFull());
    assertFalse(rangeHeader.isInValidRange());

    //range is with in file length, both start and end offset are same
    rangeHeader = RangeHeaderParserUtil.parseRangeHeader("bytes=0-0", 10);
    assertEquals(0, rangeHeader.getStartOffset());
    assertEquals(0, rangeHeader.getEndOffset());
    assertFalse(rangeHeader.isReadFull());
    assertFalse(rangeHeader.isInValidRange());

    //range is not with in file length, both start and end offset are greater
    // than length
    rangeHeader = RangeHeaderParserUtil.parseRangeHeader("bytes=11-10", 10);
    assertTrue(rangeHeader.isInValidRange());

    // range is satisfying, one of the range is with in the length. So, read
    // full file
    rangeHeader = RangeHeaderParserUtil.parseRangeHeader("bytes=11-8", 10);
    assertEquals(0, rangeHeader.getStartOffset());
    assertEquals(9, rangeHeader.getEndOffset());
    assertTrue(rangeHeader.isReadFull());
    assertFalse(rangeHeader.isInValidRange());

    // inverted range, start is still inside the object
    rangeHeader = RangeHeaderParserUtil.parseRangeHeader("bytes=8-3", 10);
    assertEquals(0, rangeHeader.getStartOffset());
    assertEquals(9, rangeHeader.getEndOffset());
    assertTrue(rangeHeader.isReadFull());
    assertFalse(rangeHeader.isInValidRange());

    // bytes spec is wrong
    rangeHeader = RangeHeaderParserUtil.parseRangeHeader("mb=11-8", 10);
    assertEquals(0, rangeHeader.getStartOffset());
    assertEquals(9, rangeHeader.getEndOffset());
    assertTrue(rangeHeader.isReadFull());
    assertFalse(rangeHeader.isInValidRange());

    // range specified is invalid
    rangeHeader = RangeHeaderParserUtil.parseRangeHeader("bytes=-11-8", 10);
    assertEquals(0, rangeHeader.getStartOffset());
    assertEquals(9, rangeHeader.getEndOffset());
    assertTrue(rangeHeader.isReadFull());
    assertFalse(rangeHeader.isInValidRange());

    //Last n bytes
    rangeHeader = RangeHeaderParserUtil.parseRangeHeader("bytes=-6", 10);
    assertEquals(4, rangeHeader.getStartOffset());
    assertEquals(9, rangeHeader.getEndOffset());
    assertFalse(rangeHeader.isReadFull());
    assertFalse(rangeHeader.isInValidRange());

    rangeHeader = RangeHeaderParserUtil.parseRangeHeader("bytes=-106", 10);
    assertEquals(0, rangeHeader.getStartOffset());
    assertEquals(9, rangeHeader.getEndOffset());
    assertFalse(rangeHeader.isInValidRange());

    rangeHeader = RangeHeaderParserUtil.parseRangeHeader("bytes=3977248768" +
            "-4977248768", 4977248769L);
    assertEquals(3977248768L, rangeHeader.getStartOffset());
    assertEquals(4977248768L, rangeHeader.getEndOffset());
    assertFalse(rangeHeader.isInValidRange());

  }

  @Test
  public void testUnsatisfiableRanges() {
    // start is past the last byte and no end is given
    assertTrue(RangeHeaderParserUtil.parseRangeHeader("bytes=10-", 10).isInValidRange());
    assertTrue(RangeHeaderParserUtil.parseRangeHeader("bytes=11-", 10).isInValidRange());

    // zero suffix length selects no bytes
    assertTrue(RangeHeaderParserUtil.parseRangeHeader("bytes=-0", 10).isInValidRange());

    // an empty object has no bytes to select
    assertTrue(RangeHeaderParserUtil.parseRangeHeader("bytes=-5", 0).isInValidRange());
    assertTrue(RangeHeaderParserUtil.parseRangeHeader("bytes=0-", 0).isInValidRange());
    assertTrue(RangeHeaderParserUtil.parseRangeHeader("bytes=0-5", 0).isInValidRange());

    // the last byte is still satisfiable
    RangeHeader rangeHeader = RangeHeaderParserUtil.parseRangeHeader("bytes=9-", 10);
    assertEquals(9, rangeHeader.getStartOffset());
    assertEquals(9, rangeHeader.getEndOffset());
    assertFalse(rangeHeader.isReadFull());
    assertFalse(rangeHeader.isInValidRange());
  }

  @Test
  public void testRangeWithoutStartAndEndIsIgnored() {
    RangeHeader rangeHeader = RangeHeaderParserUtil.parseRangeHeader("bytes=-", 10);
    assertEquals(0, rangeHeader.getStartOffset());
    assertEquals(9, rangeHeader.getEndOffset());
    assertTrue(rangeHeader.isReadFull());
    assertFalse(rangeHeader.isInValidRange());
  }

  @Test
  public void testRangeWithLongMaxValue() {
    RangeHeader rangeHeader = RangeHeaderParserUtil.parseRangeHeader("bytes=0-" + Long.MAX_VALUE, 10);
    assertEquals(0, rangeHeader.getStartOffset());
    assertEquals(9, rangeHeader.getEndOffset());
    assertFalse(rangeHeader.isReadFull());
    assertFalse(rangeHeader.isInValidRange());
  }

  // 9223372036854775808 is Long.MAX_VALUE + 1
  @ParameterizedTest
  @ValueSource(strings = {
      "bytes=0-9223372036854775808",
      "bytes=0-99999999999999999999",
      "bytes=9223372036854775808-",
      "bytes=99999999999999999999-",
      "bytes=-9223372036854775808",
      "bytes=-99999999999999999999",
      "bytes=99999999999999999999-99999999999999999999"
  })
  public void testRangeOverflowingLongIsIgnored(String range) {
    RangeHeader rangeHeader = RangeHeaderParserUtil.parseRangeHeader(range, 10);
    assertEquals(0, rangeHeader.getStartOffset());
    assertEquals(9, rangeHeader.getEndOffset());
    assertTrue(rangeHeader.isReadFull());
    assertFalse(rangeHeader.isInValidRange());
  }

}
