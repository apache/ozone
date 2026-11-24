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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.RDBTableSplitter.SstInfo;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RDBTableSplitter}.
 */
public class TestRDBTableSplitter {
  private static final String CF = "cf";

  private static byte[] b(String s) {
    return s.getBytes(UTF_8);
  }

  private static SstInfo sst(int level, String smallest, String largest) {
    return new SstInfo(CF, level, b(smallest), b(largest));
  }

  private static SstInfo sst(int level, String smallest, String largest, long size) {
    return new SstInfo(CF, level, b(smallest), b(largest), size);
  }

  /** Ranges must be contiguous, non-empty, sorted and cover the input. */
  private static void assertCovers(KeyRange range, List<KeyRange> splits) {
    assertTrue(Arrays.equals(range.getStart(), splits.get(0).getStart()));
    assertTrue(Arrays.equals(range.getEnd(),
        splits.get(splits.size() - 1).getEnd()));
    for (int i = 0; i < splits.size(); i++) {
      final KeyRange s = splits.get(i);
      if (s.getStart() != null && s.getEnd() != null) {
        assertTrue(KeyRange.compare(s.getStart(), s.getEnd()) < 0,
            "empty split " + s);
      }
      if (i > 0) {
        assertTrue(Arrays.equals(splits.get(i - 1).getEnd(), s.getStart()),
            "gap/overlap at " + i + ": " + splits);
      }
    }
  }

  @Test
  public void testNoFilesGivesSingleRange() {
    final KeyRange all = new KeyRange(null, null);
    assertEquals(Collections.singletonList(all),
        RDBTableSplitter.splitInfos(Collections.emptyList(), CF, all, 8));
  }

  @Test
  public void testMaxSplitsOneGivesSingleRange() {
    final KeyRange all = new KeyRange(null, null);
    assertEquals(1, RDBTableSplitter.splitInfos(
        Arrays.asList(sst(1, "b", "c"), sst(1, "d", "e")), CF, all, 1).size());
  }

  @Test
  public void testPrefersLargestLevelAndAvoidsGapSplits() {
    final KeyRange all = new KeyRange(null, null);
    final List<KeyRange> splits = RDBTableSplitter.splitInfos(Arrays.asList(
        sst(0, "a", "z"),
        sst(2, "b", "d"), sst(2, "d", "f"), sst(2, "g", "k"),
        new SstInfo("other", 6, b("c"), b("x"), 1000L)), CF, all, 100);
    // 3 SSTs in level 2 -> interior smallestKey boundaries d, g -> 3 splits
    assertEquals(Arrays.asList(
        new KeyRange(null, b("d")),
        new KeyRange(b("d"), b("g")),
        new KeyRange(b("g"), null)), splits);
    assertCovers(all, splits);
  }

  @Test
  public void testSelectsLevelWithMostBytesOverTinyDeepestLevel() {
    final KeyRange all = new KeyRange(null, null);
    final List<KeyRange> splits = RDBTableSplitter.splitInfos(Arrays.asList(
        sst(6, "a", "b", 10L),
        sst(5, "a", "d", 100L), sst(5, "e", "h", 100L),
        sst(5, "i", "m", 100L), sst(5, "n", "z", 100L)), CF, all, 100);
    assertEquals(4, splits.size());
    assertCovers(all, splits);
  }

  @Test
  public void testOverlappingSameSmallestKeyFallsBackToLargestKeys() {
    final KeyRange all = new KeyRange(null, null);
    final List<KeyRange> splits = RDBTableSplitter.splitInfos(Arrays.asList(
        sst(0, "a", "d", 50L),
        sst(0, "a", "h", 50L),
        sst(0, "a", "z", 50L)), CF, all, 100);
    assertEquals(Arrays.asList(
        new KeyRange(null, b("d")),
        new KeyRange(b("d"), b("h")),
        new KeyRange(b("h"), null)), splits);
    assertCovers(all, splits);
  }

  @Test
  public void testClippedToRangeAndBoundaryEqualToEndpoint() {
    // Range start equals an SST smallest key, range end equals another SST
    // largest key: 2 overlapping SSTs produce 2 clean splits with no empty range.
    final KeyRange range = new KeyRange(b("d"), b("k"));
    final List<KeyRange> splits = RDBTableSplitter.splitInfos(Arrays.asList(
        sst(1, "a", "c"), sst(1, "d", "f"), sst(1, "g", "k"),
        sst(1, "m", "p")), CF, range, 100);
    assertCovers(range, splits);
    assertEquals(Arrays.asList(new KeyRange(b("d"), b("g")),
        new KeyRange(b("g"), b("k"))), splits);
  }

  @Test
  public void testLimitsNumberOfSplits() {
    final KeyRange all = new KeyRange(null, null);
    final List<KeyRange> splits = RDBTableSplitter.splitInfos(Arrays.asList(
        sst(3, "a", "b"), sst(3, "c", "d"), sst(3, "e", "f"),
        sst(3, "g", "h"), sst(3, "i", "j")), CF, all, 3);
    assertEquals(3, splits.size());
    assertCovers(all, splits);
  }

  @Test
  public void testNonOverlappingFilesIgnored() {
    final KeyRange range = new KeyRange(b("m"), b("n"));
    assertEquals(Collections.singletonList(range),
        RDBTableSplitter.splitInfos(Arrays.asList(sst(4, "a", "c"),
            sst(4, "x", "z")), CF, range, 8));
  }
}
