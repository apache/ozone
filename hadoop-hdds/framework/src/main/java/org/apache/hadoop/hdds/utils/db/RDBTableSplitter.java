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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.rocksdb.LiveFileMetaData;

/**
 * Computes split boundaries for a parallel table scan up front, from the SST
 * file metadata of a column family. No per-element coordination is needed:
 * each returned {@link KeyRange} is disjoint, and together they cover the
 * requested range exactly.
 *
 * <p>Boundaries come from the SST files in the level with the largest total
 * overlapping byte size (ties broken by deeper level). Using {@code smallestKey}
 * of SSTs {@code 1..N-1} partitions {@code N} non-overlapping SSTs into {@code N}
 * SST-aligned ranges without creating empty or single-key gap splits. When all
 * files share the same {@code smallestKey} (e.g. overlapping L0 flushes),
 * {@code largestKey}s are used as fallback.
 */
public final class RDBTableSplitter {
  private RDBTableSplitter() {
  }

  /** Simplified SST metadata, to decouple the split logic from RocksJava. */
  static final class SstInfo {
    private final String columnFamily;
    private final int level;
    private final byte[] smallestKey;
    private final byte[] largestKey;
    private final long size;

    SstInfo(String columnFamily, int level, byte[] smallestKey,
        byte[] largestKey) {
      this(columnFamily, level, smallestKey, largestKey, 1L);
    }

    SstInfo(String columnFamily, int level, byte[] smallestKey,
        byte[] largestKey, long size) {
      this.columnFamily = columnFamily;
      this.level = level;
      this.smallestKey = smallestKey;
      this.largestKey = largestKey;
      this.size = Math.max(0L, size);
    }

    static SstInfo of(LiveFileMetaData f) {
      return new SstInfo(
          new String(f.columnFamilyName(), StandardCharsets.UTF_8),
          f.level(), f.smallestKey(), f.largestKey(), f.size());
    }

    boolean overlaps(KeyRange range) {
      final byte[] start = range.getStart();
      final byte[] end = range.getEnd();
      return (end == null || KeyRange.compare(smallestKey, end) < 0)
          && (start == null || KeyRange.compare(largestKey, start) >= 0);
    }
  }

  /**
   * @param files live SST files of the DB (all column families).
   * @param columnFamily the column family name to split.
   * @param range the range to split, e.g. the prefix range.
   * @param maxSplits the maximum number of ranges to return, at least 1.
   */
  public static List<KeyRange> split(List<LiveFileMetaData> files,
      String columnFamily, KeyRange range, int maxSplits) {
    final List<SstInfo> infos = new ArrayList<>(files.size());
    for (LiveFileMetaData f : files) {
      infos.add(SstInfo.of(f));
    }
    return splitInfos(infos, columnFamily, range, maxSplits);
  }

  static List<KeyRange> splitInfos(List<SstInfo> files, String columnFamily,
      KeyRange range, int maxSplits) {
    if (maxSplits <= 1) {
      return Collections.singletonList(range);
    }
    final int level = selectLevel(files, columnFamily, range);
    if (level < 0) {
      return Collections.singletonList(range);
    }
    final List<SstInfo> chosenFiles = new ArrayList<>();
    for (SstInfo f : files) {
      if (f.level == level && f.columnFamily.equals(columnFamily)
          && f.overlaps(range)) {
        chosenFiles.add(f);
      }
    }
    final TreeSet<byte[]> boundaries = extractBoundaries(chosenFiles, range);
    final List<byte[]> chosen = pickEvenly(new ArrayList<>(boundaries),
        maxSplits - 1);
    final List<KeyRange> ranges = new ArrayList<>(chosen.size() + 1);
    byte[] start = range.getStart();
    for (byte[] b : chosen) {
      ranges.add(new KeyRange(start, b));
      start = b;
    }
    ranges.add(new KeyRange(start, range.getEnd()));
    return ranges;
  }

  private static int selectLevel(List<SstInfo> files, String columnFamily,
      KeyRange range) {
    final Map<Integer, Long> bytesByLevel = new HashMap<>();
    for (SstInfo f : files) {
      if (f.columnFamily.equals(columnFamily) && f.overlaps(range)) {
        bytesByLevel.put(f.level,
            bytesByLevel.getOrDefault(f.level, 0L) + f.size);
      }
    }
    int bestLevel = -1;
    long bestBytes = -1L;
    for (Map.Entry<Integer, Long> e : bytesByLevel.entrySet()) {
      final int lvl = e.getKey();
      final long bytes = e.getValue();
      if (bytes > bestBytes || (bytes == bestBytes && lvl > bestLevel)) {
        bestLevel = lvl;
        bestBytes = bytes;
      }
    }
    return bestLevel;
  }

  private static TreeSet<byte[]> extractBoundaries(List<SstInfo> levelFiles,
      KeyRange range) {
    final TreeSet<byte[]> boundaries = new TreeSet<>(KeyRange.COMPARATOR);
    if (levelFiles.size() <= 1) {
      return boundaries;
    }
    final TreeSet<byte[]> smallest = new TreeSet<>(KeyRange.COMPARATOR);
    for (SstInfo f : levelFiles) {
      if (f.smallestKey != null) {
        smallest.add(f.smallestKey);
      }
    }
    // Drop the minimum smallestKey so split 0 covers the first SST instead of
    // creating an empty [range.start, first.smallestKey) split.
    smallest.pollFirst();
    for (byte[] k : smallest) {
      addIfInside(boundaries, k, range);
    }
    if (boundaries.isEmpty()) {
      final TreeSet<byte[]> largest = new TreeSet<>(KeyRange.COMPARATOR);
      for (SstInfo f : levelFiles) {
        if (f.largestKey != null) {
          largest.add(f.largestKey);
        }
      }
      largest.pollLast();
      for (byte[] k : largest) {
        addIfInside(boundaries, k, range);
      }
    }
    return boundaries;
  }

  private static void addIfInside(TreeSet<byte[]> set, byte[] key,
      KeyRange range) {
    // A boundary equal to the range start or end would create an empty split.
    if (key != null && range.isStrictlyInside(key)) {
      set.add(Arrays.copyOf(key, key.length));
    }
  }

  private static List<byte[]> pickEvenly(List<byte[]> sorted, int n) {
    if (sorted.size() <= n) {
      return sorted;
    }
    final List<byte[]> picked = new ArrayList<>(n);
    for (int i = 1; i <= n; i++) {
      picked.add(sorted.get((int) ((long) i * sorted.size() / (n + 1))));
    }
    return picked;
  }
}
