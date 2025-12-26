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

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * An index for answering "does this point fall within any of these ranges?" efficiently.
 *
 * <p>The indexed ranges are <em>half-open</em> intervals of the form
 * {@code [startInclusive, endExclusive)}.
 *
 * <p><strong>Core idea (sweep-line / prefix-sum over range boundaries):</strong>
 * Instead of scanning every range on each query, this index stores a sorted map from
 * boundary points to a running count of "active" ranges at that point.
 *
 * <ul>
 *   <li>For each range {@code [s, e)}, we add a delta {@code +1} at {@code s} and a delta
 *       {@code -1} at {@code e}.</li>
 *   <li>We then convert the deltas into a prefix sum in key order, so every boundary key
 *       stores the number of ranges active at that coordinate.</li>
 *   <li>For any query point {@code k}, the active count is {@code floorEntry(k).value}.
 *       If it is {@code > 0}, then {@code k} intersects at least one range.</li>
 * </ul>
 *
 * <p><strong>Update model:</strong> this index supports only removing ranges that were part of the
 * initial set. Removal updates the prefix sums for keys in {@code [startInclusive, endExclusive)}
 * (net effect of removing {@code +1} at start and {@code -1} at end).
 *
 * <p><strong>Complexities:</strong>
 * <ul>
 *   <li>Build: {@code O(R log B)} where {@code R} is #ranges and {@code B} is #distinct boundaries.</li>
 *   <li>{@link #containsIntersectingRange(Object)}: {@code O(log B)}.</li>
 *   <li>{@link #removeRange(Range)}: {@code O(log B + K)} where {@code K} is #boundaries in the range.</li>
 * </ul>
 *
 * @param <T> boundary type (must be {@link Comparable} to be stored in a {@link TreeMap})
 */
class RangeQueryIndex<T extends Comparable<T>> {

  private final TreeMap<T, Integer> rangeCountIndexMap;
  private final Set<Range<T>> ranges;

  RangeQueryIndex(Set<Range<T>> ranges) {
    this.rangeCountIndexMap = new TreeMap<>();
    this.ranges = ranges;
    init();
  }

  private void init() {
    // Phase 1: store boundary deltas (+1 at start, -1 at end).
    for (Range<T> range : ranges) {
      rangeCountIndexMap.compute(range.startInclusive, (k, v) -> v == null ? 1 : v + 1);
      rangeCountIndexMap.compute(range.endExclusive, (k, v) -> v == null ? -1 : v - 1);
    }

    // Phase 2: convert deltas to prefix sums so each key holds the active range count at that coordinate.
    int totalCount = 0;
    for (Map.Entry<T, Integer> entry : rangeCountIndexMap.entrySet()) {
      totalCount += entry.getValue();
      entry.setValue(totalCount);
    }
  }

  /**
   * Remove a range from the index.
   *
   * <p>This method assumes the range set is "popped" over time (ranges are removed but not added).
   * Internally, removing {@code [s, e)} decreases the active count by 1 for all boundary keys in
   * {@code [s, e)} and leaves counts outside the range unchanged.
   *
   * @throws IOException if the given {@code range} is not part of the indexed set
   */
  void removeRange(Range<T> range) throws IOException {
    if (!ranges.contains(range)) {
      throw new IOException(String.format("Range %s not found in index structure : %s", range, ranges));
    }
    ranges.remove(range);
    for (Map.Entry<T, Integer> entry : rangeCountIndexMap.subMap(range.startInclusive, true,
        range.endExclusive, false).entrySet()) {
      entry.setValue(entry.getValue() - 1);
    }
  }

  /**
   * @return true iff {@code key} is contained in at least one indexed range.
   *
   * <p>Implementation detail: uses {@link TreeMap#floorEntry(Object)} to find the last boundary
   * at or before {@code key}, and checks the prefix-summed active count at that point.</p>
   */
  boolean containsIntersectingRange(T key) {
    Map.Entry<T, Integer> countEntry = rangeCountIndexMap.floorEntry(key);
    if (countEntry == null) {
      return false;
    }
    return countEntry.getValue() > 0;
  }

  /**
   * Returns an intersecting range containing {@code key}, if any.
   *
   * <p>This method first checks {@link #containsIntersectingRange(Comparable)} using the index;
   * if the count indicates an intersection exists, it then scans the backing {@link #ranges}
   * set to find a concrete {@link Range} that contains {@code key}.</p>
   *
   * <p>Note that because {@link #ranges} is a {@link Set}, "first" refers to whatever iteration
   * order that set provides (it is not guaranteed to be deterministic unless the provided set is).</p>
   *
   * @return a containing range, or null if none intersect
   */
  Range<T> getFirstIntersectingRange(T key) {
    Map.Entry<T, Integer> countEntry = rangeCountIndexMap.floorEntry(key);
    if (countEntry == null) {
      return null;
    }
    for (Range<T> range : ranges) {
      if (range.contains(key)) {
        return range;
      }
    }
    return null;
  }

  /**
   * A half-open interval {@code [startInclusive, endExclusive)}.
   *
   * <p>For a value {@code k} to be contained, it must satisfy:
   * {@code startInclusive <= k < endExclusive} (according to {@link Comparable#compareTo(Object)}).</p>
   */
  static final class Range<T extends Comparable<T>> {
    private final T startInclusive;
    private final T endExclusive;

    Range(T startInclusive, T endExclusive) {
      this.startInclusive = Objects.requireNonNull(startInclusive, "start == null");
      this.endExclusive = Objects.requireNonNull(endExclusive, "end == null");
    }

    @Override
    public boolean equals(Object o) {
      return this == o;
    }

    @Override
    public int hashCode() {
      return Objects.hash(startInclusive, endExclusive);
    }

    T getStartInclusive() {
      return startInclusive;
    }

    T getEndExclusive() {
      return endExclusive;
    }

    /**
     * @return true iff {@code key} is within {@code [startInclusive, endExclusive)}.
     */
    public boolean contains(T key) {
      return startInclusive.compareTo(key) <= 0 && key.compareTo(endExclusive) < 0;
    }

    @Override
    public String toString() {
      return "Range{" +
          "startInclusive=" + startInclusive +
          ", endExclusive=" + endExclusive +
          '}';
    }
  }
}
