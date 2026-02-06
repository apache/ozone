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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.hadoop.hdds.utils.db.RangeQueryIndex.Range;
import org.junit.jupiter.api.Test;

/**
 * Test class for validating the behavior and functionality of the {@code RangeQueryIndex} class.
 *
 * <p>This class contains a collection of unit tests to ensure correct behavior of the range
 * indexing system under various scenarios, such as intersections, overlaps, boundary conditions,
 * and removal of range objects. The tests leverage the {@code Range} class for defining
 * half-open intervals and test different operations provided by the {@code RangeQueryIndex}.
 *
 * <p>The tested operations include:
 * - Checking for intersecting ranges.
 * - Retrieving the first intersecting range.
 * - Handling overlaps and nested ranges.
 * - Adjacency between ranges.
 * - Behaviors when handling duplicate ranges or ranges with identical bounds but different instances.
 * - Error conditions when attempting invalid removals of ranges.
 */
public class TestRangeQueryIndex {

  @Test
  public void testContainsIntersectingRangeHalfOpenBoundaries() {
    final Range<Integer> r1 = new Range<>(10, 20); // [10, 20)
    final Range<Integer> r2 = new Range<>(30, 40); // [30, 40)
    final Set<Range<Integer>> ranges = new LinkedHashSet<>();
    ranges.add(r1);
    ranges.add(r2);

    final RangeQueryIndex<Integer> index = new RangeQueryIndex<>(ranges);

    // Before first range
    assertFalse(index.containsIntersectingRange(0));
    assertFalse(index.containsIntersectingRange(9));

    // Start is inclusive
    assertTrue(index.containsIntersectingRange(10));
    assertTrue(index.containsIntersectingRange(19));

    // End is exclusive
    assertFalse(index.containsIntersectingRange(20));
    assertFalse(index.containsIntersectingRange(29));

    // Second range
    assertTrue(index.containsIntersectingRange(30));
    assertTrue(index.containsIntersectingRange(39));
    assertFalse(index.containsIntersectingRange(40));
    assertFalse(index.containsIntersectingRange(100));
  }

  @Test
  public void testGetFirstIntersectingRangeAndRemovalWithOverlaps() throws Exception {
    // Use LinkedHashSet to make iteration order deterministic for getFirstIntersectingRange().
    final Range<Integer> r2 = new Range<>(5, 15);  // overlaps with r1 for [5, 10)
    final Range<Integer> r1 = new Range<>(0, 10);
    final Set<Range<Integer>> ranges = new LinkedHashSet<>();
    ranges.add(r2);
    ranges.add(r1);

    final RangeQueryIndex<Integer> index = new RangeQueryIndex<>(ranges);

    assertTrue(index.containsIntersectingRange(7));
    final Range<Integer> first = index.getFirstIntersectingRange(7);
    assertNotNull(first);
    assertSame(r2, first, "should return the first containing range in set iteration order");

    index.removeRange(r2);
    assertTrue(index.containsIntersectingRange(7), "still intersecting due to remaining overlapping range");
    assertSame(r1, index.getFirstIntersectingRange(7));

    index.removeRange(r1);
    assertFalse(index.containsIntersectingRange(7));
    assertNull(index.getFirstIntersectingRange(7));
  }

  @Test
  public void testAdjacentRangesShareBoundary() {
    final Range<Integer> left = new Range<>(0, 10);   // [0, 10)
    final Range<Integer> right = new Range<>(10, 20); // [10, 20)
    final Set<Range<Integer>> ranges = new LinkedHashSet<>();
    ranges.add(left);
    ranges.add(right);
    final RangeQueryIndex<Integer> index = new RangeQueryIndex<>(ranges);

    // End is exclusive for left; start is inclusive for right.
    assertTrue(index.containsIntersectingRange(9));
    assertTrue(index.containsIntersectingRange(0));
    assertTrue(index.containsIntersectingRange(10));
    assertTrue(index.containsIntersectingRange(19));
    assertFalse(index.containsIntersectingRange(20));
  }

  @Test
  public void testMultipleOverlapsAndNestedRangesRemovalOrder() throws Exception {
    // rOuter covers everything; rMid overlaps partially; rInner is nested.
    final Range<Integer> rOuter = new Range<>(0, 100);  // [0, 100)
    final Range<Integer> rMid = new Range<>(20, 80);    // [20, 80)
    final Range<Integer> rInner = new Range<>(30, 40);  // [30, 40)
    final Set<Range<Integer>> ranges = new LinkedHashSet<>();
    ranges.add(rOuter);
    ranges.add(rMid);
    ranges.add(rInner);
    final RangeQueryIndex<Integer> index = new RangeQueryIndex<>(ranges);

    // Covered by outer only
    assertTrue(index.containsIntersectingRange(10));
    assertSame(rOuter, index.getFirstIntersectingRange(10));

    // Covered by all three
    assertTrue(index.containsIntersectingRange(35));
    assertSame(rOuter, index.getFirstIntersectingRange(35));

    // Remove the middle range first.
    index.removeRange(rMid);
    assertTrue(index.containsIntersectingRange(35), "still covered by outer + inner");

    // Remove the inner range next.
    index.removeRange(rInner);
    assertTrue(index.containsIntersectingRange(35), "still covered by outer");

    // Now remove the outer range; should become uncovered.
    index.removeRange(rOuter);
    assertFalse(index.containsIntersectingRange(35));
    assertNull(index.getFirstIntersectingRange(35));
  }

  @Test
  public void testDuplicateSameBoundsDifferentInstances() throws Exception {
    // Range.equals is identity-based, so two ranges with the same bounds can co-exist in the Set.
    final Range<Integer> r1 = new Range<>(0, 10);
    final Range<Integer> r2 = new Range<>(0, 10);
    final Set<Range<Integer>> ranges = new LinkedHashSet<>();
    ranges.add(r1);
    ranges.add(r2);

    final RangeQueryIndex<Integer> index = new RangeQueryIndex<>(ranges);
    assertTrue(index.containsIntersectingRange(5));

    // Remove one instance: should still intersect due to the other.
    index.removeRange(r1);
    assertTrue(index.containsIntersectingRange(5));

    // Remove the second instance: now it should not intersect.
    index.removeRange(r2);
    assertFalse(index.containsIntersectingRange(5));
  }

  @Test
  public void testRemoveSameInstanceTwiceThrows() throws Exception {
    final Range<Integer> r = new Range<>(0, 10);
    final Set<Range<Integer>> ranges = new LinkedHashSet<>();
    ranges.add(r);

    final RangeQueryIndex<Integer> index = new RangeQueryIndex<>(ranges);
    index.removeRange(r);
    assertThrows(IOException.class, () -> index.removeRange(r));
  }

  @Test
  public void testRemoveRangeNotFoundThrows() throws Exception {
    final Range<Integer> r1 = new Range<>(0, 10);
    final Set<Range<Integer>> ranges = new LinkedHashSet<>();
    ranges.add(r1);

    final RangeQueryIndex<Integer> index = new RangeQueryIndex<>(ranges);

    // Range.equals is identity-based, so a different object with same bounds is not "found".
    final Range<Integer> sameBoundsDifferentInstance = new Range<>(0, 10);
    assertThrows(IOException.class, () -> index.removeRange(sameBoundsDifferentInstance));

    // Removing the original instance works.
    index.removeRange(r1);
    assertFalse(index.containsIntersectingRange(0));
  }

  @Test
  public void testRemoveRangeDifferentBoundsThrows() {
    final Range<Integer> r1 = new Range<>(0, 10);
    final Set<Range<Integer>> ranges = new LinkedHashSet<>();
    ranges.add(r1);
    final RangeQueryIndex<Integer> index = new RangeQueryIndex<>(ranges);

    assertThrows(IOException.class, () -> index.removeRange(new Range<>(1, 2)));
    assertTrue(index.containsIntersectingRange(1), "index should remain unchanged after failed remove");
  }
}


