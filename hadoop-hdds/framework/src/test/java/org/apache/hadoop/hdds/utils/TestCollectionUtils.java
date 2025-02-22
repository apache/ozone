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

package org.apache.hadoop.hdds.utils;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.reverseOrder;
import static java.util.Collections.singletonList;
import static java.util.Comparator.naturalOrder;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

/** Test for {@link CollectionUtils}. */
public class TestCollectionUtils {
  @Test
  void singleNonEmptyLists() {
    List<List<String>> listOfLists = new ArrayList<>();
    listOfLists.add(asList("a", "b"));
    assertIteration(asList("a", "b"), listOfLists);
  }

  @Test
  void allNonEmptyLists() {
    List<List<String>> listOfLists = new ArrayList<>();
    listOfLists.add(asList("a", "b"));
    listOfLists.add(singletonList("c"));
    listOfLists.add(singletonList("d"));
    assertIteration(asList("a", "b", "c", "d"), listOfLists);
  }

  @Test
  void singleEmptyLists() {
    List<List<String>> listOfLists = new ArrayList<>();
    listOfLists.add(emptyList());
    assertIteration(emptyList(), listOfLists);
  }

  @Test
  void someEmptyLists() {
    List<List<String>> listOfLists = new ArrayList<>();
    listOfLists.add(asList("a", "b"));
    listOfLists.add(emptyList());
    listOfLists.add(emptyList());
    listOfLists.add(singletonList("c"));
    listOfLists.add(singletonList("d"));
    listOfLists.add(emptyList());

    assertIteration(asList("a", "b", "c", "d"), listOfLists);
  }

  @Test
  void allEmptyLists() {
    List<List<String>> listOfLists = new ArrayList<>();
    listOfLists.add(emptyList());
    listOfLists.add(emptyList());

    assertIteration(emptyList(), listOfLists);
  }

  @Test
  void empty() {
    assertIteration(emptyList(), emptyList());
  }

  private static <T> void assertIteration(List<T> expected,
      List<List<T>> listOfLists) {
    List<T> actual = new ArrayList<>();
    CollectionUtils.newIterator(listOfLists).forEachRemaining(actual::add);
    assertEquals(expected, actual);
  }

  @Test
  void topN() {
    testTopNInts(Arrays.asList(5, 8, 10, 20, 1, 2, 3, 42));
    testTopNStrings(Arrays.asList("abc", "QWERTY", "hello world", "mayday",
        "a new day", "\n", "LICENSE"));
  }

  private void testTopNStrings(List<String> strings) {
    testTopN(strings,
        s -> s.startsWith("a"),
        s -> s.equals(s.toLowerCase()));
  }

  private static void testTopNInts(List<Integer> ints) {
    testTopN(ints,
        i -> (i % 2 == 0),
        i -> (i < 20));
  }

  @SafeVarargs
  private static <T extends Comparable<T>> void testTopN(List<T> items,
      Predicate<T>... predicates) {

    testTopN(items, naturalOrder(), any -> true);
    testTopN(items, reverseOrder(), any -> true);

    for (Predicate<T> predicate : predicates) {
      testTopN(items, naturalOrder(), predicate);
      testTopN(items, reverseOrder(), predicate);
    }
  }

  private static <T> void testTopN(List<T> items, Comparator<T> comparator,
      Predicate<T> predicate) {
    List<T> shuffled = new ArrayList<>(items);
    Collections.shuffle(shuffled);

    List<T> sorted = new ArrayList<>(items);
    sorted.sort(comparator.reversed()); // descending order

    for (int i = 0; i <= items.size() + 1; i++) {
      assertTopN(items, comparator, predicate, sorted, i);
    }
    assertTopN(items, comparator, predicate, sorted, Integer.MAX_VALUE);
  }

  private static <T> void assertTopN(List<T> items, Comparator<T> comparator,
      Predicate<T> filter, List<T> sorted, int limit) {
    assertEquals(
        sorted.stream().filter(filter).limit(limit).collect(toList()),
        CollectionUtils.findTopN(items, limit, comparator, filter));
  }
}
