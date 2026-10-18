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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.utils.db.LatestVersionedKWayMergeIterator.MergedKeyValue;
import org.apache.hadoop.ozone.util.ClosableIterator;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestLatestVersionedKWayMergeIterator {

  private static Stream<Arguments> mergeScenarios() {
    return Stream.of(
        Named.of("recreate: tombstone then newer value emits both",
            scenario(
                expected(kv("k1", 2, 0, null), kv("k1", 3, 1, "v3")),
                source(kv("k1", 1, 1, "v1"), kv("k1", 2, 0, null)),
                source(kv("k1", 3, 1, "v3")))),
        Named.of("delete only: latest tombstone wins",
            scenario(
                expected(kv("k1", 5, 0, null)),
                source(kv("k1", 1, 1, "v1")),
                source(kv("k1", 5, 0, null)))),
        Named.of("value only: latest value wins",
            scenario(
                expected(kv("k1", 5, 1, "v5")),
                source(kv("k1", 1, 1, "v1")),
                source(kv("k1", 5, 1, "v5")))),
        Named.of("three-file worked example",
            scenario(
                expected(kv("k1", 30, 1, "v30"), kv("k2", 15, 0, null), kv("k2", 25, 1, "v25")),
                source(kv("k1", 10, 1, "v10"), kv("k1", 5, 1, "v5"), kv("k2", 20, 1, "v20")),
                source(kv("k1", 3, 1, "v3"), kv("k1", 15, 1, "v15"), kv("k2", 15, 0, null)),
                source(kv("k1", 1, 1, "v1"), kv("k1", 30, 1, "v30"), kv("k2", 25, 1, "v25")))),
        Named.of("multi-key: recreate on k1, delete-only on k2",
            scenario(
                expected(kv("k1", 3, 0, null), kv("k1", 10, 1, "v10"), kv("k2", 15, 0, null)),
                source(kv("k1", 10, 1, "v10"), kv("k1", 3, 0, null), kv("k2", 10, 1, "v10")),
                source(kv("k2", 15, 0, null)))),
        Named.of("duplicate tombstones deduped to highest sequence",
            scenario(
                expected(kv("k1", 7, 0, null)),
                source(kv("k1", 4, 0, null), kv("k1", 2, 0, null)),
                source(kv("k1", 7, 0, null)))),
        Named.of("multiple recreate cycles on same key",
            scenario(
                expected(kv("k1", 4, 0, null), kv("k1", 6, 1, "v6")),
                source(
                    kv("k1", 1, 1, "v1"),
                    kv("k1", 2, 0, null),
                    kv("k1", 3, 1, "v3"),
                    kv("k1", 4, 0, null),
                    kv("k1", 5, 1, "v5"),
                    kv("k1", 6, 1, "v6")))),
        Named.of("interleaved keys preserve user-key order",
            scenario(
                expected(kv("a", 1, 1, "a1"), kv("b", 2, 1, "b1"), kv("c", 3, 1, "c1")),
                source(kv("a", 1, 1, "a1"), kv("c", 3, 1, "c1")),
                source(kv("b", 2, 1, "b1")))),
        Named.of("empty source files are ignored",
            scenario(
                expected(kv("k1", 2, 1, "v2")),
                source(kv("k1", 1, 1, "v1")),
                source(),
                source(kv("k1", 2, 1, "v2"))))
    ).map(Arguments::of);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("mergeScenarios")
  void testMergeMatchesExpectedOutput(MergeScenario scenario) {
    assertResultsEqual(scenario.expected, merge(scenario.sources));
  }

  @Test
  void testExclusiveMinSequenceNumberFiltersPerKey() {
    long exclusiveMinSequenceNumber = 5L;
    List<List<MergedKeyValue>> sources = Arrays.asList(
        Arrays.asList(
            kv("k1", 7, 1, "v7"),
            kv("k2", 1, 1, "v1"),
            kv("k3", 6, 0, null),
            kv("k4", 10, 1, "v10")),
        Arrays.asList(
            kv("k1", 3, 0, null),
            kv("k2", 3, 0, null),
            kv("k3", 8, 1, "v8")));
    List<MergedKeyValue> expected = Arrays.asList(
        kv("k1", 7, 1, "v7"),
        kv("k3", 6, 0, null),
        kv("k3", 8, 1, "v8"),
        kv("k4", 10, 1, "v10"));

    assertResultsEqual(expected, merge(sources, exclusiveMinSequenceNumber));
  }

  private static List<MergedKeyValue> merge(List<List<MergedKeyValue>> sources) {
    return merge(sources, null);
  }

  private static List<MergedKeyValue> merge(List<List<MergedKeyValue>> sources,
      Long exclusiveMinSequenceNumber) {
    List<ClosableIterator<MergedKeyValue>> iterators = sources.stream()
        .map(ListIterator::new)
        .collect(Collectors.toList());

    List<MergedKeyValue> results = new ArrayList<>();
    try (LatestVersionedKWayMergeIterator iterator =
             LatestVersionedKWayMergeIterator.forTest(iterators, exclusiveMinSequenceNumber)) {
      while (iterator.hasNext()) {
        results.add(iterator.next());
      }
    }
    return results;
  }

  private static void assertResultsEqual(List<MergedKeyValue> expected, List<MergedKeyValue> actual) {
    assertEquals(expected.size(), actual.size(),
        () -> "expected=" + describe(expected) + " actual=" + describe(actual));

    for (int i = 0; i < expected.size(); i++) {
      MergedKeyValue exp = expected.get(i);
      MergedKeyValue act = actual.get(i);
      assertArrayEquals(exp.getUserKey(), act.getUserKey(), "key mismatch at index " + i);
      assertEquals(exp.getSequence(), act.getSequence(), "sequence mismatch at index " + i);
      assertEquals(exp.getValueType(), act.getValueType(), "type mismatch at index " + i);
      if (exp.getValue() == null) {
        assertNull(act.getValue(), "value should be null at index " + i);
      } else {
        assertArrayEquals(exp.getValue(), act.getValue(), "value mismatch at index " + i);
      }
    }
  }

  private static String describe(List<MergedKeyValue> entries) {
    StringBuilder sb = new StringBuilder("[");
    for (MergedKeyValue entry : entries) {
      sb.append('{')
          .append(asString(entry.getUserKey()))
          .append(", seq=").append(entry.getSequence())
          .append(", type=").append(entry.getValueType())
          .append("} ");
    }
    return sb.append(']').toString();
  }

  private static MergeScenario scenario(Expected expected, Source... sources) {
    List<List<MergedKeyValue>> sourceKvs = new ArrayList<>();
    for (Source source : sources) {
      sourceKvs.add(source.entries);
    }
    return new MergeScenario(sourceKvs, expected.entries);
  }

  private static Expected expected(MergedKeyValue... entries) {
    return new Expected(Arrays.asList(entries));
  }

  private static Source source(MergedKeyValue... entries) {
    return new Source(Arrays.asList(entries));
  }

  private static MergedKeyValue kv(String key, long sequence, int type, String value) {
    return MergedKeyValue.of(
        key.getBytes(StandardCharsets.UTF_8),
        sequence,
        type,
        value == null ? null : value.getBytes(StandardCharsets.UTF_8));
  }

  private static String asString(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static final class MergeScenario {
    private final List<List<MergedKeyValue>> sources;
    private final List<MergedKeyValue> expected;

    private MergeScenario(List<List<MergedKeyValue>> sources,
        List<MergedKeyValue> expected) {
      this.sources = sources;
      this.expected = expected;
    }
  }

  private static final class Expected {
    private final List<MergedKeyValue> entries;

    private Expected(List<MergedKeyValue> entries) {
      this.entries = entries;
    }
  }

  private static final class Source {
    private final List<MergedKeyValue> entries;

    private Source(List<MergedKeyValue> entries) {
      this.entries = entries;
    }
  }

  private static final class ListIterator implements ClosableIterator<MergedKeyValue> {
    private final Iterator<MergedKeyValue> iterator;

    private ListIterator(List<MergedKeyValue> entries) {
      this.iterator = entries.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public MergedKeyValue next() {
      return iterator.next();
    }

    @Override
    public void close() {
      // Nothing to close.
    }
  }
}
