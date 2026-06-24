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
import org.apache.hadoop.hdds.utils.db.ExpectedLatestVersionMergeOutput.SourceRecord;
import org.apache.hadoop.hdds.utils.db.LatestVersionedKWayMergeIterator.MergedKeyValue;
import org.apache.hadoop.ozone.util.ClosableIterator;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestLatestVersionedKWayMergeIterator {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestLatestVersionedKWayMergeIterator.class);

  private static Stream<Arguments> mergeScenarios() {
    return Stream.of(
        Named.of("recreate: tombstone then newer value emits both",
            scenario(
                source("A", kv("k1", 1, 1, "v1"), kv("k1", 2, 0, null)),
                source("B", kv("k1", 3, 1, "v3")))),
        Named.of("delete only: latest tombstone wins",
            scenario(
                source("A", kv("k1", 1, 1, "v1")),
                source("B", kv("k1", 5, 0, null)))),
        Named.of("value only: latest value wins",
            scenario(
                source("A", kv("k1", 1, 1, "v1")),
                source("B", kv("k1", 5, 1, "v5")))),
        Named.of("three-file worked example",
            scenario(
                source("A", kv("k1", 10, 1, "v10"), kv("k1", 5, 1, "v5"), kv("k2", 20, 1, "v20")),
                source("B", kv("k1", 3, 1, "v3"), kv("k1", 15, 1, "v15"), kv("k2", 15, 0, null)),
                source("C", kv("k1", 1, 1, "v1"), kv("k1", 30, 1, "v30"), kv("k2", 25, 1, "v25")))),
        Named.of("multi-key: recreate on k1, delete-only on k2",
            scenario(
                source("A", kv("k1", 10, 1, "v10"), kv("k1", 3, 0, null), kv("k2", 10, 1, "v10")),
                source("B", kv("k2", 15, 0, null)))),
        Named.of("tombstone newer than value emits tombstone only",
            scenario(
                source("A", kv("k1", 10, 1, "v10")),
                source("B", kv("k1", 20, 0, null)))),
        Named.of("duplicate tombstones deduped to highest sequence",
            scenario(
                source("A", kv("k1", 4, 0, null), kv("k1", 2, 0, null)),
                source("B", kv("k1", 7, 0, null)))),
        Named.of("multiple recreate cycles on same key",
            scenario(
                source("A",
                    kv("k1", 1, 1, "v1"),
                    kv("k1", 2, 0, null),
                    kv("k1", 3, 1, "v3"),
                    kv("k1", 4, 0, null),
                    kv("k1", 5, 1, "v5"),
                    kv("k1", 6, 1, "v6")))),
        Named.of("interleaved keys preserve user-key order",
            scenario(
                source("A", kv("a", 1, 1, "a1"), kv("c", 3, 1, "c1")),
                source("B", kv("b", 2, 1, "b1")))),
        Named.of("empty source files are ignored",
            scenario(
                source("A", kv("k1", 1, 1, "v1")),
                source("B"),
                source("C", kv("k1", 2, 1, "v2"))))
    ).map(Arguments::of);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("mergeScenarios")
  void testMergeMatchesExpectedOutput(MergeScenario scenario) {
    List<SourceRecord> expected =
        ExpectedLatestVersionMergeOutput.fromSourceRecords(scenario.sourceRecords);
    List<MergedKeyValue> actual = merge(scenario.sources);

    LOG.info("scenario={}", scenario.name);
    logExpected(expected);
    logActual(actual);

    assertResultsEqual(expected, actual);
  }

  @Test
  void testSkipsEntriesAtOrBelowExclusiveMinSequenceNumber() {
    List<List<MergedKeyValue>> sources = Arrays.asList(
        Arrays.asList(
            kv("k1", 1, 1, "v1"),
            kv("k1", 4, 1, "v4"),
            kv("k2", 10, 1, "v10")),
        Arrays.asList(
            kv("k1", 2, 0, null),
            kv("k1", 5, 1, "v5")));
    List<List<SourceRecord>> filtered = filterSourceRecordsAboveSequence(sources, 3L);
    List<SourceRecord> expected =
        ExpectedLatestVersionMergeOutput.fromSourceRecords(filtered);
    List<MergedKeyValue> actual = merge(sources, 3L);

    assertResultsEqual(expected, actual);
  }

  @Test
  void testExclusiveMinSequenceNumberRemovesKeyWhenAllVersionsSkipped() {
    List<List<MergedKeyValue>> sources = Arrays.asList(
        Arrays.asList(kv("k1", 1, 1, "v1"), kv("k1", 2, 0, null)),
        Arrays.asList(kv("k2", 10, 1, "v10")));
    List<List<SourceRecord>> filtered = filterSourceRecordsAboveSequence(sources, 5L);
    List<SourceRecord> expected =
        ExpectedLatestVersionMergeOutput.fromSourceRecords(filtered);
    List<MergedKeyValue> actual = merge(sources, 5L);

    assertResultsEqual(expected, actual);
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

  private static List<List<SourceRecord>> filterSourceRecordsAboveSequence(
      List<List<MergedKeyValue>> sources, long exclusiveMinSequenceNumber) {
    List<List<SourceRecord>> filtered = new ArrayList<>(sources.size());
    for (List<MergedKeyValue> source : sources) {
      List<SourceRecord> records = new ArrayList<>();
      for (MergedKeyValue entry : source) {
        if (entry.getSequence() > exclusiveMinSequenceNumber) {
          records.add(toSourceRecord(entry));
        }
      }
      filtered.add(records);
    }
    return filtered;
  }

  private static void assertResultsEqual(List<SourceRecord> expected, List<MergedKeyValue> actual) {
    assertEquals(expected.size(), actual.size(),
        () -> "expected=" + expected + " actual=" + describe(actual));

    for (int i = 0; i < expected.size(); i++) {
      SourceRecord exp = expected.get(i);
      MergedKeyValue act = actual.get(i);
      assertArrayEquals(exp.getUserKey(), act.getUserKey(), "key mismatch at index " + i);
      assertEquals(exp.getSequence(), act.getSequence(), "sequence mismatch at index " + i);
      assertEquals(exp.getType(), act.getValueType(), "type mismatch at index " + i);
      if (exp.getValue() == null) {
        assertNull(act.getValue(), "value should be null at index " + i);
      } else {
        assertArrayEquals(exp.getValue(), act.getValue(), "value mismatch at index " + i);
      }
    }
  }

  private static String describe(List<MergedKeyValue> actual) {
    StringBuilder sb = new StringBuilder("[");
    for (MergedKeyValue entry : actual) {
      sb.append('{')
          .append(asString(entry.getUserKey()))
          .append(", seq=").append(entry.getSequence())
          .append(", type=").append(entry.getValueType())
          .append("} ");
    }
    return sb.append(']').toString();
  }

  private static MergeScenario scenario(Source... sources) {
    String name = Arrays.stream(sources)
        .map(Source::getName)
        .collect(Collectors.joining("+"));
    List<List<MergedKeyValue>> sourceKvs = new ArrayList<>();
    List<List<SourceRecord>> sourceRecords = new ArrayList<>();
    for (Source source : sources) {
      sourceKvs.add(source.entries);
      sourceRecords.add(source.entries.stream()
          .map(TestLatestVersionedKWayMergeIterator::toSourceRecord)
          .collect(Collectors.toList()));
    }
    return new MergeScenario(name, sourceKvs, sourceRecords);
  }

  private static Source source(String name, MergedKeyValue... entries) {
    return new Source(name, Arrays.asList(entries));
  }

  private static MergedKeyValue kv(String key, long sequence, int type, String value) {
    return MergedKeyValue.of(
        key.getBytes(StandardCharsets.UTF_8),
        sequence,
        type,
        value == null ? null : value.getBytes(StandardCharsets.UTF_8));
  }

  private static SourceRecord toSourceRecord(MergedKeyValue entry) {
    return new SourceRecord(entry.getUserKey(), entry.getSequence(), entry.getValueType(),
        entry.getValue());
  }

  private static String asString(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static void logExpected(List<SourceRecord> expected) {
    for (SourceRecord entry : expected) {
      LOG.info("expected key={} seq={} type={}",
          asString(entry.getUserKey()), entry.getSequence(), entry.getType());
    }
  }

  private static void logActual(List<MergedKeyValue> actual) {
    for (MergedKeyValue entry : actual) {
      LOG.info("actual key={} seq={} type={}",
          asString(entry.getUserKey()), entry.getSequence(), entry.getValueType());
    }
  }

  private static final class MergeScenario {
    private final String name;
    private final List<List<MergedKeyValue>> sources;
    private final List<List<SourceRecord>> sourceRecords;

    private MergeScenario(String name, List<List<MergedKeyValue>> sources,
        List<List<SourceRecord>> sourceRecords) {
      this.name = name;
      this.sources = sources;
      this.sourceRecords = sourceRecords;
    }
  }

  private static final class Source {
    private final String name;
    private final List<MergedKeyValue> entries;

    private Source(String name, List<MergedKeyValue> entries) {
      this.name = name;
      this.entries = entries;
    }

    private String getName() {
      return name;
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
